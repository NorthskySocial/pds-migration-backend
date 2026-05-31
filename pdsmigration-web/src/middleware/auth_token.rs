use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::header::HeaderMap;
use actix_web::{web, Error, ResponseError};
use futures::future::{ready, LocalBoxFuture, Ready};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::config::AppConfig;
use crate::errors::ApiError;

#[derive(Clone, Default)]
pub struct AuthToken;

impl AuthToken {
    pub fn new() -> Self {
        Self
    }
}

impl<S, B> Transform<S, ServiceRequest> for AuthToken
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody, B>>;
    type Error = Error;
    type Transform = AuthTokenMiddleware<S, B>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthTokenMiddleware {
            service: Arc::new(service),
            _phantom: std::marker::PhantomData,
        }))
    }
}

pub struct AuthTokenMiddleware<S, B> {
    service: Arc<S>,
    _phantom: std::marker::PhantomData<B>,
}

impl<S, B> Service<ServiceRequest> for AuthTokenMiddleware<S, B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody, B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Bypass for certain paths
        let path = req.path();
        let bypass = is_bypass_path(path);

        // Get configured token (if any)
        let maybe_cfg = req.app_data::<web::Data<AppConfig>>().cloned();

        let service = self.service.clone();

        Box::pin(async move {
            let configured_token = maybe_cfg.as_ref().and_then(|d| d.server.auth_token.clone());

            if !bypass {
                if let Some(expected) = configured_token {
                    // Enforce token check
                    let headers = req.headers();
                    if !is_authorized(headers, &expected) {
                        let api_err = ApiError::Authentication {
                            message: "Invalid or missing auth token".to_string(),
                        };
                        let resp = api_err.error_response();
                        return Ok(req.into_response(resp).map_into_left_body());
                    }
                }
            }

            let res = service.call(req).await?;
            Ok(res.map_into_right_body())
        })
    }
}

fn is_bypass_path(path: &str) -> bool {
    // Allow health checks and documentation/metrics without auth
    path == "/health"
        || path.starts_with("/swagger-ui")
        || path.starts_with("/api-docs")
        || path == "/metrics"
}

fn is_authorized(headers: &HeaderMap, expected: &str) -> bool {
    if let Some(h) = headers.get("X-Auth-Token") {
        if let Ok(v) = h.to_str() {
            if v == expected {
                return true;
            }
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, ExternalServices, ServerConfig};
    use actix_web::http::header::{HeaderName, HeaderValue};
    use actix_web::http::StatusCode;
    use actix_web::{test as actix_test, web, App, HttpResponse};

    fn config_with_token(token: Option<&str>) -> AppConfig {
        AppConfig {
            server: ServerConfig {
                port: 0,
                workers: 1,
                concurrent_tasks_per_job: 1,
                upload_max_attempts: 4,
                rate_limit_window_secs: 60,
                rate_limit_max_requests: 60,
                auth_token: token.map(|t| t.to_string()),
            },
            external_services: ExternalServices {
                s3_endpoint: "http://test-s3.example.com".to_string(),
            },
        }
    }

    async fn ok_handler() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    #[test]
    fn is_bypass_path_allows_documented_routes() {
        assert!(is_bypass_path("/health"));
        assert!(is_bypass_path("/metrics"));
        assert!(is_bypass_path("/swagger-ui/"));
        assert!(is_bypass_path("/swagger-ui/index.html"));
        assert!(is_bypass_path("/api-docs/openapi.json"));
    }

    #[test]
    fn is_bypass_path_rejects_other_routes() {
        assert!(!is_bypass_path("/"));
        assert!(!is_bypass_path("/jobs"));
        assert!(!is_bypass_path("/create-account"));
        assert!(!is_bypass_path("/health/extra"));
    }

    #[test]
    fn is_authorized_accepts_matching_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-auth-token"),
            HeaderValue::from_static("expected-token"),
        );
        assert!(is_authorized(&headers, "expected-token"));
    }

    #[test]
    fn is_authorized_rejects_missing_header() {
        let headers = HeaderMap::new();
        assert!(!is_authorized(&headers, "expected-token"));
    }

    #[test]
    fn is_authorized_rejects_wrong_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-auth-token"),
            HeaderValue::from_static("bad-token"),
        );
        assert!(!is_authorized(&headers, "expected-token"));
    }

    #[actix_rt::test]
    async fn middleware_allows_request_when_no_token_configured() {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config_with_token(None)))
                .wrap(AuthToken::new())
                .route("/protected", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/protected")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn middleware_blocks_when_token_missing() {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config_with_token(Some("expected"))))
                .wrap(AuthToken::new())
                .route("/protected", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/protected")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_rt::test]
    async fn middleware_allows_when_token_matches() {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config_with_token(Some("expected"))))
                .wrap(AuthToken::new())
                .route("/protected", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/protected")
            .insert_header(("X-Auth-Token", "expected"))
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn middleware_blocks_when_token_mismatched() {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config_with_token(Some("expected"))))
                .wrap(AuthToken::new())
                .route("/protected", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/protected")
            .insert_header(("X-Auth-Token", "wrong"))
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_rt::test]
    async fn middleware_bypasses_health_without_token() {
        let app = actix_test::init_service(
            App::new()
                .app_data(web::Data::new(config_with_token(Some("expected"))))
                .wrap(AuthToken::new())
                .route("/health", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get().uri("/health").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn middleware_allows_when_no_config_data_attached() {
        let app = actix_test::init_service(
            App::new()
                .wrap(AuthToken::new())
                .route("/protected", web::get().to(ok_handler)),
        )
        .await;

        let req = actix_test::TestRequest::get()
            .uri("/protected")
            .to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
