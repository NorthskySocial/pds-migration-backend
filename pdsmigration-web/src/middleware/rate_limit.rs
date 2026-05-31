use futures::future::LocalBoxFuture;
use std::collections::HashMap;
use std::future::{ready, Ready};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use actix_web::ResponseError;

use crate::errors::ApiError;

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

struct RateLimiterInner {
    window: Duration,
    max_requests: u64,
    // Map of ip -> (window_start, count)
    state: Mutex<HashMap<IpAddr, (Instant, u64)>>,
}

impl RateLimiter {
    pub fn new(max_requests: u64, window: Duration) -> Self {
        Self {
            inner: Arc::new(RateLimiterInner {
                window,
                max_requests,
                state: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimiter
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody, B>>;
    type Error = Error;
    type Transform = RateLimiterMiddleware<S, B>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimiterMiddleware {
            service: Arc::new(service),
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }))
    }
}

pub struct RateLimiterMiddleware<S, B> {
    service: Arc<S>,
    inner: Arc<RateLimiterInner>,
    _phantom: std::marker::PhantomData<B>,
}

impl<S, B> Service<ServiceRequest> for RateLimiterMiddleware<S, B>
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
        let peer_ip = req
            .connection_info()
            .realip_remote_addr()
            .and_then(|s| s.split(':').next())
            .and_then(|ip_str| ip_str.parse::<IpAddr>().ok())
            .or_else(|| req.peer_addr().map(|s| s.ip()));

        let inner = self.inner.clone();
        let service = self.service.clone();

        let path = req.path().to_string();
        let is_exempt = path.starts_with("/jobs/")
            || path == "/jobs"
            || path == "/health"
            || path == "/metrics";

        Box::pin(async move {
            if !is_exempt {
                if let Some(ip) = peer_ip {
                    let now = Instant::now();
                    let mut map = inner.state.lock().expect("rate limiter mutex poisoned");
                    let entry = map.entry(ip).or_insert((now, 0));

                    // Reset window if elapsed
                    if now.duration_since(entry.0) >= inner.window {
                        *entry = (now, 0);
                    }

                    // Check before incrementing: allow exactly max_requests within window
                    if entry.1 >= inner.max_requests {
                        // Too many requests
                        drop(map);
                        let api_err = ApiError::RateLimit {
                            message: "Rate limit exceeded".to_string(),
                        };
                        let resp = api_err.error_response();
                        return Ok(req.into_response(resp).map_into_left_body());
                    }

                    // Increment count and proceed
                    entry.1 += 1;
                }
            }

            let res = service.call(req).await?;
            Ok(res.map_into_right_body())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App, HttpResponse};

    async fn ok_handler() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    fn test_app(
        limiter: RateLimiter,
    ) -> App<
        impl actix_web::dev::ServiceFactory<
            ServiceRequest,
            Config = (),
            Response = ServiceResponse<EitherBody<BoxBody>>,
            Error = Error,
            InitError = (),
        >,
    > {
        App::new()
            .wrap(limiter)
            .route("/limited", web::get().to(ok_handler))
            .route("/limited", web::post().to(ok_handler))
            .route("/health", web::get().to(ok_handler))
            .route("/metrics", web::get().to(ok_handler))
            .route("/jobs", web::get().to(ok_handler))
            .route("/jobs/{id}", web::get().to(ok_handler))
    }

    #[actix_rt::test]
    async fn allows_requests_under_limit() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        for _ in 0..3 {
            let req = test::TestRequest::get()
                .uri("/limited")
                .peer_addr("10.0.0.1:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[actix_rt::test]
    async fn rejects_requests_over_limit_with_429() {
        let limiter = RateLimiter::new(2, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        for _ in 0..2 {
            let req = test::TestRequest::get()
                .uri("/limited")
                .peer_addr("10.0.0.2:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let req = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.2:1234".parse().unwrap())
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[actix_rt::test]
    async fn per_ip_budget_is_independent() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        let req_a = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.3:1111".parse().unwrap())
            .to_request();
        assert_eq!(
            test::call_service(&app, req_a).await.status(),
            StatusCode::OK
        );

        let req_b = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.4:2222".parse().unwrap())
            .to_request();
        assert_eq!(
            test::call_service(&app, req_b).await.status(),
            StatusCode::OK
        );

        let req_a2 = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.3:1111".parse().unwrap())
            .to_request();
        assert_eq!(
            test::call_service(&app, req_a2).await.status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[actix_rt::test]
    async fn window_resets_after_elapsing() {
        let limiter = RateLimiter::new(1, Duration::from_millis(50));
        let app = test::init_service(test_app(limiter)).await;

        let req = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.5:1234".parse().unwrap())
            .to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);

        let req = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.5:1234".parse().unwrap())
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            StatusCode::TOO_MANY_REQUESTS
        );

        actix_rt::time::sleep(Duration::from_millis(80)).await;

        let req = test::TestRequest::get()
            .uri("/limited")
            .peer_addr("10.0.0.5:1234".parse().unwrap())
            .to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn health_endpoint_is_exempt() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        for _ in 0..10 {
            let req = test::TestRequest::get()
                .uri("/health")
                .peer_addr("10.0.0.6:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[actix_rt::test]
    async fn metrics_endpoint_is_exempt() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        for _ in 0..10 {
            let req = test::TestRequest::get()
                .uri("/metrics")
                .peer_addr("10.0.0.7:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[actix_rt::test]
    async fn jobs_polling_is_exempt() {
        let limiter = RateLimiter::new(1, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;

        for _ in 0..10 {
            let req = test::TestRequest::get()
                .uri("/jobs/550e8400-e29b-41d4-a716-446655440000")
                .peer_addr("10.0.0.8:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        for _ in 0..10 {
            let req = test::TestRequest::get()
                .uri("/jobs")
                .peer_addr("10.0.0.8:1234".parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[actix_rt::test]
    async fn limit_applies_across_http_methods_and_paths() {
        let limiter = RateLimiter::new(2, Duration::from_secs(60));
        let app = test::init_service(test_app(limiter)).await;
        let ip = "10.0.0.10:1234";

        let req = test::TestRequest::get()
            .uri("/limited")
            .peer_addr(ip.parse().unwrap())
            .to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);

        let req = test::TestRequest::post()
            .uri("/limited")
            .peer_addr(ip.parse().unwrap())
            .to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);

        let req = test::TestRequest::post()
            .uri("/limited")
            .peer_addr(ip.parse().unwrap())
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
