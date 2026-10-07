use crate::config::AppConfig;
use crate::errors::{ApiError, ApiErrorBody};
use crate::migration_tracker::MigrationStatus;
use actix_web::{get, web, HttpResponse};
use bsky_sdk::api::types::string::Did;
use serde::Deserialize;

#[derive(Deserialize)]
struct MigrationsQuery {
    did: Option<String>,
}

#[utoipa::path(
    get,
    path = "/migrations",
    params(("did" = Option<String>, Query, description = "Optional DID to check")),
    responses(
        (status = 200, description = "Current migration count and capacity", body = MigrationStatus),
        (status = 400, description = "Invalid DID", body = ApiErrorBody)
    ),
    security(()),
    tag = "pdsmigration-web"
)]
#[get("/migrations")]
pub async fn get_migrations_api(
    config: web::Data<AppConfig>,
    query: web::Query<MigrationsQuery>,
) -> Result<HttpResponse, ApiError> {
    let did = query
        .did
        .as_deref()
        .map(str::parse::<Did>)
        .transpose()
        .map_err(|_error| ApiError::Validation {
            field: "did".to_string(),
        })?;

    Ok(HttpResponse::Ok().json(config.migration_tracker.status(did)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::migration_tracker::MigrationTracker;
    use actix_web::{http::StatusCode, test, web, App};
    use std::time::Duration;

    #[actix_rt::test]
    async fn migrations_endpoint_returns_count() {
        let config = AppConfig {
            port: 8080,
            workers: 1,
            concurrent_tasks_per_job: 1,
            upload_max_attempts: 1,
            rate_limit_window_secs: 60,
            rate_limit_max_requests: 60,
            job_retention_secs: 60,
            artifact_retention_secs: 60,
            artifact_gc_interval_secs: 60,
            auth_token: Some("secret".to_string()),
            migration_tracker: MigrationTracker::new(Duration::from_secs(60), 1),
        };
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config))
                .service(get_migrations_api),
        )
        .await;

        let request = test::TestRequest::get().uri("/migrations").to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["has_capacity"], true);
        assert!(body.get("did").is_none());

        let request = test::TestRequest::get()
            .uri("/migrations?did=did%3Aplc%3Aabcd1234efgh5678ijkl")
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);

        let request = test::TestRequest::get()
            .uri("/migrations?did=not-a-did")
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
