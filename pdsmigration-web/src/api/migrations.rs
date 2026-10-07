use crate::config::AppConfig;
use crate::migration_tracker::MigrationStatus;
use actix_web::{get, web, HttpResponse, Responder};

#[utoipa::path(
    get,
    path = "/migrations",
    responses(
        (status = 200, description = "Current migration count and capacity", body = MigrationStatus)
    ),
    security(()),
    tag = "pdsmigration-web"
)]
#[get("/migrations")]
pub async fn get_migrations_api(config: web::Data<AppConfig>) -> impl Responder {
    HttpResponse::Ok().json(config.migration_tracker.status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::migration_tracker::MigrationTracker;
    use actix_web::{http::StatusCode, test, web, App};
    use std::time::Duration;

    #[actix_rt::test]
    async fn migrations_endpoint_returns_only_count_and_capacity() {
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
        config.migration_tracker.refresh("did:plc:private");
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
        assert_eq!(body["ongoing_migrations"], 1);
        assert_eq!(body["has_capacity"], false);
        assert!(body.get("did").is_none());
    }
}
