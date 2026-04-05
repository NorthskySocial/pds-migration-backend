use actix_web::{http::StatusCode, test, web, App};
use pdsmigration_web::{
    api::{
        activate_account_api, cancel_job_api, create_account_api, deactivate_account_api,
        enqueue_export_blobs_job_api, enqueue_upload_blobs_job_api, export_blobs_api,
        export_pds_api, get_job_api, get_service_auth_api, health_check, import_pds_api,
        list_jobs_api, migrate_plc_api, migrate_preferences_api, missing_blobs_api,
        request_token_api, upload_blobs_api,
    },
    background_jobs::JobManager,
    config::{AppConfig, ExternalServices, ServerConfig},
};
use serde_json::json;

#[cfg(test)]
mod integration_tests {
    use super::*;

    fn create_test_config() -> AppConfig {
        AppConfig {
            server: ServerConfig {
                port: 8080,
                workers: 1,
                concurrent_tasks_per_job: 3,
                rate_limit_window_secs: 60,
                rate_limit_max_requests: 60,
                auth_token: None,
            },
            external_services: ExternalServices {
                s3_endpoint: "http://test-s3.example.com".to_string(),
            },
        }
    }

    #[actix_rt::test]
    async fn test_health_endpoint() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(health_check),
        )
        .await;

        let req = test::TestRequest::get().uri("/health").to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        assert_eq!(body, "OK");
    }

    #[actix_rt::test]
    async fn test_all_routes_configured() {
        let app_config = create_test_config();

        // Test that we can create an app with all routes without errors
        let _app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(health_check)
                .service(request_token_api)
                .service(create_account_api)
                .service(export_pds_api)
                .service(import_pds_api)
                .service(missing_blobs_api)
                .service(export_blobs_api)
                .service(upload_blobs_api)
                .service(activate_account_api)
                .service(deactivate_account_api)
                .service(migrate_preferences_api)
                .service(migrate_plc_api)
                .service(get_service_auth_api),
        )
        .await;
    }

    #[actix_rt::test]
    async fn test_create_account_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(create_account_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/create-account")
            .set_json(json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_request_token_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(request_token_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/request-token")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_export_pds_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(export_pds_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/export-pds")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_import_pds_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(import_pds_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/import-pds")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_missing_blobs_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(missing_blobs_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/missing-blobs")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_export_blobs_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(export_blobs_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/export-blobs")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_upload_blobs_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(upload_blobs_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/upload-blobs")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_activate_account_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(activate_account_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/activate-account")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_deactivate_account_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(deactivate_account_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/deactivate-account")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_migrate_preferences_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(migrate_preferences_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/migrate-preferences")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_migrate_plc_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(migrate_plc_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/migrate-plc")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_get_service_auth_missing_fields() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(get_service_auth_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/get-service-auth")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_invalid_route() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(health_check),
        )
        .await;

        let req = test::TestRequest::get().uri("/non-existent").to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_wrong_http_method() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(create_account_api),
        )
        .await;

        let req = test::TestRequest::get().uri("/create-account").to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_malformed_json() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(create_account_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/create-account")
            .set_payload("invalid json{")
            .insert_header(("content-type", "application/json"))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_create_account_with_invalid_did() {
        let app_config = create_test_config();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .service(create_account_api),
        )
        .await;

        let invalid_request = json!({
            "email": "test@example.com",
            "handle": "test.bsky.social",
            "invite_code": "test-invite",
            "password": "testpass123",
            "token": "test-token",
            "pds_host": "https://test.pds.host",
            "did": "invalid-did-format"
        });

        let req = test::TestRequest::post()
            .uri("/create-account")
            .set_json(&invalid_request)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_list_jobs_endpoint() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(list_jobs_api),
        )
        .await;

        let req = test::TestRequest::get().uri("/jobs").to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(jobs.len(), 0);
    }

    #[actix_rt::test]
    async fn test_enqueue_export_blobs_job_missing_fields() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(enqueue_export_blobs_job_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/jobs/export-blobs")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_cancel_job_with_invalid_id() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(cancel_job_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/jobs/550e8400-e29b-41d4-a716-446655440000/cancel")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["success"], false);
    }

    #[actix_rt::test]
    async fn test_get_nonexistent_job() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(get_job_api),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/jobs/550e8400-e29b-41d4-a716-446655440000")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_cancel_job_with_invalid_uuid_format() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(cancel_job_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/jobs/invalid-uuid/cancel")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_get_job_with_invalid_uuid_format() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(get_job_api),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/jobs/not-a-uuid")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[actix_rt::test]
    async fn test_get_existing_job() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_export_blobs_job_api)
                .service(get_job_api),
        )
        .await;

        let export_request = json!({
            "destination": "https://destination.pds.host",
            "origin": "https://origin.pds.host",
            "did": "did:plc:test123456789",
            "origin_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature",
            "destination_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
        });

        let req = test::TestRequest::post()
            .uri("/jobs/export-blobs")
            .set_json(&export_request)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let body = test::read_body(resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job_id = response["job_id"].as_str().unwrap();

        let get_req = test::TestRequest::get()
            .uri(&format!("/jobs/{}", job_id))
            .to_request();

        let get_resp = test::call_service(&app, get_req).await;
        assert_eq!(get_resp.status(), StatusCode::OK);

        let job_body = test::read_body(get_resp).await;
        let job: serde_json::Value = serde_json::from_slice(&job_body).unwrap();
        assert_eq!(job["id"].as_str().unwrap(), job_id);
    }

    #[actix_rt::test]
    async fn test_list_jobs_with_existing_jobs() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_export_blobs_job_api)
                .service(list_jobs_api),
        )
        .await;

        for i in 0..2 {
            let export_request = json!({
                "destination": format!("https://destination{}.pds.host", i),
                "origin": "https://origin.pds.host",
                "did": "did:plc:test123456789",
                "origin_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature",
                "destination_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
            });

            let req = test::TestRequest::post()
                .uri("/jobs/export-blobs")
                .set_json(&export_request)
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }

        let list_req = test::TestRequest::get().uri("/jobs").to_request();

        let list_resp = test::call_service(&app, list_req).await;
        assert_eq!(list_resp.status(), StatusCode::OK);

        let body = test::read_body(list_resp).await;
        let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[actix_rt::test]
    async fn test_cancel_existing_job() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_export_blobs_job_api)
                .service(cancel_job_api),
        )
        .await;

        let export_request = json!({
            "destination": "https://destination.pds.host",
            "origin": "https://origin.pds.host",
            "did": "did:plc:test123456789",
            "origin_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature",
            "destination_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
        });

        let req = test::TestRequest::post()
            .uri("/jobs/export-blobs")
            .set_json(&export_request)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let body = test::read_body(resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job_id = response["job_id"].as_str().unwrap();

        let cancel_req = test::TestRequest::post()
            .uri(&format!("/jobs/{}/cancel", job_id))
            .to_request();

        let cancel_resp = test::call_service(&app, cancel_req).await;
        assert_eq!(cancel_resp.status(), StatusCode::OK);

        let cancel_body = test::read_body(cancel_resp).await;
        let cancel_response: serde_json::Value = serde_json::from_slice(&cancel_body).unwrap();
        assert_eq!(cancel_response["success"], true);
    }

    #[actix_rt::test]
    async fn test_enqueue_upload_blobs_job_missing_fields() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager)
                .service(enqueue_upload_blobs_job_api),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/jobs/upload-blobs")
            .set_json(&json!({}))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn test_get_existing_upload_blobs_job() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_upload_blobs_job_api)
                .service(get_job_api),
        )
        .await;

        let upload_request = json!({
            "pds_host": "https://destination.pds.host",
            "did": "did:plc:test123456789",
            "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
        });

        let req = test::TestRequest::post()
            .uri("/jobs/upload-blobs")
            .set_json(&upload_request)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let body = test::read_body(resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job_id = response["job_id"].as_str().unwrap();

        let get_req = test::TestRequest::get()
            .uri(&format!("/jobs/{}", job_id))
            .to_request();

        let get_resp = test::call_service(&app, get_req).await;
        assert_eq!(get_resp.status(), StatusCode::OK);

        let job_body = test::read_body(get_resp).await;
        let job: serde_json::Value = serde_json::from_slice(&job_body).unwrap();
        assert_eq!(job["id"].as_str().unwrap(), job_id);
    }

    #[actix_rt::test]
    async fn test_list_jobs_with_upload_blobs_jobs() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_upload_blobs_job_api)
                .service(list_jobs_api),
        )
        .await;

        for i in 0..2 {
            let upload_request = json!({
                "pds_host": format!("https://destination{}.pds.host", i),
                "did": "did:plc:test123456789",
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
            });

            let req = test::TestRequest::post()
                .uri("/jobs/upload-blobs")
                .set_json(&upload_request)
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
        }

        let list_req = test::TestRequest::get().uri("/jobs").to_request();

        let list_resp = test::call_service(&app, list_req).await;
        assert_eq!(list_resp.status(), StatusCode::OK);

        let body = test::read_body(list_resp).await;
        let jobs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(jobs.len(), 2);
    }

    #[actix_rt::test]
    async fn test_cancel_upload_blobs_job() {
        let app_config = create_test_config();
        let job_manager = web::Data::new(JobManager::new());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_config))
                .app_data(job_manager.clone())
                .service(enqueue_upload_blobs_job_api)
                .service(cancel_job_api),
        )
        .await;

        let upload_request = json!({
            "pds_host": "https://destination.pds.host",
            "did": "did:plc:test123456789",
            "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature"
        });

        let req = test::TestRequest::post()
            .uri("/jobs/upload-blobs")
            .set_json(&upload_request)
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let body = test::read_body(resp).await;
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let job_id = response["job_id"].as_str().unwrap();

        let cancel_req = test::TestRequest::post()
            .uri(&format!("/jobs/{}/cancel", job_id))
            .to_request();

        let cancel_resp = test::call_service(&app, cancel_req).await;
        assert_eq!(cancel_resp.status(), StatusCode::OK);

        let cancel_body = test::read_body(cancel_resp).await;
        let cancel_response: serde_json::Value = serde_json::from_slice(&cancel_body).unwrap();
        assert_eq!(cancel_response["success"], true);
    }
}
