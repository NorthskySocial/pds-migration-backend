use actix_web::{http::StatusCode, test, App};
use pdsmigration_common::repo_car_path;
use pdsmigration_web::api::export_pds_api;
use serde_json::json;
use std::env;
use std::sync::{Mutex, MutexGuard};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{session_body, unique_did};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn set_endpoint_env(value: Option<&str>) -> (MutexGuard<'static, ()>, Option<String>) {
    let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let previous = env::var("ENDPOINT").ok();
    match value {
        Some(v) => env::set_var("ENDPOINT", v),
        None => env::remove_var("ENDPOINT"),
    }
    (guard, previous)
}

fn restore_endpoint_env(previous: Option<String>) {
    match previous {
        Some(v) => env::set_var("ENDPOINT", v),
        None => env::remove_var("ENDPOINT"),
    }
}

#[actix_rt::test]
async fn export_repo_sync_api_succeeds_with_mocked_pds_and_s3() {
    let pds = MockServer::start().await;
    let s3 = MockServer::start().await;
    let did = unique_did("websyncsuccess");
    let payload: &[u8] = b"sync-export-repo-payload";

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&pds)
        .await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&pds)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&s3)
        .await;

    let car_path = repo_car_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_file(&car_path);

    let app = test::init_service(App::new().service(export_pds_api)).await;
    let (_guard, previous) = set_endpoint_env(Some(&s3.uri()));
    let req = test::TestRequest::post()
        .uri("/export-repo")
        .set_json(json!({
            "pds_host": pds.uri(),
            "did": did,
            "token": "origin-jwt",
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    restore_endpoint_env(previous);

    let uploaded = s3.received_requests().await.expect("requests recorded");
    assert!(
        uploaded.iter().any(|r| r.method.as_str() == "PUT"),
        "S3 mock should receive an upload PUT request"
    );
    let on_disk = std::fs::read(&car_path).expect("export should write CAR file");
    assert_eq!(on_disk, payload);
    let _ = std::fs::remove_file(&car_path);
}

#[actix_rt::test]
async fn export_repo_sync_api_returns_runtime_error_without_endpoint_env() {
    let app = test::init_service(App::new().service(export_pds_api)).await;

    let (_guard, previous) = set_endpoint_env(None);
    let req = test::TestRequest::post()
        .uri("/export-repo")
        .set_json(json!({
            "pds_host": "https://pds.example.com",
            "did": "did:plc:abc123",
            "token": "origin-jwt",
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = test::read_body(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "Runtime");
    restore_endpoint_env(previous);
}
