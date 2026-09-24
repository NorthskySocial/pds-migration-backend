use actix_web::{http::StatusCode, test, App};
use pdsmigration_common::repo_car_path;
use pdsmigration_web::api::export_pds_api;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{session_body, unique_did};

#[actix_rt::test]
async fn export_repo_sync_api_succeeds_with_mocked_pds() {
    let pds = MockServer::start().await;
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

    let car_path = repo_car_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_file(&car_path);

    let app = test::init_service(App::new().service(export_pds_api)).await;
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

    let on_disk = std::fs::read(&car_path).expect("export should write CAR file");
    assert_eq!(on_disk, payload);
    let _ = std::fs::remove_file(&car_path);
}

#[actix_rt::test]
async fn export_repo_sync_api_returns_runtime_error_when_pds_unreachable() {
    let app = test::init_service(App::new().service(export_pds_api)).await;

    let req = test::TestRequest::post()
        .uri("/export-repo")
        .set_json(json!({
            "pds_host": "http://pds.invalid",
            "did": "did:plc:abc123",
            "token": "origin-jwt",
        }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = test::read_body(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "Runtime");
}
