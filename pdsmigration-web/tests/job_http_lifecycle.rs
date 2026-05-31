use actix_web::{http::StatusCode, test, web, App};
use pdsmigration_common::did_blobs_path;
use pdsmigration_web::{
    api::{enqueue_export_blobs_job_api, get_job_api},
    background_jobs::JobManager,
};
use serde_json::json;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{create_test_config, session_body, unique_did};

#[actix_rt::test]
async fn export_job_reaches_success_through_http_api() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("webjoblifecycle");
    let payload: &[u8] = b"http-lifecycle-blob-bytes";
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    let session_body = session_body(&did);
    for server in [&origin, &destination] {
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&session_body))
            .mount(server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "blobs": [{ "cid": blob_cid, "recordUri": record_uri }],
        })))
        .mount(&destination)
        .await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&origin)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(create_test_config()))
            .app_data(web::Data::new(JobManager::new()))
            .service(enqueue_export_blobs_job_api)
            .service(get_job_api),
    )
    .await;

    // Enqueue over HTTP and read back the job id from the 202 response.
    let enqueue = test::TestRequest::post()
        .uri("/jobs/export-blobs")
        .set_json(json!({
            "destination": destination.uri(),
            "origin": origin.uri(),
            "did": did,
            "origin_token": "origin-jwt",
            "destination_token": "destination-jwt",
            "is_missing_blob_request": false,
        }))
        .to_request();
    let enqueue_resp = test::call_service(&app, enqueue).await;
    assert_eq!(enqueue_resp.status(), StatusCode::ACCEPTED);
    let body = test::read_body(enqueue_resp).await;
    let enqueued: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let job_id = enqueued["job_id"].as_str().expect("job_id in 202 body");

    // Poll the GET endpoint until the job reaches a terminal state.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let job = loop {
        let get = test::TestRequest::get()
            .uri(&format!("/jobs/{job_id}"))
            .to_request();
        let get_resp = test::call_service(&app, get).await;
        assert_eq!(get_resp.status(), StatusCode::OK);
        let job_body = test::read_body(get_resp).await;
        let job: serde_json::Value = serde_json::from_slice(&job_body).unwrap();
        match job["status"].as_str().unwrap() {
            "success" | "error" => break job,
            _ => {}
        }
        if std::time::Instant::now() >= deadline {
            panic!("job did not finish in time; last={job:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        job["status"].as_str().unwrap(),
        "success",
        "export job should succeed against the mocked PDSes; got {job:?}"
    );
    assert_eq!(job["id"].as_str().unwrap(), job_id);
    let progress = &job["progress"];
    assert_eq!(
        progress["successful_blobs"].as_u64(),
        Some(1),
        "the one blob should be reported successful through the API"
    );
    assert_eq!(progress["invalid_blobs"].as_u64(), Some(0));
    assert_eq!(progress["total"].as_u64(), Some(1));

    let _ = std::fs::remove_dir_all(&blob_dir);
}
