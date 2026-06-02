use pdsmigration_common::{did_blobs_path, ExportBlobsRequest};
use pdsmigration_web::background_jobs::{JobManager, JobStatus};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{await_job, unique_did};

#[tokio::test]
async fn export_blobs_job_reactivates_deactivated_origin() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;

    let did = unique_did("webjobdeactivatedorigin");
    let payload: &[u8] = b"fake-blob-bytes-after-reactivation";
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    // Origin reports the account as deactivated.
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": did,
            "handle": "anothermigration.bsky.social",
            "active": false,
            "status": "deactivated",
        })))
        .mount(&origin)
        .await;

    // Destination reports as active (default test session body).
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": did,
            "handle": "anothermigration.bsky.social",
            "active": true,
        })))
        .mount(&destination)
        .await;

    // Destination reports one missing blob.
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "blobs": [{
                "cid": blob_cid,
                "recordUri": record_uri,
            }],
        })))
        .mount(&destination)
        .await;

    // Origin accepts activate and deactivate, and streams the blob bytes.
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.activateAccount"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&origin)
        .await;
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.deactivateAccount"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&origin)
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

    let jobs = JobManager::new();
    let export_id = jobs
        .spawn_export_blobs(ExportBlobsRequest {
            destination: destination.uri(),
            origin: origin.uri(),
            did: did.clone(),
            origin_token: "origin-jwt".to_string(),
            destination_token: "destination-jwt".to_string(),
            is_missing_blob_request: true,
        })
        .await
        .expect("spawn_export_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, export_id).await,
        JobStatus::Success,
        "export job should finish successfully even when origin is deactivated"
    );

    let received = origin
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let activate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .count();
    let deactivate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.deactivateAccount")
        .count();
    let blob_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.sync.getBlob")
        .count();
    assert_eq!(
        activate_calls, 1,
        "origin should be reactivated exactly once"
    );
    assert_eq!(
        deactivate_calls, 1,
        "origin should be re-deactivated exactly once after the download"
    );
    assert_eq!(
        blob_calls, 1,
        "getBlob should have been called for the missing blob"
    );

    // Blob bytes should have made it to disk.
    let on_disk = std::fs::read(blob_dir.join(blob_cid))
        .expect("export job should have written the blob file");
    assert_eq!(on_disk, payload);

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_job_leaves_active_origin_untouched() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;

    let did = unique_did("webjobactiveorigin");
    let payload: &[u8] = b"fake-blob-bytes-active-origin";
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    let session_body = json!({
        "did": did,
        "handle": "anothermigration.bsky.social",
        "active": true,
    });
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
            "blobs": [{
                "cid": blob_cid,
                "recordUri": record_uri,
            }],
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

    let jobs = JobManager::new();
    let export_id = jobs
        .spawn_export_blobs(ExportBlobsRequest {
            destination: destination.uri(),
            origin: origin.uri(),
            did: did.clone(),
            origin_token: "origin-jwt".to_string(),
            destination_token: "destination-jwt".to_string(),
            is_missing_blob_request: true,
        })
        .await
        .expect("spawn_export_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, export_id).await,
        JobStatus::Success,
        "export job should finish successfully for an active origin"
    );

    let received = origin
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let activate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .count();
    let deactivate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.deactivateAccount")
        .count();
    assert_eq!(
        activate_calls, 0,
        "activateAccount must not be called when origin is already active"
    );
    assert_eq!(
        deactivate_calls, 0,
        "deactivateAccount must not be called when origin is already active"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_job_skips_activation_check_on_regular_flow() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;

    let did = unique_did("webjobregularflow");
    let payload: &[u8] = b"fake-blob-bytes-regular-flow";
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    let session_body = json!({
        "did": did,
        "handle": "anothermigration.bsky.social",
        "active": true,
    });
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
            "blobs": [{
                "cid": blob_cid,
                "recordUri": record_uri,
            }],
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

    let jobs = JobManager::new();
    let export_id = jobs
        .spawn_export_blobs(ExportBlobsRequest {
            destination: destination.uri(),
            origin: origin.uri(),
            did: did.clone(),
            origin_token: "origin-jwt".to_string(),
            destination_token: "destination-jwt".to_string(),
            is_missing_blob_request: false,
        })
        .await
        .expect("spawn_export_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, export_id).await,
        JobStatus::Success,
        "regular-flow export job should finish successfully"
    );

    let received = origin
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let activate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .count();
    let deactivate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.deactivateAccount")
        .count();
    assert_eq!(activate_calls, 0, "regular flow must not reactivate origin");
    assert_eq!(
        deactivate_calls, 0,
        "regular flow must not re-deactivate origin"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}
