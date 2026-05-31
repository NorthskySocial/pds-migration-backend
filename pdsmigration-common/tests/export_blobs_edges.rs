use pdsmigration_common::{
    did_blobs_path, export_all_blobs_api, export_blobs_api, ExportAllBlobsRequest,
    ExportBlobsRequest, MigrationError,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use pdsmigration_common::unique_did;

const CID_ALPHA: &str = "bafyreieo2p3k22c3swpk24bckghbv53m3alpr2hmptg5uhwuaghi6ird7a";
const CID_BETA: &str = "bafyreihujzsooxzzjdu7op4n7kkhehcm5df3j4tfyr4qy4blfva47pzhkm";

fn session_body(did: &str) -> serde_json::Value {
    json!({
        "did": did,
        "handle": "anothermigration.bsky.social",
        "active": true
    })
}

async fn mount_session(server: &MockServer, did: &str) {
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(did)))
        .mount(server)
        .await;
}

#[tokio::test]
async fn export_blobs_downloads_all_missing_blobs() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("expmulti");
    let payload: &[u8] = b"multi-blob-payload";

    mount_session(&origin, &did).await;
    mount_session(&destination, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "blobs": [
                { "cid": CID_ALPHA, "recordUri": format!("at://{did}/app.bsky.feed.post/aaa") },
                { "cid": CID_BETA, "recordUri": format!("at://{did}/app.bsky.feed.post/bbb") },
            ],
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

    let response = export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: false,
    })
    .await
    .expect("export should succeed when all blobs download");

    assert_eq!(response.successful_blobs.len(), 2, "both blobs should succeed");
    assert!(response.invalid_blobs.is_empty(), "no invalid blobs expected");
    for cid in [CID_ALPHA, CID_BETA] {
        let on_disk = std::fs::read(blob_dir.join(cid)).expect("blob file should exist");
        assert_eq!(on_disk, payload, "on-disk bytes should match streamed body");
    }

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_empty_missing_list_is_ok() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("expempty");

    mount_session(&origin, &did).await;
    mount_session(&destination, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "blobs": [] })))
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    let response = export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: false,
    })
    .await
    .expect("empty missing list should not error");

    assert!(response.successful_blobs.is_empty());
    assert!(response.invalid_blobs.is_empty());

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_missing_request_cleans_existing_directory() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("expclean");

    mount_session(&origin, &did).await;
    mount_session(&destination, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "blobs": [] })))
        .mount(&destination)
        .await;

    // Seed a stale artifact from a previous run.
    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create seed dir");
    let stale = blob_dir.join("stale-blob");
    std::fs::write(&stale, b"old").expect("write stale file");

    export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: true,
    })
    .await
    .expect("clean run should succeed");

    assert!(
        !stale.exists(),
        "is_missing_blob_request=true must wipe the prior directory contents"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_non_missing_request_preserves_existing_files() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("exppreserve");

    mount_session(&origin, &did).await;
    mount_session(&destination, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "blobs": [] })))
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create seed dir");
    let keep = blob_dir.join("already-downloaded");
    std::fs::write(&keep, b"keep me").expect("write file");

    export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: false,
    })
    .await
    .expect("run should succeed");

    assert!(
        keep.exists(),
        "is_missing_blob_request=false must not delete previously downloaded blobs"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_blobs_aborts_on_hard_download_error() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("expabort");

    mount_session(&origin, &did).await;
    mount_session(&destination, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.repo.listMissingBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "blobs": [
                { "cid": CID_ALPHA, "recordUri": format!("at://{did}/app.bsky.feed.post/aaa") },
            ],
        })))
        .mount(&destination)
        .await;

    // A non-rate-limit download failure should abort the whole export.
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(ResponseTemplate::new(400).set_body_string("nope"))
        .mount(&origin)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    let err = export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: false,
    })
    .await
    .expect_err("a hard download error must abort the export");
    assert!(
        matches!(err, MigrationError::Runtime { .. }),
        "expected Runtime abort, got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn export_all_blobs_records_per_blob_failures_without_aborting() {
    let origin = MockServer::start().await;
    let did = unique_did("expall");
    let payload: &[u8] = b"all-blobs-payload";

    mount_session(&origin, &did).await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.listBlobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "cids": [CID_ALPHA, CID_BETA],
        })))
        .mount(&origin)
        .await;

    // First blob downloads fine...
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .and(query_param("cid", CID_ALPHA))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&origin)
        .await;

    // ...the second fails with a hard error and must be recorded, not fatal.
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .and(query_param("cid", CID_BETA))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad blob"))
        .mount(&origin)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    let response = export_all_blobs_api(ExportAllBlobsRequest {
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
    })
    .await
    .expect("export_all_blobs should not abort on a single bad blob");

    assert_eq!(
        response.successful_blobs.len(),
        1,
        "one blob should download successfully"
    );
    assert_eq!(
        response.failed_blobs.len(),
        1,
        "the bad blob should be recorded as failed"
    );
    let good = std::fs::read(blob_dir.join(CID_ALPHA)).expect("good blob written");
    assert_eq!(good, payload);
    assert!(
        !blob_dir.join(CID_BETA).exists(),
        "failed blob should not leave a partial file"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}
