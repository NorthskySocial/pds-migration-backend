use pdsmigration_common::{
    did_blobs_path, export_blobs_api, upload_blobs_api, ExportBlobsRequest, UploadBlobsRequest,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use pdsmigration_common::unique_did;

#[tokio::test]
async fn export_upload_blob_roundtrip() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;

    let did = unique_did("blobroundtrip");
    let payload: &[u8] = b"fake-blob-bytes-for-wiremock-roundtrip";
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    let session_body = json!({
        "did": did,
        "handle": "anothermigration.bsky.social",
        "active": true
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

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "blob": {
                "$type": "blob",
                "ref": { "$link": blob_cid },
                "mimeType": "application/octet-stream",
                "size": payload.len(),
            }
        })))
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    // Find and download missing blobs (one)
    export_blobs_api(ExportBlobsRequest {
        destination: destination.uri(),
        origin: origin.uri(),
        did: did.clone(),
        origin_token: "origin-jwt".to_string(),
        destination_token: "destination-jwt".to_string(),
        is_missing_blob_request: false,
    })
    .await
    .expect("export_blobs_api should succeed against mocked PDSes");

    // Did the blob land where we expect it to (where upload will look)?
    let blob_file = blob_dir.join(blob_cid);
    let on_disk = std::fs::read(&blob_file).expect("export should have written the blob file");
    assert_eq!(
        on_disk, payload,
        "bytes on disk should match what the origin PDS streamed"
    );

    // Upload the blob to the destination PDS
    upload_blobs_api(UploadBlobsRequest {
        pds_host: destination.uri(),
        did: did.clone(),
        token: "destination-jwt".to_string(),
    })
    .await
    .expect("upload_blobs_api should succeed against mocked PDS");

    // Did we receive the expected bytes in the upload API call?
    let received = destination
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let upload_req = received
        .iter()
        .find(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .expect("uploadBlob should have been called");
    assert_eq!(
        upload_req.body.as_slice(),
        payload,
        "uploadBlob should receive exactly the bytes export wrote"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&blob_dir);
}
