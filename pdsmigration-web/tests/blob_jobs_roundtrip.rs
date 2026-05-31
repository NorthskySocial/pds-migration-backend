use pdsmigration_common::{did_blobs_path, ExportBlobsRequest, UploadBlobsRequest};
use pdsmigration_web::background_jobs::{JobManager, JobStatus};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{await_job, unique_did};

#[tokio::test]
async fn job_manager_blob_roundtrip() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;

    let did = unique_did("webjobroundtrip");
    let payload: &[u8] = b"fake-blob-bytes-for-job-manager-roundtrip";
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

    // Origin streams the blob bytes.
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&origin)
        .await;

    // Destination accepts uploads.
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(ResponseTemplate::new(200).insert_header("ratelimit-remaining", "1000"))
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);

    let jobs = JobManager::new();

    // Run the export job through the JobManager.
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
        "export job should finish successfully"
    );

    // Did the job write the blob to the path the upload job will read?
    let blob_file = blob_dir.join(blob_cid);
    let on_disk = std::fs::read(&blob_file).expect("export job should have written the blob file");
    assert_eq!(
        on_disk, payload,
        "bytes on disk should match what the origin PDS streamed"
    );

    // Run the upload job through the JobManager.
    let upload_id = jobs
        .spawn_upload_blobs(
            UploadBlobsRequest {
                pds_host: destination.uri(),
                did: did.clone(),
                token: "destination-jwt".to_string(),
            },
            1,
            4,
        )
        .await
        .expect("spawn_upload_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, upload_id).await,
        JobStatus::Success,
        "upload job should finish successfully"
    );

    // Did the destination PDS receive exactly the bytes the export job wrote?
    let received = destination
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let upload_req = received
        .iter()
        .find(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .expect("uploadBlob should have been called by the job runner");
    assert_eq!(
        upload_req.body.as_slice(),
        payload,
        "uploadBlob should receive exactly the bytes the export job wrote"
    );

    // Did the upload job record the successful blob in its progress?
    let final_record = jobs.get(upload_id).await.expect("upload job record");
    let progress = final_record
        .progress
        .as_ref()
        .expect("upload job should track progress");
    assert_eq!(progress.successful_blobs, 1);
    assert_eq!(progress.invalid_blobs, 0);
    assert_eq!(progress.total, Some(1));

    let _ = std::fs::remove_dir_all(&blob_dir);
}
