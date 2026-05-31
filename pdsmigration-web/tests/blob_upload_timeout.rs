use pdsmigration_common::{did_blobs_path, UploadBlobsRequest};
use pdsmigration_web::background_jobs::{JobManager, JobStatus};
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{await_job, session_body, unique_did};

#[tokio::test]
async fn upload_job_retries_after_request_timeout() {
    unsafe {
        std::env::set_var("BLOB_REQUEST_TIMEOUT_SECS", "1");
    }

    let destination = MockServer::start().await;
    let did = unique_did("timeout_retry");

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&destination)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_delay(Duration::from_secs(3)),
        )
        .up_to_n_times(1)
        .mount(&destination)
        .await;
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(ResponseTemplate::new(200).insert_header("ratelimit-remaining", "1000"))
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create blob dir");
    std::fs::write(blob_dir.join("blob-slow"), b"slow-then-fast").expect("seed blob");

    let jobs = JobManager::new();
    let upload_id = jobs
        .spawn_upload_blobs(
            UploadBlobsRequest {
                pds_host: destination.uri(),
                did: did.clone(),
                token: "destination-jwt".to_string(),
            },
            1,
            3,
        )
        .await
        .expect("spawn_upload_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, upload_id).await,
        JobStatus::Success,
        "a request timeout should produce a retryable Upstream error, \
         and the retry should succeed"
    );

    let record = jobs.get(upload_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(progress.successful_blobs, 1);
    assert_eq!(progress.invalid_blobs, 0);

    let received = destination.received_requests().await.expect("requests");
    let uploads = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .count();
    assert_eq!(
        uploads, 2,
        "expected one timed-out attempt plus one successful retry"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}
