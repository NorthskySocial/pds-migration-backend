use pdsmigration_common::{did_blobs_path, ExportBlobsRequest, UploadBlobsRequest};
use pdsmigration_web::background_jobs::{JobManager, JobStatus};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{await_job, session_body, unique_did};

#[tokio::test]
async fn export_job_records_failed_blob_but_still_succeeds() {
    let origin = MockServer::start().await;
    let destination = MockServer::start().await;
    let did = unique_did("export");
    let blob_cid = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
    let record_uri = format!("at://{did}/app.bsky.feed.post/abc123");

    for server in [&origin, &destination] {
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
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

    // Origin rejects the blob
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_string("bad blob"),
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
        "a single failed blob must not fail the whole export job"
    );

    let record = jobs.get(export_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(progress.successful_blobs, 0, "no blob should succeed");
    assert_eq!(progress.invalid_blobs, 1, "the bad blob should be recorded");
    assert_eq!(progress.total, Some(1));

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn upload_job_retries_once_and_succeeds() {
    let destination = MockServer::start().await;
    let did = unique_did("retry");

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&destination)
        .await;

    // First upload attempt fails (non-rate-limit), then any subsequent
    // attempt succeeds
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(
            ResponseTemplate::new(500)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_string("transient"),
        )
        .up_to_n_times(1)
        .mount(&destination)
        .await;
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(ResponseTemplate::new(200).insert_header("ratelimit-remaining", "1000"))
        .mount(&destination)
        .await;

    // Seed a blob on disk for the upload job to read.
    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create blob dir");
    std::fs::write(blob_dir.join("blob-one"), b"retry-me").expect("seed blob");

    let jobs = JobManager::new();
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
        "upload job should finish once the retry succeeds"
    );

    let record = jobs.get(upload_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(
        progress.successful_blobs, 1,
        "the retried blob should be counted as a success"
    );
    assert_eq!(progress.invalid_blobs, 0);

    // The destination should have been hit twice: the failure and the retry.
    let received = destination.received_requests().await.expect("requests");
    let uploads = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .count();
    assert_eq!(uploads, 2, "expected one failed attempt plus one retry");

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn upload_job_records_invalid_when_both_attempts_fail() {
    let destination = MockServer::start().await;
    let did = unique_did("bothfail");

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&destination)
        .await;

    // Every upload attempt fails
    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_string("permanently bad"),
        )
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create blob dir");
    std::fs::write(blob_dir.join("blob-bad"), b"no good").expect("seed blob");

    let jobs = JobManager::new();
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
        "a blob that fails both attempts should still let the job complete"
    );

    let record = jobs.get(upload_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(progress.successful_blobs, 0);
    assert_eq!(
        progress.invalid_blobs, 1,
        "the permanently bad blob should be recorded as invalid"
    );

    // Both the initial attempt and the retry should have hit the PDS.
    let received = destination.received_requests().await.expect("requests");
    let uploads = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .count();
    assert_eq!(uploads, 2, "expected one attempt plus one retry");

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn upload_job_first_pass_exhausts_retries_then_second_pass_succeeds() {
    let destination = MockServer::start().await;
    let did = unique_did("exhaust_then_success");

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&destination)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(
            ResponseTemplate::new(500)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_string("transient"),
        )
        .up_to_n_times(2)
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
    std::fs::write(blob_dir.join("blob-eventual"), b"second-pass-recovers").expect("seed blob");

    let jobs = JobManager::new();
    let upload_id = jobs
        .spawn_upload_blobs(
            UploadBlobsRequest {
                pds_host: destination.uri(),
                did: did.clone(),
                token: "destination-jwt".to_string(),
            },
            1,
            2,
        )
        .await
        .expect("spawn_upload_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, upload_id).await,
        JobStatus::Success,
        "second pass should be able to rescue a blob that exhausted first-pass retries"
    );

    let record = jobs.get(upload_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(
        progress.successful_blobs, 1,
        "the rescued blob should be counted as a success"
    );
    assert_eq!(progress.invalid_blobs, 0);

    let received = destination.received_requests().await.expect("requests");
    let uploads = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .count();
    assert_eq!(
        uploads, 3,
        "expected 2 first-pass attempts plus 1 second-pass attempt"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}

#[tokio::test]
async fn upload_job_marks_invalid_when_first_pass_retries_and_second_pass_all_fail() {
    let destination = MockServer::start().await;
    let did = unique_did("exhaust_all");

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&destination)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.uploadBlob"))
        .respond_with(
            ResponseTemplate::new(500)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_string("always broken"),
        )
        .mount(&destination)
        .await;

    let blob_dir = did_blobs_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_dir_all(&blob_dir);
    std::fs::create_dir_all(&blob_dir).expect("create blob dir");
    std::fs::write(blob_dir.join("blob-doomed"), b"never works").expect("seed blob");

    let jobs = JobManager::new();
    let upload_id = jobs
        .spawn_upload_blobs(
            UploadBlobsRequest {
                pds_host: destination.uri(),
                did: did.clone(),
                token: "destination-jwt".to_string(),
            },
            1,
            2,
        )
        .await
        .expect("spawn_upload_blobs should accept the request");

    assert_eq!(
        await_job(&jobs, upload_id).await,
        JobStatus::Success,
        "the job itself should still finish even when every attempt fails"
    );

    let record = jobs.get(upload_id).await.expect("job record");
    let progress = record.progress.as_ref().expect("progress tracked");
    assert_eq!(progress.successful_blobs, 0);
    assert_eq!(progress.invalid_blobs, 1);

    let received = destination.received_requests().await.expect("requests");
    let uploads = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.repo.uploadBlob")
        .count();
    assert_eq!(
        uploads, 3,
        "expected 2 first-pass attempts plus 1 second-pass attempt"
    );

    let _ = std::fs::remove_dir_all(&blob_dir);
}
