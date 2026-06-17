use crate::errors::ApiError;
use bsky_sdk::api::agent::Configure;
use derive_more::Display;
use futures_util::StreamExt;
use pdsmigration_common::{
    activate_account_agent, build_agent, deactivate_account, did_blobs_path, did_to_car_filename,
    download_blob, format_cid, login_helper, missing_blobs, repo_car_path, upload_blob_v2,
    wait_for_rate_limit, ExportBlobsRequest, ExportPDSRequest, GetBlobRequest, MigrationError,
    UploadBlobsRequest,
};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)] // Used in schema attribute macros
use serde_json::json;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use utoipa::ToSchema;
use uuid::Uuid;

const MAX_BACKOFF_MS: u64 = 10_000;
const BASE_BACKOFF_MS: u64 = 250;
const BACKOFF_JITTER_MS: u64 = 250;

fn backoff_base_ms(attempt: u32) -> u64 {
    let shift = attempt.min(6);
    BASE_BACKOFF_MS
        .saturating_mul(1u64 << shift)
        .min(MAX_BACKOFF_MS)
}

fn backoff_jitter_ms(max: u64) -> u64 {
    if max == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % max
}

fn backoff_delay(attempt: u32) -> Duration {
    Duration::from_millis(backoff_base_ms(attempt) + backoff_jitter_ms(BACKOFF_JITTER_MS))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, Display, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    ExportBlobs,
    UploadBlobs,
    ExportRepo,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Success,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct JobProgress {
    #[schema(example = 1)]
    pub successful_blobs: u64,
    #[schema(example = 1)]
    pub invalid_blobs: u64,
    #[schema(example = 2)]
    pub total: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobRecord {
    #[schema(example = "550e8400-e29b-41d4-a716-446655440000")]
    pub id: String,
    #[schema(example = "ExportBlobs")]
    pub kind: JobKind,
    #[schema(example = "Queued")]
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[schema(value_type = u64, example = 1700000000)]
    pub created_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = u64, example = 1700000001)]
    pub started_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = u64, example = 1700000100)]
    pub finished_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = json!({
            "successful_blobs": 99,
            "invalid_blobs": 1,
            "total": 100
        }))]
    pub progress: Option<JobProgress>,
}

impl JobRecord {
    pub fn new(id: Uuid, kind: JobKind) -> Self {
        Self {
            id: id.to_string(),
            kind,
            status: JobStatus::Queued,
            error: None,
            created_at: now_millis(),
            started_at: None,
            finished_at: None,
            progress: Some(JobProgress::default()),
        }
    }
}

#[derive(Clone)]
pub struct JobManager {
    state: Arc<RwLock<JobState>>,
}

#[derive(Default, Debug)]
struct JobState {
    records: HashMap<Uuid, JobRecord>,
}

impl JobState {
    pub fn set_running(&mut self, id: Uuid) {
        if let Some(r) = self.records.get_mut(&id) {
            r.status = JobStatus::Running;
            r.started_at = Some(now_millis());
            tracing::info!(
                job_id = %r.id,
                kind = %r.kind,
                "Job status: Queued -> Running",
            );
        }
    }

    pub fn finalize(&mut self, id: Uuid, result: Result<(), MigrationError>) {
        if let Some(r) = self.records.get_mut(&id) {
            let elapsed_ms = r.started_at.map(|s| now_millis().saturating_sub(s));
            let progress = r.progress.clone().unwrap_or_default();
            match result {
                Ok(_) => {
                    r.status = JobStatus::Success;
                    r.finished_at = Some(now_millis());
                    tracing::info!(
                        job_id = %r.id,
                        kind = %r.kind,
                        elapsed_ms = elapsed_ms.unwrap_or(0),
                        successful_blobs = progress.successful_blobs,
                        invalid_blobs = progress.invalid_blobs,
                        total = progress.total.unwrap_or(0),
                        "Job finished: Running -> Success",
                    );
                }
                Err(e) => {
                    let msg = format!("{}", e);
                    r.status = JobStatus::Error;
                    r.error = Some(msg.clone());
                    r.finished_at = Some(now_millis());
                    tracing::error!(
                        job_id = %r.id,
                        kind = %r.kind,
                        elapsed_ms = elapsed_ms.unwrap_or(0),
                        successful_blobs = progress.successful_blobs,
                        invalid_blobs = progress.invalid_blobs,
                        total = progress.total.unwrap_or(0),
                        error = %msg,
                        error_debug = ?e,
                        "Job errored: Running -> Error",
                    );
                }
            }
        }
    }

    pub fn update_total(&mut self, id: Uuid, total: u64) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.total = Some(total);
            }
        }
    }

    pub fn record_success(&mut self, id: Uuid) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.successful_blobs += 1;
            }
        }
    }

    pub fn record_failure(&mut self, id: Uuid) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.invalid_blobs += 1;
            }
        }
    }
}

impl JobManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(JobState::default())),
        }
    }

    pub async fn get(&self, id: Uuid) -> Option<JobRecord> {
        let st = self.state.read().await;
        st.records.get(&id).cloned()
    }

    #[tracing::instrument(skip(self))]
    pub async fn spawn_upload_blobs(
        &self,
        request: UploadBlobsRequest,
        concurrent_tasks: usize,
        max_attempts: u32,
    ) -> Result<Uuid, ApiError> {
        let id = Uuid::new_v4();
        let did = request.did.clone();
        let pds_host = request.pds_host.clone();
        tracing::info!(
            "[{}] Spawning upload_blobs job {} for {} (concurrency={}, max_attempts={})",
            did,
            id,
            pds_host,
            concurrent_tasks,
            max_attempts
        );
        let rec = JobRecord::new(id, JobKind::UploadBlobs);

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            {
                let mut st = state.write().await;
                st.set_running(id);
            }

            let result =
                upload_blobs_api_job(id, state.clone(), request, concurrent_tasks, max_attempts)
                    .await;
            {
                let mut st = state.write().await;
                st.finalize(id, result);
            }
        });

        Ok(id)
    }

    #[tracing::instrument(skip(self))]
    pub async fn spawn_export_blobs(&self, request: ExportBlobsRequest) -> Result<Uuid, ApiError> {
        let id = Uuid::new_v4();
        let did = request.did.clone();
        let origin = request.origin.clone();
        tracing::info!("[{}] Spawning export_blobs job {} from {}", did, id, origin);
        let rec = JobRecord::new(id, JobKind::ExportBlobs);

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            {
                let mut st = state.write().await;
                st.set_running(id);
            }
            let result = export_blobs_api_job(id, state.clone(), request).await;
            {
                let mut st = state.write().await;
                st.finalize(id, result);
            }
        });

        Ok(id)
    }

    #[tracing::instrument(skip(self))]
    pub async fn spawn_export_repo(
        &self,
        request: ExportPDSRequest,
        s3_endpoint: String,
    ) -> Result<Uuid, ApiError> {
        let id = Uuid::new_v4();
        let did = request.did.clone();
        let pds_host = request.pds_host.clone();
        tracing::info!("[{}] Spawning export_repo job {} for {}", did, id, pds_host);
        let rec = JobRecord::new(id, JobKind::ExportRepo);

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
            st.update_total(id, 1);
        }

        let state = self.state.clone();
        tokio::spawn(async move {
            {
                let mut st = state.write().await;
                st.set_running(id);
            }
            let result = export_repo_api_job(id, state.clone(), request, s3_endpoint).await;
            {
                let mut st = state.write().await;
                st.finalize(id, result);
            }
        });

        Ok(id)
    }
}

impl Default for JobManager {
    fn default() -> Self {
        Self::new()
    }
}

#[tracing::instrument(skip(state))]
async fn export_blobs_api_job(
    id: Uuid,
    state: Arc<RwLock<JobState>>,
    req: ExportBlobsRequest,
) -> Result<(), MigrationError> {
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.destination.as_str(),
        req.did.as_str(),
        req.destination_token.as_str(),
    )
    .await?;
    let missing_blobs = missing_blobs(&agent).await?;
    {
        let mut st = state.write().await;
        st.update_total(id, missing_blobs.len() as u64);
    }
    let session = login_helper(
        &agent,
        req.origin.as_str(),
        req.did.as_str(),
        req.origin_token.as_str(),
    )
    .await?;

    let did_blobs_path = did_blobs_path(&session.did)?;
    let did = session.did.as_str();

    // on missing blob requests, the origin PDS may reject `sync` operations
    // if the account is deactivated (expected after a successful migration),
    // so we temporarily reactivate it to fetch missing blobs
    let origin_was_deactivated = if req.is_missing_blob_request {
        match agent.api.com.atproto.server.get_session().await {
            Ok(output) => output.active == Some(false),
            Err(error) => {
                tracing::warn!(
                    "[{}] Could not query origin session to check activation state: {}",
                    did,
                    error
                );
                false
            }
        }
    } else {
        false
    };
    if origin_was_deactivated {
        tracing::info!(
            "[{}] Origin reports deactivated; reactivating temporarily to download missing blobs",
            did
        );
        activate_account_agent(&agent).await?;
    }

    if req.is_missing_blob_request {
        if let Err(e) = tokio::fs::remove_dir_all(did_blobs_path.as_path()).await {
            if e.kind() != ErrorKind::NotFound {
                return Err(MigrationError::Runtime {
                    message: format!("Failed to clean directory: {}", e),
                });
            }
        }
        tracing::info!("[{}] Cleaned directory for missing blob request", did);
    }
    match tokio::fs::create_dir(did_blobs_path.as_path()).await {
        Ok(_) => {
            tracing::info!("[{}] Successfully created directory", did);
        }
        Err(e) => {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(MigrationError::Runtime {
                    message: format!("{}", e),
                });
            }
        }
    }
    for missing_blob in &missing_blobs {
        tracing::debug!("[{}] Missing blob: {:?}", did, missing_blob);
        let session = match agent.get_session().await {
            Some(session) => session,
            None => {
                return Err(MigrationError::Runtime {
                    message: "Failed to get session".to_string(),
                });
            }
        };
        let mut filepath = did_blobs_path.clone();
        filepath.push(
            missing_blob
                .record_uri
                .as_str()
                .split("/")
                .last()
                .unwrap_or("fallback"),
        );
        if !tokio::fs::try_exists(filepath).await.unwrap() {
            let blob_cid_str = format_cid(&missing_blob.cid);
            let get_blob_request = GetBlobRequest {
                did: session.did.clone(),
                cid: blob_cid_str.clone(),
                token: session.access_jwt.clone(),
            };
            match download_blob(agent.get_endpoint().await.as_str(), &get_blob_request).await {
                Ok(mut stream) => {
                    tracing::info!("[{}] Successfully fetched missing blob", did);
                    let mut blob_path = did_blobs_path.clone();
                    blob_path.push(&blob_cid_str);
                    let mut file = tokio::fs::File::create(blob_path.as_path()).await.unwrap();

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.unwrap();
                        file.write_all(&chunk).await.unwrap();
                    }

                    file.flush().await.unwrap();

                    {
                        let mut st = state.write().await;
                        st.record_success(id);
                    }
                }
                Err(e) => {
                    if matches!(e, MigrationError::RateLimitReached) {
                        wait_for_rate_limit(did, &JobKind::ExportBlobs.to_string()).await;
                    }
                    tracing::error!(
                        did = %did,
                        kind = %JobKind::ExportBlobs,
                        cid = %blob_cid_str,
                        step = "download_blob",
                        error = %e,
                        error_debug = ?e,
                        "Failed to process blob",
                    );
                    {
                        let mut st = state.write().await;
                        st.record_failure(id);
                    }
                }
            }
        }
    }
    if origin_was_deactivated {
        tracing::info!(
            "[{}] Restoring origin to deactivated state after missing-blob download",
            did
        );
        if let Err(error) = deactivate_account(&agent).await {
            tracing::warn!(
                "[{}] Failed to re-deactivate origin after missing-blob download: {}",
                did,
                error
            );
        }
    }
    Ok(())
}

#[tracing::instrument(skip(state))]
async fn upload_blobs_api_job(
    id: Uuid,
    state: Arc<RwLock<JobState>>,
    req: UploadBlobsRequest,
    concurrent_tasks: usize,
    max_attempts: u32,
) -> Result<(), MigrationError> {
    let agent = build_agent().await?;
    agent.configure_endpoint(req.pds_host.clone());
    let session = login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    let did = session.did.as_str();

    let mut blob_dir;
    let path = did_blobs_path(&session.did)?;
    match tokio::fs::read_dir(path.as_path()).await {
        Ok(output) => blob_dir = output,
        Err(error) => {
            tracing::error!(
                did = %did,
                path = %path.display(),
                error = %error,
                "Failed to read blob directory",
            );
            return Err(MigrationError::Runtime {
                message: "Failed to read blob directory".to_string(),
            });
        }
    }

    let mut blobs_in_dir = Vec::new();
    while let Ok(Some(entry)) = blob_dir.next_entry().await {
        blobs_in_dir.push(entry);
    }

    {
        let mut st = state.write().await;
        st.update_total(id, blobs_in_dir.len() as u64);
    }

    // process blobs in parallel and record results in a stream
    let did_owned = did.to_string();
    let mut first_pass_stream = futures_util::stream::iter(blobs_in_dir.into_iter())
        .map(|blob| {
            let agent = agent.clone();
            let did_inner = did_owned.clone();
            async move {
                let path = blob.path();
                let blob_cid_str = blob.file_name().to_string_lossy().to_string();
                let file = match tokio::fs::read(&path).await {
                    Ok(data) => data,
                    Err(error) => {
                        tracing::error!("[{}] {}", did_inner, error.to_string());
                        return (
                            path,
                            blob_cid_str,
                            Err(MigrationError::Runtime {
                                message: "Failed to read blob file".to_string(),
                            }),
                        );
                    }
                };
                tracing::info!(
                    "[{}] Uploading blob: {:?} with size {}...",
                    did_inner,
                    blob_cid_str,
                    file.len()
                );
                let result =
                    upload_blob_with_retries(&agent, file, &blob_cid_str, &did_inner, max_attempts)
                        .await;
                (path, blob_cid_str, result)
            }
        })
        .buffer_unordered(concurrent_tasks);

    // collect results of the first pass as soon as they arrive,
    // and track failures for a second-pass restry
    let mut failed: Vec<(std::path::PathBuf, String)> = Vec::new();
    while let Some((path, blob_cid_str, result)) = first_pass_stream.next().await {
        match result {
            Ok(()) => {
                let mut st = state.write().await;
                st.record_success(id);
            }
            Err(e) => {
                tracing::warn!(
                    "[{}][{}] Blob {} failed first pass: {}",
                    did,
                    JobKind::UploadBlobs,
                    blob_cid_str,
                    e
                );
                failed.push((path, blob_cid_str));
            }
        }
    }

    // second pass: retry the still-failed blobs sequentially one more time
    if !failed.is_empty() {
        tracing::info!(
            "[{}][{}] Re-attempting {} failed blob(s) sequentially",
            did,
            JobKind::UploadBlobs,
            failed.len()
        );
        for (path, blob_cid_str) in failed {
            let file = match tokio::fs::read(&path).await {
                Ok(data) => data,
                Err(error) => {
                    tracing::error!("[{}] {}", did, error.to_string());
                    let mut st = state.write().await;
                    st.record_failure(id);
                    continue;
                }
            };
            match upload_blob_v2(&agent, file, &blob_cid_str).await {
                Ok(()) => {
                    tracing::info!(
                        "[{}][{}] Second pass succeeded for blob {}",
                        did,
                        JobKind::UploadBlobs,
                        blob_cid_str
                    );
                    let mut st = state.write().await;
                    st.record_success(id);
                }
                Err(e) => {
                    tracing::error!(
                        "[{}][{}] Second pass failed for blob {}: {}",
                        did,
                        JobKind::UploadBlobs,
                        blob_cid_str,
                        e
                    );
                    let mut st = state.write().await;
                    st.record_failure(id);
                }
            }
        }
    }

    tracing::info!("[{}] Finished uploading blobs", did);
    Ok(())
}

#[tracing::instrument(skip(state, req))]
async fn export_repo_api_job(
    id: Uuid,
    state: Arc<RwLock<JobState>>,
    req: ExportPDSRequest,
    s3_endpoint: String,
) -> Result<(), MigrationError> {
    match export_repo_to_s3(req, &s3_endpoint).await {
        Ok(()) => {
            let mut st = state.write().await;
            st.record_success(id);
            Ok(())
        }
        Err(error) => {
            let mut st = state.write().await;
            st.record_failure(id);
            Err(error)
        }
    }
}

#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
pub async fn export_repo_to_s3(
    req: ExportPDSRequest,
    endpoint_url: &str,
) -> Result<(), MigrationError> {
    let did = req.did.clone();
    tracing::info!("[{}] Export repository request received", did);
    let download_start = Instant::now();
    pdsmigration_common::export_pds_api(req).await?;
    tracing::info!(
        "[{}] Repository download phase finished in {:.1}s, starting S3 upload",
        did,
        download_start.elapsed().as_secs_f64()
    );

    tracing::debug!(
        "[{}] Loading AWS config with endpoint: {}",
        did,
        endpoint_url
    );
    let config = aws_config::from_env()
        .region("auto")
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    let bucket_name = "migration".to_string();
    let file_name = did_to_car_filename(&did);
    let key = format!("migration/{file_name}");
    let file_path = repo_car_path(&did).map_err(|e| MigrationError::Runtime {
        message: e.to_string(),
    })?;

    tracing::debug!(
        "[{}] Uploading file {} to S3 bucket {} with key {}",
        did,
        file_path.display(),
        bucket_name,
        key
    );

    match tokio::fs::metadata(&file_path).await {
        Ok(meta) => tracing::info!(
            "[{}] Exported repository file size: {} bytes",
            did,
            meta.len()
        ),
        Err(e) => tracing::warn!(
            "[{}] Failed to read exported repository file metadata: {:?}",
            did,
            e
        ),
    }

    let upload_start = Instant::now();
    let body = aws_sdk_s3::primitives::ByteStream::from_path(&file_path)
        .await
        .map_err(|e| MigrationError::Runtime {
            message: e.to_string(),
        })?;

    client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await
        .map_err(|e| MigrationError::Runtime {
            message: e.to_string(),
        })?;

    tracing::info!(
        "[{}] Repository exported and uploaded to S3 successfully (upload phase {:.1}s)",
        did,
        upload_start.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Upload a single blob, retrying transient failures with exponential backoff.
async fn upload_blob_with_retries(
    agent: &bsky_sdk::BskyAgent,
    file: Vec<u8>,
    blob_cid: &str,
    did: &str,
    max_attempts: u32,
) -> Result<(), MigrationError> {
    let mut attempt: u32 = 1;
    let mut rate_limit_waits: u32 = 0;
    loop {
        match upload_blob_v2(agent, file.clone(), blob_cid).await {
            Ok(()) => return Ok(()),
            Err(MigrationError::RateLimitReached) => {
                if rate_limit_waits >= max_attempts.max(1) {
                    tracing::error!(
                        "[{}][{}] Rate limit retries exhausted for blob {}",
                        did,
                        JobKind::UploadBlobs,
                        blob_cid
                    );
                    return Err(MigrationError::RateLimitReached);
                }
                rate_limit_waits += 1;
                wait_for_rate_limit(did, &JobKind::UploadBlobs.to_string()).await;
            }
            Err(e) => {
                if !matches!(e, MigrationError::Upstream { .. }) || attempt >= max_attempts {
                    return Err(e);
                }
                let delay = backoff_delay(attempt);
                tracing::warn!(
                    "[{}][{}] Upload attempt {} failed for blob {} (error: {}); retrying in {:?}",
                    did,
                    JobKind::UploadBlobs,
                    attempt,
                    blob_cid,
                    e,
                    delay
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pdsmigration_common::unique_did;
    use serde_json::json;
    use std::env;
    use std::sync::LazyLock;
    use tokio::sync::{Mutex, MutexGuard};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    struct EnvGuard {
        _guard: MutexGuard<'static, ()>,
        previous: Vec<(String, Option<String>)>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.previous {
                match v {
                    Some(value) => env::set_var(k, value),
                    None => env::remove_var(k),
                }
            }
        }
    }

    async fn with_aws_test_env() -> EnvGuard {
        let guard = ENV_LOCK.lock().await;
        let vars = [
            ("AWS_ACCESS_KEY_ID", Some("test-access-key")),
            ("AWS_SECRET_ACCESS_KEY", Some("test-secret-key")),
            ("AWS_EC2_METADATA_DISABLED", Some("true")),
        ];

        let previous = vars
            .iter()
            .map(|(k, _)| ((*k).to_string(), env::var(k).ok()))
            .collect::<Vec<_>>();

        for (k, v) in vars {
            match v {
                Some(value) => env::set_var(k, value),
                None => env::remove_var(k),
            }
        }

        EnvGuard {
            _guard: guard,
            previous,
        }
    }

    #[test]
    fn test_job_record_new_export_blobs() {
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);

        assert_eq!(record.id, id.to_string());
        assert!(matches!(record.kind, JobKind::ExportBlobs));
        assert_eq!(record.status, JobStatus::Queued);
        assert!(record.error.is_none());
        assert!(record.started_at.is_none());
        assert!(record.finished_at.is_none());
        assert!(record.progress.is_some());

        let progress = record.progress.unwrap();
        assert_eq!(progress.successful_blobs, 0);
        assert_eq!(progress.invalid_blobs, 0);
        assert!(progress.total.is_none());
    }

    #[test]
    fn test_job_record_new_upload_blobs() {
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::UploadBlobs);

        assert!(matches!(record.kind, JobKind::UploadBlobs));
    }

    #[test]
    fn test_job_record_new_export_repo() {
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportRepo);

        assert!(matches!(record.kind, JobKind::ExportRepo));
    }

    #[test]
    fn test_job_state_set_running() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.set_running(id);

        let record = state.records.get(&id).unwrap();
        assert_eq!(record.status, JobStatus::Running);
        assert!(record.started_at.is_some());
    }

    #[test]
    fn test_job_state_finalize_success() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.finalize(id, Ok(()));

        let record = state.records.get(&id).unwrap();
        assert_eq!(record.status, JobStatus::Success);
        assert!(record.finished_at.is_some());
        assert!(record.error.is_none());
    }

    #[test]
    fn test_job_state_finalize_error() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        let err = MigrationError::Runtime {
            message: "test error".to_string(),
        };
        state.finalize(id, Err(err));

        let record = state.records.get(&id).unwrap();
        assert_eq!(record.status, JobStatus::Error);
        assert!(record.finished_at.is_some());
        assert!(record.error.as_ref().unwrap().contains("test error"));
    }

    #[test]
    fn test_job_state_update_total() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.update_total(id, 42);

        let record = state.records.get(&id).unwrap();
        assert_eq!(record.progress.as_ref().unwrap().total, Some(42));
    }

    #[test]
    fn test_job_state_record_success() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.record_success(id);
        state.record_success(id);

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.successful_blobs, 2);
    }

    #[test]
    fn test_job_state_record_failure() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.record_failure(id);
        state.record_failure(id);

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.invalid_blobs, 2);
    }

    #[test]
    fn test_job_state_mixed_blob_results() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::UploadBlobs);
        state.records.insert(id, record);

        state.update_total(id, 5);
        state.record_success(id);
        state.record_success(id);
        state.record_success(id);
        state.record_failure(id);
        state.record_failure(id);

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.total, Some(5));
        assert_eq!(progress.successful_blobs, 3);
        assert_eq!(progress.invalid_blobs, 2);
    }

    #[test]
    fn test_job_state_set_running_unknown_id_is_noop() {
        let mut state = JobState::default();
        state.set_running(Uuid::new_v4());
        assert!(state.records.is_empty());
    }

    #[test]
    fn test_job_state_record_success_failure_unknown_id_is_noop() {
        let mut state = JobState::default();
        state.record_success(Uuid::new_v4());
        state.record_failure(Uuid::new_v4());
        state.update_total(Uuid::new_v4(), 1);
        assert!(state.records.is_empty());
    }

    #[actix_rt::test]
    async fn test_job_manager_get_returns_none_when_unknown() {
        let mgr = JobManager::new();
        assert!(mgr.get(Uuid::new_v4()).await.is_none());
    }

    #[actix_rt::test]
    async fn test_job_manager_get_after_manual_insert() {
        let mgr = JobManager::new();
        let id = Uuid::new_v4();
        {
            let mut st = mgr.state.write().await;
            st.records
                .insert(id, JobRecord::new(id, JobKind::UploadBlobs));
        }

        let got = mgr.get(id).await.unwrap();
        assert_eq!(got.id, id.to_string());
        assert!(matches!(got.kind, JobKind::UploadBlobs));
    }

    #[actix_rt::test]
    async fn test_export_repo_to_s3_success() {
        let _env_guard = with_aws_test_env().await;
        let pds = MockServer::start().await;
        let s3 = MockServer::start().await;
        let did = unique_did("jobexportreposuccess");
        let payload: &[u8] = b"export-repo-bytes";

        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "did": did,
                "handle": "anothermigration.bsky.social",
                "active": true
            })))
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

        let result = export_repo_to_s3(
            ExportPDSRequest {
                pds_host: pds.uri(),
                did: did.clone(),
                token: "origin-jwt".to_string(),
            },
            &s3.uri(),
        )
        .await;

        assert!(
            result.is_ok(),
            "expected export_repo_to_s3 success: {result:?}"
        );
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
    async fn test_export_repo_to_s3_returns_error_when_s3_upload_fails() {
        let _env_guard = with_aws_test_env().await;
        let pds = MockServer::start().await;
        let s3 = MockServer::start().await;
        let did = unique_did("jobexportrepofail");

        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.server.getSession"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "did": did,
                "handle": "anothermigration.bsky.social",
                "active": true
            })))
            .mount(&pds)
            .await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.sync.getRepo"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("ratelimit-remaining", "1000")
                    .set_body_bytes(b"repo-bytes"),
            )
            .mount(&pds)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&s3)
            .await;

        let result = export_repo_to_s3(
            ExportPDSRequest {
                pds_host: pds.uri(),
                did,
                token: "origin-jwt".to_string(),
            },
            &s3.uri(),
        )
        .await;

        assert!(result.is_err(), "expected export_repo_to_s3 failure");
    }

    #[test]
    fn test_backoff_base_ms_is_exponential_and_capped() {
        assert_eq!(backoff_base_ms(0), BASE_BACKOFF_MS);
        assert_eq!(backoff_base_ms(1), BASE_BACKOFF_MS * 2);
        assert_eq!(backoff_base_ms(2), BASE_BACKOFF_MS * 4);
        assert_eq!(backoff_base_ms(3), BASE_BACKOFF_MS * 8);
        assert_eq!(backoff_base_ms(20), MAX_BACKOFF_MS);
    }

    #[test]
    fn test_backoff_base_ms_monotonic_non_decreasing() {
        let mut prev = 0;
        for attempt in 0..12 {
            let current = backoff_base_ms(attempt);
            assert!(current >= prev, "backoff should never decrease");
            prev = current;
        }
    }

    #[test]
    fn test_backoff_jitter_within_bounds() {
        for _ in 0..1000 {
            assert!(backoff_jitter_ms(BACKOFF_JITTER_MS) < BACKOFF_JITTER_MS);
        }
        assert_eq!(backoff_jitter_ms(0), 0);
    }

    #[test]
    fn test_backoff_delay_includes_base() {
        let delay = backoff_delay(2);
        assert!(delay >= Duration::from_millis(backoff_base_ms(2)));
        assert!(delay < Duration::from_millis(backoff_base_ms(2) + BACKOFF_JITTER_MS));
    }
}
