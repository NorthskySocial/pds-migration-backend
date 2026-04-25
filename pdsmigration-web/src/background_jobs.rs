use crate::errors::ApiError;
use bsky_sdk::api::agent::Configure;
use derive_more::Display;
use futures_util::StreamExt;
use pdsmigration_common::{
    build_agent, did_to_dirname, download_blob, format_cid, login_helper, missing_blobs,
    upload_blob_v2, ExportBlobsRequest, GetBlobRequest, MigrationError, UploadBlobsRequest,
};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)] // Used in schema attribute macros
use serde_json::json;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use utoipa::ToSchema;
use uuid::Uuid;

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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Success,
    Error,
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct JobProgress {
    #[schema(example = 1)]
    pub successful_blobs: u64,
    #[schema(example = json!(["550e8400-e29b-41d4-a716-446655440000"]))]
    pub successful_blobs_ids: Vec<String>,
    #[schema(example = 1)]
    pub invalid_blobs: u64,
    #[schema(example = json!(["550e8400-e29b-41d4-a716-446655440001"]))]
    pub invalid_blob_ids: Vec<String>,
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
            "successful_blobs_ids": ["550e8400-e29b-41d4-a716-446655440000"],
            "invalid_blobs": 1,
            "invalid_blob_ids": ["550e8400-e29b-41d4-a716-446655440001"],
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

#[derive(Debug)]
struct RunningJob {
    handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct JobManager {
    state: Arc<RwLock<JobState>>,
}

#[derive(Default, Debug)]
struct JobState {
    records: HashMap<Uuid, JobRecord>,
    running: HashMap<Uuid, RunningJob>,
}

impl JobState {
    pub fn set_running(&mut self, id: Uuid) {
        if let Some(r) = self.records.get_mut(&id) {
            r.status = JobStatus::Running;
            r.started_at = Some(now_millis());
        }
    }

    pub fn finalize(&mut self, id: Uuid, result: Result<(), MigrationError>) {
        if let Some(r) = self.records.get_mut(&id) {
            match result {
                Ok(_) => {
                    r.status = JobStatus::Success;
                }
                Err(e) => {
                    r.status = JobStatus::Error;
                    r.error = Some(format!("{}", e));
                }
            }
            r.finished_at = Some(now_millis());
        }
        self.running.remove(&id);
    }

    pub fn update_total(&mut self, id: Uuid, total: u64) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.total = Some(total);
            }
        }
    }

    pub fn record_success(&mut self, id: Uuid, blob_id: String) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.successful_blobs += 1;
                progress.successful_blobs_ids.push(blob_id);
            }
        }
    }

    pub fn record_failure(&mut self, id: Uuid, blob_id: String) {
        if let Some(r) = self.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.invalid_blobs += 1;
                progress.invalid_blob_ids.push(blob_id);
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

    pub async fn list(&self) -> Vec<JobRecord> {
        let st = self.state.read().await;
        st.records.values().cloned().collect()
    }

    pub async fn get(&self, id: Uuid) -> Option<JobRecord> {
        let st = self.state.read().await;
        st.records.get(&id).cloned()
    }

    pub async fn cancel(&self, id: Uuid) -> bool {
        let mut st = self.state.write().await;
        if let Some(running) = st.running.remove(&id) {
            running.handle.abort();
            if let Some(rec) = st.records.get_mut(&id) {
                rec.status = JobStatus::Canceled;
                rec.finished_at = Some(now_millis());
            }
            true
        } else {
            false
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn spawn_upload_blobs(
        &self,
        request: UploadBlobsRequest,
        concurrent_tasks: usize,
    ) -> Result<Uuid, ApiError> {
        let id = Uuid::new_v4();
        let did = request.did.clone();
        let pds_host = request.pds_host.clone();
        tracing::info!(
            "[{}] Spawning upload_blobs job {} for {} (concurrency={})",
            did,
            id,
            pds_host,
            concurrent_tasks
        );
        let rec = JobRecord::new(id, JobKind::UploadBlobs);

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
        }

        let state = self.state.clone();
        let handle = tokio::spawn(async move {
            {
                let mut st = state.write().await;
                st.set_running(id);
            }

            let result = upload_blobs_api_job(id, state.clone(), request, concurrent_tasks).await;
            {
                let mut st = state.write().await;
                st.finalize(id, result);
            }
        });

        {
            let mut st = self.state.write().await;
            st.running.insert(id, RunningJob { handle });
        }

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
        let handle = tokio::spawn(async move {
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

        {
            let mut st = self.state.write().await;
            st.running.insert(id, RunningJob { handle });
        }

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

    let mut path = match std::env::current_dir() {
        Ok(path) => path,
        Err(e) => {
            return Err(MigrationError::Runtime {
                message: e.to_string(),
            })
        }
    };
    path.push(did_to_dirname(&session.did));
    let did = session.did.as_str();
    if req.is_missing_blob_request {
        if let Err(e) = tokio::fs::remove_dir_all(path.as_path()).await {
            if e.kind() != ErrorKind::NotFound {
                return Err(MigrationError::Runtime {
                    message: format!("Failed to clean directory: {}", e),
                });
            }
        }
        tracing::info!("[{}] Cleaned directory for missing blob request", did);
    }
    match tokio::fs::create_dir(path.as_path()).await {
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
        let mut filepath = match std::env::current_dir() {
            Ok(res) => res,
            Err(e) => {
                return Err(MigrationError::Runtime {
                    message: e.to_string(),
                });
            }
        };
        filepath.push(did_to_dirname(&session.did));
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
                    let mut path = std::env::current_dir().unwrap();
                    path.push(did_to_dirname(&session.did));
                    path.push(&blob_cid_str);
                    let mut file = tokio::fs::File::create(path.as_path()).await.unwrap();

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.unwrap();
                        file.write_all(&chunk).await.unwrap();
                    }

                    file.flush().await.unwrap();

                    {
                        let mut st = state.write().await;
                        st.record_success(id, blob_cid_str.clone());
                    }
                }
                Err(e) => {
                    handle_rate_limit_error(&e, &blob_cid_str, did, JobKind::ExportBlobs).await;
                    {
                        let mut st = state.write().await;
                        st.record_failure(id, blob_cid_str.clone());
                    }
                }
            }
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
    let mut path = std::env::current_dir().unwrap();
    path.push(did_to_dirname(&session.did));
    match tokio::fs::read_dir(path.as_path()).await {
        Ok(output) => blob_dir = output,
        Err(error) => {
            tracing::error!("[{}] {}", did, error.to_string());
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

    // process blobs in parallel
    let did_owned = did.to_string();
    futures_util::stream::iter(blobs_in_dir.into_iter())
        .map(|blob| {
            let agent = agent.clone();
            let state = state.clone();
            let did_inner = did_owned.clone();
            async move {
                let file = match tokio::fs::read(blob.path()).await {
                    Ok(data) => data,
                    Err(error) => {
                        tracing::error!("[{}] {}", did_inner, error.to_string());
                        let blob_cid_str = blob.file_name().to_string_lossy().to_string();
                        let mut st = state.write().await;
                        st.record_failure(id, blob_cid_str);
                        return;
                    }
                };
                let blob_cid_str = blob.file_name().to_string_lossy().to_string();
                tracing::info!(
                    "[{}] Uploading blob: {:?} with size {}...",
                    did_inner,
                    blob_cid_str,
                    file.len()
                );
                match upload_blob_v2(&agent, file, &blob_cid_str).await {
                    Ok(_) => {
                        let mut st = state.write().await;
                        st.record_success(id, blob_cid_str.clone());
                    }
                    Err(e) => {
                        handle_rate_limit_error(
                            &e,
                            &blob_cid_str,
                            &did_inner,
                            JobKind::UploadBlobs,
                        )
                        .await;
                        let mut st = state.write().await;
                        st.record_failure(id, blob_cid_str.clone());
                    }
                }
            }
        })
        .buffer_unordered(concurrent_tasks)
        .collect::<Vec<_>>()
        .await;

    tracing::info!("[{}] Finished uploading blobs", did);
    Ok(())
}

async fn handle_rate_limit_error(
    error: &MigrationError,
    blob_id: &str,
    did: &str,
    operation: JobKind,
) {
    match error {
        MigrationError::RateLimitReached => {
            tracing::error!(
                "[{}][{}] Rate limit reached, waiting 5 minutes",
                did,
                operation
            );
            let five_minutes = Duration::from_secs(300);
            tokio::time::sleep(five_minutes).await;
        }
        _ => {
            tracing::error!(
                "[{}][{}] Unexpected error when processing blob: {}",
                did,
                operation,
                error.to_string()
            );
        }
    }
    tracing::error!(
        "[{}][{}] Failed to process blob {} with error: {}",
        did,
        operation,
        blob_id,
        error
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(progress.successful_blobs_ids.is_empty());
        assert!(progress.invalid_blob_ids.is_empty());
        assert!(progress.total.is_none());
    }

    #[test]
    fn test_job_record_new_upload_blobs() {
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::UploadBlobs);

        assert!(matches!(record.kind, JobKind::UploadBlobs));
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

        state.record_success(id, "blob1".to_string());
        state.record_success(id, "blob2".to_string());

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.successful_blobs, 2);
        assert_eq!(progress.successful_blobs_ids, vec!["blob1", "blob2"]);
    }

    #[test]
    fn test_job_state_record_failure() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::ExportBlobs);
        state.records.insert(id, record);

        state.record_failure(id, "bad_blob1".to_string());
        state.record_failure(id, "bad_blob2".to_string());

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.invalid_blobs, 2);
        assert_eq!(progress.invalid_blob_ids, vec!["bad_blob1", "bad_blob2"]);
    }

    #[test]
    fn test_job_state_mixed_blob_results() {
        let mut state = JobState::default();
        let id = Uuid::new_v4();
        let record = JobRecord::new(id, JobKind::UploadBlobs);
        state.records.insert(id, record);

        state.update_total(id, 5);
        state.record_success(id, "ok1".to_string());
        state.record_success(id, "ok2".to_string());
        state.record_success(id, "ok3".to_string());
        state.record_failure(id, "fail1".to_string());
        state.record_failure(id, "fail2".to_string());

        let record = state.records.get(&id).unwrap();
        let progress = record.progress.as_ref().unwrap();
        assert_eq!(progress.total, Some(5));
        assert_eq!(progress.successful_blobs, 3);
        assert_eq!(progress.invalid_blobs, 2);
    }
}
