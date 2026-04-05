use crate::errors::ApiError;
use bsky_sdk::api::agent::Configure;
use futures_util::StreamExt;
use pdsmigration_common::{
    build_agent, download_blob, login_helper, missing_blobs, upload_blob, ExportBlobsRequest,
    GetBlobRequest, MigrationError, UploadBlobsRequest,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
    pub async fn spawn_upload_blobs(&self, request: UploadBlobsRequest) -> Result<Uuid, ApiError> {
        let id = Uuid::new_v4();
        let rec = JobRecord {
            id: id.to_string(),
            kind: JobKind::UploadBlobs,
            status: JobStatus::Queued,
            error: None,
            created_at: now_millis(),
            started_at: None,
            finished_at: None,
            progress: Some(JobProgress {
                successful_blobs: 0,
                successful_blobs_ids: vec![],
                invalid_blobs: 0,
                invalid_blob_ids: vec![],
                total: None,
            }),
        };

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
        }

        let state = self.state.clone();
        let handle = tokio::spawn(async move {
            {
                let mut st = state.write().await;
                if let Some(r) = st.records.get_mut(&id) {
                    r.status = JobStatus::Running;
                    r.started_at = Some(now_millis());
                }
            }

            let result = upload_blobs_api_job(id, state.clone(), request).await;

            match result {
                Ok(_) => {
                    let mut st = state.write().await;
                    if let Some(r) = st.records.get_mut(&id) {
                        r.status = JobStatus::Success;
                        r.finished_at = Some(now_millis());
                    }
                    st.running.remove(&id);
                }
                Err(e) => {
                    let mut st = state.write().await;
                    if let Some(r) = st.records.get_mut(&id) {
                        r.status = JobStatus::Error;
                        r.error = Some(format!("{}", e));
                        r.finished_at = Some(now_millis());
                    }
                    st.running.remove(&id);
                }
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
        let rec = JobRecord {
            id: id.to_string(),
            kind: JobKind::ExportBlobs,
            status: JobStatus::Queued,
            error: None,
            created_at: now_millis(),
            started_at: None,
            finished_at: None,
            progress: Some(JobProgress {
                successful_blobs: 0,
                successful_blobs_ids: vec![],
                invalid_blobs: 0,
                invalid_blob_ids: vec![],
                total: None,
            }),
        };

        {
            let mut st = self.state.write().await;
            st.records.insert(id, rec);
        }

        let state = self.state.clone();
        let handle = tokio::spawn(async move {
            {
                let mut st = state.write().await;
                if let Some(r) = st.records.get_mut(&id) {
                    r.status = JobStatus::Running;
                    r.started_at = Some(now_millis());
                }
            }

            let result = export_blobs_api_job(id, state.clone(), request).await;

            match result {
                Ok(_) => {
                    let mut st = state.write().await;
                    if let Some(r) = st.records.get_mut(&id) {
                        r.status = JobStatus::Success;
                        r.finished_at = Some(now_millis());
                    }
                    st.running.remove(&id);
                }
                Err(e) => {
                    let mut st = state.write().await;
                    if let Some(r) = st.records.get_mut(&id) {
                        r.status = JobStatus::Error;
                        r.error = Some(format!("{}", e));
                        r.finished_at = Some(now_millis());
                    }
                    st.running.remove(&id);
                }
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
        if let Some(r) = st.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.total = Some(missing_blobs.len() as u64);
            }
        }
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
    path.push(session.did.as_str().replace(":", "-"));
    match tokio::fs::create_dir(path.as_path()).await {
        Ok(_) => {
            tracing::info!("Successfully created directory");
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
        tracing::debug!("Missing blob: {:?}", missing_blob);
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
        filepath.push(session.did.as_str().replace(":", "-"));
        filepath.push(
            missing_blob
                .record_uri
                .as_str()
                .split("/")
                .last()
                .unwrap_or("fallback"),
        );
        if !tokio::fs::try_exists(filepath).await.unwrap() {
            let missing_blob_cid = missing_blob.cid.clone();
            let blob_cid_str = format!("{missing_blob_cid:?}")
                .strip_prefix("Cid(Cid(")
                .unwrap()
                .strip_suffix("))")
                .unwrap()
                .to_string();
            let get_blob_request = GetBlobRequest {
                did: session.did.clone(),
                cid: blob_cid_str.clone(),
                token: session.access_jwt.clone(),
            };
            match download_blob(agent.get_endpoint().await.as_str(), &get_blob_request).await {
                Ok(mut stream) => {
                    tracing::info!("Successfully fetched missing blob");
                    let mut path = std::env::current_dir().unwrap();
                    path.push(session.did.as_str().replace(":", "-"));
                    path.push(&blob_cid_str);
                    let mut file = tokio::fs::File::create(path.as_path()).await.unwrap();

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.unwrap();
                        file.write_all(&chunk).await.unwrap();
                    }

                    file.flush().await.unwrap();

                    {
                        let mut st = state.write().await;
                        if let Some(r) = st.records.get_mut(&id) {
                            if let Some(progress) = r.progress.as_mut() {
                                progress.successful_blobs += 1;
                                progress.successful_blobs_ids.push(blob_cid_str.clone());
                            }
                        }
                    }
                }
                Err(e) => {
                    match e {
                        MigrationError::RateLimitReached => {
                            tracing::error!("Rate limit reached, waiting 5 minutes");
                            let five_minutes = Duration::from_secs(300);
                            tokio::time::sleep(five_minutes).await;
                        }
                        _ => {
                            tracing::error!(
                                "Unexpected error when downloading blob: {}",
                                e.to_string()
                            );
                        }
                    }
                    tracing::error!("Failed to download missing blob with cid: {}", blob_cid_str);
                    {
                        let mut st = state.write().await;
                        if let Some(r) = st.records.get_mut(&id) {
                            if let Some(progress) = r.progress.as_mut() {
                                progress.invalid_blobs += 1;
                                progress.invalid_blob_ids.push(blob_cid_str.clone());
                            }
                        }
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

    let mut blob_dir;
    let mut path = std::env::current_dir().unwrap();
    path.push(session.did.as_str().replace(":", "-"));
    match tokio::fs::read_dir(path.as_path()).await {
        Ok(output) => blob_dir = output,
        Err(error) => {
            tracing::error!("{}", error.to_string());
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
        if let Some(r) = st.records.get_mut(&id) {
            if let Some(progress) = r.progress.as_mut() {
                progress.total = Some(blobs_in_dir.len() as u64);
            }
        }
    }

    for blob in blobs_in_dir {
        let file = tokio::fs::read(blob.path()).await.map_err(|error| {
            tracing::error!("{}", error.to_string());
            MigrationError::Runtime {
                message: "Failed to read next blob".to_string(),
            }
        })?;
        let blob_cid_str = blob.file_name().to_string_lossy().to_string();
        tracing::info!(
            "Uploading blob: {:?} with size {}...",
            blob_cid_str,
            file.len()
        );
        match upload_blob(&agent, file).await {
            Ok(_) => {
                let mut st = state.write().await;
                if let Some(r) = st.records.get_mut(&id) {
                    if let Some(progress) = r.progress.as_mut() {
                        progress.successful_blobs += 1;
                        progress.successful_blobs_ids.push(blob_cid_str.clone());
                    }
                }
            }
            Err(e) => {
                match e {
                    MigrationError::RateLimitReached => {
                        tracing::error!("Rate limit reached, waiting 5 minutes");
                        let five_minutes = Duration::from_secs(300);
                        tokio::time::sleep(five_minutes).await;
                    }
                    _ => {
                        tracing::error!("Unexpected error when uploading blob: {}", e.to_string());
                    }
                }
                tracing::error!("Failed to upload blob {}: {}", blob_cid_str, e);
                {
                    let mut st = state.write().await;
                    if let Some(r) = st.records.get_mut(&id) {
                        if let Some(progress) = r.progress.as_mut() {
                            progress.invalid_blobs += 1;
                            progress.invalid_blob_ids.push(blob_cid_str.clone());
                        }
                    }
                }
            }
        }
    }

    tracing::info!("Finished uploading blobs for DID {}", session.did.as_str());
    Ok(())
}
