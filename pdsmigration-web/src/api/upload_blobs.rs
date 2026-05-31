use crate::api::EnqueueJobResponse;
use crate::background_jobs::JobManager;
use crate::config::AppConfig;
use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use crate::Json;
use actix_web::{web, HttpResponse};
use pdsmigration_common::{UploadBlobsRequest, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct UploadBlobsApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl fmt::Debug for UploadBlobsApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadBlobsApiRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

impl From<UploadBlobsApiRequest> for UploadBlobsRequest {
    fn from(req: UploadBlobsApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            did: req.did,
            token: req.token,
        }
    }
}

#[utoipa::path(
    post,
    path = "/jobs/upload-blobs",
    request_body = UploadBlobsApiRequest,
    responses(
        (status = 202, description = "Job enqueued", body = EnqueueJobResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs, req, config))]
#[post("/jobs/upload-blobs")]
pub async fn enqueue_upload_blobs_job_api(
    jobs: web::Data<JobManager>,
    config: web::Data<AppConfig>,
    req: Json<UploadBlobsApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req_inner = req.into_inner();
    let did = req_inner.did.clone();
    tracing::info!("[{}] Enqueueing upload-blobs job", did);
    let id = jobs
        .spawn_upload_blobs(
            UploadBlobsRequest::from(req_inner),
            config.server.concurrent_tasks_per_job,
            config.server.upload_max_attempts,
        )
        .await?;
    tracing::info!("[{}] Enqueued upload-blobs job {}", did, id);
    Ok(HttpResponse::Accepted().json(EnqueueJobResponse {
        job_id: id.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_blobs_api_request_redacts_token() {
        let req = UploadBlobsApiRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
