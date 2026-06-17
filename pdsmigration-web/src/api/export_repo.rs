use crate::api::EnqueueJobResponse;
use crate::background_jobs::{export_repo_to_s3, JobManager};
use crate::config::AppConfig;
use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use crate::Json;
use actix_web::{web, HttpResponse};
use pdsmigration_common::{ExportPDSRequest, REDACTED};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ExportPDSApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl fmt::Debug for ExportPDSApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportPDSApiRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

impl From<ExportPDSApiRequest> for ExportPDSRequest {
    fn from(req: ExportPDSApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            did: req.did,
            token: req.token,
        }
    }
}

#[utoipa::path(
    post,
    path = "/export-repo",
    request_body = ExportPDSApiRequest,
    responses(
        (status = 200, description = "Export Repo completed successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
#[post("/export-repo")]
pub async fn export_pds_api(req: Json<ExportPDSApiRequest>) -> Result<HttpResponse, ApiError> {
    let req_inner = req.into_inner();
    let did = req_inner.did.clone();
    let endpoint_url = env::var("ENDPOINT").map_err(|e| {
        tracing::error!(
            "[{}] Failed to get ENDPOINT environment variable: {}",
            did,
            e
        );
        ApiError::Runtime {
            message: e.to_string(),
        }
    })?;
    export_repo_to_s3(req_inner.into(), &endpoint_url)
        .await
        .map_err(|e| ApiError::Runtime {
            message: e.to_string(),
        })?;
    let response = HttpResponse::Ok().finish();
    tracing::info!(
        "[{}] Export repository request complete, returning HTTP {}",
        did,
        response.status()
    );
    Ok(response)
}

#[utoipa::path(
    post,
    path = "/jobs/export-repo",
    request_body = ExportPDSApiRequest,
    responses(
        (status = 202, description = "Job enqueued", body = EnqueueJobResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs, config, req))]
#[post("/jobs/export-repo")]
pub async fn enqueue_export_repo_job_api(
    jobs: web::Data<JobManager>,
    config: web::Data<AppConfig>,
    req: Json<ExportPDSApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req_inner = req.into_inner();
    let did = req_inner.did.clone();
    tracing::info!("[{}] Enqueueing export-repo job", did);
    let id = jobs
        .spawn_export_repo(
            ExportPDSRequest::from(req_inner),
            config.external_services.s3_endpoint.clone(),
        )
        .await?;
    tracing::info!("[{}] Enqueued export-repo job {}", did, id);
    Ok(HttpResponse::Accepted().json(EnqueueJobResponse {
        job_id: id.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_repo_api_request_redacts_token() {
        let req = ExportPDSApiRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
