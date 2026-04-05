use crate::api::{ExportBlobsApiRequest, UploadBlobsApiRequest};
use crate::background_jobs::{JobManager, JobRecord};
use crate::errors::{ApiError, ApiErrorBody};
use crate::{post, Json};
use actix_web::{get, web, HttpResponse};
use pdsmigration_common::{ExportBlobsRequest, UploadBlobsRequest};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct EnqueueJobResponse {
    #[schema(example = "550e8400-e29b-41d4-a716-446655440000")]
    pub job_id: String,
}

#[utoipa::path(
    post,
    path = "/jobs/export-blobs",
    request_body = ExportBlobsApiRequest,
    responses(
        (status = 202, description = "Job enqueued", body = EnqueueJobResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs, req))]
#[post("/jobs/export-blobs")]
pub async fn enqueue_export_blobs_job_api(
    jobs: web::Data<JobManager>,
    req: Json<ExportBlobsApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let id = jobs
        .spawn_export_blobs(ExportBlobsRequest::from(req.into_inner()))
        .await?;
    Ok(HttpResponse::Accepted().json(EnqueueJobResponse {
        job_id: id.to_string(),
    }))
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
#[tracing::instrument(skip(jobs, req))]
#[post("/jobs/upload-blobs")]
pub async fn enqueue_upload_blobs_job_api(
    jobs: web::Data<JobManager>,
    req: Json<UploadBlobsApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let id = jobs
        .spawn_upload_blobs(UploadBlobsRequest::from(req.into_inner()))
        .await?;
    Ok(HttpResponse::Accepted().json(EnqueueJobResponse {
        job_id: id.to_string(),
    }))
}

#[utoipa::path(
    get,
    path = "/jobs",
    responses(
        (status = 200, description = "List jobs", body = [JobRecord], content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs))]
#[get("/jobs")]
pub async fn list_jobs_api(jobs: web::Data<JobManager>) -> Result<HttpResponse, ApiError> {
    let list = jobs.list().await;
    Ok(HttpResponse::Ok().json(list))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CancelJobResponse {
    pub success: bool,
}

#[utoipa::path(
    post,
    path = "/jobs/{id}/cancel",
    params(("id" = String, Path, description = "Job ID (UUID)")),
    responses(
        (status = 200, description = "Cancel job result", body = CancelJobResponse, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs))]
#[post("/jobs/{id}/cancel")]
pub async fn cancel_job_api(
    jobs: web::Data<JobManager>,
    path: web::Path<(Uuid,)>,
) -> Result<HttpResponse, ApiError> {
    let id = path.into_inner().0;
    let success = jobs.cancel(id).await;
    Ok(HttpResponse::Ok().json(CancelJobResponse { success }))
}

#[utoipa::path(
    get,
    path = "/jobs/{id}",
    params(("id" = String, Path, description = "Job ID (UUID)")),
    responses(
        (status = 200, description = "Job details", body = JobRecord, content_type = "application/json",
            example = json!({
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "kind": "ExportBlobs",
                "status": "Success",
                "created_at": 1700000000,
                "started_at": 1700000001,
                "finished_at": 1700000100,
                "progress": {
                    "successful_blobs": 1,
                    "successful_blobs_ids": ["550e8400-e29b-41d4-a716-446655440000"],
                    "invalid_blobs": 1,
                    "invalid_blob_ids": ["550e8400-e29b-41d4-a716-446655440001"],
                    "total": 2
                }
            })
        ),
        (status = 404, description = "Not found"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(jobs))]
#[get("/jobs/{id}")]
pub async fn get_job_api(
    jobs: web::Data<JobManager>,
    path: web::Path<(Uuid,)>,
) -> Result<HttpResponse, ApiError> {
    let id = path.into_inner().0;
    let list = jobs.list().await;
    let job_ids: Vec<String> = list.iter().map(|job| job.id.clone()).collect();
    tracing::info!(request_guid = %id, job_ids = ?job_ids, "Getting job with ID: {}", id);

    match jobs.get(id).await {
        Some(job) => {
            if let Some(progress) = &job.progress {
                tracing::info!(
                    job_status = ?job.status,
                    successful_blobs = progress.successful_blobs,
                    invalid_blobs = progress.invalid_blobs,
                    total = progress.total,
                    "Job found, logging progress"
                );
            }
            Ok(HttpResponse::Ok().json(job))
        }
        None => Ok(HttpResponse::NotFound().finish()),
    }
}
