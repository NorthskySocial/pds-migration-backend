use crate::background_jobs::{JobManager, JobRecord};
use crate::errors::ApiError;
use crate::post;
use actix_web::{get, web, HttpResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Response returned by all endpoints that enqueue a background job.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct EnqueueJobResponse {
    #[schema(example = "550e8400-e29b-41d4-a716-446655440000")]
    pub job_id: String,
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
    tracing::info!("Cancelling job {}", id);
    let success = jobs.cancel(id).await;
    if success {
        tracing::info!("Cancelled job {}", id);
    } else {
        tracing::warn!(
            "Cancel requested for unknown or already-finished job {}",
            id
        );
    }
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

    match jobs.get(id).await {
        Some(job) => Ok(HttpResponse::Ok().json(job)),
        None => {
            tracing::info!(request_guid = %id, "Job not found");
            Ok(HttpResponse::NotFound().finish())
        }
    }
}
