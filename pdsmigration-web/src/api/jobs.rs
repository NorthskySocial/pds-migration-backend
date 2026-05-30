use crate::background_jobs::{JobManager, JobRecord};
use crate::errors::ApiError;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_job_response_serializes_with_job_id_field() {
        let resp = EnqueueJobResponse {
            job_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["job_id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn enqueue_job_response_round_trips_via_serde() {
        let payload = serde_json::json!({ "job_id": "abc-123" });
        let parsed: EnqueueJobResponse = serde_json::from_value(payload).unwrap();
        assert_eq!(parsed.job_id, "abc-123");
    }
}
