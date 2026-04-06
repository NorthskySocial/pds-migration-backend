use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use crate::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{ExportBlobsRequest, ExportBlobsResponse};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ExportBlobsApiRequest {
    #[schema(example = "https://destinationPDS.example.com")]
    pub destination: String,
    #[schema(example = "https://sourcePDS.example.com")]
    pub origin: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub origin_token: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub destination_token: String,
    pub is_missing_blob_request: bool,
}

impl From<ExportBlobsApiRequest> for ExportBlobsRequest {
    fn from(req: ExportBlobsApiRequest) -> Self {
        Self {
            destination: req.destination,
            origin: req.origin,
            did: req.did,
            origin_token: req.origin_token,
            destination_token: req.destination_token,
            is_missing_blob_request: req.is_missing_blob_request,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ExportBlobsApiResponse {
    pub successful_blobs: Vec<String>,
    pub invalid_blobs: Vec<String>,
}

impl From<ExportBlobsResponse> for ExportBlobsApiResponse {
    fn from(req: ExportBlobsResponse) -> Self {
        Self {
            successful_blobs: req.successful_blobs,
            invalid_blobs: req.invalid_blobs,
        }
    }
}

#[utoipa::path(
    post,
    path = "/export-blobs",
    request_body = ExportBlobsApiRequest,
    responses(
        (status = 200, description = "Export Blobs completed successfully", body = ExportBlobsApiResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req))]
#[post("/export-blobs")]
pub async fn export_blobs_api(req: Json<ExportBlobsApiRequest>) -> Result<HttpResponse, ApiError> {
    tracing::info!("Export blobs request received");
    let req = req.into_inner();
    let result = pdsmigration_common::export_blobs_api(req.into()).await?;
    tracing::info!("Blobs exported successfully");
    let result: ExportBlobsApiResponse = result.into();
    Ok(HttpResponse::Ok().json(result))
}
