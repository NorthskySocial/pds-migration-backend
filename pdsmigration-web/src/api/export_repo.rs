use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::ExportPDSRequest;
use serde::{Deserialize, Serialize};
use std::env;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ExportPDSApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
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
#[tracing::instrument(skip(req))]
#[post("/export-repo")]
pub async fn export_pds_api(req: Json<ExportPDSApiRequest>) -> Result<HttpResponse, ApiError> {
    let req_inner = req.into_inner();
    let did = req_inner.did.clone();
    tracing::info!("[{}] Export repository request received", did);
    pdsmigration_common::export_pds_api(req_inner.into())
        .await
        .map_err(|e| {
            tracing::error!("[{}] Failed to export repository: {}", did, e);
            ApiError::Runtime {
                message: e.to_string(),
            }
        })?;

    // Upload the downloaded file to AWS S3
    let endpoint_url = env::var("ENDPOINT").map_err(|e| {
        tracing::error!("[{}] Failed to get ENDPOINT environment variable: {}", did, e);
        ApiError::Runtime {
            message: e.to_string(),
        }
    })?;

    tracing::debug!("[{}] Loading AWS config with endpoint: {}", did, endpoint_url);
    let config = aws_config::from_env()
        .region("auto")
        .endpoint_url(&endpoint_url)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    let bucket_name = "migration".to_string();
    let file_name = did.replace(":", "-") + ".car";
    let key = "migration/".to_string() + &did.replace(":", "-") + ".car";

    tracing::debug!(
        "[{}] Uploading file {} to S3 bucket {} with key {}",
        did,
        file_name,
        bucket_name,
        key
    );

    let body = match aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(&file_name))
        .await
    {
        Ok(body) => {
            tracing::debug!("[{}] Successfully created ByteStream from file", did);
            body
        }
        Err(e) => {
            tracing::error!(
                "[{}] Failed to create ByteStream from file {}: {:?}",
                did,
                file_name,
                e
            );
            return Err(ApiError::Runtime {
                message: e.to_string(),
            });
        }
    };

    match client
        .put_object()
        .bucket(&bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(
                "[{}] Failed to upload to S3: bucket={}, key={}, error={:?}",
                did,
                bucket_name,
                key,
                e
            );
            return Err(ApiError::Runtime {
                message: e.to_string(),
            });
        }
    };

    tracing::info!("[{}] Repository exported and uploaded to S3 successfully", did);
    Ok(HttpResponse::Ok().finish())
}
