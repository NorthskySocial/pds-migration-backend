use crate::config::AppConfig;
use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::{Data, Json};
use actix_web::HttpResponse;
use pdsmigration_common::{did_to_car_filename, downloads_dir, ImportPDSRequest, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ImportPDSApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl fmt::Debug for ImportPDSApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPDSApiRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

impl From<ImportPDSApiRequest> for ImportPDSRequest {
    fn from(req: ImportPDSApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            did: req.did,
            token: req.token,
        }
    }
}

#[utoipa::path(
    post,
    path = "/import-repo",
    request_body = ImportPDSApiRequest,
    responses(
        (status = 200, description = "Repo imported successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
#[post("/import-repo")]
pub async fn import_pds_api(
    req: Json<ImportPDSApiRequest>,
    config: Data<AppConfig>,
) -> Result<HttpResponse, ApiError> {
    let req_inner = req.into_inner();
    let did = req_inner.did.clone();
    tracing::info!("[{}] Import repository request received", did);
    let endpoint_url = config.external_services.s3_endpoint.clone();
    let config = aws_config::from_env()
        .region("auto")
        .endpoint_url(&endpoint_url)
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&config);

    let bucket_name = "migration".to_string();
    let file_name = did_to_car_filename(&did);
    let key = format!("migration/{file_name}");

    // Download the file from S3
    let s3_response = client
        .get_object()
        .bucket(&bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(|error| ApiError::Runtime {
            message: error.to_string(),
        })?;

    // Save the file locally using AWS SDK's built-in method
    let body_bytes = s3_response
        .body
        .collect()
        .await
        .map_err(|error| ApiError::Runtime {
            message: error.to_string(),
        })?;

    let file_path = downloads_dir()
        .map_err(|error| ApiError::Runtime {
            message: error.to_string(),
        })?
        .join(&file_name);
    std::fs::write(&file_path, body_bytes.into_bytes()).map_err(|error| ApiError::Runtime {
        message: error.to_string(),
    })?;
    pdsmigration_common::import_pds_api(req_inner.into()).await?;
    tracing::info!("[{}] Repository imported successfully", did);

    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_pds_api_request_redacts_token() {
        let req = ImportPDSApiRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
