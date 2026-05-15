use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{MigrationError, UploadBlobsRequest, REDACTED};
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
    path = "/upload-blobs",
    request_body = UploadBlobsApiRequest,
    responses(
        (status = 200, description = "Upload exported blobs successful"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json")
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req))]
#[post("/upload-blobs")]
pub async fn upload_blobs_api(req: Json<UploadBlobsApiRequest>) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Upload blobs request received", did);
    pdsmigration_common::upload_blobs_api(req.into())
        .await
        .map_err(|e| {
            tracing::error!("[{}] Failed to upload blobs: {}", did, e);
            match e {
                MigrationError::Validation { .. } => ApiError::Runtime {
                    message: "Unexpected error occurred".to_string(),
                },
                MigrationError::Upstream { .. } => ApiError::Runtime {
                    message: "Unexpected error occurred".to_string(),
                },
                MigrationError::Runtime { .. } => ApiError::Runtime {
                    message: "Unexpected error occurred".to_string(),
                },
                MigrationError::RateLimitReached => ApiError::Runtime {
                    message: "Unexpected error occurred".to_string(),
                },
                MigrationError::Authentication { message } => ApiError::Authentication { message },
            }
        })?;
    Ok(HttpResponse::Ok().finish())
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
