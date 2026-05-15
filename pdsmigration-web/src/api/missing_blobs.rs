use crate::errors::{ApiError, ApiErrorBody};
use crate::{post, APPLICATION_JSON};
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{MissingBlobsRequest, MissingBlobsResponse, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct MissingBlobsApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl fmt::Debug for MissingBlobsApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MissingBlobsApiRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

impl From<MissingBlobsApiRequest> for MissingBlobsRequest {
    fn from(req: MissingBlobsApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            did: req.did,
            token: req.token,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct MissingBlobsApiResponse {
    pub missing_blobs: Vec<String>,
}

impl From<MissingBlobsResponse> for MissingBlobsApiResponse {
    fn from(req: MissingBlobsResponse) -> Self {
        Self {
            missing_blobs: req.missing_blobs,
        }
    }
}

#[utoipa::path(
    post,
    path = "/missing-blobs",
    request_body = MissingBlobsApiRequest,
    responses(
        (status = 200, description = "Missing blobs determined", body = MissingBlobsApiResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json")
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req))]
#[post("/missing-blobs")]
pub async fn missing_blobs_api(
    req: Json<MissingBlobsApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Missing blobs request received", did);
    let response = pdsmigration_common::missing_blobs_api(req.into()).await?;
    let response: MissingBlobsApiResponse = response.into();
    Ok(HttpResponse::Ok()
        .content_type(APPLICATION_JSON)
        .json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_blobs_api_request_redacts_token() {
        let req = MissingBlobsApiRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
