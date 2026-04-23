use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::DeactivateAccountRequest;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct DeactivateAccountApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl From<DeactivateAccountApiRequest> for DeactivateAccountRequest {
    fn from(req: DeactivateAccountApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            did: req.did,
            token: req.token,
        }
    }
}

#[utoipa::path(
    post,
    path = "/deactivate-account",
    request_body = DeactivateAccountApiRequest,
    responses(
        (status = 200, description = "Account deactivated successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json")
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
#[post("/deactivate-account")]
pub async fn deactivate_account_api(
    req: Json<DeactivateAccountApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Deactivate account request received", did);
    pdsmigration_common::deactivate_account_api(req.into()).await?;
    Ok(HttpResponse::Ok().finish())
}
