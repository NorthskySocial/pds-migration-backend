use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ActivateAccountApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
#[utoipa::path(
    post,
    path = "/activate-account",
    request_body = ActivateAccountApiRequest,
    responses(
        (status = 200, description = "Account activated successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[post("/activate-account")]
pub async fn activate_account_api(
    req: Json<ActivateAccountApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Activate account request received", did);
    let token = req.token.clone();
    let pds_host = req.pds_host.clone();
    pdsmigration_common::activate_account(pds_host.as_str(), did.as_str(), token.as_str()).await?;
    Ok(HttpResponse::Ok().finish())
}
