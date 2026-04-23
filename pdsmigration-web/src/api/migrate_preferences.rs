use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::MigratePreferencesRequest;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct MigratePreferencesApiRequest {
    #[schema(example = "https://destinationPDS.example.com")]
    pub destination: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub destination_token: String,
    #[schema(example = "https://sourcePDS.example.com")]
    pub origin: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub origin_token: String,
}

impl From<MigratePreferencesApiRequest> for MigratePreferencesRequest {
    fn from(req: MigratePreferencesApiRequest) -> Self {
        Self {
            destination: req.destination,
            destination_token: req.destination_token,
            origin: req.origin,
            did: req.did,
            origin_token: req.origin_token,
        }
    }
}

#[utoipa::path(
    post,
    path = "/migrate-preferences",
    request_body = MigratePreferencesApiRequest,
    responses(
        (status = 200, description = "User preferences migrated successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json")
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req))]
#[post("/migrate-preferences")]
pub async fn migrate_preferences_api(
    req: Json<MigratePreferencesApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Migrate preferences request received", did);
    pdsmigration_common::migrate_preferences_api(req.into()).await?;
    Ok(HttpResponse::Ok().finish())
}
