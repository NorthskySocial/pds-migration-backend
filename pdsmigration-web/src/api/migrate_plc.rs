use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{MigratePlcRequest, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct MigratePlcApiRequest {
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
    #[schema(example = "7G54NB")]
    pub plc_signing_token: String,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub user_recovery_key: Option<String>,
}

impl fmt::Debug for MigratePlcApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigratePlcApiRequest")
            .field("destination", &self.destination)
            .field("destination_token", &REDACTED)
            .field("origin", &self.origin)
            .field("did", &self.did)
            .field("origin_token", &REDACTED)
            .field("plc_signing_token", &REDACTED)
            .field(
                "user_recovery_key",
                &self.user_recovery_key.as_ref().map(|_| REDACTED),
            )
            .finish()
    }
}

impl From<MigratePlcApiRequest> for MigratePlcRequest {
    fn from(req: MigratePlcApiRequest) -> Self {
        Self {
            destination: req.destination,
            destination_token: req.destination_token,
            origin: req.origin,
            did: req.did,
            origin_token: req.origin_token,
            plc_signing_token: req.plc_signing_token,
            user_recovery_key: req.user_recovery_key,
        }
    }
}

#[utoipa::path(
    post,
    path = "/migrate-plc",
    request_body = MigratePlcApiRequest,
    responses(
        (status = 200, description = "PLC migrated successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json"),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req))]
#[post("/migrate-plc")]
pub async fn migrate_plc_api(req: Json<MigratePlcApiRequest>) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!(
        "[{}] Migrate PLC from origin to destination request received",
        did
    );
    pdsmigration_common::migrate_plc_api(req.into()).await?;
    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_plc_api_request_redacts_all_secrets() {
        let req = MigratePlcApiRequest {
            destination: "https://dst.example.com".to_string(),
            destination_token: "dst-secret".to_string(),
            origin: "https://src.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            origin_token: "src-secret".to_string(),
            plc_signing_token: "plc-signing-secret".to_string(),
            user_recovery_key: Some("recovery-secret".to_string()),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        for secret in [
            "dst-secret",
            "src-secret",
            "plc-signing-secret",
            "recovery-secret",
        ] {
            assert!(!dbg.contains(secret), "leaked secret: {secret}");
        }
    }
}
