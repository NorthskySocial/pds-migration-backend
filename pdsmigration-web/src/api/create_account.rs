use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{create_account, CreateAccountRequest};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct CreateAccountApiRequest {
    #[schema(example = "user@example.com")]
    pub email: String,
    #[schema(example = "alice.test")]
    pub handle: String,
    #[schema(example = "bsky-invite-abc123-xyz789")]
    pub invite_code: String,
    #[schema(example = "StrongP@ssw0rd!")]
    pub password: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    #[schema(example = "did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme")]
    pub recovery_key: Option<String>,
}

impl fmt::Debug for CreateAccountApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateAccountApiRequest")
            .field("email", &self.email)
            .field("handle", &self.handle)
            .field("invite_code", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .field("token", &"[REDACTED]")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field(
                "recovery_key",
                &self.recovery_key.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[utoipa::path(
    post,
    path = "/create-account",
    request_body = CreateAccountApiRequest,
    responses(
        (status = 200, description = "Account created successfully"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json", example = json!({
            "code": "VALIDATION_ERROR",
            "message": "Field 'did' is invalid"
        })),
        (status = 401, description = "Authentication error", body = ApiError, content_type = "application/json", example = json!({
            "code": "AUTHENTICATION_ERROR",
            "message": "Invalid or expired token"
        })),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json", example = json!({
            "code": "RATE_LIMIT",
            "message": "Rate limit reached. Please try again later."
        })),
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(
    email = %req.email,
    handle = %req.handle,
    pds_host = %req.pds_host,
    did = %req.did
))]
#[post("/create-account")]
pub async fn create_account_api(
    req: Json<CreateAccountApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Create account request received", did);

    let did_parsed = req.did.parse().map_err(|_error| ApiError::Validation {
        field: "did".to_string(),
    })?;

    let handle = req.handle.parse().map_err(|_error| ApiError::Validation {
        field: "handle".to_string(),
    })?;

    create_account(
        req.pds_host.as_str(),
        &CreateAccountRequest {
            did: did_parsed,
            email: Some(req.email.clone()),
            handle,
            invite_code: Some(req.invite_code.trim().to_string()),
            password: Some(req.password.clone()),
            recovery_key: req.recovery_key.clone(),
            verification_code: Some(String::from("")),
            verification_phone: None,
            plc_op: None,
            token: Some(req.token.clone()),
        },
    )
    .await
    .map_err(ApiError::from)?;

    tracing::info!(
        "[{}] Account created successfully - Used invite code {}",
        did,
        req.invite_code
    );
    Ok(HttpResponse::Ok().finish())
}
