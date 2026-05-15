use crate::errors::{ApiError, ApiErrorBody};
use crate::post;
use actix_web::web::Json;
use actix_web::HttpResponse;
use pdsmigration_common::{ServiceAuthRequest, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct ServiceAuthApiRequest {
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[schema(example = "did:web:northsky.social")]
    pub aud: String,
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: String,
    #[schema(example = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example.signature")]
    pub token: String,
}

impl fmt::Debug for ServiceAuthApiRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAuthApiRequest")
            .field("pds_host", &self.pds_host)
            .field("aud", &self.aud)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

impl From<ServiceAuthApiRequest> for ServiceAuthRequest {
    fn from(req: ServiceAuthApiRequest) -> Self {
        Self {
            pds_host: req.pds_host,
            aud: req.aud,
            did: req.did,
            token: req.token,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema)]
struct ServiceAuthResponse {
    token: String,
}

impl fmt::Debug for ServiceAuthResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAuthResponse")
            .field("token", &REDACTED)
            .finish()
    }
}

#[utoipa::path(
    post,
    path = "/service-auth",
    request_body = ServiceAuthApiRequest,
    responses(
        (status = 200, description = "Service Auth token successfully requested", body = ServiceAuthResponse, content_type = "application/json"),
        (status = 400, description = "Invalid request", body = ApiErrorBody, content_type = "application/json"),
        (status = 401, description = "Authentication error", body = ApiErrorBody, content_type = "application/json"),
        (status = 429, description = "Rate limit exceeded", body = ApiErrorBody, content_type = "application/json")
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host, aud = %req.aud))]
#[post("/service-auth")]
pub async fn get_service_auth_api(
    req: Json<ServiceAuthApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    let did = req.did.clone();
    tracing::info!("[{}] Service auth request received with aud: {}, pds_host: {}", did, req.aud, req.pds_host);
    let response = pdsmigration_common::get_service_auth_api(req.into()).await?;
    let response = ServiceAuthResponse { token: response };
    Ok(HttpResponse::Ok().json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_auth_api_request_redacts_token() {
        let req = ServiceAuthApiRequest {
            pds_host: "https://pds.example.com".to_string(),
            aud: "did:web:example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
        assert!(dbg.contains("https://pds.example.com"));
    }

    #[test]
    fn service_auth_response_redacts_token() {
        let resp = ServiceAuthResponse {
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", resp);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
