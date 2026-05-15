use crate::agent::{get_service_auth, login_helper};
use crate::{build_agent, MigrationError, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct ServiceAuthRequest {
    pub pds_host: String,
    pub aud: String,
    pub did: String,
    pub token: String,
}

impl fmt::Debug for ServiceAuthRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAuthRequest")
            .field("pds_host", &self.pds_host)
            .field("aud", &self.aud)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
pub struct ServiceAuthResponse {
    pub token: String,
}

impl fmt::Debug for ServiceAuthResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceAuthResponse")
            .field("token", &REDACTED)
            .finish()
    }
}

#[tracing::instrument(skip(req), fields(aud = %req.aud, pds_host = %req.pds_host))]
pub async fn get_service_auth_api(req: ServiceAuthRequest) -> Result<String, MigrationError> {
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    let token = get_service_auth(&agent, req.aud.as_str()).await?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_auth_request_redacts_token() {
        let req = ServiceAuthRequest {
            pds_host: "https://pds.example.com".to_string(),
            aud: "did:web:example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
        assert!(dbg.contains("https://pds.example.com"));
        assert!(dbg.contains("did:plc:abc123"));
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
