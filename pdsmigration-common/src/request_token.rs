use crate::agent::{login_helper, request_token};
use crate::{build_agent, MigrationError, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct RequestTokenRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

impl fmt::Debug for RequestTokenRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestTokenRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
pub async fn request_token_api(req: RequestTokenRequest) -> Result<(), MigrationError> {
    let did = req.did.as_str();
    tracing::info!(
        "[{}] Requesting PLC operation signature token from {}",
        did,
        req.pds_host
    );
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    request_token(&agent).await?;
    tracing::info!(
        "[{}] Successfully requested PLC operation signature token",
        did
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_token_request_redacts_token() {
        let req = RequestTokenRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
