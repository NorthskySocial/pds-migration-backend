use crate::agent::{deactivate_account, login_helper};
use crate::{build_agent, MigrationError, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct DeactivateAccountRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

impl fmt::Debug for DeactivateAccountRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeactivateAccountRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
pub async fn deactivate_account_api(req: DeactivateAccountRequest) -> Result<(), MigrationError> {
    let did = req.did.as_str();
    tracing::info!(
        "[{}] Starting account deactivation on {}",
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
    deactivate_account(&agent).await?;
    tracing::info!(
        "[{}] Successfully deactivated account on {}",
        did,
        req.pds_host
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deactivate_account_request_redacts_token() {
        let req = DeactivateAccountRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
