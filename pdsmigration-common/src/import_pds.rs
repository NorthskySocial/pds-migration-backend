use crate::agent::{account_import, login_helper};
use crate::{build_agent, did_to_car_filename, MigrationError, REDACTED};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct ImportPDSRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

impl fmt::Debug for ImportPDSRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportPDSRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

#[tracing::instrument(skip(req), fields(did = %req.did, pds_host = %req.pds_host))]
pub async fn import_pds_api(req: ImportPDSRequest) -> Result<(), MigrationError> {
    let did = req.did.as_str();
    tracing::info!("[{}] Starting PDS repo import to {}", did, req.pds_host);
    let agent = build_agent().await?;
    let session = login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    let filename = did_to_car_filename(&session.did);
    tracing::info!("[{}] Importing repo from {}", did, filename);
    account_import(&agent, filename.as_str()).await?;
    tracing::info!("[{}] Successfully imported PDS", did);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_pds_request_redacts_token() {
        let req = ImportPDSRequest {
            pds_host: "https://pds.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
