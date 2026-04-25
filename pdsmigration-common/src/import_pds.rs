use crate::agent::{account_import, login_helper};
use crate::{build_agent, did_to_car_filename, MigrationError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct ImportPDSRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

#[tracing::instrument(skip(req))]
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
