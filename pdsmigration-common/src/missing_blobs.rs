use crate::agent::{login_helper, missing_blobs};
use crate::{build_agent, MigrationError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct MissingBlobsRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MissingBlobsResponse {
    pub missing_blobs: Vec<String>,
}

#[tracing::instrument(skip(req))]
pub async fn missing_blobs_api(
    req: MissingBlobsRequest,
) -> Result<MissingBlobsResponse, MigrationError> {
    let did = req.did.as_str();
    tracing::info!("[{}] Listing missing blobs on {}", did, req.pds_host);
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    let initial_missing_blobs = missing_blobs(&agent).await?;
    let mut missing_blob_cids = Vec::new();
    for blob in &initial_missing_blobs {
        missing_blob_cids.push(format!("{:?}", blob.cid));
    }
    tracing::info!(
        "[{}] Returning {} missing blob ids",
        did,
        missing_blob_cids.len()
    );
    Ok(MissingBlobsResponse {
        missing_blobs: missing_blob_cids,
    })
}
