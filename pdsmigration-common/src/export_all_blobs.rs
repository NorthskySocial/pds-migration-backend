use crate::agent::{download_blob, list_all_blobs, login_helper};
use crate::{build_agent, did_to_dirname, format_cid, MigrationError};
use bsky_sdk::api::types::string::Did;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Deserialize, Serialize)]
pub struct GetBlobRequest {
    pub did: Did,
    pub cid: String,
    pub token: String,
}

impl fmt::Debug for GetBlobRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetBlobRequest")
            .field("did", &self.did)
            .field("cid", &self.cid)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
pub struct ExportAllBlobsRequest {
    pub origin: String,
    pub did: String,
    pub origin_token: String,
}

impl fmt::Debug for ExportAllBlobsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportAllBlobsRequest")
            .field("origin", &self.origin)
            .field("did", &self.did)
            .field("origin_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExportAllBlobsResponse {
    pub successful_blobs: Vec<String>,
    pub failed_blobs: Vec<String>,
}

#[tracing::instrument]
pub async fn export_all_blobs_api(
    req: ExportAllBlobsRequest,
) -> Result<ExportAllBlobsResponse, MigrationError> {
    let agent = build_agent().await?;
    let session = login_helper(
        &agent,
        req.origin.as_str(),
        req.did.as_str(),
        req.origin_token.as_str(),
    )
    .await?;
    let did = session.did.as_str();
    tracing::info!("[{}] Starting export of all blobs from {}", did, req.origin);
    let blobs = list_all_blobs(&agent).await?;
    let mut path = std::env::current_dir().unwrap();
    path.push(did_to_dirname(did));
    match tokio::fs::create_dir(path.as_path()).await {
        Ok(_) => {}
        Err(e) => {
            if e.kind() != ErrorKind::AlreadyExists {
                tracing::error!("[{}] Error creating directory: {:?}", did, e);
                return Err(MigrationError::Runtime {
                    message: e.to_string(),
                });
            }
        }
    }

    let mut successful_blobs = vec![];
    let mut failed_blobs = vec![];
    for blob in &blobs {
        let session = agent.get_session().await.unwrap();
        let blob_cid_str = format_cid(blob);
        let mut filepath = std::env::current_dir().unwrap();
        filepath.push(did_to_dirname(&session.did));
        filepath.push(&blob_cid_str);
        if !tokio::fs::try_exists(filepath).await.unwrap() {
            let get_blob_request = GetBlobRequest {
                did: session.did.clone(),
                cid: blob_cid_str.clone(),
                token: session.access_jwt.clone(),
            };
            match download_blob(agent.get_endpoint().await.as_str(), &get_blob_request).await {
                Ok(mut stream) => {
                    tracing::info!("[{}] Successfully fetched missing blob", did);
                    let mut path = std::env::current_dir().unwrap();
                    path.push(did_to_dirname(&session.did));
                    path.push(&blob_cid_str);
                    let mut file = tokio::fs::File::create(path.as_path()).await.unwrap();

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.unwrap();
                        file.write_all(&chunk).await.unwrap();
                    }

                    file.flush().await.unwrap();
                    successful_blobs.push(format!("{blob:?}"));
                }
                Err(e) => {
                    match e {
                        MigrationError::RateLimitReached => {
                            tracing::error!("[{}] Rate limit reached, waiting 5 minutes", did);
                            let five_minutes = Duration::from_secs(300);
                            tokio::time::sleep(five_minutes).await;
                        }
                        _ => {
                            //todo
                        }
                    }
                    tracing::error!("[{}] Failed to determine missing blobs", did);
                    failed_blobs.push(format!("{blob:?}"));
                }
            }
        }
    }

    Ok(ExportAllBlobsResponse {
        successful_blobs,
        failed_blobs,
    })
}
