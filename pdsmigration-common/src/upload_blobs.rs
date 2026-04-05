use crate::agent::{login_helper, upload_blob};
use crate::{build_agent, MigrationError};
use bsky_sdk::api::agent::Configure;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct UploadBlobsRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

#[tracing::instrument]
pub async fn upload_blobs_api(req: UploadBlobsRequest) -> Result<(), MigrationError> {
    let agent = build_agent().await?;
    agent.configure_endpoint(req.pds_host.clone());
    let session = login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;

    let mut blob_dir;
    let mut path = std::env::current_dir().unwrap();
    path.push(session.did.as_str().replace(":", "-"));
    match tokio::fs::read_dir(path.as_path()).await {
        Ok(output) => blob_dir = output,
        Err(error) => {
            tracing::error!("{}", error.to_string());
            return Err(MigrationError::Runtime {
                message: "Failed to read blob directory".to_string(),
            });
        }
    }

    while let Some(blob) = blob_dir.next_entry().await.map_err(|error| {
        tracing::error!("{}", error.to_string());
        MigrationError::Runtime {
            message: "Failed to get next blob".to_string(),
        }
    })? {
        let file = tokio::fs::read(blob.path()).await.map_err(|error| {
            tracing::error!("{}", error.to_string());
            MigrationError::Runtime {
                message: "Failed to read next blob".to_string(),
            }
        })?;
        tracing::debug!("Uploading blob: {:?} with size {}...", blob.file_name(), file.len());
        upload_blob(&agent, file).await?;
    }

    tracing::info!("Finished uploading blobs for DID {}", session.did);
    Ok(())
}
