use crate::agent::{download_repo, login_helper};
use crate::{build_agent, did_to_car_filename, GetRepoRequest, MigrationError};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Deserialize, Serialize)]
pub struct ExportPDSRequest {
    pub pds_host: String,
    pub did: String,
    pub token: String,
}

impl std::fmt::Debug for ExportPDSRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportPDSRequest")
            .field("pds_host", &self.pds_host)
            .field("did", &self.did)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

#[tracing::instrument(skip(req))]
pub async fn export_pds_api(req: ExportPDSRequest) -> Result<(), MigrationError> {
    let agent = build_agent().await?;
    let session = login_helper(
        &agent,
        req.pds_host.as_str(),
        req.did.as_str(),
        req.token.as_str(),
    )
    .await?;
    let did = session.did.as_str();
    let get_repo_request = GetRepoRequest {
        did: session.did.clone(),
        token: session.access_jwt.clone(),
    };
    match download_repo(agent.get_endpoint().await.as_str(), &get_repo_request).await {
        Ok(mut stream) => {
            let mut path = std::env::current_dir().map_err(|error| {
                tracing::error!("[{}] Failed to get current directory: {}", did, error);
                MigrationError::Runtime {
                    message: "Failed to get current directory".to_string(),
                }
            })?;
            path.push(did_to_car_filename(&session.did));

            let mut file = tokio::fs::File::create(path.as_path())
                .await
                .map_err(|error| MigrationError::Runtime {
                    message: format!(
                        "Failed to create file {}, with error {}",
                        path.display(),
                        error
                    ),
                })?;

            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| {
                    tracing::error!("[{}] Failed to read stream chunk: {}", did, error);
                    MigrationError::Runtime {
                        message: "Failed to read stream chunk".to_string(),
                    }
                })?;
                file.write_all(&chunk).await.map_err(|error| {
                    tracing::error!("[{}] Failed to write chunk to file: {}", did, error);
                    MigrationError::Runtime {
                        message: "Failed to write chunk to file".to_string(),
                    }
                })?;
            }
            file.flush().await.map_err(|error| {
                tracing::error!("[{}] Failed to flush file: {}", did, error);
                MigrationError::Runtime {
                    message: "Failed to flush file".to_string(),
                }
            })?;
            tracing::info!(
                "[{}] Successfully exported repository to {}",
                did,
                path.display()
            );
            return Ok(());
        }
        Err(e) => {
            match e {
                MigrationError::RateLimitReached => {
                    tracing::error!("[{}] Rate limit reached, waiting 5 minutes", did);
                    let five_minutes = Duration::from_secs(300);
                    tokio::time::sleep(five_minutes).await;
                }
                _ => {
                    tracing::error!("[{}] Failed to download repo", did);
                    //todo
                }
            }
            tracing::error!("[{}] Failed to download Repo", did);
        }
    }
    Ok(())
}
