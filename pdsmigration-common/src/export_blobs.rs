use crate::agent::{download_blob, login_helper, missing_blobs};
use crate::export_all_blobs::GetBlobRequest;
use crate::{build_agent, MigrationError};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Deserialize, Serialize)]
pub struct ExportBlobsRequest {
    pub destination: String,
    pub origin: String,
    pub did: String,
    pub origin_token: String,
    pub destination_token: String,
    pub is_missing_blob_request: bool,
}

impl std::fmt::Debug for ExportBlobsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportBlobsRequest")
            .field("destination", &self.destination)
            .field("origin", &self.origin)
            .field("did", &self.did)
            .field("origin_token", &"[REDACTED]")
            .field("destination_token", &"[REDACTED]")
            .field("is_missing_blob_request", &self.is_missing_blob_request)
            .finish()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExportBlobsResponse {
    pub successful_blobs: Vec<String>,
    pub invalid_blobs: Vec<String>,
}

#[tracing::instrument]
pub async fn export_blobs_api(
    req: ExportBlobsRequest,
) -> Result<ExportBlobsResponse, MigrationError> {
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.destination.as_str(),
        req.did.as_str(),
        req.destination_token.as_str(),
    )
    .await?;
    let missing_blobs = missing_blobs(&agent).await?;
    let session = login_helper(
        &agent,
        req.origin.as_str(),
        req.did.as_str(),
        req.origin_token.as_str(),
    )
    .await?;
    let did = session.did.as_str();

    // Initialize collections to track successful and failed blob IDs
    let mut successful_blobs = Vec::new();
    let mut invalid_blobs = Vec::new();
    let mut path = match std::env::current_dir() {
        Ok(path) => path,
        Err(e) => {
            return Err(MigrationError::Runtime {
                message: e.to_string(),
            })
        }
    };
    path.push(did.replace(":", "-"));

    if req.is_missing_blob_request {
        if let Err(e) = tokio::fs::remove_dir_all(path.as_path()).await {
            if e.kind() != ErrorKind::NotFound {
                return Err(MigrationError::Runtime {
                    message: format!("Failed to clean directory: {}", e),
                });
            }
        }
        tracing::info!("[{}] Cleaned directory for missing blob request", did);
    }

    match tokio::fs::create_dir(path.as_path()).await {
        Ok(_) => {
            tracing::info!("[{}] Successfully created directory", did);
        }
        Err(e) => {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(MigrationError::Runtime {
                    message: format!("{}", e),
                });
            }
        }
    }
    for missing_blob in &missing_blobs {
        tracing::debug!("[{}] Missing blob: {:?}", did, missing_blob);
        let session = match agent.get_session().await {
            Some(session) => session,
            None => {
                return Err(MigrationError::Runtime {
                    message: "Failed to get session".to_string(),
                });
            }
        };
        let mut filepath = match std::env::current_dir() {
            Ok(res) => res,
            Err(e) => {
                return Err(MigrationError::Runtime {
                    message: e.to_string(),
                });
            }
        };
        filepath.push(session.did.as_str().replace(":", "-"));
        filepath.push(
            missing_blob
                .record_uri
                .as_str()
                .split("/")
                .last()
                .unwrap_or("fallback"),
        );
        if !tokio::fs::try_exists(filepath).await.unwrap() {
            let missing_blob_cid = missing_blob.cid.clone();
            let blob_cid_str = format!("{missing_blob_cid:?}")
                .strip_prefix("Cid(Cid(")
                .unwrap()
                .strip_suffix("))")
                .unwrap()
                .to_string();
            let get_blob_request = GetBlobRequest {
                did: session.did.clone(),
                cid: blob_cid_str.clone(),
                token: session.access_jwt.clone(),
            };
            match download_blob(agent.get_endpoint().await.as_str(), &get_blob_request).await {
                Ok(mut stream) => {
                    tracing::info!("[{}] Successfully fetched missing blob", did);
                    let mut path = std::env::current_dir().unwrap();
                    path.push(session.did.as_str().replace(":", "-"));
                    path.push(&blob_cid_str);
                    let mut file = tokio::fs::File::create(path.as_path()).await.unwrap();

                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.unwrap();
                        file.write_all(&chunk).await.unwrap();
                    }

                    file.flush().await.unwrap();
                    successful_blobs.push(blob_cid_str);
                }
                Err(e) => {
                    match e {
                        MigrationError::RateLimitReached => {
                            tracing::error!("[{}] Rate limit reached, waiting 5 minutes", did);
                            let five_minutes = Duration::from_secs(300);
                            tokio::time::sleep(five_minutes).await;
                        }
                        _ => {
                            tracing::error!("[{}] Failed to determine missing blobs", did);
                            return Err(MigrationError::Runtime {
                                message: e.to_string(),
                            });
                        }
                    }
                    tracing::error!("[{}] Failed to determine missing blobs", did);
                    invalid_blobs.push(blob_cid_str);
                }
            }
        }
    }
    Ok(ExportBlobsResponse {
        successful_blobs,
        invalid_blobs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_blobs_request_redacts_tokens() {
        let request = ExportBlobsRequest {
            destination: "https://destination.example.com".to_string(),
            origin: "https://origin.example.com".to_string(),
            did: "did:plc:example123".to_string(),
            origin_token: "secret-origin-token-12345".to_string(),
            destination_token: "secret-destination-token-67890".to_string(),
            is_missing_blob_request: false,
        };

        let debug_output = format!("{:?}", request);

        // Verify that the sensitive tokens are redacted
        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("secret-origin-token-12345"));
        assert!(!debug_output.contains("secret-destination-token-67890"));

        // Verify that non-sensitive fields are still visible
        assert!(debug_output.contains("https://destination.example.com"));
        assert!(debug_output.contains("https://origin.example.com"));
        assert!(debug_output.contains("did:plc:example123"));
    }
}
