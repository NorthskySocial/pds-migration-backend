use crate::{
    try_parse_error_response, GetBlobParams, GetBlobParamsData, GetBlobRequest, ListBlobsParams,
    ListBlobsParamsData, ListMissingBlobsParams, ListMissingBlobsParamsData, MigrationError,
    APPLICATION_JSON,
};
use bsky_sdk::api::com::atproto::repo::list_missing_blobs::RecordBlob;
use bsky_sdk::api::types::string::{Cid, Did};
use bsky_sdk::BskyAgent;
use futures_util::StreamExt;
use ipld_core::ipld::Ipld;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

const DEFAULT_BLOB_REQUEST_TIMEOUT_SECS: u64 = 120;

/// Cooldown applied when an upstream PDS reports we hit its rate limit
pub const RATE_LIMIT_COOLDOWN_SECS: u64 = 300;

pub async fn wait_for_rate_limit(did: &str, operation: &str) {
    tracing::error!(
        did = %did,
        operation = %operation,
        cooldown_secs = RATE_LIMIT_COOLDOWN_SECS,
        "Rate limit reached, waiting before retrying",
    );
    tokio::time::sleep(Duration::from_secs(RATE_LIMIT_COOLDOWN_SECS)).await;
}

/// Shared HTTP client for all blob upload/download requests.
/// reqwest holds a connection pool internally to improve performance by reusing
/// connections and avoiding setup overhead
fn blob_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .timeout(blob_request_timeout())
            .build()
            .expect("failed to build shared blob HTTP client")
    })
}

/// Resolve the blob request timeout from the `BLOB_REQUEST_TIMEOUT_SECS`
/// environment variable, falling back to the default if not set or invalid.
fn blob_request_timeout() -> Duration {
    let secs = std::env::var("BLOB_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_BLOB_REQUEST_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

#[tracing::instrument(skip(agent))]
pub async fn list_all_blobs(agent: &BskyAgent) -> Result<Vec<Cid>, MigrationError> {
    let mut result = vec![];
    let mut cursor = None;
    let mut length = None;
    let did = agent.did().await.clone().unwrap();
    let did_str = did.as_str();
    while length.is_none() || length.unwrap() >= 500 {
        let output = agent
            .api
            .com
            .atproto
            .sync
            .list_blobs(ListBlobsParams {
                data: ListBlobsParamsData {
                    cursor: cursor.clone(),
                    did: did.clone(),
                    limit: None,
                    since: None,
                },
                extra_data: Ipld::Null,
            })
            .await;
        match output {
            Ok(output) => {
                tracing::info!("[{}] {:?}", did_str, output);
                cursor = output.cursor.clone();
                length = Some(output.cids.len());
                let mut blob_cids = output.cids.clone();
                result.append(blob_cids.as_mut());
            }
            Err(e) => {
                tracing::error!("[{}] list_blobs failed: {:?}", did_str, e);
                return Err(MigrationError::Upstream {
                    message: e.to_string(),
                });
            }
        }
    }
    tracing::info!("[{}] Retrieved {} blobs total", did_str, result.len());
    Ok(result)
}

#[tracing::instrument(skip(agent))]
pub async fn missing_blobs(agent: &BskyAgent) -> Result<Vec<RecordBlob>, MigrationError> {
    let did = agent.did().await;
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    tracing::info!("[{}] Fetching missing blobs", did_str);
    let mut result: Vec<RecordBlob> = vec![];
    let mut length = None;
    let mut cursor = None;
    while length.is_none() || length.unwrap() >= 500 {
        let output = agent
            .api
            .com
            .atproto
            .repo
            .list_missing_blobs(ListMissingBlobsParams {
                data: ListMissingBlobsParamsData {
                    cursor: cursor.clone(),
                    limit: None,
                },
                extra_data: Ipld::Null,
            })
            .await
            .map_err(|error| {
                tracing::error!("[{}] list_missing_blobs failed: {:?}", did_str, error);
                MigrationError::Upstream {
                    message: error.to_string(),
                }
            })?;
        length = Some(output.blobs.len());
        let mut temp = output.blobs.clone();
        result.append(temp.as_mut());
        cursor = output.cursor.clone();
    }
    tracing::info!("[{}] Found {} missing blobs", did_str, result.len());
    Ok(result)
}

#[tracing::instrument(skip(agent))]
pub async fn get_blob(agent: &BskyAgent, cid: Cid, did: Did) -> Result<Vec<u8>, ()> {
    let did_str = did.as_str();
    let result = agent
        .api
        .com
        .atproto
        .sync
        .get_blob(GetBlobParams {
            data: GetBlobParamsData {
                cid,
                did: did.clone(),
            },
            extra_data: Ipld::Null,
        })
        .await;
    match result {
        Ok(output) => {
            tracing::debug!("[{}] Successfully fetched blob: {:?}", did_str, output);
            Ok(output.clone())
        }
        Err(e) => {
            tracing::error!("[{}] Failed to fetch blob: {:?}", did_str, e);
            Err(())
        }
    }
}

#[tracing::instrument(skip(agent))]
pub async fn upload_blob(agent: &BskyAgent, input: Vec<u8>) -> Result<(), MigrationError> {
    agent
        .api
        .com
        .atproto
        .repo
        .upload_blob(input)
        .await
        .map_err(|error| MigrationError::Runtime {
            message: error.to_string(),
        })?;
    Ok(())
}

#[tracing::instrument(skip(agent, input))]
pub async fn upload_blob_v2(
    agent: &BskyAgent,
    input: Vec<u8>,
    blob_id: &str,
) -> Result<(), MigrationError> {
    let pds_host = agent.get_endpoint().await;
    let session = agent
        .get_session()
        .await
        .ok_or_else(|| MigrationError::Runtime {
            message: "No session available for upload".to_string(),
        })?;
    let did_str = session.did.as_str();

    let client = blob_http_client();
    let url = format!("{}/xrpc/com.atproto.repo.uploadBlob", pds_host);

    tracing::debug!(
        "[{}] Uploading blob {} of size {} to {}",
        did_str,
        blob_id,
        input.len(),
        url
    );
    let result = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .bearer_auth(&session.access_jwt)
        .body(input)
        .send()
        .await;

    match result {
        Ok(output) => {
            let ratelimit_remaining = output
                .headers()
                .get("ratelimit-remaining")
                .map(|v| v.to_str().unwrap_or("1000"))
                .unwrap_or("1000")
                .parse::<i32>()
                .unwrap_or(1000);
            if ratelimit_remaining < 100 {
                tracing::error!("[{}] Ratelimit reached for blob {}", did_str, blob_id);
                return Err(MigrationError::RateLimitReached);
            }

            match output.status() {
                reqwest::StatusCode::OK => {
                    tracing::info!("[{}] Successfully uploaded blob {}", did_str, blob_id);
                    Ok(())
                }
                reqwest::StatusCode::BAD_REQUEST => {
                    let status = output.status();
                    let body = output.text().await.unwrap_or_default();
                    tracing::error!(
                        "[{}] BadRequest Error uploading blob {} (status {}): {}",
                        did_str,
                        blob_id,
                        status,
                        body
                    );
                    Err(MigrationError::BadRequest {
                        message: "BadRequest uploading blob".to_string(),
                    })
                }
                _ => {
                    let status = output.status();
                    let body = output.text().await.unwrap_or_default();
                    tracing::error!(
                        "[{}] Runtime Error uploading blob {} (status {}): {}",
                        did_str,
                        blob_id,
                        status,
                        body
                    );
                    Err(MigrationError::Upstream {
                        message: "Runtime Error uploading blob".to_string(),
                    })
                }
            }
        }
        Err(e) => {
            if e.is_timeout() || e.is_connect() || e.is_request() {
                tracing::error!(
                    "[{}] Transport Error uploading blob {}: {:?}",
                    did_str,
                    blob_id,
                    e
                );
                Err(MigrationError::Upstream {
                    message: "Transport Error uploading blob".to_string(),
                })
            } else {
                tracing::error!(
                    "[{}] Unexpected Error uploading blob {}: {:?}",
                    did_str,
                    blob_id,
                    e
                );
                Err(MigrationError::Runtime {
                    message: "Unexpected Error uploading blob".to_string(),
                })
            }
        }
    }
}

#[tracing::instrument]
pub async fn download_blob(
    pds_host: &str,
    request: &GetBlobRequest,
) -> Result<impl futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>>, MigrationError>
{
    let did_str = request.did.as_str();
    tracing::debug!("[{}] Downloading blob", did_str);
    let client = blob_http_client();
    let url = format!("{pds_host}/xrpc/com.atproto.sync.getBlob");
    let result = client
        .get(url)
        .query(&[
            ("did", request.did.as_str().to_string()),
            ("cid", request.cid.clone()),
        ])
        .header("Content-Type", APPLICATION_JSON)
        .bearer_auth(request.token.clone())
        .send()
        .await;
    match result {
        Ok(output) => {
            let ratelimit_remaining = output
                .headers()
                .get("ratelimit-remaining")
                .map(|v| v.to_str().unwrap_or("1000"))
                .unwrap_or("1000")
                .parse::<i32>()
                .unwrap_or(1000);
            if ratelimit_remaining < 100 {
                tracing::error!("[{}] Ratelimit reached", did_str);
                return Err(MigrationError::RateLimitReached);
            }

            match output.status() {
                reqwest::StatusCode::OK => {
                    tracing::info!("[{}] Successfully downloaded blob", did_str);
                    Ok(output.bytes_stream())
                }
                status => {
                    let message = try_parse_error_response(output).await;
                    tracing::error!(
                        did = %did_str,
                        cid = %request.cid,
                        status = %status,
                        error = %message,
                        "Blob download returned an error",
                    );
                    Err(MigrationError::Upstream {
                        message: format!("HTTP {status}: {message}"),
                    })
                }
            }
        }
        Err(e) => {
            tracing::error!(
                did = %did_str,
                cid = %request.cid,
                error = %e,
                error_debug = ?e,
                "Blob download request failed",
            );
            Err(MigrationError::Runtime {
                message: format!("Transport error downloading blob: {e}"),
            })
        }
    }
}

pub async fn write_blob_stream(
    path: impl AsRef<std::path::Path>,
    mut stream: impl futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
) -> Result<(), MigrationError> {
    let mut file =
        tokio::fs::File::create(path)
            .await
            .map_err(|error| MigrationError::Runtime {
                message: format!("Failed to create blob file: {error}"),
            })?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| MigrationError::Runtime {
            message: format!("Failed to read blob response: {error}"),
        })?;
        file.write_all(&chunk)
            .await
            .map_err(|error| MigrationError::Runtime {
                message: format!("Failed to write blob file: {error}"),
            })?;
    }
    file.flush()
        .await
        .map_err(|error| MigrationError::Runtime {
            message: format!("Failed to flush blob file: {error}"),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_http_client_is_shared() {
        let a = blob_http_client();
        let b = blob_http_client();
        assert!(std::ptr::eq(a, b), "shared client should be a singleton");
    }

    #[test]
    fn rate_limit_cooldown_is_five_minutes() {
        assert_eq!(RATE_LIMIT_COOLDOWN_SECS, 300);
    }
}
