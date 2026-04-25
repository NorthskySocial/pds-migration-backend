use crate::{
    GetBlobParams, GetBlobParamsData, GetBlobRequest, ListBlobsParams, ListBlobsParamsData,
    ListMissingBlobsParams, ListMissingBlobsParamsData, MigrationError,
};
use bsky_sdk::api::com::atproto::repo::list_missing_blobs::RecordBlob;
use bsky_sdk::api::types::string::{Cid, Did};
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;

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
                tracing::error!("[{}] {:?}", did_str, e);
                return Err(MigrationError::Upstream {
                    message: e.to_string(),
                });
            }
        }
    }
    Ok(result)
}

#[tracing::instrument(skip(agent))]
pub async fn missing_blobs(agent: &BskyAgent) -> Result<Vec<RecordBlob>, MigrationError> {
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
            .map_err(|error| MigrationError::Upstream {
                message: error.to_string(),
            })?;
        length = Some(output.blobs.len());
        let mut temp = output.blobs.clone();
        result.append(temp.as_mut());
        cursor = output.cursor.clone();
    }
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
                did: did.parse().unwrap(),
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

    let client = reqwest::Client::new();
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
                    tracing::error!(
                        "[{}] BadRequest Error uploading blob {}: {:?}",
                        did_str,
                        blob_id,
                        output
                    );
                    tracing::error!(
                        "[{}] Response body for blob {}: {:?}",
                        did_str,
                        blob_id,
                        output.text().await
                    );
                    Err(MigrationError::Upstream {
                        message: "BadRequest uploading blob".to_string(),
                    })
                }
                _ => {
                    tracing::error!(
                        "[{}] Runtime Error uploading blob {}: {:?}",
                        did_str,
                        blob_id,
                        output
                    );
                    tracing::error!(
                        "[{}] Response body for blob {}: {:?}",
                        did_str,
                        blob_id,
                        output.text().await
                    );
                    Err(MigrationError::Upstream {
                        message: "Runtime Error uploading blob".to_string(),
                    })
                }
            }
        }
        Err(e) => {
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

#[tracing::instrument]
pub async fn download_blob(
    pds_host: &str,
    request: &GetBlobRequest,
) -> Result<impl futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>>, MigrationError>
{
    let did_str = request.did.as_str();
    tracing::debug!("[{}] Downloading blob", did_str);
    let client = reqwest::Client::new();
    let url = format!("{pds_host}/xrpc/com.atproto.sync.getBlob");
    let result = client
        .get(url)
        .query(&[
            ("did", request.did.as_str().to_string()),
            ("cid", request.cid.clone()),
        ])
        .header("Content-Type", "application/json")
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
                reqwest::StatusCode::BAD_REQUEST => {
                    tracing::error!(
                        "[{}] BadRequest Error downloading blob: {:?}",
                        did_str,
                        output
                    );
                    Err(MigrationError::Upstream {
                        message: "BadRequest downloading blob".to_string(),
                    })
                }
                _ => {
                    tracing::error!("[{}] Runtime Error downloading blob: {:?}", did_str, output);
                    Err(MigrationError::Upstream {
                        message: "Runtime Error downloading blob".to_string(),
                    })
                }
            }
        }
        Err(e) => {
            tracing::error!("[{}] Unexpected Error downloading blob: {:?}", did_str, e);
            Err(MigrationError::Runtime {
                message: "Unexpected Error downloading blob".to_string(),
            })
        }
    }
}
