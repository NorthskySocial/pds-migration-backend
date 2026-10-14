use super::with_rate_limit_bypass_header;
use crate::{GetRepoRequest, MigrationError, APPLICATION_JSON};
use bsky_sdk::BskyAgent;

#[tracing::instrument]
pub async fn download_repo(
    pds_host: &str,
    request: &GetRepoRequest,
) -> Result<impl futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>>, MigrationError>
{
    let client = reqwest::Client::new();

    let url = format!("{pds_host}/xrpc/com.atproto.sync.getRepo");
    let result = client
        .get(url)
        .query(&[("did", request.did.as_str().to_string())])
        .header("Content-Type", APPLICATION_JSON)
        .bearer_auth(request.token.clone())
        .send()
        .await;
    match result {
        Ok(output) => {
            let ratelimit_remaining = match output.headers().get("ratelimit-remaining") {
                None => 1000,
                Some(rate_limit_remaining) => rate_limit_remaining
                    .to_str()
                    .unwrap_or("1000")
                    .parse::<i32>()
                    .unwrap_or(1000),
            };
            if ratelimit_remaining < 100 {
                tracing::error!("[{}] Ratelimit reached", request.did.as_str());
                return Err(MigrationError::RateLimitReached);
            }

            match output.status() {
                reqwest::StatusCode::OK => {
                    tracing::info!("[{}] Started downloading Repo", request.did.as_str());
                    Ok(output.bytes_stream())
                }
                _ => {
                    tracing::error!(
                        "[{}] Runtime Error downloading Repo: {:?}",
                        request.did.as_str(),
                        output
                    );
                    Err(MigrationError::Upstream {
                        message: "Runtime Error downloading Repo".to_string(),
                    })
                }
            }
        }
        Err(e) => {
            tracing::error!(
                "[{}] Unexpected Error downloading Repo: {:?}",
                request.did.as_str(),
                e
            );
            Err(MigrationError::Runtime {
                message: "Unexpected Error downloading Repo".to_string(),
            })
        }
    }
}

#[tracing::instrument(skip(agent))]
pub async fn account_import(agent: &BskyAgent, filepath: &str) -> Result<(), MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    let repo_bytes = tokio::fs::read(filepath).await.unwrap();
    tracing::info!(
        "[{}] Importing repo file {} ({} bytes)",
        did_str,
        filepath,
        repo_bytes.len()
    );
    let session = agent
        .get_session()
        .await
        .ok_or_else(|| MigrationError::Runtime {
            message: "No session available for repo import".to_string(),
        })?;
    let pds_host = agent.get_endpoint().await;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30 * 60))
        .build()
        .map_err(|error| MigrationError::Runtime {
            message: error.to_string(),
        })?;
    let response = with_rate_limit_bypass_header(
        client
            .post(format!("{pds_host}/xrpc/com.atproto.repo.importRepo"))
            .header("Content-Type", "application/vnd.ipld.car")
            .bearer_auth(&session.access_jwt)
            .body(repo_bytes),
    )
    .send()
    .await
    .map_err(|error| MigrationError::Runtime {
        message: error.to_string(),
    })?;
    if !response.status().is_success() {
        tracing::error!("[{}] Failed to import repo: {}", did_str, response.status());
        return Err(MigrationError::Runtime {
            message: format!("Repo import failed with HTTP {}", response.status()),
        });
    }
    tracing::info!("[{}] Successfully imported repo from {}", did_str, filepath);
    Ok(())
}
