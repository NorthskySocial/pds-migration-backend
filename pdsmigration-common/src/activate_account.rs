use crate::agent::login_helper;
use crate::{build_agent, MigrationError};

#[tracing::instrument(skip(token))]
pub async fn activate_account(
    pds_host: &str,
    did: &str,
    token: &str,
) -> Result<(), MigrationError> {
    tracing::info!("[{}] Starting account activation on {}", did, pds_host);
    let agent = build_agent().await?;
    login_helper(&agent, pds_host, did, token).await?;
    agent
        .api
        .com
        .atproto
        .server
        .activate_account()
        .await
        .map_err(|error| {
            tracing::error!(
                "[{}] Failed to activate account on {}: {}",
                did,
                pds_host,
                error
            );
            MigrationError::Upstream {
                message: error.to_string(),
            }
        })?;
    tracing::info!("[{}] Successfully activated account on {}", did, pds_host);
    Ok(())
}
