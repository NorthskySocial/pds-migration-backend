use crate::{build_agent, login_helper, recommended_plc, sign_plc, submit_plc, MigrationError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct MigratePlcRequest {
    pub destination: String,
    pub destination_token: String,
    pub origin: String,
    pub did: String,
    pub origin_token: String,
    pub plc_signing_token: String,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub user_recovery_key: Option<String>,
}

#[tracing::instrument(skip(req))]
pub async fn migrate_plc_api(req: MigratePlcRequest) -> Result<(), MigrationError> {
    let did = req.did.as_str();
    tracing::info!(
        "[{}] Starting PLC migration from {} to {}",
        did,
        req.origin,
        req.destination
    );
    let agent = build_agent().await?;
    tracing::debug!("[{}] Logging in to destination {}", did, req.destination);
    login_helper(
        &agent,
        req.destination.as_str(),
        req.did.as_str(),
        req.destination_token.as_str(),
    )
    .await?;
    tracing::info!("[{}] Fetching recommended PLC credentials", did);
    let recommended_did = recommended_plc(&agent).await?;
    use bsky_sdk::api::com::atproto::identity::sign_plc_operation::InputData;

    let mut rotation_keys = recommended_did.rotation_keys.unwrap();

    if let Some(recovery_key) = &req.user_recovery_key {
        tracing::info!("[{}] Inserting user recovery key into rotation keys", did);
        rotation_keys.insert(0, recovery_key.clone());
    }

    let new_plc = InputData {
        also_known_as: recommended_did.also_known_as,
        rotation_keys: Some(rotation_keys),
        services: recommended_did.services,
        token: Some(req.plc_signing_token.clone()),
        verification_methods: recommended_did.verification_methods,
    };
    tracing::debug!(
        "[{}] Logging in to origin {} to sign PLC op",
        did,
        req.origin
    );
    login_helper(
        &agent,
        req.origin.as_str(),
        req.did.as_str(),
        req.origin_token.as_str(),
    )
    .await?;
    tracing::info!("[{}] Signing PLC operation", did);
    let output = sign_plc(&agent, new_plc.clone()).await?;
    tracing::info!("[{}] PLC operation signed; submitting to destination", did);
    login_helper(
        &agent,
        req.destination.as_str(),
        req.did.as_str(),
        req.destination_token.as_str(),
    )
    .await?;
    submit_plc(&agent, output).await?;
    tracing::info!("[{}] PLC migration completed successfully", did);
    Ok(())
}
