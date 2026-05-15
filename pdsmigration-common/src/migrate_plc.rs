use crate::{
    build_agent, login_helper, recommended_plc, sign_plc, submit_plc, MigrationError, REDACTED,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
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

impl fmt::Debug for MigratePlcRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigratePlcRequest")
            .field("destination", &self.destination)
            .field("destination_token", &REDACTED)
            .field("origin", &self.origin)
            .field("did", &self.did)
            .field("origin_token", &REDACTED)
            .field("plc_signing_token", &REDACTED)
            .field(
                "user_recovery_key",
                &self.user_recovery_key.as_ref().map(|_| REDACTED),
            )
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(user_recovery_key: Option<String>) -> MigratePlcRequest {
        MigratePlcRequest {
            destination: "https://dst.example.com".to_string(),
            destination_token: "dst-secret".to_string(),
            origin: "https://src.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            origin_token: "src-secret".to_string(),
            plc_signing_token: "plc-signing-secret".to_string(),
            user_recovery_key,
        }
    }

    #[test]
    fn migrate_plc_request_redacts_all_secrets() {
        let req = sample_request(Some("recovery-secret".to_string()));
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        for secret in [
            "dst-secret",
            "src-secret",
            "plc-signing-secret",
            "recovery-secret",
        ] {
            assert!(!dbg.contains(secret), "leaked secret: {secret}");
        }
    }

    #[test]
    fn migrate_plc_request_with_no_recovery_key_still_safe() {
        let req = sample_request(None);
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(dbg.contains("None"));
    }
}
