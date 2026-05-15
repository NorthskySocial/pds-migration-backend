use crate::{
    build_agent, export_preferences, import_preferences, login_helper, MigrationError, REDACTED,
};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Deserialize, Serialize)]
pub struct MigratePreferencesRequest {
    pub destination: String,
    pub destination_token: String,
    pub origin: String,
    pub did: String,
    pub origin_token: String,
}

impl fmt::Debug for MigratePreferencesRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigratePreferencesRequest")
            .field("destination", &self.destination)
            .field("destination_token", &REDACTED)
            .field("origin", &self.origin)
            .field("did", &self.did)
            .field("origin_token", &REDACTED)
            .finish()
    }
}

#[tracing::instrument(skip(req))]
pub async fn migrate_preferences_api(req: MigratePreferencesRequest) -> Result<(), MigrationError> {
    let did = req.did.as_str();
    tracing::info!(
        "[{}] Starting preferences migration from {} to {}",
        did,
        req.origin,
        req.destination
    );
    let agent = build_agent().await?;
    login_helper(
        &agent,
        req.origin.as_str(),
        req.did.as_str(),
        req.origin_token.as_str(),
    )
    .await?;
    tracing::info!("[{}] Exporting preferences from origin", did);
    let preferences = export_preferences(&agent).await?;
    tracing::info!("[{}] Preferences exported; logging in to destination", did);
    login_helper(
        &agent,
        req.destination.as_str(),
        req.did.as_str(),
        req.destination_token.as_str(),
    )
    .await?;
    tracing::info!("[{}] Importing preferences into destination", did);
    import_preferences(&agent, preferences).await?;
    tracing::info!("[{}] Preferences migration completed successfully", did);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_preferences_request_redacts_both_tokens() {
        let req = MigratePreferencesRequest {
            destination: "https://dst.example.com".to_string(),
            destination_token: "dst-secret".to_string(),
            origin: "https://src.example.com".to_string(),
            did: "did:plc:abc123".to_string(),
            origin_token: "src-secret".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("dst-secret"));
        assert!(!dbg.contains("src-secret"));
        assert!(dbg.contains("https://dst.example.com"));
        assert!(dbg.contains("https://src.example.com"));
    }
}
