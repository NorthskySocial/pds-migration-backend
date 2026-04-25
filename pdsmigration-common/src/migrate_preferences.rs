use crate::{build_agent, export_preferences, import_preferences, login_helper, MigrationError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct MigratePreferencesRequest {
    pub destination: String,
    pub destination_token: String,
    pub origin: String,
    pub did: String,
    pub origin_token: String,
}

#[tracing::instrument]
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
