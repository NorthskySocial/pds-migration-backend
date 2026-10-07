use crate::{
    build_agent, export_preferences, import_preferences, login_helper, MigrationError, REDACTED,
};
use bsky_sdk::api::app::bsky::actor::defs::Preferences;
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

#[tracing::instrument(skip(req), fields(did = %req.did, origin = %req.origin, destination = %req.destination))]
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
    match serde_json::to_vec(&PreferencesPayload {
        preferences: &preferences,
    }) {
        Ok(payload) => {
            let details = preferences_payload_details(&payload);
            tracing::info!(
                "[{}] Preferences payload with bytes={}; valid_json={}; has_preferences_key={}; preference_count={}; error={:?}",
                did,
                details.payload_bytes,
                details.valid_json,
                details.has_preferences_key,
                details.preference_count,
                details.error
            );
        }
        Err(error) => tracing::error!(
            "[{}] Failed to serialize preferences payload to get details: {}",
            did,
            error
        ),
    }

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

#[derive(Serialize)]
struct PreferencesPayload<'a> {
    preferences: &'a Preferences,
}

#[derive(Debug)]
struct PreferencesPayloadDetails {
    payload_bytes: usize,
    valid_json: bool,
    has_preferences_key: bool,
    preference_count: usize,
    error: Option<String>,
}

fn preferences_payload_details(payload: &[u8]) -> PreferencesPayloadDetails {
    let payload_bytes = payload.len();
    let value = match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(value) => value,
        Err(error) => {
            return PreferencesPayloadDetails {
                payload_bytes,
                valid_json: false,
                has_preferences_key: false,
                preference_count: 0,
                error: Some(error.to_string()),
            };
        }
    };

    let Some(items) = value
        .get("preferences")
        .and_then(serde_json::Value::as_array)
    else {
        return PreferencesPayloadDetails {
            payload_bytes,
            valid_json: true,
            has_preferences_key: false,
            preference_count: 0,
            error: Some("missing preferences array".to_string()),
        };
    };

    PreferencesPayloadDetails {
        payload_bytes,
        valid_json: true,
        has_preferences_key: true,
        preference_count: items.len(),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_payload_details_reports_payload_sizes() {
        let payload =
            "{\"preferences\":[{\"$type\":\"feedPref\",\"value\":\"caf\u{e9} private\"}]}";
        let payload = payload.as_bytes();

        let details = preferences_payload_details(payload);

        assert_eq!(details.payload_bytes, payload.len());
        assert!(details.valid_json);
        assert!(details.has_preferences_key);
        assert_eq!(details.preference_count, 1);
        assert!(!format!("{:?}", details).contains("private"));
        assert_eq!(details.error, None);
    }

    #[test]
    fn preferences_payload_details_reports_invalid_json() {
        let details = preferences_payload_details(b"not json");

        assert_eq!(details.payload_bytes, 8);
        assert!(!details.valid_json);
        assert!(!details.has_preferences_key);
        assert_eq!(details.preference_count, 0);
        assert!(details.error.is_some());
    }

    #[test]
    fn preferences_payload_details_reports_missing_preferences_array() {
        let details = preferences_payload_details(br#"{"other":[]}"#);

        assert!(details.valid_json);
        assert!(!details.has_preferences_key);
        assert_eq!(details.preference_count, 0);
        assert_eq!(details.error.as_deref(), Some("missing preferences array"));
    }

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
