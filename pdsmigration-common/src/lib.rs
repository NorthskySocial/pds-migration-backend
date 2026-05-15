use bsky_sdk::api::types::string::Did;
use serde::{Deserialize, Serialize};

mod activate_account;
mod agent;
mod create_account;
mod deactivate_account;
mod errors;
mod export_all_blobs;
mod export_blobs;
mod export_pds;
mod import_pds;
mod migrate_plc;
mod migrate_preferences;
mod request_token;
mod service_auth;
mod upload_blobs;

pub use activate_account::*;
pub use agent::*;
pub use create_account::*;
pub use deactivate_account::*;
pub use errors::*;
pub use export_all_blobs::*;
pub use export_blobs::*;
pub use export_pds::*;
pub use import_pds::*;
pub use migrate_plc::*;
pub use migrate_preferences::*;
pub use request_token::*;
pub use service_auth::*;
pub use upload_blobs::*;

pub const REDACTED: &str = "[REDACTED]";
pub const APPLICATION_JSON: &str = "application/json";

#[derive(Deserialize, Serialize)]
pub struct GetRepoRequest {
    pub did: Did,
    pub token: String,
}

impl std::fmt::Debug for GetRepoRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetRepoRequest")
            .field("did", &self.did)
            .field("token", &REDACTED)
            .finish()
    }
}

/// Convert a DID into a canonical CAR filename for repository
/// exports / imports (`<did-with-colons-replaced>.car`).
pub fn did_to_car_filename<D: AsRef<str>>(did: D) -> String {
    let mut name = did_to_dirname(did);
    name.push_str(".car");
    name
}

/// Normalize a CID representation Cid(Cid(<inner>)) into
/// its inner ID as a string.
pub fn format_cid<T: std::fmt::Debug>(cid: &T) -> String {
    let raw = format!("{cid:?}");
    raw.strip_prefix("Cid(Cid(")
        .and_then(|s| s.strip_suffix("))"))
        .map(str::to_string)
        .unwrap_or(raw)
}

/// Resolve the downloads directory used to store exported repo CAR
/// files and blob directories. We use the directory containing the
/// running executable for the migration tool, or the current working
/// directory as a fallback.
/// Platform-specific: if we use `std::env::current_exe()` on macOS,
/// the path would resolve to the user's home directory.
pub fn downloads_dir() -> Result<std::path::PathBuf, MigrationError> {
    match std::env::current_exe() {
        Ok(exe_path) => match exe_path.parent() {
            Some(parent) => Ok(parent.to_path_buf()),
            None => std::env::current_dir().map_err(|e| MigrationError::Runtime {
                message: e.to_string(),
            }),
        },
        Err(_) => std::env::current_dir().map_err(|e| MigrationError::Runtime {
            message: e.to_string(),
        }),
    }
}

/// Build the canonical on-disk directory path for a DID's blobs:
/// `<downloads_dir>/<did-with-colons-replaced>`.
pub fn did_blobs_path<D: AsRef<str>>(did: D) -> Result<std::path::PathBuf, MigrationError> {
    let mut path = downloads_dir()?;
    path.push(did_to_dirname(did));
    Ok(path)
}

/// Convert a DID into the directory / file basename used on disk.
fn did_to_dirname<D: AsRef<str>>(did: D) -> String {
    did.as_ref().replace(':', "-")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsky_sdk::api::types::string::{Cid, Did};
    use std::str::FromStr;

    #[test]
    fn dirname_replaces_colons() {
        assert_eq!(
            did_to_dirname("did:plc:abc123"),
            "did-plc-abc123".to_string()
        );
    }

    #[test]
    fn dirname_accepts_did_type() {
        let did = Did::new("did:plc:abc123".to_string()).expect("valid test DID");
        assert_eq!(did_to_dirname(&did), "did-plc-abc123".to_string());
    }

    #[test]
    fn dirname_passthrough_when_no_colon() {
        assert_eq!(did_to_dirname("plain"), "plain".to_string());
    }

    #[test]
    fn dirname_handles_empty_string() {
        assert_eq!(did_to_dirname(""), "".to_string());
    }

    #[test]
    fn car_filename_appends_extension() {
        assert_eq!(
            did_to_car_filename("did:plc:abc123"),
            "did-plc-abc123.car".to_string()
        );
    }

    #[test]
    fn car_filename_for_web_did() {
        assert_eq!(
            did_to_car_filename("did:web:example.com"),
            "did-web-example.com.car".to_string()
        );
    }

    #[test]
    fn car_filename_accepts_did_type() {
        let did = Did::new("did:plc:abc123".to_string()).expect("valid test DID");
        assert_eq!(did_to_car_filename(&did), "did-plc-abc123.car".to_string());
    }

    #[test]
    fn format_cid_strips_double_wrapping() {
        let raw = "bafyreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
        let cid = Cid::from_str(raw).expect("valid test CID");
        assert_eq!(format!("{cid:?}"), format!("Cid(Cid({raw}))"));
        assert_eq!(format_cid(&cid), raw.to_string());
    }

    #[test]
    fn format_cid_returns_raw_when_unwrapping_fails() {
        let value = "not-a-cid";
        assert_eq!(format_cid(&value), format!("{value:?}"));
    }

    #[test]
    fn did_blobs_path_appends_dirname_to_base() {
        let base = downloads_dir().expect("downloads dir should be readable in tests");
        let path =
            did_blobs_path("did:plc:abc123").expect("downloads dir should be readable in tests");
        assert_eq!(path, base.join("did-plc-abc123"));
    }

    #[test]
    fn did_blobs_path_accepts_did_type() {
        let base = downloads_dir().expect("downloads dir should be readable in tests");
        let did = Did::new("did:plc:abc123".to_string()).expect("valid test DID");
        let path = did_blobs_path(&did).expect("downloads dir should be readable in tests");
        assert_eq!(path, base.join("did-plc-abc123"));
    }

    #[test]
    fn get_repo_request_redacts_token() {
        let did = Did::new("did:plc:abc123".to_string()).expect("valid test DID");
        let req = GetRepoRequest {
            did,
            token: "supersecret-jwt".to_string(),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        assert!(!dbg.contains("supersecret-jwt"));
    }
}
