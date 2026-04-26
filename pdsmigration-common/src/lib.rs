use bsky_sdk::api::types::string::Did;
use multibase::Base::Base58Btc;
use secp256k1::PublicKey;
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
mod missing_blobs;
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
pub use missing_blobs::*;
pub use request_token::*;
pub use service_auth::*;
pub use upload_blobs::*;

#[derive(Deserialize, Serialize)]
pub struct GetRepoRequest {
    pub did: Did,
    pub token: String,
}

impl std::fmt::Debug for GetRepoRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetRepoRequest")
            .field("did", &self.did)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

pub fn multicodec_wrap(bytes: Vec<u8>) -> Vec<u8> {
    let mut buf = [0u8; 3];
    unsigned_varint::encode::u16(0xe7, &mut buf);
    let mut v: Vec<u8> = Vec::new();
    for b in &buf {
        v.push(*b);
        // varint uses first bit to indicate another byte follows, stop if not the case
        if *b <= 127 {
            break;
        }
    }
    v.extend(bytes);
    v
}

pub fn public_key_to_did_key(public_key: PublicKey) -> String {
    let pk_compact = public_key.serialize();
    let pk_wrapped = multicodec_wrap(pk_compact.to_vec());
    let pk_multibase = multibase::encode(Base58Btc, pk_wrapped.as_slice());
    let public_key_str = format!("did:key:{pk_multibase}");
    public_key_str
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

/// Build the canonical on-disk directory path for a DID's blobs:
/// `<current_dir>/<did-with-colons-replaced>`.
pub fn did_blobs_path<D: AsRef<str>>(did: D) -> Result<std::path::PathBuf, MigrationError> {
    let mut path = std::env::current_dir().map_err(|e| MigrationError::Runtime {
        message: e.to_string(),
    })?;
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
    fn did_blob_dir_appends_dirname_to_cwd() {
        let cwd = std::env::current_dir().expect("cwd should be readable in tests");
        let path = did_blobs_path("did:plc:abc123").expect("cwd should be readable in tests");
        assert_eq!(path, cwd.join("did-plc-abc123"));
    }

    #[test]
    fn did_blob_dir_accepts_did_type() {
        let cwd = std::env::current_dir().expect("cwd should be readable in tests");
        let did = Did::new("did:plc:abc123".to_string()).expect("valid test DID");
        let path = did_blobs_path(&did).expect("cwd should be readable in tests");
        assert_eq!(path, cwd.join("did-plc-abc123"));
    }
}
