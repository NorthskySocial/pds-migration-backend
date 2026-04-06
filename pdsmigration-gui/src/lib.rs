use crate::agent::login_helper2;
use crate::app::PdsMigrationApp;
use crate::errors::GuiError;
use crate::ipld::cid_for_cbor;
use crate::session::session_config::{PdsSession, SessionConfig};
use base64ct::{Base64, Encoding};
use bsky_sdk::api::agent::Configure;
use bsky_sdk::api::types::string::Did;
use bsky_sdk::BskyAgent;
use hex::ToHex;
use indexmap::IndexMap;
use multibase::Base::Base58Btc;
use pdsmigration_common::{
    CreateAccountRequest, DeactivateAccountRequest, ExportAllBlobsRequest, ExportBlobsRequest,
    ExportPDSRequest, ImportPDSRequest, MigratePlcRequest, MigratePreferencesRequest,
    MigrationError, PlcOperation, RequestTokenRequest, ServiceAuthRequest, UploadBlobsRequest,
};
use rand::distr::Alphanumeric;
use rand::Rng;
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use zip::write::SimpleFileOptions;
use zip::{AesMode, ZipWriter};

pub mod agent;
pub mod app;
pub mod error_window;
pub mod errors;
pub mod ipld;
pub mod log_viewer;
pub mod screens;
pub mod session;
pub mod styles;
pub mod success_window;

#[derive(PartialEq, Clone)]
pub enum ScreenType {
    Basic,
    Advanced,
    OldLogin,
    AccountCreate,
    MigratePLC,
    Success,
    ExportBlobs,
    ImportBlobs,
    MigratePreferences,
    ActiveAccounts,
    CreateOrLoginAccount,
    ExportRepo,
    ImportRepo,
}

#[tracing::instrument(skip(session_config))]
pub async fn activate_account(session_config: SessionConfig) -> Result<(), GuiError> {
    let pds_host = session_config.host().to_string();
    let token = session_config.access_token().to_string();
    let did = session_config.did().to_string();

    tracing::info!("Activating Account started");
    match pdsmigration_common::activate_account(pds_host.as_str(), token.as_str(), did.as_str())
        .await
    {
        Ok(_) => {
            tracing::info!("Activating Account completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error activating account: {pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument(skip(session_config))]
pub async fn deactivate_account(session_config: SessionConfig) -> Result<(), GuiError> {
    let pds_host = session_config.host().to_string();
    let token = session_config.access_token().to_string();
    let did = session_config.did().to_string();

    tracing::info!("Deactivating Account started");
    let request = DeactivateAccountRequest {
        pds_host,
        did,
        token,
    };
    match pdsmigration_common::deactivate_account_api(request).await {
        Ok(_) => {
            tracing::info!("Deactivating Account completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error deactivating account: {pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument]
pub fn generate_recovery_key(user_recovery_key_password: String) -> Result<String, GuiError> {
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand::rng());
    let pk_compact = public_key.serialize();
    let pk_wrapped = multicodec_wrap(pk_compact.to_vec());
    let pk_multibase = multibase::encode(Base58Btc, pk_wrapped.as_slice());
    let public_key_str = format!("did:key:{pk_multibase}");

    let sk_compact = secret_key.secret_bytes().to_vec();
    let sk_str = secret_key.secret_bytes().encode_hex::<String>();
    let sk_wrapped = multicodec_wrap(sk_compact.to_vec());
    let sk_multibase = multibase::encode(Base58Btc, sk_wrapped.as_slice());
    let _secret_key_str = format!("did:key:{sk_multibase}");

    let path = std::path::Path::new("RotationKey.zip");
    let file = match std::fs::File::create(path) {
        Ok(file) => file,
        Err(e) => {
            tracing::error!("Error creating file: {e}");
            return Err(GuiError::Runtime);
        }
    };

    let mut zip = ZipWriter::new(file);

    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .with_aes_encryption(AesMode::Aes256, user_recovery_key_password.as_str());
    match zip.start_file("RotationKey", options) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error starting file: {e}");
            return Err(GuiError::Runtime);
        }
    }
    match zip.write_all(sk_str.as_bytes()) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error writing file: {e}");
            return Err(GuiError::Runtime);
        }
    }

    match zip.finish() {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error finishing file: {e}");
            return Err(GuiError::Runtime);
        }
    }
    Ok(public_key_str)
}

#[tracing::instrument]
pub async fn generate_signing_key() -> (String, String) {
    let secp = Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut rand::rng());
    let pk_compact = public_key.serialize();
    let pk_wrapped = multicodec_wrap(pk_compact.to_vec());
    let pk_multibase = multibase::encode(Base58Btc, pk_wrapped.as_slice());
    let public_key_str = format!("did:key:{pk_multibase}");

    let sk_compact = secret_key.secret_bytes().to_vec();
    let sk_wrapped = multicodec_wrap(sk_compact.to_vec());
    let sk_multibase = multibase::encode(Base58Btc, sk_wrapped.as_slice());
    let secret_key_str = format!("did:key:{sk_multibase}");

    let path = std::path::Path::new("SigningKeypair.zip");
    let file = match std::fs::File::create(path) {
        Ok(file) => file,
        Err(e) => {
            tracing::error!("Error creating file: {e}");
            panic!("Error creating file: {e}");
        }
    };

    let mut zip = ZipWriter::new(file);

    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

    match zip.start_file("SigningKeypair", options) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error starting file: {e}");
            panic!("Error starting file: {e}");
        }
    }
    match zip.write_all(secret_key_str.as_bytes()) {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error writing file: {e}");
            panic!("Error writing file: {e}");
        }
    }

    match zip.finish() {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error finishing file: {e}");
            panic!("Error finishing file: {e}");
        }
    }
    (public_key_str, secret_key_str)
}

#[tracing::instrument(skip(session_config))]
pub async fn request_token(session_config: SessionConfig) -> Result<(), GuiError> {
    let pds_host = session_config.host().to_string();
    let token = session_config.access_token().to_string();
    let did = session_config.did().to_string();

    tracing::info!("Requesting Token started");
    let request = RequestTokenRequest {
        pds_host,
        did,
        token,
    };
    match pdsmigration_common::request_token_api(request).await {
        Ok(_) => {
            tracing::info!("Requesting Token completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error requesting token: {pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn migrate_preferences(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let new_session_config = match &pds_session.new_session_config() {
        None => {
            tracing::error!("No new session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let origin = old_session_config.host().to_string();
    let destination = new_session_config.host().to_string();
    let origin_token = old_session_config.access_token().to_string();
    let destination_token = new_session_config.access_token().to_string();

    tracing::info!("Migrating Preferences started");
    let request = MigratePreferencesRequest {
        destination,
        destination_token,
        origin,
        did,
        origin_token,
    };
    match pdsmigration_common::migrate_preferences_api(request).await {
        Ok(_) => {
            tracing::info!("Migrating Preferences completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error migrating Preferences: {pds_error}");
            Err(GuiError::Runtime)
        }
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

#[tracing::instrument(skip(pds_session))]
pub async fn migrate_plc_via_pds(
    pds_session: PdsSession,
    plc_signing_token: String,
    user_recovery_key: Option<String>,
) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let new_session_config = match &pds_session.new_session_config() {
        None => {
            tracing::error!("No new session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let origin = old_session_config.host().to_string();
    let destination = new_session_config.host().to_string();
    let origin_token = old_session_config.access_token().to_string();
    let destination_token = new_session_config.access_token().to_string();

    tracing::info!("Migrating PLC started");
    let request = MigratePlcRequest {
        destination,
        destination_token,
        origin,
        did,
        origin_token,
        plc_signing_token,
        user_recovery_key,
    };
    match pdsmigration_common::migrate_plc_api(request).await {
        Ok(_) => {
            tracing::info!("Migrating PLC completed");
            Ok(())
        }
        Err(_pds_error) => {
            tracing::error!("Error migrating PLC: {_pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn upload_blobs(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let new_session_config = match &pds_session.new_session_config() {
        None => {
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let pds_host = new_session_config.host().to_string();
    let token = new_session_config.access_token().to_string();

    tracing::info!("Uploading Blobs started");
    let request = UploadBlobsRequest {
        pds_host,
        did,
        token,
    };
    match pdsmigration_common::upload_blobs_api(request).await {
        Ok(_) => {
            tracing::info!("Uploading Blobs completed");
            Ok(())
        }
        Err(_pds_error) => {
            tracing::error!("Error uploading blobs: {_pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn export_all_blobs(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let old_pds_host = old_session_config.host().to_string();
    let old_token = old_session_config.access_token().to_string();

    tracing::info!("Exporting All Blobs started");
    let request = ExportAllBlobsRequest {
        origin: old_pds_host,
        did,
        origin_token: old_token,
    };
    match pdsmigration_common::export_all_blobs_api(request).await {
        Ok(_) => {
            tracing::info!("Exporting All Blobs completed");
            Ok(())
        }
        Err(pds_error) => match pds_error {
            MigrationError::Validation { .. } => {
                tracing::error!(
                    "Error exporting all blobs, validation error: {:?}",
                    pds_error
                );
                Err(GuiError::Other)
            }
            _ => {
                tracing::error!("Error exporting all blobs: {:?}", pds_error);
                Err(GuiError::Runtime)
            }
        },
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn export_missing_blobs(pds_session: PdsSession) -> Result<(), GuiError> {
    tracing::info!("Lib: Exporting Missing Blobs started");
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let new_session_config = match &pds_session.new_session_config() {
        None => {
            tracing::error!("No new session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let old_pds_host = old_session_config.host().to_string();
    let new_pds_host = new_session_config.host().to_string();
    let old_token = old_session_config.access_token().to_string();
    let new_token = new_session_config.access_token().to_string();

    tracing::info!("Exporting Missing Blobs started");
    let request = ExportBlobsRequest {
        destination: new_pds_host,
        origin: old_pds_host,
        did,
        origin_token: old_token,
        destination_token: new_token,
        is_missing_blob_request: false,
    };
    match pdsmigration_common::export_blobs_api(request).await {
        Ok(_) => {
            tracing::info!("Exporting Missing Blobs completed");
            //TODO add a check for failed blobs
            Ok(())
        }
        Err(pds_error) => match pds_error {
            MigrationError::Validation { .. } => {
                tracing::error!(
                    "Error exporting missing blobs, validation error: {:?}",
                    pds_error
                );
                Err(GuiError::Other)
            }
            _ => {
                tracing::error!("Error exporting missing blobs: {:?}", pds_error);
                Err(GuiError::Runtime)
            }
        },
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn import_repo(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };
    let new_session_config = match &pds_session.new_session_config() {
        None => {
            tracing::error!("No new session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let pds_host = new_session_config.host().to_string();
    let token = new_session_config.access_token().to_string();

    tracing::info!("Importing Repo started");
    let request = ImportPDSRequest {
        pds_host,
        did,
        token,
    };
    match pdsmigration_common::import_pds_api(request).await {
        Ok(_) => {
            tracing::info!("Importing Repo completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error importing repo: {:?}", pds_error);
            Err(GuiError::Runtime)
        }
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn export_repo(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };

    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let pds_host = old_session_config.host().to_string();
    let token = old_session_config.access_token().to_string();

    tracing::info!("Exporting Repo started");
    let request = ExportPDSRequest {
        pds_host,
        did,
        token,
    };
    match pdsmigration_common::export_pds_api(request).await {
        Ok(_res) => {
            tracing::info!("Exporting Repo completed");
            Ok(())
        }
        Err(pds_error) => {
            tracing::error!("Error exporting repo: {:?}", pds_error);
            Err(GuiError::Other)
        }
    }
}

#[tracing::instrument(skip(pds_session))]
pub async fn export_blobs(pds_session: PdsSession) -> Result<(), GuiError> {
    let did = match pds_session.did().clone() {
        None => {
            tracing::error!("No DID found");
            return Err(GuiError::Other);
        }
        Some(did) => did.to_string(),
    };

    let old_session_config = match &pds_session.old_session_config() {
        None => {
            tracing::error!("No old session config found");
            return Err(GuiError::Other);
        }
        Some(config) => config,
    };
    let pds_host = old_session_config.host().to_string();
    let token = old_session_config.access_token().to_string();

    tracing::info!("Exporting Repo started");
    let request = ExportPDSRequest {
        pds_host,
        did,
        token,
    };
    pdsmigration_common::export_pds_api(request)
        .await
        .map_err(|error| {
            tracing::error!("Error exporting repo: {:?}", error);
            GuiError::Other
        })
}

pub struct DescribePDS {
    pub terms_of_service: Option<String>,
    pub privacy_policy: Option<String>,
    pub invite_code_required: bool,
    pub available_user_domains: Vec<String>,
}

#[tracing::instrument]
pub async fn check_did_exists(new_pds_host: &str, did: &str) -> Result<bool, GuiError> {
    tracing::info!("Checking if DID exists on new PDS: {new_pds_host} {did}");
    use bsky_sdk::api::com::atproto::sync::get_repo_status::{Parameters, ParametersData};
    let bsky_agent = BskyAgent::builder().build().await.unwrap();
    bsky_agent.configure_endpoint(new_pds_host.to_string());
    match bsky_agent
        .api
        .com
        .atproto
        .sync
        .get_repo_status(Parameters {
            data: ParametersData {
                did: Did::from_str(did).unwrap(),
            },
            extra_data: ipld_core::ipld::Ipld::Null,
        })
        .await
    {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[tracing::instrument]
pub async fn fetch_tos_and_privacy_policy(new_pds_host: String) -> Result<DescribePDS, GuiError> {
    tracing::info!(
        "Fetching TOS and Privacy Policy from new PDS: {}",
        new_pds_host
    );
    let bsky_agent = BskyAgent::builder().build().await.unwrap();
    bsky_agent.configure_endpoint(new_pds_host);
    match bsky_agent.api.com.atproto.server.describe_server().await {
        Ok(result) => match result.links.clone() {
            None => Ok(DescribePDS {
                terms_of_service: None,
                privacy_policy: None,
                invite_code_required: result.invite_code_required.unwrap_or(false),
                available_user_domains: result.available_user_domains.clone(),
            }),
            Some(links) => Ok(DescribePDS {
                terms_of_service: links.terms_of_service.clone(),
                privacy_policy: links.privacy_policy.clone(),
                invite_code_required: result.invite_code_required.unwrap_or(false),
                available_user_domains: result.available_user_domains.clone(),
            }),
        },
        Err(error) => {
            tracing::error!(
                "Error fetching TOS and Privacy Policy from new PDS: {:?}",
                error
            );
            Err(GuiError::Runtime)
        }
    }
}

pub struct CreateAccountParameters {
    pds_session: PdsSession,
    new_email: String,
    new_pds_host: String,
    new_password: String,
    new_handle: String,
    invite_code: String,
}

#[tracing::instrument(skip(parameters))]
pub async fn create_account(parameters: CreateAccountParameters) -> Result<PdsSession, GuiError> {
    tracing::info!("Creating Account started");
    let mut pds_session = parameters.pds_session.clone();
    let old_session_config = match &pds_session.old_session_config() {
        None => return Err(GuiError::Other),
        Some(session_config) => session_config.clone(),
    };
    let did = match pds_session.did().clone() {
        None => return Err(GuiError::Other),
        Some(did) => did.to_string(),
    };
    let email = parameters.new_email.clone();
    let new_pds_host = parameters.new_pds_host.clone();
    let aud = new_pds_host.replace("https://", "did:web:");

    let password = parameters.new_password.clone();
    let invite_code = parameters.invite_code.clone();
    let handle = parameters.new_handle.clone();
    tracing::info!("Creating Account started");
    let service_auth_request = ServiceAuthRequest {
        pds_host: old_session_config.host().to_string(),
        aud,
        did: did.clone(),
        token: old_session_config.access_token().to_string(),
    };
    let service_token = match pdsmigration_common::get_service_auth_api(service_auth_request).await
    {
        Ok(res) => res,
        Err(_pds_error) => {
            tracing::error!("Error getting service auth token");
            return Err(GuiError::Runtime);
        }
    };

    let did = did
        .parse()
        .map_err(|_error| MigrationError::Validation {
            field: "did".to_string(),
        })
        .unwrap();

    let handle = handle.trim().to_string();
    let create_account_request = CreateAccountRequest {
        did,
        email: Some(email.clone()),
        handle: handle.clone().parse().unwrap(),
        invite_code: Some(invite_code.trim().to_string()),
        password: Some(password.clone()),
        recovery_key: None,
        verification_code: Some(String::from("")),
        verification_phone: None,
        plc_op: None,
        token: Some(service_token.clone()),
    };
    match pdsmigration_common::create_account(new_pds_host.as_str(), &create_account_request).await
    {
        Ok(_) => {
            tracing::info!("Creating Account completed");
            let bsky_agent = BskyAgent::builder().build().await.unwrap();
            match login_helper2(
                &bsky_agent,
                new_pds_host.as_str(),
                handle.as_str(),
                password.as_str(),
            )
            .await
            {
                Ok(res) => {
                    tracing::info!("Login successful");
                    let access_token = res.access_jwt.clone();
                    let refresh_token = res.refresh_jwt.clone();
                    let did = res.did.as_str().to_string();
                    if pds_session
                        .create_new_session(
                            did.as_str(),
                            access_token.as_str(),
                            refresh_token.as_str(),
                            new_pds_host.as_str(),
                        )
                        .is_err()
                    {
                        return Err(GuiError::Runtime);
                    }
                    Ok(pds_session)
                }
                Err(e) => {
                    tracing::error!("Error logging in: {e}");
                    Err(GuiError::Other)
                }
            }
        }
        Err(pds_error) => {
            tracing::error!("Error creating account: {pds_error}");
            Err(GuiError::Runtime)
        }
    }
}

pub fn run() -> eframe::Result {
    use std::time::Duration;
    use tokio::runtime::Runtime;

    let rt = Runtime::new().expect("Unable to create Runtime");

    // Enter the runtime so that `tokio::spawn` is available immediately.
    let _enter = rt.enter();

    // Execute the runtime in its own thread.
    // The future doesn't have to do anything. In this example, it just sleeps forever.
    std::thread::spawn(move || {
        rt.block_on(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
    });

    let icon_data =
        eframe::icon_data::from_png_bytes(include_bytes!("../assets/Northsky-Icon_Color.png"))
            .expect("The icon data must be valid");

    let options = eframe::NativeOptions {
        viewport: {
            egui::ViewportBuilder {
                icon: Some(Arc::new(icon_data)),
                ..Default::default()
            }
        },
        ..Default::default()
    };

    eframe::run_native(
        "PDS Migration Tool",
        options,
        Box::new(|cc| {
            egui_extras::install_image_loaders(&cc.egui_ctx);
            styles::setup_fonts(&cc.egui_ctx);
            Ok(Box::new(PdsMigrationApp::new(cc)))
        }),
    )
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServiceJwtHeader {
    pub typ: String,
    pub alg: String,
}

pub struct ServiceJwtParams {
    pub iss: String,
    pub aud: String,
    pub exp: Option<u64>,
    pub lxm: Option<String>,
    pub jti: Option<String>,
    pub secret_key: SecretKey,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServiceJwtPayload {
    pub iss: String,
    pub aud: String,
    pub exp: Option<u64>,
    pub lxm: Option<String>,
    pub jti: Option<String>,
}

pub fn get_random_str() -> String {
    #[allow(deprecated)]
    let token: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect();
    token
}

pub fn json_to_b64url<T: Serialize>(obj: &T) -> String {
    Base64::encode_string(serde_json::to_string(obj).unwrap().as_ref()).replace("=", "")
}

pub async fn create_service_jwt(params: ServiceJwtParams) -> String {
    let ServiceJwtParams {
        iss,
        aud,
        secret_key,
        ..
    } = params;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("timestamp in micros since UNIX epoch")
        .as_micros() as usize;
    let exp = params.exp.unwrap_or(((now + 6000_usize) / 1000) as u64);
    let lxm = params.lxm;
    let jti = get_random_str();
    let header = ServiceJwtHeader {
        typ: "JWT".to_string(),
        alg: "ES256K".to_string(),
    };
    let payload = ServiceJwtPayload {
        iss,
        aud,
        exp: Some(exp),
        lxm,
        jti: Some(jti),
    };
    let to_sign_str = format!("{0}.{1}", json_to_b64url(&header), json_to_b64url(&payload));
    let hash = Sha256::digest(to_sign_str.clone());
    #[allow(deprecated)]
    let message = Message::from_digest_slice(hash.as_ref()).unwrap();
    let mut sig = secret_key.sign_ecdsa(message);
    // Convert to low-s
    sig.normalize_s();
    // ASN.1 encoded per decode_dss_signature
    let compact_sig = sig.serialize_compact();
    format!(
        "{0}.{1}",
        to_sign_str,
        base64_url::encode(&compact_sig).replace("=", "") // Base 64 encode signature bytes
    )
}

pub async fn create_update_op<G>(last_op: PlcOperation, signer: &SecretKey, func: G) -> PlcOperation
where
    G: Fn(PlcOperation) -> PlcOperation,
{
    let last_op_json = serde_json::to_string(&last_op).unwrap();
    let last_op_index_map: IndexMap<String, Value> = serde_json::from_str(&last_op_json).unwrap();
    let prev = cid_for_cbor(&last_op_index_map);
    // omit sig so it doesn't accidentally make its way into the next operation
    let mut normalized = last_op;
    normalized.sig = None;

    let mut unsigned = func(normalized);
    unsigned.prev = Some(prev.to_string());

    add_signature(unsigned, signer).await
}

pub async fn add_signature(mut obj: PlcOperation, key: &SecretKey) -> PlcOperation {
    let sig = atproto_sign(&obj, key).to_vec();
    obj.sig = Some(base64_url::encode(&sig).replace("=", ""));
    obj
}

pub fn atproto_sign<T: Serialize>(obj: &T, key: &SecretKey) -> [u8; 64] {
    // Encode object to json before dag-cbor because serde_ipld_dagcbor doesn't properly
    // sort by keys
    let json = serde_json::to_string(obj).unwrap();
    // Deserialize to IndexMap with preserve key order enabled. serde_ipld_dagcbor does not sort nested
    // objects properly by keys
    let map_unsigned: IndexMap<String, Value> = serde_json::from_str(&json).unwrap();
    let unsigned_bytes = serde_ipld_dagcbor::to_vec(&map_unsigned).unwrap();
    // Hash dag_cbor to sha256
    let hash = Sha256::digest(&*unsigned_bytes);
    // Sign sha256 hash using private key
    #[allow(deprecated)]
    let message = Message::from_digest_slice(hash.as_ref()).unwrap();
    let mut sig = key.sign_ecdsa(message);
    // Convert to low-s
    sig.normalize_s();
    // ASN.1 encoded per decode_dss_signature
    sig.serialize_compact()
}

pub fn get_keys_from_private_key_str(private_key: String) -> (SecretKey, PublicKey) {
    let secp = Secp256k1::new();
    let decoded_key = hex::decode(private_key.as_bytes()).unwrap();
    #[allow(deprecated)]
    let secret_key = SecretKey::from_slice(&decoded_key).unwrap();
    let public_key = secret_key.public_key(&secp);
    (secret_key, public_key)
}

pub fn decode_did_secret_key(private_key: &str) -> (SecretKey, PublicKey) {
    let secp = Secp256k1::new();
    let decoded_key = hex::decode(private_key.as_bytes())
        .map_err(|_error| {
            let _context = format!("Issue decoding hex '{private_key}'");
            panic!()
        })
        .unwrap();
    #[allow(deprecated)]
    let secret_key = SecretKey::from_slice(&decoded_key)
        .map_err(|_error| {
            let _context = format!("Issue creating secret key from input '{private_key}'");
            panic!()
        })
        .unwrap();
    let public_key = secret_key.public_key(&secp);
    (secret_key, public_key)
}

pub fn extract_multikey(did: &String) -> String {
    if !did.starts_with(DID_KEY_PREFIX) {
        panic!("Incorrect prefix for did:key: {did}")
    }
    did[DID_KEY_PREFIX.len()..].to_string()
}

pub const DID_KEY_PREFIX: &str = "did:key:";

pub fn encode_did_key(pubkey: &PublicKey) -> String {
    let pk_compact = pubkey.serialize();
    let pk_wrapped = multicodec_wrap(pk_compact.to_vec());
    let pk_multibase = multibase::encode(Base58Btc, pk_wrapped.as_slice());
    format!("{DID_KEY_PREFIX}{pk_multibase}")
}
