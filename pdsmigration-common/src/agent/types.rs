use bsky_sdk::BskyAgent;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type GetAgentResult = Result<BskyAgent, Box<dyn std::error::Error>>;
pub type RecommendedDidOutputData =
    bsky_sdk::api::com::atproto::identity::get_recommended_did_credentials::OutputData;
pub type CreateAccountInput = bsky_sdk::api::com::atproto::server::create_account::Input;
pub type CreateAccountInputData = bsky_sdk::api::com::atproto::server::create_account::InputData;
pub type DeactivatedAccountInput = bsky_sdk::api::com::atproto::server::deactivate_account::Input;
pub type DeactivatedAccountInputData =
    bsky_sdk::api::com::atproto::server::deactivate_account::InputData;
pub type CreateSessionOutputData = bsky_sdk::api::com::atproto::server::create_session::OutputData;
pub type GetServiceAuthParams = bsky_sdk::api::com::atproto::server::get_service_auth::Parameters;
pub type GetServiceAuthParamsData =
    bsky_sdk::api::com::atproto::server::get_service_auth::ParametersData;
pub type ListBlobsParams = bsky_sdk::api::com::atproto::sync::list_blobs::Parameters;
pub type ListBlobsParamsData = bsky_sdk::api::com::atproto::sync::list_blobs::ParametersData;
pub type ListMissingBlobsParams = bsky_sdk::api::com::atproto::repo::list_missing_blobs::Parameters;
pub type ListMissingBlobsParamsData =
    bsky_sdk::api::com::atproto::repo::list_missing_blobs::ParametersData;
pub type GetBlobParams = bsky_sdk::api::com::atproto::sync::get_blob::Parameters;
pub type GetBlobParamsData = bsky_sdk::api::com::atproto::sync::get_blob::ParametersData;
pub type SignPlcOperationInput = bsky_sdk::api::com::atproto::identity::sign_plc_operation::Input;
pub type SubmitPlcOperationInput =
    bsky_sdk::api::com::atproto::identity::submit_plc_operation::Input;
pub type SubmitPlcOperationInputData =
    bsky_sdk::api::com::atproto::identity::submit_plc_operation::InputData;
pub type PlcLogAudit = Vec<PlcLogAuditEntry>;

pub const CREATE_ACCOUNT_PATH: &str = "/xrpc/com.atproto.server.createAccount";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlcLogAuditEntry {
    pub did: String,
    pub operation: PlcOperation,
    pub cid: String,
    pub nullified: bool,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlcOperation {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "rotationKeys")]
    pub rotation_keys: Vec<String>,
    #[serde(rename = "verificationMethods")]
    pub verification_methods: BTreeMap<String, String>,
    #[serde(rename = "alsoKnownAs")]
    pub also_known_as: Vec<String>,
    pub services: BTreeMap<String, PlcOpService>,
    pub prev: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlcOpService {
    #[serde(rename = "type")]
    pub r#type: String,
    pub endpoint: String,
}
