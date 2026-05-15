use crate::agent::types::RecommendedDidOutputData;
use crate::{
    MigrationError, SignPlcOperationInput, SubmitPlcOperationInput, SubmitPlcOperationInputData,
};
use bsky_sdk::api::com::atproto::identity::sign_plc_operation::InputData;
use bsky_sdk::api::types::Unknown;
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;

#[tracing::instrument(skip(agent))]
pub async fn recommended_plc(
    agent: &BskyAgent,
) -> Result<RecommendedDidOutputData, MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    let result = agent
        .api
        .com
        .atproto
        .identity
        .get_recommended_did_credentials()
        .await
        .map_err(|error| {
            tracing::error!("[{}] Failed to get recommended did: {:?}", did_str, error);
            MigrationError::Runtime {
                message: error.to_string(),
            }
        })?;
    Ok(result.data)
}

#[tracing::instrument(skip(agent))]
pub async fn sign_plc(
    agent: &BskyAgent,
    plc_input_data: InputData,
) -> Result<Unknown, MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    let result = agent
        .api
        .com
        .atproto
        .identity
        .sign_plc_operation(SignPlcOperationInput {
            data: plc_input_data,
            extra_data: Ipld::Null,
        })
        .await;
    match result {
        Ok(output) => Ok(output.operation.clone()),
        Err(e) => {
            tracing::error!("[{}] Failed to sign plc: {:?}", did_str, e);
            Err(MigrationError::Runtime {
                message: e.to_string(),
            })
        }
    }
}

#[tracing::instrument(skip(agent))]
pub async fn submit_plc(agent: &BskyAgent, signed_plc: Unknown) -> Result<(), MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    let result = agent
        .api
        .com
        .atproto
        .identity
        .submit_plc_operation(SubmitPlcOperationInput {
            data: SubmitPlcOperationInputData {
                operation: signed_plc,
            },
            extra_data: Ipld::Null,
        })
        .await;
    match result {
        Ok(res) => Ok(res),
        Err(e) => {
            tracing::error!("[{}] Failed to submit plc: {:?}", did_str, e);
            Err(MigrationError::Runtime {
                message: e.to_string(),
            })
        }
    }
}

#[tracing::instrument(skip(agent))]
pub async fn request_token(agent: &BskyAgent) -> Result<(), MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    let result = agent
        .api
        .com
        .atproto
        .identity
        .request_plc_operation_signature()
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("[{}] Failed to request token: {:?}", did_str, e);
            Err(MigrationError::Runtime {
                message: e.to_string(),
            })
        }
    }
}
