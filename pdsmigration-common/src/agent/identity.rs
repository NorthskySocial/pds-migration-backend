use crate::agent::types::{GetRecommendedResponse, RecommendedDidOutputData};
use crate::{
    MigrationError, SignPlcOperationInput, SubmitPlcOperationInput, SubmitPlcOperationInputData,
    GET_RECOMMENDED_DID_CREDENTIALS_PATH,
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

#[tracing::instrument(skip(access_token))]
pub async fn get_recommended(
    pds_host: &str,
    access_token: &str,
) -> Result<GetRecommendedResponse, MigrationError> {
    let client = reqwest::Client::new();
    let result = client
        .get(pds_host.to_string() + GET_RECOMMENDED_DID_CREDENTIALS_PATH)
        .bearer_auth(access_token)
        .send()
        .await;
    match result {
        Ok(output) => match output.status() {
            reqwest::StatusCode::OK => {
                tracing::info!("Successfully Fetched Recommended account");
                output
                    .json::<GetRecommendedResponse>()
                    .await
                    .map_err(|error| {
                        tracing::error!("Error fetching recommended account: {:?}", error);
                        MigrationError::Upstream {
                            message: error.to_string(),
                        }
                    })
            }
            _ => {
                tracing::error!("Error fetching recommended account: {:?}", output);
                Err(MigrationError::Upstream {
                    message: "Error fetching recommended account".to_string(),
                })
            }
        },
        Err(e) => {
            tracing::error!("Error fetching recommended: {:?}", e);
            Err(MigrationError::Upstream {
                message: "Error fetching recommended".to_string(),
            })
        }
    }
}
