use crate::{
    CreateAccountInput, CreateAccountInputData, CreateAccountRequest,
    CreateAccountWithoutPDSRequest, DeactivatedAccountInput, DeactivatedAccountInputData,
    MigrationError, CREATE_ACCOUNT_PATH, try_parse_error_response,
};
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;

#[tracing::instrument(skip(account_request))]
pub async fn create_account(
    pds_host: &str,
    account_request: &CreateAccountRequest,
) -> Result<(), MigrationError> {
    let client = reqwest::Client::new();
    let request_body = serde_json::to_string(&CreateAccountInput {
        data: CreateAccountInputData {
            did: Some(account_request.did.clone()),
            email: account_request.email.clone(),
            handle: account_request.handle.clone(),
            invite_code: account_request.invite_code.clone(),
            password: account_request.password.clone(),
            plc_op: None,
            recovery_key: account_request.recovery_key.clone(),
            verification_code: account_request.verification_code.clone(),
            verification_phone: account_request.verification_phone.clone(),
        },
        extra_data: Ipld::Null,
    })
    .map_err(|error| {
        tracing::error!(
            "Failed to create account - Error mapping input data to JSON: {:?}",
            error
        );
        MigrationError::Runtime {
            message: "Failed to create account".to_string(),
        }
    })?;
    let mut request_builder = client
        .post(pds_host.to_string() + CREATE_ACCOUNT_PATH)
        .body(request_body)
        .header("Content-Type", "application/json");

    if let Some(token) = &account_request.token {
        request_builder = request_builder.bearer_auth(token);
    }

    let result = request_builder.send().await;
    match result {
        Ok(output) => match output.status() {
            reqwest::StatusCode::OK => {
                tracing::info!("Successfully created account");
            }
            reqwest::StatusCode::BAD_REQUEST => {
                let error_message = try_parse_error_response(output).await;

                tracing::error!(
                    "Failed to create account - Bad Request: {}",
                    error_message
                );
                return Err(MigrationError::Upstream {
                    message: error_message,
                });
            }
            _ => {
                let status = output.status();
                let response_text = output
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                tracing::error!(
                    "Failed to create account - Received non-OK status on Create Account: {} - Response: {}",
                    status,
                    response_text
                );
                return Err(MigrationError::Runtime {
                    message: "Failed to create account".to_string(),
                });
            }
        },
        Err(e) => {
            return Err(MigrationError::Runtime {
                message: e.to_string(),
            });
        }
    }
    Ok(())
}

#[tracing::instrument(skip(account_request))]
pub async fn create_account_without_pds(
    pds_host: &str,
    account_request: &CreateAccountWithoutPDSRequest,
) -> Result<(), MigrationError> {
    let client = reqwest::Client::new();
    let x = serde_json::to_string(&CreateAccountInput {
        data: CreateAccountInputData {
            did: Some(account_request.did.clone()),
            email: account_request.email.clone(),
            handle: account_request.handle.parse().unwrap(),
            invite_code: account_request.invite_code.clone(),
            password: account_request.password.clone(),
            plc_op: None,
            recovery_key: account_request.recovery_key.clone(),
            verification_code: account_request.verification_code.clone(),
            verification_phone: account_request.verification_phone.clone(),
        },
        extra_data: Ipld::Null,
    })
    .map_err(|error| MigrationError::Runtime {
        message: error.to_string(),
    })?;
    let result = client
        .post(pds_host.to_string() + CREATE_ACCOUNT_PATH)
        .body(x)
        .header("Content-Type", "application/json")
        .send()
        .await;
    match result {
        Ok(output) => match output.status() {
            reqwest::StatusCode::OK => {
                tracing::info!("Successfully created account");
            }
            reqwest::StatusCode::BAD_REQUEST => {
                let error_message = try_parse_error_response(output).await;

                tracing::error!(
                    "Failed to create account - Bad Request: {}",
                    error_message
                );
                return Err(MigrationError::Upstream {
                    message: error_message,
                });
            }
            _ => {
                let status = output.status();
                let response_text = output
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unable to read response".to_string());
                tracing::error!(
                    "Failed to create account - Received non-OK status on Create Account: {} - Response: {}",
                    status,
                    response_text
                );
                return Err(MigrationError::Runtime {
                    message: "Failed to create account".to_string(),
                });
            }
        },
        Err(e) => {
            tracing::error!("Failed to create account - Error sending request: {:?}", e);
            return Err(MigrationError::Runtime {
                message: e.to_string(),
            });
        }
    }
    Ok(())
}

#[tracing::instrument(skip(agent))]
pub async fn deactivate_account(agent: &BskyAgent) -> Result<(), MigrationError> {
    agent
        .api
        .com
        .atproto
        .server
        .deactivate_account(DeactivatedAccountInput {
            data: DeactivatedAccountInputData { delete_after: None },
            extra_data: Ipld::Null,
        })
        .await
        .map_err(|error| {
            tracing::error!("Failed to deactivate account: {:?}", error);
            MigrationError::Runtime {
                message: error.to_string(),
            }
        })?;
    Ok(())
}
