use crate::{
    CreateSessionOutputData, GetServiceAuthParams, GetServiceAuthParamsData, MigrationError,
};
use bsky_sdk::api::agent::atp_agent::AtpSession;
use bsky_sdk::api::agent::Configure;
use bsky_sdk::api::types::string::{Did, Handle, Nsid};
use bsky_sdk::api::xrpc::Error;
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;

pub async fn build_agent() -> Result<BskyAgent, MigrationError> {
    BskyAgent::builder()
        .build()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: error.to_string(),
        })
}

#[tracing::instrument(skip(agent))]
pub async fn login_helper(
    agent: &BskyAgent,
    pds_host: &str,
    did: &str,
    token: &str,
) -> Result<AtpSession, MigrationError> {
    tracing::info!("[{}] Logging in to {}", did, pds_host);
    agent.configure_endpoint(pds_host.to_string());
    match agent
        .resume_session(AtpSession {
            data: CreateSessionOutputData {
                access_jwt: token.to_string(),
                active: Some(true),
                did: Did::new(did.to_string()).unwrap(),
                did_doc: None,
                email: None,
                email_auth_factor: None,
                email_confirmed: None,
                handle: Handle::new("anothermigration.bsky.social".to_string()).unwrap(),
                refresh_jwt: "".to_string(),
                status: None,
            },
            extra_data: Ipld::Null,
        })
        .await
    {
        Ok(_) => Ok(agent.get_session().await.unwrap()),
        Err(error) => {
            tracing::error!("[{}] Error while logging in: {}", did, error);
            match error {
                Error::Authentication(_) => Err(MigrationError::Authentication {
                    message: error.to_string(),
                }),
                Error::XrpcResponse(ref error_response) => {
                    if error_response.status.as_u16() == 401 {
                        Err(MigrationError::Authentication {
                            message: error.to_string(),
                        })
                    } else {
                        Err(MigrationError::Upstream {
                            message: error.to_string(),
                        })
                    }
                }
                _ => Err(MigrationError::Upstream {
                    message: error.to_string(),
                }),
            }
        }
    }
}

#[tracing::instrument(skip(agent))]
pub async fn get_service_auth(agent: &BskyAgent, aud: &str) -> Result<String, MigrationError> {
    let result = agent
        .api
        .com
        .atproto
        .server
        .get_service_auth(GetServiceAuthParams {
            data: GetServiceAuthParamsData {
                aud: aud.parse().map_err(|_error| MigrationError::Validation {
                    field: "Aud is invalid".to_string(),
                })?,
                exp: None,
                lxm: Some(Nsid::new("com.atproto.server.createAccount".to_string()).unwrap()),
            },
            extra_data: Ipld::Null,
        })
        .await
        .map_err(|error| {
            tracing::error!(
                "[{}] Failed to get service auth token for {}: {}",
                did_str,
                aud,
                error
            );
            MigrationError::Runtime {
                message: error.to_string(),
            }
        })?;
    Ok(result.token.clone())
}
