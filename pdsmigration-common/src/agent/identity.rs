use crate::agent::account::get_pds_endpoint_from_did;
use crate::agent::types::RecommendedDidOutputData;
use crate::{
    MigrationError, SignPlcOperationInput, SubmitPlcOperationInput, SubmitPlcOperationInputData,
};
use bsky_sdk::api::com::atproto::identity::sign_plc_operation::InputData;
use bsky_sdk::api::types::Unknown;
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;
use serde::Deserialize;

#[derive(Deserialize)]
struct ResolveHandleResponse {
    did: String,
}

#[derive(Deserialize)]
struct PdsErrorResponse {
    message: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DescribeServerResponse {
    #[serde(default)]
    available_user_domains: Vec<String>,
}

#[tracing::instrument(skip(did), fields(handle, pds_host))]
pub async fn check_handle_availability(
    pds_host: &str,
    handle: &str,
    did: Option<&str>,
) -> Result<bool, MigrationError> {
    if handle.is_empty() {
        return Err(MigrationError::Validation {
            field: "handle".to_string(),
        });
    }

    let mut url = reqwest::Url::parse(pds_host).map_err(|_| MigrationError::Validation {
        field: "pds_host".to_string(),
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MigrationError::Validation {
            field: "pds_host".to_string(),
        });
    }
    url.set_path("/xrpc/com.atproto.identity.resolveHandle");
    url.set_query(None);
    url.query_pairs_mut().append_pair("handle", handle);

    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to check handle availability: {error}"),
        })?;

    let status = response.status();
    let response_body = response
        .text()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to read handle lookup response: {error}"),
        })?;

    let available = if status.is_success() {
        let resolved: ResolveHandleResponse =
            serde_json::from_str(&response_body).map_err(|error| MigrationError::Upstream {
                message: format!("Unable to read handle lookup response: {error}"),
            })?;
        did == Some(resolved.did.as_str())
    } else {
        let error_body = serde_json::from_str::<PdsErrorResponse>(&response_body).ok();
        if error_body.as_ref().and_then(|body| body.message.as_deref())
            == Some("Unable to resolve handle")
        {
            true
        } else {
            return Err(MigrationError::Upstream {
                message: error_body
                    .and_then(|body| body.message)
                    .unwrap_or_else(|| format!("Handle lookup failed with status {status}")),
            });
        }
    };

    if !available || did.is_none() {
        return Ok(available);
    }

    let Some(did) = did else {
        return Ok(true);
    };
    let current_pds =
        get_pds_endpoint_from_did(did)
            .await
            .ok_or_else(|| MigrationError::Upstream {
                message: "Unable to determine current PDS from the DID document".to_string(),
            })?;
    if pds_offers_handle(&current_pds, handle).await? {
        return Err(MigrationError::BadRequest {
            message: format!(
                "The handle {handle} is offered by your current PDS and cannot be migrated. Choose a different handle."
            ),
        });
    }

    Ok(true)
}

async fn pds_offers_handle(pds_host: &str, handle: &str) -> Result<bool, MigrationError> {
    let mut url = reqwest::Url::parse(pds_host).map_err(|_| MigrationError::Upstream {
        message: "Current PDS URL from DID document is invalid".to_string(),
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MigrationError::Upstream {
            message: "Current PDS URL from DID document is invalid".to_string(),
        });
    }
    url.set_path("/xrpc/com.atproto.server.describeServer");
    url.set_query(None);

    let response = reqwest::Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(2))
        .send()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to describe current PDS: {error}"),
        })?;
    if !response.status().is_success() {
        let message = crate::try_parse_error_response(response).await;
        return Err(MigrationError::Upstream {
            message: format!("Unable to describe current PDS: {message}"),
        });
    }

    let response_body = response
        .text()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to read current PDS description: {error}"),
        })?;
    let description: DescribeServerResponse =
        serde_json::from_str(&response_body).map_err(|error| MigrationError::Upstream {
            message: format!("Unable to read current PDS description: {error}"),
        })?;

    Ok(description
        .available_user_domains
        .iter()
        .any(|domain| handle_matches_domain(handle, domain)))
}

fn handle_matches_domain(handle: &str, domain: &str) -> bool {
    let handle = handle.trim_end_matches('.').to_ascii_lowercase();
    let domain = domain
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('.')
        .to_ascii_lowercase();
    !domain.is_empty() && handle.ends_with(&format!(".{domain}"))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn unresolved_handle_is_available() {
        let pds = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .and(query_param("handle", "alice.test"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "InvalidRequest",
                "message": "Unable to resolve handle"
            })))
            .mount(&pds)
            .await;

        assert!(check_handle_availability(&pds.uri(), "alice.test", None)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn resolved_handle_owned_by_another_did_is_unavailable() {
        let pds = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "did": "did:plc:existing"
            })))
            .mount(&pds)
            .await;

        assert!(
            !check_handle_availability(&pds.uri(), "alice.test", Some("did:plc:other"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn upstream_error_message_is_preserved() {
        let pds = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/xrpc/com.atproto.identity.resolveHandle"))
            .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
                "error": "Unavailable",
                "message": "PDS lookup failed"
            })))
            .mount(&pds)
            .await;

        let error = check_handle_availability(&pds.uri(), "alice.test", None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, MigrationError::Upstream { message } if message == "PDS lookup failed")
        );
    }

    #[test]
    fn handle_matches_only_a_subdomain_of_offered_domain() {
        assert!(handle_matches_domain("alice.bsky.social", ".bsky.social"));
        assert!(handle_matches_domain("alice.BSKY.SOCIAL", "bsky.social"));
        assert!(!handle_matches_domain("bsky.social", ".bsky.social"));
        assert!(!handle_matches_domain("alice.example.com", ".bsky.social"));
    }
}
