use crate::{
    try_parse_error_response, CreateAccountInput, CreateAccountInputData, CreateAccountRequest,
    DeactivatedAccountInput, DeactivatedAccountInputData, MigrationError, APPLICATION_JSON,
    CREATE_ACCOUNT_PATH,
};
use base64ct::Encoding;
use bsky_sdk::BskyAgent;
use ipld_core::ipld::Ipld;
use serde::Deserialize;

pub const SERVICE_AUTH_SIGNATURE_MISMATCH_MESSAGE: &str =
    "The origin PDS does not match the current PDS in the DID document.";

#[derive(Deserialize)]
struct TokenClaims {
    iss: Option<String>,
    aud: Option<String>,
    lxm: Option<String>,
    exp: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DidDocument {
    #[serde(default)]
    verification_method: Vec<DidDocumentEntry>,
    #[serde(default)]
    service: Vec<DidDocumentEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DidDocumentEntry {
    id: String,
    public_key_multibase: Option<String>,
    service_endpoint: Option<serde_json::Value>,
}

async fn fetch_did_document(did: &str) -> Option<DidDocument> {
    let url = if let Some(host) = did.strip_prefix("did:web:") {
        format!("https://{}/.well-known/did.json", host)
    } else {
        let directory =
            std::env::var("PLC_DIRECTORY").unwrap_or_else(|_| "https://plc.directory".to_string());
        format!("{}/{}", directory.trim_end_matches('/'), did)
    };

    let body = async {
        reqwest::Client::new()
            .get(&url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await?
            .text()
            .await
    }
    .await;

    match body {
        Ok(body) => serde_json::from_str(&body).ok(),
        Err(error) => {
            tracing::warn!("[{}] Unable to read {}: {}", did, url, error);
            None
        }
    }
}

pub(super) async fn validate_service_auth_pds(
    did: &str,
    login_pds: &str,
) -> Result<(), MigrationError> {
    let pds_endpoint = fetch_did_document(did).await.and_then(|document| {
        document
            .service
            .iter()
            .find(|service| service.id.ends_with("#atproto_pds"))
            .and_then(|service| service.service_endpoint.as_ref())
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    });
    let pds_origins_match = pds_endpoint
        .as_deref()
        .and_then(|endpoint| reqwest::Url::parse(endpoint).ok())
        .zip(reqwest::Url::parse(login_pds).ok())
        .is_some_and(|(endpoint, login)| {
            endpoint.origin().ascii_serialization() == login.origin().ascii_serialization()
        });

    if pds_origins_match {
        return Ok(());
    }

    tracing::error!(
        "[{}] Service auth rejected because login PDS {} does not match the PDS in the DID document {}",
        did,
        login_pds,
        pds_endpoint.as_deref().unwrap_or("unknown")
    );
    Err(MigrationError::BadRequest {
        message: SERVICE_AUTH_SIGNATURE_MISMATCH_MESSAGE.to_string(),
    })
}

/// Logs why the PDS refused a service auth token
async fn log_service_token_diagnostics(did: &str, token: Option<&str>) {
    let claims = token
        .and_then(|token| token.split('.').nth(1))
        .and_then(|payload| base64ct::Base64UrlUnpadded::decode_vec(payload).ok())
        .and_then(|payload| serde_json::from_slice::<TokenClaims>(&payload).ok());
    let Some(claims) = claims else {
        tracing::error!(
            "[{}] The create account request had no readable service auth token",
            did
        );
        return;
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_secs() as i64)
        .unwrap_or_default();
    let document = fetch_did_document(did).await;
    let signing_key = document
        .as_ref()
        .and_then(|document| {
            document
                .verification_method
                .iter()
                .find(|method| method.id.ends_with("#atproto"))
                .and_then(|method| method.public_key_multibase.clone())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let pds_endpoint = document
        .as_ref()
        .and_then(|document| {
            document
                .service
                .iter()
                .find(|service| service.id.ends_with("#atproto_pds"))
                .and_then(|service| service.service_endpoint.as_ref())
                .map(|endpoint| endpoint.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let cause = if claims.iss.as_deref() != Some(did) {
        "the token was issued for a different account, because iss does not match the DID"
    } else if claims.exp.is_some_and(|exp| exp <= now) {
        "the token has expired"
    } else {
        "iss and exp are correct, the old PDS may have signed the token with a different key than the DID document sets"
    };

    tracing::error!(
        "[{}] Service auth token - iss: {}, aud: {}, lxm: {}, exp: {}, now: {}; DID document signing key: {}, pds: {}; probable cause: {}",
        did,
        claims.iss.as_deref().unwrap_or("absent"),
        claims.aud.as_deref().unwrap_or("absent"),
        claims.lxm.as_deref().unwrap_or("absent"),
        claims
            .exp
            .map(|exp| exp.to_string())
            .unwrap_or_else(|| "absent".to_string()),
        now,
        signing_key,
        pds_endpoint,
        cause
    );
}

#[tracing::instrument(skip(account_request))]
pub async fn create_account(
    pds_host: &str,
    account_request: &CreateAccountRequest,
) -> Result<(), MigrationError> {
    let did_str = account_request.did.as_str();
    tracing::info!(
        "[{}] Creating account on {} - handle: {}, has_email: {}, has_invite_code: {}, has_password: {}, has_recovery_key: {}, has_service_token: {}",
        did_str,
        pds_host,
        account_request.handle.as_str(),
        account_request.email.is_some(),
        account_request.invite_code.is_some(),
        account_request.password.is_some(),
        account_request.recovery_key.is_some(),
        account_request.token.is_some()
    );
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
            "[{}] Failed to create account - Error mapping input data to JSON: {:?}",
            did_str,
            error
        );
        MigrationError::Runtime {
            message: "Failed to create account".to_string(),
        }
    })?;
    let mut request_builder = client
        .post(pds_host.to_string() + CREATE_ACCOUNT_PATH)
        .body(request_body)
        .header("Content-Type", APPLICATION_JSON);

    if let Some(token) = &account_request.token {
        request_builder = request_builder.bearer_auth(token);
    }

    let result = request_builder.send().await;
    match result {
        Ok(output) => match output.status() {
            reqwest::StatusCode::OK => {
                tracing::info!("[{}] Successfully created account", did_str);
            }
            reqwest::StatusCode::BAD_REQUEST => {
                let error_message = try_parse_error_response(output).await;

                tracing::error!(
                    "[{}] Failed to create account on {} - Bad Request: {}",
                    did_str,
                    pds_host,
                    error_message
                );
                log_service_token_diagnostics(did_str, account_request.token.as_deref()).await;
                if error_message.starts_with("BadJwtSignature:") {
                    return Err(MigrationError::BadRequest {
                        message: SERVICE_AUTH_SIGNATURE_MISMATCH_MESSAGE.to_string(),
                    });
                }
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
                    "[{}] Failed to create account on {} - Received non-OK status on Create Account: {} - Response: {}",
                    did_str,
                    pds_host,
                    status,
                    response_text
                );
                log_service_token_diagnostics(did_str, account_request.token.as_deref()).await;
                return Err(MigrationError::Runtime {
                    message: "Failed to create account".to_string(),
                });
            }
        },
        Err(e) => {
            tracing::error!(
                "[{}] Failed to create account on {} - Request failed (timeout: {}, connect: {}, request: {}): {:?}",
                did_str,
                pds_host,
                e.is_timeout(),
                e.is_connect(),
                e.is_request(),
                e
            );
            return Err(MigrationError::Runtime {
                message: e.to_string(),
            });
        }
    }
    Ok(())
}

#[tracing::instrument(skip(agent))]
pub async fn deactivate_account(agent: &BskyAgent) -> Result<(), MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
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
            tracing::error!("[{}] Failed to deactivate account: {:?}", did_str, error);
            MigrationError::Runtime {
                message: error.to_string(),
            }
        })?;
    Ok(())
}

#[tracing::instrument(skip(agent))]
pub async fn activate_account_agent(agent: &BskyAgent) -> Result<(), MigrationError> {
    let did = agent.did().await.clone();
    let did_str = did.as_ref().map(|d| d.as_str()).unwrap_or("unknown");
    agent
        .api
        .com
        .atproto
        .server
        .activate_account()
        .await
        .map_err(|error| {
            tracing::error!("[{}] Failed to activate account: {:?}", did_str, error);
            MigrationError::Upstream {
                message: error.to_string(),
            }
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn jwt(claims: serde_json::Value) -> String {
        format!(
            "header.{}.signature",
            base64ct::Base64UrlUnpadded::encode_string(claims.to_string().as_bytes())
        )
    }

    // PLC_DIRECTORY is process-wide state, so the tests that set it run one after the other.
    static PLC_DIRECTORY_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn fetch_did_document_reads_the_atproto_entries() {
        let _guard = PLC_DIRECTORY_GUARD.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/did:plc:abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                serde_json::json!({
                    "verificationMethod": [{
                        "id": "did:plc:abc123#atproto",
                        "publicKeyMultibase": "zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
                    }],
                    "service": [{ "id": "#atproto_pds", "serviceEndpoint": "https://old.example.com" }],
                })
                .to_string(),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/did:plc:def456"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;
        std::env::set_var("PLC_DIRECTORY", server.uri());

        let document = fetch_did_document("did:plc:abc123")
            .await
            .expect("the mock gives a DID document");
        assert_eq!(document.verification_method[0].id, "did:plc:abc123#atproto");
        assert_eq!(
            document.verification_method[0]
                .public_key_multibase
                .as_deref(),
            Some("zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme")
        );
        assert_eq!(document.service[0].id, "#atproto_pds");
        assert_eq!(
            document.service[0]
                .service_endpoint
                .as_ref()
                .and_then(|endpoint| endpoint.as_str()),
            Some("https://old.example.com")
        );

        assert!(fetch_did_document("did:plc:def456").await.is_none());

        std::env::set_var("PLC_DIRECTORY", "http://127.0.0.1:1");
        assert!(fetch_did_document("did:plc:ghi789").await.is_none());

        std::env::remove_var("PLC_DIRECTORY");
    }

    #[tokio::test]
    async fn validate_service_auth_pds_requires_a_matching_endpoint() {
        let _guard = PLC_DIRECTORY_GUARD.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/did:plc:abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "service": [{
                    "id": "#atproto_pds",
                    "serviceEndpoint": "https://origin.example.com"
                }]
            })))
            .mount(&server)
            .await;
        std::env::set_var("PLC_DIRECTORY", server.uri());

        validate_service_auth_pds("did:plc:abc123", "https://origin.example.com/")
            .await
            .expect("the login PDS should match the DID document");

        let error = validate_service_auth_pds("did:plc:abc123", "https://other.example.com")
            .await
            .expect_err("a different login PDS must be rejected");
        assert!(matches!(
            error,
            MigrationError::BadRequest { message }
                if message == SERVICE_AUTH_SIGNATURE_MISMATCH_MESSAGE
        ));

        std::env::remove_var("PLC_DIRECTORY");
    }

    #[tokio::test]
    async fn service_token_diagnostics_survive_every_input() {
        let _guard = PLC_DIRECTORY_GUARD.lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/did:plc:abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                serde_json::json!({
                    "verificationMethod": [{
                        "id": "did:plc:abc123#atproto",
                        "publicKeyMultibase": "zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme",
                    }],
                    "service": [{ "id": "#atproto_pds", "serviceEndpoint": "https://old.example.com" }],
                })
                .to_string(),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/did:plc:def456"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;
        std::env::set_var("PLC_DIRECTORY", server.uri());

        for token in [
            None,
            Some(""),
            Some("not-a-jwt"),
            Some("!!!.???.signature"),
            Some("header.e30.signature"),
        ] {
            log_service_token_diagnostics("did:plc:abc123", token).await;
        }

        let token = jwt(serde_json::json!({
            "iss": "did:plc:abc123",
            "aud": "did:web:new.example.com",
            "lxm": "com.atproto.server.createAccount",
            "exp": 1_600_000_060,
        }));
        log_service_token_diagnostics("did:plc:abc123", Some(token.as_str())).await;
        log_service_token_diagnostics("did:plc:def456", Some(token.as_str())).await;

        std::env::set_var("PLC_DIRECTORY", "http://127.0.0.1:1");
        log_service_token_diagnostics("did:plc:ghi789", Some(token.as_str())).await;

        std::env::remove_var("PLC_DIRECTORY");
    }
}
