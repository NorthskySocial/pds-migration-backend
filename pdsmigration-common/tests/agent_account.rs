use bsky_sdk::api::agent::Configure;
use pdsmigration_common::{
    activate_account_agent, build_agent, deactivate_account, login_helper, unique_did,
    MigrationError,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn mount_get_session(server: &MockServer, did: &str) {
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": did,
            "handle": "anothermigration.bsky.social",
            "active": true,
        })))
        .mount(server)
        .await;
}

async fn logged_in_agent(server: &MockServer, did: &str, token: &str) -> bsky_sdk::BskyAgent {
    let agent = build_agent().await.expect("build_agent should succeed");
    agent.configure_endpoint(server.uri());
    login_helper(&agent, &server.uri(), did, token)
        .await
        .expect("login_helper should resume the mocked session");
    agent
}

#[tokio::test]
async fn activate_account_agent_happy_returns_ok() {
    let server = MockServer::start().await;
    let did = unique_did("activatehappy");
    mount_get_session(&server, &did).await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.activateAccount"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let agent = logged_in_agent(&server, &did, "origin-jwt").await;
    activate_account_agent(&agent)
        .await
        .expect("activate_account_agent should succeed on 200");

    let received = server.received_requests().await.expect("requests recorded");
    let activate_calls: Vec<_> = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .collect();
    assert_eq!(
        activate_calls.len(),
        1,
        "activateAccount should be called exactly once"
    );

    let auth = activate_calls[0]
        .headers
        .get("authorization")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert_eq!(
        auth, "Bearer origin-jwt",
        "activateAccount must forward the bearer token"
    );

    let deactivate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.deactivateAccount")
        .count();
    assert_eq!(
        deactivate_calls, 0,
        "activate must not also call deactivateAccount"
    );
}

#[tokio::test]
async fn activate_account_agent_upstream_error_maps_to_upstream() {
    let server = MockServer::start().await;
    let did = unique_did("activate500");
    mount_get_session(&server, &did).await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.activateAccount"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let agent = logged_in_agent(&server, &did, "origin-jwt").await;
    let err = match activate_account_agent(&agent).await {
        Ok(()) => panic!("a 500 on activateAccount must surface as an error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::Upstream { .. }),
        "expected MigrationError::Upstream, got {err:?}"
    );
}

#[tokio::test]
async fn deactivate_account_happy_returns_ok() {
    let server = MockServer::start().await;
    let did = unique_did("deactivatehappy");
    mount_get_session(&server, &did).await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.deactivateAccount"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let agent = logged_in_agent(&server, &did, "origin-jwt").await;
    deactivate_account(&agent)
        .await
        .expect("deactivate_account should succeed on 200");

    let received = server.received_requests().await.expect("requests recorded");
    let deactivate_calls: Vec<_> = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.deactivateAccount")
        .collect();
    assert_eq!(
        deactivate_calls.len(),
        1,
        "deactivateAccount should be called exactly once"
    );

    let auth = deactivate_calls[0]
        .headers
        .get("authorization")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert_eq!(
        auth, "Bearer origin-jwt",
        "deactivateAccount must forward the bearer token"
    );

    // Body should be a JSON object (deleteAfter omitted is fine).
    let content_type = deactivate_calls[0]
        .headers
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        content_type.starts_with("application/json"),
        "deactivateAccount should send JSON, got content-type {content_type:?}"
    );

    let activate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .count();
    assert_eq!(
        activate_calls, 0,
        "deactivate must not also call activateAccount"
    );
}

#[tokio::test]
async fn deactivate_account_error_maps_to_runtime() {
    let server = MockServer::start().await;
    let did = unique_did("deactivate500");
    mount_get_session(&server, &did).await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.deactivateAccount"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let agent = logged_in_agent(&server, &did, "origin-jwt").await;
    let err = match deactivate_account(&agent).await {
        Ok(()) => panic!("a 500 on deactivateAccount must surface as an error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::Runtime { .. }),
        "expected MigrationError::Runtime, got {err:?}"
    );
}

#[tokio::test]
async fn activate_account_agent_idempotent_returns_ok() {
    let server = MockServer::start().await;
    let did = unique_did("activateidempotent");
    mount_get_session(&server, &did).await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.activateAccount"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let agent = logged_in_agent(&server, &did, "origin-jwt").await;
    activate_account_agent(&agent).await.expect("first call ok");
    activate_account_agent(&agent)
        .await
        .expect("second call ok (idempotent)");

    let received = server.received_requests().await.expect("requests recorded");
    let activate_calls = received
        .iter()
        .filter(|r| r.url.path() == "/xrpc/com.atproto.server.activateAccount")
        .count();
    assert_eq!(
        activate_calls, 2,
        "two calls should produce two activateAccount requests, no retries"
    );
}
