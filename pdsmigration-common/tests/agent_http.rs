use bsky_sdk::api::types::string::{Did, Handle};
use futures_util::StreamExt;
use pdsmigration_common::{
    create_account, download_blob, download_repo, CreateAccountRequest, GetBlobRequest,
    GetRepoRequest, MigrationError,
};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use pdsmigration_common::unique_did;

fn test_did() -> Did {
    Did::new(unique_did("agenthttp")).expect("valid test DID")
}

async fn collect_stream<S>(stream: S) -> Vec<u8>
where
    S: futures_core::Stream<Item = Result<bytes::Bytes, reqwest::Error>>,
{
    futures_util::pin_mut!(stream);
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.expect("chunk should stream cleanly"));
    }
    bytes
}

#[tokio::test]
async fn download_blob_happy_streams_bytes_and_sends_query() {
    let server = MockServer::start().await;
    let payload: &[u8] = b"streamed-blob-bytes";
    let cid = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .and(query_param("cid", cid))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&server)
        .await;

    let did = test_did();
    let request = GetBlobRequest {
        did: did.clone(),
        cid: cid.to_string(),
        token: "origin-jwt".to_string(),
    };

    let stream = download_blob(&server.uri(), &request)
        .await
        .expect("download_blob should succeed on 200");
    let body = collect_stream(stream).await;
    assert_eq!(body, payload, "streamed bytes should match the server body");

    // The DID must be propagated as a query param so the PDS serves the right repo.
    let received = server.received_requests().await.expect("requests recorded");
    let did_param = received[0]
        .url
        .query_pairs()
        .find(|(k, _)| k == "did")
        .map(|(_, v)| v.into_owned());
    assert_eq!(
        did_param.as_deref(),
        Some(did.as_str()),
        "request should carry the did query param"
    );
}

#[tokio::test]
async fn download_blob_rate_limited_returns_rate_limit_error() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "42")
                .set_body_bytes(b"ignored".to_vec()),
        )
        .mount(&server)
        .await;

    let request = GetBlobRequest {
        did: test_did(),
        cid: "bafkreirateblob".to_string(),
        token: "origin-jwt".to_string(),
    };

    let err = match download_blob(&server.uri(), &request).await {
        Ok(_) => panic!("a low ratelimit-remaining header must short-circuit"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::RateLimitReached),
        "expected RateLimitReached, got {err:?}"
    );
}

#[tokio::test]
async fn download_blob_bad_request_maps_to_upstream() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(ResponseTemplate::new(400).set_body_string("nope"))
        .mount(&server)
        .await;

    let request = GetBlobRequest {
        did: test_did(),
        cid: "bafkreibadblob".to_string(),
        token: "origin-jwt".to_string(),
    };

    let err = match download_blob(&server.uri(), &request).await {
        Ok(_) => panic!("400 must be an error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::Upstream { .. }),
        "expected Upstream for 400, got {err:?}"
    );
}

#[tokio::test]
async fn download_blob_server_error_maps_to_upstream() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getBlob"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let request = GetBlobRequest {
        did: test_did(),
        cid: "bafkreierrblob".to_string(),
        token: "origin-jwt".to_string(),
    };

    let err = match download_blob(&server.uri(), &request).await {
        Ok(_) => panic!("500 must be an error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::Upstream { .. }),
        "expected Upstream for 500, got {err:?}"
    );
}

#[tokio::test]
async fn download_repo_happy_streams_car_bytes() {
    let server = MockServer::start().await;
    let car: &[u8] = b"fake-car-file-bytes";

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(car),
        )
        .mount(&server)
        .await;

    let request = GetRepoRequest {
        did: test_did(),
        token: "origin-jwt".to_string(),
    };

    let stream = download_repo(&server.uri(), &request)
        .await
        .expect("download_repo should succeed on 200");
    let body = collect_stream(stream).await;
    assert_eq!(body, car, "streamed CAR bytes should match server body");
}

#[tokio::test]
async fn download_repo_rate_limited_returns_rate_limit_error() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "10")
                .set_body_bytes(b"ignored".to_vec()),
        )
        .mount(&server)
        .await;

    let request = GetRepoRequest {
        did: test_did(),
        token: "origin-jwt".to_string(),
    };

    let err = match download_repo(&server.uri(), &request).await {
        Ok(_) => panic!("low ratelimit-remaining must short-circuit"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::RateLimitReached),
        "expected RateLimitReached, got {err:?}"
    );
}

#[tokio::test]
async fn download_repo_server_error_maps_to_upstream() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let request = GetRepoRequest {
        did: test_did(),
        token: "origin-jwt".to_string(),
    };

    let err = match download_repo(&server.uri(), &request).await {
        Ok(_) => panic!("500 must be an error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, MigrationError::Upstream { .. }),
        "expected Upstream for 500, got {err:?}"
    );
}

fn create_account_request(token: Option<String>) -> CreateAccountRequest {
    CreateAccountRequest {
        did: Did::new(unique_did("createacct")).expect("valid test DID"),
        email: Some("alice@example.com".to_string()),
        handle: Handle::new("alice.test".to_string()).expect("valid handle"),
        invite_code: Some("invite-123".to_string()),
        password: Some("hunter2".to_string()),
        recovery_key: None,
        verification_code: None,
        verification_phone: None,
        plc_op: None,
        token,
    }
}

#[tokio::test]
async fn create_account_happy_posts_json_body() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createAccount"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessJwt": "a",
            "refreshJwt": "r",
            "handle": "alice.test",
            "did": "did:plc:created",
        })))
        .mount(&server)
        .await;

    let request = create_account_request(None);
    create_account(&server.uri(), &request)
        .await
        .expect("create_account should succeed on 200");

    // Verify the handle / email actually made it into the posted body.
    let received = server.received_requests().await.expect("requests recorded");
    let body = std::str::from_utf8(&received[0].body).expect("utf8 body");
    assert!(body.contains("alice.test"), "body should carry the handle");
    assert!(
        body.contains("alice@example.com"),
        "body should carry the email"
    );
}

#[tokio::test]
async fn create_account_with_token_sends_bearer_auth() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createAccount"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&server)
        .await;

    let request = create_account_request(Some("service-token".to_string()));
    create_account(&server.uri(), &request)
        .await
        .expect("create_account should succeed");

    let received = server.received_requests().await.expect("requests recorded");
    let auth = received[0]
        .headers
        .get("authorization")
        .expect("authorization header present when token supplied");
    assert_eq!(
        auth.to_str().unwrap(),
        "Bearer service-token",
        "token should be sent as bearer auth"
    );
}

#[tokio::test]
async fn create_account_without_token_omits_bearer_auth() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createAccount"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
        .mount(&server)
        .await;

    let request = create_account_request(None);
    create_account(&server.uri(), &request)
        .await
        .expect("create_account should succeed");

    let received = server.received_requests().await.expect("requests recorded");
    assert!(
        received[0].headers.get("authorization").is_none(),
        "no authorization header should be sent without a token"
    );
}

#[tokio::test]
async fn create_account_bad_request_surfaces_parsed_error_message() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createAccount"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "InvalidHandle",
            "message": "Handle already taken",
        })))
        .mount(&server)
        .await;

    let request = create_account_request(None);
    let err = create_account(&server.uri(), &request)
        .await
        .expect_err("400 should be an error");
    match err {
        MigrationError::Upstream { message } => {
            assert!(
                message.contains("Handle already taken") || message.contains("InvalidHandle"),
                "parsed error message should carry the PDS detail, got: {message}"
            );
        }
        other => panic!("expected Upstream for 400, got {other:?}"),
    }
}

#[tokio::test]
async fn create_account_unexpected_status_maps_to_runtime() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.server.createAccount"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&server)
        .await;

    let request = create_account_request(None);
    let err = create_account(&server.uri(), &request)
        .await
        .expect_err("500 should be an error");
    assert!(
        matches!(err, MigrationError::Runtime { .. }),
        "expected Runtime for unexpected 500 status, got {err:?}"
    );
}
