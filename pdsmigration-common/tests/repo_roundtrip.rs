use pdsmigration_common::{
    export_pds_api, import_pds_api, repo_car_path, ExportPDSRequest, ImportPDSRequest,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::unique_did;

#[tokio::test]
async fn export_import_repo_roundtrip() {
    let server = MockServer::start().await;
    let did = unique_did("reporoundtrip");
    let payload: &[u8] = b"fake-car-payload-for-wiremock-roundtrip";

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "did": did,
            "handle": "anothermigration.bsky.social",
            "active": true
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/xrpc/com.atproto.repo.importRepo"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let car_path = repo_car_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_file(&car_path);

    // Running the export logic (mocked)
    export_pds_api(ExportPDSRequest {
        pds_host: server.uri(),
        did: did.clone(),
        token: "test-access-jwt".to_string(),
    })
    .await
    .expect("export_pds_api should succeed against mocked PDS");

    // Did the file land where we expect it to?
    let on_disk = std::fs::read(&car_path).expect("export should have written the CAR file");
    assert_eq!(
        on_disk, payload,
        "bytes on disk should match what the mock PDS streamed"
    );

    // Running the import logic (mocked)
    import_pds_api(ImportPDSRequest {
        pds_host: server.uri(),
        did: did.clone(),
        token: "test-access-jwt".to_string(),
    })
    .await
    .expect("import_pds_api should succeed against mocked PDS");

    // Did we receive the expected bytes in the import API call?
    let received = server
        .received_requests()
        .await
        .expect("wiremock should record requests");
    let import_req = received
        .iter()
        .find(|r| r.url.path() == "/xrpc/com.atproto.repo.importRepo")
        .expect("importRepo should have been called");
    assert_eq!(
        import_req.body.as_slice(),
        payload,
        "importRepo should receive exactly the bytes export wrote"
    );

    // Cleanup
    let _ = std::fs::remove_file(&car_path);
}
