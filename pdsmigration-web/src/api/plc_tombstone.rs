use crate::errors::{ApiError, ApiErrorBody};
use crate::{post, Json};
use actix_web::{get, web::Path, HttpResponse};
use pdsmigration_common::{
    get_latest_plc_operation_cid, sign_and_submit_plc_tombstone, submit_plc_tombstone,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct PlcLatestOperationResponse {
    #[schema(example = "bafyreiabeqf6lvg5trhkh76wy6l7o5xrg3riaip7g34vj5f3exkkq7xn24")]
    pub cid: String,
}

#[derive(Deserialize, ToSchema)]
pub struct SubmitPlcTombstoneApiRequest {
    #[serde(deserialize_with = "super::deserialize_trimmed")]
    #[schema(example = "bafyreiabeqf6lvg5trhkh76wy6l7o5xrg3riaip7g34vj5f3exkkq7xn24")]
    pub prev: String,
    #[serde(deserialize_with = "super::deserialize_trimmed")]
    #[schema(example = "<base64url-signature>")]
    pub sig: String,
}

#[derive(Deserialize, ToSchema)]
pub struct SignAndSubmitPlcTombstoneApiRequest {
    #[serde(deserialize_with = "super::deserialize_trimmed")]
    /// Hex-encoded 32-byte secp256k1 rotation private key. Sent to the server for signing.
    #[schema(
        example = "0000000000000000000000000000000000000000000000000000000000000001",
        write_only = true
    )]
    pub private_key: String,
}

#[utoipa::path(
    get,
    path = "/plc/{did}/latest-operation",
    params(("did" = String, Path, description = "PLC DID to inspect")),
    responses(
        (status = 200, description = "CID of latest valid PLC operation; use as prev when signing the next operation", body = PlcLatestOperationResponse),
        (status = 400, description = "Invalid DID", body = ApiErrorBody),
        (status = 500, description = "Unable to read PLC audit log", body = ApiErrorBody)
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(fields(did = %did))]
#[get("/plc/{did}/latest-operation")]
pub async fn get_latest_plc_operation_api(did: Path<String>) -> Result<HttpResponse, ApiError> {
    let cid = get_latest_plc_operation_cid(&did)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().json(PlcLatestOperationResponse { cid }))
}

#[utoipa::path(
    post,
    path = "/plc/{did}/tombstone",
    params(("did" = String, Path, description = "PLC DID to deactivate")),
    request_body = SubmitPlcTombstoneApiRequest,
    responses(
        (status = 200, description = "Tombstone accepted; DID data is cleared. PLC recovery/nullification rules still apply."),
        (status = 400, description = "Invalid or stale tombstone, or PLC directory returned a 4xx response", body = ApiErrorBody),
        (status = 500, description = "PLC directory returned a non-200 non-4xx response or request failed", body = ApiErrorBody)
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %did))]
#[post("/plc/{did}/tombstone")]
pub async fn submit_plc_tombstone_api(
    did: Path<String>,
    req: Json<SubmitPlcTombstoneApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    submit_plc_tombstone(&did, &req.prev, &req.sig)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().finish())
}

#[utoipa::path(
    post,
    path = "/plc/{did}/tombstone/sign-and-submit",
    params(("did" = String, Path, description = "DID to deactivate")),
    request_body = SignAndSubmitPlcTombstoneApiRequest,
    responses(
        (status = 200, description = "Tombstone signed and accepted by the PLC directory"),
        (status = 400, description = "Invalid DID or rotation key, or PLC directory returned a 4xx response", body = ApiErrorBody),
        (status = 500, description = "PLC directory returned a non-200 non-4xx response or request failed", body = ApiErrorBody)
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(did = %did))]
#[post("/plc/{did}/tombstone/sign-and-submit")]
pub async fn sign_and_submit_plc_tombstone_api(
    did: Path<String>,
    req: Json<SignAndSubmitPlcTombstoneApiRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    sign_and_submit_plc_tombstone(&did, &req.private_key)
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test, App};
    use serde_json::Value;

    #[actix_rt::test]
    async fn get_rejects_non_plc_did() {
        let app = test::init_service(App::new().service(get_latest_plc_operation_api)).await;
        let request = test::TestRequest::get()
            .uri("/plc/did:web:example.com/latest-operation")
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn post_rejects_malformed_signature_before_network_call() {
        let app = test::init_service(App::new().service(submit_plc_tombstone_api)).await;
        let request = test::TestRequest::post()
            .uri("/plc/did:plc:abc123/tombstone")
            .set_json(serde_json::json!({"prev": "cid", "sig": "bad signature"}))
            .to_request();
        let response = test::call_service(&app, request).await;
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["code"], "Validation");
        assert_eq!(body["message"], "sig");
    }

    #[actix_rt::test]
    async fn post_rejects_wrong_signature_length_before_network_call() {
        let app = test::init_service(App::new().service(submit_plc_tombstone_api)).await;
        let request = test::TestRequest::post()
            .uri("/plc/did:plc:abc123/tombstone")
            .set_json(serde_json::json!({"prev": "cid", "sig": "AA"}))
            .to_request();
        let response = test::call_service(&app, request).await;
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["code"], "Validation");
        assert_eq!(body["message"], "sig");
    }

    #[actix_rt::test]
    async fn sign_and_submit_rejects_malformed_private_key_before_network_call() {
        let app = test::init_service(App::new().service(sign_and_submit_plc_tombstone_api)).await;
        let request = test::TestRequest::post()
            .uri("/plc/did:plc:abc123/tombstone/sign-and-submit")
            .set_json(serde_json::json!({"private_key": "bad key"}))
            .to_request();
        let response = test::call_service(&app, request).await;
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["code"], "Validation");
        assert_eq!(body["message"], "private_key");
    }
}
