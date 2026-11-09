use crate::errors::{ApiError, ApiErrorBody};
use crate::{post, Json};
use actix_web::HttpResponse;
use pdsmigration_common::check_handle_availability;
use bsky_sdk::api::types::string::Handle;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
pub struct CheckHandleApiRequest {
    #[serde(deserialize_with = "super::deserialize_trimmed")]
    #[schema(example = "alice.northsky.social")]
    pub handle: String,
    #[serde(deserialize_with = "super::deserialize_trimmed")]
    #[schema(example = "https://pds.example.com")]
    pub pds_host: String,
    #[serde(default)]
    #[schema(example = "did:plc:abcd1234efgh5678ijkl")]
    pub did: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct CheckHandleApiResponse {
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[utoipa::path(
    post,
    path = "/check-handle",
    request_body = CheckHandleApiRequest,
    responses(
        (status = 200, description = "Handle availability result", body = CheckHandleApiResponse),
        (status = 400, description = "Invalid request", body = ApiErrorBody),
        (status = 500, description = "Unable to check handle availability", body = ApiErrorBody)
    ),
    tag = "pdsmigration-web"
)]
#[tracing::instrument(skip(req), fields(handle = %req.handle, pds_host = %req.pds_host))]
#[post("/check-handle")]
pub async fn check_handle_api(req: Json<CheckHandleApiRequest>) -> Result<HttpResponse, ApiError> {
    let req = req.into_inner();
    if Handle::new(req.handle.clone()).is_err() {
        return Ok(HttpResponse::Ok().json(CheckHandleApiResponse {
            available: false,
            message: Some("Handle is invalid".to_string()),
        }));
    }

    let available = check_handle_availability(&req.pds_host, &req.handle, req.did.as_deref())
        .await
        .map_err(ApiError::from)?;
    Ok(HttpResponse::Ok().json(CheckHandleApiResponse {
        available,
        message: (!available).then(|| "Handle is already in use".to_string()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use serde_json::Value;
    async fn call_check_handle(body: Value) -> (actix_web::http::StatusCode, Value) {
        let app = test::init_service(App::new().service(check_handle_api)).await;
        let request = test::TestRequest::post()
            .uri("/check-handle")
            .set_json(body)
            .to_request();
        let response = test::call_service(&app, request).await;
        let status = response.status();
        let body = test::read_body_json(response).await;
        (status, body)
    }

    #[actix_rt::test]
    async fn invalid_pds_host_returns_validation_error() {
        let (status, body) = call_check_handle(serde_json::json!({
            "handle": "alice.test",
            "pds_host": "ftp://invalid.test"
        }))
        .await;

        assert_eq!(status, actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(body["message"], "pds_host");
    }

    #[actix_rt::test]
    async fn invalid_handle_returns_invalid_message() {
        let (status, body) = call_check_handle(serde_json::json!({
            "handle": "alice.",
            "pds_host": "https://pds.example.com"
        }))
        .await;

        assert_eq!(status, actix_web::http::StatusCode::OK);
        assert_eq!(body["available"], false);
        assert_eq!(body["message"], "Handle is invalid");
    }
}
