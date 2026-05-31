use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use derive_more::{Display, Error};
use pdsmigration_common::MigrationError;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Display, Clone, Serialize, Deserialize, ToSchema)]
pub enum ApiErrorCode {
    #[display("VALIDATION_ERROR")]
    Validation,
    #[display("UPSTREAM_ERROR")]
    Upstream,
    #[display("RUNTIME_ERROR")]
    Runtime,
    #[display("AUTHENTICATION_ERROR")]
    Authentication,
    #[display("RATE_LIMIT")]
    RateLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorBody {
    #[schema(example = ApiErrorCode::Validation)]
    code: ApiErrorCode,
    #[schema(example = "did")]
    message: String,
}

#[derive(Debug, Display, Error, ToSchema)]
pub enum ApiError {
    #[display("Validation error on field: {field}")]
    #[schema(title = "Validation")]
    Validation { field: String },
    #[display("Upstream error: {message}")]
    #[schema(title = "Upstream")]
    Upstream { message: String },
    #[display("Unexpected error occurred: {message}")]
    #[schema(title = "Runtime")]
    Runtime { message: String },
    #[display("Authentication error: {message}")]
    #[schema(title = "Authentication")]
    Authentication { message: String },
    #[display("Too many requests: {message}")]
    #[schema(title = "Rate limit")]
    RateLimit { message: String },
}

impl ResponseError for ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Validation { .. } => StatusCode::BAD_REQUEST,
            ApiError::Upstream { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Runtime { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Authentication { .. } => StatusCode::UNAUTHORIZED,
            ApiError::RateLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let (code, message) = match self {
            ApiError::Validation { field } => (ApiErrorCode::Validation, field.to_string()),
            ApiError::Upstream { message } => (ApiErrorCode::Upstream, message.to_string()),
            ApiError::Runtime { message } => (ApiErrorCode::Runtime, message.to_string()),
            ApiError::Authentication { message } => {
                (ApiErrorCode::Authentication, message.to_string())
            }
            ApiError::RateLimit { message } => (ApiErrorCode::RateLimit, message.to_string()),
        };

        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .json(ApiErrorBody { code, message })
    }
}

impl From<MigrationError> for ApiError {
    fn from(error: MigrationError) -> Self {
        match error {
            MigrationError::Validation { field } => ApiError::Validation { field },
            MigrationError::BadRequest { message } => ApiError::Validation { field: message },
            MigrationError::Upstream { message } => ApiError::Upstream { message },
            MigrationError::Runtime { message } => ApiError::Runtime { message },
            MigrationError::RateLimitReached => ApiError::RateLimit {
                message: "Rate limit reached. Please try again later.".to_string(),
            },
            MigrationError::Authentication { .. } => ApiError::Authentication {
                message: "Authentication failed".to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::body::to_bytes;

    #[test]
    fn status_codes_match_variants() {
        assert_eq!(
            ApiError::Validation {
                field: "f".to_string()
            }
            .status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::Upstream {
                message: "x".to_string()
            }
            .status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ApiError::Runtime {
                message: "x".to_string()
            }
            .status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ApiError::Authentication {
                message: "x".to_string()
            }
            .status_code(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            ApiError::RateLimit {
                message: "x".to_string()
            }
            .status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn display_messages_include_context() {
        let v = ApiError::Validation {
            field: "did".to_string(),
        }
        .to_string();
        assert!(v.contains("did"));
        let u = ApiError::Upstream {
            message: "boom".to_string(),
        }
        .to_string();
        assert!(u.contains("boom"));
    }

    #[actix_rt::test]
    async fn error_response_serializes_validation_body() {
        let err = ApiError::Validation {
            field: "pds_host".to_string(),
        };
        let resp = err.error_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body_bytes = to_bytes(resp.into_body()).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["code"], "Validation");
        assert_eq!(body["message"], "pds_host");
    }

    #[actix_rt::test]
    async fn error_response_serializes_rate_limit_body() {
        let err = ApiError::RateLimit {
            message: "Rate limit exceeded".to_string(),
        };
        let resp = err.error_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        let body_bytes = to_bytes(resp.into_body()).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["code"], "RateLimit");
        assert_eq!(body["message"], "Rate limit exceeded");
    }

    #[test]
    fn from_migration_error_validation() {
        let api: ApiError = MigrationError::Validation {
            field: "did".to_string(),
        }
        .into();
        match api {
            ApiError::Validation { field } => assert_eq!(field, "did"),
            _ => panic!("expected Validation"),
        }
    }

    #[test]
    fn from_migration_error_upstream() {
        let api: ApiError = MigrationError::Upstream {
            message: "bad gateway".to_string(),
        }
        .into();
        match api {
            ApiError::Upstream { message } => assert_eq!(message, "bad gateway"),
            _ => panic!("expected Upstream"),
        }
    }

    #[test]
    fn from_migration_error_runtime() {
        let api: ApiError = MigrationError::Runtime {
            message: "io".to_string(),
        }
        .into();
        match api {
            ApiError::Runtime { message } => assert_eq!(message, "io"),
            _ => panic!("expected Runtime"),
        }
    }

    #[test]
    fn from_migration_error_rate_limit_uses_static_message() {
        let api: ApiError = MigrationError::RateLimitReached.into();
        match api {
            ApiError::RateLimit { message } => {
                assert!(message.to_lowercase().contains("rate limit"));
            }
            _ => panic!("expected RateLimit"),
        }
    }

    #[test]
    fn from_migration_error_authentication_masks_message() {
        let api: ApiError = MigrationError::Authentication {
            message: "supersecret-jwt-leak".to_string(),
        }
        .into();
        match api {
            ApiError::Authentication { message } => {
                assert!(!message.contains("supersecret-jwt-leak"));
                assert_eq!(message, "Authentication failed");
            }
            _ => panic!("expected Authentication"),
        }
    }

    #[test]
    fn api_error_code_display_strings_are_stable() {
        assert_eq!(ApiErrorCode::Validation.to_string(), "VALIDATION_ERROR");
        assert_eq!(ApiErrorCode::Upstream.to_string(), "UPSTREAM_ERROR");
        assert_eq!(ApiErrorCode::Runtime.to_string(), "RUNTIME_ERROR");
        assert_eq!(
            ApiErrorCode::Authentication.to_string(),
            "AUTHENTICATION_ERROR"
        );
        assert_eq!(ApiErrorCode::RateLimit.to_string(), "RATE_LIMIT");
    }
}
