use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
pub enum MigrationError {
    #[display("Validation error on field: {field}")]
    Validation { field: String },
    #[display("Authentication error")]
    Authentication { message: String },
    #[display("Upstream error: {message}")]
    Upstream { message: String },
    #[display("Unexpected error occurred: {message}")]
    Runtime { message: String },
    #[display("Rate limit reached. Please try again later.")]
    RateLimitReached,
}

pub async fn try_parse_error_response(response: reqwest::Response) -> String {
    let response_text = response
        .text()
        .await
        .unwrap_or_else(|_| "Unable to read response".to_string());

    if let Ok(error_json) = serde_json::from_str::<serde_json::Value>(&response_text) {
        format!(
            "{}: {}",
            error_json
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error"),
            error_json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("No message")
        )
    } else {
        response_text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    #[tokio::test]
    async fn test_parse_error_response_with_valid_json() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(
                ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "The request was invalid"
                })),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "InvalidRequest: The request was invalid");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_missing_error_field() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(
                ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "message": "Something went wrong"
                })),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "Unknown error: Something went wrong");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_missing_message_field() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(
                ResponseTemplate::new(400).set_body_json(serde_json::json!({
                    "error": "ServerError"
                })),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "ServerError: No message");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_missing_both_fields() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "status": "failure"
            })))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "Unknown error: No message");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_plain_text() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(400).set_body_string("Internal Server Error"))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "Internal Server Error");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_invalid_json() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_string("{invalid json content}")
                    .insert_header("content-type", "application/json"),
            )
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "{invalid json content}");
    }

    #[tokio::test]
    async fn test_parse_error_response_with_empty_body() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/test"))
            .respond_with(ResponseTemplate::new(400).set_body_string(""))
            .mount(&mock_server)
            .await;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}/test", mock_server.uri()))
            .send()
            .await
            .unwrap();

        let result = try_parse_error_response(response).await;

        assert_eq!(result, "");
    }
}
