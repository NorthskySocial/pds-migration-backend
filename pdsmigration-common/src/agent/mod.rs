mod account;
mod auth;
mod blobs;
mod identity;
mod preferences;
mod repo;
mod types;

pub use account::*;
pub use auth::*;
pub use blobs::*;
pub use identity::*;
pub use preferences::*;
pub use repo::*;
pub use types::*;

pub(crate) fn rate_limit_bypass_key() -> Option<String> {
    std::env::var("PDS_RATE_LIMIT_BYPASS_KEY").ok()
}

pub(crate) fn with_rate_limit_bypass_header(
    request: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    match rate_limit_bypass_key() {
        Some(key) => request.header("x-ratelimit-bypass", key),
        None => request,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn rate_limit_bypass_header_is_optional() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let previous = std::env::var("PDS_RATE_LIMIT_BYPASS_KEY").ok();

        std::env::set_var("PDS_RATE_LIMIT_BYPASS_KEY", "test-key");
        let configured =
            with_rate_limit_bypass_header(reqwest::Client::new().post("http://localhost"))
                .build()
                .unwrap()
                .headers()
                .get("x-ratelimit-bypass")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);

        std::env::remove_var("PDS_RATE_LIMIT_BYPASS_KEY");
        let unset = with_rate_limit_bypass_header(reqwest::Client::new().post("http://localhost"))
            .build()
            .unwrap()
            .headers()
            .contains_key("x-ratelimit-bypass");

        match previous {
            Some(value) => std::env::set_var("PDS_RATE_LIMIT_BYPASS_KEY", value),
            None => std::env::remove_var("PDS_RATE_LIMIT_BYPASS_KEY"),
        }

        assert_eq!(configured.as_deref(), Some("test-key"));
        assert!(!unset);
    }
}
