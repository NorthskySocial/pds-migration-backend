use bsky_sdk::api::types::string::{Did, Handle};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::REDACTED;

#[derive(Deserialize, Serialize)]
pub struct CreateAccountRequest {
    pub did: Did,
    pub email: Option<String>,
    pub handle: Handle,
    pub invite_code: Option<String>,
    pub password: Option<String>,
    pub recovery_key: Option<String>,
    pub verification_code: Option<String>,
    pub verification_phone: Option<String>,
    pub plc_op: Option<String>,
    pub token: Option<String>,
}

impl fmt::Debug for CreateAccountRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateAccountRequest")
            .field("did", &self.did)
            .field("email", &self.email)
            .field("handle", &self.handle)
            .field("invite_code", &self.invite_code)
            .field("password", &self.password.as_ref().map(|_| REDACTED))
            .field(
                "recovery_key",
                &self.recovery_key.as_ref().map(|_| REDACTED),
            )
            .field(
                "verification_code",
                &self.verification_code.as_ref().map(|_| REDACTED),
            )
            .field("verification_phone", &self.verification_phone)
            .field("plc_op", &self.plc_op.as_ref().map(|_| REDACTED))
            .field("token", &self.token.as_ref().map(|_| REDACTED))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_did() -> Did {
        Did::new("did:plc:abc123".to_string()).expect("valid test DID")
    }

    fn valid_handle() -> Handle {
        Handle::new("alice.test".to_string()).expect("valid test handle")
    }

    #[test]
    fn create_account_request_redacts_all_secrets() {
        let req = CreateAccountRequest {
            did: valid_did(),
            email: Some("user@example.com".to_string()),
            handle: valid_handle(),
            invite_code: Some("public-invite-code".to_string()),
            password: Some("password-secret".to_string()),
            recovery_key: Some("recovery-secret".to_string()),
            verification_code: Some("verification-secret".to_string()),
            verification_phone: Some("+15555555555".to_string()),
            plc_op: Some("plc-op-secret".to_string()),
            token: Some("token-secret".to_string()),
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains(REDACTED));
        for secret in [
            "password-secret",
            "recovery-secret",
            "verification-secret",
            "plc-op-secret",
            "token-secret",
        ] {
            assert!(!dbg.contains(secret), "leaked secret: {secret}");
        }
        // invite_code is not sensitive and should remain visible.
        assert!(dbg.contains("public-invite-code"));
        assert!(dbg.contains("user@example.com"));
        assert!(dbg.contains("alice.test"));
        assert!(dbg.contains("+15555555555"));
    }

    #[test]
    fn create_account_request_handles_none_secrets() {
        let req = CreateAccountRequest {
            did: valid_did(),
            email: None,
            handle: valid_handle(),
            invite_code: None,
            password: None,
            recovery_key: None,
            verification_code: None,
            verification_phone: None,
            plc_op: None,
            token: None,
        };
        let dbg = format!("{:?}", req);
        assert!(dbg.contains("None"));
    }
}
