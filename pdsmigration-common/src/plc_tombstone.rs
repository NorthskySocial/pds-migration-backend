use crate::MigrationError;
use base64ct::{Base64UrlUnpadded, Encoding};
use bsky_sdk::api::types::string::Cid;
use bsky_sdk::api::types::string::Did;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditLogEntry {
    did: String,
    operation: serde_json::Value,
    cid: String,
    nullified: bool,
    created_at: String,
}

#[derive(Serialize)]
struct SignedTombstone<'a> {
    #[serde(rename = "type")]
    operation_type: &'static str,
    prev: &'a str,
    sig: &'a str,
}

pub async fn get_latest_plc_operation_cid(did: &str) -> Result<String, MigrationError> {
    let did = validate_did(did)?;
    let directory = plc_directory();
    latest_active_operation_cid(&did, &directory).await
}

pub async fn submit_plc_tombstone(
    did: &str,
    prev: &str,
    signature: &str,
) -> Result<(), MigrationError> {
    let did = validate_did(did)?;
    validate_signed_tombstone(prev, signature)?;

    let directory = plc_directory();
    submit_tombstone_at(&did, prev, signature, &directory).await
}

fn validate_did(did: &str) -> Result<String, MigrationError> {
    let parsed = Did::new(did.to_string()).map_err(|_| MigrationError::Validation {
        field: "did".to_string(),
    })?;
    if !did.starts_with("did:plc:") {
        return Err(MigrationError::Validation {
            field: "did".to_string(),
        });
    }
    Ok(parsed.to_string())
}

fn validate_signed_tombstone(prev: &str, signature: &str) -> Result<(), MigrationError> {
    if prev.is_empty() {
        return Err(MigrationError::Validation {
            field: "prev".to_string(),
        });
    }
    let signature_bytes =
        Base64UrlUnpadded::decode_vec(signature).map_err(|_| MigrationError::Validation {
            field: "sig".to_string(),
        })?;
    if signature_bytes.len() != 64 {
        return Err(MigrationError::Validation {
            field: "sig".to_string(),
        });
    }
    Ok(())
}

fn plc_directory() -> String {
    std::env::var("PLC_DIRECTORY").unwrap_or_else(|_| "https://plc.directory".to_string())
}

fn plc_url(directory: &str, path: &[&str]) -> Result<reqwest::Url, MigrationError> {
    let mut url = reqwest::Url::parse(directory).map_err(|_| MigrationError::Runtime {
        message: "PLC_DIRECTORY is not a valid URL".to_string(),
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MigrationError::Runtime {
            message: "PLC_DIRECTORY must use HTTP or HTTPS".to_string(),
        });
    }
    url.path_segments_mut()
        .map_err(|_| MigrationError::Runtime {
            message: "PLC_DIRECTORY must use HTTP or HTTPS".to_string(),
        })?
        .pop_if_empty()
        .extend(path.iter().copied());
    Ok(url)
}

async fn latest_operation(did: &str, directory: &str) -> Result<(String, bool), MigrationError> {
    let url = plc_url(directory, &[did, "log", "audit"])?;
    let response = reqwest::Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to fetch PLC audit log: {error}"),
        })?;
    if !response.status().is_success() {
        return Err(MigrationError::Upstream {
            message: format!(
                "PLC directory returned status {} for audit log",
                response.status()
            ),
        });
    }

    let response_body = response
        .bytes()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to parse PLC audit log: {error}"),
        })?;
    let entries: Vec<AuditLogEntry> =
        serde_json::from_slice(&response_body).map_err(|error| MigrationError::Upstream {
            message: format!("Unable to parse PLC audit log: {error}"),
        })?;
    let mut latest = None;
    let mut previous_created_at: Option<String> = None;
    for entry in entries {
        let operation_type = entry
            .operation
            .get("type")
            .and_then(serde_json::Value::as_str);
        if entry.did != did
            || Cid::from_str(&entry.cid).is_err()
            || entry.created_at.is_empty()
            || !matches!(
                operation_type,
                Some("create" | "plc_operation" | "plc_tombstone")
            )
        {
            return Err(MigrationError::Upstream {
                message: "PLC audit log contains an invalid entry".to_string(),
            });
        }
        if previous_created_at
            .as_ref()
            .is_some_and(|previous| entry.created_at < *previous)
        {
            return Err(MigrationError::Upstream {
                message: "PLC audit log entries are out of order".to_string(),
            });
        }
        previous_created_at = Some(entry.created_at.clone());
        if !entry.nullified {
            let tombstoned = operation_type == Some("plc_tombstone");
            latest = Some((entry.cid, tombstoned));
        }
    }
    latest.ok_or_else(|| MigrationError::Upstream {
        message: "PLC audit log has no active operations".to_string(),
    })
}

async fn latest_active_operation_cid(did: &str, directory: &str) -> Result<String, MigrationError> {
    let (prev, tombstoned) = latest_operation(did, directory).await?;
    if tombstoned {
        return Err(MigrationError::BadRequest {
            message: "DID is already tombstoned".to_string(),
        });
    }
    Ok(prev)
}

async fn submit_tombstone_at(
    did: &str,
    prev: &str,
    signature: &str,
    directory: &str,
) -> Result<(), MigrationError> {
    let (latest_cid, tombstoned) = latest_operation(did, directory).await?;
    if tombstoned {
        return Err(MigrationError::BadRequest {
            message: "DID is already tombstoned".to_string(),
        });
    }
    if prev != latest_cid {
        return Err(MigrationError::BadRequest {
            message: "PLC operation is stale; fetch the latest operation CID and sign again"
                .to_string(),
        });
    }
    let url = plc_url(directory, &[did])?;
    let operation = SignedTombstone {
        operation_type: "plc_tombstone",
        prev,
        sig: signature,
    };
    let body = serde_json::to_vec(&operation).map_err(|error| MigrationError::Runtime {
        message: format!("Unable to serialize PLC tombstone: {error}"),
    })?;
    let response = reqwest::Client::new()
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, crate::APPLICATION_JSON)
        .body(body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| MigrationError::Upstream {
            message: format!("Unable to submit PLC tombstone: {error}"),
        })?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(MigrationError::Upstream {
            message: format!(
                "PLC directory rejected tombstone with status {}",
                response.status()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const DID: &str = "did:plc:abc123";
    const PREV: &str = "bafyreiabeqf6lvg5trhkh76wy6l7o5xrg3riaip7g34vj5f3exkkq7xn24";
    const CID_OLD: &str = "bafyreihujzsooxzzjdu7op4n7kkhehcm5df3j4tfyr4qy4blfva47pzhkm";
    const SIG: &str =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn audit_entry(cid: &str, nullified: bool, operation_type: &str) -> serde_json::Value {
        json!({
            "did": DID,
            "operation": {"type": operation_type},
            "cid": cid,
            "nullified": nullified,
            "createdAt": "2026-01-01T00:00:00.000Z"
        })
    }

    async fn mount_audit_log(server: &MockServer, entries: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path(format!("/{DID}/log/audit")))
            .respond_with(ResponseTemplate::new(200).set_body_json(entries))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn chooses_last_non_nullified_operation() {
        let server = MockServer::start().await;
        mount_audit_log(
            &server,
            json!([
                audit_entry(CID_OLD, false, "plc_operation"),
                audit_entry(CID_OLD, true, "plc_operation"),
                audit_entry(PREV, false, "plc_operation")
            ]),
        )
        .await;

        let (cid, tombstoned) = latest_operation(DID, &server.uri()).await.unwrap();
        assert_eq!(cid, PREV);
        assert!(!tombstoned);
    }

    #[tokio::test]
    async fn rejects_invalid_audit_entries() {
        let server = MockServer::start().await;
        mount_audit_log(&server, json!([{"did": DID}])).await;

        assert!(matches!(
            latest_operation(DID, &server.uri()).await,
            Err(MigrationError::Upstream { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_out_of_order_audit_entries() {
        let server = MockServer::start().await;
        mount_audit_log(
            &server,
            json!([
                audit_entry(CID_OLD, false, "plc_operation"),
                {
                    "did": DID,
                    "operation": {"type": "plc_operation"},
                    "cid": PREV,
                    "nullified": false,
                    "createdAt": "2025-01-01T00:00:00.000Z"
                }
            ]),
        )
        .await;

        assert!(matches!(
            latest_operation(DID, &server.uri()).await,
            Err(MigrationError::Upstream { .. })
        ));
    }

    #[tokio::test]
    async fn refuses_to_fetch_prev_for_active_tombstone() {
        let server = MockServer::start().await;
        mount_audit_log(&server, json!([audit_entry(PREV, false, "plc_tombstone")])).await;

        assert!(matches!(
            latest_active_operation_cid(DID, &server.uri()).await,
            Err(MigrationError::BadRequest { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_stale_prev_without_submitting() {
        let server = MockServer::start().await;
        mount_audit_log(
            &server,
            json!([audit_entry(CID_OLD, false, "plc_operation")]),
        )
        .await;

        let result = submit_tombstone_at(DID, PREV, SIG, &server.uri()).await;
        assert!(matches!(result, Err(MigrationError::BadRequest { .. })));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn posts_signed_tombstone_after_freshness_check() {
        let server = MockServer::start().await;
        let directory = format!("{}/proxy/plc", server.uri());
        Mock::given(method("GET"))
            .and(path(format!("/proxy/plc/{DID}/log/audit")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                audit_entry(PREV, false, "plc_operation")
            ])))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/proxy/plc/{DID}")))
            .and(body_json(
                json!({"type": "plc_tombstone", "prev": PREV, "sig": SIG}),
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        submit_tombstone_at(DID, PREV, SIG, &directory)
            .await
            .unwrap();
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[test]
    fn signature_must_be_unpadded_base64url_for_64_bytes() {
        assert!(validate_signed_tombstone(PREV, SIG).is_ok());
        assert!(validate_signed_tombstone(PREV, "bad signature").is_err());
        assert!(validate_signed_tombstone(PREV, "AA").is_err());
    }

    #[test]
    fn did_must_be_plc() {
        assert!(validate_did(DID).is_ok());
        assert!(validate_did("did:web:example.com").is_err());
    }
}
