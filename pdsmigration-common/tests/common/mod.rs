//! Shared helpers for tests.

/// Build a DID that's unique per process and per call so parallel
/// integration tests don't collide
pub fn unique_did(prefix: &str) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    format!("did:plc:{prefix}{pid}{nanos}")
}
