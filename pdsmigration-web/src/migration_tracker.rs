use bsky_sdk::api::types::string::Did;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use utoipa::ToSchema;

#[derive(Debug, Clone)]
pub struct MigrationTracker {
    ttl: Duration,
    limit: i64,
    ongoing_migrations: Arc<Mutex<HashMap<Did, Instant>>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MigrationStatus {
    pub has_capacity: bool,
}

impl MigrationTracker {
    pub fn new(ttl: Duration, limit: i64) -> Self {
        Self {
            ttl,
            limit,
            ongoing_migrations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn status(&self, did: Option<Did>) -> MigrationStatus {
        let ongoing_migrations = self.ongoing_migrations.lock().unwrap();
        let did_is_ongoing = did
            .as_ref()
            .is_some_and(|did| ongoing_migrations.contains_key(did));
        let has_capacity =
            did_is_ongoing || self.limit == -1 || ongoing_migrations.len() as i64 <= self.limit;

        MigrationStatus { has_capacity }
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut ongoing_migrations = self.ongoing_migrations.lock().unwrap();
        let previous_count = ongoing_migrations.len();
        ongoing_migrations.retain(|_, inserted_at| now.duration_since(*inserted_at) < self.ttl);
        previous_count - ongoing_migrations.len()
    }
}

pub async fn run_periodic_cleanup(tracker: MigrationTracker, interval: Duration) {
    tracing::info!(
        interval_secs = interval.as_secs(),
        "Starting migration tracker cleanup"
    );

    loop {
        tokio::time::sleep(interval).await;
        let removed = tracker.cleanup_expired();
        if removed > 0 {
            tracing::info!(removed, "Removed expired migration tracker entries");
        }
    }
}

impl Default for MigrationTracker {
    fn default() -> Self {
        Self::new(Duration::from_secs(30 * 60), -1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_returns_capacity_for_ongoing_dids_or_available_slots() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), 0);
        let did: Did = "did:plc:abcd1234efgh5678ijkl".parse().unwrap();
        let other_did: Did = "did:plc:efgh5678ijklabcd1234".parse().unwrap();

        assert!(tracker.status(None).has_capacity);
        tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .insert(did.clone(), Instant::now());
        assert!(tracker.status(Some(did)).has_capacity);
        assert!(!tracker.status(Some(other_did)).has_capacity);
        assert!(!tracker.status(None).has_capacity);

        let unlimited_tracker = MigrationTracker::new(Duration::from_secs(60), -1);
        assert!(unlimited_tracker.status(None).has_capacity);
    }

    #[test]
    fn cleanup_removes_entries_older_than_ttl() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), 0);
        let expired_did: Did = "did:plc:abcd1234efgh5678ijkl".parse().unwrap();
        let ongoing_did: Did = "did:plc:efgh5678ijklabcd1234".parse().unwrap();
        let now = Instant::now();
        let mut ongoing_migrations = tracker.ongoing_migrations.lock().unwrap();
        ongoing_migrations.insert(expired_did.clone(), now - Duration::from_secs(61));
        ongoing_migrations.insert(ongoing_did.clone(), now);
        drop(ongoing_migrations);

        assert_eq!(tracker.cleanup_expired(), 1);
        assert!(!tracker.status(Some(expired_did)).has_capacity);
        assert!(tracker.status(Some(ongoing_did)).has_capacity);
        assert!(!tracker.status(None).has_capacity);
    }
}
