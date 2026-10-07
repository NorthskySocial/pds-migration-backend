use crate::errors::ApiError;
use bsky_sdk::api::types::string::Did;
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use utoipa::ToSchema;

#[derive(Debug, Clone)]
pub struct MigrationTracker {
    ttl: Duration,
    limit: Option<usize>,
    ongoing_migrations: Arc<Mutex<HashMap<Did, Instant>>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MigrationStatus {
    pub has_capacity: bool,
}

impl MigrationTracker {
    pub fn new(ttl: Duration, limit: Option<usize>) -> Self {
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
        let has_capacity = did_is_ongoing
            || match self.limit {
                None => true,
                Some(limit) => ongoing_migrations.len() < limit,
            };

        MigrationStatus { has_capacity }
    }

    pub fn refresh(&self, did: &str) {
        if let Ok(did) = did.parse::<Did>() {
            self.ongoing_migrations
                .lock()
                .unwrap()
                .insert(did, Instant::now());
        }
    }

    pub fn finish_job(&self, did: &str, succeeded: bool, completes_migration: bool) {
        let Ok(did) = did.parse::<Did>() else {
            return;
        };
        let mut ongoing_migrations = self.ongoing_migrations.lock().unwrap();
        if succeeded && !completes_migration {
            ongoing_migrations.insert(did, Instant::now());
        } else {
            ongoing_migrations.remove(&did);
        }
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let mut ongoing_migrations = self.ongoing_migrations.lock().unwrap();
        let previous_count = ongoing_migrations.len();
        ongoing_migrations.retain(|_, inserted_at| now.duration_since(*inserted_at) < self.ttl);
        previous_count - ongoing_migrations.len()
    }

    pub async fn track<F, E>(
        &self,
        did: &str,
        completes_migration: bool,
        callback: F,
    ) -> Result<(), ApiError>
    where
        F: Future<Output = Result<(), E>>,
        E: Into<ApiError>,
    {
        let did = did.parse::<Did>().map_err(|_| ApiError::Validation {
            field: "did".to_string(),
        })?;

        match callback.await {
            Ok(()) => {
                let mut ongoing_migrations = self.ongoing_migrations.lock().unwrap();
                if completes_migration {
                    if ongoing_migrations.remove(&did).is_some() {
                        tracing::info!(
                            did = ?did,
                            current_migrations = ongoing_migrations.len(),
                            "Migration completed and removed"
                        );
                    }
                } else {
                    let is_new = ongoing_migrations
                        .insert(did.clone(), Instant::now())
                        .is_none();
                    if is_new {
                        tracing::info!(
                            did = ?did,
                            current_migrations = ongoing_migrations.len(),
                            "Migration started"
                        );
                    }
                }
                Ok(())
            }
            Err(error) => {
                let mut ongoing_migrations = self.ongoing_migrations.lock().unwrap();
                if ongoing_migrations.remove(&did).is_some() {
                    tracing::info!(
                        did = ?did,
                        current_migrations = ongoing_migrations.len(),
                        "Migration removed after callback failure"
                    );
                }
                Err(error.into())
            }
        }
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
        Self::new(Duration::from_secs(30 * 60), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_returns_capacity_for_ongoing_dids_or_available_slots() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(1));
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

        let zero_limit_tracker = MigrationTracker::new(Duration::from_secs(60), Some(0));
        assert!(!zero_limit_tracker.status(None).has_capacity);

        let unlimited_tracker = MigrationTracker::new(Duration::from_secs(60), None);
        unlimited_tracker.refresh("did:plc:abcd1234efgh5678ijkl");
        assert!(unlimited_tracker.status(None).has_capacity);
    }

    #[test]
    fn cleanup_removes_entries_older_than_ttl() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(0));
        let expired_did: Did = "did:plc:abcd1234efgh5678ijkl".parse().unwrap();
        let ongoing_did: Did = "did:plc:efgh5678ijklabcd1234".parse().unwrap();
        let ttl_boundary_did: Did = "did:plc:ijklabcd1234efgh5678".parse().unwrap();
        let now = Instant::now();
        let mut ongoing_migrations = tracker.ongoing_migrations.lock().unwrap();
        ongoing_migrations.insert(expired_did.clone(), now - Duration::from_secs(61));
        ongoing_migrations.insert(ongoing_did.clone(), now);
        ongoing_migrations.insert(ttl_boundary_did.clone(), now - tracker.ttl);
        drop(ongoing_migrations);

        assert_eq!(tracker.cleanup_expired(), 2);
        assert!(!tracker.status(Some(expired_did)).has_capacity);
        assert!(!tracker.status(Some(ttl_boundary_did)).has_capacity);
        assert!(tracker.status(Some(ongoing_did)).has_capacity);
        assert!(!tracker.status(None).has_capacity);
    }

    #[tokio::test]
    async fn track_inserts_and_updates_did_after_incomplete_success() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(0));
        let did = "did:plc:abcd1234efgh5678ijkl";
        let parsed_did: Did = did.parse().unwrap();
        tracker
            .track(did, false, async { Ok::<(), ApiError>(()) })
            .await
            .unwrap();

        let first_insert_time = *tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .get(&parsed_did)
            .unwrap();
        assert!(tracker.status(Some(parsed_did.clone())).has_capacity);

        let old_insert_time = Instant::now() - Duration::from_secs(1);
        tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .insert(parsed_did.clone(), old_insert_time);
        tracker
            .track(did, false, async { Ok::<(), ApiError>(()) })
            .await
            .unwrap();

        let ongoing_migrations = tracker.ongoing_migrations.lock().unwrap();
        assert!(*ongoing_migrations.get(&parsed_did).unwrap() > old_insert_time);
        assert!(*ongoing_migrations.get(&parsed_did).unwrap() >= first_insert_time);
    }

    #[tokio::test]
    async fn track_removes_did_after_completed_success() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(0));
        let did = "did:plc:abcd1234efgh5678ijkl";
        let parsed_did: Did = did.parse().unwrap();
        tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .insert(parsed_did.clone(), Instant::now());

        tracker
            .track(did, true, async { Ok::<(), ApiError>(()) })
            .await
            .unwrap();

        assert!(!tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .contains_key(&parsed_did));
    }

    #[tokio::test]
    async fn track_removes_did_after_callback_error() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(0));
        let did = "did:plc:abcd1234efgh5678ijkl";
        let parsed_did: Did = did.parse().unwrap();
        tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .insert(parsed_did.clone(), Instant::now());

        let result = tracker
            .track(did, false, async {
                Err::<(), ApiError>(ApiError::Runtime {
                    message: "callback failed".to_string(),
                })
            })
            .await;

        assert!(result.is_err());
        assert!(!tracker
            .ongoing_migrations
            .lock()
            .unwrap()
            .contains_key(&parsed_did));
    }

    #[tokio::test]
    async fn track_rejects_invalid_did_without_running_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(1));
        let callback_ran = Arc::new(AtomicBool::new(false));
        let callback_flag = callback_ran.clone();

        let result = tracker
            .track("not-a-did", false, async move {
                callback_flag.store(true, Ordering::SeqCst);
                Ok::<(), ApiError>(())
            })
            .await;

        assert!(matches!(result, Err(ApiError::Validation { field }) if field == "did"));
        assert!(!callback_ran.load(Ordering::SeqCst));
        assert!(tracker.ongoing_migrations.lock().unwrap().is_empty());
    }

    #[test]
    fn refresh_and_finish_job_update_tracker_state() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), Some(1));
        let refreshed_did: Did = "did:plc:abcd1234efgh5678ijkl".parse().unwrap();
        let completed_did: Did = "did:plc:efgh5678ijklabcd1234".parse().unwrap();
        let failed_did: Did = "did:plc:ijklabcd1234efgh5678".parse().unwrap();
        let continuing_job_did: Did = "did:plc:1234efgh5678ijklabcd".parse().unwrap();
        let old_insert_time = Instant::now() - Duration::from_secs(1);
        {
            let mut ongoing_migrations = tracker.ongoing_migrations.lock().unwrap();
            ongoing_migrations.insert(refreshed_did.clone(), old_insert_time);
            ongoing_migrations.insert(completed_did.clone(), old_insert_time);
            ongoing_migrations.insert(failed_did.clone(), old_insert_time);
            ongoing_migrations.insert(continuing_job_did.clone(), old_insert_time);
        }

        tracker.refresh("did:plc:abcd1234efgh5678ijkl");
        tracker.finish_job("did:plc:efgh5678ijklabcd1234", true, true);
        tracker.finish_job("did:plc:ijklabcd1234efgh5678", false, false);
        tracker.finish_job("did:plc:1234efgh5678ijklabcd", true, false);
        tracker.refresh("not-a-did");
        tracker.finish_job("not-a-did", true, false);

        let ongoing_migrations = tracker.ongoing_migrations.lock().unwrap();
        assert!(*ongoing_migrations.get(&refreshed_did).unwrap() > old_insert_time);
        assert!(!ongoing_migrations.contains_key(&completed_did));
        assert!(!ongoing_migrations.contains_key(&failed_did));
        assert!(*ongoing_migrations.get(&continuing_job_did).unwrap() > old_insert_time);
        assert_eq!(ongoing_migrations.len(), 2);
    }
}
