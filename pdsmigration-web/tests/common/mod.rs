//! Shared helpers for the `pdsmigration-web` integration tests.

#![allow(dead_code)]

use pdsmigration_web::background_jobs::{JobManager, JobStatus};
use pdsmigration_web::config::{AppConfig, ExternalServices, ServerConfig};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

pub use pdsmigration_common::unique_did;

/// A baseline `AppConfig` for tests that need app state wired up.
pub fn create_test_config() -> AppConfig {
    AppConfig {
        server: ServerConfig {
            port: 8080,
            workers: 1,
            concurrent_tasks_per_job: 3,
            rate_limit_window_secs: 60,
            rate_limit_max_requests: 60,
            auth_token: None,
        },
        external_services: ExternalServices {
            s3_endpoint: "http://test-s3.example.com".to_string(),
        },
    }
}

/// The `getSession` response body a mocked PDS returns for `did`.
pub fn session_body(did: &str) -> serde_json::Value {
    json!({
        "did": did,
        "handle": "anothermigration.bsky.social",
        "active": true
    })
}

/// Poll a job until it reaches a terminal status, panicking if it doesn't
/// settle within ten seconds.
pub async fn await_job(jobs: &JobManager, id: Uuid) -> JobStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let record = jobs
            .get(id)
            .await
            .expect("job record should exist for spawned job");
        match record.status {
            JobStatus::Success | JobStatus::Error => return record.status,
            JobStatus::Queued | JobStatus::Running => {}
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "job {id} did not reach terminal status within timeout; last={:?}, error={:?}",
                record.status, record.error
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
