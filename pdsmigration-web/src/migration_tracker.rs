use serde::Serialize;
use std::time::Duration;
use utoipa::ToSchema;

#[derive(Debug, Clone)]
pub struct MigrationTracker {
    ttl: Duration,
    limit: i64,
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
        }
    }

    pub fn status(&self) -> MigrationStatus {
        MigrationStatus {
            has_capacity: true,
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
    fn status_returns_capacity() {
        let tracker = MigrationTracker::new(Duration::from_secs(60), 1);

        let status = tracker.status();
        assert!(status.has_capacity);
    }
}
