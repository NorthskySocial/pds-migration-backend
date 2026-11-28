use pdsmigration_common::MigrationError;
use prometheus::{
    core::Collector, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MigrationMetrics {
    registry: Registry,
    pub migrations_active: IntGauge,
    migration_operations_total: IntCounterVec,
    migration_operation_duration_seconds: HistogramVec,
    migration_jobs_active: IntGaugeVec,
    migration_jobs_total: IntCounterVec,
    migration_job_duration_seconds: HistogramVec,
    migration_blobs_total: IntCounterVec,
    migration_blob_failures_total: IntCounterVec,
    migration_rate_limit_events_total: IntCounterVec,
}

impl MigrationMetrics {
    pub fn new(registry: Registry) -> Self {
        let migrations_active = IntGauge::with_opts(
            Opts::new(
                "migrations_active",
                "Number of migrations currently ongoing",
            )
            .namespace("pdsmigration"),
        )
        .expect("migration active gauge should be valid");

        let migration_operations_total = IntCounterVec::new(
            Opts::new(
                "migration_operations_total",
                "Completed migration operations",
            )
            .namespace("pdsmigration"),
            &["operation", "outcome", "error_type"],
        )
        .expect("migration operations counter should be valid");

        let migration_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "migration_operation_duration_seconds",
                "Migration operation duration in seconds",
            )
            .namespace("pdsmigration"),
            &["operation", "outcome"],
        )
        .expect("migration operation histogram should be valid");

        let migration_jobs_active = IntGaugeVec::new(
            Opts::new(
                "migration_jobs_active",
                "Background migration jobs by status",
            )
            .namespace("pdsmigration"),
            &["kind", "status"],
        )
        .expect("migration jobs gauge should be valid");

        let migration_jobs_total = IntCounterVec::new(
            Opts::new(
                "migration_jobs_total",
                "Completed background migration jobs",
            )
            .namespace("pdsmigration"),
            &["kind", "outcome", "error_type"],
        )
        .expect("migration jobs counter should be valid");

        let migration_job_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "migration_job_duration_seconds",
                "Background migration job duration in seconds",
            )
            .namespace("pdsmigration"),
            &["kind", "outcome"],
        )
        .expect("migration job histogram should be valid");

        let migration_blobs_total = IntCounterVec::new(
            Opts::new("migration_blobs_total", "Blob operations completed")
                .namespace("pdsmigration"),
            &["kind", "outcome"],
        )
        .expect("migration blobs counter should be valid");

        let migration_blob_failures_total = IntCounterVec::new(
            Opts::new(
                "migration_blob_failures_total",
                "Blob operations that failed",
            )
            .namespace("pdsmigration"),
            &["kind", "step", "error_type"],
        )
        .expect("migration blob failures counter should be valid");

        let migration_rate_limit_events_total = IntCounterVec::new(
            Opts::new(
                "migration_rate_limit_events_total",
                "Migration operations that encountered a rate limit",
            )
            .namespace("pdsmigration"),
            &["kind"],
        )
        .expect("migration rate limit counter should be valid");

        for collector in [
            Box::new(migrations_active.clone()) as Box<dyn Collector>,
            Box::new(migration_operations_total.clone()) as Box<dyn Collector>,
            Box::new(migration_operation_duration_seconds.clone()) as Box<dyn Collector>,
            Box::new(migration_jobs_active.clone()) as Box<dyn Collector>,
            Box::new(migration_jobs_total.clone()) as Box<dyn Collector>,
            Box::new(migration_job_duration_seconds.clone()) as Box<dyn Collector>,
            Box::new(migration_blobs_total.clone()) as Box<dyn Collector>,
            Box::new(migration_blob_failures_total.clone()) as Box<dyn Collector>,
            Box::new(migration_rate_limit_events_total.clone()) as Box<dyn Collector>,
        ] {
            registry
                .register(collector)
                .expect("migration metrics should register once");
        }

        Self {
            registry,
            migrations_active,
            migration_operations_total,
            migration_operation_duration_seconds,
            migration_jobs_active,
            migration_jobs_total,
            migration_job_duration_seconds,
            migration_blobs_total,
            migration_blob_failures_total,
            migration_rate_limit_events_total,
        }
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn set_migrations_active(&self, count: usize) {
        self.migrations_active.set(count as i64);
    }

    pub fn record_operation(
        &self,
        operation: &str,
        outcome: &str,
        error_type: &str,
        duration: Duration,
    ) {
        self.migration_operations_total
            .with_label_values(&[operation, outcome, error_type])
            .inc();
        self.migration_operation_duration_seconds
            .with_label_values(&[operation, outcome])
            .observe(duration.as_secs_f64());
    }

    pub fn job_enqueued(&self, kind: &str) {
        self.migration_jobs_active
            .with_label_values(&[kind, "queued"])
            .inc();
    }

    pub fn job_started(&self, kind: &str) {
        self.migration_jobs_active
            .with_label_values(&[kind, "queued"])
            .dec();
        self.migration_jobs_active
            .with_label_values(&[kind, "running"])
            .inc();
    }

    pub fn job_finished(&self, kind: &str, outcome: &str, error_type: &str, duration: Duration) {
        self.migration_jobs_active
            .with_label_values(&[kind, "running"])
            .dec();
        self.migration_jobs_total
            .with_label_values(&[kind, outcome, error_type])
            .inc();
        self.migration_job_duration_seconds
            .with_label_values(&[kind, outcome])
            .observe(duration.as_secs_f64());
    }

    pub fn record_blob(&self, kind: &str, outcome: &str) {
        self.migration_blobs_total
            .with_label_values(&[kind, outcome])
            .inc();
    }

    pub fn record_blob_failure(&self, kind: &str, step: &str, error_type: &str) {
        self.migration_blob_failures_total
            .with_label_values(&[kind, step, error_type])
            .inc();
    }

    pub fn record_rate_limit(&self, kind: &str) {
        self.migration_rate_limit_events_total
            .with_label_values(&[kind])
            .inc();
    }
}

pub fn migration_error_type(error: &MigrationError) -> &'static str {
    match error {
        MigrationError::Validation { .. } => "validation",
        MigrationError::Authentication { .. } => "authentication",
        MigrationError::BadRequest { .. } => "bad_request",
        MigrationError::Upstream { .. } => "upstream",
        MigrationError::Runtime { .. } => "runtime",
        MigrationError::RateLimitReached => "rate_limit",
    }
}
