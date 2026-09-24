use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub port: u16,
    pub workers: usize,
    pub concurrent_tasks_per_job: usize,
    pub upload_max_attempts: u32,
    pub rate_limit_window_secs: u64,
    pub rate_limit_max_requests: u64,
    pub job_retention_secs: u64,
    pub artifact_retention_secs: u64,
    pub artifact_gc_interval_secs: u64,
    pub auth_token: Option<String>,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let server_port = env::var("SERVER_PORT").unwrap_or("9090".to_string());
        let worker_count = env::var("WORKER_COUNT").unwrap_or("2".to_string());
        let concurrent_tasks_per_job =
            env::var("CONCURRENT_TASKS_PER_JOB").unwrap_or("3".to_string());
        let upload_max_attempts = env::var("UPLOAD_MAX_ATTEMPTS").unwrap_or("3".to_string());
        let rate_limit_window_secs = env::var("RATE_LIMIT_WINDOW_SECS").unwrap_or("60".to_string());
        let rate_limit_max_requests =
            env::var("RATE_LIMIT_MAX_REQUESTS").unwrap_or("240".to_string());
        let job_retention_secs = env::var("JOB_RETENTION_SECS").unwrap_or("3600".to_string());
        let artifact_retention_secs =
            env::var("ARTIFACT_RETENTION_SECS").unwrap_or("86400".to_string());
        let artifact_gc_interval_secs: u64 = env::var("ARTIFACT_GC_INTERVAL_SECS")
            .unwrap_or("3600".to_string())
            .parse()
            .unwrap();
        assert!(
            artifact_gc_interval_secs > 0,
            "ARTIFACT_GC_INTERVAL_SECS must be greater than zero"
        );

        Self {
            port: server_port.parse().unwrap(),
            workers: worker_count.parse().unwrap(),
            concurrent_tasks_per_job: concurrent_tasks_per_job.parse().unwrap(),
            upload_max_attempts: upload_max_attempts.parse().unwrap(),
            rate_limit_window_secs: rate_limit_window_secs.parse().unwrap(),
            rate_limit_max_requests: rate_limit_max_requests.parse().unwrap(),
            job_retention_secs: job_retention_secs.parse().unwrap(),
            artifact_retention_secs: artifact_retention_secs.parse().unwrap(),
            artifact_gc_interval_secs,
            auth_token: env::var("AUTH_TOKEN").ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env_guard<F: FnOnce()>(vars: &[(&str, Option<&str>)], f: F) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        // snapshot previous values
        let previous: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(k, _)| ((*k).to_string(), env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(value) => env::set_var(k, value),
                None => env::remove_var(k),
            }
        }
        // run
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        // restore
        for (k, v) in previous {
            match v {
                Some(value) => env::set_var(&k, value),
                None => env::remove_var(&k),
            }
        }
        if let Err(payload) = result {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn from_env_uses_defaults_when_unset() {
        with_env_guard(
            &[
                ("SERVER_PORT", None),
                ("WORKER_COUNT", None),
                ("CONCURRENT_TASKS_PER_JOB", None),
                ("UPLOAD_MAX_ATTEMPTS", None),
                ("RATE_LIMIT_WINDOW_SECS", None),
                ("RATE_LIMIT_MAX_REQUESTS", None),
                ("JOB_RETENTION_SECS", None),
                ("ARTIFACT_RETENTION_SECS", None),
                ("ARTIFACT_GC_INTERVAL_SECS", None),
                ("AUTH_TOKEN", None),
            ],
            || {
                let cfg = AppConfig::from_env();
                assert_eq!(cfg.port, 9090);
                assert_eq!(cfg.workers, 2);
                assert_eq!(cfg.concurrent_tasks_per_job, 3);
                assert_eq!(cfg.upload_max_attempts, 3);
                assert_eq!(cfg.rate_limit_window_secs, 60);
                assert_eq!(cfg.rate_limit_max_requests, 240);
                assert_eq!(cfg.job_retention_secs, 3600);
                assert_eq!(cfg.artifact_retention_secs, 86400);
                assert_eq!(cfg.artifact_gc_interval_secs, 3600);
                assert!(cfg.auth_token.is_none());
            },
        );
    }

    #[test]
    fn from_env_reads_overrides() {
        with_env_guard(
            &[
                ("SERVER_PORT", Some("8181")),
                ("WORKER_COUNT", Some("4")),
                ("CONCURRENT_TASKS_PER_JOB", Some("12")),
                ("UPLOAD_MAX_ATTEMPTS", Some("7")),
                ("RATE_LIMIT_WINDOW_SECS", Some("30")),
                ("RATE_LIMIT_MAX_REQUESTS", Some("100")),
                ("JOB_RETENTION_SECS", Some("120")),
                ("ARTIFACT_RETENTION_SECS", Some("600")),
                ("ARTIFACT_GC_INTERVAL_SECS", Some("60")),
                ("AUTH_TOKEN", Some("secret-token")),
            ],
            || {
                let cfg = AppConfig::from_env();
                assert_eq!(cfg.port, 8181);
                assert_eq!(cfg.workers, 4);
                assert_eq!(cfg.concurrent_tasks_per_job, 12);
                assert_eq!(cfg.upload_max_attempts, 7);
                assert_eq!(cfg.rate_limit_window_secs, 30);
                assert_eq!(cfg.rate_limit_max_requests, 100);
                assert_eq!(cfg.job_retention_secs, 120);
                assert_eq!(cfg.artifact_retention_secs, 600);
                assert_eq!(cfg.artifact_gc_interval_secs, 60);
                assert_eq!(cfg.auth_token.as_deref(), Some("secret-token"));
            },
        );
    }

    #[test]
    #[should_panic]
    fn from_env_panics_on_invalid_port() {
        with_env_guard(&[("SERVER_PORT", Some("not-a-number"))], || {
            let _ = AppConfig::from_env();
        });
    }

    #[test]
    #[should_panic(expected = "ARTIFACT_GC_INTERVAL_SECS must be greater than zero")]
    fn from_env_rejects_zero_artifact_gc_interval() {
        with_env_guard(&[("ARTIFACT_GC_INTERVAL_SECS", Some("0"))], || {
            let _ = AppConfig::from_env();
        });
    }
}
