use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub external_services: ExternalServices,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub workers: usize,
    pub concurrent_tasks_per_job: usize,
    pub rate_limit_window_secs: u64,
    pub rate_limit_max_requests: u64,
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExternalServices {
    pub s3_endpoint: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let server_port = env::var("SERVER_PORT").unwrap_or("9090".to_string());
        let worker_count = env::var("WORKER_COUNT").unwrap_or("2".to_string());
        let concurrent_tasks_per_job =
            env::var("CONCURRENT_TASKS_PER_JOB").unwrap_or("3".to_string());
        let s3_endpoint = env::var("ENDPOINT").expect("ENDPOINT environment variable not set");
        let rate_limit_window_secs = env::var("RATE_LIMIT_WINDOW_SECS").unwrap_or("60".to_string());
        let rate_limit_max_requests =
            env::var("RATE_LIMIT_MAX_REQUESTS").unwrap_or("60".to_string());

        Self {
            server: ServerConfig {
                port: server_port.parse().unwrap(),
                workers: worker_count.parse().unwrap(),
                concurrent_tasks_per_job: concurrent_tasks_per_job.parse().unwrap(),
                rate_limit_window_secs: rate_limit_window_secs.parse().unwrap(),
                rate_limit_max_requests: rate_limit_max_requests.parse().unwrap(),
                auth_token: env::var("AUTH_TOKEN").ok(),
            },
            external_services: ExternalServices { s3_endpoint },
        }
    }
}
