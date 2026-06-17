use actix_web::{http::StatusCode, test, web, App};
use pdsmigration_common::repo_car_path;
use pdsmigration_web::{
    api::{enqueue_export_repo_job_api, get_job_api},
    background_jobs::JobManager,
};
use serde_json::json;
use std::env;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{create_test_config, session_body, unique_did};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct EnvGuard {
    _guard: MutexGuard<'static, ()>,
    previous: Vec<(String, Option<String>)>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in &self.previous {
            match v {
                Some(value) => env::set_var(k, value),
                None => env::remove_var(k),
            }
        }
    }
}

async fn with_aws_test_env() -> EnvGuard {
    let guard = ENV_LOCK.lock().await;
    let vars = [
        ("AWS_ACCESS_KEY_ID", Some("test-access-key")),
        ("AWS_SECRET_ACCESS_KEY", Some("test-secret-key")),
        ("AWS_EC2_METADATA_DISABLED", Some("true")),
    ];

    let previous = vars
        .iter()
        .map(|(k, _)| ((*k).to_string(), env::var(k).ok()))
        .collect::<Vec<_>>();

    for (k, v) in vars {
        match v {
            Some(value) => env::set_var(k, value),
            None => env::remove_var(k),
        }
    }

    EnvGuard {
        _guard: guard,
        previous,
    }
}

#[actix_rt::test]
async fn export_repo_job_reaches_success_through_http_api() {
    let _env_guard = with_aws_test_env().await;
    let pds = MockServer::start().await;
    let s3 = MockServer::start().await;
    let did = unique_did("webexportrepojob");
    let payload: &[u8] = b"fake-car-bytes-from-get-repo";

    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.server.getSession"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_body(&did)))
        .mount(&pds)
        .await;
    Mock::given(method("GET"))
        .and(path("/xrpc/com.atproto.sync.getRepo"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("ratelimit-remaining", "1000")
                .set_body_bytes(payload),
        )
        .mount(&pds)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&s3)
        .await;

    let car_path = repo_car_path(&did).expect("downloads dir resolvable");
    let _ = std::fs::remove_file(&car_path);

    let mut config = create_test_config();
    config.external_services.s3_endpoint = s3.uri();
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(config))
            .app_data(web::Data::new(JobManager::new()))
            .service(enqueue_export_repo_job_api)
            .service(get_job_api),
    )
    .await;

    let enqueue = test::TestRequest::post()
        .uri("/jobs/export-repo")
        .set_json(json!({
            "pds_host": pds.uri(),
            "did": did,
            "token": "origin-jwt",
        }))
        .to_request();
    let enqueue_resp = test::call_service(&app, enqueue).await;
    assert_eq!(enqueue_resp.status(), StatusCode::ACCEPTED);
    let body = test::read_body(enqueue_resp).await;
    let enqueued: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let job_id = enqueued["job_id"].as_str().expect("job_id in 202 body");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let job = loop {
        let get = test::TestRequest::get()
            .uri(&format!("/jobs/{job_id}"))
            .to_request();
        let get_resp = test::call_service(&app, get).await;
        assert_eq!(get_resp.status(), StatusCode::OK);
        let job_body = test::read_body(get_resp).await;
        let job: serde_json::Value = serde_json::from_slice(&job_body).unwrap();
        match job["status"].as_str().unwrap() {
            "success" | "error" => break job,
            _ => {}
        }
        if std::time::Instant::now() >= deadline {
            panic!("job did not finish in time; last={job:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(
        job["status"].as_str().unwrap(),
        "success",
        "export-repo job should succeed against mocked services; got {job:?}"
    );
    assert_eq!(job["id"].as_str().unwrap(), job_id);
    let progress = &job["progress"];
    assert_eq!(progress["successful_blobs"].as_u64(), Some(1));
    assert_eq!(progress["invalid_blobs"].as_u64(), Some(0));
    assert_eq!(progress["total"].as_u64(), Some(1));

    let _ = std::fs::remove_file(&car_path);
}
