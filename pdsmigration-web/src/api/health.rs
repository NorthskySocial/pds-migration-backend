use crate::HttpResponse;
use actix_web::{get, Responder};

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Health check OK", body = String)
    ),
    tag = "pdsmigration-web"
)]
#[get("/health")]
pub async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::body::to_bytes;
    use actix_web::http::StatusCode;
    use actix_web::{test as actix_test, App};

    #[actix_rt::test]
    async fn health_endpoint_returns_ok_body() {
        let app = actix_test::init_service(App::new().service(health_check)).await;
        let req = actix_test::TestRequest::get().uri("/health").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(&body[..], b"OK");
    }

    #[actix_rt::test]
    async fn health_endpoint_does_not_match_post() {
        let app = actix_test::init_service(App::new().service(health_check)).await;
        let req = actix_test::TestRequest::post().uri("/health").to_request();
        let resp = actix_test::call_service(&app, req).await;
        assert_ne!(resp.status(), StatusCode::OK);
    }
}
