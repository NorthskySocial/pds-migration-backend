use futures::future::LocalBoxFuture;
use std::collections::HashMap;
use std::future::{ready, Ready};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use actix_web::body::{BoxBody, EitherBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;
use actix_web::ResponseError;

use crate::errors::ApiError;

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

struct RateLimiterInner {
    window: Duration,
    max_requests: u64,
    // Map of ip -> (window_start, count)
    state: Mutex<HashMap<IpAddr, (Instant, u64)>>,
}

impl RateLimiter {
    pub fn new(max_requests: u64, window: Duration) -> Self {
        Self {
            inner: Arc::new(RateLimiterInner {
                window,
                max_requests,
                state: Mutex::new(HashMap::new()),
            }),
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimiter
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody, B>>;
    type Error = Error;
    type Transform = RateLimiterMiddleware<S, B>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RateLimiterMiddleware {
            service: Arc::new(service),
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }))
    }
}

pub struct RateLimiterMiddleware<S, B> {
    service: Arc<S>,
    inner: Arc<RateLimiterInner>,
    _phantom: std::marker::PhantomData<B>,
}

impl<S, B> Service<ServiceRequest> for RateLimiterMiddleware<S, B>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<EitherBody<BoxBody, B>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let peer_ip = req
            .connection_info()
            .realip_remote_addr()
            .and_then(|s| s.split(':').next())
            .and_then(|ip_str| ip_str.parse::<IpAddr>().ok())
            .or_else(|| req.peer_addr().map(|s| s.ip()));

        let inner = self.inner.clone();
        let service = self.service.clone();

        let path = req.path().to_string();
        let is_exempt = path.starts_with("/jobs/")
            || path == "/jobs"
            || path == "/health"
            || path == "/metrics";

        Box::pin(async move {
            if !is_exempt {
                if let Some(ip) = peer_ip {
                    let now = Instant::now();
                    let mut map = inner.state.lock().expect("rate limiter mutex poisoned");
                    let entry = map.entry(ip).or_insert((now, 0));

                    // Reset window if elapsed
                    if now.duration_since(entry.0) >= inner.window {
                        *entry = (now, 0);
                    }

                    // Check before incrementing: allow exactly max_requests within window
                    if entry.1 >= inner.max_requests {
                        // Too many requests
                        drop(map);
                        let api_err = ApiError::RateLimit {
                            message: "Rate limit exceeded".to_string(),
                        };
                        let resp = api_err.error_response();
                        return Ok(req.into_response(resp).map_into_left_body());
                    }

                    // Increment count and proceed
                    entry.1 += 1;
                }
            }

            let res = service.call(req).await?;
            Ok(res.map_into_right_body())
        })
    }
}
