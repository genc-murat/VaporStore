//! Prometheus metrics middleware with histogram for latency percentiles.

use axum::{
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use chrono::Utc;
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec, Registry, HistogramOpts, Opts};
use std::sync::OnceLock;
use tracing::debug;

/// Global Prometheus registry
pub static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Request latency histogram (in seconds)
pub static REQUEST_LATENCY: OnceLock<HistogramVec> = OnceLock::new();

/// Request counter by method and status
pub static REQUEST_COUNT: OnceLock<IntCounterVec> = OnceLock::new();

/// In-flight requests gauge
pub static INFLIGHT_REQUESTS: OnceLock<IntGaugeVec> = OnceLock::new();

/// Initialize Prometheus metrics
pub fn init_metrics() -> Result<(), prometheus::Error> {
    let registry = Registry::new();
    
    // Request latency histogram with custom buckets
    let latency_opts = HistogramOpts::new(
        "vaporstore_request_latency_seconds",
        "HTTP request latency in seconds",
    )
    .buckets(vec![
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]);
    let latency = HistogramVec::new(latency_opts, &["method", "endpoint", "status"])?;
    registry.register(Box::new(latency.clone()))?;
    
    // Request counter
    let count_opts = Opts::new(
        "vaporstore_requests_total",
        "Total number of HTTP requests",
    );
    let count = IntCounterVec::new(count_opts, &["method", "endpoint", "status"])?;
    registry.register(Box::new(count.clone()))?;
    
    // In-flight requests gauge
    let inflight_opts = Opts::new(
        "vaporstore_inflight_requests",
        "Number of requests currently being processed",
    );
    let inflight = IntGaugeVec::new(inflight_opts, &["method", "endpoint"])?;
    registry.register(Box::new(inflight.clone()))?;
    
    REGISTRY.set(registry).expect("Failed to set global registry");
    REQUEST_LATENCY.set(latency).expect("Failed to set global latency");
    REQUEST_COUNT.set(count).expect("Failed to set global count");
    INFLIGHT_REQUESTS.set(inflight).expect("Failed to set global inflight");
    
    Ok(())
}

/// Get the global registry
pub fn get_registry() -> &'static Registry {
    REGISTRY.get().expect("Metrics not initialized. Call init_metrics() first.")
}

/// Get or create default metrics (for testing)
pub fn get_or_init_default() -> &'static Registry {
    REGISTRY.get_or_init(|| {
        let registry = Registry::new();
        
        // Create default metrics
        if let Ok(latency) = HistogramVec::new(
            HistogramOpts::new("vaporstore_request_latency_seconds", "HTTP request latency in seconds")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["method", "endpoint", "status"]
        ) {
            let _ = registry.register(Box::new(latency.clone()));
            let _ = REQUEST_LATENCY.set(latency);
        }
        
        if let Ok(count) = IntCounterVec::new(
            Opts::new("vaporstore_requests_total", "Total number of HTTP requests"),
            &["method", "endpoint", "status"]
        ) {
            let _ = registry.register(Box::new(count.clone()));
            let _ = REQUEST_COUNT.set(count);
        }
        
        if let Ok(inflight) = IntGaugeVec::new(
            Opts::new("vaporstore_inflight_requests", "Number of requests currently being processed"),
            &["method", "endpoint"]
        ) {
            let _ = registry.register(Box::new(inflight.clone()));
            let _ = INFLIGHT_REQUESTS.set(inflight);
        }
        
        registry
    })
}

/// Middleware to track request metrics
pub async fn metrics_middleware(
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    
    // Track in-flight requests
    if let Some(inflight) = INFLIGHT_REQUESTS.get() {
        inflight
            .with_label_values(&[&method, &path])
            .inc();
    }
    
    let start = Utc::now();
    let response = next.run(req).await;
    let duration = Utc::now().signed_duration_since(start);
    
    let status = response.status().as_u16().to_string();
    let latency_secs = duration.num_milliseconds() as f64 / 1000.0;
    
    // Record latency
    if let Some(latency) = REQUEST_LATENCY.get() {
        latency
            .with_label_values(&[&method, &path, &status])
            .observe(latency_secs);
    }
    
    // Record request count
    if let Some(count) = REQUEST_COUNT.get() {
        count
            .with_label_values(&[&method, &path, &status])
            .inc();
    }
    
    // Decrement in-flight
    if let Some(inflight) = INFLIGHT_REQUESTS.get() {
        inflight
            .with_label_values(&[&method, &path])
            .dec();
    }
    
    debug!("Request {} {} completed in {:.3}s with status {}", method, path, latency_secs, status);
    
    Ok(response)
}
