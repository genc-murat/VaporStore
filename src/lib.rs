pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod hybrid;
pub mod persistence;
pub mod storage;
pub mod xml;

use axum::{
    extract::DefaultBodyLimit,
    routing::get,
    Router,
};
use tower_http::{
    cors::CorsLayer,
    trace::TraceLayer,
    compression::CompressionLayer,
};
use tower::ServiceBuilder;

use config::Config;

// ─── App Factory ───────────────────────────────────────────────────────────

pub fn app(store: api::SharedStore) -> Router {
    app_with_config(store, Config::default())
}

pub fn app_with_config(store: api::SharedStore, config: Config) -> Router {
    let body_limit = config.max_object_size + 1024;

    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(axum::middleware::from_fn(auth::auth_middleware));

    let mut router = Router::new()
        // Service-level
        .route("/", get(api::list_buckets))
        .route("/health", get(api::health))
        .route("/metrics", get(api::metrics))
        // Bucket operations
        .route(
            "/{bucket}",
            get(api::list_objects)
                .put(api::create_bucket)
                .delete(api::delete_bucket)
                .head(api::head_bucket),
        )
        .route(
            "/{bucket}/",
            get(api::list_objects)
                .put(api::create_bucket)
                .delete(api::delete_bucket)
                .head(api::head_bucket),
        )
        // Object operations
        .route(
            "/{bucket}/{*key}",
            get(api::get_object)
                .post(api::post_object)
                .put(api::put_object)
                .head(api::head_object)
                .delete(api::delete_object),
        )
        .fallback(api::not_found)
        .layer(middleware)
        .with_state(store);

    // Optional rate limiting
    if config.rate_limit_rps > 0 {
        use tower_governor::GovernorLayer;
        use tower_governor::governor::GovernorConfigBuilder;

        let governor_conf = GovernorConfigBuilder::default()
            .per_second(config.rate_limit_rps)
            .burst_size(config.rate_limit_rps as u32 * 2)
            .finish()
            .expect("Failed to build rate limiter config");

        router = router.layer(GovernorLayer {
            config: std::sync::Arc::new(governor_conf),
        });
    }

    router
}
