pub mod api;
pub mod error;
pub mod storage;
pub mod xml;
pub mod auth;

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
use storage::MAX_OBJECT_SIZE;
use tower::ServiceBuilder;

// ─── App Factory ───────────────────────────────────────────────────────────

pub fn app(store: api::SharedStore) -> Router {
    let middleware = ServiceBuilder::new()
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .layer(CompressionLayer::new())
        .layer(DefaultBodyLimit::max(MAX_OBJECT_SIZE + 1024))
        .layer(axum::middleware::from_fn(auth::auth_middleware));

    let router = Router::new()
        // Service-level
        .route("/", get(api::list_buckets))
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

    router
}
