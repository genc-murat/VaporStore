pub mod api;
pub mod error;
pub mod storage;
pub mod xml;

use axum::{
    extract::DefaultBodyLimit,
    routing::get,
    Router,
};
use storage::MAX_OBJECT_SIZE;

// ─── App Factory ───────────────────────────────────────────────────────────

pub fn app(store: api::SharedStore) -> Router {
    Router::new()
        // Service-level
        .route("/", get(api::list_buckets))
        // Bucket operations (GET + PUT + DELETE on same path)
        .route(
            "/{bucket}",
            get(api::list_objects)
                .put(api::create_bucket)
                .delete(api::delete_bucket),
        )
        // Object operations — catch-all key to support slashes in key names
        .route(
            "/{bucket}/{*key}",
            get(api::get_object)
                .put(api::put_object)
                .head(api::head_object)
                .delete(api::delete_object),
        )
        // Explicit body size limit: 5MB + 1KB overhead for headers/metadata
        .layer(DefaultBodyLimit::max(MAX_OBJECT_SIZE + 1024))
        .fallback(api::not_found)
        .with_state(store)
}
