use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;
use vaporstore::{app, storage, storage::StorageBackend};

#[tokio::main]
async fn main() {
    // Initialise tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "vaporstore=info".to_string())
                .as_str(),
        )
        .init();

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9353);

    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(storage::InMemoryBackend::new());

    // ── Background TTL reaper (every 30 seconds) ──────────────────────────────
    {
        let reaper_store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let store_clone = Arc::clone(&reaper_store);
                let removed = store_clone.cleanup_expired().await;
                if removed > 0 {
                    info!("TTL reaper: removed {} expired object(s)", removed);
                }
            }
        });
    }

    let app = app(Arc::clone(&store));

    let addr = format!("0.0.0.0:{}", port);
    info!("VaporStore listening on http://{}", addr);
    info!(
        "Max object size: {} MB | Default TTL: {}s | TTL reaper: every 30s",
        storage::MAX_OBJECT_SIZE / (1024 * 1024),
        storage::DEFAULT_TTL_SECONDS
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    axum::serve(listener, app).await.expect("Server error");
}
