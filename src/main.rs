use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;
use vaporstore::{
    app_with_config, config::Config,
    hybrid::HybridBackend,
    storage::{InMemoryBackend, StorageBackend},
};

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

    let config = Config::from_env();
    let port = config.port;
    let reaper_interval = config.reaper_interval_seconds;
    let persistence_enabled = config.persistence_enabled;
    let snapshot_interval = config.snapshot_interval_seconds;

    // ── Choose backend ────────────────────────────────────────────────────────
    let hybrid: Option<Arc<HybridBackend>>;
    let store: Arc<dyn StorageBackend + Send + Sync>;

    if persistence_enabled {
        info!("Disk persistence ENABLED (dir: {})", config.data_dir);
        let hb = Arc::new(HybridBackend::load(config.clone()));
        hybrid = Some(Arc::clone(&hb));
        store = hb;
    } else {
        info!("Disk persistence DISABLED (in-memory only)");
        hybrid = None;
        store = Arc::new(InMemoryBackend::with_config(config.clone()));
    }

    // ── Background TTL reaper ─────────────────────────────────────────────────
    {
        let reaper_store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(reaper_interval));
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

    // ── Periodic snapshot timer ───────────────────────────────────────────────
    if let Some(ref hb) = hybrid {
        if snapshot_interval > 0 {
            let snap_hb = Arc::clone(hb);
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(snapshot_interval));
                loop {
                    interval.tick().await;
                    if let Err(e) = snap_hb.save_snapshot() {
                        tracing::warn!("Periodic snapshot failed: {}", e);
                    }
                }
            });
        }
    }

    let app = app_with_config(Arc::clone(&store), config.clone());

    let addr = format!("0.0.0.0:{}", port);
    info!("VaporStore listening on http://{}", addr);
    info!(
        "Max object size: {} MB | Default TTL: {}s | Reaper interval: {}s",
        config.max_object_size / (1024 * 1024),
        config.default_ttl_seconds,
        config.reaper_interval_seconds,
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");

    // ── Graceful shutdown ─────────────────────────────────────────────────────
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Server error");

    // ── Persist state on shutdown ─────────────────────────────────────────────
    if let Some(ref hb) = hybrid {
        hb.shutdown();
    }

    info!("VaporStore shut down gracefully.");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
        _ = terminate => info!("Received SIGTERM, shutting down..."),
    }
}

