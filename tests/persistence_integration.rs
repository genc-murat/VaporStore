use std::collections::HashMap;
use bytes::Bytes;
use tempfile::TempDir;
use vaporstore::{
    config::Config,
    hybrid::HybridBackend,
    storage::{StorageBackend},
};

fn get_persistence_config(data_dir: &str) -> Config {
    let mut config = Config::default();
    config.persistence_enabled = true;
    config.data_dir = data_dir.to_string();
    config.wal_enabled = true;
    config
}

#[tokio::test]
async fn test_persistence_full_snapshot_recovery() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    let config = get_persistence_config(data_dir);

    // 1. Initial run: Create data
    {
        let hb = HybridBackend::load(config.clone());
        hb.create_bucket("bucket1").await.unwrap();
        hb.put_object("bucket1", "key1", Bytes::from("data1"), None, None, HashMap::new()).await.unwrap();

        // Graceful shutdown saves snapshot
        hb.shutdown().await;
        // Give async WAL time to flush
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // 2. Restart: Verify data reloaded from snapshot
    {
        let hb = HybridBackend::load(config.clone());
        assert!(hb.bucket_exists("bucket1").await);
        let obj = hb.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(obj.data, Bytes::from("data1"));
    }
}

#[tokio::test]
async fn test_persistence_wal_replay_on_crash() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    let config = get_persistence_config(data_dir);

    // 1. Initial run: Create data
    {
        let hb = HybridBackend::load(config.clone());
        hb.create_bucket("bucket-crash").await.unwrap();
        hb.put_object("bucket-crash", "crash-key", Bytes::from("crash-data"), None, None, HashMap::new()).await.unwrap();

        // Give async WAL time to flush before "crash"
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        // NO shutdown() call here - simulating a crash/process kill
        // The data should be in the WAL but not in a snapshot
    }

    // 2. Restart: Verify data reloaded via WAL replay
    {
        let hb = HybridBackend::load(config.clone());
        assert!(hb.bucket_exists("bucket-crash").await);
        let obj = hb.get_object("bucket-crash", "crash-key").await.unwrap();
        assert_eq!(obj.data, Bytes::from("crash-data"));
    }
}

#[tokio::test]
async fn test_persistence_deletion_recovery() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    let config = get_persistence_config(data_dir);

    // 1. Create data
    {
        let hb = HybridBackend::load(config.clone());
        hb.create_bucket("del-bucket").await.unwrap();
        hb.put_object("del-bucket", "del-key", Bytes::from("del-data"), None, None, HashMap::new()).await.unwrap();
        hb.shutdown().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // 2. Delete data (only in WAL)
    {
        let hb = HybridBackend::load(config.clone());
        hb.delete_object("del-bucket", "del-key").await.unwrap();
        // Give async WAL time to flush
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        // Crash again (no snapshot)
    }

    // 3. Restart: Verify deletion persists (snapshot + WAL replay)
    {
        let hb = HybridBackend::load(config.clone());
        assert!(hb.bucket_exists("del-bucket").await);
        let result = hb.get_object("del-bucket", "del-key").await;
        assert!(result.is_err());
    }
}

#[tokio::test]
async fn test_persistence_bucket_deletion_recovery() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    let config = get_persistence_config(data_dir);

    // 1. Create bucket
    {
        let hb = HybridBackend::load(config.clone());
        hb.create_bucket("temp-bucket").await.unwrap();
        hb.shutdown().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // 2. Delete bucket (WAL only)
    {
        let hb = HybridBackend::load(config.clone());
        hb.delete_bucket("temp-bucket").await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    // 3. Restart: Verify bucket is gone
    {
        let hb = HybridBackend::load(config.clone());
        assert!(!hb.bucket_exists("temp-bucket").await);
    }
}
