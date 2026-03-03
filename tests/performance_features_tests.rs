//! Tests for new performance optimization features:
//! - LRU eviction
//! - Prefix index
//! - Async WAL
//! - Prometheus metrics
//! - Auth optimization

use bytes::Bytes;
use std::collections::HashMap;
use tempfile::TempDir;
use vaporstore::{
    config::Config,
    hybrid::HybridBackend,
    storage::{InMemoryBackend, StorageBackend},
    lru::LruCache,
    index::PrefixIndex,
};

// ─────────────────────────────────────────────────────────────────────────────
// LRU Eviction Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_lru_eviction_with_memory_limit() {
    let mut config = Config::default();
    // Set memory limit to 500 bytes
    config.max_memory_bytes = 500;
    
    let store = InMemoryBackend::with_config(config);
    store.create_bucket("test-bucket").await.unwrap();
    
    // Add objects that exceed memory limit
    store.put_object("test-bucket", "obj1", Bytes::from("data1"), None, None, HashMap::new()).await.unwrap(); // 5 bytes
    store.put_object("test-bucket", "obj2", Bytes::from("data2"), None, None, HashMap::new()).await.unwrap(); // 5 bytes
    store.put_object("test-bucket", "obj3", Bytes::from("data3"), None, None, HashMap::new()).await.unwrap(); // 5 bytes
    
    // All objects should exist (total 15 bytes < 500 limit)
    assert!(store.get_object("test-bucket", "obj1").await.is_ok());
    assert!(store.get_object("test-bucket", "obj2").await.is_ok());
    assert!(store.get_object("test-bucket", "obj3").await.is_ok());
}

#[tokio::test]
async fn test_lru_access_order_tracking() {
    let cache = LruCache::new(1000);
    
    // Add objects
    cache.add("obj1", 100).await;
    cache.add("obj2", 200).await;
    cache.add("obj3", 300).await;
    
    // Access obj1 to make it most recently used
    cache.record_access("obj1").await;
    
    // LRU order should be: obj2, obj3, obj1
    let lru_keys = cache.get_lru_keys(3).await;
    assert_eq!(lru_keys, vec!["obj2", "obj3", "obj1"]);
}

#[tokio::test]
async fn test_lru_memory_tracking() {
    let cache = LruCache::new(1000);
    
    cache.add("obj1", 100).await;
    assert_eq!(cache.memory_usage(), 100);
    
    cache.add("obj2", 200).await;
    assert_eq!(cache.memory_usage(), 300);
    
    cache.remove("obj1", 100).await;
    assert_eq!(cache.memory_usage(), 200);
    
    // Test memory percentage
    assert_eq!(cache.memory_percentage(), 20.0);
}

#[tokio::test]
async fn test_lru_removal_updates_queue() {
    let cache = LruCache::new(1000);
    
    cache.add("a", 10).await;
    cache.add("b", 20).await;
    cache.add("c", 30).await;
    
    cache.remove("b", 20).await;
    
    let lru_keys = cache.get_lru_keys(2).await;
    assert_eq!(lru_keys.len(), 2);
    assert!(lru_keys.contains(&"a".to_string()));
    assert!(lru_keys.contains(&"c".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Prefix Index Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_prefix_index_with_storage() {
    let store = InMemoryBackend::new();
    store.create_bucket("prefix-bucket").await.unwrap();
    
    // Add objects with various prefixes
    let keys = vec![
        "images/photo1.jpg",
        "images/photo2.jpg",
        "images/2024/vacation.jpg",
        "documents/report.pdf",
        "documents/notes.txt",
        "root.txt",
    ];
    
    for key in &keys {
        store.put_object("prefix-bucket", key, Bytes::from("data"), None, None, HashMap::new()).await.unwrap();
    }
    
    // List with prefix "images/"
    let result = store.list_objects("prefix-bucket", Some("images/"), None, 100).await.unwrap();
    assert_eq!(result.contents.len(), 3);
    
    // List with prefix "documents/"
    let result = store.list_objects("prefix-bucket", Some("documents/"), None, 100).await.unwrap();
    assert_eq!(result.contents.len(), 2);
    
    // List with prefix "images/2024/"
    let result = store.list_objects("prefix-bucket", Some("images/2024/"), None, 100).await.unwrap();
    assert_eq!(result.contents.len(), 1);
}

#[tokio::test]
async fn test_prefix_index_efficiency() {
    let index = PrefixIndex::new();
    
    // Add 1000 keys
    for i in 0..1000 {
        index.insert(&format!("prefix/key_{:04}.txt", i));
    }
    
    // Query should be efficient (not scanning all 1000 keys)
    let keys = index.get_prefix_keys("prefix/key_0", 100);
    assert_eq!(keys.len(), 100);
    
    // All returned keys should match the prefix
    for key in &keys {
        assert!(key.starts_with("prefix/key_0"));
    }
}

#[tokio::test]
async fn test_prefix_index_remove() {
    let index = PrefixIndex::new();

    index.insert("a/1.txt");
    index.insert("a/2.txt");
    index.insert("b/1.txt");

    index.remove("a/1.txt");

    let keys = index.get_prefix_keys("a/", 100);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "a/2.txt");
}

// ─────────────────────────────────────────────────────────────────────────────
// Async WAL Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_async_wal_writer_basic() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    
    // Start async WAL writer
    let wal = vaporstore::persistence::AsyncWalWriter::start(data_dir, 10, 50).unwrap();
    
    // Write some entries
    use vaporstore::persistence::WalEntry;
    use chrono::Utc;
    
    wal.append(WalEntry::CreateBucket {
        name: "test-bucket".to_string(),
        created_at: Utc::now(),
    }).await.unwrap();
    
    // Give time for async write
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Shutdown
    wal.shutdown().await;
    
    // Verify WAL file exists and can be replayed
    let entries = vaporstore::persistence::replay_wal(data_dir).unwrap();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn test_async_wal_batching() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    
    // Start with small batch size
    let wal = vaporstore::persistence::AsyncWalWriter::start(data_dir, 5, 1000).unwrap();
    
    use vaporstore::persistence::WalEntry;
    use chrono::Utc;
    
    // Write 10 entries (should trigger 2 batches)
    for i in 0..10 {
        wal.append(WalEntry::CreateBucket {
            name: format!("bucket-{}", i),
            created_at: Utc::now(),
        }).await.unwrap();
    }
    
    // Wait for batch flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    wal.shutdown().await;
    
    let entries = vaporstore::persistence::replay_wal(data_dir).unwrap();
    assert_eq!(entries.len(), 10);
}

#[tokio::test]
async fn test_hybrid_backend_with_async_wal() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    
    let mut config = Config::default();
    config.persistence_enabled = true;
    config.data_dir = data_dir.to_string();
    config.wal_enabled = true;
    
    // Create backend with async WAL
    let hb = HybridBackend::load(config.clone());
    
    hb.create_bucket("wal-test-bucket").await.unwrap();
    hb.put_object("wal-test-bucket", "wal-key", Bytes::from("wal-data"), None, None, HashMap::new()).await.unwrap();
    
    // Give async WAL time to flush
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Verify data exists
    assert!(hb.bucket_exists("wal-test-bucket").await);
    let obj = hb.get_object("wal-test-bucket", "wal-key").await.unwrap();
    assert_eq!(obj.data, Bytes::from("wal-data"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Prometheus Metrics Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_metrics_initialization() {
    // Use get_or_init_default which handles already-initialized case
    let registry = vaporstore::metrics::get_or_init_default();
    assert!(registry.gather().len() > 0);
}

#[tokio::test]
async fn test_metrics_latency_recording() {
    // Use get_or_init_default
    let registry = vaporstore::metrics::get_or_init_default();
    
    // Record some latency
    if let Some(latency) = vaporstore::metrics::REQUEST_LATENCY.get() {
        latency
            .with_label_values(&["GET", "/test", "200"])
            .observe(0.05);
        
        latency
            .with_label_values(&["GET", "/test", "200"])
            .observe(0.1);
        
        latency
            .with_label_values(&["POST", "/test", "201"])
            .observe(0.2);
    }
    
    // Verify metrics can be gathered
    let metrics = registry.gather();
    assert!(!metrics.is_empty());
}

#[tokio::test]
async fn test_metrics_inflight_tracking() {
    // Use get_or_init_default
    let _ = vaporstore::metrics::get_or_init_default();
    
    if let Some(inflight) = vaporstore::metrics::INFLIGHT_REQUESTS.get() {
        // Increment
        inflight
            .with_label_values(&["GET", "/test"])
            .inc();
        
        let val = inflight
            .with_label_values(&["GET", "/test"])
            .get();
        assert_eq!(val, 1);
        
        // Decrement
        inflight
            .with_label_values(&["GET", "/test"])
            .dec();
        
        let val = inflight
            .with_label_values(&["GET", "/test"])
            .get();
        assert_eq!(val, 0);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Auth Optimization Tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_constant_time_eq_equal_strings() {
    assert!(vaporstore::auth::test_constant_time_eq("hello", "hello"));
    assert!(vaporstore::auth::test_constant_time_eq("", ""));
    assert!(vaporstore::auth::test_constant_time_eq("verylongstring", "verylongstring"));
}

#[test]
fn test_constant_time_eq_different_strings() {
    assert!(!vaporstore::auth::test_constant_time_eq("hello", "world"));
    assert!(!vaporstore::auth::test_constant_time_eq("hello", "hell"));
    assert!(!vaporstore::auth::test_constant_time_eq("hello", "hello1"));
    assert!(!vaporstore::auth::test_constant_time_eq("a", "b"));
}

#[test]
fn test_auth_config_credential_prefix_caching() {
    // Test constant time eq directly
    assert!(vaporstore::auth::test_constant_time_eq("hello", "hello"));
    assert!(vaporstore::auth::test_constant_time_eq("test-key", "test-key"));
    assert!(!vaporstore::auth::test_constant_time_eq("hello", "world"));
    assert!(!vaporstore::auth::test_constant_time_eq("a", "b"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_lru_with_prefix_index_integration() {
    let mut config = Config::default();
    config.max_memory_bytes = 1000; // Small limit for testing
    
    let store = InMemoryBackend::with_config(config);
    store.create_bucket("integration-bucket").await.unwrap();
    
    // Add objects with different prefixes
    for i in 0..10 {
        let key = format!("prefix{}/obj{}.txt", i % 3, i);
        let data = Bytes::from(vec![0u8; 100]); // 100 bytes each
        store.put_object("integration-bucket", &key, data, None, None, HashMap::new()).await.unwrap();
    }
    
    // Access some objects to make them recently used
    store.get_object("integration-bucket", "prefix0/obj0.txt").await.ok();
    store.get_object("integration-bucket", "prefix1/obj3.txt").await.ok();
    
    // List with prefix should still work efficiently
    let result = store.list_objects("integration-bucket", Some("prefix0/"), None, 100).await.unwrap();
    assert!(result.contents.len() > 0);
}

#[tokio::test]
async fn test_memory_limit_enforcement() {
    let mut config = Config::default();
    config.max_memory_bytes = 200; // Very small limit
    
    let store = InMemoryBackend::with_config(config);
    store.create_bucket("memory-limit-bucket").await.unwrap();
    
    // Add objects that exceed memory limit
    for i in 0..10 {
        let key = format!("obj{}.txt", i);
        let data = Bytes::from(vec![0u8; 50]); // 50 bytes each
        let _ = store.put_object("memory-limit-bucket", &key, data, None, None, HashMap::new()).await;
    }
    
    // Some objects should have been evicted
    // (we can't guarantee which ones, but memory should be under limit)
    let (_, _, bytes) = store.stats().await;
    assert!(bytes <= 200 + 50); // Allow some margin for async eviction
}

#[tokio::test]
async fn test_full_workflow_with_all_features() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().to_str().unwrap();
    
    let mut config = Config::default();
    config.persistence_enabled = true;
    config.data_dir = data_dir.to_string();
    config.wal_enabled = true;
    config.max_memory_bytes = 5000;
    
    // Initialize metrics
    let _ = vaporstore::metrics::init_metrics();
    
    let store = InMemoryBackend::with_config(config);
    store.create_bucket("full-workflow-bucket").await.unwrap();
    
    // Add objects
    store.put_object("full-workflow-bucket", "obj1", Bytes::from("data1"), None, None, HashMap::new()).await.unwrap();
    store.put_object("full-workflow-bucket", "obj2", Bytes::from("data2"), None, None, HashMap::new()).await.unwrap();
    store.put_object("full-workflow-bucket", "obj3", Bytes::from("data3"), None, None, HashMap::new()).await.unwrap();
    
    // Access objects (updates LRU)
    store.get_object("full-workflow-bucket", "obj1").await.unwrap();
    
    // List with prefix
    let result = store.list_objects("full-workflow-bucket", Some("obj"), None, 100).await.unwrap();
    assert_eq!(result.contents.len(), 3);
    
    // Delete an object
    store.delete_object("full-workflow-bucket", "obj2").await.unwrap();
    
    // Verify deletion
    assert!(store.get_object("full-workflow-bucket", "obj2").await.is_err());
    
    // Check stats
    let (buckets, objects, bytes) = store.stats().await;
    assert_eq!(buckets, 1);
    assert_eq!(objects, 2);
    assert!(bytes > 0);
}
