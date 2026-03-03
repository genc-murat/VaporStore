//! LRU (Least Recently Used) cache for memory management.
//!
//! This module provides a concurrent LRU cache that tracks access order
//! and enables eviction of least-recently-used objects when memory limits are reached.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::RwLock;

/// A concurrent LRU tracker for object keys.
/// Uses a simple VecDeque for LRU ordering.
#[derive(Debug)]
pub struct LruCache {
    /// LRU queue: front = least recently used, back = most recently used
    queue: Arc<RwLock<VecDeque<String>>>,
    /// Current memory usage in bytes
    memory_bytes: std::sync::atomic::AtomicUsize,
    /// Maximum memory usage in bytes (0 = unlimited)
    max_memory_bytes: usize,
}

impl LruCache {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            queue: Arc::new(RwLock::new(VecDeque::with_capacity(1024))),
            memory_bytes: std::sync::atomic::AtomicUsize::new(0),
            max_memory_bytes,
        }
    }

    /// Record access to an object (mark as most recently used)
    pub async fn record_access(&self, key: &str) {
        let mut queue = self.queue.write().await;
        
        // Remove old position if exists
        if let Some(pos) = queue.iter().position(|k| k == key) {
            queue.remove(pos);
        }
        
        // Add to back (most recently used)
        queue.push_back(key.to_string());
    }

    /// Add a new object to the LRU cache
    pub async fn add(&self, key: &str, size: usize) {
        self.record_access(key).await;
        self.memory_bytes.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Remove an object from the LRU cache
    pub async fn remove(&self, key: &str, size: usize) {
        let mut queue = self.queue.write().await;
        
        // Find and remove the key
        if let Some(pos) = queue.iter().position(|k| k == key) {
            queue.remove(pos);
        }
        
        self.memory_bytes.fetch_sub(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update size of an existing object
    pub async fn update_size(&self, _key: &str, old_size: usize, new_size: usize) {
        let delta = new_size as isize - old_size as isize;
        if delta > 0 {
            self.memory_bytes.fetch_add(delta as usize, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.memory_bytes.fetch_sub(-delta as usize, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Get the least recently used keys for eviction
    /// Returns up to `count` keys
    pub async fn get_lru_keys(&self, count: usize) -> Vec<String> {
        let queue = self.queue.read().await;
        queue.iter().take(count).cloned().collect()
    }

    /// Get current memory usage
    pub fn memory_usage(&self) -> usize {
        self.memory_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check if we're over the memory limit
    pub fn is_over_limit(&self) -> bool {
        self.max_memory_bytes > 0 && self.memory_usage() > self.max_memory_bytes
    }

    /// Get memory usage percentage
    pub fn memory_percentage(&self) -> f64 {
        if self.max_memory_bytes == 0 {
            return 0.0;
        }
        (self.memory_usage() as f64 / self.max_memory_bytes as f64) * 100.0
    }

    /// Clear the cache
    pub async fn clear(&self) {
        self.queue.write().await.clear();
        self.memory_bytes.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get number of tracked objects
    pub async fn len(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Check if cache is empty
    pub async fn is_empty(&self) -> bool {
        self.queue.read().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lru_order() {
        let cache = LruCache::new(1000);
        
        cache.add("a", 10).await;
        cache.add("b", 20).await;
        cache.add("c", 30).await;
        
        // Access "a" to make it most recently used
        cache.record_access("a").await;
        
        // LRU order should be: b, c, a
        let lru = cache.get_lru_keys(3).await;
        assert_eq!(lru, vec!["b", "c", "a"]);
    }

    #[tokio::test]
    async fn test_lru_removal() {
        let cache = LruCache::new(1000);
        
        cache.add("a", 10).await;
        cache.add("b", 20).await;
        cache.add("c", 30).await;
        
        cache.remove("b", 20).await;
        
        // LRU order should be: a, c
        let lru = cache.get_lru_keys(2).await;
        assert_eq!(lru, vec!["a", "c"]);
    }

    #[tokio::test]
    async fn test_memory_tracking() {
        let cache = LruCache::new(1000);
        
        cache.add("a", 100).await;
        cache.add("b", 200).await;
        
        assert_eq!(cache.memory_usage(), 300);
        
        cache.remove("a", 100).await;
        assert_eq!(cache.memory_usage(), 200);
    }

    #[tokio::test]
    async fn test_memory_limit() {
        let cache = LruCache::new(250);
        
        cache.add("a", 100).await;
        cache.add("b", 200).await;
        
        assert!(cache.is_over_limit());
        assert!(cache.memory_percentage() > 100.0);
    }
}
