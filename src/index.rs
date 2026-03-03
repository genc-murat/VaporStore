//! Prefix-based index for efficient key lookups and prefix scans.
//!
//! This module provides a concurrent index that maps object keys
//! enabling efficient prefix-based listing operations without scanning
//! the entire bucket. Uses a BTreeMap for ordered key storage.

use std::collections::BTreeMap;
use std::sync::RwLock;

/// A concurrent prefix index for a single bucket.
/// Uses a BTreeMap for ordered key storage, enabling efficient range queries.
#[derive(Debug)]
pub struct PrefixIndex {
    keys: RwLock<BTreeMap<String, ()>>,
}

impl PrefixIndex {
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(BTreeMap::new()),
        }
    }

    /// Insert a key into the index
    pub fn insert(&self, key: &str) {
        if let Ok(mut map) = self.keys.write() {
            map.insert(key.to_string(), ());
        }
    }

    /// Remove a key from the index
    pub fn remove(&self, key: &str) {
        if let Ok(mut map) = self.keys.write() {
            map.remove(key);
        }
    }

    /// Get all keys with the given prefix, up to a limit
    pub fn get_prefix_keys(&self, prefix: &str, limit: usize) -> Vec<String> {
        let map = match self.keys.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };

        if prefix.is_empty() {
            return map.keys().take(limit).cloned().collect();
        }

        // Use range query for efficient prefix matching
        let mut results = Vec::with_capacity(limit.min(1024));
        
        // Start from prefix, end at prefix with last char incremented
        let start = prefix.to_string();
        let end = increment_prefix(prefix);
        
        for (key, _) in map.range(start..end) {
            if results.len() >= limit {
                break;
            }
            results.push(key.clone());
        }
        
        results
    }

    /// Get total number of keys in the index
    pub fn len(&self) -> usize {
        self.keys.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for PrefixIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Given a prefix like "foo/bar", returns the next prefix for range queries.
/// Uses the standard trick: append char::MAX to get the upper bound
fn increment_prefix(prefix: &str) -> String {
    // For range queries, we want all keys that start with prefix
    // The standard trick is to use prefix..prefix + char::MAX
    // But since we need an exclusive end, we need to be clever
    // 
    // Actually, the simplest approach: use prefix with a high suffix
    // that's guaranteed to be after any valid key with that prefix.
    // 
    // Better approach: use std::str::from_utf8 to create a byte-level increment
    // 
    // Simplest working approach: just use prefix + '{' (ASCII 123, after 'z')
    // This works for typical S3 keys which use alphanumeric + common chars
    
    let mut result = prefix.to_string();
    // Use char::MAX as the suffix - this ensures we get all keys starting with prefix
    result.push(char::MAX);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_index_insert_and_query() {
        let index = PrefixIndex::new();
        
        index.insert("a/1.txt");
        index.insert("a/2.txt");
        index.insert("b/1.txt");
        index.insert("c.txt");

        // Query all
        let all = index.get_prefix_keys("", 100);
        assert_eq!(all.len(), 4);

        // Query with prefix "a/"
        let a_prefix = index.get_prefix_keys("a/", 100);
        assert_eq!(a_prefix.len(), 2);
        assert!(a_prefix.contains(&"a/1.txt".to_string()));
        assert!(a_prefix.contains(&"a/2.txt".to_string()));

        // Query with prefix "a"
        let a_prefix2 = index.get_prefix_keys("a", 100);
        assert_eq!(a_prefix2.len(), 2);

        // Query non-existent prefix
        let empty = index.get_prefix_keys("z/", 100);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_prefix_index_remove() {
        let index = PrefixIndex::new();
        
        index.insert("a/1.txt");
        index.insert("a/2.txt");
        index.insert("b/1.txt");

        index.remove("a/1.txt");

        let all = index.get_prefix_keys("", 100);
        assert_eq!(all.len(), 2);
        assert!(!all.contains(&"a/1.txt".to_string()));

        let a_prefix = index.get_prefix_keys("a/", 100);
        assert_eq!(a_prefix.len(), 1);
        assert!(a_prefix.contains(&"a/2.txt".to_string()));
    }

    #[test]
    fn test_prefix_index_limit() {
        let index = PrefixIndex::new();
        
        for i in 0..100 {
            index.insert(&format!("file{:03}.txt", i));
        }

        let limited = index.get_prefix_keys("", 10);
        assert_eq!(limited.len(), 10);

        let all = index.get_prefix_keys("", 1000);
        assert_eq!(all.len(), 100);
    }
}
