use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;

pub const MAX_OBJECT_SIZE: usize = 5 * 1024 * 1024; // 5 MB
pub const DEFAULT_TTL_SECONDS: i64 = 300; // 5 minutes

/// A single stored object with metadata and TTL.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct StorageObject {
    pub key: String,
    pub data: Bytes,
    pub content_type: String,
    pub size: usize,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl StorageObject {
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }
}

/// Per-bucket storage: key → Arc<StorageObject> (zero-copy on read)
type BucketMap = DashMap<String, Arc<StorageObject>>;

/// Top-level store: bucket_name → BucketMap
#[derive(Default, Clone)]
pub struct ObjectStore {
    buckets: DashMap<String, BucketMap>,
}

#[derive(Debug)]
pub enum StoreError {
    NoSuchBucket,
    NoSuchKey,
    BucketAlreadyExists,
    EntityTooLarge,
    BucketNotEmpty,
}

impl ObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    // ─── Bucket operations ────────────────────────────────────────────────────

    pub fn create_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        if self.buckets.contains_key(bucket) {
            return Err(StoreError::BucketAlreadyExists);
        }
        self.buckets.insert(bucket.to_string(), DashMap::new());
        Ok(())
    }

    pub fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let entry = self.buckets.get(bucket).ok_or(StoreError::NoSuchBucket)?;
        if !entry.is_empty() {
            return Err(StoreError::BucketNotEmpty);
        }
        drop(entry);
        self.buckets.remove(bucket);
        Ok(())
    }

    pub fn list_buckets(&self) -> Vec<(String, DateTime<Utc>)> {
        self.buckets
            .iter()
            .map(|e| (e.key().clone(), Utc::now()))
            .collect()
    }

    pub fn bucket_exists(&self, bucket: &str) -> bool {
        self.buckets.contains_key(bucket)
    }

    // ─── Object operations ────────────────────────────────────────────────────

    /// Store an object. Returns the ETag.
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<String>,
        ttl_secs: Option<i64>,
        metadata: HashMap<String, String>,
    ) -> Result<String, StoreError> {
        if data.len() > MAX_OBJECT_SIZE {
            return Err(StoreError::EntityTooLarge);
        }

        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or(StoreError::NoSuchBucket)?;

        let etag = format!("{:x}", md5::compute(&data));
        let now = Utc::now();
        let ttl = ttl_secs.unwrap_or(DEFAULT_TTL_SECONDS);
        let expires_at = now + Duration::seconds(ttl);
        let size = data.len();

        let obj = Arc::new(StorageObject {
            key: key.to_string(),
            data,
            content_type: content_type
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            size,
            etag: etag.clone(),
            last_modified: now,
            expires_at,
            metadata,
        });

        bucket_map.insert(key.to_string(), obj);
        Ok(etag)
    }

    /// Retrieve an object (returns Arc — zero-copy, no allocation).
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or(StoreError::NoSuchBucket)?;

        let obj = bucket_map.get(key).ok_or(StoreError::NoSuchKey)?;

        if obj.is_expired() {
            drop(obj);
            bucket_map.remove(key);
            return Err(StoreError::NoSuchKey);
        }

        Ok(Arc::clone(obj.value()))
    }

    /// Retrieve only metadata (HEAD) — same zero-copy Arc path.
    pub fn head_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        self.get_object(bucket, key)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or(StoreError::NoSuchBucket)?;

        bucket_map.remove(key);
        Ok(())
    }

    /// List objects in a bucket with optional prefix and delimiter.
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> Result<ListResult, StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or(StoreError::NoSuchBucket)?;

        let now = Utc::now();
        let prefix = prefix.unwrap_or("");
        let delimiter = delimiter.unwrap_or("");

        let mut contents: Vec<ObjectMeta> = Vec::new();
        let mut common_prefixes: HashSet<String> = HashSet::new();
        let mut expired_keys: Vec<String> = Vec::new();

        for entry in bucket_map.iter() {
            let obj = entry.value();

            if obj.expires_at <= now {
                expired_keys.push(obj.key.clone());
                continue;
            }

            if !obj.key.starts_with(prefix) {
                continue;
            }

            if !delimiter.is_empty() {
                let after_prefix = &obj.key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter) {
                    let common = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                    common_prefixes.insert(common);
                    continue;
                }
            }

            contents.push(ObjectMeta {
                key: obj.key.clone(),
                size: obj.size,
                etag: obj.etag.clone(),
                last_modified: obj.last_modified,
            });

            if contents.len() >= max_keys {
                break;
            }
        }

        drop(bucket_map);

        // Batch cleanup of expired objects
        if !expired_keys.is_empty() {
            if let Some(bm) = self.buckets.get(bucket) {
                for k in expired_keys {
                    bm.remove(&k);
                }
            }
        }

        contents.sort_by(|a, b| a.key.cmp(&b.key));

        let mut sorted_prefixes: Vec<String> = common_prefixes.into_iter().collect();
        sorted_prefixes.sort();

        Ok(ListResult {
            contents,
            common_prefixes: sorted_prefixes,
            is_truncated: false,
        })
    }

    /// Remove all expired objects across all buckets. Called by background task.
    pub fn cleanup_expired(&self) -> usize {
        let now = Utc::now();
        let mut removed = 0usize;

        for bucket_entry in self.buckets.iter() {
            let expired_keys: Vec<String> = bucket_entry
                .value()
                .iter()
                .filter(|e| e.value().expires_at <= now)
                .map(|e| e.key().clone())
                .collect();

            for k in &expired_keys {
                bucket_entry.value().remove(k);
                removed += 1;
            }
        }

        removed
    }
}

#[derive(Debug)]
pub struct ObjectMeta {
    pub key: String,
    pub size: usize,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ListResult {
    pub contents: Vec<ObjectMeta>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_bucket_crud() {
        let store = ObjectStore::new();
        let bucket = "test-bucket";

        // Create
        store.create_bucket(bucket).unwrap();
        assert!(store.bucket_exists(bucket));

        // Duplicate
        assert!(matches!(
            store.create_bucket(bucket),
            Err(StoreError::BucketAlreadyExists)
        ));

        // List
        let buckets = store.list_buckets();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].0, bucket);

        // Delete (empty)
        store.delete_bucket(bucket).unwrap();
        assert!(!store.bucket_exists(bucket));

        // Delete (non-existent)
        assert!(matches!(
            store.delete_bucket(bucket),
            Err(StoreError::NoSuchBucket)
        ));
    }

    #[test]
    fn test_object_crud() {
        let store = ObjectStore::new();
        let bucket = "b1";
        let key = "k1";
        let data = Bytes::from("hello");

        store.create_bucket(bucket).unwrap();

        // Put
        let etag = store
            .put_object(bucket, key, data.clone(), None, None, HashMap::new())
            .unwrap();
        assert!(!etag.is_empty());

        // Get
        let obj = store.get_object(bucket, key).unwrap();
        assert_eq!(obj.data, data);
        assert_eq!(obj.etag, etag);

        // Head
        let head = store.head_object(bucket, key).unwrap();
        assert_eq!(head.etag, etag);

        // Delete
        store.delete_object(bucket, key).unwrap();
        assert!(matches!(
            store.get_object(bucket, key),
            Err(StoreError::NoSuchKey)
        ));
    }

    #[test]
    fn test_ttl_expiry() {
        let store = ObjectStore::new();
        let bucket = "ttl-bucket";
        let key = "expired-key";
        let data = Bytes::from("gone soon");

        store.create_bucket(bucket).unwrap();

        // Put with 1s TTL
        store
            .put_object(bucket, key, data, None, Some(1), HashMap::new())
            .unwrap();

        // Should exist initially
        assert!(store.get_object(bucket, key).is_ok());

        // Wait for expiry
        thread::sleep(StdDuration::from_millis(1100));

        // Should be gone (lazy removal)
        assert!(matches!(
            store.get_object(bucket, key),
            Err(StoreError::NoSuchKey)
        ));
    }

    #[test]
    fn test_list_objects() {
        let store = ObjectStore::new();
        let bucket = "list-bucket";
        store.create_bucket(bucket).unwrap();

        let keys = vec!["a/1.txt", "a/2.txt", "b/1.txt", "c.txt"];
        for k in &keys {
            store
                .put_object(bucket, k, Bytes::from("data"), None, None, HashMap::new())
                .unwrap();
        }

        // List all
        let res = store.list_objects(bucket, None, None, 1000).unwrap();
        assert_eq!(res.contents.len(), 4);

        // Prefix
        let res = store.list_objects(bucket, Some("a/"), None, 1000).unwrap();
        assert_eq!(res.contents.len(), 2);

        // Delimiter
        let res = store.list_objects(bucket, None, Some("/"), 1000).unwrap();
        // "a/" and "b/" are prefixes
        assert_eq!(res.common_prefixes.len(), 2);
        assert!(res.common_prefixes.contains(&"a/".to_string()));
        assert!(res.common_prefixes.contains(&"b/".to_string()));
    }

    #[test]
    fn test_entity_too_large() {
        let store = ObjectStore::new();
        let bucket = "large-bucket";
        store.create_bucket(bucket).unwrap();

        let large_data = Bytes::from(vec![0u8; MAX_OBJECT_SIZE + 1]);
        assert!(matches!(
            store.put_object(bucket, "too-big", large_data, None, None, HashMap::new()),
            Err(StoreError::EntityTooLarge)
        ));
    }
}
