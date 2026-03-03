use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;

use crate::config::Config;
use crate::index::PrefixIndex;
use crate::lru::LruCache;

/// A single stored object with metadata and TTL.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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

/// Bucket entry: (creation_date, object_map, prefix_index)
struct BucketEntry {
    created_at: DateTime<Utc>,
    objects: BucketMap,
    index: PrefixIndex,
}

impl BucketEntry {
    fn new() -> Self {
        Self {
            created_at: Utc::now(),
            objects: DashMap::new(),
            index: PrefixIndex::new(),
        }
    }

    fn with_created_at(created_at: DateTime<Utc>) -> Self {
        Self {
            created_at,
            objects: DashMap::new(),
            index: PrefixIndex::new(),
        }
    }
}

#[derive(Debug)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub parts: DashMap<usize, (String, Bytes)>,
    pub metadata: HashMap<String, String>,
    pub content_type: Option<String>,
}

#[derive(Debug)]
pub enum StoreError {
    NoSuchBucket(String),
    NoSuchKey(String),
    BucketAlreadyExists(String),
    EntityTooLarge,
    BucketNotEmpty(String),
    InvalidBucketName(String),
    InvalidRequest(String),
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

use async_trait::async_trait;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError>;
    async fn list_buckets(&self) -> Vec<(String, DateTime<Utc>)>;
    async fn bucket_exists(&self, bucket: &str) -> bool;

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<String>,
        ttl_secs: Option<i64>,
        metadata: HashMap<String, String>,
    ) -> Result<String, StoreError>;

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError>;

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> Result<ListResult, StoreError>;

    async fn cleanup_expired(&self) -> usize;
    async fn stats(&self) -> (usize, usize, usize);

    async fn create_multipart_upload(&self, bucket: &str, key: &str, content_type: Option<String>, metadata: HashMap<String, String>) -> Result<String, StoreError>;
    async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str, part_number: usize, data: Bytes) -> Result<String, StoreError>;
    async fn complete_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str, parts_list: Vec<(usize, String)>) -> Result<String, StoreError>;
    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), StoreError>;
}

/// Top-level store: bucket_name → (creation_date, BucketMap)
pub struct InMemoryBackend {
    buckets: DashMap<String, BucketEntry>,
    uploads: DashMap<String, Arc<MultipartUpload>>,
    config: Config,
    object_count: AtomicUsize,
    total_bytes: AtomicUsize,
    lru: Option<Arc<LruCache>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Self {
        let lru = if config.max_memory_bytes > 0 {
            Some(Arc::new(LruCache::new(config.max_memory_bytes)))
        } else {
            None
        };
        
        Self {
            buckets: DashMap::new(),
            uploads: DashMap::new(),
            config,
            object_count: AtomicUsize::new(0),
            total_bytes: AtomicUsize::new(0),
            lru,
        }
    }

    fn validate_bucket_name(&self, bucket: &str) -> Result<(), StoreError> {
        if bucket.len() < 3 || bucket.len() > 63 {
            return Err(StoreError::InvalidBucketName(bucket.to_string()));
        }
        if !bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-') {
            return Err(StoreError::InvalidBucketName(bucket.to_string()));
        }
        if bucket.starts_with('.') || bucket.ends_with('.') || bucket.starts_with('-') || bucket.ends_with('-') {
            return Err(StoreError::InvalidBucketName(bucket.to_string()));
        }
        if bucket.contains("..") {
            return Err(StoreError::InvalidBucketName(bucket.to_string()));
        }
        Ok(())
    }

    // ── Raw helpers for persistence (snapshot restore + WAL replay) ────────

    /// Insert a bucket with a specific creation date (used during restore).
    pub fn insert_bucket_raw(&self, name: &str, created_at: DateTime<Utc>) {
        if !self.buckets.contains_key(name) {
            self.buckets.insert(name.to_string(), BucketEntry::with_created_at(created_at));
        }
    }

    /// Remove a bucket directly (used during WAL replay).
    pub fn remove_bucket_raw(&self, name: &str) {
        if let Some((_, entry)) = self.buckets.remove(name) {
            for obj in entry.objects.iter() {
                self.total_bytes.fetch_sub(obj.value().size, Ordering::Relaxed);
            }
            self.object_count.fetch_sub(entry.objects.len(), Ordering::Relaxed);
        }
    }

    /// Insert an object directly (used during restore, bypasses validation/limits).
    pub fn insert_object_raw(&self, bucket: &str, obj: StorageObject) {
        if let Some(entry) = self.buckets.get(bucket) {
            let key = obj.key.clone();
            let size = obj.size;
            let old = entry.objects.insert(key.clone(), Arc::new(obj));
            if let Some(old_obj) = old {
                self.total_bytes.fetch_sub(old_obj.size, Ordering::Relaxed);
            } else {
                self.object_count.fetch_add(1, Ordering::Relaxed);
                entry.index.insert(&key);
            }
            self.total_bytes.fetch_add(size, Ordering::Relaxed);
        }
    }

    /// Remove an object directly (used during WAL replay).
    pub fn remove_object_raw(&self, bucket: &str, key: &str) {
        if let Some(entry) = self.buckets.get(bucket) {
            if let Some((_, removed)) = entry.objects.remove(key) {
                self.object_count.fetch_sub(1, Ordering::Relaxed);
                self.total_bytes.fetch_sub(removed.size, Ordering::Relaxed);
                entry.index.remove(key);
            }
        }
    }

    /// Export the full current state as a serializable snapshot.
    pub fn to_snapshot(&self) -> crate::persistence::StoreSnapshot {
        use crate::persistence::{BucketSnapshot, StoreSnapshot};

        let buckets = self
            .buckets
            .iter()
            .map(|entry| {
                let name = entry.key().clone();
                let bucket_entry = entry.value();
                let objects: Vec<StorageObject> = bucket_entry
                    .objects
                    .iter()
                    .map(|obj_ref| obj_ref.value().as_ref().clone())
                    .collect();
                BucketSnapshot {
                    name,
                    created_at: bucket_entry.created_at,
                    objects,
                }
            })
            .collect();

        StoreSnapshot { buckets }
    }
}

#[async_trait]
impl StorageBackend for InMemoryBackend {
    async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        self.validate_bucket_name(bucket)?;
        if self.buckets.contains_key(bucket) {
            return Err(StoreError::BucketAlreadyExists(bucket.to_string()));
        }
        if self.config.max_buckets > 0 && self.buckets.len() >= self.config.max_buckets {
            return Err(StoreError::InvalidRequest(
                format!("Maximum number of buckets ({}) reached", self.config.max_buckets),
            ));
        }
        self.buckets.insert(bucket.to_string(), BucketEntry::new());
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let entry = self.buckets.get(bucket).ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;
        if !entry.value().objects.is_empty() {
            return Err(StoreError::BucketNotEmpty(bucket.to_string()));
        }
        drop(entry);
        self.buckets.remove(bucket);
        Ok(())
    }

    async fn list_buckets(&self) -> Vec<(String, DateTime<Utc>)> {
        self.buckets
            .iter()
            .map(|e| (e.key().clone(), e.value().created_at))
            .collect()
    }

    async fn bucket_exists(&self, bucket: &str) -> bool {
        self.buckets.contains_key(bucket)
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<String>,
        ttl_secs: Option<i64>,
        metadata: HashMap<String, String>,
    ) -> Result<String, StoreError> {
        if data.len() > self.config.max_object_size {
            return Err(StoreError::EntityTooLarge);
        }

        let bucket_entry = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        let is_new = !bucket_entry.objects.contains_key(key);
        if self.config.max_objects_per_bucket > 0
            && bucket_entry.objects.len() >= self.config.max_objects_per_bucket
            && is_new
        {
            return Err(StoreError::InvalidRequest(
                format!("Maximum objects per bucket ({}) reached", self.config.max_objects_per_bucket),
            ));
        }

        let etag = format!("{:x}", md5::compute(&data));
        let now = Utc::now();
        let ttl = ttl_secs.unwrap_or(self.config.default_ttl_seconds);
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

        // Check memory limit and evict if necessary
        if let Some(ref lru) = self.lru {
            let full_key = format!("{}/{}", bucket, key);
            
            // Get old size if updating
            let old_size = bucket_entry.objects.get(&full_key).map(|o| o.size).unwrap_or(0);
            
            // Add to LRU (or update if exists)
            lru.add(&full_key, size).await;
            
            // Evict LRU objects if over limit
            while lru.is_over_limit() {
                let lru_keys = lru.get_lru_keys(10).await;
                if lru_keys.is_empty() {
                    break;
                }
                
                for lru_key in lru_keys {
                    // Parse bucket and key from full key
                    if let Some((lru_bucket, lru_obj_key)) = lru_key.split_once('/') {
                        if let Some(be) = self.buckets.get(lru_bucket) {
                            if let Some((_, removed)) = be.value().objects.remove(lru_obj_key) {
                                be.value().index.remove(lru_obj_key);
                                lru.remove(&lru_key, removed.size).await;
                                self.object_count.fetch_sub(1, Ordering::Relaxed);
                                self.total_bytes.fetch_sub(removed.size, Ordering::Relaxed);
                                tracing::info!("Evicted object {} due to memory limit", lru_key);
                            }
                        }
                    }
                }
            }
            
            // Record access for the new/updated object
            if old_size > 0 {
                lru.update_size(&full_key, old_size, size).await;
            }
        }

        let old = bucket_entry.objects.insert(key.to_string(), obj);
        if let Some(old_obj) = old {
            self.total_bytes.fetch_sub(old_obj.size, Ordering::Relaxed);
        } else {
            self.object_count.fetch_add(1, Ordering::Relaxed);
            bucket_entry.index.insert(key);
        }
        self.total_bytes.fetch_add(size, Ordering::Relaxed);
        Ok(etag)
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        let bucket_entry = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        let obj = bucket_entry.objects.get(key).ok_or_else(|| StoreError::NoSuchKey(key.to_string()))?;

        if obj.is_expired() {
            let size = obj.size;
            let key_clone = key.to_string();
            drop(obj);
            bucket_entry.objects.remove(&key_clone);
            bucket_entry.index.remove(&key_clone);
            
            // Remove from LRU
            if let Some(ref lru) = self.lru {
                let full_key = format!("{}/{}", bucket, key_clone);
                lru.remove(&full_key, size).await;
            }
            
            self.object_count.fetch_sub(1, Ordering::Relaxed);
            self.total_bytes.fetch_sub(size, Ordering::Relaxed);
            return Err(StoreError::NoSuchKey(key.to_string()));
        }

        // Record access in LRU
        if let Some(ref lru) = self.lru {
            let full_key = format!("{}/{}", bucket, key);
            lru.record_access(&full_key).await;
        }

        Ok(Arc::clone(obj.value()))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        self.get_object(bucket, key).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let bucket_entry = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        if let Some((_, removed)) = bucket_entry.objects.remove(key) {
            self.object_count.fetch_sub(1, Ordering::Relaxed);
            self.total_bytes.fetch_sub(removed.size, Ordering::Relaxed);
            bucket_entry.index.remove(key);
            
            // Remove from LRU
            if let Some(ref lru) = self.lru {
                let full_key = format!("{}/{}", bucket, key);
                lru.remove(&full_key, removed.size).await;
            }
        }
        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> Result<ListResult, StoreError> {
        let bucket_entry = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        let prefix = prefix.unwrap_or("");
        let delimiter = delimiter.unwrap_or("");
        let now = Utc::now();

        // Use prefix index to get candidate keys efficiently
        let candidate_keys = bucket_entry.index.get_prefix_keys(prefix, max_keys * 2);

        let mut contents: Vec<ObjectMeta> = Vec::with_capacity(max_keys);
        let mut common_prefixes: HashSet<String> = HashSet::new();
        let mut expired_keys: Vec<String> = Vec::new();

        for key in candidate_keys {
            // Get the actual object
            let obj_ref = match bucket_entry.objects.get(&key) {
                Some(obj) => obj,
                None => continue, // Object was removed concurrently
            };

            // Check expiry
            if obj_ref.expires_at <= now {
                expired_keys.push(key.clone());
                continue;
            }

            // Apply delimiter logic
            if !delimiter.is_empty() {
                let after_prefix = &obj_ref.key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter) {
                    let common = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                    common_prefixes.insert(common);
                    continue;
                }
            }

            contents.push(ObjectMeta {
                key: obj_ref.key.clone(),
                size: obj_ref.size,
                etag: obj_ref.etag.clone(),
                last_modified: obj_ref.last_modified,
            });

            if contents.len() >= max_keys {
                break;
            }
        }

        drop(bucket_entry);

        // Clean up expired keys
        if !expired_keys.is_empty() {
            if let Some(be) = self.buckets.get(bucket) {
                for k in &expired_keys {
                    if let Some((_, removed)) = be.value().objects.remove(k) {
                        self.total_bytes.fetch_sub(removed.size, Ordering::Relaxed);
                        be.value().index.remove(k);
                    }
                }
                self.object_count.fetch_sub(expired_keys.len(), Ordering::Relaxed);
            }
        }

        contents.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        let mut sorted_prefixes: Vec<String> = common_prefixes.into_iter().collect();
        sorted_prefixes.sort();

        Ok(ListResult {
            contents,
            common_prefixes: sorted_prefixes,
            is_truncated: false,
        })
    }

    async fn cleanup_expired(&self) -> usize {
        let now = Utc::now();
        let mut removed = 0usize;

        for bucket_ref in self.buckets.iter() {
            let bucket_entry = bucket_ref.value();
            let before = bucket_entry.objects.len();
            
            // Collect expired keys first to avoid holding locks during removal
            let mut expired_keys = Vec::new();
            for obj_ref in bucket_entry.objects.iter() {
                if obj_ref.expires_at <= now {
                    expired_keys.push(obj_ref.key().clone());
                }
            }

            // Remove expired objects
            for key in &expired_keys {
                if let Some((_, removed_obj)) = bucket_entry.objects.remove(key) {
                    self.total_bytes.fetch_sub(removed_obj.size, Ordering::Relaxed);
                    bucket_entry.index.remove(key);
                }
            }

            let count = before - bucket_entry.objects.len();
            self.object_count.fetch_sub(count, Ordering::Relaxed);
            removed += count;
        }

        removed
    }

    async fn stats(&self) -> (usize, usize, usize) {
        (
            self.buckets.len(),
            self.object_count.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
        )
    }

    async fn create_multipart_upload(&self, bucket: &str, key: &str, content_type: Option<String>, metadata: HashMap<String, String>) -> Result<String, StoreError> {
        if !self.buckets.contains_key(bucket) {
            return Err(StoreError::NoSuchBucket(bucket.into()));
        }

        let upload_id = Uuid::new_v4().to_string();
        self.uploads.insert(upload_id.clone(), Arc::new(MultipartUpload {
            upload_id: upload_id.clone(),
            bucket: bucket.into(),
            key: key.into(),
            parts: DashMap::new(),
            metadata,
            content_type,
        }));

        Ok(upload_id)
    }

    async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str, part_number: usize, data: Bytes) -> Result<String, StoreError> {
        let upload = self.uploads.get(upload_id).map(|u| u.clone()).ok_or_else(|| StoreError::InvalidRequest("NoSuchUpload".into()))?;
        if upload.bucket != bucket || upload.key != key {
            return Err(StoreError::InvalidRequest("Upload metadata mismatch".into()));
        }

        let etag = format!("{:x}", md5::compute(&data));
        upload.parts.insert(part_number, (etag.clone(), data));
        
        Ok(etag)
    }

    async fn complete_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str, parts_list: Vec<(usize, String)>) -> Result<String, StoreError> {
        let upload = self.uploads.get(upload_id).map(|u| u.clone()).ok_or_else(|| StoreError::InvalidRequest("NoSuchUpload".into()))?;
        if upload.bucket != bucket || upload.key != key {
            return Err(StoreError::InvalidRequest("Upload metadata mismatch".into()));
        }

        let total_size: usize = upload.parts.iter().map(|e| e.value().1.len()).sum();
        let mut combined_data = Vec::with_capacity(total_size);

        let mut sorted_parts = parts_list;
        sorted_parts.sort_by_key(|p| p.0);

        for (part_num, expected_etag) in sorted_parts {
            let part_entry = upload.parts.get(&part_num).ok_or_else(|| StoreError::InvalidRequest(format!("Missing part {}", part_num)))?;
            let (ref stored_etag, ref part_data) = *part_entry;
            if *stored_etag != expected_etag.trim_matches('"') {
                return Err(StoreError::InvalidRequest(format!("ETag mismatch for part {}", part_num)));
            }
            combined_data.extend_from_slice(part_data);
        }

        let result_etag = self.put_object(bucket, key, Bytes::from(combined_data), upload.content_type.clone(), None, upload.metadata.clone()).await?;

        self.uploads.remove(upload_id);

        Ok(result_etag)
    }

    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<(), StoreError> {
        let upload = self.uploads.get(upload_id).map(|u| u.clone()).ok_or_else(|| StoreError::InvalidRequest("NoSuchUpload".into()))?;
        if upload.bucket != bucket || upload.key != key {
            return Err(StoreError::InvalidRequest("Upload metadata mismatch".into()));
        }

        self.uploads.remove(upload_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration as StdDuration;
    use tokio;

    #[tokio::test]
    async fn test_bucket_crud() {
        let store = InMemoryBackend::new();
        let bucket = "test-bucket";

        store.create_bucket(bucket).await.unwrap();
        assert!(store.bucket_exists(bucket).await);

        assert!(matches!(
            store.create_bucket(bucket).await,
            Err(StoreError::BucketAlreadyExists(_))
        ));

        let buckets = store.list_buckets().await;
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].0, bucket);

        store.delete_bucket(bucket).await.unwrap();
        assert!(!store.bucket_exists(bucket).await);

        assert!(matches!(
            store.delete_bucket(bucket).await,
            Err(StoreError::NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn test_object_crud() {
        let store = InMemoryBackend::new();
        let bucket = "bucket-b1";
        let key = "k1";
        let data = Bytes::from("hello");

        store.create_bucket(bucket).await.unwrap();

        let etag = store
            .put_object(bucket, key, data.clone(), None, None, HashMap::new())
            .await
            .unwrap();
        assert!(!etag.is_empty());

        let obj = store.get_object(bucket, key).await.unwrap();
        assert_eq!(obj.data, data);
        assert_eq!(obj.etag, etag);

        let head = store.head_object(bucket, key).await.unwrap();
        assert_eq!(head.etag, etag);

        store.delete_object(bucket, key).await.unwrap();
        assert!(matches!(
            store.get_object(bucket, key).await,
            Err(StoreError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn test_ttl_expiry() {
        let store = InMemoryBackend::new();
        let bucket = "ttl-bucket";
        let key = "expired-key";
        let data = Bytes::from("gone soon");

        store.create_bucket(bucket).await.unwrap();

        store
            .put_object(bucket, key, data, None, Some(1), HashMap::new())
            .await
            .unwrap();

        assert!(store.get_object(bucket, key).await.is_ok());

        tokio::time::sleep(StdDuration::from_millis(1100)).await;

        assert!(matches!(
            store.get_object(bucket, key).await,
            Err(StoreError::NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn test_list_objects() {
        let store = InMemoryBackend::new();
        let bucket = "list-bucket";
        store.create_bucket(bucket).await.unwrap();

        let keys = vec!["a/1.txt", "a/2.txt", "b/1.txt", "c.txt"];
        for k in &keys {
            store
                .put_object(bucket, k, Bytes::from("data"), None, None, HashMap::new())
                .await
                .unwrap();
        }

        let res = store.list_objects(bucket, None, None, 1000).await.unwrap();
        assert_eq!(res.contents.len(), 4);

        let res = store.list_objects(bucket, Some("a/"), None, 1000).await.unwrap();
        assert_eq!(res.contents.len(), 2);

        let res = store.list_objects(bucket, None, Some("/"), 1000).await.unwrap();
        assert_eq!(res.common_prefixes.len(), 2);
        assert!(res.common_prefixes.contains(&"a/".to_string()));
        assert!(res.common_prefixes.contains(&"b/".to_string()));
    }

    #[tokio::test]
    async fn test_entity_too_large() {
        let store = InMemoryBackend::new();
        let bucket = "large-bucket";
        store.create_bucket(bucket).await.unwrap();

        let max_size = store.config.max_object_size;
        let large_data = Bytes::from(vec![0u8; max_size + 1]);
        assert!(matches!(
            store.put_object(bucket, "too-big", large_data, None, None, HashMap::new()).await,
            Err(StoreError::EntityTooLarge)
        ));
    }
}
