use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

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

#[derive(Debug)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub parts: DashMap<usize, Bytes>,
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

/// Top-level store: bucket_name → BucketMap
#[derive(Default, Clone)]
pub struct InMemoryBackend {
    buckets: DashMap<String, BucketMap>,
    uploads: DashMap<String, Arc<MultipartUpload>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
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
}

#[async_trait]
impl StorageBackend for InMemoryBackend {
    async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        self.validate_bucket_name(bucket)?;
        if self.buckets.contains_key(bucket) {
            return Err(StoreError::BucketAlreadyExists(bucket.to_string()));
        }
        self.buckets.insert(bucket.to_string(), DashMap::new());
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let entry = self.buckets.get(bucket).ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;
        if !entry.is_empty() {
            return Err(StoreError::BucketNotEmpty(bucket.to_string()));
        }
        drop(entry);
        self.buckets.remove(bucket);
        Ok(())
    }

    async fn list_buckets(&self) -> Vec<(String, DateTime<Utc>)> {
        self.buckets
            .iter()
            .map(|e| (e.key().clone(), Utc::now()))
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
        if data.len() > MAX_OBJECT_SIZE {
            return Err(StoreError::EntityTooLarge);
        }

        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

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

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        let obj = bucket_map.get(key).ok_or_else(|| StoreError::NoSuchKey(key.to_string()))?;

        if obj.is_expired() {
            drop(obj);
            bucket_map.remove(key);
            return Err(StoreError::NoSuchKey(key.to_string()));
        }

        Ok(Arc::clone(obj.value()))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        self.get_object(bucket, key).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

        bucket_map.remove(key);
        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> Result<ListResult, StoreError> {
        let bucket_map = self
            .buckets
            .get(bucket)
            .ok_or_else(|| StoreError::NoSuchBucket(bucket.to_string()))?;

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

    async fn cleanup_expired(&self) -> usize {
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

    async fn stats(&self) -> (usize, usize, usize) {
        let bucket_count = self.buckets.len();
        let mut object_count = 0;
        let mut total_bytes = 0;

        for entry in self.buckets.iter() {
            let bucket_map = entry.value();
            object_count += bucket_map.len();
            for obj_entry in bucket_map.iter() {
                total_bytes += obj_entry.value().size;
            }
        }

        (bucket_count, object_count, total_bytes)
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
        upload.parts.insert(part_number, data);
        
        Ok(etag)
    }

    async fn complete_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str, parts_list: Vec<(usize, String)>) -> Result<String, StoreError> {
        let upload = self.uploads.get(upload_id).map(|u| u.clone()).ok_or_else(|| StoreError::InvalidRequest("NoSuchUpload".into()))?;
        if upload.bucket != bucket || upload.key != key {
            return Err(StoreError::InvalidRequest("Upload metadata mismatch".into()));
        }

        let mut total_size = 0;
        let mut combined_data = Vec::new();
        
        let mut sorted_parts = parts_list;
        sorted_parts.sort_by_key(|p| p.0);

        for (part_num, expected_etag) in sorted_parts {
            let part_data = upload.parts.get(&part_num).ok_or_else(|| StoreError::InvalidRequest(format!("Missing part {}", part_num)))?;
            let etag = format!("{:x}", md5::compute(&*part_data));
            if etag != expected_etag.trim_matches('"') {
                return Err(StoreError::InvalidRequest(format!("ETag mismatch for part {}", part_num)));
            }
            total_size += part_data.len();
            combined_data.extend_from_slice(&part_data);
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

        let large_data = Bytes::from(vec![0u8; MAX_OBJECT_SIZE + 1]);
        assert!(matches!(
            store.put_object(bucket, "too-big", large_data, None, None, HashMap::new()).await,
            Err(StoreError::EntityTooLarge)
        ));
    }
}
