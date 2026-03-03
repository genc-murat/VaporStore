//! HybridBackend: wraps InMemoryBackend with optional disk persistence.
//!
//! Reads are served from memory. Writes go to both memory and WAL.
//! On startup the store is hydrated from snapshot + WAL replay.
//! On shutdown a full snapshot is written and the WAL is truncated.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use tracing::{info, warn};

use crate::config::Config;
use crate::persistence::{
    self, AsyncWalWriter, StoreSnapshot, WalEntry,
};
use crate::storage::{
    InMemoryBackend, ListResult, StorageBackend, StorageObject, StoreError,
};

/// A storage backend that delegates to [`InMemoryBackend`] and optionally
/// persists mutations through a WAL and periodic snapshots.
pub struct HybridBackend {
    inner: InMemoryBackend,
    wal: Option<Arc<AsyncWalWriter>>,
    config: Config,
}

impl HybridBackend {
    /// Create a HybridBackend.
    /// If persistence is enabled, loads from disk (snapshot + WAL replay).
    pub fn load(config: Config) -> Self {
        if !config.persistence_enabled {
            return Self {
                inner: InMemoryBackend::with_config(config.clone()),
                wal: None,
                config,
            };
        }

        let data_dir = &config.data_dir;
        let mut inner = InMemoryBackend::with_config(config.clone());

        // 1. Load snapshot
        match persistence::load_snapshot(data_dir) {
            Ok(Some(snap)) => {
                Self::apply_snapshot(&mut inner, snap);
            }
            Ok(None) => {
                info!("No snapshot found, starting fresh");
            }
            Err(e) => {
                warn!("Failed to load snapshot: {}, starting fresh", e);
            }
        }

        // 2. Replay WAL
        match persistence::replay_wal(data_dir) {
            Ok(entries) if !entries.is_empty() => {
                info!(count = entries.len(), "Replaying WAL entries");
                Self::apply_wal_entries(&inner, entries);
            }
            Ok(_) => {}
            Err(e) => {
                warn!("Failed to replay WAL: {}", e);
            }
        }

        // 3. Open WAL for new writes (async with batching)
        let wal = if config.wal_enabled {
            match AsyncWalWriter::start(data_dir, 100, 50) {
                Ok(writer) => Some(Arc::new(writer)),
                Err(e) => {
                    warn!("Failed to start async WAL: {}, continuing without WAL", e);
                    None
                }
            }
        } else {
            None
        };

        Self { inner, wal, config }
    }

    /// Apply a snapshot to the inner backend.
    fn apply_snapshot(inner: &mut InMemoryBackend, snap: StoreSnapshot) {
        for bucket_snap in snap.buckets {
            // Create bucket with its original timestamp
            inner.insert_bucket_raw(&bucket_snap.name, bucket_snap.created_at);

            for obj in bucket_snap.objects {
                // Skip expired objects
                if obj.is_expired() {
                    continue;
                }
                inner.insert_object_raw(&bucket_snap.name, obj);
            }
        }
    }

    /// Apply WAL entries to the inner backend.
    fn apply_wal_entries(inner: &InMemoryBackend, entries: Vec<WalEntry>) {
        for entry in entries {
            match entry {
                WalEntry::CreateBucket { name, created_at } => {
                    inner.insert_bucket_raw(&name, created_at);
                }
                WalEntry::DeleteBucket { name } => {
                    inner.remove_bucket_raw(&name);
                }
                WalEntry::PutObject {
                    bucket,
                    key,
                    data,
                    content_type,
                    size,
                    etag,
                    last_modified,
                    expires_at,
                    metadata,
                } => {
                    let obj = crate::storage::StorageObject {
                        key,
                        data,
                        content_type,
                        size,
                        etag,
                        last_modified,
                        expires_at,
                        metadata,
                    };
                    if !obj.is_expired() {
                        inner.insert_object_raw(&bucket, obj);
                    }
                }
                WalEntry::DeleteObject { bucket, key } => {
                    inner.remove_object_raw(&bucket, &key);
                }
            }
        }
    }

    /// Write a WAL entry (if WAL is enabled).
    fn wal_append(&self, entry: &WalEntry) {
        if let Some(ref wal) = self.wal {
            let wal = Arc::clone(wal);
            let entry = entry.clone();
            tokio::spawn(async move {
                if let Err(e) = wal.append(entry).await {
                    warn!("WAL append failed: {}", e);
                }
            });
        }
    }

    /// Take a full snapshot of the current state to disk.
    pub fn save_snapshot(&self) -> std::io::Result<()> {
        if !self.config.persistence_enabled {
            return Ok(());
        }

        let snap = self.inner.to_snapshot();
        persistence::save_snapshot(&self.config.data_dir, &snap)?;

        // Note: WAL truncation is now handled differently with async WAL
        // The async WAL continuously flushes, so we don't truncate here
        // Instead, snapshots serve as recovery points

        Ok(())
    }

    /// Graceful shutdown: save snapshot and close WAL.
    pub async fn shutdown(&self) {
        if !self.config.persistence_enabled {
            return;
        }

        info!("Persisting state to disk before shutdown...");
        if let Err(e) = self.save_snapshot() {
            warn!("Failed to save shutdown snapshot: {}", e);
        }

        // Shutdown async WAL writer - give it time to flush pending writes
        if self.wal.is_some() {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}

#[async_trait]
impl StorageBackend for HybridBackend {
    async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let result = self.inner.create_bucket(bucket).await?;
        self.wal_append(&WalEntry::CreateBucket {
            name: bucket.to_string(),
            created_at: Utc::now(),
        });
        Ok(result)
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let result = self.inner.delete_bucket(bucket).await?;
        self.wal_append(&WalEntry::DeleteBucket {
            name: bucket.to_string(),
        });
        Ok(result)
    }

    async fn list_buckets(&self) -> Vec<(String, DateTime<Utc>)> {
        self.inner.list_buckets().await
    }

    async fn bucket_exists(&self, bucket: &str) -> bool {
        self.inner.bucket_exists(bucket).await
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
        let size = data.len();
        let etag = self
            .inner
            .put_object(bucket, key, data.clone(), content_type.clone(), ttl_secs, metadata.clone())
            .await?;

        let now = Utc::now();
        let ttl = ttl_secs.unwrap_or(self.config.default_ttl_seconds);
        let expires_at = now + Duration::seconds(ttl);

        self.wal_append(&WalEntry::PutObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            data,
            content_type: content_type.unwrap_or_else(|| "application/octet-stream".to_string()),
            size,
            etag: etag.clone(),
            last_modified: now,
            expires_at,
            metadata,
        });

        Ok(etag)
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        self.inner.get_object(bucket, key).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<Arc<StorageObject>, StoreError> {
        self.inner.head_object(bucket, key).await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let result = self.inner.delete_object(bucket, key).await?;
        self.wal_append(&WalEntry::DeleteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
        });
        Ok(result)
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_keys: usize,
    ) -> Result<ListResult, StoreError> {
        self.inner.list_objects(bucket, prefix, delimiter, max_keys).await
    }

    async fn cleanup_expired(&self) -> usize {
        self.inner.cleanup_expired().await
    }

    async fn stats(&self) -> (usize, usize, usize) {
        self.inner.stats().await
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<String>,
        metadata: HashMap<String, String>,
    ) -> Result<String, StoreError> {
        self.inner
            .create_multipart_upload(bucket, key, content_type, metadata)
            .await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: usize,
        data: Bytes,
    ) -> Result<String, StoreError> {
        self.inner
            .upload_part(bucket, key, upload_id, part_number, data)
            .await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts_list: Vec<(usize, String)>,
    ) -> Result<String, StoreError> {
        let etag = self
            .inner
            .complete_multipart_upload(bucket, key, upload_id, parts_list)
            .await?;

        // Log the final assembled object
        if let Ok(obj) = self.inner.get_object(bucket, key).await {
            self.wal_append(&WalEntry::from_put(bucket, &obj));
        }

        Ok(etag)
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StoreError> {
        self.inner
            .abort_multipart_upload(bucket, key, upload_id)
            .await
    }
}
