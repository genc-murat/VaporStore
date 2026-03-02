//! Disk persistence: Snapshot (full dump) + WAL (write-ahead log).
//!
//! Startup: load snapshot → replay WAL → ready.
//! Shutdown: write snapshot → truncate WAL.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::storage::StorageObject;

// ─── Snapshot ────────────────────────────────────────────────────────────────

/// Serializable representation of a bucket with its objects.
#[derive(Serialize, Deserialize)]
pub struct BucketSnapshot {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub objects: Vec<StorageObject>,
}

/// Serializable representation of the entire store.
#[derive(Serialize, Deserialize)]
pub struct StoreSnapshot {
    pub buckets: Vec<BucketSnapshot>,
}

/// Write a full snapshot to `<data_dir>/snapshot.bin`.
pub fn save_snapshot(data_dir: &str, snapshot: &StoreSnapshot) -> io::Result<()> {
    let dir = Path::new(data_dir);
    fs::create_dir_all(dir)?;

    let tmp_path = dir.join("snapshot.bin.tmp");
    let final_path = dir.join("snapshot.bin");

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    bincode::serialize_into(&mut writer, snapshot)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    writer.flush()?;

    // Atomic rename so a crash mid-write doesn't corrupt the snapshot
    fs::rename(&tmp_path, &final_path)?;

    info!(
        path = %final_path.display(),
        buckets = snapshot.buckets.len(),
        "Snapshot saved"
    );
    Ok(())
}

/// Load a snapshot from `<data_dir>/snapshot.bin`. Returns `None` if file doesn't exist.
pub fn load_snapshot(data_dir: &str) -> io::Result<Option<StoreSnapshot>> {
    let path = Path::new(data_dir).join("snapshot.bin");
    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let snap: StoreSnapshot = bincode::deserialize_from(reader)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    info!(
        path = %path.display(),
        buckets = snap.buckets.len(),
        "Snapshot loaded"
    );
    Ok(Some(snap))
}

// ─── WAL ─────────────────────────────────────────────────────────────────────

/// A single WAL entry representing a mutation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalEntry {
    CreateBucket {
        name: String,
        created_at: DateTime<Utc>,
    },
    DeleteBucket {
        name: String,
    },
    PutObject {
        bucket: String,
        key: String,
        data: Bytes,
        content_type: String,
        size: usize,
        etag: String,
        last_modified: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        metadata: HashMap<String, String>,
    },
    DeleteObject {
        bucket: String,
        key: String,
    },
}

impl WalEntry {
    /// Convert a StorageObject into a PutObject WAL entry.
    pub fn from_put(bucket: &str, obj: &StorageObject) -> Self {
        WalEntry::PutObject {
            bucket: bucket.to_string(),
            key: obj.key.clone(),
            data: obj.data.clone(),
            content_type: obj.content_type.clone(),
            size: obj.size,
            etag: obj.etag.clone(),
            last_modified: obj.last_modified,
            expires_at: obj.expires_at,
            metadata: obj.metadata.clone(),
        }
    }

    /// Convert a PutObject WAL entry back into a StorageObject.
    pub fn into_storage_object(self) -> Option<(String, StorageObject)> {
        match self {
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
            } => Some((
                bucket,
                StorageObject {
                    key,
                    data,
                    content_type,
                    size,
                    etag,
                    last_modified,
                    expires_at,
                    metadata,
                },
            )),
            _ => None,
        }
    }
}

/// Append-only WAL writer. Each entry is written as a length-prefixed bincode blob.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl WalWriter {
    /// Open (or create) the WAL file at `<data_dir>/wal.log`.
    pub fn open(data_dir: &str) -> io::Result<Self> {
        let dir = Path::new(data_dir);
        fs::create_dir_all(dir)?;

        let path = dir.join("wal.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        info!(path = %path.display(), "WAL opened");
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    /// Append a single entry to the WAL.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        let data = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Write length prefix (u32 LE) + data
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;
        Ok(())
    }

    /// Truncate the WAL (after a successful snapshot).
    pub fn truncate(&mut self) -> io::Result<()> {
        // Flush pending writes before truncating
        self.writer.flush()?;
        // Close and re-create the file
        let file = File::create(&self.path)?;
        self.writer = BufWriter::new(file);
        info!(path = %self.path.display(), "WAL truncated");
        Ok(())
    }
}

/// Replay all WAL entries from `<data_dir>/wal.log`.
pub fn replay_wal(data_dir: &str) -> io::Result<Vec<WalEntry>> {
    let path = Path::new(data_dir).join("wal.log");
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    let mut len_buf = [0u8; 4];

    loop {
        use std::io::Read;
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data)?;

        match bincode::deserialize::<WalEntry>(&data) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                warn!("Skipping corrupt WAL entry: {}", e);
                break;
            }
        }
    }

    info!(entries = entries.len(), "WAL replayed");
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_test_snapshot() -> StoreSnapshot {
        StoreSnapshot {
            buckets: vec![BucketSnapshot {
                name: "test-bucket".to_string(),
                created_at: Utc::now(),
                objects: vec![StorageObject {
                    key: "hello.txt".to_string(),
                    data: Bytes::from("hello world"),
                    content_type: "text/plain".to_string(),
                    size: 11,
                    etag: "abc123".to_string(),
                    last_modified: Utc::now(),
                    expires_at: Utc::now() + chrono::Duration::seconds(300),
                    metadata: HashMap::new(),
                }],
            }],
        }
    }

    #[test]
    fn test_snapshot_save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let snap = make_test_snapshot();
        save_snapshot(dir, &snap).unwrap();

        let loaded = load_snapshot(dir).unwrap().unwrap();
        assert_eq!(loaded.buckets.len(), 1);
        assert_eq!(loaded.buckets[0].name, "test-bucket");
        assert_eq!(loaded.buckets[0].objects.len(), 1);
        assert_eq!(loaded.buckets[0].objects[0].key, "hello.txt");
        assert_eq!(loaded.buckets[0].objects[0].data, Bytes::from("hello world"));
    }

    #[test]
    fn test_load_snapshot_missing_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let result = load_snapshot(dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_wal_write_replay_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let mut writer = WalWriter::open(dir).unwrap();

        writer
            .append(&WalEntry::CreateBucket {
                name: "mybucket".to_string(),
                created_at: Utc::now(),
            })
            .unwrap();

        writer
            .append(&WalEntry::PutObject {
                bucket: "mybucket".to_string(),
                key: "file.bin".to_string(),
                data: Bytes::from_static(&[1, 2, 3]),
                content_type: "application/octet-stream".to_string(),
                size: 3,
                etag: "etag1".to_string(),
                last_modified: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::seconds(60),
                metadata: HashMap::new(),
            })
            .unwrap();

        writer
            .append(&WalEntry::DeleteObject {
                bucket: "mybucket".to_string(),
                key: "file.bin".to_string(),
            })
            .unwrap();

        drop(writer);

        let entries = replay_wal(dir).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(matches!(entries[0], WalEntry::CreateBucket { .. }));
        assert!(matches!(entries[1], WalEntry::PutObject { .. }));
        assert!(matches!(entries[2], WalEntry::DeleteObject { .. }));
    }

    #[test]
    fn test_wal_truncate() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let mut writer = WalWriter::open(dir).unwrap();
        writer
            .append(&WalEntry::CreateBucket {
                name: "b1".to_string(),
                created_at: Utc::now(),
            })
            .unwrap();

        writer.truncate().unwrap();

        let entries = replay_wal(dir).unwrap();
        assert!(entries.is_empty());
    }
}
