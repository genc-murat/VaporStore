use serde::Serialize;

use crate::storage::ObjectMeta;

// ─── Bucket listing ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: BucketList,
}

#[derive(Debug, Serialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Debug, Serialize)]
pub struct BucketList {
    #[serde(rename = "Bucket")]
    pub bucket: Vec<BucketEntry>,
}

#[derive(Debug, Serialize)]
pub struct BucketEntry {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

// ─── Object listing ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "MaxKeys")]
    pub max_keys: usize,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Contents")]
    pub contents: Vec<ObjectContent>,
    #[serde(rename = "CommonPrefixes")]
    pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Debug, Serialize)]
pub struct ObjectContent {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: usize,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

impl ObjectContent {
    pub fn from_meta(meta: &ObjectMeta) -> Self {
        Self {
            key: meta.key.clone(),
            last_modified: meta.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            etag: format!("\"{}\"", meta.etag),
            size: meta.size,
            storage_class: "STANDARD".to_string(),
        }
    }
}

// ─── Error response ───────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename = "Error")]
pub struct S3Error {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Resource")]
    pub resource: String,
    #[serde(rename = "RequestId")]
    pub request_id: String,
}

// ─── Serialization helpers ────────────────────────────────────────────────────

pub fn to_xml<T: Serialize>(value: &T) -> Result<String, quick_xml::SeError> {
    let mut buf = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let xml_body = quick_xml::se::to_string(value)?;
    buf.push_str(&xml_body);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_buckets_xml() {
        let result = ListAllMyBucketsResult {
            owner: Owner {
                id: "id1".to_string(),
                display_name: "user1".to_string(),
            },
            buckets: BucketList {
                bucket: vec![BucketEntry {
                    name: "b1".to_string(),
                    creation_date: "2024-01-01T00:00:00Z".to_string(),
                }],
            },
        };

        let xml = to_xml(&result).unwrap();
        assert!(xml.contains("<ListAllMyBucketsResult>"));
        assert!(xml.contains("<Bucket><Name>b1</Name>"));
    }

    #[test]
    fn test_error_xml() {
        let err = S3Error {
            code: "AccessDenied".to_string(),
            message: "Denied".to_string(),
            resource: "/b1".to_string(),
            request_id: "req1".to_string(),
        };

        let xml = to_xml(&err).unwrap();
        assert!(xml.contains("<Error>"));
        assert!(xml.contains("<Code>AccessDenied</Code>"));
    }
}
