use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;

use crate::error::ApiError;
use crate::storage::ObjectStore;
use crate::xml::{
    to_xml, BucketEntry, BucketList, CommonPrefix, ListAllMyBucketsResult, ListBucketResult,
    ObjectContent, Owner,
};

pub type SharedStore = Arc<ObjectStore>;

fn request_id() -> String {
    Uuid::new_v4().to_string()
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

// ─── Bucket handlers ──────────────────────────────────────────────────────────

/// GET / — List all buckets
pub async fn list_buckets(State(store): State<SharedStore>) -> Response {
    let buckets = store.list_buckets();
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

    let result = ListAllMyBucketsResult {
        owner: Owner {
            id: "vaporstore-owner".into(),
            display_name: "vaporstore".into(),
        },
        buckets: BucketList {
            bucket: buckets
                .iter()
                .map(|(name, _)| BucketEntry {
                    name: name.clone(),
                    creation_date: now.clone(),
                })
                .collect(),
        },
    };

    match to_xml(&result) {
        Ok(body) => xml_response(StatusCode::OK, body),
        Err(e) => ApiError::Internal(e.to_string()).into_response(),
    }
}

/// PUT /{bucket} — Create bucket
pub async fn create_bucket(
    State(store): State<SharedStore>,
    Path(bucket): Path<String>,
) -> Response {
    info!("CreateBucket: {}", bucket);
    match store.create_bucket(&bucket) {
        Ok(_) => {
            let req_id = request_id();
            (
                StatusCode::OK,
                [
                    ("x-amz-request-id", req_id.as_str()),
                    ("Location", &format!("/{}", bucket)),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StoreError::BucketAlreadyExists) => {
            ApiError::BucketAlreadyExists(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// DELETE /{bucket} — Delete bucket
pub async fn delete_bucket(
    State(store): State<SharedStore>,
    Path(bucket): Path<String>,
) -> Response {
    info!("DeleteBucket: {}", bucket);
    match store.delete_bucket(&bucket) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(crate::storage::StoreError::NoSuchBucket) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(crate::storage::StoreError::BucketNotEmpty) => {
            ApiError::BucketNotEmpty(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// GET /{bucket}?list-type=2&prefix=...&delimiter=... — List objects
pub async fn list_objects(
    State(store): State<SharedStore>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    debug!("ListObjects: {} {:?}", bucket, params);

    let prefix = params.get("prefix").map(String::as_str);
    let delimiter = params.get("delimiter").map(String::as_str);
    let max_keys: usize = params
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match store.list_objects(&bucket, prefix, delimiter, max_keys) {
        Ok(result) => {
            let contents: Vec<ObjectContent> =
                result.contents.iter().map(ObjectContent::from_meta).collect();
            let common_prefixes: Vec<CommonPrefix> = result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: p })
                .collect();

            let xml_result = ListBucketResult {
                name: bucket.clone(),
                prefix: prefix.unwrap_or("").to_string(),
                max_keys,
                is_truncated: result.is_truncated,
                contents,
                common_prefixes,
            };

            match to_xml(&xml_result) {
                Ok(body) => xml_response(StatusCode::OK, body),
                Err(e) => ApiError::Internal(e.to_string()).into_response(),
            }
        }
        Err(crate::storage::StoreError::NoSuchBucket) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

// ─── Object handlers ──────────────────────────────────────────────────────────

/// PUT /{bucket}/{key} — Upload object
pub async fn put_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    info!("PutObject: {}/{} ({} bytes)", bucket, key, body.len());

    if !store.bucket_exists(&bucket) {
        return ApiError::NoSuchBucket(bucket).into_response();
    }

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // Parse custom TTL from x-amz-meta-ttl-seconds header
    let ttl_secs: Option<i64> = headers
        .get("x-amz-meta-ttl-seconds")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());

    // Collect x-amz-meta-* headers
    let metadata: HashMap<String, String> = headers
        .iter()
        .filter_map(|(name, value)| {
            let n = name.as_str();
            if n.starts_with("x-amz-meta-") {
                Some((n.to_string(), value.to_str().unwrap_or("").to_string()))
            } else {
                None
            }
        })
        .collect();

    match store.put_object(&bucket, &key, body, content_type, ttl_secs, metadata) {
        Ok(etag) => {
            let req_id = request_id();
            (
                StatusCode::OK,
                [
                    ("x-amz-request-id", req_id.as_str()),
                    (header::ETAG.as_str(), &format!("\"{}\"", etag)),
                    (header::CONTENT_LENGTH.as_str(), "0"),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StoreError::EntityTooLarge) => {
            ApiError::EntityTooLarge.into_response()
        }
        Err(crate::storage::StoreError::NoSuchBucket) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// GET /{bucket}/{key} — Download object (zero-copy via Arc)
pub async fn get_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!("GetObject: {}/{}", bucket, key);

    match store.get_object(&bucket, &key) {
        Ok(obj) => {
            let req_id = request_id();
            let etag_val = format!("\"{}\"", obj.etag);
            let last_modified = obj.last_modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            let expires = obj.expires_at.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

            // Bytes::clone() is cheap — just Arc increment, no memcpy
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE.as_str(), obj.content_type.as_str()),
                    (header::ETAG.as_str(), etag_val.as_str()),
                    (header::LAST_MODIFIED.as_str(), last_modified.as_str()),
                    ("x-amz-request-id", req_id.as_str()),
                    ("Expires", expires.as_str()),
                ],
                Body::from(obj.data.clone()),
            )
                .into_response()
        }
        Err(crate::storage::StoreError::NoSuchBucket) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(crate::storage::StoreError::NoSuchKey) => {
            ApiError::NoSuchKey(format!("/{}/{}", bucket, key)).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// HEAD /{bucket}/{key} — Object metadata only (zero-copy via Arc)
pub async fn head_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    debug!("HeadObject: {}/{}", bucket, key);

    match store.head_object(&bucket, &key) {
        Ok(obj) => {
            let req_id = request_id();
            let etag_val = format!("\"{}\"", obj.etag);
            let last_modified = obj.last_modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE.as_str(), obj.content_type.as_str()),
                    (header::CONTENT_LENGTH.as_str(), obj.size.to_string().as_str()),
                    (header::ETAG.as_str(), etag_val.as_str()),
                    (header::LAST_MODIFIED.as_str(), last_modified.as_str()),
                    ("x-amz-request-id", req_id.as_str()),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StoreError::NoSuchKey) | Err(crate::storage::StoreError::NoSuchBucket) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// DELETE /{bucket}/{key} — Delete object
pub async fn delete_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    info!("DeleteObject: {}/{}", bucket, key);

    match store.delete_object(&bucket, &key) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(crate::storage::StoreError::NoSuchBucket) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}
