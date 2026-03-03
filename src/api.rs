use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    body::Body,
    debug_handler,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use chrono::Utc;
use tracing::{debug, info};
use uuid::Uuid;

use crate::error::ApiError;
use crate::storage::StorageBackend;
use crate::xml::{
    to_xml, BucketEntry, BucketList, CommonPrefix, ListAllMyBucketsResult, ListBucketResult,
    ObjectContent, Owner,
};

pub type SharedStore = Arc<dyn StorageBackend + Send + Sync>;

fn request_id() -> String {
    Uuid::now_v7().to_string()
}

fn xml_response(status: StatusCode, body: String) -> Response {
    let req_id = request_id();
    let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    (
        status,
        [
            (header::CONTENT_TYPE, "application/xml"),
            (header::DATE, now.as_str()),
            (header::HeaderName::from_static("x-amz-request-id"), req_id.as_str()),
        ],
        body,
    )
        .into_response()
}

// ─── Bucket handlers ──────────────────────────────────────────────────────────

/// GET / — List all buckets
pub async fn list_buckets(State(store): State<SharedStore>) -> Response {
    let buckets = store.list_buckets().await;

    let result = ListAllMyBucketsResult {
        owner: Owner {
            id: "vaporstore-owner".into(),
            display_name: "vaporstore".into(),
        },
        buckets: BucketList {
            bucket: buckets
                .into_iter()
                .map(|(name, created)| BucketEntry {
                    name,
                    creation_date: created.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
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

    info!("Creating bucket");
    match store.create_bucket(&bucket).await {
        Ok(_) => {
            let req_id = request_id();
            let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            (
                StatusCode::OK,
                [
                    ("x-amz-request-id", req_id.as_str()),
                    (header::DATE.as_str(), now.as_str()),
                    ("Location", &format!("/{}", bucket)),
                ],
            )
                .into_response()
        }
        Err(crate::storage::StoreError::BucketAlreadyExists(_)) => {
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

    info!("Deleting bucket");
    match store.delete_bucket(&bucket).await {
        Ok(_) => {
            let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            (
                StatusCode::NO_CONTENT,
                [(header::DATE.as_str(), now.as_str())],
            )
                .into_response()
        }
        Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(crate::storage::StoreError::BucketNotEmpty(_)) => {
            ApiError::BucketNotEmpty(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// HEAD /{bucket} — Check if bucket exists
pub async fn head_bucket(
    State(store): State<SharedStore>,
    Path(bucket): Path<String>,
) -> Response {

    debug!("Checking bucket existence");
    if store.bucket_exists(&bucket).await {
        let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        (
            StatusCode::OK,
            [(header::DATE.as_str(), now.as_str())],
        )
            .into_response()
    } else {
        ApiError::NoSuchBucket(bucket).into_response()
    }
}

/// GET /{bucket}?list-type=2&prefix=...&delimiter=... — List objects
pub async fn list_objects(
    State(store): State<SharedStore>,
    Path(bucket): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {

    debug!("Listing objects: {:?}", params);

    let prefix = params.get("prefix").map(String::as_str);
    let delimiter = params.get("delimiter").map(String::as_str);
    let max_keys: usize = params
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match store.list_objects(&bucket, prefix, delimiter, max_keys).await {
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
        Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

// ─── Object handlers ──────────────────────────────────────────────────────────

#[debug_handler]
pub async fn post_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if params.contains_key("uploads") {
        // Create Multipart Upload
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        
        let metadata = HashMap::new(); // Simplify for now

        match store.create_multipart_upload(&bucket, &key, content_type, metadata).await {
            Ok(upload_id) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                    bucket, key, upload_id
                );
                (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
            }
            Err(e) => ApiError::from(e).into_response(),
        }
    } else if let Some(upload_id) = params.get("uploadId") {
        // Complete Multipart Upload
        // Very basic XML parser for <CompleteMultipartUpload>
        let xml_str = String::from_utf8_lossy(&body);
        let mut parts = Vec::new();
        let mut cur_part_num = None;
        for line in xml_str.split('<') {
            if line.starts_with("PartNumber>") {
                cur_part_num = line.trim_start_matches("PartNumber>").split('<').next().and_then(|s| s.parse::<usize>().ok());
            } else if line.starts_with("ETag>") {
                if let Some(num) = cur_part_num {
                    let etag = line.trim_start_matches("ETag>").split('<').next().unwrap_or("").to_string();
                    parts.push((num, etag));
                    cur_part_num = None;
                }
            }
        }
        
        match store.complete_multipart_upload(&bucket, &key, upload_id, parts).await {
            Ok(etag) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Location>http://localhost:9353/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
                    bucket, key, bucket, key, etag
                );
                (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
            }
            Err(e) => ApiError::from(e).into_response(),
        }
    } else {
        ApiError::Internal("Unknown POST request".into()).into_response()
    }
}

/// PUT /{bucket}/{key} — Upload object or part
#[debug_handler]
pub async fn put_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let (Some(upload_id), Some(part_str)) = (params.get("uploadId"), params.get("partNumber")) {
        if let Ok(part_number) = part_str.parse::<usize>() {
            return match store.upload_part(&bucket, &key, upload_id, part_number, body).await {
                Ok(etag) => {
                    let etag_val = format!("\"{}\"", etag);
                    (StatusCode::OK, [(header::ETAG.as_str(), etag_val.as_str())], Body::empty()).into_response()
                }
                Err(e) => ApiError::from(e).into_response(),
            };
        }
    }

    info!("Uploading object ({} bytes)", body.len());

    if !store.bucket_exists(&bucket).await {
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

    if let Some(source) = headers.get("x-amz-copy-source") {
        if let Ok(source_str) = source.to_str() {
            let source_path = source_str.trim_start_matches('/');
            let mut parts = source_path.splitn(2, '/');
            if let (Some(src_bucket), Some(src_key)) = (parts.next(), parts.next()) {
                match store.get_object(src_bucket, src_key).await {
                    Ok(src_obj) => {
                        let c_type = content_type.unwrap_or(src_obj.content_type.clone());
                        match store.put_object(&bucket, &key, src_obj.data.clone(), Some(c_type), ttl_secs, metadata).await {
                            Ok(etag) => {
                                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
                                let body = format!(
                                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult><LastModified>{}</LastModified><ETag>\"{}\"</ETag></CopyObjectResult>",
                                    now, etag
                                );
                                return (StatusCode::OK, [(header::CONTENT_TYPE, "application/xml")], body).into_response();
                            }
                            Err(e) => return ApiError::from(e).into_response(),
                        }
                    }
                    Err(_) => return ApiError::NoSuchKey(src_key.into()).into_response(),
                }
            }
        }
    }

    match store.put_object(&bucket, &key, body, content_type, ttl_secs, metadata).await {
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
        Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// GET /{bucket}/{key} — Download object (zero-copy via Arc)
#[debug_handler]
pub async fn get_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {

    debug!("Downloading object");

    match store.get_object(&bucket, &key).await {
        Ok(obj) => {
            let req_id = request_id();
            let etag_val = format!("\"{}\"", obj.etag);
            let last_modified = obj.last_modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
            let expires = obj.expires_at.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

            let (status, body, content_range) = if let Some(range_hdr) = headers.get(header::RANGE) {
                if let Ok(range_str) = range_hdr.to_str() {
                    if let Some(range_val) = range_str.strip_prefix("bytes=") {
                        let parts: Vec<&str> = range_val.split('-').collect();
                        let start = parts.get(0).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                        let end = parts
                            .get(1)
                            .filter(|s| !s.is_empty())
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(obj.size.saturating_sub(1));
                        
                        let start = start.min(obj.size);
                        let end = end.min(obj.size.saturating_sub(1));
                        let slice = obj.data.slice(start..=end);
                        let cr = format!("bytes {}-{}/{}", start, end, obj.size);
                        (StatusCode::PARTIAL_CONTENT, slice, cr)
                    } else {
                        (StatusCode::OK, obj.data.clone(), String::new())
                    }
                } else {
                    (StatusCode::OK, obj.data.clone(), String::new())
                }
            } else {
                (StatusCode::OK, obj.data.clone(), String::new())
            };

            let mut res = (
                status,
                [
                    (header::CONTENT_TYPE.as_str(), obj.content_type.as_str()),
                    (header::ETAG.as_str(), etag_val.as_str()),
                    (header::LAST_MODIFIED.as_str(), last_modified.as_str()),
                    ("x-amz-request-id", req_id.as_str()),
                    ("Expires", expires.as_str()),
                ],
                Body::from(body),
            ).into_response();

            if !content_range.is_empty() {
                res.headers_mut().insert(header::CONTENT_RANGE, content_range.parse().unwrap());
                res.headers_mut().insert(
                    header::ACCEPT_RANGES,
                    axum::http::HeaderValue::from_static("bytes"),
                );
            }

            res
        }
        Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(crate::storage::StoreError::NoSuchKey(_)) => {
            ApiError::NoSuchKey(format!("/{}/{}", bucket, key)).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// HEAD /{bucket}/{key} — Object metadata only (zero-copy via Arc)
#[debug_handler]
pub async fn head_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {

    debug!("Checking object metadata");

    match store.head_object(&bucket, &key).await {
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
        Err(crate::storage::StoreError::NoSuchKey(_)) | Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            StatusCode::NOT_FOUND.into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

/// DELETE /{bucket}/{key} — Delete object or abort multipart upload
#[debug_handler]
pub async fn delete_object(
    State(store): State<SharedStore>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {

    if let Some(upload_id) = params.get("uploadId") {
        info!("Aborting multipart upload");
        return match store.abort_multipart_upload(&bucket, &key, upload_id).await {
            Ok(_) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => ApiError::from(e).into_response(),
        };
    }

    info!("Deleting object");

    match store.delete_object(&bucket, &key).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(crate::storage::StoreError::NoSuchBucket(_)) => {
            ApiError::NoSuchBucket(bucket).into_response()
        }
        Err(e) => ApiError::from(e).into_response(),
    }
}

// ─── Health handler ───────────────────────────────────────────────────────────

static START_TIME: std::sync::OnceLock<chrono::DateTime<Utc>> = std::sync::OnceLock::new();

fn uptime_secs() -> i64 {
    let start = START_TIME.get_or_init(Utc::now);
    (Utc::now() - *start).num_seconds()
}

/// GET /health — Liveness / readiness probe
pub async fn health(State(store): State<SharedStore>) -> Response {
    let (buckets, objects, bytes) = store.stats().await;
    let body = format!(
        r#"{{"status":"ok","version":"{}","uptime_seconds":{},"buckets":{},"objects":{},"bytes":{}}}"#,
        env!("CARGO_PKG_VERSION"),
        uptime_secs(),
        buckets,
        objects,
        bytes,
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

// ─── Metrics handler ──────────────────────────────────────────────────────────

#[debug_handler]
pub async fn metrics(State(store): State<SharedStore>) -> Response {
    // Get basic storage metrics
    let (buckets, objects, bytes) = store.stats().await;
    
    // Get Prometheus metrics (use lazy init for tests)
    let prometheus_metrics = {
        let registry = match crate::metrics::REGISTRY.get() {
            Some(r) => r,
            None => crate::metrics::get_or_init_default(),
        };
        
        match registry.gather() {
            mfs if !mfs.is_empty() => {
                use prometheus::Encoder;
                let mut buffer = Vec::new();
                let encoder = prometheus::TextEncoder::new();
                if encoder.encode(&mfs, &mut buffer).is_ok() {
                    String::from_utf8_lossy(&buffer).to_string()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    };

    // Combine basic and histogram metrics
    let body = format!(
        "# HELP vaporstore_buckets_total Total number of buckets\n\
         # TYPE vaporstore_buckets_total gauge\n\
         vaporstore_buckets_total {}\n\
         # HELP vaporstore_objects_total Total number of objects\n\
         # TYPE vaporstore_objects_total gauge\n\
         vaporstore_objects_total {}\n\
         # HELP vaporstore_storage_bytes Total size of objects in bytes\n\
         # TYPE vaporstore_storage_bytes gauge\n\
         vaporstore_storage_bytes {}\n\
         {}",
        buckets, objects, bytes, prometheus_metrics
    );

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// Fallback for unknown routes
pub async fn not_found(req: axum::http::Request<axum::body::Body>) -> Response {
    let _method = req.method().to_string();
    let uri = req.uri().to_string();

    info!("Endpoint not found");
    
    let request_id = uuid::Uuid::now_v7().to_string();
    let now = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    let err = crate::xml::S3Error {
        code: "NotFound".to_string(),
        message: format!("The specified resource was not found. (Path: {})", uri),
        resource: uri,
        request_id,
    };

    let body = crate::xml::to_xml(&err).unwrap_or_else(|_| "<Error/>".to_string());
    (
        StatusCode::NOT_FOUND,
        [
            ("Content-Type", "application/xml"),
            (header::DATE.as_str(), now.as_str()),
        ],
        body,
    )
        .into_response()
}
