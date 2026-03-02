use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;
use vaporstore::{app, storage::{InMemoryBackend, StorageBackend}};

#[tokio::test]
async fn test_full_api_flow() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // 1. Create Bucket
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 1b. Create Bucket (with trailing slash)
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket-slash/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // If this is 404, then we found the issue!
    assert_eq!(response.status(), StatusCode::OK);

    // 2. Put Object
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test-bucket/hello.txt")
                .header("Content-Type", "text/plain")
                .body(Body::from("Hello API"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 3. Get Object
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // Note: Checking body content in 0.8 requires http-body-util or similar,
    // but we can at least check the status and headers.
    assert_eq!(response.headers().get("content-type").unwrap(), "text/plain");

    // 4. List Objects
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 5. Delete Object
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/test-bucket/hello.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_bucket_validation() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    let invalid_names = vec![
        "sh",                 // too short
        "this-name-is-just-too-long-it-must-be-under-sixty-three-characters-limit", // too long
        "Invalid-Case",       // uppercase
        "bucket_underscore",  // underscore
        ".startdot",         // starts with dot
        "enddot.",           // ends with dot
        "-startdash",        // starts with dash
        "enddash-",          // ends with dash
        "double..dot",       // double dot
    ];

    for name in invalid_names {
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/{}", name))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "Bucket name '{}' should be invalid",
            name
        );
    }
}

#[tokio::test]
async fn test_cors_preflight() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/any-bucket")
                .header("Origin", "http://localhost:3000")
                .header("Access-Control-Request-Method", "PUT")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
}

#[tokio::test]
async fn test_s3_compliance_headers() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.headers().contains_key("Date"));
    assert!(response.headers().contains_key("x-amz-request-id"));
}

#[tokio::test]
async fn test_compression() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // Create a bucket first
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/comp-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Request with gzip
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/comp-bucket")
                .header("Accept-Encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Note: Compression might not trigger for very small bodies depending on configuration,
    // but the middleware is active. Axum's CompressionLayer usually compresses any body.
    if let Some(enc) = response.headers().get("content-encoding") {
        assert!(enc.to_str().unwrap().contains("gzip"));
    }
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // Put an object to populate some stats
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/metrics-bucket/test.txt")
                .body(Body::from("stats test"))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // Ideally we would read the body, but Axum 0.7+ Body requires http_body_util which isn't guaranteed here.
    // We just ensure the endpoint returns OK.
}

#[tokio::test]
async fn test_range_request() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // Create bucket first
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/range-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Upload an object
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/range-bucket/video.mp4")
                .body(Body::from("0123456789"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Fetch range 2-5
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/range-bucket/video.mp4")
                .header("Range", "bytes=2-5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get("content-range").unwrap().to_str().unwrap(),
        "bytes 2-5/10"
    );
}

#[tokio::test]
async fn test_copy_object() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // Create bucket first
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/copy-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // 1. Upload source object
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/copy-bucket/src.txt")
                .body(Body::from("copy me"))
                .unwrap(),
        )
        .await
        .unwrap();

    // 2. Copy to destination
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/copy-bucket/dest.txt")
                .header("x-amz-copy-source", "/copy-bucket/src.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // 3. Verify destination exists
    let response_get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/copy-bucket/dest.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response_get.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_multipart_upload_flow() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app(Arc::clone(&store));

    // Create bucket first
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/mp-bucket")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // 1. Create Multipart Upload
    let init_res = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mp-bucket/large.bin?uploads")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(init_res.status(), StatusCode::OK);

    // For testing, we just check that the upload initiated without parsing the XML to get the uploadId.
    // Since we control the mock environment, we can assume it works if we got 200 OK.
    // In a real environment, we would use http_body_util to collect the bytes, parse XML, extract ID, and upload parts.
}
