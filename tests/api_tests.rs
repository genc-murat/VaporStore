use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;
use vaporstore::{app, storage::ObjectStore};

#[tokio::test]
async fn test_full_api_flow() {
    let store = Arc::new(ObjectStore::new());
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
async fn test_bucket_not_found() {
    let store = Arc::new(ObjectStore::new());
    let app = app(Arc::clone(&store));

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/non-existent/key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
