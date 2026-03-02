use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;
use vaporstore::{app_with_config, config::Config, storage::{InMemoryBackend, StorageBackend}};

#[tokio::test]
async fn test_health_check_returns_ok_with_stats() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app_with_config(store, Config::default());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let body_json = String::from_utf8(bytes.to_vec()).unwrap();
    
    assert!(body_json.contains("\"status\":\"ok\""));
    assert!(body_json.contains("\"version\""));
    assert!(body_json.contains("\"uptime_seconds\""));
}

#[tokio::test]
async fn test_bucket_creation_date_remains_constant() {
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::new());
    let app = app_with_config(Arc::clone(&store), Config::default());

    // Create bucket
    let bucket_name = "test-const-date";
    let _ = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&format!("/{}", bucket_name))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Initial list
    let resp_initial = app.clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let bytes_initial = axum::body::to_bytes(resp_initial.into_body(), 4096).await.unwrap();
    let xml_initial = String::from_utf8(bytes_initial.to_vec()).unwrap();
    
    let date_marker = "<CreationDate>";
    let start = xml_initial.find(date_marker).unwrap() + date_marker.len();
    let end = xml_initial.find("</CreationDate>").unwrap();
    let date_first = &xml_initial[start..end];

    // Wait 1.1s to ensure a clock tick would change the date if it weren't constant
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Second list
    let resp_later = app
        .oneshot(
            Request::builder()
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let bytes_later = axum::body::to_bytes(resp_later.into_body(), 4096).await.unwrap();
    let xml_later = String::from_utf8(bytes_later.to_vec()).unwrap();
    
    let start_later = xml_later.find(date_marker).unwrap() + date_marker.len();
    let end_later = xml_later.find("</CreationDate>").unwrap();
    let date_second = &xml_later[start_later..end_later];

    assert_eq!(date_first, date_second, "Bucket creation date changed on subsequent lists!");
}

#[tokio::test]
async fn test_enforce_maximum_buckets_limit() {
    let mut config = Config::default();
    config.max_buckets = 2;
    
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::with_config(config.clone()));
    let app = app_with_config(store, config);

    // Create 1st
    let resp_1 = app.clone().oneshot(Request::builder().method("PUT").uri("/bucket1").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp_1.status(), StatusCode::OK);

    // Create 2nd
    let resp_2 = app.clone().oneshot(Request::builder().method("PUT").uri("/bucket2").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp_2.status(), StatusCode::OK);

    // Create 3rd (fails)
    let resp_excess = app.clone().oneshot(Request::builder().method("PUT").uri("/bucket3").body(Body::empty()).unwrap()).await.unwrap();
    let status = resp_excess.status();
    let bytes = axum::body::to_bytes(resp_excess.into_body(), 4096).await.unwrap();
    let body_err = String::from_utf8(bytes.to_vec()).unwrap();
    
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "Expected 500 for limit exceeded, got {}. Body: {}", status, body_err);
}

#[tokio::test]
async fn test_enforce_maximum_objects_per_bucket_limit() {
    let mut config = Config::default();
    config.max_objects_per_bucket = 2;
    
    let store: Arc<dyn StorageBackend + Send + Sync> = Arc::new(InMemoryBackend::with_config(config.clone()));
    let app = app_with_config(store, config);

    // Create bucket first
    let resp_bucket = app.clone().oneshot(Request::builder().method("PUT").uri("/limit-bucket").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp_bucket.status(), StatusCode::OK);

    // Put 1st object
    let resp_obj1 = app.clone().oneshot(Request::builder().method("PUT").uri("/limit-bucket/obj1").body(Body::from("data")).unwrap()).await.unwrap();
    assert_eq!(resp_obj1.status(), StatusCode::OK);

    // Put 2nd object
    let resp_obj2 = app.clone().oneshot(Request::builder().method("PUT").uri("/limit-bucket/obj2").body(Body::from("data")).unwrap()).await.unwrap();
    assert_eq!(resp_obj2.status(), StatusCode::OK);

    // Put 3rd object (should fail)
    let resp_excess = app.clone().oneshot(Request::builder().method("PUT").uri("/limit-bucket/obj3").body(Body::from("data")).unwrap()).await.unwrap();
    assert_eq!(resp_excess.status(), StatusCode::INTERNAL_SERVER_ERROR);
    
    // Update existing object (should still be allowed)
    let resp_update = app.clone().oneshot(Request::builder().method("PUT").uri("/limit-bucket/obj1").body(Body::from("new data")).unwrap()).await.unwrap();
    assert_eq!(resp_update.status(), StatusCode::OK);
}
