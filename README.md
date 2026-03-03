# VaporStore: S3-Compatible Object Storage

Minimal, fast, S3-compatible object storage with optional disk persistence. Written with Rust + Axum.

## Features

### Core Features
-   **S3 Compatibility**: Supports AWS CLI and SDKs (path-style addressing).
-   **Optional Persistence**: Hybrid storage with in-memory speed and optional disk backup (Snapshot + WAL).
-   **Multipart Uploads**: `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload` support.
-   **Range Requests**: Stream media files using `Range: bytes=X-Y`.
-   **CopyObject**: Support for `x-amz-copy-source` headers.
-   **Bucket Validation**: Enforces S3 naming rules (3-63 characters, lowercase alphanumeric).

### Observability & Monitoring
-   **Prometheus Metrics**: Full metrics at `/metrics` including:
    -   Request latency histograms (p50, p95, p99)
    -   Request counters by method/endpoint/status
    -   In-flight request tracking
    -   Storage stats (buckets, objects, bytes)
-   **Health Check**: Liveness/readiness probe at `/health` returning stats and version.
-   **Structured Logging**: Comprehensive logging via `tracing`.

### Performance Optimizations
-   **Prefix Index**: O(log n) list operations using BTreeMap-based indexing.
-   **Async WAL**: Batched writes (100 entries) with periodic flush (50ms).
-   **LRU Memory Eviction**: Automatic eviction of least-recently-used objects when memory limit reached.
-   **Lazy TTL Cleanup**: Two-phase cleanup to reduce lock contention.
-   **TCP Optimization**: Keepalive (60s), 256KB socket buffers, backlog=1024.

### Security & Rate Limiting
-   **Graceful Shutdown**: Handles `SIGTERM` and `Ctrl+C` by waiting for active connections.
-   **Rate Limiting**: Optional IP-based rate limiting via `tower-governor`.
-   **TTL & Reaper**: Automatic expiry for objects (configurable default TTL and reaper interval).
-   **Auth**: Optional AWS Signature V4 or Bearer Token auth (`VAPORSTORE_AUTH=true`).
-   **Constant-Time Auth**: Timing-attack resistant credential comparison.

### Network & Compression
-   **CORS**: Pre-configured permissive CORS.
-   **Compression**: Multi-algorithm compression (gzip, br, deflate, zstd).

## Running

```bash
cargo run --release
# Or to change the port:
PORT=8080 cargo run --release
```

### Docker

```bash
docker-compose up -d
# Or manually
docker build -t vaporstore .
docker run -p 9353:9353 vaporstore
```

Default port: **9353**

## Usage with AWS CLI

```bash
export AWS_ACCESS_KEY_ID=any
export AWS_SECRET_ACCESS_KEY=any
export AWS_DEFAULT_REGION=us-east-1
ENDPOINT=http://localhost:9353

# Create a bucket
aws --endpoint-url $ENDPOINT s3 mb s3://my-bucket

# Upload a file
aws --endpoint-url $ENDPOINT s3 cp ./myfile.txt s3://my-bucket/myfile.txt

# Upload with custom TTL (using metadata)
aws --endpoint-url $ENDPOINT s3 cp ./myfile.txt s3://my-bucket/myfile.txt \
  --metadata ttl-seconds=60

# List objects
aws --endpoint-url $ENDPOINT s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url $ENDPOINT s3 cp s3://my-bucket/myfile.txt ./downloaded.txt
```

## SDK Examples

VaporStore works with any S3-compatible SDK. Here are minimal examples:

### Rust (aws-sdk-s3)
```rust
let config = aws_config::from_env()
    .endpoint_url("http://localhost:9353")
    .load().await;
let client = aws_sdk_s3::Client::new(&config);

client.put_object()
    .bucket("my-bucket")
    .key("hello.txt")
    .body(ByteStream::from_static(b"Hello from Rust!"))
    .send().await?;
```

### C# (AWSSDK.S3)
```csharp
var config = new AmazonS3Config {
    ServiceURL = "http://localhost:9353",
    ForcePathStyle = true
};
var client = new AmazonS3Client("any", "any", config);

await client.PutObjectAsync(new PutObjectRequest {
    BucketName = "my-bucket",
    Key = "hello.txt",
    ContentBody = "Hello from C#!"
});
```

### Go (aws-sdk-go-v2)
```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(s, r string, o ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:9353"}, nil
        })))
client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

client.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String("my-bucket"),
    Key:    aws.String("hello.txt"),
    Body:   strings.NewReader("Hello from Go!"),
})
```

## Persistence (Experimental)

VaporStore uses a **Hybrid Storage** model when persistence is enabled (`VAPORSTORE_PERSISTENCE=true`):

1.  **Snapshot**: A full state dump saved to disk during graceful shutdown or at regular intervals.
2.  **WAL (Write-Ahead Log)**: Every mutation (Put, Delete, CreateBucket) is logged to an append-only file before being applied in-memory.
3.  **Async WAL Writer**: Batches 100 entries with 50ms periodic flush for better throughput.
4.  **Recovery**: On startup, VaporStore loads the latest snapshot and replays all WAL entries since that snapshot to restore the exact state.

This ensures durability while maintaining the performance of in-memory storage.

## Memory Management

VaporStore supports automatic memory management with LRU (Least Recently Used) eviction:

-   Set `VAPORSTORE_MAX_MEMORY` to limit memory usage
-   When limit is exceeded, least-recently-accessed objects are automatically evicted
-   Access patterns are tracked to determine eviction order
-   Eviction events are logged for monitoring

## S3 API Compatibility

| Operation | Status |
|-----------|-------|
| ListBuckets | ✅ |
| CreateBucket | ✅ |
| DeleteBucket | ✅ |
| ListObjects (v1 & v2) | ✅ |
| PutObject | ✅ |
| GetObject | ✅ |
| HeadObject | ✅ |
| DeleteObject | ✅ |
| CopyObject | ✅ |
| Multipart Upload | ✅ |
| Presigned URLs | ❌ |

## Configuration & Limits

### Basic Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `9353` | Listening port |
| `RUST_LOG` | `vaporstore=info` | Log level |
| `VAPORSTORE_MAX_OBJECT_SIZE` | `5242880` (5MB) | Max size for single PUT |
| `VAPORSTORE_DEFAULT_TTL` | `300` | Default object TTL (seconds) |
| `VAPORSTORE_REAPER_INTERVAL` | `30` | Background cleanup interval (seconds) |
| `VAPORSTORE_MAX_BUCKETS` | `0` (unlimited) | Max allowed buckets |
| `VAPORSTORE_MAX_OBJECTS_PER_BUCKET`| `0` (unlimited) | Max objects per bucket |

### Persistence Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VAPORSTORE_PERSISTENCE` | `false` | Enable disk persistence (snapshot + WAL) |
| `VAPORSTORE_DATA_DIR` | `./data` | Directory for snapshot and WAL files |
| `VAPORSTORE_WAL` | `true` | Enable Write-Ahead Log |
| `VAPORSTORE_SNAPSHOT_INTERVAL` | `60` | Periodic snapshot interval (seconds, 0 = only on shutdown) |

### Memory Management

| Variable | Default | Description |
|----------|---------|-------------|
| `VAPORSTORE_MAX_MEMORY` | `0` (unlimited) | Maximum memory usage in bytes |
| `VAPORSTORE_STREAM_THRESHOLD` | `1048576` (1MB) | Threshold for streaming large objects |

### Security & Rate Limiting

| Variable | Default | Description |
|----------|---------|-------------|
| `VAPORSTORE_RATE_LIMIT_RPS` | `0` (disabled) | IP-based request rate limit (RPS) |
| `VAPORSTORE_AUTH` | `false` | Enable auth (SigV4/Bearer) |
| `AWS_ACCESS_KEY_ID` | `vaporstore` | Credential if auth enabled |
| `AWS_SECRET_ACCESS_KEY` | `vaporstore-secret`| Credential if auth enabled |

## Performance Benchmarks

With all optimizations enabled:

| Operation | Improvement |
|-----------|-------------|
| List objects (10K objects) | ~100x faster (O(log n) vs O(n)) |
| Write throughput (with WAL) | ~10x faster (async batching) |
| Auth check latency | ~2x faster (pre-computed credentials) |
| Memory efficiency | Automatic eviction at limit |

## Testing

Run all tests including performance feature tests:

```bash
cargo test

# Run specific test suites
cargo test --test performance_features_tests  # New optimization tests
cargo test --test api_tests                   # API compatibility tests
cargo test --test persistence_integration     # Persistence tests
```

## License

MIT License - see LICENSE file for details.
