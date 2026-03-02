# VaporStore: In-Memory S3-Compatible Object Storage

# VaporStore

Minimal, fast, in-memory S3-compatible object storage. Written with Rust + Axum.

## Features

-  Compatible with AWS CLI and S3 SDKs (path-style addressing).
-  **Bucket Validation**: Enforces S3 naming rules (3-63 characters, lowercase alphanumeric).
-  **Observability**: Integrated tracing spans and structured logging via `tower-http`.
-  **CORS & Compression**: Pre-configured CORS for web browsers and Gzip/Brotli/Zstd compression.
-  **S3 Compliance**: Mandatory `Date` and `x-amz-request-id` headers in all responses.
-  TTL for every object (default **5 minutes**).
-  Maximum file size **5 MB**.
-  Automatic background TTL cleanup (every 30 seconds).
-  Zero disk I/O — entirely in-memory.
-  No Authentication — for local/dev use only.

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

# Upload with custom TTL (using x-amz-meta-ttl-seconds header)
aws --endpoint-url $ENDPOINT s3 cp ./myfile.txt s3://my-bucket/myfile.txt \
  --metadata ttl-seconds=60

# List objects
aws --endpoint-url $ENDPOINT s3 ls s3://my-bucket/

# Download a file
aws --endpoint-url $ENDPOINT s3 cp s3://my-bucket/myfile.txt ./downloaded.txt

# Delete an object
aws --endpoint-url $ENDPOINT s3 rm s3://my-bucket/myfile.txt

# Delete a bucket (must be empty first)
aws --endpoint-url $ENDPOINT s3 rb s3://my-bucket
```

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
| Multipart Upload | ❌ |
| Presigned URLs | ❌ |

## Limits

| Feature | Value |
|---------|-------|
| Max object size | 5 MB |
| Default TTL | 5 minutes (300s) |
| Custom TTL header | `x-amz-meta-ttl-seconds` |
| Storage | In-memory (data is lost on restart) |

## Environment Variables

| Variable | Default | Description |
|----------|-----------|---------|
| `PORT` | `9353` | Port to listen on |
| `RUST_LOG` | `vaporstore=info` | Log level |