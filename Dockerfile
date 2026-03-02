# Build stage
FROM rust:1 as builder

WORKDIR /usr/src/vaporstore
COPY . .

# Build the binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install ca-certificates and curl for health checks
RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/vaporstore/target/release/vaporstore /app/vaporstore

# Ensure the binary has execution permissions and create data directory
RUN chmod +x /app/vaporstore && mkdir -p /app/data

EXPOSE 9353

# Environment variables
ENV RUST_LOG=vaporstore=info
ENV PORT=9353
ENV VAPORSTORE_DATA_DIR=/app/data

# Health check using the internal health endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:9353/health || exit 1

# Run the binary
CMD ["./vaporstore"]
