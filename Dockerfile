# Build stage
FROM rust:1 as builder

WORKDIR /usr/src/vaporstore
COPY . .

# Build the binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install ca-certificates for potential HTTPS needs
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/vaporstore/target/release/vaporstore /app/vaporstore

# Ensure the binary has execution permissions
RUN chmod +x /app/vaporstore

EXPOSE 9353

# Environment variables
ENV RUST_LOG=vaporstore=info
ENV PORT=9353

# Run the binary
CMD ["./vaporstore"]
