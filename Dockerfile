# syntax=docker/dockerfile:1.7
# Multi-stage build for sql-splitter.
# Builder uses the latest stable Rust on Debian bookworm so glibc matches the runtime.
# Build deps include g++/cmake/pkg-config because duckdb (bundled) compiles its C++ from source.

FROM rust:slim-bookworm AS builder
WORKDIR /app
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      g++ \
      cmake \
      pkg-config \
      libssl-dev \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --locked --bin sql-splitter

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/sql-splitter /usr/local/bin/sql-splitter
WORKDIR /data
ENTRYPOINT ["sql-splitter"]
