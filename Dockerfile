# syntax=docker/dockerfile:1.7
# Multi-stage build for sql-splitter.
# Build deps include g++/cmake/pkg-config because duckdb (bundled) compiles its C++ from source.

FROM rust:1-alpine AS builder
WORKDIR /app
RUN apk add --no-cache \
      g++ \
      cmake \
      pkgconfig \
      openssl-dev \
      musl-dev \
      make \
      perl \
      linux-headers \
      ca-certificates
COPY . .
RUN cargo build --release --locked --bin sql-splitter \
 && strip target/release/sql-splitter

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/sql-splitter /usr/local/bin/sql-splitter
WORKDIR /data
ENTRYPOINT ["sql-splitter"]
