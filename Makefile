.PHONY: build release native test bench fmt check clippy clean install docker-build docker-bench

# Debug build
build:
	cargo build

# Release build
release:
	cargo build --release

# Optimized build for current CPU (best performance)
native:
	RUSTFLAGS="-C target-cpu=native" cargo build --release

# Run all tests
test:
	cargo test

# Run criterion benchmarks
bench:
	cargo bench

# Format code
fmt:
	cargo fmt

# Check code without building
check:
	cargo check

# Run clippy lints
clippy:
	cargo clippy -- -D warnings

# Clean build artifacts
clean:
	cargo clean

# Install locally
install:
	cargo install --path .

# Docker benchmark setup
docker-build:
	docker compose -f docker/docker-compose.benchmark.yml build

# Run benchmarks in Docker (generates test data)
docker-bench:
	./docker/run-benchmark.sh --generate
