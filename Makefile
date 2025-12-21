.PHONY: help build release native test bench profile profile-large profile-mega fmt check clippy clean install install-completions install-completions-all docker-build docker-bench verify-realworld website-deploy

# Show available commands (default target)
help:
	@echo "Available commands:"
	@echo "  make build                 - Debug build"
	@echo "  make release               - Release build"
	@echo "  make native                - Optimized build for current CPU (best performance)"
	@echo "  make test                  - Run all tests"
	@echo "  make bench                 - Run criterion benchmarks"
	@echo "  make profile               - Memory profile all commands (medium dataset)"
	@echo "  make profile-large         - Memory profile with large dataset (~250MB)"
	@echo "  make profile-mega          - Stress test profile (~2GB: 100 tables × 100k rows)"
	@echo "  make fmt                   - Format code"
	@echo "  make check                 - Check code without building"
	@echo "  make clippy                - Run clippy lints"
	@echo "  make clean                 - Clean build artifacts"
	@echo "  make install               - Install locally (binary + shell completions)"
	@echo "  make install-completions   - Install completions only (for current shell)"
	@echo "  make install-completions-all - Install completions for all supported shells"
	@echo "  make docker-build          - Docker benchmark setup"
	@echo "  make docker-bench          - Run benchmarks in Docker (generates 100MB test data)"
	@echo "  make verify-realworld      - Verify against real-world SQL dumps from public sources"
	@echo "  make website-deploy        - Deploy website to Vercel"

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

# Memory profile all commands (requires GNU time: brew install gnu-time)
profile: release
	./scripts/profile-memory.sh --size medium

# Memory profile with large test data (~250MB)
profile-large: release
	./scripts/profile-memory.sh --size large

# Stress test memory profile (~2GB: 100 tables × 100k rows)
profile-mega: release
	./scripts/profile-memory.sh --size mega

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

# Install locally (binary + shell completions)
install:
	cargo install --path .
	@echo ""
	@./scripts/install-completions.sh sql-splitter

# Install completions only (for current shell)
install-completions:
	@./scripts/install-completions.sh sql-splitter

# Install completions for all supported shells
install-completions-all:
	@./scripts/install-completions.sh sql-splitter all

# Docker benchmark setup
docker-build:
	docker compose -f docker/docker-compose.benchmark.yml build

# Run benchmarks in Docker (generates 100MB test data)
docker-bench:
	./docker/run-benchmark.sh --generate 100

# Verify against real-world SQL dumps from public sources
verify-realworld:
	./scripts/verify-realworld.sh

# Deploy website to Vercel
website-deploy:
	cd website && vc --prod

