.PHONY: build build-release build-native test bench bench-all bench-compare clean fmt check clippy bench-docker bench-docker-build bench-docker-file test-workflows test-ci test-release test-job

# Default build (debug)
build:
	cargo build

# Release build
build-release:
	cargo build --release

# Optimized build for current CPU (best for benchmarking)
build-native:
	RUSTFLAGS="-C target-cpu=native" cargo build --release

# Run all tests
test:
	cargo test

# Run tests with output
test-verbose:
	cargo test -- --nocapture

# Run all criterion benchmarks
bench:
	cargo bench

# Run parser benchmarks only
bench-parser:
	cargo bench --bench parser_bench

# Run writer benchmarks only
bench-writer:
	cargo bench --bench writer_bench

# Run end-to-end benchmarks only
bench-e2e:
	cargo bench --bench e2e_bench

# Run throughput benchmarks
bench-throughput:
	cargo bench -- parser_throughput

# Run buffer size comparison benchmarks
bench-buffers:
	cargo bench -- buffer_sizes

# Run comprehensive benchmark against all competitor tools
# Usage: make bench-all [FILE=/path/to/dump.sql]
bench-all:
	@if [ -z "$(FILE)" ]; then \
		./scripts/benchmark-all.sh; \
	else \
		./scripts/benchmark-all.sh "$(FILE)"; \
	fi

# Quick benchmark with smaller test files
bench-quick:
	./scripts/benchmark-all.sh --sizes 10 --runs 3 --warmup 1

# Benchmark only the Rust implementation (no competitor comparison)
bench-rust-only:
	./scripts/benchmark-all.sh --rust-only

# Legacy: Run old comparison script
bench-compare:
	@if [ -z "$(FILE)" ]; then \
		./scripts/benchmark.sh; \
	else \
		./scripts/benchmark.sh "$(FILE)"; \
	fi

# Generate HTML benchmark report
bench-report:
	cargo bench -- --verbose
	@echo "Report available at: target/criterion/report/index.html"

# Profile with flamegraph (requires cargo-flamegraph)
profile:
	@echo "Profiling split command..."
	cargo flamegraph --bin sql-splitter -- split /tmp/benchmark_test.sql -o /tmp/profile-output

# Format code
fmt:
	cargo fmt

# Check code without building
check:
	cargo check

# Clippy lints
clippy:
	cargo clippy -- -D warnings

# Clean build artifacts
clean:
	cargo clean
	rm -rf /tmp/rs-bench /tmp/go-bench

# Build and run help
run-help:
	cargo run --release -- --help


# Show binary size
size:
	@ls -lh target/release/sql-splitter 2>/dev/null || echo "Run 'make build-release' first"

# Install locally
install:
	cargo install --path .

# Uninstall
uninstall:
	cargo uninstall sql-splitter

# Docker benchmarking
bench-docker-build:
	docker compose -f docker/docker-compose.benchmark.yml build

bench-docker:
	./docker/run-benchmark.sh -- --generate

# Docker benchmark with custom file
# Usage: make bench-docker-file FILE=/path/to/dump.sql
bench-docker-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make bench-docker-file FILE=/path/to/dump.sql"; \
		exit 1; \
	fi
	./docker/run-benchmark.sh --file "$(FILE)"

# === GitHub Actions Testing with act ===

# Test all workflows locally using act
test-workflows:
	@command -v act >/dev/null 2>&1 || { echo "Install act: brew install act"; exit 1; }
	act --list

# Test the test workflow
test-ci:
	@command -v act >/dev/null 2>&1 || { echo "Install act: brew install act"; exit 1; }
	act -W .github/workflows/test.yml

# Test the release workflow (dry run)
test-release:
	@command -v act >/dev/null 2>&1 || { echo "Install act: brew install act"; exit 1; }
	act -W .github/workflows/release.yml -n

# Test with specific job
test-job:
	@if [ -z "$(JOB)" ]; then echo "Usage: make test-job JOB=lint"; exit 1; fi
	act -j $(JOB)
