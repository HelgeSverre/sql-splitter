# sql-splitter justfile
# Run `just` to see available commands

# Show available commands (default target)
default:
    @just --list

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

# Benchmark against competitor tools (generates 100MB test data if no file provided)
bench-competitors file="":
    ./scripts/benchmark-competitors.sh {{ file }}

# Docker benchmark (generates test data of specified size in MB)
docker-bench size="100":
    ./docker/run-benchmark.sh --generate {{ size }}

# Docker benchmark with a specific file
docker-bench-file file:
    ./docker/run-benchmark.sh {{ file }}

# Build Docker benchmark container
docker-build:
    docker compose -f docker/docker-compose.benchmark.yml build

# Memory profile all commands (medium dataset)
profile: release
    ./scripts/profile-memory.sh --size medium --output benchmark-results/profile-medium.txt

# Memory profile with large dataset (~125MB)
profile-large: release
    ./scripts/profile-memory.sh --size large --output benchmark-results/profile-large.txt

# Stress test memory profile (~1GB: 100 tables × 100k rows)
profile-mega: release
    ./scripts/profile-memory.sh --size mega --output benchmark-results/profile-mega.txt

# Extreme stress test (~10GB MySQL only)
profile-giga: release
    ./scripts/profile-memory.sh --size giga --output benchmark-results/profile-giga.txt

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

# Install locally (binary + shell completions + man pages)
install: man
    cargo install --path .
    @echo ""
    @./scripts/install-completions.sh sql-splitter
    @./scripts/install-man.sh

# Install completions only (for current shell)
install-completions:
    @./scripts/install-completions.sh sql-splitter

# Install completions for all supported shells
install-completions-all:
    @./scripts/install-completions.sh sql-splitter all

# Install man pages only
install-man: man
    @./scripts/install-man.sh

# Verify against real-world SQL dumps from public sources
verify-realworld:
    cargo test --test realworld -- --ignored

# Deploy website to Vercel
website-deploy:
    cd website && vc --prod

# Generate man pages
man:
    cargo run --example generate-man
    @echo ""
    @echo "Man pages generated in man/ directory"
