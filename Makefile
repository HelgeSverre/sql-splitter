.PHONY: help build release native test bench profile profile-large profile-mega profile-giga fmt check clippy clean install install-completions install-completions-all install-man docker-build docker-bench verify-realworld website-deploy man schemas version website-update-version release-prepare

# Show available commands (default target)
help:
	@echo "Available commands:"
	@echo "  make build                 - Debug build"
	@echo "  make release               - Release build"
	@echo "  make native                - Optimized build for current CPU (best performance)"
	@echo "  make test                  - Run all tests"
	@echo "  make bench                 - Run criterion benchmarks"
	@echo "  make profile               - Memory profile all commands (medium dataset)"
	@echo "  make profile-large         - Memory profile with large dataset (~125MB)"
	@echo "  make profile-mega          - Stress test profile (~1GB: 100 tables × 100k rows)"
	@echo "  make profile-giga          - Extreme stress test (~10GB MySQL only)"
	@echo "  make fmt                   - Format code (Rust + Markdown)"
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
	@echo "  make man                   - Generate man pages"
	@echo "  make schemas               - Generate JSON schemas from Rust types"
	@echo "  make version               - Show current version from Cargo.toml"
	@echo "  make website-update-version - Update version in website files"
	@echo "  make release-prepare       - Prepare release (build, test, update website)"

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
	./scripts/profile-memory.sh --size medium --output benchmark-results/profile-medium.txt

# Memory profile with large test data (~125MB)
profile-large: release
	./scripts/profile-memory.sh --size large --output benchmark-results/profile-large.txt

# Stress test memory profile (~1GB: 100 tables × 100k rows)
profile-mega: release
	./scripts/profile-memory.sh --size mega --output benchmark-results/profile-mega.txt

# Extreme stress test (~10GB MySQL only, takes 10-30 min to generate)
profile-giga: release
	./scripts/profile-memory.sh --size giga --output benchmark-results/profile-giga.txt

# Format code (Rust + Markdown)
fmt:
	cargo fmt
	npx prettier --write "**/*.md" --log-level warn

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

# Docker benchmark setup
docker-build:
	docker compose -f docker/docker-compose.benchmark.yml build

# Run benchmarks in Docker (generates 100MB test data)
docker-bench:
	./docker/run-benchmark.sh --generate 100

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
	@echo "Install with: sudo cp man/*.1 /usr/local/share/man/man1/"

# Generate JSON schemas from Rust types, validate, and copy to website
schemas: release
	@echo "Generating JSON schemas from Rust types..."
	./target/release/sql-splitter schema -o schemas/
	@echo ""
	@echo "Formatting schemas with prettier..."
	npx prettier --write "schemas/*.schema.json" --log-level warn
	@echo ""
	@echo "Validating schemas against actual CLI output..."
	cargo test --test json_schema_tests -- --quiet
	@echo ""
	@echo "Copying schemas to website..."
	cp schemas/*.schema.json website/public/schemas/
	@echo ""
	@echo "✓ Schemas generated, formatted, validated, and copied to website/public/schemas/"

# Show current version from Cargo.toml
version:
	@grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'

# Update version in website files from Cargo.toml
website-update-version:
	cd website && npx tsx scripts/update-version.ts

# Prepare release (builds, tests, updates website version)
release-prepare: release test schemas website-update-version
	@echo ""
	@echo "✓ Release preparation complete"
	@echo ""
	@echo "Version: $$(make version)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Update CHANGELOG.md"
	@echo "  2. Review and commit changes"
	@echo "  3. Create tag: git tag -a v$$(make version) -m 'Release v$$(make version)'"
	@echo "  4. Push: git push origin main --tags"
	@echo "  5. Create GitHub release: gh release create v$$(make version) --latest"

