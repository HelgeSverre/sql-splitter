# sql-splitter justfile — run `just` (or `just --list`) to see grouped commands.
# External tools: coverage → cargo-llvm-cov; flamegraph/samply → cargo-flamegraph / samply.
# Website recipes auto-install npm deps on first use (see the private `_website-deps`).

# Show available commands (default)
default:
    @just --list

# Debug build
[group('build')]
build:
    cargo build

# Release build
[group('build')]
release:
    cargo build --release

# Release build tuned for the current CPU
[group('build')]
native:
    RUSTFLAGS="-C target-cpu=native" cargo build --release

# Build with profiling symbols (flamegraph/samply)
[group('build')]
build-profiling:
    cargo build --profile profiling

# Format code (Rust + Markdown)
[group('lint')]
fmt:
    cargo fmt
    npx prettier --write "**/*.md" --log-level warn

# Type-check without building
[group('lint')]
check:
    cargo check

# Run clippy (deny warnings)
[group('lint')]
clippy:
    cargo clippy -- -D warnings

# Clean build artifacts
[group('lint')]
clean:
    cargo clean

# Run all tests (nextest)
[group('test')]
test:
    cargo nextest run

# Coverage HTML report (opens in browser)
[group('test')]
coverage:
    cargo llvm-cov nextest --html --open

# Coverage summary in the terminal
[group('test')]
coverage-summary:
    cargo llvm-cov nextest --summary-only

# Coverage as ./lcov.info (for CI / Codecov)
[group('test')]
coverage-lcov:
    cargo llvm-cov nextest --lcov --output-path lcov.info

# Verify against real-world SQL dumps
[group('test')]
verify-realworld:
    cargo nextest run --test realworld --run-ignored only

# Run criterion benchmarks
[group('bench')]
bench:
    cargo bench

# Benchmark against competitor tools (optional file, else generates 100MB)
[group('bench')]
bench-competitors file="":
    ./scripts/benchmark-competitors.sh {{ file }}

# Save a criterion baseline
[group('bench')]
bench-baseline name="main":
    cargo bench -- --save-baseline {{ name }}

# Compare benchmarks against a saved baseline
[group('bench')]
bench-compare baseline="main":
    cargo bench -- --baseline {{ baseline }}

# Flamegraph for the split command
[group('bench')]
flamegraph file: build-profiling
    @mkdir -p benchmark-results
    cargo flamegraph --profile profiling --bin sql-splitter -o benchmark-results/flamegraph-split.svg -- split {{ file }}

# Profile split with samply (opens Firefox Profiler)
[group('bench')]
samply file: build-profiling
    samply record ./target/profiling/sql-splitter split {{ file }}

# Memory profile, medium dataset
[group('bench')]
profile: release
    ./scripts/profile-memory.sh --size medium --output benchmark-results/profile-medium.txt

# Memory profile, large dataset (~125MB)
[group('bench')]
profile-large: release
    ./scripts/profile-memory.sh --size large --output benchmark-results/profile-large.txt

# Memory profile, mega dataset (~1GB: 100 tables × 100k rows)
[group('bench')]
profile-mega: release
    ./scripts/profile-memory.sh --size mega --output benchmark-results/profile-mega.txt

# Memory profile, giga dataset (~10GB, MySQL only)
[group('bench')]
profile-giga: release
    ./scripts/profile-memory.sh --size giga --output benchmark-results/profile-giga.txt

# Build the Docker benchmark container
[group('docker')]
docker-build:
    docker compose -f docker/docker-compose.benchmark.yml build

# Docker benchmark (generates data, size in MB)
[group('docker')]
docker-bench size="100":
    ./docker/run-benchmark.sh --generate {{ size }}

# Docker benchmark with a specific file
[group('docker')]
docker-bench-file file:
    ./docker/run-benchmark.sh {{ file }}

# Install locally (binary + completions + man pages)
[group('install')]
install: man
    cargo install --path .
    @echo ""
    @./scripts/install-completions.sh sql-splitter
    @./scripts/install-man.sh

# Install completions for the current shell
[group('install')]
install-completions:
    @./scripts/install-completions.sh sql-splitter

# Install completions for all supported shells
[group('install')]
install-completions-all:
    @./scripts/install-completions.sh sql-splitter all

# Install man pages only
[group('install')]
install-man: man
    @./scripts/install-man.sh

# Generate man pages
[group('install')]
man:
    cargo run --example generate-man
    @echo ""
    @echo "Man pages generated in man/ directory"

# Generate + validate JSON schemas, copy to website
[group('schema')]
schemas: release
    @echo "Generating JSON schemas from Rust types..."
    ./target/release/sql-splitter schema -o schemas/
    @echo ""
    @echo "Formatting schemas with prettier..."
    npx prettier --write "schemas/*.schema.json" --log-level warn
    @echo ""
    @echo "Validating schemas against actual CLI output..."
    cargo nextest run --test json_schema_tests
    @echo ""
    @echo "Copying schemas to website..."
    cp schemas/*.schema.json website/public/schemas/
    @echo ""
    @echo "✓ Schemas generated, formatted, validated, and copied to website/public/schemas/"

# Ensure website deps are installed (idempotent; installs only if missing)
[group('website')]
[private]
_website-deps:
    cd website && ( [ -d node_modules ] && [ -f package-lock.json ] || npm install )

# Build website for production
[group('website')]
website-build: _website-deps
    cd website && npm run build

# Start website dev server (hot reload)
[group('website')]
website-dev: _website-deps
    cd website && npm run dev

# Preview the production build locally (builds first)
[group('website')]
website-preview: website-build
    cd website && npm run preview

# Deploy website to Vercel (production)
[group('website')]
website-deploy:
    cd website && vc --prod

# Check the Astro project (types, diagnostics)
[group('website')]
website-check: _website-deps
    cd website && npm run astro check

# Clean website build artifacts and caches
[group('website')]
website-clean:
    cd website && rm -rf dist .astro node_modules/.cache

# Deep clean (including node_modules)
[group('website')]
website-clean-all:
    cd website && rm -rf dist .astro node_modules

# Clean and rebuild the website from scratch
[group('website')]
website-rebuild: website-clean
    cd website && npm install && npm run build

# Install/update website dependencies
[group('website')]
website-install:
    cd website && npm install

# Update website dependencies to latest
[group('website')]
website-update: _website-deps
    cd website && npm update

# Check for outdated website dependencies
[group('website')]
website-outdated: _website-deps
    cd website && npm outdated

# Audit website dependencies for vulnerabilities
[group('website')]
website-audit: _website-deps
    cd website && npm audit

# Fix website dependency vulnerabilities
[group('website')]
website-audit-fix: _website-deps
    cd website && npm audit fix

# Generate the OG image
[group('website')]
website-og-image: _website-deps
    cd website && node generate-og-image.js

# Validate internal links (runs a build)
[group('website')]
website-validate-links: website-build
    @echo "✓ Links validated during build via starlight-links-validator"

# List available website npm scripts
[group('website')]
website-scripts:
    cd website && npm run

# Open the local website (localhost:4321)
[group('website')]
website-open:
    @echo "Opening http://localhost:4321"
    @open http://localhost:4321 || xdg-open http://localhost:4321 || echo "Please open http://localhost:4321 in your browser"

# Full website maintenance (clean, reinstall, audit, build, check)
[group('website')]
website-maintain: website-clean website-install website-audit website-build website-check
    @echo "✓ Website maintenance complete"

# Website CI checks (build + validation)
[group('website')]
website-ci: website-build
    @echo "✓ Website CI checks passed"

# Sync website version from Cargo.toml
[group('website')]
website-update-version:
    cd website && npx tsx scripts/update-version.ts

# Show the current version from Cargo.toml
[group('release')]
version:
    @grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'

# Bump the version (usage: just bump 1.14.0)
[group('release')]
bump new_version:
    @echo "Bumping version to {{ new_version }}..."
    awk -v new="{{ new_version }}" '!done && /^version = "/ { sub(/^version = ".*"/, "version = \"" new "\""); done=1 } { print }' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml
    cargo check
    @echo "✓ Version bumped to {{ new_version }}"
    @echo ""
    @echo "Next steps:"
    @echo "  1. Update CHANGELOG.md"
    @echo "  2. Run: just release-prepare"

# Prepare a release (build, test, schemas, website version)
[group('release')]
release-prepare: release test schemas website-update-version
    @echo ""
    @echo "✓ Release preparation complete"
    @echo ""
    @echo "Version: $(just version)"
    @echo ""
    @echo "Next steps:"
    @echo "  1. Review and commit changes"
    @echo "  2. Create tag: git tag -a v$(just version) -m 'Release v$(just version)'"
    @echo "  3. Push: git push origin main --tags"
    @echo "  4. Create GitHub release: gh release create v$(just version)"

# Commit + tag a release (usage: just release-tag 1.14.0)
[group('release')]
release-tag version:
    @echo "Creating release v{{ version }}..."
    git add Cargo.toml Cargo.lock CHANGELOG.md
    git commit -m "chore: release v{{ version }}"
    git tag -a v{{ version }} -m "Release v{{ version }}"
    @echo ""
    @echo "✓ Tag v{{ version }} created"
    @echo ""
    @echo "To publish:"
    @echo "  git push origin main --tags"
    @echo "  gh release create v{{ version }} --latest"
