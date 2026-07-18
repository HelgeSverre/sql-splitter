# sql-splitter justfile — run `just` (or `just --list`) for grouped commands.
# External tools: coverage → cargo-llvm-cov; flamegraph/samply → cargo-flamegraph / samply.
# Website recipes auto-install deps on first use (private `_website-deps`).

# Show available commands (default target)
default:
    @just --list

[group('build')]
build:
    cargo build

[group('build')]
release:
    cargo build --release

# Optimized build for current CPU (best performance)
[group('build')]
native:
    RUSTFLAGS="-C target-cpu=native" cargo build --release

# Build with profiling symbols (for flamegraph/samply)
[group('build')]
build-profiling:
    cargo build --profile profiling

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

# Smoke-test generate against every top-level SQL fixture
[group('test')]
generate-smoke: build
    ./scripts/smoke-test-generate.sh

# Format code (Rust + Markdown)
[group('lint')]
fmt:
    cargo fmt
    bunx prettier --write "**/*.md" --log-level warn

# Check code without building
[group('lint')]
check:
    cargo check

[group('lint')]
clippy:
    cargo clippy -- -D warnings

# Run criterion benchmarks
[group('bench')]
bench:
    cargo bench

# Save criterion benchmark baseline
[group('bench')]
bench-baseline name="main":
    cargo bench -- --save-baseline {{ name }}

# Compare current benchmarks against a saved baseline
[group('bench')]
bench-compare baseline="main":
    cargo bench -- --baseline {{ baseline }}

# Benchmark against competitor tools (generates 100MB test data if no file provided)
[group('bench')]
bench-competitors file="":
    ./scripts/benchmark-competitors.sh {{ file }}

# Docker benchmark (generates test data of specified size in MB)
[group('docker')]
docker-bench size="100":
    ./docker/run-benchmark.sh --generate {{ size }}

# Docker benchmark with a specific file
[group('docker')]
docker-bench-file file:
    ./docker/run-benchmark.sh {{ file }}

# Build Docker benchmark container
[group('docker')]
docker-build:
    docker compose -f docker/docker-compose.benchmark.yml build

# Memory profile all commands (medium dataset)
[group('profile')]
profile: release
    ./scripts/profile-memory.sh --size medium --output benchmark-results/profile-medium.txt

# Memory profile with large dataset (~125MB)
[group('profile')]
profile-large: release
    ./scripts/profile-memory.sh --size large --output benchmark-results/profile-large.txt

# Stress test memory profile (~1GB: 100 tables × 100k rows)
[group('profile')]
profile-mega: release
    ./scripts/profile-memory.sh --size mega --output benchmark-results/profile-mega.txt

# Extreme stress test (~10GB MySQL only)
[group('profile')]
profile-giga: release
    ./scripts/profile-memory.sh --size giga --output benchmark-results/profile-giga.txt

# Generate flamegraph for split command
[group('profile')]
flamegraph file: build-profiling
    @mkdir -p benchmark-results
    cargo flamegraph --profile profiling --bin sql-splitter -o benchmark-results/flamegraph-split.svg -- split {{ file }}

# Profile split command with samply (opens Firefox Profiler)
[group('profile')]
samply file: build-profiling
    samply record ./target/profiling/sql-splitter split {{ file }}

# Clean build artifacts
clean:
    cargo clean

# Install locally (binary + shell completions + man pages)
[group('install')]
install: man
    cargo install --path .
    @echo ""
    @./scripts/install-completions.sh sql-splitter
    @./scripts/install-man.sh

# Install completions only (for current shell)
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

# Generate man pages (real `man` subcommand, built with the man-pages feature)
[group('docs')]
man:
    cargo run --release --features man-pages -- man -o man/
    @echo ""
    @echo "Man pages generated in man/ directory"

# Generate JSON schemas from Rust types (--json output schemas, plus
# generate-config.schema.json for the `generate` command's YAML model/
# overrides language), validate, and copy to website. Safe to run repeatedly:
# regenerating twice in a row produces no diff.
[group('docs')]
schemas: release
    @echo "Generating JSON schemas from Rust types..."
    ./target/release/sql-splitter schema -o schemas/
    @echo ""
    @echo "Formatting schemas with prettier..."
    bunx prettier --write "schemas/*.schema.json" --log-level warn
    @echo ""
    @echo "Validating schemas against actual CLI output and generate fixtures..."
    cargo nextest run --test json_schema_tests
    @echo ""
    @echo "Copying schemas to website..."
    rm -f website/public/schemas/*.schema.json
    cp schemas/*.schema.json website/public/schemas/
    diff -qr schemas website/public/schemas
    @echo ""
    @echo "✓ Schemas generated, validated, and synchronized with website/public/schemas/"

# Install website deps and start the dev server
[group('website')]
website: website-install
    cd website && bun run dev

# Ensure website deps are installed (idempotent; installs only if missing)
[group('website')]
[private]
_website-deps:
    cd website && ( [ -d node_modules ] && [ -f bun.lock ] || bun install )

# Build website for production
[group('website')]
website-build: _website-deps
    cd website && bun run build

# Start development server with hot reload
[group('website')]
website-dev: _website-deps
    cd website && bun run dev

# Preview production build locally (builds first)
[group('website')]
website-preview: website-build
    cd website && bun run preview

# Check Astro project (type checking, diagnostics)
[group('website')]
website-check: _website-deps
    cd website && bun run astro check

# Lint: type-check + format-check (non-mutating, used as a deploy gate)
[group('website')]
website-lint: _website-deps
    cd website && bun run astro check
    cd website && bunx prettier . --check

# Validate generated schema files against the JSON schema spec
[group('website')]
website-validate-schemas: _website-deps
    cd website && bun run validate

# Deploy website to Vercel (production) — refreshes schemas, lints, validates, and builds first; aborts if any step fails
[group('website')]
website-deploy: schemas website-lint website-validate-schemas website-build
    cd website && vc --prod

# Clean website build artifacts and caches
[group('website')]
website-clean:
    cd website && rm -rf dist .astro node_modules/.cache

# Deep clean (including node_modules)
[group('website')]
website-clean-all:
    cd website && rm -rf dist .astro node_modules

# Clean and rebuild website from scratch
[group('website')]
website-rebuild: website-clean
    cd website && bun install && bun run build

# Install/update website dependencies
[group('website')]
website-install:
    cd website && bun install

# Update website dependencies to latest versions
[group('website')]
website-update: _website-deps
    cd website && bun update

# Check for outdated website dependencies
[group('website')]
website-outdated: _website-deps
    cd website && bun outdated

# Audit website dependencies for vulnerabilities (bun has no auto-fix; apply fixes manually with `bun update <pkg>`)
[group('website')]
website-audit: _website-deps
    cd website && bun audit

# Generate OG image
[group('website')]
website-og-image: _website-deps
    cd website && bun run og

# Open website in browser (localhost:4321)
[group('website')]
website-open:
    @echo "Opening http://localhost:4321"
    @open http://localhost:4321 || xdg-open http://localhost:4321 || echo "Please open http://localhost:4321 in your browser"

# Full website maintenance (clean, reinstall, audit, build, check)
[group('website')]
[private]
website-maintain: website-clean website-install website-audit website-build website-check
    @echo "✓ Website maintenance complete"

# Show current version from Cargo.toml
[group('release')]
version:
    @grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'

# Bump version (usage: just bump 1.14.0)
[group('release')]
bump new_version:
    @echo "Bumping version to {{ new_version }}..."
    awk -v new="{{ new_version }}" '!done && /^version = "/ { sub(/^version = ".*"/, "version = \"" new "\""); done=1 } { print }' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml
    cd website && bun pm pkg set version={{ new_version }}
    cargo check
    @echo "✓ Version bumped to {{ new_version }}"
    @echo ""
    @echo "Next steps:"
    @echo "  1. Update CHANGELOG.md"
    @echo "  2. Run: just release-prepare"

# Prepare release (builds, tests, and updates generated schemas)
[group('release')]
release-prepare: release test schemas
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

# Full release workflow (interactive)
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
