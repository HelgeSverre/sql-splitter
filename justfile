# sql-splitter justfile — `just --list` for grouped commands.
# External tools: cargo-llvm-cov, cargo-flamegraph, samply, wasm-pack.

# List all recipes
default:
    @just --list

[group('build')]
build:
    cargo build

[group('build')]
release:
    cargo build --release

# Release build tuned for this CPU
[group('build')]
native:
    RUSTFLAGS="-C target-cpu=native" cargo build --release

# Build with profiling symbols
[group('build')]
build-profiling:
    cargo build --profile profiling

# Run all tests
[group('test')]
test:
    cargo nextest run

# Coverage HTML report, opens in browser
[group('test')]
coverage:
    cargo llvm-cov nextest --html --open

# Coverage summary in the terminal
[group('test')]
coverage-summary:
    cargo llvm-cov nextest --summary-only

# Coverage as lcov.info, for CI
[group('test')]
coverage-lcov:
    cargo llvm-cov nextest --lcov --output-path lcov.info

# Test against real-world dumps
[group('test')]
verify-realworld:
    cargo nextest run --test realworld --run-ignored only

# Smoke-test generate on every SQL fixture
[group('test')]
generate-smoke: build
    ./scripts/smoke-test-generate.sh

# Fuzz model YAML parsing and row generation
[group('test')]
fuzz-model-yaml seconds="60":
    mkdir -p fuzz/corpus/model_yaml
    cargo +nightly fuzz run model_yaml fuzz/corpus/model_yaml fuzz/seeds/model_yaml -- -max_len=65536 -max_total_time={{ seconds }} -timeout=5 -rss_limit_mb=2048 -malloc_limit_mb=512 -dict=fuzz/model_yaml.dict

# Minimise the fuzz corpus, preserving coverage
[group('test')]
fuzz-model-yaml-cmin:
    cargo +nightly fuzz cmin model_yaml fuzz/corpus/model_yaml -- -timeout=5

# Format Rust + Markdown
[group('lint')]
fmt:
    cargo fmt
    bunx prettier --write "**/*.md" --log-level warn

# Type-check without building
[group('lint')]
check:
    cargo check

[group('lint')]
clippy:
    cargo clippy -- -D warnings

# Run benchmarks
[group('bench')]
bench:
    cargo bench

# Save a benchmark baseline
[group('bench')]
bench-baseline name="main":
    cargo bench -- --save-baseline {{ name }}

# Compare against a saved baseline
[group('bench')]
bench-compare baseline="main":
    cargo bench -- --baseline {{ baseline }}

# Benchmark against competitors; generates 100MB if no file given
[group('bench')]
bench-competitors file="":
    ./scripts/benchmark-competitors.sh {{ file }}

# Docker benchmark; size in MB
[group('docker')]
docker-bench size="100":
    ./docker/run-benchmark.sh --generate {{ size }}

# Docker benchmark on a given file
[group('docker')]
docker-bench-file file:
    ./docker/run-benchmark.sh {{ file }}

# Build the benchmark container
[group('docker')]
docker-build:
    docker compose -f docker/docker-compose.benchmark.yml build

# Memory profile, medium dataset
[group('profile')]
profile: release
    ./scripts/profile-memory.sh --size medium --output benchmark-results/profile-medium.txt

# Memory profile, ~125MB
[group('profile')]
profile-large: release
    ./scripts/profile-memory.sh --size large --output benchmark-results/profile-large.txt

# Memory profile, ~1GB
[group('profile')]
profile-mega: release
    ./scripts/profile-memory.sh --size mega --output benchmark-results/profile-mega.txt

# Memory profile, ~10GB, MySQL only
[group('profile')]
profile-giga: release
    ./scripts/profile-memory.sh --size giga --output benchmark-results/profile-giga.txt

# Flamegraph for split
[group('profile')]
flamegraph file: build-profiling
    @mkdir -p benchmark-results
    cargo flamegraph --profile profiling --bin sql-splitter -o benchmark-results/flamegraph-split.svg -- split {{ file }}

# Profile split with samply
[group('profile')]
samply file: build-profiling
    samply record ./target/profiling/sql-splitter split {{ file }}

# Remove build artifacts
clean:
    cargo clean

# Install binary, completions, and man pages
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

# Install completions for all shells
[group('install')]
install-completions-all:
    @./scripts/install-completions.sh sql-splitter all

# Install man pages
[group('install')]
install-man: man
    @./scripts/install-man.sh

# Generate man pages into man/
[group('docs')]
man:
    cargo run --release --features man-pages -- man -o man/
    @echo ""
    @echo "Man pages generated in man/ directory"

# Regenerate JSON schemas from Rust types, validate, and sync to website/. Idempotent
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

# Install deps and start the dev server
[group('website')]
website: website-install
    cd website && bun run dev

# Install deps only if missing
[group('website')]
[private]
_website-deps:
    cd website && ( [ -d node_modules ] && [ -f bun.lock ] || bun install )

# Rebuild the playground WASM into website/public/wasm/. Commit the artifact:
# Vercel serves it and never builds Rust. Needs wasm-pack
[group('website')]
wasm: _website-deps
    rustup target add wasm32-unknown-unknown
    RUSTFLAGS='--cfg getrandom_backend="wasm_js" -C target-feature=+simd128' \
      CARGO_PROFILE_RELEASE_OPT_LEVEL=3 \
      wasm-pack build crates/sql-splitter-wasm --release --target web --no-pack \
      --out-dir {{ justfile_directory() }}/website/public/wasm
    rm -f website/public/wasm/.gitignore
    command -v wasm-opt >/dev/null && wasm-opt -O3 website/public/wasm/sql_splitter_wasm_bg.wasm -o website/public/wasm/sql_splitter_wasm_bg.wasm || echo "wasm-opt not installed; skipping extra size pass"
    cd website && bunx prettier --write "public/wasm/*.js" "public/wasm/*.ts" --log-level warn
    ls -lh website/public/wasm/*.wasm

# Regenerate the playground example dumps. --max-rows must stay >= 200
# (categories needs 5 children per tenant) and order_items needs exactly
# 4x orders, hence the orders pin
[group('website')]
playground-examples:
    cargo run --release -- generate -c tests/fixtures/generate/stress/everything.yaml \
      --dialect mysql --max-rows 200 --table-rows orders=50 \
      -o website/public/playground/saas-mysql.sql
    cargo run --release -- generate -c tests/fixtures/generate/stress/everything.yaml \
      --dialect postgres --max-rows 200 --table-rows orders=50 \
      -o website/public/playground/saas-postgres.sql
    cargo run --release -- generate -c tests/fixtures/generate/stress/car_dealership.yaml \
      --dialect mysql --max-rows 150 \
      -o website/public/playground/dealership-mysql.sql
    cargo run --release -- generate -c tests/fixtures/generate/stress/banking_ledger.yaml \
      --dialect mssql --max-rows 150 \
      -o website/public/playground/ledger-mssql.sql
    cargo run --release -- generate -c tests/fixtures/generate/stress/cms_kitchensink.yaml \
      --dialect sqlite --max-rows 150 \
      -o website/public/playground/cms-sqlite.sql
    ls -lh website/public/playground/*.sql

# Production build
[group('website')]
website-build: _website-deps wasm
    cd website && bun run build

# Dev server with hot reload
[group('website')]
website-dev: _website-deps wasm
    cd website && bun run dev

# Serve the production build locally
[group('website')]
website-preview: website-build
    cd website && bun run preview

# Playwright e2e tests; builds first so the preview server serves a fresh dist/
[group('website')]
website-e2e *args: website-build
    cd website && bunx playwright install chromium
    cd website && bun run test {{ args }}

# Astro type-check and diagnostics
[group('website')]
website-check: _website-deps
    cd website && bun run astro check

# Type-check + format-check; deploy gate
[group('website')]
website-lint: _website-deps
    cd website && bun run astro check
    cd website && bunx prettier . --check

# Validate the generated schema files
[group('website')]
website-validate-schemas: _website-deps
    cd website && bun run validate

# Deploy to Vercel; aborts if any gate fails
[group('website')]
website-deploy: schemas wasm website-lint website-validate-schemas website-build
    sql_splitter_version="$(just version)"; cd website && vc --prod --build-env "SQL_SPLITTER_VERSION=$sql_splitter_version"

# Remove build artifacts and caches
[group('website')]
website-clean:
    cd website && rm -rf dist .astro node_modules/.cache

# Also remove node_modules
[group('website')]
website-clean-all:
    cd website && rm -rf dist .astro node_modules

# Clean and rebuild from scratch
[group('website')]
website-rebuild: website-clean
    cd website && bun install && bun run build

# Install dependencies
[group('website')]
website-install:
    cd website && bun install

# Update dependencies
[group('website')]
website-update: _website-deps
    cd website && bun update

# List outdated dependencies
[group('website')]
website-outdated: _website-deps
    cd website && bun outdated

# Audit dependencies; fix manually with `bun update <pkg>`
[group('website')]
website-audit: _website-deps
    cd website && bun audit

# Generate OG image
[group('website')]
website-og-image: _website-deps
    cd website && bun run og

# Open localhost:4321
[group('website')]
website-open:
    @echo "Opening http://localhost:4321"
    @open http://localhost:4321 || xdg-open http://localhost:4321 || echo "Please open http://localhost:4321 in your browser"

# Clean, reinstall, audit, build, check
[group('website')]
[private]
website-maintain: website-clean website-install website-audit website-build website-check
    @echo "✓ Website maintenance complete"

# Show the current version
[group('release')]
version:
    @grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'

# Bump version, e.g. `just bump 1.14.0`
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

# Build, test, and refresh schemas
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

# Commit and tag a release
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
