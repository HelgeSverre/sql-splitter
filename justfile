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

# Build with profiling symbols (for flamegraph/samply)
build-profiling:
    cargo build --profile profiling

# Generate flamegraph for split command
flamegraph file: build-profiling
    @mkdir -p benchmark-results
    cargo flamegraph --profile profiling --bin sql-splitter -o benchmark-results/flamegraph-split.svg -- split {{ file }}

# Profile split command with samply (opens Firefox Profiler)
samply file: build-profiling
    samply record ./target/profiling/sql-splitter split {{ file }}

# Save criterion benchmark baseline
bench-baseline name="main":
    cargo bench -- --save-baseline {{ name }}

# Compare current benchmarks against a saved baseline
bench-compare baseline="main":
    cargo bench -- --baseline {{ baseline }}

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

# Verify against real-world SQL dumps from public sources
verify-realworld:
    cargo test --test realworld -- --ignored

# Generate man pages
man:
    cargo run --example generate-man
    @echo ""
    @echo "Man pages generated in man/ directory"

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

# [website] Build website for production
website-build:
    cd website && npm run build

# [website] Start development server with hot reload
website-dev:
    cd website && npm run dev

# [website] Preview production build locally
website-preview:
    cd website && npm run preview

# [website] Deploy website to Vercel (production)
website-deploy:
    cd website && vc --prod

# [website] Check Astro project (type checking, diagnostics)
website-check:
    cd website && npm run astro check

# [website] Clean website build artifacts and caches
website-clean:
    cd website && rm -rf dist .astro node_modules/.cache

# [website] Deep clean (including node_modules)
website-clean-all:
    cd website && rm -rf dist .astro node_modules

# [website] Clean and rebuild website from scratch
website-rebuild: website-clean
    cd website && npm install && npm run build

# [website] Install/update website dependencies
website-install:
    cd website && npm install

# [website] Update website dependencies to latest versions
website-update:
    cd website && npm update

# [website] Check for outdated website dependencies
website-outdated:
    cd website && npm outdated

# [website] Audit website dependencies for vulnerabilities
website-audit:
    cd website && npm audit

# [website] Fix website dependency vulnerabilities
website-audit-fix:
    cd website && npm audit fix

# [website] Generate OG image
website-og-image:
    cd website && node generate-og-image.js

# [website] Validate internal links (built into starlight-links-validator during build)
website-validate-links: website-build
    @echo "✓ Links validated during build via starlight-links-validator"

# [website] List all available npm scripts
website-scripts:
    cd website && npm run

# [website] Open website in browser (localhost:4321)
website-open:
    @echo "Opening http://localhost:4321"
    @open http://localhost:4321 || xdg-open http://localhost:4321 || echo "Please open http://localhost:4321 in your browser"

# [website] Full website maintenance (audit, clean, install, build, check)
website-maintain: website-audit website-clean website-install website-build website-check
    @echo "✓ Website maintenance complete"

# [website] Quick CI checks (build + validation)
website-ci: website-build
    @echo "✓ Website CI checks passed"

# [website] Update version in website files from Cargo.toml
website-update-version:
    cd website && npx tsx scripts/update-version.ts

# Show current version from Cargo.toml
version:
    @grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'

# Bump version (usage: just bump 1.14.0)
bump new_version:
    @echo "Bumping version to {{ new_version }}..."
    sed -i '' 's/^version = ".*"/version = "{{ new_version }}"/' Cargo.toml
    cargo check
    @echo "✓ Version bumped to {{ new_version }}"
    @echo ""
    @echo "Next steps:"
    @echo "  1. Update CHANGELOG.md"
    @echo "  2. Run: just release-prepare"

# Prepare release (builds, tests, updates website version)
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

# Full release workflow (interactive)
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
