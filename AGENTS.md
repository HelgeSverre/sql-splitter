# AGENTS.md

This file provides guidance when working with code in this repository.

## Project Overview

High-performance CLI tool written in Rust for splitting large SQL dump files into individual table files.

## Available Commands

### Building and Running

```bash
# Build (debug)
cargo build

# Build (release)
cargo build --release

# Build optimized for current CPU (best for benchmarking)
RUSTFLAGS="-C target-cpu=native" cargo build --release
# or
make build-native

# Run tests
cargo test
# or
make test

# Run benchmarks
cargo bench
# or
make bench
```

### Main Commands

```bash
# Split a MySQL/MariaDB dump file (default)
./target/release/sql-splitter split large-dump.sql --output=tables

# Split a PostgreSQL pg_dump file
./target/release/sql-splitter split pg_dump.sql --output=tables --dialect=postgres

# Split a SQLite .dump file
./target/release/sql-splitter split sqlite.sql --output=tables --dialect=sqlite

# Analyze a SQL file to gather statistics
./target/release/sql-splitter analyze database.sql --progress

# Get help
./target/release/sql-splitter --help
./target/release/sql-splitter split --help
```

### Supported Dialects

| Dialect | Flag | Dump Tool | Key Features |
|---------|------|-----------|--------------|
| MySQL/MariaDB | `--dialect=mysql` (default) | mysqldump | Backtick quoting, backslash escapes |
| PostgreSQL | `--dialect=postgres` | pg_dump | Double-quote identifiers, COPY FROM stdin, dollar-quoting |
| SQLite | `--dialect=sqlite` | sqlite3 .dump | Double-quote identifiers |

## Architecture

### High-Level Design

```
BufReader (fill_buf) → Parser (Streaming) → WriterPool (BufWriter) → Table Files
    64KB Buffer          Statement Buffer       256KB Buffers per table
```

### Key Components

#### `src/parser/mod.rs` - Streaming SQL Parser

- Uses `BufReader::fill_buf()` + `consume()` pattern for streaming reads
- String-aware parsing: tracks escaped quotes and multi-line strings
- Manual table name extraction with regex fallback
- Pre-compiled static regexes via `once_cell::Lazy`

Key functions:
- `read_statement()`: Reads complete SQL statement (handles strings, escaping)
- `parse_statement()`: Identifies statement type and extracts table name
- `determine_buffer_size()`: Selects optimal buffer size based on file size

#### `src/writer/mod.rs` - Buffered File Writers

- `TableWriter`: Manages buffered writes to single table file
- `WriterPool`: HashMap of table writers using `ahash` for fast hashing
- 256KB `BufWriter` per table
- Auto-flush every 100 statements

#### `src/splitter/mod.rs` - Orchestration

- Coordinates parsing and writing
- Maintains processing statistics
- Routes statements to appropriate table writers
- Supports dry-run and table filtering

#### `src/analyzer/mod.rs` - Statistical Analysis

- Counts INSERTs, CREATE TABLEs per table
- Calculates total bytes per table
- Optional progress tracking
- Sorts results by INSERT count

## Performance Characteristics

### Key Optimizations

1. **Streaming I/O**: `fill_buf` + `consume` pattern for zero-copy reading
2. **Manual parsing**: Byte-level table name extraction before regex fallback
3. **Fast hashing**: `ahash::AHashMap` instead of default SipHash
4. **Pre-compiled regexes**: Static initialization via `once_cell::Lazy`
5. **Minimal allocations**: Work with `&[u8]` slices in hot path
6. **Buffered writes**: 256KB buffers with periodic flush

### Buffer Sizes

- File < 1GB: 64KB read buffer (optimal for CPU cache)
- File > 1GB: 256KB read buffer
- All tables: 256KB write buffers with 100-statement buffering

## Testing

```bash
# All tests
cargo test

# Specific module
cargo test parser::tests

# With output
cargo test -- --nocapture
```

## Benchmarking

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench -- read_statement
```

## Key Implementation Details

- **Language**: Rust 2021 edition
- **CLI Framework**: clap v4 with derive macros
- **Regex**: `regex` crate with bytes API
- **HashMap**: `ahash::AHashMap` for performance
- **Buffer management**: `std::io::{BufReader, BufWriter}`
- **Statement types**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE, COPY (PostgreSQL)

## Release Process

Follow these steps to create a new release. **Both a git tag AND a GitHub release are required.**

### 1. Pre-release Checks

```bash
# Ensure all tests pass
cargo test

# Ensure it builds in release mode
cargo build --release

# Run lints
cargo clippy

# Verify formatting
cargo fmt --check

# Optional: Run Docker benchmarks to verify performance
./docker/run-benchmark.sh --generate 50
```

### 2. Update Version

1. Update version in `Cargo.toml`:
   ```toml
   version = "X.Y.Z"
   ```

2. Update `CHANGELOG.md`:
   - Add new version section with today's date: `## [X.Y.Z] - YYYY-MM-DD`
   - Move items from `[Unreleased]` to the new version section
   - Document all notable changes under Added/Changed/Fixed/Removed

### 3. Commit, Tag, and Push

```bash
# Stage all release files
git add Cargo.toml Cargo.lock CHANGELOG.md src/ docs/

# Commit with descriptive message
git commit -m "feat: <brief description> (vX.Y.Z)"

# Create annotated tag
git tag -a vX.Y.Z -m "Release vX.Y.Z

<Brief summary of changes>"

# Push commit and tag together
git push origin main --tags
```

### 4. Create GitHub Release (REQUIRED)

**Always create a GitHub release** - this makes the release visible on the releases page and generates release notes.

```bash
# Extract release notes for this version from CHANGELOG.md and create release
gh release create vX.Y.Z \
  --title "vX.Y.Z" \
  --notes "$(sed -n '/## \[X.Y.Z\]/,/## \[/p' CHANGELOG.md | head -n -1)" \
  --latest
```

Or manually at: https://github.com/HelgeSverre/sql-splitter/releases/new
- Select the tag `vX.Y.Z`
- Title: `vX.Y.Z`
- Description: Copy relevant section from CHANGELOG.md

### 5. Publish to crates.io (Optional)

```bash
# Dry run first
cargo publish --dry-run

# Publish (requires crates.io API token)
cargo publish
```

### 6. Post-release Verification

- [ ] GitHub release visible: https://github.com/HelgeSverre/sql-splitter/releases
- [ ] Tag visible: `git tag -l | grep vX.Y.Z`
- [ ] crates.io updated (if published): https://crates.io/crates/sql-splitter
- [ ] Website auto-deployed via Vercel (if applicable)

### Versioning Guidelines

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (X.0.0): Breaking changes to CLI interface or output format
- **MINOR** (0.X.0): New features, new dialects, new commands
- **PATCH** (0.0.X): Bug fixes, performance improvements, documentation

### Quick Release Checklist

```
[ ] cargo test passes
[ ] cargo clippy clean
[ ] Version bumped in Cargo.toml
[ ] CHANGELOG.md updated
[ ] git commit + tag created
[ ] git push origin main --tags
[ ] gh release create vX.Y.Z (REQUIRED!)
[ ] Verify release at github.com/HelgeSverre/sql-splitter/releases
```
