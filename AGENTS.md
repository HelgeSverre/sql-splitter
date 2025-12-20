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

Follow these steps to create a new release:

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
```

### 2. Update Version

1. Update version in `Cargo.toml`:
   ```toml
   version = "X.Y.Z"
   ```

2. Update `CHANGELOG.md`:
   - Move items from `[Unreleased]` to new version section
   - Add release date
   - Document all notable changes

### 3. Commit and Tag

```bash
# Commit version bump
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "chore: bump version to X.Y.Z"

# Create annotated tag
git tag -a vX.Y.Z -m "Release vX.Y.Z"

# Push with tags
git push origin main --tags
```

### 4. Publish to crates.io

```bash
# Dry run first
cargo publish --dry-run

# Publish
cargo publish
```

### 5. Create GitHub Release

```bash
# Create release from tag (uses CHANGELOG content)
gh release create vX.Y.Z \
  --title "vX.Y.Z" \
  --notes-file CHANGELOG.md \
  --latest
```

Or create via GitHub UI at https://github.com/HelgeSverre/sql-splitter/releases/new

### 6. Post-release

- Verify crates.io listing: https://crates.io/crates/sql-splitter
- Verify GitHub release: https://github.com/HelgeSverre/sql-splitter/releases
- Update website if needed (auto-deploys via Vercel)

### Versioning Guidelines

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (X.0.0): Breaking changes to CLI interface or output format
- **MINOR** (0.X.0): New features, new dialects, new commands
- **PATCH** (0.0.X): Bug fixes, performance improvements, documentation
