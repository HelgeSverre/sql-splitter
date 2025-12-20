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
# Split a SQL file into individual table files
./target/release/sql-splitter split large-dump.sql --output=tables

# Analyze a SQL file to gather statistics
./target/release/sql-splitter analyze database.sql --progress

# Get help
./target/release/sql-splitter --help
./target/release/sql-splitter split --help
```

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
- **Statement types**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE
