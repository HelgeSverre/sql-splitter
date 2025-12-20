# Benchmarks

Comprehensive benchmarking for sql-splitter.

## Quick Comparison

### sql-splitter Performance

| File Size | Time | Throughput |
|-----------|------|------------|
| 122 MB    | 293 ms | 416 MB/s |
| 2.9 GB    | 6.82 s | 425 MB/s |
| 10 GB     | 27.4 s | 365 MB/s |

### vs afrase/mysqldumpsplit

| File Size | sql-splitter | mysqldumpsplit | Comparison |
|-----------|--------------|----------------|------------|
| 122 MB    | 293 ms       | 205 ms         | 1.43x slower |
| 2.9 GB    | 6.82 s       | 6.54 s         | ~equal     |
| 10 GB     | 55.1 s       | 39.0 s         | 1.41x slower |

*Note: afrase/mysqldumpsplit uses a different parsing approach optimized for raw speed.*

### Competitive Landscape

| Tool | Language | Stars | Speed | Notes |
|------|----------|-------|-------|-------|
| [ripienaar/mysql-dump-split](https://github.com/ripienaar/mysql-dump-split) | Ruby | 77 | Slow | Most popular, simple implementation |
| [afrase/mysqldumpsplit](https://github.com/afrase/mysqldumpsplit) | Go | ~40 | **Fastest** | Optimized for raw speed |
| **sql-splitter** | Rust | - | **Fast** | Feature-rich: dry-run, table filter, analyze |
| [ooooak/sql-split](https://github.com/ooooak/sql-split) | Rust | ~5 | Fast | Minimal Rust implementation |
| [Scoopit/mysqldumpsplit](https://github.com/Scoopit/mysqldumpsplit) | Rust | ~10 | Fast | Another Rust alternative |
| [rafael-luigi-bekkema/mysql-dump-splitter](https://github.com/rafael-luigi-bekkema/mysql-dump-splitter) | Go | ~5 | Fast | Go alternative |
| [vekexasia/mysqldumpsplit](https://github.com/vekexasia/mysqldumpsplit) | Node.js | ~30 | ~133 MB/s | npm package |

### Key Findings

- sql-splitter achieves **300-400+ MB/s** throughput on large files
- afrase/mysqldumpsplit is ~1.4x faster due to specialized parsing
- sql-splitter offers more features (--dry-run, --tables filter, --progress, analyze command)
- Both produce **identical output**

## Running Benchmarks

### Hyperfine (Recommended for CLI comparison)

```bash
# Benchmark with cleanup between runs
hyperfine --warmup 1 --prepare 'rm -rf /tmp/bench-output' \
  './target/release/sql-splitter split dump.sql -o /tmp/bench-output'

# Export results
hyperfine --export-markdown bench-results.md \
  './target/release/sql-splitter split dump.sql -o /tmp/output'
```

### Make Targets

```bash
# Comprehensive benchmark against all competitor tools
make bench-all

# Quick benchmark with smaller test files
make bench-quick

# Benchmark only the Rust implementation
make bench-rust-only

# Benchmark with specific SQL file
make bench-all FILE=/path/to/dump.sql

# Docker-based reproducible benchmarks
make bench-docker-build   # Build container once
make bench-docker         # Run benchmarks in container
```

### Criterion Microbenchmarks

```bash
# All benchmarks
cargo bench

# Parser benchmarks (throughput, buffer sizes)
make bench-parser

# Writer benchmarks (pool, flush patterns)
make bench-writer

# End-to-end benchmarks
make bench-e2e

# Generate HTML report
make bench-report
# Opens: target/criterion/report/index.html
```

## Criterion Results

### Parser Throughput (In-Memory)

| Statements | Throughput |
|------------|------------|
| 1,000      | 600 MB/s   |
| 10,000     | 608 MB/s   |
| 50,000     | 597 MB/s   |

### Buffer Size Comparison

| Buffer Size | Throughput |
|-------------|------------|
| 16 KB       | 598 MB/s   |
| 32 KB       | 602 MB/s   |
| 64 KB       | 605 MB/s   |
| 128 KB      | 605 MB/s   |
| 256 KB      | 600 MB/s   |

*64-128KB buffers are optimal for CPU cache utilization.*

### End-to-End Split Performance

| Configuration | Throughput |
|---------------|------------|
| 10 tables × 1000 rows | 282 MB/s |
| 50 tables × 500 rows  | 244 MB/s |
| 100 tables × 200 rows | 120 MB/s |

*More tables = more file handles = slightly lower throughput.*

### Statement Type Parsing

| Statement Type | Time per Parse |
|----------------|----------------|
| CREATE TABLE   | ~45 ns         |
| INSERT INTO    | ~40 ns         |
| ALTER TABLE    | ~50 ns         |
| DROP TABLE     | ~45 ns         |

## Benchmark Suites

| Suite | File | Description |
|-------|------|-------------|
| `parser_bench` | `benches/parser_bench.rs` | Parser throughput, buffer sizes, statement types, string handling |
| `writer_bench` | `benches/writer_bench.rs` | Writer pool, table writer, flush patterns |
| `e2e_bench` | `benches/e2e_bench.rs` | Full split/analyze operations |

## Profiling

### CPU Profiling with Flamegraph

```bash
# Install cargo-flamegraph
cargo install flamegraph

# Generate flamegraph
make profile
# or
cargo flamegraph --bin sql-splitter -- split dump.sql -o /tmp/output
```

### Memory Profiling

```bash
# On macOS with Instruments
xcrun xctrace record --template 'Allocations' --launch -- \
  ./target/release/sql-splitter split dump.sql -o /tmp/output

# On Linux with heaptrack
heaptrack ./target/release/sql-splitter split dump.sql -o /tmp/output
```

## Reproducing Results

### Prerequisites

```bash
# Build optimized binary
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Test Files

Generate test data of various sizes:

```bash
# Generate ~50MB test file
python3 << 'PYTHON'
import random
tables = ['users', 'posts', 'comments', 'orders', 'products']
with open('/tmp/test_50mb.sql', 'w') as f:
    for table in tables:
        f.write(f"CREATE TABLE {table} (id INT, data TEXT);\n")
    for i in range(100000):
        table = random.choice(tables)
        data = "Lorem ipsum " * 10
        f.write(f"INSERT INTO {table} VALUES ({i}, '{data}');\n")
PYTHON
```

### Run Benchmark

```bash
# Using the benchmark script
./scripts/benchmark.sh /tmp/test_50mb.sql

# Using hyperfine
hyperfine --warmup 2 --runs 5 \
  --prepare 'rm -rf /tmp/output' \
  './target/release/sql-splitter split /tmp/test_50mb.sql -o /tmp/output'
```

## Performance Optimizations

1. **No GC pauses**: Rust's ownership model eliminates garbage collection overhead
2. **Zero-copy parsing**: `fill_buf` + `consume` pattern avoids unnecessary copies
3. **Faster hashing**: `ahash` is 2-3x faster than default SipHash
4. **Better inlining**: LTO and monomorphization produce tighter code
5. **CPU-native codegen**: `target-cpu=native` enables platform-specific optimizations

## Hardware

All benchmarks run on:
- **CPU**: Apple M2 Max
- **RAM**: 32 GB
- **Storage**: NVMe SSD
- **OS**: macOS Sonoma
