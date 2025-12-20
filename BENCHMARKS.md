# Benchmarks

Comprehensive benchmarking for sql-splitter.

## Quick Comparison

### sql-splitter Performance

| File Size | Time | Throughput | Tables |
|-----------|------|------------|--------|
| 10 MB     | 16 ms | 643 MB/s | 10 |
| 100 MB    | 142 ms | 726 MB/s | 10 |
| 1 GB      | 1.32 s | 783 MB/s | 10 |
| 5 GB      | 8.5 s | 611 MB/s | 10 |
| 10 GB     | 23.1 s | 445 MB/s | 281 |

*Tested on Apple M2 Max, compiled with `-C target-cpu=native`*

### vs Competitor Tools

**200MB mysqldump file (Docker Linux arm64):**

| Command | Mean | Relative |
|:--------|-----:|:---------|
| mysqldumpsplit (Go)* | 278 ms | 0.73x (faster) |
| **sql-splitter** (Rust) | 382 ms | 1.00 |
| mysql_splitdump (csplit) | 415 ms | 1.09x slower |
| mysqldumpsplit (Node.js) | 582 ms | 1.52x slower |
| mysql-dump-split (Ruby) | 1,759 ms | **4.61x slower** |
| mysqldumpsplitter (Bash/awk) | 1,817 ms | **4.76x slower** |

**100MB mysqldump file (Docker Linux arm64):**

| Command | Mean | Relative |
|:--------|-----:|:---------|
| mysqldumpsplit (Go)* | 141 ms | 0.76x (faster) |
| **sql-splitter** (Rust) | 186 ms | 1.00 |
| mysql_splitdump (csplit) | 211 ms | 1.13x slower |
| mysqldumpsplit (Node.js) | 328 ms | 1.76x slower |
| mysql-dump-split (Ruby) | 887 ms | **4.77x slower** |
| mysqldumpsplitter (Bash/awk) | 892 ms | **4.80x slower** |

*\*Go tool has a deadlock bug in the original; we used a patched version for benchmarks.*

*Benchmarks run in Docker on Apple M2 Max (linux/arm64)*

### Competitive Landscape

| Tool | Language | Stars | Speed | Notes |
|------|----------|-------|-------|-------|
| [afrase/mysqldumpsplit](https://github.com/afrase/mysqldumpsplit)* | Go | ~40 | **Fastest** | Buffers in memory, high RAM usage |
| **sql-splitter** | Rust | - | Fast | Multi-dialect, streaming I/O, low RAM |
| [jasny/mysql_splitdump.sh](https://gist.github.com/jasny/1608062) | Bash/csplit | 93 | 1.1x slower | mysqldump only, needs GNU coreutils |
| [vekexasia/mysqldumpsplit](https://github.com/vekexasia/mysqldumpsplit) | Node.js | 55 | 1.5-1.8x slower | Requires Node.js 18 (bugs on v22) |
| [kedarvj/mysqldumpsplitter](https://github.com/kedarvj/mysqldumpsplitter) | Bash/awk | 540+ | 5x slower | mysqldump format only |
| [ripienaar/mysql-dump-split](https://github.com/ripienaar/mysql-dump-split) | Ruby | 77 | 5x slower | Archived project |

*\*Original Go tool has a deadlock bug.*

### Multi-Database Dialect Support

| Dialect | Flag | Dump Tool | COPY Support |
|---------|------|-----------|--------------|
| MySQL/MariaDB | `--dialect=mysql` (default) | mysqldump, mariadb-dump | N/A |
| PostgreSQL | `--dialect=postgres` | pg_dump | ✅ COPY FROM stdin |
| SQLite | `--dialect=sqlite` | sqlite3 .dump | N/A |

### Format Compatibility

This is the **key differentiator**. Most competitors only work with standard `mysqldump` output:

| Tool | MySQL | MariaDB | PostgreSQL | SQLite | TablePlus/DBeaver |
|------|-------|---------|------------|--------|-------------------|
| **sql-splitter** | ✅ | ✅ | ✅ | ✅ | ✅ |
| mysqldumpsplitter (Bash) | ✅ | ✅ | ❌ | ❌ | ❌ |
| Other tools | ❌ | ❌ | ❌ | ❌ | ❌ |

**Why?** Competitor tools look for specific comment markers:
```sql
-- Table structure for table `users`
```

These markers are only present in standard `mysqldump` output. **sql-splitter** parses actual SQL statements (`CREATE TABLE`, `INSERT INTO`, `COPY`), making it compatible with any SQL file format.

### Key Findings

- sql-splitter is the **fastest streaming tool** with low memory usage
- Our patched Go tool is faster but buffers entire file in memory
- sql-splitter is **1.1x faster** than csplit, **1.5-1.8x faster** than Node.js, **5x faster** than awk/Ruby
- sql-splitter achieves **600-800 MB/s** throughput on synthetic files  
- sql-splitter achieves **~450 MB/s** on real 10GB production dumps
- sql-splitter is the **only multi-dialect tool** (MySQL, PostgreSQL, SQLite)
- sql-splitter works with ANY SQL format (TablePlus, DBeaver, mysqldump, pg_dump, etc.)

## Real-World Benchmarks

### TablePlus Format (non-mysqldump)

| File | Size | Tables | Time | Throughput |
|------|------|--------|------|------------|
| boatflow_latest_2.sql | 122 MB | 53 | 1.03 s | 118 MB/s |
| wip.sql | 633 MB | 16 | 5.45 s | 116 MB/s |

*Competitor tools produce **0 tables** on these files because they're not standard mysqldump format.*

### Running Benchmarks

```bash
# Full benchmark suite (generates 10MB, 100MB, 1GB, 5GB synthetic files)
./scripts/run-benchmarks.sh

# Generate a custom-sized test file
python3 ./scripts/generate-test-dump.py 500 -o /tmp/test_500mb.sql

# Benchmark on your own SQL dumps
./scripts/benchmark-real-dumps.sh

# Set custom real dump file
REAL_DUMP=/path/to/dump.sql ./scripts/run-benchmarks.sh
```

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
