# sql-splitter

A high-performance CLI tool for splitting large SQL dump files into individual table files. Written in Rust for maximum throughput.

## Features

- **⚡ Fast**: 300+ MB/s throughput on large files
- **💾 Memory Efficient**: Streams data, constant ~100MB usage regardless of file size
- **📊 Analyze Mode**: Get table statistics without splitting
- **🎯 Filter Tables**: Split only specific tables
- **🔍 Dry Run**: Preview what would be created
- **🦀 Cross-Platform**: Works on macOS, Linux, and Windows

## Installation

### Using Cargo

```bash
cargo install sql-splitter
```

### Download Pre-built Binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/helgesverre/sql-splitter/releases).

### Build from Source

```bash
git clone https://github.com/helgesverre/sql-splitter.git
cd sql-splitter
cargo build --release

# The binary is at ./target/release/sql-splitter
```

## Usage

### Split a SQL Dump

```bash
# Basic usage
sql-splitter split dump.sql

# Specify output directory
sql-splitter split dump.sql --output tables/

# Show progress
sql-splitter split dump.sql -o tables/ --progress

# Split only specific tables
sql-splitter split dump.sql --tables users,posts,orders

# Preview without writing files
sql-splitter split dump.sql --dry-run
```

### Analyze a SQL Dump

```bash
# Show table statistics
sql-splitter analyze dump.sql

# With progress
sql-splitter analyze dump.sql --progress
```

**Output:**

```
Analyzing SQL file: dump.sql (1250.50 MB)

✓ Analysis completed in 2.300s

Found 25 tables:

Table Name                                  INSERTs   Total Stmts   Size (MB)
────────────────────────────────────────────────────────────────────────────
users                                         50000         50001      500.25
posts                                         30000         30001      300.15
comments                                      25000         25001      250.10
────────────────────────────────────────────────────────────────────────────
TOTAL                                        105000             -     1050.50
```

## Performance

| File Size | Time | Throughput |
|-----------|------|------------|
| 122 MB | 293 ms | 416 MB/s |
| 2.9 GB | 6.82 s | 425 MB/s |
| 10 GB | 27.4 s | 365 MB/s |

See [BENCHMARKS.md](BENCHMARKS.md) for detailed benchmarks and comparisons.

## Command Reference

### Split

```
sql-splitter split [OPTIONS] <FILE>

Arguments:
  <FILE>  Input SQL file

Options:
  -o, --output <DIR>     Output directory [default: output]
  -t, --tables <LIST>    Only split these tables (comma-separated)
  -p, --progress         Show progress
  -v, --verbose          Verbose output
      --dry-run          Preview without writing
  -h, --help             Print help
```

### Analyze

```
sql-splitter analyze [OPTIONS] <FILE>

Arguments:
  <FILE>  Input SQL file

Options:
  -p, --progress  Show progress
  -h, --help      Print help
```

## Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Build optimized for your CPU
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## License

MIT
