# sql-splitter

[![Crates.io](https://img.shields.io/crates/v/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)
[![Build](https://github.com/helgesverre/sql-splitter/actions/workflows/ci.yml/badge.svg)](https://github.com/helgesverre/sql-splitter/actions)

High-performance CLI tool for splitting large SQL dump files into individual table files.

Supports MySQL, MariaDB, PostgreSQL (including `COPY FROM stdin`), and SQLite. Fastest streaming tool with 1.1-5x speedup over alternatives.

## Installation

### From crates.io

```bash
cargo install sql-splitter
```

### From source

```bash
cargo install --git https://github.com/helgesverre/sql-splitter
```

Or download pre-built binaries from [GitHub Releases](https://github.com/helgesverre/sql-splitter/releases).

## Usage

```bash
# MySQL/MariaDB dump (default)
sql-splitter split dump.sql -o tables/

# PostgreSQL pg_dump
sql-splitter split pg_dump.sql -o tables/ --dialect=postgres

# SQLite dump
sql-splitter split sqlite.sql -o tables/ --dialect=sqlite

# Split specific tables only
sql-splitter split dump.sql --tables users,posts,orders

# Analyze without splitting
sql-splitter analyze dump.sql
```

## Performance

| File Size | Time | Throughput |
|-----------|------|------------|
| 100 MB | 142 ms | 726 MB/s |
| 1 GB | 1.32 s | 783 MB/s |
| 10 GB | 23.1 s | 445 MB/s |

See [BENCHMARKS.md](BENCHMARKS.md) for detailed comparisons.

## Options

```
sql-splitter split [OPTIONS] <FILE>

  -o, --output <DIR>      Output directory [default: output]
  -d, --dialect <DIALECT> mysql, postgres, sqlite [default: mysql]
  -t, --tables <LIST>     Only split these tables (comma-separated)
  -p, --progress          Show progress bar
      --dry-run           Preview without writing files
```

## License

This project is licensed under the MIT License. See [LICENSE.md](LICENSE.md) for details.
