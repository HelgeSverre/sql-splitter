# sql-splitter

[![Crates.io](https://img.shields.io/crates/v/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Downloads](https://img.shields.io/crates/d/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Build](https://github.com/helgesverre/sql-splitter/actions/workflows/test.yml/badge.svg)](https://github.com/helgesverre/sql-splitter/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)

Split large SQL dump files into individual table files. Fast, memory-efficient, multi-dialect.

- **600+ MB/s** throughput on modern hardware
- **MySQL, PostgreSQL, SQLite** support (including `COPY FROM stdin`)
- **Streaming architecture** — handles files larger than RAM
- **5x faster** than shell-based alternatives

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

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output directory | `output` |
| `-d, --dialect` | SQL dialect: `mysql`, `postgres`, `sqlite` | `mysql` |
| `-t, --tables` | Only split these tables (comma-separated) | — |
| `-p, --progress` | Show progress bar | — |
| `--dry-run` | Preview without writing files | — |

## Performance

See [BENCHMARKS.md](BENCHMARKS.md) for detailed comparisons.

## Testing

```bash
# Unit tests
cargo test

# Verify against real-world SQL dumps (MySQL, PostgreSQL, WordPress, etc.)
make verify-realworld
```

## License

MIT — see [LICENSE.md](LICENSE.md)
