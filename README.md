# sql-splitter

[![Crates.io](https://img.shields.io/crates/v/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Downloads](https://img.shields.io/crates/d/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Build](https://github.com/helgesverre/sql-splitter/actions/workflows/test.yml/badge.svg)](https://github.com/helgesverre/sql-splitter/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)

Split large SQL dump files into individual table files. Fast, memory-efficient, multi-dialect.

- **600+ MB/s** throughput on modern hardware
- **MySQL, PostgreSQL, SQLite** support (including `COPY FROM stdin`)
- **Compressed files** — gzip, bzip2, xz, zstd auto-detected
- **Streaming architecture** — handles files larger than RAM
- **5x faster** than shell-based alternatives

## Installation

### From crates.io

```bash
cargo install sql-splitter
```

### From source

```bash
git clone https://github.com/helgesverre/sql-splitter
cd sql-splitter
make install  # Installs binary + shell completions
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

# Compressed files (auto-detected)
sql-splitter split backup.sql.gz -o tables/
sql-splitter split backup.sql.zst -o tables/

# Split specific tables only
sql-splitter split dump.sql --tables users,posts,orders

# Schema only (CREATE TABLE, indexes, etc.)
sql-splitter split dump.sql -o schema/ --schema-only

# Data only (INSERT/COPY statements)
sql-splitter split dump.sql -o data/ --data-only

# Analyze without splitting
sql-splitter analyze dump.sql

# Generate shell completions (auto-installed with make install)
sql-splitter completions bash >> ~/.bashrc
sql-splitter completions zsh >> ~/.zshrc
sql-splitter completions fish >> ~/.config/fish/completions/sql-splitter.fish
```

### Shell Completions

Shell completions are automatically installed when using `make install`. For manual installation:

```bash
# Install for current shell only
make install-completions

# Install for all shells (bash, zsh, fish)
make install-completions-all
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output directory | `output` |
| `-d, --dialect` | SQL dialect: `mysql`, `postgres`, `sqlite` | auto-detect |
| `-t, --tables` | Only split these tables (comma-separated) | — |
| `-p, --progress` | Show progress bar | — |
| `--dry-run` | Preview without writing files | — |
| `--schema-only` | Only DDL statements (CREATE, ALTER, DROP) | — |
| `--data-only` | Only DML statements (INSERT, COPY) | — |

## Performance

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for detailed comparisons.

## Testing

```bash
# Unit tests
cargo test

# Verify against real-world SQL dumps (MySQL, PostgreSQL, WordPress, etc.)
make verify-realworld
```

## License

MIT — see [LICENSE.md](LICENSE.md)
