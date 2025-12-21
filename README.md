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

# Merge split files back into single dump
sql-splitter merge tables/ -o restored.sql

# Merge specific tables only
sql-splitter merge tables/ -o partial.sql --tables users,orders

# Merge with transaction wrapper
sql-splitter merge tables/ -o restored.sql --transaction

# Analyze without splitting
sql-splitter analyze dump.sql

# Convert between SQL dialects
sql-splitter convert mysql_dump.sql -o postgres_dump.sql --to postgres
sql-splitter convert pg_dump.sql -o mysql_dump.sql --to mysql
sql-splitter convert dump.sql -o sqlite_dump.sql --to sqlite

# Convert with explicit source dialect
sql-splitter convert dump.sql --from postgres --to mysql -o output.sql

# Validate SQL dump integrity
sql-splitter validate dump.sql

# Validate with strict mode (warnings = errors)
sql-splitter validate dump.sql --strict

# Validate with JSON output for CI
sql-splitter validate dump.sql --json

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

## Why sql-splitter?

sql-splitter is a **dump-first, CLI-first** tool designed for automation and CI/CD pipelines.

**What it's optimized for**

| Strength | Description |
|----------|-------------|
| **One tool for the workflow** | Split → sample → shard → convert → merge in a single binary |
| **Works on dump files** | No running database or JDBC connection needed (unlike mydumper, Jailer, Condenser) |
| **Streaming architecture** | 10GB+ dumps with constant memory, 600+ MB/s throughput |
| **Multi-dialect + conversion** | MySQL, PostgreSQL, SQLite including `COPY FROM stdin` → INSERT |
| **FK-aware operations** | Sampling and tenant sharding preserve referential integrity |

**When another tool might be better**

- **[mydumper](https://github.com/mydumper/mydumper)** — Parallel snapshots from live MySQL/MariaDB databases
- **[Jailer](https://github.com/Wisser/Jailer)** — Rich GUI-based FK subsetting with JDBC across 12+ databases  
- **[sqlglot](https://github.com/tobymao/sqlglot)** — Query-level transpilation and AST manipulation (31 dialects)
- **[DuckDB](https://github.com/duckdb/duckdb)** — Complex analytical queries over SQL/CSV/JSON/Parquet

See [docs/COMPETITIVE_ANALYSIS.md](docs/COMPETITIVE_ANALYSIS.md) for detailed comparisons.

## Options

### Split Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output directory | `output` |
| `-d, --dialect` | SQL dialect: `mysql`, `postgres`, `sqlite` | auto-detect |
| `-t, --tables` | Only split these tables (comma-separated) | — |
| `-p, --progress` | Show progress bar | — |
| `--dry-run` | Preview without writing files | — |
| `--schema-only` | Only DDL statements (CREATE, ALTER, DROP) | — |
| `--data-only` | Only DML statements (INSERT, COPY) | — |

### Merge Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output SQL file | stdout |
| `-d, --dialect` | SQL dialect for headers/footers | `mysql` |
| `-t, --tables` | Only merge these tables (comma-separated) | all |
| `-e, --exclude` | Exclude these tables (comma-separated) | — |
| `--transaction` | Wrap in BEGIN/COMMIT transaction | — |
| `--no-header` | Skip header comments | — |
| `-p, --progress` | Show progress bar | — |
| `--dry-run` | Preview without writing files | — |

### Convert Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output SQL file | stdout |
| `--from` | Source dialect: `mysql`, `postgres`, `sqlite` | auto-detect |
| `--to` | Target dialect: `mysql`, `postgres`, `sqlite` | required |
| `--strict` | Fail on any unsupported feature | — |
| `-p, --progress` | Show progress bar | — |
| `--dry-run` | Preview without writing files | — |

**Supported conversions:**
- MySQL ↔ PostgreSQL (including COPY → INSERT)
- MySQL ↔ SQLite
- PostgreSQL ↔ SQLite

**Features:**
- 30+ data type mappings
- AUTO_INCREMENT ↔ SERIAL ↔ INTEGER PRIMARY KEY
- PostgreSQL COPY → INSERT with NULL and escape handling
- Session command stripping
- Warnings for unsupported features (ENUM, arrays, triggers)

### Validate Options

| Flag | Description | Default |
|------|-------------|---------|
| `-d, --dialect` | SQL dialect: `mysql`, `postgres`, `sqlite` | auto-detect |
| `--strict` | Treat warnings as errors (exit 1) | — |
| `--json` | Output results as JSON | — |
| `--max-rows-per-table` | Max rows per table for PK/FK checks | 1,000,000 |
| `--no-fk-checks` | Skip PK/FK data integrity checks | — |
| `-p, --progress` | Show progress bar | — |

**Validation checks:**
- SQL syntax validation (parser errors)
- DDL/DML consistency (INSERTs reference existing tables)
- Encoding validation (UTF-8)
- Duplicate primary key detection (all dialects)
- FK referential integrity (all dialects)

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
