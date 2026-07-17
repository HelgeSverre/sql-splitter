# sql-splitter

[![Crates.io](https://img.shields.io/crates/v/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Downloads](https://img.shields.io/crates/d/sql-splitter.svg)](https://crates.io/crates/sql-splitter)
[![Build](https://github.com/helgesverre/sql-splitter/actions/workflows/test.yml/badge.svg)](https://github.com/helgesverre/sql-splitter/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)
[![Amp](https://img.shields.io/badge/Amp%20Code-191C19.svg?logo=data:image/svg%2bxml;base64,PHN2ZyB3aWR0aD0iMjEiIGhlaWdodD0iMjEiIHZpZXdCb3g9IjAgMCAyMSAyMSIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTMuNzY4NzkgMTguMzAxNUw4LjQ5ODM5IDEzLjUwNUwxMC4yMTk2IDIwLjAzOTlMMTIuNzIgMTkuMzU2MUwxMC4yMjg4IDkuODY3NDlMMC44OTA4NzYgNy4zMzg0NEwwLjIyNTk0IDkuODkzMzFMNi42NTEzNCAxMS42Mzg4TDEuOTQxMzggMTYuNDI4MkwzLjc2ODc5IDE4LjMwMTVaIiBmaWxsPSIjRjM0RTNGIi8+CjxwYXRoIGQ9Ik0xNy40MDc0IDEyLjc0MTRMMTkuOTA3OCAxMi4wNTc1TDE3LjQxNjcgMi41Njg5N0w4LjA3ODczIDAuMDM5OTI0Nkw3LjQxMzggMi41OTQ4TDE1LjI5OTIgNC43MzY4NUwxNy40MDc0IDEyLjc0MTRaIiBmaWxsPSIjRjM0RTNGIi8+CjxwYXRoIGQ9Ik0xMy44MTg0IDE2LjM4ODNMMTYuMzE4OCAxNS43MDQ0TDEzLjgyNzYgNi4yMTU4OEw0LjQ4OTcxIDMuNjg2ODNMMy44MjQ3NyA2LjI0MTcxTDExLjcxMDEgOC4zODM3NkwxMy44MTg0IDE2LjM4ODNaIiBmaWxsPSIjRjM0RTNGIi8+Cjwvc3ZnPg==&style=flat)](https://ampcode.com/@helgesverre)

Split large SQL dump files into individual table files. Fast, memory-efficient, multi-dialect.

- **~1 GB/s** end-to-end split throughput on large dumps (0.8–1.4 GB/s across dialects on Apple M2 Max; hardware-dependent)
- **MySQL, PostgreSQL, SQLite, MSSQL** support (including `COPY FROM stdin`, `GO` batches)
- **Compressed files** — gzip, bzip2, xz, zstd, zip (single `.sql` member) auto-detected
- **Streaming architecture** — handles files larger than RAM
- **10x faster** than shell-based alternatives

## Installation

### From crates.io

```bash
cargo install sql-splitter
```

### Cargo features (library consumers)

`sql-splitter` enables all features by default. To reduce dependency footprint:

```toml
sql-splitter = { version = "1", default-features = false }
```

Optional features:

- `compression` (gzip/bzip2/xz/zstd support, for compressed input and per-file output)
- `archive` (single-file `tar.*`/`zip` output, plus `.zip` archive input; implies `compression`)
- `duckdb-query` (enables the `query` command and DuckDB integration)

### From source

```bash
git clone https://github.com/helgesverre/sql-splitter
cd sql-splitter
make install  # Installs binary + shell completions + man pages
```

Or download pre-built binaries from [GitHub Releases](https://github.com/helgesverre/sql-splitter/releases).

### Man pages (optional)

After installation, view documentation with `man sql-splitter` or `man sql-splitter-diff`.

For `cargo install` users, install man pages manually:

```bash
git clone https://github.com/helgesverre/sql-splitter
cd sql-splitter
make install-man
```

## Usage

```bash
# MySQL/MariaDB dump (default)
sql-splitter split dump.sql -o tables/

# PostgreSQL pg_dump
sql-splitter split pg_dump.sql -o tables/ --dialect=postgres

# SQLite dump
sql-splitter split sqlite.sql -o tables/ --dialect=sqlite

# MSSQL/T-SQL dump (SSMS "Generate Scripts", sqlcmd)
sql-splitter split mssql_dump.sql -o tables/ --dialect=mssql

# Compressed input (auto-detected)
sql-splitter split backup.sql.gz -o tables/
sql-splitter split backup.sql.zst -o tables/

# Zipped input — a .zip with exactly one .sql member (deflated or stored)
sql-splitter split backup.zip -o tables/

# Compressed output — one compressed file per table (users.sql.zst, ...)
sql-splitter split dump.sql -o tables/ --compress zstd   # or gzip, bzip2, xz

# Single-archive output — all tables in one file (format from the extension)
sql-splitter split dump.sql -o dump.tar.gz               # .tgz/.tar.zst/.tar.bz2/.tar.xz/.tar
sql-splitter split dump.sql -o dump.zip

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
sql-splitter convert mssql_dump.sql -o mysql_dump.sql --to mysql

# Convert with explicit source dialect
sql-splitter convert dump.sql --from postgres --to mysql -o output.sql
sql-splitter convert dump.sql --from mssql --to postgres -o output.sql

# Validate SQL dump integrity
sql-splitter validate dump.sql

# Validate with strict mode (warnings = errors)
sql-splitter validate dump.sql --strict

# Validate with JSON output for CI
sql-splitter validate dump.sql --json

# Batch operations with glob patterns
sql-splitter validate "dumps/*.sql" --fail-fast
sql-splitter analyze "**/*.sql"
sql-splitter split "*.sql" -o output/
sql-splitter convert "*.sql" --to postgres -o converted/

# Compare two SQL dumps for changes
sql-splitter diff old.sql new.sql

# Diff with schema-only or data-only
sql-splitter diff old.sql new.sql --schema-only
sql-splitter diff old.sql new.sql --data-only

# Diff with JSON or SQL migration output
sql-splitter diff old.sql new.sql --format json -o diff.json
sql-splitter diff old.sql new.sql --format sql -o migration.sql

# Diff with verbose PK samples and ignore timestamp columns
sql-splitter diff old.sql new.sql --verbose --ignore-columns "*.updated_at,*.created_at"

# Override primary key for tables without PK
sql-splitter diff old.sql new.sql --primary-key logs:timestamp+message

# Redact sensitive data using inline patterns
sql-splitter redact dump.sql -o safe.sql --null "*.ssn" --hash "*.email" --fake "*.name"

# Redact using YAML config file
sql-splitter redact dump.sql -o safe.sql --config redact.yaml

# Generate redaction config by analyzing input file
sql-splitter redact dump.sql --generate-config -o redact.yaml

# Reproducible redaction with seed
sql-splitter redact dump.sql -o safe.sql --null "*.password" --seed 42

# Generate ERD (Entity-Relationship Diagram)
sql-splitter graph dump.sql -o schema.html        # Interactive HTML (default)
sql-splitter graph dump.sql -o schema.dot         # Graphviz DOT format
sql-splitter graph dump.sql -o schema.mmd         # Mermaid erDiagram
sql-splitter graph dump.sql -o schema.json        # JSON with full schema details

# Graph with filtering
sql-splitter graph dump.sql --tables "user*,order*" -o filtered.html
sql-splitter graph dump.sql --exclude "log*,audit*" -o clean.html
sql-splitter graph dump.sql --cycles-only         # Only tables in circular dependencies

# Focus on specific table and its relationships
sql-splitter graph dump.sql --table orders --transitive  # Show all dependencies
sql-splitter graph dump.sql --table users --reverse      # Show all dependents

# Reorder SQL dump in topological FK order
sql-splitter order dump.sql -o ordered.sql        # Safe import order
sql-splitter order dump.sql --check               # Check for cycles
sql-splitter order dump.sql --reverse             # Reverse (for DROP operations)

# Query SQL dumps with DuckDB analytics engine
sql-splitter query dump.sql "SELECT COUNT(*) FROM users"
sql-splitter query dump.sql "SELECT * FROM orders WHERE total > 100" -f json
sql-splitter query dump.sql "SELECT * FROM users LIMIT 10" -o results.csv -f csv
sql-splitter query dump.sql --interactive         # Start REPL session
sql-splitter query huge.sql "SELECT ..." --disk   # Use disk mode for large files

# Query with caching for repeated queries
sql-splitter query dump.sql "SELECT ..." --cache  # Cache imported database
sql-splitter query --list-cache                   # Show cached databases
sql-splitter query --clear-cache                  # Clear all cached databases

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

| Strength                       | Description                                                                        |
| ------------------------------ | ---------------------------------------------------------------------------------- |
| **One tool for the workflow**  | Split → sample → shard → convert → merge in a single binary                        |
| **Works on dump files**        | No running database or JDBC connection needed (unlike mydumper, Jailer, Condenser) |
| **Streaming architecture**     | 10GB+ dumps with bounded memory, ~1 GB/s split throughput                          |
| **Multi-dialect + conversion** | MySQL, PostgreSQL, SQLite including `COPY FROM stdin` → INSERT                     |
| **FK-aware operations**        | Sampling and tenant sharding preserve referential integrity                        |

**When another tool might be better**

- **[mydumper](https://github.com/mydumper/mydumper)** — Parallel snapshots from live MySQL/MariaDB databases
- **[Jailer](https://github.com/Wisser/Jailer)** — Rich GUI-based FK subsetting with JDBC across 12+ databases
- **[sqlglot](https://github.com/tobymao/sqlglot)** — Query-level transpilation and AST manipulation (31 dialects)
- **[DuckDB](https://github.com/duckdb/duckdb)** — Complex analytical queries over SQL/CSV/JSON/Parquet

See [docs/COMPETITIVE_ANALYSIS.md](docs/COMPETITIVE_ANALYSIS.md) for detailed comparisons.

## Options

### Split Options

| Flag             | Description                                                 | Default     |
| ---------------- | ----------------------------------------------------------- | ----------- |
| `-o, --output`   | Output directory, or an archive path (`.tar.gz`, `.zip`, …) | `output`    |
| `-d, --dialect`  | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql`         | auto-detect |
| `-t, --tables`   | Only split these tables (comma-separated)                   | —           |
| `--compress`     | Compress each output file: `gzip`, `zstd`, `bzip2`, `xz`    | `none`      |
| `--io-strategy`  | Output device I/O strategy: `auto`, `ssd`, `hdd`, `cheap`   | `auto`      |
| `-p, --progress` | Show progress bar                                           | —           |
| `--dry-run`      | Preview without writing files                               | —           |
| `--schema-only`  | Only DDL statements (CREATE, ALTER, DROP)                   | —           |
| `--data-only`    | Only DML statements (INSERT, COPY)                          | —           |
| `--fail-fast`    | Stop on first error (for glob patterns)                     | —           |
| `--json`         | Output results as JSON                                      | —           |

Input can be a file path or glob pattern (e.g., `*.sql`, `dumps/**/*.sql`),
plain or compressed (`.gz`/`.bz2`/`.xz`/`.zst`), or a `.zip` archive
containing exactly one `.sql` member (deflated or stored; junk entries like
`__MACOSX/` and `.DS_Store` are ignored). This applies to every command that
reads a dump, not just `split`.

**Output formats.** By default `split` writes one plain `<table>.sql` per table.
`--compress <fmt>` instead writes one _compressed_ file per table
(`<table>.sql.gz`/`.zst`/`.bz2`/`.xz`) — useful for archival or transfer; each
file compresses independently and in parallel. Alternatively, give `-o` an
archive path (`dump.tar.gz`, `.tgz`, `.tar.zst`, `.tar.bz2`, `.tar.xz`, `.tar`,
or `.zip`) to pack **all** tables into a single archive of `<table>.sql`
entries. Archive output requires a single input file, and `--compress` applies
only to directory output (for an archive the codec comes from the extension).

**Slow output devices.** The write path is tuned for SSDs by default. When the
output lands on a spinning disk, USB stick, or network mount, `--io-strategy
auto` (the default) detects it — a quick startup probe plus runtime
backpressure monitoring — and adjusts how writes are scheduled. In our tests
on a single USB 3.0 spinning drive (ExFAT), this ran roughly 1.5–2.5× faster
than the SSD settings; treat that as indicative for similar hardware, not a
guarantee — the gain depends on the device, filesystem, and workload.

The profiles aren't magic — they set three concrete knobs (writer thread
count, write size, staging memory), and every profile produces byte-identical
output. Pin one when you already know the target device:

| Profile | Writers | Write size  | Staging | Built for                                                                                                                                                     |
| ------- | ------- | ----------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ssd`   | up to 4 | 256 KB      | 32 MB   | SSD/NVMe — high-IOPS devices that like many small parallel writes                                                                                             |
| `hdd`   | 1       | up to 64 MB | 256 MB  | Spinning disks — one writer avoids competing seeks; big sequential writes amortize head movement                                                              |
| `cheap` | 1       | up to 64 MB | 512 MB  | Cheap USB flash, network mounts — every write _operation_ is expensive regardless of locality, so issue the fewest, largest ones. (Also answers to `potato`.) |
| `auto`  | adapts  | adapts      | adapts  | Unknown targets — probes the output dir at startup, then switches between the profiles above based on observed backpressure                                   |

### Merge Options

| Flag             | Description                               | Default |
| ---------------- | ----------------------------------------- | ------- |
| `-o, --output`   | Output SQL file                           | stdout  |
| `-d, --dialect`  | SQL dialect for headers/footers           | `mysql` |
| `-t, --tables`   | Only merge these tables (comma-separated) | all     |
| `-e, --exclude`  | Exclude these tables (comma-separated)    | —       |
| `--transaction`  | Wrap in BEGIN/COMMIT transaction          | —       |
| `--no-header`    | Skip header comments                      | —       |
| `-p, --progress` | Show progress bar                         | —       |
| `--dry-run`      | Preview without writing files             | —       |
| `--json`         | Output results as JSON                    | —       |

### Analyze Options

| Flag             | Description                                         | Default     |
| ---------------- | --------------------------------------------------- | ----------- |
| `-d, --dialect`  | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `-p, --progress` | Show progress bar                                   | —           |
| `--fail-fast`    | Stop on first error (for glob patterns)             | —           |
| `--json`         | Output results as JSON                              | —           |

Input can be a file path or glob pattern (e.g., `*.sql`, `dumps/**/*.sql`).

### Convert Options

| Flag             | Description                                            | Default     |
| ---------------- | ------------------------------------------------------ | ----------- |
| `-o, --output`   | Output SQL file or directory (required for glob)       | stdout      |
| `--from`         | Source dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `--to`           | Target dialect: `mysql`, `postgres`, `sqlite`, `mssql` | required    |
| `--strict`       | Fail on any unsupported feature                        | —           |
| `-p, --progress` | Show progress bar                                      | —           |
| `--dry-run`      | Preview without writing files                          | —           |
| `--fail-fast`    | Stop on first error (for glob patterns)                | —           |
| `--json`         | Output results as JSON                                 | —           |

Input can be a file path or glob pattern (e.g., `*.sql`, `dumps/**/*.sql`).

**Supported conversions (12 pairs):**

- MySQL ↔ PostgreSQL (including COPY → INSERT)
- MySQL ↔ SQLite
- MySQL ↔ MSSQL
- PostgreSQL ↔ SQLite
- PostgreSQL ↔ MSSQL
- SQLite ↔ MSSQL

**Features:**

- 50+ data type mappings
- AUTO_INCREMENT ↔ SERIAL ↔ INTEGER PRIMARY KEY ↔ IDENTITY
- PostgreSQL COPY → INSERT with NULL and escape handling
- Session command stripping
- Warnings for unsupported features (ENUM, arrays, triggers)

### Validate Options

| Flag                   | Description                                         | Default     |
| ---------------------- | --------------------------------------------------- | ----------- |
| `-d, --dialect`        | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `--strict`             | Treat warnings as errors (exit 1)                   | —           |
| `--json`               | Output results as JSON                              | —           |
| `--max-rows-per-table` | Max rows per table for PK/FK checks (0 = no limit)  | 1,000,000   |
| `--no-limit`           | Disable row limit for PK/FK checks                  | —           |
| `--no-fk-checks`       | Skip PK/FK data integrity checks                    | —           |
| `-p, --progress`       | Show progress bar                                   | —           |
| `--fail-fast`          | Stop on first error (for glob patterns)             | —           |

Input can be a file path or glob pattern (e.g., `*.sql`, `dumps/**/*.sql`).

**Validation checks:**

- SQL syntax validation (parser errors)
- DDL/DML consistency (INSERTs reference existing tables)
- Encoding validation (UTF-8)
- Duplicate primary key detection (all dialects)
- FK referential integrity (all dialects)

### Sample Options

| Flag                   | Description                                         | Default     |
| ---------------------- | --------------------------------------------------- | ----------- |
| `-o, --output`         | Output SQL file                                     | stdout      |
| `-d, --dialect`        | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `--percent`            | Sample percentage (1-100)                           | —           |
| `--rows`               | Sample fixed number of rows per table               | —           |
| `--preserve-relations` | Preserve FK relationships                           | —           |
| `-t, --tables`         | Only sample these tables (comma-separated)          | all         |
| `-e, --exclude`        | Exclude these tables (comma-separated)              | —           |
| `--root-tables`        | Explicit root tables for sampling                   | —           |
| `--include-global`     | Global table handling: `none`, `lookups`, `all`     | `lookups`   |
| `--seed`               | Random seed for reproducibility                     | random      |
| `-c, --config`         | YAML config file for per-table settings             | —           |
| `--max-total-rows`     | Maximum total rows to sample (0 = no limit)         | —           |
| `--no-limit`           | Disable row limit                                   | —           |
| `--strict-fk`          | Fail if any FK integrity issues detected            | —           |
| `--no-schema`          | Exclude CREATE TABLE statements from output         | —           |
| `-p, --progress`       | Show progress bar                                   | —           |
| `--dry-run`            | Preview without writing files                       | —           |
| `--json`               | Output results as JSON                              | —           |

### Shard Options

| Flag                  | Description                                         | Default     |
| --------------------- | --------------------------------------------------- | ----------- |
| `-o, --output`        | Output SQL file or directory                        | stdout      |
| `-d, --dialect`       | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `--tenant-column`     | Column name for tenant identification               | auto-detect |
| `--tenant-value`      | Single tenant value to extract                      | —           |
| `--tenant-values`     | Multiple tenant values (comma-separated)            | —           |
| `--root-tables`       | Explicit root tables with tenant column             | —           |
| `--include-global`    | Global table handling: `none`, `lookups`, `all`     | `lookups`   |
| `-c, --config`        | YAML config file for table classification           | —           |
| `--max-selected-rows` | Maximum rows to select (0 = no limit)               | —           |
| `--no-limit`          | Disable row limit                                   | —           |
| `--strict-fk`         | Fail if any FK integrity issues detected            | —           |
| `--no-schema`         | Exclude CREATE TABLE statements from output         | —           |
| `-p, --progress`      | Show progress bar                                   | —           |
| `--dry-run`           | Preview without writing files                       | —           |
| `--json`              | Output results as JSON                              | —           |

### Diff Options

| Flag               | Description                                                  | Default     |
| ------------------ | ------------------------------------------------------------ | ----------- |
| `-o, --output`     | Output file (default: stdout)                                | stdout      |
| `-d, --dialect`    | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql`          | auto-detect |
| `--schema-only`    | Compare schema only, skip data                               | —           |
| `--data-only`      | Compare data only, skip schema                               | —           |
| `--format`         | Output format: `text`, `json`, `sql`                         | `text`      |
| `-t, --tables`     | Only compare these tables (comma-separated)                  | all         |
| `-e, --exclude`    | Exclude these tables (comma-separated)                       | —           |
| `--max-pk-entries` | Max PK entries to track (0 = no limit)                       | 10,000,000  |
| `-v, --verbose`    | Show sample PK values for added/removed/modified rows        | —           |
| `--primary-key`    | Override PK for tables (format: `table:col1+col2`)           | auto-detect |
| `--ignore-order`   | Ignore column order differences in schema comparison         | —           |
| `--ignore-columns` | Ignore columns matching glob patterns (e.g., `*.updated_at`) | —           |
| `--allow-no-pk`    | Compare tables without PK using all columns as key           | —           |
| `-p, --progress`   | Show progress bar                                            | —           |

**What diff detects:**

- Tables added/removed/modified (columns, types, nullability)
- Primary key changes
- Foreign key changes
- Index changes (CREATE INDEX, inline INDEX/KEY)
- Rows added/removed/modified (via PK-based comparison)

**Output formats:**

- `text`: Human-readable summary with optional PK samples
- `json`: Structured data for automation (includes warnings)
- `sql`: Migration script with ALTER/CREATE INDEX/DROP INDEX statements

### Graph Options

| Flag             | Description                                            | Default     |
| ---------------- | ------------------------------------------------------ | ----------- |
| `-o, --output`   | Output file (html, dot, mmd, json, png, svg, pdf)      | stdout      |
| `--format`       | Output format: `html`, `dot`, `mermaid`, `json`        | auto-detect |
| `-d, --dialect`  | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql`    | auto-detect |
| `--layout`       | Layout direction: `lr` (left-right), `tb` (top-bottom) | `lr`        |
| `-t, --tables`   | Only include tables matching glob patterns             | all         |
| `-e, --exclude`  | Exclude tables matching glob patterns                  | —           |
| `--table`        | Focus on a specific table                              | —           |
| `--transitive`   | Show all dependencies of focused table                 | —           |
| `--reverse`      | Show all tables that depend on focused table           | —           |
| `--max-depth`    | Limit traversal depth                                  | unlimited   |
| `--cycles-only`  | Only show tables in circular dependencies              | —           |
| `--render`       | Render DOT to PNG/SVG/PDF using Graphviz               | —           |
| `-p, --progress` | Show progress bar                                      | —           |
| `--json`         | Output as JSON                                         | —           |

**Output formats:**

- `html`: Interactive diagram with dark/light theme, copy Mermaid button
- `dot`: Graphviz DOT with ERD-style tables (columns, types, PK/FK markers)
- `mermaid`: Mermaid erDiagram syntax
- `json`: Full schema with tables, columns, relationships, and stats
- `png`/`svg`/`pdf`: Rendered image (requires Graphviz `dot` command)

### Order Options

| Flag            | Description                                         | Default     |
| --------------- | --------------------------------------------------- | ----------- |
| `-o, --output`  | Output SQL file                                     | stdout      |
| `-d, --dialect` | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql` | auto-detect |
| `--check`       | Check for cycles and report order (don't write)     | —           |
| `--dry-run`     | Show topological order without writing              | —           |
| `--reverse`     | Reverse order (children before parents, for DROP)   | —           |

### Redact Options

| Flag                | Description                                             | Default     |
| ------------------- | ------------------------------------------------------- | ----------- |
| `-o, --output`      | Output SQL file                                         | stdout      |
| `-d, --dialect`     | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql`     | auto-detect |
| `-c, --config`      | YAML config file for redaction rules                    | —           |
| `--generate-config` | Analyze input and generate annotated YAML config        | —           |
| `--null`            | Columns to set to NULL (glob patterns, comma-separated) | —           |
| `--hash`            | Columns to hash with SHA256 (glob patterns)             | —           |
| `--fake`            | Columns to replace with fake data (glob patterns)       | —           |
| `--mask`            | Columns to partially mask (format: `pattern=column`)    | —           |
| `--constant`        | Column=value pairs for constant replacement             | —           |
| `--seed`            | Random seed for reproducible redaction                  | random      |
| `--locale`          | Locale for fake data (en, de_de, fr_fr, etc.)           | `en`        |
| `-t, --tables`      | Only redact specific tables (comma-separated)           | all         |
| `-e, --exclude`     | Exclude specific tables (comma-separated)               | —           |
| `--strict`          | Fail on warnings (e.g., unsupported locale)             | —           |
| `-p, --progress`    | Show progress bar                                       | —           |
| `--dry-run`         | Preview without writing files                           | —           |
| `--json`            | Output results as JSON                                  | —           |
| `--validate`        | Validate config only, don't process                     | —           |

**Redaction strategies:**

- `null`: Replace value with NULL
- `constant`: Replace with fixed value
- `hash`: SHA256 hash (deterministic, preserves FK relationships)
- `mask`: Partial masking with pattern (`*`=asterisk, `X`=keep, `#`=random digit)
- `fake`: Generate realistic fake data (25+ generators)
- `shuffle`: Redistribute values within column (preserves distribution)
- `skip`: No redaction (passthrough)

**Fake data generators:**

`email`, `name`, `first_name`, `last_name`, `phone`, `address`, `city`, `state`, `zip`, `country`, `company`, `job_title`, `username`, `url`, `ip`, `ipv6`, `uuid`, `date`, `datetime`, `credit_card`, `iban`, `ssn`, `lorem`, `paragraph`, `sentence`

### Query Options

| Flag                | Description                                           | Default     |
| ------------------- | ----------------------------------------------------- | ----------- |
| `-f, --format`      | Output format: `table`, `json`, `jsonl`, `csv`, `tsv` | `table`     |
| `-o, --output`      | Write output to file instead of stdout                | stdout      |
| `-d, --dialect`     | SQL dialect: `mysql`, `postgres`, `sqlite`, `mssql`   | auto-detect |
| `-i, --interactive` | Start interactive REPL session                        | —           |
| `--disk`            | Use disk-based storage (for large dumps >2GB)         | auto        |
| `--cache`           | Cache imported database for repeated queries          | —           |
| `-t, --tables`      | Only import specific tables (comma-separated)         | all         |
| `--memory-limit`    | Memory limit for DuckDB (e.g., "4GB")                 | —           |
| `--timing`          | Show query execution time                             | —           |
| `-p, --progress`    | Show import progress                                  | —           |
| `--list-cache`      | List cached databases                                 | —           |
| `--clear-cache`     | Clear all cached databases                            | —           |

**REPL commands:**

- `.tables` — List all tables
- `.schema [table]` — Show schema (all tables or specific table)
- `.describe <table>` — Describe a specific table
- `.format <fmt>` — Set output format (table, json, csv, tsv)
- `.count <table>` — Count rows in a table
- `.sample <table> [n]` — Show sample rows (default: 10)
- `.export <file> <query>` — Export query results to file
- `.exit` — Exit the REPL

## Performance

See [BENCHMARKS.md](BENCHMARKS.md) for detailed comparisons.

## Testing

```bash
# Unit tests
cargo test

# Verify against real-world SQL dumps (MySQL, PostgreSQL, WordPress, etc.)
make verify-realworld
```

## AI Agent Integration

sql-splitter includes documentation optimized for AI agents:

- **[llms.txt](website/llms.txt)** - LLM-friendly documentation following the [llmstxt.org](https://llmstxt.org) specification
- **[Agent Skill](skills/sql-splitter/SKILL.md)** - Claude Code / Amp skill for automatic tool discovery

Install the skill in Claude Code / Amp:

```bash
amp skill add helgesverre/sql-splitter
```

## License

MIT — see [LICENSE.md](LICENSE.md)
