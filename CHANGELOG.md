# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.7.0] - 2025-12-20

### Added

- **Convert command**: Convert SQL dumps between MySQL, PostgreSQL, and SQLite dialects
  - `sql-splitter convert mysql.sql -o postgres.sql --to postgres`
  - `sql-splitter convert mysql.sql -o sqlite.sql --to sqlite`
  - `--from` flag to specify source dialect (auto-detected if not specified)
  - `--to` flag to specify target dialect (required)
  - `--strict` flag to fail on any unsupported feature
  - `--dry-run` flag to preview conversion statistics
  - `--progress` flag to show conversion progress
  - Supports compressed input files (.gz, .bz2, .xz, .zst)
- **Identifier quoting conversion**: Backticks ↔ double quotes between dialects
- **String escape normalization**: MySQL `\'` → PostgreSQL/SQLite `''`
- **Data type mapping**: Comprehensive type conversion including:
  - `TINYINT(1)` → `BOOLEAN` (PostgreSQL)
  - `INT AUTO_INCREMENT` → `SERIAL` (PostgreSQL) or `INTEGER` (SQLite)
  - `LONGTEXT/MEDIUMTEXT/TINYTEXT` → `TEXT`
  - `BLOB/LONGBLOB` → `BYTEA` (PostgreSQL) or `BLOB` (SQLite)
  - `DATETIME` → `TIMESTAMP` (PostgreSQL) or `TEXT` (SQLite)
  - `JSON` → `JSONB` (PostgreSQL) or `TEXT` (SQLite)
  - `ENUM`/`SET` → `VARCHAR(255)` with warning
  - `UNSIGNED` modifier removed with warning
- **MySQL-specific cleanup**: Strips ENGINE, CHARSET, COLLATE clauses and conditional comments
- **Warning system**: Reports unsupported features (ENUM, SET, UNSIGNED, FULLTEXT indexes)
- 20 new unit tests for converter module
- 9 new integration tests for convert command

### Changed

- Test suite expanded from 359 to 406 tests

## [1.6.0] - 2025-12-20

### Added

- **Shard command**: Extract tenant-specific data from multi-tenant SQL dumps
  - `sql-splitter shard dump.sql -o tenant_5.sql --tenant-value 5`
  - `--tenant-column` flag to specify tenant column (auto-detected if not specified)
  - Automatic tenant column detection (looks for company_id, tenant_id, organization_id, etc.)
  - FK-ordered tenant selection that follows FK chains to include dependent data
  - Table classification: tenant-root, tenant-dependent, junction, lookup, system
  - `--root-tables` flag for explicit root table specification
  - `--include-global` flag to control lookup table handling (none, lookups, all)
  - `--config` flag for YAML-based table classification overrides
  - `--max-selected-rows` memory guard (default 10M rows)
  - `--strict-fk` flag to fail on FK integrity issues
  - `--no-schema` flag to exclude CREATE TABLE statements
  - `--dry-run` flag to preview sharding statistics
  - Supports MySQL, PostgreSQL (COPY format), and SQLite dialects
- **Shard YAML configuration**: Per-table classification overrides
  - Tenant column and root table configuration
  - Table role overrides (tenant-root, junction, lookup, system)
  - Self-FK column specification for hierarchical tables (future use)
- 9 new unit tests for shard command

### Changed

- Extended shared infrastructure from v1.5.0 for tenant extraction use case
- Schema graph reused for FK dependency analysis

## [1.5.0] - 2025-12-20

### Added

- **Sample command**: Create reduced datasets from large SQL dumps for development/testing
  - `sql-splitter sample dump.sql -o dev.sql --percent 10` - Sample 10% of rows
  - `sql-splitter sample dump.sql -o dev.sql --rows 1000` - Sample up to 1000 rows per table
  - `--preserve-relations` flag for FK-aware sampling that maintains referential integrity
  - `--root-tables` flag to start sampling from specific tables
  - `--include-global` flag to control lookup/global table handling (none, lookups, all)
  - `--tables` and `--exclude` flags for table filtering
  - `--seed` flag for reproducible sampling
  - `--config` flag for YAML-based per-table sampling strategies
  - `--max-total-rows` explosion guard
  - `--strict-fk` flag to fail on FK integrity issues
  - `--no-schema` flag to exclude CREATE TABLE statements
  - `--dry-run` flag to preview sampling statistics
  - Supports MySQL, PostgreSQL (COPY format), and SQLite dialects
- **Schema graph module**: FK dependency analysis with topological sorting
  - `src/schema/` module for DDL parsing and FK extraction
  - Supports all three SQL dialects (backticks, double quotes, unquoted identifiers)
  - Cycle detection for self-referential and circular FK relationships
- **Row parsing module**: Extract individual rows from INSERT and COPY statements
  - `src/parser/mysql_insert.rs` for MySQL INSERT parsing with PK/FK extraction
  - `src/parser/postgres_copy.rs` for PostgreSQL COPY format parsing
- **YAML configuration**: Per-table sampling strategies via config file
  - Table classification (root, lookup, system, junction, normal)
  - Per-table percent or row count overrides
  - Skip specific tables
- 15 new integration tests for sample command across all dialects
- 40+ new unit tests for schema parsing, row parsing, and reservoir sampling

### Changed

- DDL parsing now supports PostgreSQL and SQLite identifier quoting
- Column type parsing extended with PostgreSQL types (SERIAL, TIMESTAMPTZ, BYTEA, etc.)
- Test suite expanded from 130 to 359 tests

### Fixed

- PostgreSQL COPY data now correctly converts to INSERT VALUES format in output

## [1.4.0] - 2025-12-20

### Added

- **Merge command**: Combine split SQL files back into a single dump file
  - `sql-splitter merge tables/ -o restored.sql`
  - `--tables` flag to merge specific tables only
  - `--exclude` flag to skip certain tables
  - `--transaction` flag to wrap output in BEGIN/COMMIT
  - `--dialect` flag for MySQL, PostgreSQL, or SQLite output format
  - Streaming concatenation with 256KB buffers
  - Progress bar support with `--progress` flag
- **Test data generator**: New `test_data_gen` crate for generating synthetic SQL fixtures
  - Deterministic, seed-based generation for reproducible tests
  - Multi-tenant schema with 16 tables (FK chains, self-refs, junctions, global tables)
  - Three scales: small (~500 rows), medium (~10K), large (~200K)
  - MySQL, PostgreSQL (COPY + INSERT), and SQLite output
  - CLI tool: `gen-fixtures --dialect mysql --scale small --seed 42`
- **New merger module**: Reusable `Merger` struct in `src/merger/` for programmatic use
- **Static test fixtures**: Edge case SQL files for MySQL, PostgreSQL, SQLite in `tests/fixtures/static/`
- 7 new tests for merge functionality

### Changed

- Project now uses Cargo workspace with `crates/test_data_gen/` subcrate
- Expanded test suite from 123 to 130 unit tests

## [1.3.1] - 2025-12-20

### Added

- **Animated progress bar**: New progress display using `indicatif` crate with:
  - Animated spinner (⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏)
  - Visual progress bar with gradient characters (█▓▒░)
  - Elapsed time, bytes processed, and percentage display
  - Smooth 100ms tick rate animation
- Benchmark script now supports `--progress` flag to test progress bar performance

### Fixed

- Locale-related formatting issue in benchmark summary output

## [1.3.0] - 2025-12-20

### Added

- **Compressed file support**: Automatically decompress gzip (`.gz`), bzip2 (`.bz2`), xz (`.xz`), and zstd (`.zst`) files during processing — no manual decompression needed
- **Schema/Data filtering**: New `--schema-only` and `--data-only` flags to extract only DDL or DML statements
  - `--schema-only`: Only CREATE TABLE, CREATE INDEX, ALTER TABLE, DROP TABLE
  - `--data-only`: Only INSERT and COPY statements
- **Shell completions**: New `completions` subcommand generates completions for bash, zsh, fish, elvish, and PowerShell
  - `sql-splitter completions bash >> ~/.bashrc`
- **Automatic shell completion installation**: `make install` now automatically installs shell completions for the detected shell
- New Makefile targets:
  - `make install-completions`: Install completions for current shell only
  - `make install-completions-all`: Install completions for bash, zsh, and fish
- New `scripts/install-completions.sh` script for flexible completion installation
- 4 new tests for compression and content filtering

### Changed

- Updated README with improved installation instructions and shell completions section
- Expanded test suite from 119 to 123 unit tests

## [1.2.1] - 2025-12-20

### Fixed

- **PostgreSQL dollar-quoting bug**: Fixed parser getting stuck when SQL files contained mixed dollar-quote tags (e.g., `$_$` followed by `$$`). The parser now validates that dollar-quote tags are syntactically valid (empty or identifier-like `[A-Za-z_][A-Za-z0-9_]*`)
- **DROP TABLE IF EXISTS**: Fixed table name extraction for `DROP TABLE IF EXISTS` statements

### Added

- Real-world SQL verification script (`scripts/verify-realworld.sh`) that tests against 25 public SQL dumps
- `make verify-realworld` target for running real-world verification tests
- 32 new edge case tests for PostgreSQL, MySQL, SQLite, and cross-dialect parsing
- Tests for dollar-quoting, schema-qualified tables, IF EXISTS/IF NOT EXISTS clauses

### Changed

- Expanded test suite from 87 to 119 unit tests

## [1.0.0] - 2025-12-20

### Added

- Initial release of sql-splitter CLI tool (Rust rewrite of the Go version)
- **`split` command**: Split large SQL dump files into individual table files
  - `--output, -o`: Specify output directory (default: `output`)
  - `--verbose, -v`: Enable verbose output
  - `--progress, -p`: Show progress during processing
  - `--tables, -t`: Filter to split only specific tables (comma-separated)
  - `--dry-run`: Preview what would be split without writing files
- **`analyze` command**: Analyze SQL files and display statistics
  - `--progress, -p`: Show progress during analysis
- **High-performance streaming parser**
  - 400-500 MB/s typical throughput (1.25x faster than Go version on large files)
  - Memory-efficient: ~80 MB constant usage regardless of file size
  - Handles strings with escaped characters and multi-line statements
  - Adaptive buffer sizing based on file size
- **Concurrent writing**: Efficient multi-table writing with writer pools
- **Statement type support**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE
- **Version flag**: `--version` to display version information

### Performance

- Streaming architecture handles files larger than available RAM
- Zero-copy parsing using byte slices where possible
- Pre-compiled regexes for fast pattern matching
- Optimized buffer sizes for CPU cache efficiency
- 1.25x faster than Go version on files >1GB

### Documentation

- Comprehensive README with usage examples
- AGENTS.md for AI assistant guidance
- Performance benchmarks and comparison with Go/PHP versions

## [Unreleased]

### Planned

- Compressed file support (gzip, bzip2)
- Parallel parsing for very large files

## [1.2.0] - 2025-12-20

### Added

- **Automatic dialect detection**: The `--dialect` flag is now optional. When omitted, sql-splitter automatically detects the SQL dialect by analyzing the first 8KB of the file
  - Uses weighted scoring to identify PostgreSQL, MySQL/MariaDB, or SQLite formats
  - Reports detection confidence level (high, medium, low)
  - Detects MySQL/MariaDB by: header comments, conditional comments (`/*!40...`), `LOCK TABLES`, backticks
  - Detects PostgreSQL by: pg_dump header, `COPY ... FROM stdin`, `search_path`, dollar-quoting, `CREATE EXTENSION`
  - Detects SQLite by: header comments, `PRAGMA`, `BEGIN TRANSACTION`
  - Defaults to MySQL when no markers are found

### Changed

- `--dialect` flag is now optional for both `split` and `analyze` commands
- Added 16 new tests for dialect detection

### Documentation

- Updated docs/DIALECT_AUTODETECTION.md with implementation details

## [1.1.0] - 2025-12-20

### Added

- **Multi-dialect support**: Now supports MySQL, PostgreSQL, and SQLite dump formats
  - `--dialect=mysql` (default): MySQL/MariaDB mysqldump format
  - `--dialect=postgres`: PostgreSQL pg_dump format with COPY FROM stdin support
  - `--dialect=sqlite`: SQLite .dump format
- **PostgreSQL COPY statement support**: Properly handles `COPY table FROM stdin` blocks
- **Dollar-quoting support**: PostgreSQL dollar-quoted strings (`$$`, `$tag$`) are correctly parsed
- **Comprehensive benchmarks**: Added benchmark suite comparing against 6 competitor tools
- **Test data generator**: Python script for generating synthetic mysqldump files

### Changed

- Improved table name extraction for different identifier quoting styles
- Updated documentation with multi-dialect examples and benchmark results

### Fixed

- Correct handling of double-quote identifiers in PostgreSQL/SQLite
- Edge cases with escaped characters in different SQL dialects
