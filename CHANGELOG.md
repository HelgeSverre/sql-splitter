# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
