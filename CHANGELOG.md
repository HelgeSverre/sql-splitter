# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.10.0] - 2025-12-21

### Added

- **Redact command**: Anonymize sensitive data (PII) in SQL dumps
  - `sql-splitter redact dump.sql -o safe.sql --config redact.yaml` - Apply redaction rules from YAML config
  - `sql-splitter redact dump.sql -o safe.sql --null "*.ssn" --hash "*.email" --fake "*.name"` - Inline patterns
  - `sql-splitter redact dump.sql --generate-config -o redact.yaml` - Auto-generate config by analyzing schema
  - **7 redaction strategies**: null, constant, hash, mask, shuffle, fake, skip
  - **25+ fake data generators**: email, name, first_name, last_name, phone, address, city, zip, company, ip, uuid, date, credit_card, ssn, lorem, and more
  - **Glob pattern matching**: Match columns with patterns like `*.email`, `users.ssn`, `*.password`
  - **YAML config support**: Define rules in a config file with defaults, rules, and skip_tables
  - **Multi-locale fake data**: Support for 8 locales (en, de_de, fr_fr, zh_cn, zh_tw, ja_jp, pt_br, ar_sa)
  - **Reproducible redaction**: `--seed` flag for deterministic fake data
  - **Streaming architecture**: Constant memory usage (~87MB for 10GB files)
  - **High throughput**: ~230 MB/s on large files
  - `--dry-run` flag to preview redaction without writing
  - `--json` flag for machine-readable output
  - `--strict` flag to fail on warnings
  - `--tables` and `--exclude` flags for table filtering
  - Supports all 3 dialects (MySQL, PostgreSQL, SQLite)
  - Supports compressed input files (.gz, .bz2, .xz, .zst)
  - Man page: `man sql-splitter-redact`
  - Command alias: `rd`

### Changed

- Updated verify-realworld.sh to test redact command on all 25+ real-world dumps
- Updated profile-memory.sh to profile redact command

## [1.9.2] - 2025-12-21

### Added

- **CLI UX improvements**: Enhanced help output and discoverability
  - **Help headings**: Options grouped by category (Input/Output, Filtering, Mode, Behavior, Limits, Output)
  - **After-help examples**: Each command shows 2-5 practical usage examples
  - **Command aliases**: Short aliases for common commands (`sp`, `an`, `mg`, `sa`, `sh`, `cv`, `val`, `df`)
  - **Value hints**: Improved shell completion suggestions for file/directory paths
  - **Improved completions command**: Detailed installation instructions for bash, zsh, fish, PowerShell
- **Man page generation**: `make man` generates man pages for all commands
  - Uses `clap_mangen` for automatic generation from CLI definitions
  - Generates `sql-splitter.1` and per-command pages like `sql-splitter-diff.1`
  - `make install` now installs man pages automatically
  - `make install-man` for standalone man page installation

### Changed

- Updated CLI about text: "High-performance CLI for splitting, merging, converting, and analyzing SQL dump files"
- All commands now show `--help` when run without required arguments

## [1.9.1] - 2025-12-21

### Added

- **Diff command enhanced features**: Extended schema and data comparison capabilities
  - `--verbose` flag: Show sample PK values for added/removed/modified rows (up to 100 samples)
  - `--primary-key` flag: Override PK for data comparison (format: `table:col1+col2,table2:col`)
  - `--ignore-order` flag: Ignore column order differences in schema comparison
  - `--ignore-columns` flag: Exclude columns from comparison using glob patterns (e.g., `*.updated_at`)
  - `--allow-no-pk` flag: Compare tables without primary key using all columns as key
  - **Index diff**: Detect added/removed indexes in schema comparison (inline and standalone CREATE INDEX)
  - **Warnings system**: Emit warnings for tables without PK, invalid PK override columns, etc.
  - Supports index types: BTREE, HASH, GIN (PostgreSQL), FULLTEXT (MySQL)
- **40 new integration tests** for enhanced diff features across all 3 dialects
- **Added `glob` dependency** (0.3) for column ignore pattern matching

### Changed

- `diff --format sql` now generates `CREATE INDEX` and `DROP INDEX` statements for index changes

## [1.9.0] - 2025-12-21

### Added

- **Diff command**: Compare two SQL dumps and report schema + data differences
  - `sql-splitter diff old.sql new.sql` - Compare two SQL dump files
  - Schema comparison: tables added/removed, columns added/removed/modified, PK changes, FK changes
  - Data comparison: rows added/removed/modified per table with memory-bounded PK tracking
  - Output formats: text (human-readable), json (structured), sql (migration script)
  - `--schema-only` flag to compare schema only, skip data
  - `--data-only` flag to compare data only, skip schema
  - `--tables` flag to compare only specific tables
  - `--exclude` flag to exclude specific tables
  - `--max-pk-entries` flag to limit memory usage for large tables (default: 10M entries ~160MB)
  - Supports all 3 dialects (MySQL, PostgreSQL, SQLite)
  - Progress bar support with `--progress` flag
  - Supports compressed input files (.gz, .bz2, .xz, .zst)
- **17 new integration tests** for diff command (schema diff, data diff, filters, output formats, PostgreSQL COPY)

### Fixed

- **PostgreSQL COPY data parsing**: Fixed a bug where data rows in PostgreSQL COPY ... FROM stdin format were not correctly parsed for PK/FK validation and data diff operations
  - The parser returns COPY header and data as separate statements; consumers now correctly handle this
  - Affects: `diff --data-only` and `validate` commands with PostgreSQL dumps using COPY format
  - Now correctly detects duplicate PKs, FK violations, and row changes in COPY data blocks

- **`--json` flag for all commands**: Machine-readable JSON output for automation and CI/CD pipelines
  - `split --json`: Output split statistics, table names, and throughput as JSON
  - `analyze --json`: Output table statistics, INSERT counts, and byte sizes as JSON
  - `merge --json`: Output merge statistics and table list as JSON
  - `sample --json`: Output sample mode, statistics, and per-table breakdown as JSON
  - `shard --json`: Output tenant info, statistics, and table breakdown as JSON
  - `convert --json`: Output conversion statistics and warnings as JSON
  - Multi-file operations return aggregated JSON with per-file results
  - All JSON output uses `serde_json::to_string_pretty()` for readability
  - Progress bars automatically suppressed in JSON mode
- **11 new integration tests** for JSON output validation across all commands

## [1.8.2] - 2025-12-21

### Changed

- **Sample command memory optimization**: Reduced memory usage by ~98.5% for large files
  - 2.9 GB file: 8.2 GB → 114 MB peak RSS
  - Uses streaming approach with temp files instead of in-memory accumulation
  - Introduced `PkHashSet` (64-bit hashes) for compact primary key tracking
  - Both `--percent` and `--rows` modes benefit from optimization

### Added

- **Memory profiling infrastructure**: New profiling script and Makefile targets
  - `make profile` - Profile with medium dataset (~50MB)
  - `make profile-large` - Profile with large dataset (~250MB)
  - `make profile-mega` - Stress test with ~2GB dataset (100 tables × 100k rows)
  - `scripts/profile-memory.sh` - Automated profiling for all commands
  - Size presets: tiny (1MB), small (5MB), medium (50MB), large (250MB), xlarge (500MB), huge (1GB), mega (2GB)

## [1.8.1] - 2025-12-21

### Added

- **Glob pattern support**: All file-based commands now accept glob patterns for batch processing
  - `sql-splitter validate "dumps/*.sql"` - validate multiple files
  - `sql-splitter analyze "**/*.sql"` - analyze files recursively
  - `sql-splitter split "*.sql" -o output/` - split multiple files to subdirectories
  - `sql-splitter convert "*.sql" --to postgres -o converted/` - batch conversion
  - Supports `*`, `**`, `?`, and `[abc]` patterns
  - Aggregated JSON output for validate command with multi-file results
- **`--fail-fast` flag**: Stop processing on first error when using glob patterns
  - Available on: validate, analyze, split, convert commands
- **`--no-limit` flag**: Disable row limits for memory-intensive operations
  - `validate --no-limit` - no limit on rows for PK/FK checks
  - `sample --no-limit` - disable explosion guard
  - `shard --no-limit` - disable memory guard
  - Also supports `--max-rows-per-table=0` as equivalent
- **Agent Skill**: Added SKILL.md following [agentskills.io](https://agentskills.io) specification
  - Supports Amp, Claude Code, VS Code/GitHub Copilot, Cursor, Goose, Letta, OpenCode
  - Install via `amp skill add helgesverre/sql-splitter` or manual copy
  - Universal installer: `npx ai-agent-skills install sql-splitter --agent <agent>`
- **llms.txt**: Added LLM-friendly documentation with Agent Skill installation instructions

### Changed

- **Multi-dialect PK/FK validation**: Extended PK duplicate detection and FK referential integrity checking to all dialects (MySQL, PostgreSQL, SQLite). Previously MySQL-only.
  - PostgreSQL: Supports both INSERT statements and COPY ... FROM stdin format
  - SQLite: Reuses MySQL INSERT parser for validation
  - 6 new dialect-specific tests for PostgreSQL and SQLite PK/FK validation
- **Improved progress tracking**: Consolidated byte-based progress across all commands
- **Simplified skill structure**: Single `skills/sql-splitter/` directory (removed duplicate)

## [1.8.0] - 2025-12-21

### Added

- **Validate command**: Check SQL dump integrity with comprehensive validation
  - `sql-splitter validate dump.sql` - Validate a SQL dump file
  - `--dialect` flag to specify dialect (mysql, postgres, sqlite - auto-detected if not specified)
  - `--strict` flag to treat warnings as errors (non-zero exit on any warning)
  - `--json` flag to output results as JSON for programmatic parsing
  - `--max-rows-per-table` flag to limit memory usage for PK/FK checks (default: 1,000,000)
  - `--no-fk-checks` flag to disable heavy data integrity checks
  - `--progress` flag to show validation progress
  - Supports compressed input files (.gz, .bz2, .xz, .zst)
- **Validation checks**:
  - **SQL syntax validation**: Detects parser errors and malformed statements
  - **DDL/DML consistency**: Finds INSERTs referencing tables with no CREATE TABLE
  - **Encoding validation**: Warns on invalid UTF-8 byte sequences
  - **Duplicate primary key detection** (MySQL only): Finds rows with duplicate PKs
  - **FK referential integrity** (MySQL only): Detects FK violations where child references missing parent
- **Output formats**:
  - Human-readable text output with colored severity levels
  - JSON output for CI/automation integration
  - Detailed summary with per-check status
- 32 integration tests for validate command including:
  - Basic validation tests (valid dump, missing tables, duplicate PK, FK violations)
  - test_data_gen fixture tests for realistic multi-table scenarios
  - Split→Merge→Validate roundtrip tests for all 3 dialects
  - Edge case tests (empty files, comments-only, composite PKs/FKs, self-referential FKs)

### Changed

- Added `serde_json` dependency for JSON output support

## [1.7.0] - 2025-12-21

### Added

- **Convert command**: Convert SQL dumps between MySQL, PostgreSQL, and SQLite dialects
  - Supports all 6 conversion pairs:
    - MySQL → PostgreSQL, MySQL → SQLite
    - PostgreSQL → MySQL, PostgreSQL → SQLite
    - SQLite → MySQL, SQLite → PostgreSQL
  - `--from` flag to specify source dialect (auto-detected if not specified)
  - `--to` flag to specify target dialect (required)
  - `--strict` flag to fail on any unsupported feature
  - `--dry-run` flag to preview conversion statistics
  - `--progress` flag to show conversion progress
  - Supports compressed input files (.gz, .bz2, .xz, .zst)
- **Identifier quoting conversion**: Backticks ↔ double quotes between dialects
- **String escape normalization**: MySQL `\'` → PostgreSQL/SQLite `''`
- **Data type mapping** (bidirectional):
  - MySQL → PostgreSQL: `TINYINT(1)` → `BOOLEAN`, `AUTO_INCREMENT` → `SERIAL`, `LONGTEXT` → `TEXT`, `BLOB` → `BYTEA`, `DATETIME` → `TIMESTAMP`, `JSON` → `JSONB`
  - MySQL → SQLite: All types → SQLite's affinity types (INTEGER, REAL, TEXT, BLOB)
  - PostgreSQL → MySQL: `SERIAL` → `AUTO_INCREMENT`, `BYTEA` → `LONGBLOB`, `BOOLEAN` → `TINYINT(1)`, `JSONB` → `JSON`, `UUID` → `VARCHAR(36)`, `TIMESTAMPTZ` → `DATETIME`
  - PostgreSQL → SQLite: `SERIAL` → `INTEGER`, `BYTEA` → `BLOB`, `DOUBLE PRECISION` → `REAL`
  - SQLite → MySQL: `REAL` → `DOUBLE`
  - SQLite → PostgreSQL: `REAL` → `DOUBLE PRECISION`, `BLOB` → `BYTEA`
- **Session command handling**: Strips dialect-specific session commands during conversion
  - MySQL: `SET NAMES`, `SET FOREIGN_KEY_CHECKS`, `LOCK TABLES`, conditional comments
  - PostgreSQL: `SET client_encoding`, `SET search_path`, etc.
  - SQLite: `PRAGMA` statements
- **Warning system**: Reports unsupported features
  - MySQL: ENUM, SET, UNSIGNED, FULLTEXT indexes
  - PostgreSQL: Array types, INHERITS, PARTITION BY (to SQLite)
- 37 new unit tests for converter module
- 15 new integration tests for convert command

### Changed

- **Test organization overhaul**: Extracted all 235 inline tests from source files to dedicated test files in `tests/` directory
  - Created 11 new unit test files: `parser_unit_test.rs`, `convert_unit_test.rs`, `sample_unit_test.rs`, `shard_unit_test.rs`, `schema_unit_test.rs`, `splitter_unit_test.rs`, `merger_unit_test.rs`, `analyzer_unit_test.rs`, `writer_unit_test.rs`, `cmd_unit_test.rs`
  - Moved `parser_edge_cases_test.rs` (83 tests) from src/ to tests/
  - Source files now contain only production code (zero inline tests)
- **Convert command enhancements**:
  - PostgreSQL COPY → INSERT conversion with batched inserts (100 rows per INSERT)
  - NULL marker handling (`\N` → NULL) and escape sequence conversion
  - PostgreSQL type cast stripping (::regclass, ::text, ::character varying)
  - Schema prefix removal (public.table → table)
  - DEFAULT now() → DEFAULT CURRENT_TIMESTAMP conversion
  - nextval() sequence removal
  - PostgreSQL-only feature filtering with warnings (CREATE DOMAIN/TYPE/FUNCTION/SEQUENCE)
  - TIMESTAMP WITH TIME ZONE → DATETIME conversion
  - Block comment handling at statement start
- Test suite expanded from 359 to 429 tests

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
