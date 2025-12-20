# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- PostgreSQL COPY statement support
- Parallel parsing for very large files
