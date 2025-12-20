# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-12-20

### Added

- Initial release of sql-splitter CLI tool
- **`split` command**: Split large SQL dump files into individual table files
  - `--output, -o`: Specify output directory (default: `output`)
  - `--verbose, -v`: Enable verbose output
  - `--progress, -p`: Show progress during processing
  - `--tables, -t`: Filter to split only specific tables (comma-separated)
  - `--dry-run`: Preview what would be split without writing files
- **`analyze` command**: Analyze SQL files and display statistics
  - `--progress, -p`: Show progress during analysis
- **High-performance streaming parser**
  - 300-400 MB/s typical throughput (peaks at 411 MB/s on Apple M2 Max)
  - Memory-efficient: ~100 MB constant usage regardless of file size
  - Handles strings with escaped characters and multi-line statements
  - Adaptive buffer sizing based on file size
- **Concurrent writing**: Efficient multi-table writing with writer pools
- **Statement type support**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE
- **Version flag**: `--version` to display version information

### Performance

- Streaming architecture handles files larger than available RAM
- Zero-allocation parsing using byte slices and sync.Pool
- Pre-compiled regexes for 10-100x faster pattern matching
- Optimized 64KB buffers for CPU cache efficiency

### Documentation

- Comprehensive README with usage examples
- CLAUDE.md for AI assistant guidance
- Performance benchmarks and comparison with alternatives

## [Unreleased]

### Planned

- Compressed file support (gzip, bzip2)
- PostgreSQL COPY statement support
- Parallel parsing for very large files
