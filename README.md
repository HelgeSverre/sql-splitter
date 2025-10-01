# SQL Splitter - High-Performance SQL File Splitter

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A blazingly fast CLI tool written in Go for splitting large SQL dump files into individual table files. Designed for
extreme performance with efficient memory streaming, buffered I/O, and concurrent processing.

## Features

- **⚡ High Performance**: 300-400 MB/s typical throughput (peaks at 411 MB/s)
- **💾 Memory Efficient**: Processes gigabyte-scale files with ~50-200MB memory usage
- **🔄 Streaming Architecture**: Handles files larger than available RAM
- **📊 Statistical Analysis**: Analyze SQL files to gather table statistics
- **🎯 Smart Buffering**: Auto-adjusts buffer sizes based on file size
- **🧵 Concurrent Processing**: Efficient multi-table writing with writer pools
- **🛡️ Safe Parsing**: Correctly handles strings with escaped characters and multi-line statements

## Performance

Rewritten from PHP to Go with extensive profiler-guided optimizations:

### Benchmark Results (Apple M2 Max)

- **Parser Throughput**: 327 MB/s typical, 411 MB/s with optimal buffers (up from 50 MB/s PHP)
- **Table Extraction**: 65 ns/op (4.9x faster than regex-only approach)
- **Memory Usage**: ~100 MB constant regardless of file size
- **Overall Improvement**: **6.5x faster than original PHP** (50 MB/s → 327 MB/s)

### Key Optimizations

- **Batched I/O**: Chunk-based reading with `Peek()` + `Discard()` (19% faster)
- **Manual parsing**: Byte scanning for table names with regex fallback (4.9x faster)
- **Pre-compiled regexes**: All patterns compiled at initialization (eliminates hot-path overhead)
- **Buffer pooling**: 32KB `sync.Pool` buffers matching typical statement sizes
- **Optimized buffer sizes**: 64KB buffers proven optimal for CPU cache (411 MB/s in tests)
- **Concurrent writers**: Lock-free writer pool with per-table buffering

## Installation

### From Source

```bash
go install github.com/helgesverre/sql-splitter@latest
```

### Build Locally

```bash
git clone https://github.com/helgesverre/sql-splitter.git
cd sql-splitter
go build -o sql-splitter
```

## Usage

### Split Command

Split a large SQL file into individual table files:

```bash
# Basic usage
sql-splitter split large-dump.sql

# Specify output directory
sql-splitter split database.sql --output=tables

# Verbose output
sql-splitter split database.sql -o tables -v
```

**Output:**

```
Splitting SQL file: database.sql (1250.50 MB)
Output directory: tables

✓ Split completed successfully!

Statistics:
  Statements processed: 125000
  Bytes processed: 1250.50 MB
  Elapsed time: 2.5s
  Throughput: 500.20 MB/s
```

**Generated Files:**

```
tables/
├── users.sql
├── posts.sql
├── comments.sql
├── orders.sql
└── ...
```

### Analyze Command

Analyze a SQL file and display statistics:

```bash
# Basic analysis
sql-splitter analyze large-dump.sql

# With progress indicator
sql-splitter analyze database.sql --progress
```

**Output:**

```
Analyzing SQL file: database.sql (1250.50 MB)

✓ Analysis completed in 2.3s

Found 25 tables:

Table Name                                  INSERTs   Total Stmts     Size (MB)
─────────────────────────────────────────────────────────────────────────────────
users                                         50000         50001       500.25
posts                                         30000         30001       300.15
comments                                      25000         25001       250.10
orders                                        15000         15001       150.05
products                                       5000          5001        50.02
...
─────────────────────────────────────────────────────────────────────────────────
TOTAL                                        125000             -      1250.57
```

## Architecture

### High-Level Design

```
┌─────────────┐      ┌─────────────┐      ┌──────────────┐
│   Reader    │─────▶│   Parser    │─────▶│ Writer Pool  │
│  (bufio)    │      │  (Streaming)│      │ (Concurrent) │
└─────────────┘      └─────────────┘      └──────────────┘
     │                     │                      │
     │                     │                      ▼
     │                     │               ┌─────────────┐
     │                     │               │ Table Files │
     │                     │               └─────────────┘
  1-4MB Buffer      Statement Buffer       256KB Buffers
```

### Key Components

#### Parser (`internal/parser/`)

- Streaming SQL statement parser
- Handles strings with escaped characters
- Detects statement boundaries (semicolons outside strings)
- Adaptive buffer sizing based on file size
- Reuses buffers via `sync.Pool`

#### Writer (`internal/writer/`)

- Buffered file writers with configurable buffer sizes
- Per-table writer pool for concurrent access
- Statement-level buffering (default: 100 statements)
- Thread-safe with `sync.Mutex`

#### Splitter (`internal/splitter/`)

- Orchestrates parsing and writing
- Maintains statistics during processing
- Routes statements to appropriate table files

#### Analyzer (`internal/analyzer/`)

- Gathers statistics without writing files
- Counts INSERTs, CREATE TABLEs, and total bytes per table
- Optional progress tracking

## Performance Tuning

### Buffer Sizes

The tool uses performance-optimized buffer sizes based on extensive benchmarking:

| File Size | Read Buffer | Write Buffer | Rationale                                |
| --------- | ----------- | ------------ | ---------------------------------------- |
| < 1GB     | 64KB        | 256KB        | Optimal CPU cache utilization (411 MB/s) |
| > 1GB     | 256KB       | 256KB        | Better for very large files              |

_64KB buffers consistently outperform larger sizes due to L1/L2 cache hits_

### Memory Management

- Statement buffers pooled via `sync.Pool` (32KB capacity)
- Buffers released after processing with automatic pool management
- Only 0.7% of allocations are buffer-related (highly efficient)
- Constant ~100MB memory usage regardless of file size

### Concurrency

Current implementation uses:

- Single-threaded parsing (I/O bound)
- Concurrent writers via writer pool
- Lock-free writer retrieval (read-lock fast path)

## Benchmarking

Run benchmarks:

```bash
# Run all benchmarks
go test -bench=. -benchmem ./...

# Parser benchmarks
go test -bench=BenchmarkParser -benchmem ./internal/parser

# Writer benchmarks
go test -bench=BenchmarkTableWriter -benchmem ./internal/writer

# CPU profile
go test -bench=BenchmarkParser_ReadStatement -cpuprofile=cpu.prof ./internal/parser
go tool pprof cpu.prof

# Memory profile
go test -bench=BenchmarkParser_ReadStatement -memprofile=mem.prof ./internal/parser
go tool pprof mem.prof
```

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Development

### Project Structure

```
sql-splitter/
├── cmd/                    # CLI commands
│   ├── root.go            # Root command
│   ├── split.go           # Split command
│   └── analyze.go         # Analyze command
├── internal/
│   ├── parser/            # SQL parsing logic
│   │   ├── parser.go
│   │   └── parser_test.go
│   ├── writer/            # Buffered file writing
│   │   ├── writer.go
│   │   └── writer_test.go
│   ├── splitter/          # Split orchestration
│   │   └── splitter.go
│   └── analyzer/          # Analysis logic
│       └── analyzer.go
├── main.go                # Application entry point
├── go.mod
└── README.md
```

### Adding New Statement Types

To support additional SQL statements:

1. Add new `StatementType` constant in `internal/parser/parser.go`
2. Add regex pattern to `ParseStatement()` method
3. Update tests in `parser_test.go`

## Competitive Comparison

### Similar Tools

| Tool                                                                                    | Language | Throughput       | Memory  | Repository |
| --------------------------------------------------------------------------------------- | -------- | ---------------- | ------- | ---------- |
| [afrase/mysqldumpsplit](https://github.com/afrase/mysqldumpsplit)                       | Go       | **533 MB/s**     | <20MB   | GitHub     |
| **sql-splitter (this tool)**                                                            | Go       | **327-411 MB/s** | ~100MB  | -          |
| [Bash csplit scripts](https://gist.github.com/jasny/1608062)                            | Bash     | ~250 MB/s        | Unknown | Gist       |
| [PHP script (thatbytes)](https://thatbytes.co.uk/posts/mysql-dump-splitter-benchmarks/) | PHP      | 149 MB/s         | Unknown | Blog       |
| [vekexasia/mysqldumpsplit](https://github.com/vekexasia/mysqldumpsplit)                 | Node.js  | 133 MB/s         | <50MB   | GitHub     |
| [kedarvj/mysqldumpsplitter](https://github.com/kedarvj/mysqldumpsplitter)               | Bash     | Unknown          | Unknown | GitHub     |
| [SqlDumpSplittr](https://sqldumpsplitter.net/)                                          | Windows  | Unknown          | Unknown | Commercial |

_This tool ranks **#2** among open-source SQL file splitters_

### Comparison with PHP Version

| Metric            | PHP Version | Go Version | Improvement |
| ----------------- | ----------- | ---------- | ----------- |
| Parser Throughput | ~50 MB/s    | 327 MB/s   | **6.5x**    |
| Table Extraction  | ~5000 ns/op | 65 ns/op   | **75x**     |
| Memory Usage      | ~300MB      | ~100MB     | **3x**      |
| Cold Start        | ~500ms      | ~10ms      | **50x**     |
| Binary Size       | N/A (PHP)   | ~2.4MB     | Standalone  |

_Benchmarks performed on Apple M2 Max, macOS, Go 1.24_

## Known Limitations

- Does not handle nested strings or complex SQL expressions
- Assumes standard MySQL/PostgreSQL dump format
- No support for compressed input files (pipe through `zcat` if needed)
- Single-pass parsing (cannot backtrack)

## Roadmap

- [ ] Compressed file support (gzip, bzip2)
- [ ] PostgreSQL COPY statement support
- [ ] Parallel parsing for very large files
- [ ] Progress bar for split command
- [ ] Filter by table name (split only specific tables)
- [ ] Dry-run mode

## License

MIT License - see [LICENSE](LICENSE) for details.
