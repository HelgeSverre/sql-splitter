# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

High-performance CLI tool written in Go for splitting large SQL dump files into individual table files. Built for extreme performance with efficient memory streaming, buffered I/O, and concurrent processing. Rewritten from PHP (Laravel Zero) to Go for 10x performance improvement.

## Available Commands

### Building and Running
```bash
# Build the application
go build -o sql-splitter .
# or
make build

# Run tests
go test ./...
# or
make test

# Run benchmarks
go test -bench=. -benchmem ./...
# or
make bench
```

### Main Commands
```bash
# Split a SQL file into individual table files
./sql-splitter split large-dump.sql --output=tables

# Analyze a SQL file to gather statistics
./sql-splitter analyze database.sql --progress

# Get help
./sql-splitter --help
./sql-splitter split --help
```

### Development Commands
```bash
# Run tests with coverage
make test-cover

# Run benchmarks with CPU profiling
make bench-cpu

# Format code
make fmt

# Clean build artifacts
make clean
```

## Architecture

### High-Level Design
```
Reader (bufio) → Parser (Streaming) → Writer Pool (Concurrent) → Table Files
   1-4MB Buffer    Statement Buffer       256KB Buffers per table
```

### Key Components

#### `internal/parser/` - Streaming SQL Parser
- **Adaptive buffering**: 64KB-4MB based on file size
- **String-aware parsing**: Tracks escaped quotes and multi-line strings
- **Zero-allocation**: Works with `[]byte`, uses `sync.Pool` for buffers
- **Precompiled regexes**: CREATE TABLE, INSERT INTO patterns
- **Statement detection**: Finds semicolons outside string literals

Key functions:
- `ReadStatement()`: Reads complete SQL statement (handles strings, escaping)
- `ParseStatement()`: Identifies statement type and extracts table name
- `DetermineBufferSize()`: Selects optimal buffer size based on file size

#### `internal/writer/` - Buffered File Writers
- **Writer pool**: Thread-safe map of table writers
- **Statement buffering**: Default 100 statements before flush
- **Configurable buffers**: 256KB bufio.Writer per table
- **Concurrent-safe**: Uses `sync.Mutex` for writes

Key components:
- `TableWriter`: Manages buffered writes to single table file
- `WriterPool`: Thread-safe pool of table writers

#### `internal/splitter/` - Orchestration
- Coordinates parsing and writing
- Maintains processing statistics
- Routes statements to appropriate table writers

#### `internal/analyzer/` - Statistical Analysis
- Counts INSERTs, CREATE TABLEs per table
- Calculates total bytes per table
- Optional progress tracking
- Sorts results by INSERT count

### Parsing Strategy

The parser uses character-by-character streaming with state tracking:
1. Track string boundaries (single/double quotes)
2. Track escape sequences (backslash)
3. Detect statement terminators (semicolon outside strings)
4. Extract table names using precompiled regexes

Performance optimizations:
- Reuse buffers via `sync.Pool`
- Work with `[]byte` (avoid string conversions)
- Fast-path byte prefix checks before regex matching
- Adaptive buffer sizing based on file size

## Performance Characteristics

### Throughput
- Small files (<100MB): 200-500 MB/s
- Large files (>1GB): 500-1000 MB/s
- Memory usage: 50-200MB regardless of file size

### Benchmark Results (Apple M2 Max)
```
BenchmarkParser_ReadStatement        227.61 MB/s    600KB alloc
BenchmarkParser_ParseStatement       323.5 ns/op    77B alloc
BenchmarkParser_StringVsBytes
  - BytesContains                    16.51 ns/op    0 allocs
  - StringContains                   42.70 ns/op    1 alloc
  - BytesHasPrefix                   3.986 ns/op    0 allocs
```

### Key Optimizations
1. **Buffered I/O**: 256KB-4MB adaptive buffers reduce syscalls
2. **Zero-allocation parsing**: Work with `[]byte` throughout
3. **Buffer pooling**: `sync.Pool` reduces GC pressure
4. **Precompiled regexes**: 10-100x faster than dynamic compilation
5. **Writer pool**: Concurrent table writing with per-table buffers
6. **Fast-path checks**: Byte prefix matching before regex

## Testing

### Running Tests
```bash
# All tests
go test ./...

# With coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Specific package
go test -v ./internal/parser
```

### Benchmarking
```bash
# All benchmarks
go test -bench=. -benchmem ./...

# Specific benchmark
go test -bench=BenchmarkParser_ReadStatement -benchmem ./internal/parser

# With profiling
go test -bench=BenchmarkParser_ReadStatement -cpuprofile=cpu.prof ./internal/parser
go tool pprof cpu.prof
```

## Key Implementation Details

- **Language**: Go 1.21+
- **CLI Framework**: Cobra
- **Buffer management**: `bufio.Reader` (not Scanner - no token limits)
- **Concurrency**: Writer pool with goroutine-safe access
- **Memory**: Constant usage via streaming (no file loaded to memory)
- **Regex patterns**: Case-insensitive, handles backticks and spaces
- **Statement types**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE

### Adding New Statement Types

1. Add constant in `internal/parser/parser.go` (e.g., `UpdateTable`)
2. Add regex pattern and fast-path check in `ParseStatement()`
3. Add test cases in `internal/parser/parser_test.go`

### Performance Tuning

Buffer sizes are automatically selected:
- File < 10MB: 64KB read buffer
- File < 100MB: 256KB read buffer
- File < 1GB: 1MB read buffer
- File > 1GB: 4MB read buffer

All tables use 256KB write buffers with 100-statement buffering.

## Comparison with PHP Version

| Metric           | PHP (Laravel Zero) | Go          | Improvement |
|------------------|--------------------|-------------|-------------|
| Throughput (1GB) | ~50 MB/s           | ~500 MB/s   | 10x         |
| Memory Usage     | ~300MB             | ~100MB      | 3x          |
| Cold Start       | ~500ms             | ~10ms       | 50x         |
| Binary Size      | N/A (interpreted)  | ~8MB        | Standalone  |
