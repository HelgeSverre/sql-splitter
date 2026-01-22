# Benchmarks

Competitor benchmarking for sql-splitter using Docker for reproducible results.

## Tools Tested

| Tool              | Language    | GitHub Stars | URL                                                                         | Notes                                 |
| ----------------- | ----------- | ------------ | --------------------------------------------------------------------------- | ------------------------------------- |
| **sql-splitter**  | Rust        | -            | [HelgeSverre/sql-splitter](https://github.com/HelgeSverre/sql-splitter)     | Multi-dialect, streaming I/O          |
| mysqldumpsplit    | Go          | ~40          | [afrase/mysqldumpsplit](https://github.com/afrase/mysqldumpsplit)           | Buffers in memory, has deadlock bug\* |
| mysqldumpsplitter | Bash/awk    | 540+         | [kedarvj/mysqldumpsplitter](https://github.com/kedarvj/mysqldumpsplitter)   | Most popular shell-based tool         |
| mysql_splitdump   | Bash/csplit | 93           | [jasny/mysql_splitdump.sh](https://gist.github.com/jasny/1608062)           | Uses GNU coreutils csplit             |
| mysqldumpsplit    | Node.js     | 55           | [vekexasia/mysqldumpsplit](https://github.com/vekexasia/mysqldumpsplit)     | Requires Node 10 (gulp 3.x)           |
| mysql-dump-split  | Ruby        | 77           | [ripienaar/mysql-dump-split](https://github.com/ripienaar/mysql-dump-split) | Archived project                      |

_\*Original Go tool has a deadlock bug with non-interleaved dumps; benchmarks use a [patched fork](https://github.com/HelgeSverre/mysqldumpsplit/tree/fix/handle-non-interleaved-dumps)._

## How to Run Benchmarks

Benchmarks run inside Docker for reproducibility across different machines.

### Quick Start

```bash
# Build the Docker image (first time only)
make docker-build

# Run benchmark with generated 100MB test file
make docker-bench

# Run with custom size (e.g., 200MB)
./docker/run-benchmark.sh --generate 200
```

### Options

| Flag              | Description                            |
| ----------------- | -------------------------------------- |
| `--generate SIZE` | Generate test data of SIZE MB          |
| `--runs N`        | Number of benchmark runs (default: 3)  |
| `--warmup N`      | Warmup runs before timing (default: 1) |
| `--export FILE`   | Export results to markdown file        |
| `--list`          | Show installed tools                   |
| `--test`          | Test which tools work with file        |

---

## Results - 2024-12-31

> **Hardware:** Apple M2 Max, 32GB RAM, Docker Desktop (linux/arm64)

### 100MB Generated Test File

| Tool                     |   Mean |   σ | Throughput | Relative         |
| :----------------------- | -----: | --: | ---------: | :--------------- |
| mysqldumpsplit (Go)\*    | 145 ms | ±10 |   712 MB/s | 1.00 (fastest)   |
| **sql-splitter (Rust)**  | 193 ms |  ±7 |   535 MB/s | 1.33x slower     |
| mysql_splitdump (csplit) | 215 ms |  ±6 |   481 MB/s | 1.48x slower     |
| mysqldumpsplit (Node.js) | 376 ms | ±14 |   275 MB/s | 2.59x slower     |
| mysql-dump-split (Ruby)  | 838 ms |  ±7 |   123 MB/s | **5.77x slower** |
| mysqldumpsplitter (Bash) | 863 ms | ±27 |   120 MB/s | **5.94x slower** |

_\*Go tool uses patched fork; original has deadlock bug._

### 1GB Generated Test File

| Tool                     |  Mean |     σ | Throughput | Relative         |
| :----------------------- | ----: | ----: | ---------: | :--------------- |
| mysqldumpsplit (Go)\*    | 1.29s | ±0.02 |   802 MB/s | 1.00 (fastest)   |
| **sql-splitter (Rust)**  | 1.84s | ±0.07 |   563 MB/s | 1.42x slower     |
| mysql_splitdump (csplit) | 1.85s | ±0.02 |   558 MB/s | 1.44x slower     |
| mysqldumpsplit (Node.js) | 2.72s | ±0.01 |   381 MB/s | 2.11x slower     |
| mysqldumpsplitter (Bash) | 8.81s | ±0.02 |   118 MB/s | **6.82x slower** |
| mysql-dump-split (Ruby)  | 9.05s | ±0.31 |   114 MB/s | **7.01x slower** |

### 5GB Stress Test (single run)

| Tool                     |      Time |   Throughput | Relative           |
| :----------------------- | --------: | -----------: | :----------------- |
| **sql-splitter (Rust)**  | **18.4s** | **283 MB/s** | **1.00 (fastest)** |
| mysqldumpsplit (Go)\*    |     27.1s |     191 MB/s | 1.47x slower       |
| mysqldumpsplit (Node.js) |     28.7s |     181 MB/s | 1.56x slower       |
| mysqldumpsplitter (Bash) |     55.5s |      94 MB/s | 3.02x slower       |
| mysql_splitdump (csplit) |     82.5s |      63 MB/s | 4.48x slower       |
| mysql-dump-split (Ruby)  |      103s |      50 MB/s | 5.60x slower       |

**At 5GB, sql-splitter becomes the fastest tool** because the Go tool's memory-buffering strategy causes significant slowdown under memory pressure.

---

## Findings

### Speed vs Memory Tradeoffs

- **Go tool is fastest on small files** but buffers the entire file in memory. At 5GB+, it becomes slower than sql-splitter due to memory pressure.
- **sql-splitter (Rust)** uses streaming I/O with fixed ~10-15MB memory regardless of file size. ~33% slower than Go at 100MB, but **47% faster at 5GB**.
- **csplit** is surprisingly fast for a shell tool, but relies on GNU coreutils (not available on stock macOS).
- **Node.js** is 2.5x slower than Go but still reasonable for JS-based workflows.
- **Ruby/Bash/awk** are 6x slower—fine for one-off use but not for automation.

### Format Compatibility (the real differentiator)

All competitors **only work with standard mysqldump format** that includes comment markers like:

```sql
-- Table structure for table `users`
```

**sql-splitter parses actual SQL statements** (`CREATE TABLE`, `INSERT INTO`, `COPY`), so it works with:

- TablePlus exports
- DBeaver exports
- pg_dump (PostgreSQL)
- sqlite3 .dump
- Any valid SQL file

Competitors produce **0 tables** on non-mysqldump files.

### Known Limitations

| Tool                | Issue                                                                         |
| ------------------- | ----------------------------------------------------------------------------- |
| Go (afrase)         | Deadlocks on files where all INSERTs for one table come before the next table |
| Node.js (vekexasia) | Requires Node 10 (gulp 3.x incompatible with Node 12+)                        |
| Ruby                | Project archived, unmaintained                                                |
| Bash/awk            | Slow, Unix-only                                                               |
| csplit              | Requires GNU coreutils                                                        |
| sql-splitter        | ~33% slower than memory-buffered Go                                           |

### When to use sql-splitter

1. Non-mysqldump formats (TablePlus, DBeaver, pg_dump, sqlite)
2. Large files (>1GB) where memory matters
3. CI/CD pipelines needing consistent behavior
4. Multi-dialect projects (MySQL + PostgreSQL + SQLite)
