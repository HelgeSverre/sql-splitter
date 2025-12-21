# AGENTS.md

This file provides guidance when working with code in this repository.

## Project Overview

High-performance CLI tool written in Rust for splitting large SQL dump files into individual table files.

## Available Commands

### Makefile Commands

Run `make help` to see all available commands. Key commands:

| Command | Description |
|---------|-------------|
| `make build` | Debug build |
| `make release` | Release build |
| `make native` | Optimized build for current CPU (best performance) |
| `make test` | Run all tests |
| `make bench` | Run criterion benchmarks |
| `make profile` | Memory profile all commands (medium dataset) |
| `make profile-large` | Memory profile with large dataset (~250MB) |
| `make profile-mega` | Stress test profile (~2GB: 100 tables × 100k rows) |
| `make fmt` | Format code |
| `make check` | Check code without building |
| `make clippy` | Run clippy lints |
| `make clean` | Clean build artifacts |
| `make install` | Install locally (binary + shell completions) |
| `make verify-realworld` | Verify against real-world SQL dumps |
| `make website-deploy` | Deploy website to Vercel |
| `make docker-bench` | Run benchmarks in Docker (generates 100MB test data) |
| `make man` | Generate man pages in man/ directory |

### Building and Running

```bash
# Build (debug)
cargo build
# or: make build

# Build (release)
cargo build --release
# or: make release

# Build optimized for current CPU (best for benchmarking)
RUSTFLAGS="-C target-cpu=native" cargo build --release
# or: make native

# Run tests
cargo test
# or: make test

# Run benchmarks
cargo bench
# or: make bench

# Run clippy lints
cargo clippy -- -D warnings
# or: make clippy

# Format code
cargo fmt
# or: make fmt
```

### Main Commands

```bash
# Split a MySQL/MariaDB dump file (default)
./target/release/sql-splitter split large-dump.sql --output=tables

# Split a PostgreSQL pg_dump file
./target/release/sql-splitter split pg_dump.sql --output=tables --dialect=postgres

# Split a SQLite .dump file
./target/release/sql-splitter split sqlite.sql --output=tables --dialect=sqlite

# Analyze a SQL file to gather statistics
./target/release/sql-splitter analyze database.sql --progress

# Get help
./target/release/sql-splitter --help
./target/release/sql-splitter split --help
```

### Supported Dialects

| Dialect | Flag | Dump Tool | Key Features |
|---------|------|-----------|--------------|
| MySQL/MariaDB | `--dialect=mysql` (default) | mysqldump | Backtick quoting, backslash escapes |
| PostgreSQL | `--dialect=postgres` | pg_dump | Double-quote identifiers, COPY FROM stdin, dollar-quoting |
| SQLite | `--dialect=sqlite` | sqlite3 .dump | Double-quote identifiers |

## Architecture

### High-Level Design

```
BufReader (fill_buf) → Parser (Streaming) → WriterPool (BufWriter) → Table Files
    64KB Buffer          Statement Buffer       256KB Buffers per table
```

### Key Components

#### `src/parser/mod.rs` - Streaming SQL Parser

- Uses `BufReader::fill_buf()` + `consume()` pattern for streaming reads
- String-aware parsing: tracks escaped quotes and multi-line strings
- Manual table name extraction with regex fallback
- Pre-compiled static regexes via `once_cell::Lazy`

Key functions:
- `read_statement()`: Reads complete SQL statement (handles strings, escaping)
- `parse_statement()`: Identifies statement type and extracts table name
- `determine_buffer_size()`: Selects optimal buffer size based on file size

#### `src/writer/mod.rs` - Buffered File Writers

- `TableWriter`: Manages buffered writes to single table file
- `WriterPool`: HashMap of table writers using `ahash` for fast hashing
- 256KB `BufWriter` per table
- Auto-flush every 100 statements

#### `src/splitter/mod.rs` - Orchestration

- Coordinates parsing and writing
- Maintains processing statistics
- Routes statements to appropriate table writers
- Supports dry-run and table filtering

#### `src/analyzer/mod.rs` - Statistical Analysis

- Counts INSERTs, CREATE TABLEs per table
- Calculates total bytes per table
- Optional progress tracking
- Sorts results by INSERT count

## Performance Characteristics

### Key Optimizations

1. **Streaming I/O**: `fill_buf` + `consume` pattern for zero-copy reading
2. **Manual parsing**: Byte-level table name extraction before regex fallback
3. **Fast hashing**: `ahash::AHashMap` instead of default SipHash
4. **Pre-compiled regexes**: Static initialization via `once_cell::Lazy`
5. **Minimal allocations**: Work with `&[u8]` slices in hot path
6. **Buffered writes**: 256KB buffers with periodic flush

### Buffer Sizes

- File < 1GB: 64KB read buffer (optimal for CPU cache)
- File > 1GB: 256KB read buffer
- All tables: 256KB write buffers with 100-statement buffering

## Testing

```bash
# All tests
cargo test

# Specific module
cargo test parser::tests

# With output
cargo test -- --nocapture
```

## Benchmarking

```bash
# All benchmarks
cargo bench

# Specific benchmark
cargo bench -- read_statement
```

## Memory Profiling

Use GNU time to measure peak memory usage (Maximum Resident Set Size) for commands.

### Prerequisites

```bash
# macOS: Install GNU time
brew install gnu-time

# Linux: GNU time is typically at /usr/bin/time
```

### Quick Profiling

```bash
# Profile a single command with gtime (macOS) or /usr/bin/time (Linux)
gtime -v ./target/release/sql-splitter sample input.sql --percent 10 --output out.sql 2>&1 | grep "Maximum resident set size"

# Full metrics
gtime -v ./target/release/sql-splitter validate large-dump.sql --check-fk 2>&1 | tail -25
```

### Automated Profiling Script

Use the profiling script to consistently benchmark all commands:

```bash
# Profile all commands with medium-sized generated test data
./scripts/profile-memory.sh

# Profile with larger test data
./scripts/profile-memory.sh --size large

# Profile with a specific file
./scripts/profile-memory.sh --file /path/to/dump.sql

# Only generate test fixtures (don't run profiling)
./scripts/profile-memory.sh --generate-only --size xlarge
```

Size configurations:
| Size | Rows/Table | Tables | Approx File Size |
|------|------------|--------|------------------|
| tiny | 500 | 10 | ~0.5MB |
| small | 2,500 | 10 | ~2.5MB |
| medium | 25,000 | 10 | ~25MB |
| large | 125,000 | 10 | ~125MB |
| xlarge | 250,000 | 10 | ~250MB |
| huge | 500,000 | 10 | ~500MB |
| mega | 100,000 | 100 | ~1GB |
| giga | 1,000,000 | 100 | ~10GB (MySQL only) |

### Key Metrics

From GNU time output:
- **Maximum resident set size (kbytes)**: Peak memory usage
- **Elapsed (wall clock) time**: Total execution time
- **User time (seconds)**: CPU time in user mode

### Example Output

```
Command      Dialect    File Size    Peak RSS    Wall Time   Extra Args
------------------------------------------------------------
analyze      mysql        2.05 MB      6.50 MB     0:00.05
split        mysql        2.05 MB      7.20 MB     0:00.08
validate     mysql        2.05 MB     12.30 MB     0:00.15   --check-fk
sample       mysql        2.05 MB      8.10 MB     0:00.12
sample       mysql        2.05 MB      8.50 MB     0:00.14   --preserve-relations
```

### Memory Optimization Guidelines

When optimizing for memory:
1. Use streaming/chunked processing instead of loading all data
2. Use hash-based sets (`PkHashSet` with 64-bit hashes) instead of storing full values
3. Write intermediate results to temp files instead of accumulating in memory
4. Process tables sequentially in dependency order

## Key Implementation Details

- **Language**: Rust 2021 edition
- **CLI Framework**: clap v4 with derive macros
- **Regex**: `regex` crate with bytes API
- **HashMap**: `ahash::AHashMap` for performance
- **Buffer management**: `std::io::{BufReader, BufWriter}`
- **Statement types**: CREATE TABLE, INSERT INTO, CREATE INDEX, ALTER TABLE, DROP TABLE, COPY (PostgreSQL)

## Release Process

Follow these steps to create a new release. **Both a git tag AND a GitHub release are required.**

### 1. Pre-release Checks

```bash
# Ensure all tests pass
cargo test

# Ensure it builds in release mode
cargo build --release

# Run lints
cargo clippy

# Verify formatting
cargo fmt --check

# Optional: Run Docker benchmarks to verify performance
./docker/run-benchmark.sh --generate 50
```

### 2. Update Version

1. Update version in `Cargo.toml`:
   ```toml
   version = "X.Y.Z"
   ```

2. Update `CHANGELOG.md`:
   - Add new version section with today's date: `## [X.Y.Z] - YYYY-MM-DD`
   - Move items from `[Unreleased]` to the new version section
   - Document all notable changes under Added/Changed/Fixed/Removed

### 3. Commit, Tag, and Push

```bash
# Stage all release files
git add Cargo.toml Cargo.lock CHANGELOG.md src/ docs/

# Commit with descriptive message
git commit -m "feat: <brief description> (vX.Y.Z)"

# Create annotated tag
git tag -a vX.Y.Z -m "Release vX.Y.Z

<Brief summary of changes>"

# Push commit and tag together
git push origin main --tags
```

### 4. Create GitHub Release (REQUIRED)

**Always create a GitHub release** - this makes the release visible on the releases page and generates release notes.

```bash
# Extract release notes for this version from CHANGELOG.md and create release
gh release create vX.Y.Z \
  --title "vX.Y.Z" \
  --notes "$(sed -n '/## \[X.Y.Z\]/,/## \[/p' CHANGELOG.md | head -n -1)" \
  --latest
```

Or manually at: https://github.com/HelgeSverre/sql-splitter/releases/new
- Select the tag `vX.Y.Z`
- Title: `vX.Y.Z`
- Description: Copy relevant section from CHANGELOG.md

### 5. Automatic crates.io Publish

**crates.io publishing is automatic** when you push a new tag. The GitHub Action workflow handles this.

**Setup requirement**: Add the `CARGO_REGISTRY_TOKEN` secret to your GitHub repository:
1. Go to https://crates.io/settings/tokens and create a new token
2. Go to GitHub repo → Settings → Secrets and variables → Actions
3. Add new secret: `CARGO_REGISTRY_TOKEN` with your crates.io token

Manual publish (if needed):
```bash
cargo publish --dry-run  # Test first
cargo publish            # Publish
```

### 6. Post-release Verification

- [ ] GitHub release visible: https://github.com/HelgeSverre/sql-splitter/releases
- [ ] Tag visible: `git tag -l | grep vX.Y.Z`
- [ ] crates.io updated (if published): https://crates.io/crates/sql-splitter
- [ ] Website auto-deployed via Vercel (if applicable)

### Versioning Guidelines

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (X.0.0): Breaking changes to CLI interface or output format
- **MINOR** (0.X.0): New features, new dialects, new commands
- **PATCH** (0.0.X): Bug fixes, performance improvements, documentation

### Quick Release Checklist

```
[ ] cargo test passes
[ ] cargo clippy clean
[ ] Version bumped in Cargo.toml
[ ] CHANGELOG.md updated
[ ] git commit + tag created
[ ] git push origin main --tags
[ ] gh release create vX.Y.Z (REQUIRED!)
[ ] Verify release at github.com/HelgeSverre/sql-splitter/releases
```

## Website llms.txt Maintenance

The file `website/llms.txt` provides LLM-friendly documentation following the [llmstxt.org](https://llmstxt.org) specification. This file helps AI tools understand how to use and install sql-splitter.

### When to Update llms.txt

Update `website/llms.txt` when:
- Adding new CLI commands or subcommands
- Adding/changing command-line flags or options
- Adding support for new SQL dialects
- Adding support for new compression formats
- Changing installation methods
- Updating performance benchmarks significantly
- Adding new major features

### llms.txt Format Requirements

The file must follow this structure (in order):
1. **H1 header**: Project name (`# sql-splitter`)
2. **Blockquote**: Brief summary with key capabilities
3. **Body sections**: Detailed info (no H2 headers yet)
4. **H2 sections**: File lists with URLs to documentation/source

Key guidelines:
- Keep content concise and actionable for LLMs
- Include complete CLI examples with common flags
- Document all supported options in tables
- Link to GitHub source files and documentation
- Use the "Optional" H2 section for secondary resources

### Example Update

When adding a new `--format` flag:

```markdown
## Commands

### split
...
Options:
- `--format <FORMAT>`: Output format: sql, json (default: sql)  # ADD THIS
...
```

## Agent Skills

The file `skills/sql-splitter/SKILL.md` provides a skill definition following the [Agent Skills](https://agentskills.io) specification. This enables AI coding assistants to automatically discover and use sql-splitter.

### Supported Tools

Agent Skills are supported by: Amp, Claude Code, VS Code / GitHub Copilot, Cursor, Goose, Letta, and OpenCode.

### When to Update SKILL.md

Update `skills/sql-splitter/SKILL.md` when:
- Adding new CLI commands
- Changing command patterns or workflows
- Adding new flags that affect common usage patterns
- Updating decision logic for when to use commands

### SKILL.md Format Requirements

The file follows the Agent Skills specification:
1. **YAML frontmatter**: name, description, license, compatibility
2. **Markdown body**: Step-by-step instructions, patterns, and examples

Key guidelines:
- Focus on **when to use** vs **when not to use**
- Provide step-by-step patterns for common workflows
- Include error handling guidance
- Keep instructions actionable and concise

### Installing the Skill

**Amp:**
```bash
amp skill add helgesverre/sql-splitter
```

**Claude Code:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter ~/.claude/skills/
```

**VS Code / GitHub Copilot:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter .github/skills/
```

**Cursor:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter .cursor/skills/
```

**Goose:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter ~/.config/goose/skills/
```

**Letta:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter .skills/
```

**OpenCode:**
```bash
git clone https://github.com/helgesverre/sql-splitter.git /tmp/sql-splitter
cp -r /tmp/sql-splitter/skills/sql-splitter ~/.opencode/skills/
```

**Universal Installer (via npx):**
```bash
npx ai-agent-skills install sql-splitter --agent <agent>
# Supported agents: claude, cursor, amp, vscode, goose, opencode
```

### Skill Directory Structure

```
sql-splitter/
└── skills/
    └── sql-splitter/
        └── SKILL.md
```
