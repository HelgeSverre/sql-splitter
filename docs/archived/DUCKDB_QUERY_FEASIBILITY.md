# DuckDB Query Feature Feasibility Study

**Date**: 2025-12-26  
**Version Target**: v1.12.0  
**Status**: Investigation Complete  
**Estimated Effort**: 20-24 hours

---

## Executive Summary

**Verdict: ✅ HIGHLY FEASIBLE**

Integrating DuckDB as an embedded query engine for sql-splitter is technically straightforward and strategically valuable. The `duckdb` Rust crate provides excellent bindings with a `bundled` feature that compiles DuckDB directly into the binary—no external dependencies required.

### Key Findings

| Aspect                 | Assessment           | Notes                                             |
| ---------------------- | -------------------- | ------------------------------------------------- |
| **Licensing**          | ✅ MIT License       | Fully compatible with sql-splitter's MIT license  |
| **Rust Bindings**      | ✅ Excellent         | Official crate, 811 GitHub stars, 1.1M+ downloads |
| **Embedding**          | ✅ Zero dependencies | `bundled` feature compiles DuckDB into binary     |
| **Binary Size Impact** | ⚠️ +15-25 MB         | Significant but acceptable for the value          |
| **Build Time Impact**  | ⚠️ +2-5 minutes      | First build only, subsequent builds cached        |
| **Code Reuse**         | ✅ Significant       | 60-70% of convert infra can be reused             |

---

## 1. DuckDB Rust Bindings (duckdb-rs)

### Crate Information

```toml
# Cargo.toml addition
[dependencies]
duckdb = { version = "1.4.3", features = ["bundled"] }
```

- **Latest Version**: 1.4.3 (matches DuckDB 1.4.3 core)
- **Crate Size**: 2.73 MiB
- **Total Downloads**: 1,115,690+
- **License**: MIT
- **Maintainers**: Official DuckDB Foundation

### API Overview

The API is ergonomic and inspired by `rusqlite`:

```rust
use duckdb::{Connection, params, Result};

fn query_dump() -> Result<()> {
    // In-memory database (ephemeral)
    let conn = Connection::open_in_memory()?;

    // Or persistent file
    let conn = Connection::open("/path/to/cache.duckdb")?;

    // Execute DDL
    conn.execute("CREATE TABLE users (id INT, name TEXT)", [])?;

    // Bulk insert via Appender (fast!)
    let mut appender = conn.appender("users")?;
    appender.append_rows([[1, "Alice"], [2, "Bob"]])?;

    // Query
    let mut stmt = conn.prepare("SELECT * FROM users WHERE id > ?")?;
    let rows = stmt.query_map(params![0], |row| {
        Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
    })?;

    for row in rows {
        println!("{:?}", row?);
    }

    Ok(())
}
```

### Feature Flags We Need

| Feature   | Purpose                    | Impact                                  |
| --------- | -------------------------- | --------------------------------------- |
| `bundled` | Compile DuckDB from source | Required for zero-dependency deployment |
| `json`    | JSON output format         | Nice to have                            |
| `parquet` | Parquet export (future)    | For v1.12.x Parquet export              |

---

## 2. Licensing Analysis

### DuckDB License: MIT

```
MIT License

Copyright 2021-2025 Stichting DuckDB Foundation
```

**Implications for sql-splitter:**

| Requirement                        | Satisfied?                       |
| ---------------------------------- | -------------------------------- |
| Include copyright notice in binary | ✅ Yes (in LICENSE or --version) |
| Include license text in docs       | ✅ Yes                           |
| Commercial use allowed             | ✅ Yes                           |
| Modification allowed               | ✅ Yes                           |
| Distribution allowed               | ✅ Yes                           |
| No liability clause                | ✅ Acceptable                    |

**Action Required**: Add DuckDB attribution to LICENSE file or `--version` output.

---

## 3. Integration Architecture

### Architecture: Ephemeral by Default + Optional Cache

```
┌─────────────────────────────────────────────────────────────────────┐
│                        sql-splitter query                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────┐     ┌─────────────────┐     ┌──────────────────────┐  │
│  │dump.sql  │────▶│  SQL Parser     │────▶│  DuckDB Loader       │  │
│  │(any      │     │  (existing)     │     │                      │  │
│  │dialect)  │     │                 │     │  - Convert to DuckDB │  │
│  └──────────┘     │  StatementType  │     │  - CREATE TABLEs     │  │
│                   │  + table name   │     │  - INSERT/Appender   │  │
│                   └─────────────────┘     └──────────┬───────────┘  │
│                                                      │               │
│                                                      ▼               │
│                   ┌─────────────────┐     ┌──────────────────────┐  │
│                   │  User Query     │────▶│  DuckDB Engine       │  │
│                   │  (SELECT ...)   │     │                      │  │
│                   └─────────────────┘     │  - In-memory (def)   │  │
│                                           │  - Disk (--disk)     │  │
│                                           │  - Cache (--cache)   │  │
│                   ┌─────────────────┐     └──────────┬───────────┘  │
│                   │  Output         │◀───────────────┘               │
│                   │  - table/json   │                                │
│                   │  - csv/parquet  │                                │
│                   └─────────────────┘                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Operating Modes

| Mode          | Flag      | Storage    | Use Case                      |
| ------------- | --------- | ---------- | ----------------------------- |
| **In-memory** | (default) | RAM only   | Quick one-off queries         |
| **Disk**      | `--disk`  | Temp file  | Large dumps (>4GB)            |
| **Cached**    | `--cache` | Persistent | Repeated queries on same dump |

### Memory Management

DuckDB has excellent memory management:

- **Streaming execution**: Data processed chunk-by-chunk
- **Intermediate spilling**: Automatically spills to disk when memory exceeded
- **Configurable limits**: `SET memory_limit = '4GB'`

**Default strategy for sql-splitter:**

```rust
// Threshold: Use disk mode for files > 2GB
const DISK_MODE_THRESHOLD: u64 = 2 * 1024 * 1024 * 1024; // 2 GB

fn determine_mode(file_size: u64, explicit_mode: Option<Mode>) -> Mode {
    explicit_mode.unwrap_or_else(|| {
        if file_size > DISK_MODE_THRESHOLD {
            Mode::Disk
        } else {
            Mode::InMemory
        }
    })
}
```

---

## 4. Code Reuse Analysis

### Existing Infrastructure to Reuse

| Component         | Location                        | Reuse for DuckDB                       |
| ----------------- | ------------------------------- | -------------------------------------- |
| SQL Parser        | `src/parser/mod.rs`             | ✅ Statement parsing, table extraction |
| Dialect detection | `src/parser/mod.rs`             | ✅ Auto-detect source dialect          |
| COPY→INSERT       | `src/convert/copy_to_insert.rs` | ✅ PostgreSQL COPY handling            |
| Type mapping      | `src/convert/types.rs`          | ✅ Convert MySQL/PG types to DuckDB    |
| Progress tracking | `src/progress.rs`               | ✅ Import progress bar                 |
| Compression       | `src/splitter/mod.rs`           | ✅ Read .gz, .bz2, .xz, .zstd          |

### New Code Required

| Component        | Effort | Description                         |
| ---------------- | ------ | ----------------------------------- |
| DuckDB loader    | 6-8h   | Convert parsed statements to DuckDB |
| Query executor   | 3-4h   | Run user query, handle results      |
| Output formatter | 3-4h   | Table, JSON, CSV output             |
| CLI integration  | 2-3h   | Argument parsing, modes             |
| Cache manager    | 3-4h   | --cache flag implementation         |
| REPL mode        | 4-6h   | Interactive query session           |

**Total new code: ~20-24 hours** (down from original 28h estimate)

### Type Mapping: MySQL/PostgreSQL → DuckDB

DuckDB has broad type compatibility. We can leverage `src/convert/types.rs`:

```rust
// DuckDB-specific type mapping
fn to_duckdb_type(mysql_type: &str) -> &str {
    match mysql_type.to_uppercase().as_str() {
        // Numeric
        "TINYINT" => "TINYINT",
        "SMALLINT" => "SMALLINT",
        "INT" | "INTEGER" => "INTEGER",
        "BIGINT" => "BIGINT",
        "FLOAT" => "FLOAT",
        "DOUBLE" => "DOUBLE",
        "DECIMAL" | "NUMERIC" => "DECIMAL",

        // String
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => "VARCHAR",

        // Date/Time
        "DATE" => "DATE",
        "TIME" => "TIME",
        "DATETIME" | "TIMESTAMP" => "TIMESTAMP",

        // Binary
        "BLOB" | "BINARY" | "VARBINARY" => "BLOB",

        // MySQL-specific
        "ENUM" => "VARCHAR",  // Warn: enum semantics lost
        "SET" => "VARCHAR",

        // PostgreSQL-specific
        "SERIAL" => "INTEGER",
        "BIGSERIAL" => "BIGINT",
        "BYTEA" => "BLOB",

        _ => mysql_type, // Pass through unknown types
    }
}
```

---

## 5. Import Strategy

### Option A: Statement-by-Statement (Simple)

```rust
fn import_dump(conn: &Connection, dump_path: &Path) -> Result<ImportStats> {
    let mut parser = Parser::new(dump_path)?;
    let mut stats = ImportStats::default();

    while let Some(stmt) = parser.next_statement()? {
        let (stmt_type, table) = Parser::parse_statement(&stmt);

        match stmt_type {
            StatementType::CreateTable => {
                let duckdb_sql = convert_create_table(&stmt)?;
                conn.execute(&duckdb_sql, [])?;
                stats.tables_created += 1;
            }
            StatementType::Insert => {
                let duckdb_sql = convert_insert(&stmt)?;
                conn.execute(&duckdb_sql, [])?;
                stats.rows_inserted += count_rows(&stmt);
            }
            StatementType::Copy => {
                // Convert COPY to INSERTs using existing copy_to_inserts()
                let (header, data) = parse_copy_block(&stmt)?;
                let inserts = copy_to_inserts(&header, &data, SqlDialect::Postgres);
                for insert in inserts {
                    conn.execute(&String::from_utf8_lossy(&insert), [])?;
                }
                stats.rows_inserted += count_copy_rows(&data);
            }
            _ => {} // Skip other statements
        }
    }

    Ok(stats)
}
```

### Option B: Appender API (Fast)

For bulk data loading, DuckDB's Appender API is significantly faster:

```rust
fn import_with_appender(conn: &Connection, table: &str, rows: Vec<Vec<Value>>) -> Result<()> {
    let mut appender = conn.appender(table)?;

    for row in rows {
        appender.append_row(row.iter().map(|v| v.as_duckdb_value()))?;
    }

    appender.flush()?;
    Ok(())
}
```

**Recommendation**: Use statement-by-statement for CREATE TABLE, Appender for data.

---

## 6. Binary Size Impact

### Current sql-splitter Binary

```bash
$ ls -lh target/release/sql-splitter
-rwxr-xr-x  1 helge  staff  4.2M Dec 26 10:00 sql-splitter
```

### Estimated Size with DuckDB

Based on libduckdb-sys and similar projects:

| Component               | Size Impact  |
| ----------------------- | ------------ |
| DuckDB core (bundled)   | +15-20 MB    |
| Rust bindings           | +1-2 MB      |
| Extensions (if enabled) | +2-5 MB each |

**Projected binary size: ~22-30 MB**

### Mitigation Strategies

1. **Optional feature flag**: Make DuckDB opt-in via Cargo feature
2. **Separate binary**: Ship `sql-splitter-query` as separate binary
3. **Dynamic linking**: Use system DuckDB if available

**Recommendation**: Start with bundled (simplicity), consider optional feature later.

---

## 7. Cache Strategy

### Cache Location

```
~/.cache/sql-splitter/duckdb/
├── <hash1>.duckdb   # Cached database for dump1.sql
├── <hash2>.duckdb   # Cached database for dump2.sql
└── cache_index.json # Metadata: path → hash, mtime, size
```

### Cache Key Computation

```rust
fn compute_cache_key(dump_path: &Path) -> String {
    let metadata = fs::metadata(dump_path)?;
    let key_input = format!(
        "{}:{}:{}",
        dump_path.canonicalize()?.display(),
        metadata.len(),
        metadata.modified()?.duration_since(UNIX_EPOCH)?.as_secs()
    );

    let mut hasher = Sha256::new();
    hasher.update(key_input.as_bytes());
    hex::encode(hasher.finalize())
}
```

### Cache Invalidation

```rust
fn is_cache_valid(dump_path: &Path, cache_path: &Path) -> bool {
    let dump_mtime = fs::metadata(dump_path).ok()?.modified().ok()?;
    let cache_mtime = fs::metadata(cache_path).ok()?.modified().ok()?;

    cache_mtime > dump_mtime
}
```

---

## 8. CLI Design

### Basic Usage

```bash
# Single query
sql-splitter query dump.sql "SELECT COUNT(*) FROM users"

# Output formats
sql-splitter query dump.sql "SELECT * FROM users" --format table
sql-splitter query dump.sql "SELECT * FROM users" --format json
sql-splitter query dump.sql "SELECT * FROM users" --format csv

# Output to file
sql-splitter query dump.sql "SELECT * FROM users" -o results.csv

# Large dump mode
sql-splitter query huge.sql "SELECT ..." --disk

# Cached mode (fast repeated queries)
sql-splitter query dump.sql "SELECT ..." --cache

# Interactive REPL
sql-splitter query dump.sql --interactive
```

### Help Text

```
Query SQL dumps using DuckDB's analytical engine

Usage: sql-splitter query <INPUT> [QUERY] [OPTIONS]

Arguments:
  <INPUT>   SQL dump file to query
  [QUERY]   SQL query to execute (omit for --interactive mode)

Options:
  -f, --format <FORMAT>   Output format: table, json, csv [default: table]
  -o, --output <FILE>     Write output to file
  -d, --dialect <DIALECT> Source dialect (auto-detected if omitted)
      --disk              Use disk-based temp storage (for large dumps)
      --cache             Cache imported data for repeated queries
      --interactive       Start interactive query session
      --tables <TABLES>   Only import specific tables (comma-separated)
  -h, --help              Print help

Examples:
  sql-splitter query dump.sql "SELECT COUNT(*) FROM users"
  sql-splitter query dump.sql "SELECT * FROM orders WHERE total > 100" -f json
  sql-splitter query huge.sql --interactive --disk
```

---

## 9. Workflows Unlocked

### 9.1 Quick Analytics Without Database Setup

**Before**:

```bash
# Need to restore dump to run analytics
createdb tempdb
psql tempdb < dump.sql
psql tempdb -c "SELECT COUNT(*) FROM users"
dropdb tempdb
```

**After**:

```bash
sql-splitter query dump.sql "SELECT COUNT(*) FROM users"
```

### 9.2 Data Exploration

```bash
# What tables exist?
sql-splitter query dump.sql "SHOW TABLES"

# Schema inspection
sql-splitter query dump.sql "DESCRIBE users"

# Sample data
sql-splitter query dump.sql "SELECT * FROM users LIMIT 10"
```

### 9.3 Complex Aggregations

```bash
# Window functions
sql-splitter query dump.sql "
  SELECT
    user_id,
    order_total,
    SUM(order_total) OVER (PARTITION BY user_id) as lifetime_value
  FROM orders
"

# JOINs
sql-splitter query dump.sql "
  SELECT u.email, COUNT(o.id) as order_count
  FROM users u
  JOIN orders o ON u.id = o.user_id
  GROUP BY u.email
  ORDER BY order_count DESC
"
```

### 9.4 Data Quality Checks

```bash
# Find duplicates
sql-splitter query dump.sql "
  SELECT email, COUNT(*) as cnt
  FROM users
  GROUP BY email
  HAVING cnt > 1
"

# Orphaned foreign keys
sql-splitter query dump.sql "
  SELECT o.id, o.user_id
  FROM orders o
  LEFT JOIN users u ON o.user_id = u.id
  WHERE u.id IS NULL
"
```

### 9.5 Export Subset

```bash
# Extract specific data as JSON
sql-splitter query dump.sql "
  SELECT * FROM users WHERE country = 'US'
" -f json -o us_users.json
```

### 9.6 Pipeline Integration

```bash
# Chain with other sql-splitter commands
sql-splitter sample dump.sql --percent 10 | \
  sql-splitter query - "SELECT AVG(age) FROM users"

sql-splitter redact dump.sql --config redact.yaml | \
  sql-splitter query - "SELECT email FROM users LIMIT 5"
```

---

## 10. Risk Assessment

### Technical Risks

| Risk                        | Likelihood | Impact | Mitigation                                   |
| --------------------------- | ---------- | ------ | -------------------------------------------- |
| DuckDB type incompatibility | Medium     | Medium | Comprehensive type mapping, skip unsupported |
| Large dump OOM              | Low        | High   | Auto disk mode for >2GB dumps                |
| Build time increase         | High       | Low    | First build only, document in README         |
| Binary size concerns        | Medium     | Low    | Optional feature flag                        |

### Operational Risks

| Risk                    | Likelihood | Impact | Mitigation                    |
| ----------------------- | ---------- | ------ | ----------------------------- |
| DuckDB breaking changes | Low        | Medium | Pin version, test on upgrades |
| ICU extension missing   | Medium     | Low    | Document, allow runtime load  |
| Cross-platform issues   | Low        | Low    | Bundled feature handles this  |

---

## 11. Implementation Plan

### Phase 1: Core Query Engine (12-16h)

1. Add `duckdb` dependency with `bundled` feature
2. Create `src/duckdb/mod.rs` module
3. Implement DuckDB loader using existing parser
4. Implement query executor
5. Add basic output formatting (table, JSON)

### Phase 2: CLI Integration (4-6h)

1. Add `query` subcommand to CLI
2. Implement --format, --output flags
3. Add --disk and --cache modes
4. Add progress bar for import

### Phase 3: REPL Mode (4-6h)

1. Implement interactive readline loop
2. Add `.tables`, `.schema` meta-commands
3. Add query history
4. Add timing output

### Phase 4: Testing & Polish (4-6h)

1. Integration tests with all dialects
2. Memory profiling with large dumps
3. Documentation and examples
4. Benchmarks

---

## 12. Conclusion

**DuckDB integration is highly recommended** for sql-splitter v1.12.0:

| Factor                | Score      | Notes                                           |
| --------------------- | ---------- | ----------------------------------------------- |
| Technical feasibility | ⭐⭐⭐⭐⭐ | Excellent Rust bindings, bundled option         |
| Licensing             | ⭐⭐⭐⭐⭐ | MIT license, fully compatible                   |
| Code reuse            | ⭐⭐⭐⭐   | 60-70% of convert infrastructure reusable       |
| User value            | ⭐⭐⭐⭐⭐ | Transforms sql-splitter into analytics platform |
| Maintenance burden    | ⭐⭐⭐⭐   | Official crate, stable API                      |

### Next Steps

1. ✅ Feasibility study complete
2. 🔲 Create feature branch `feature/duckdb-query`
3. 🔲 Add duckdb dependency, verify build
4. 🔲 Implement core loader and query executor
5. 🔲 Add CLI integration
6. 🔲 Comprehensive testing
7. 🔲 Documentation and CHANGELOG
8. 🔲 Release v1.12.0

---

## Appendix A: DuckDB vs Alternative Approaches

### Option 1: DuckDB (Recommended)

**Pros:**

- Full SQL support (JOINs, aggregations, window functions)
- 100x faster than text processing
- Excellent Rust bindings
- Zero external dependencies with `bundled`

**Cons:**

- Binary size increase (~20 MB)
- Build time increase (~3 min first build)

### Option 2: Custom Query Engine

**Pros:**

- No external dependencies
- Full control

**Cons:**

- 100+ hours to implement basic SQL
- Would never match DuckDB's features
- Ongoing maintenance burden

### Option 3: Shell Out to DuckDB CLI

**Pros:**

- Simple implementation
- Already tested

**Cons:**

- Requires DuckDB CLI installed
- Awkward error handling
- Data transfer overhead

**Verdict**: Option 1 (DuckDB embedded) is clearly the best choice.

---

## Appendix B: Sample Implementation Sketch

```rust
// src/duckdb/mod.rs (sketch)

use duckdb::{Connection, Result as DuckResult};
use crate::parser::{Parser, SqlDialect, StatementType};
use crate::convert::{Converter, copy_to_inserts};

pub struct QueryEngine {
    conn: Connection,
    stats: ImportStats,
}

impl QueryEngine {
    pub fn new_in_memory() -> DuckResult<Self> {
        Ok(Self {
            conn: Connection::open_in_memory()?,
            stats: ImportStats::default(),
        })
    }

    pub fn new_from_file(path: &Path) -> DuckResult<Self> {
        Ok(Self {
            conn: Connection::open(path)?,
            stats: ImportStats::default(),
        })
    }

    pub fn import_dump(&mut self, path: &Path, dialect: SqlDialect) -> anyhow::Result<&ImportStats> {
        let converter = Converter::new(dialect, SqlDialect::Postgres); // DuckDB is PG-compatible

        let mut parser = Parser::new_with_dialect(path, dialect)?;

        while let Some(stmt) = parser.next_statement()? {
            let (stmt_type, table) = Parser::parse_statement(&stmt);

            match stmt_type {
                StatementType::CreateTable => {
                    let converted = converter.convert_statement(&stmt)?;
                    let sql = String::from_utf8_lossy(&converted);
                    self.conn.execute(&sql, [])?;
                    self.stats.tables += 1;
                }
                StatementType::Insert => {
                    let converted = converter.convert_statement(&stmt)?;
                    let sql = String::from_utf8_lossy(&converted);
                    self.conn.execute(&sql, [])?;
                    self.stats.inserts += 1;
                }
                StatementType::Copy => {
                    // Handle PostgreSQL COPY
                    // ... use copy_to_inserts()
                }
                _ => {} // Skip other statement types
            }
        }

        Ok(&self.stats)
    }

    pub fn query(&self, sql: &str) -> DuckResult<QueryResult> {
        let mut stmt = self.conn.prepare(sql)?;
        let column_names: Vec<String> = stmt.column_names();

        let rows: Vec<Vec<duckdb::types::Value>> = stmt
            .query_map([], |row| {
                let mut values = Vec::new();
                for i in 0..column_names.len() {
                    values.push(row.get(i)?);
                }
                Ok(values)
            })?
            .collect::<DuckResult<Vec<_>>>()?;

        Ok(QueryResult { column_names, rows })
    }
}
```
