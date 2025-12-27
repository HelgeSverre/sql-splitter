# Feature Design: DuckDB Appender-Based Bulk Loading

**Status**: Implemented  
**Author**: AI Assistant  
**Date**: 2024-12-27  
**Related Issue**: Performance optimization for MySQL/SQLite/MSSQL INSERT processing

## Implementation Notes

The feature was implemented using a batched SQL approach rather than DuckDB's Appender API. The Appender API has limitations with dynamic types in Rust, so we instead generate batched INSERT statements with up to 10,000 rows per batch.

### Performance Results (December 2024)

#### Large Dataset Benchmarks (124MB / 1.25M rows)

| Dialect | File Size | Before | After | Improvement |
|---------|-----------|--------|-------|-------------|
| MySQL | 124 MB | 36.5s | 22.78s | **1.6x faster** |
| PostgreSQL | 111 MB | 6.4s | 6.41s | Same (already optimized) |
| SQLite | 124 MB | 37.2s | 43.53s | Slight regression* |

*SQLite regression under investigation - may be related to row counting differences.

#### Medium Dataset Benchmarks (25MB / 250k rows)

| Dialect | File Size | Query Time | Peak RSS |
|---------|-----------|------------|----------|
| MySQL | 24.47 MB | 6.11s | 170 MB |
| PostgreSQL | 21.85 MB | 1.31s | 70 MB |
| SQLite | 24.47 MB | 8.51s | 170 MB |

#### Key Improvements

- MySQL query time reduced from ~36s to ~23s on large datasets
- Batched INSERTs with up to 10,000 rows per batch
- Transactional fallback for constraint violations
- All 158 DuckDB tests pass

#### MySQL Syntax Stripping (v1.12.2)

Comprehensive MySQL-specific syntax is now stripped during DuckDB import:

- **Index definitions**: UNIQUE KEY, KEY, FULLTEXT KEY, SPATIAL KEY
- **Constraints**: FOREIGN KEY constraints (DuckDB enforces them which breaks batch loading)
- **Computed columns**: GENERATED ALWAYS AS (expr) STORED/VIRTUAL
- **Type conversion**: Fixed regex to avoid matching type keywords in column names (e.g., 'internal_note' vs 'INT')

Verified against real-world production dumps:
- taskflow_production.sql (17MB, 62 tables) - 0 warnings
- db2sheets_prod.sql (17MB, 18 tables) - 0 warnings  
- boatflow_latest_2.sql (128MB, 52 tables) - 4 warnings (data quality issues only)

## Executive Summary

This document describes a refactoring of the DuckDB `query` command loader to use the Appender API for bulk inserts instead of executing individual INSERT statements. This will achieve PostgreSQL COPY-like performance (~6x improvement) for MySQL, SQLite, and MSSQL dumps.

## Problem Statement

### Current Performance Gap

| Dialect | File Size | Query Time | Peak RSS | Notes |
|---------|-----------|------------|----------|-------|
| PostgreSQL | 111 MB | 6.4s | 101 MB | Uses COPY (fast batch import) |
| MySQL | 124 MB | 36.5s | 100 MB | Uses INSERT (row-by-row parsing) |
| SQLite | 124 MB | 37.2s | 210 MB | Uses INSERT (row-by-row parsing) |

PostgreSQL dumps are **~6x faster** because COPY blocks are batched, while MySQL/SQLite/MSSQL INSERTs are executed one statement at a time via `conn.execute()`.

### Root Cause

In `src/duckdb/loader.rs`, each INSERT statement is converted and executed individually:

```rust
StatementType::Insert => {
    let duckdb_sql = self.convert_insert(&stmt, dialect)?;
    match self.conn.execute(&duckdb_sql, []) {
        Ok(_) => { stats.insert_statements += 1; ... }
        Err(e) => { stats.warnings.push(...); }
    }
}
```

This results in:
- Per-statement SQL parsing overhead in DuckDB
- Per-statement transaction overhead (autocommit)
- No batching of rows across statements

## Proposed Solution

### Architecture Overview

```
INSERT Statement
      ↓
[InsertParser] - Extract table, columns, values
      ↓
[BatchManager] - Accumulate rows per table (10,000 max)
      ↓
[DuckDB Appender] - Bulk insert via append_row()
      ↓
On Error → Rollback + Fallback to per-statement execution
```

### Key Components

#### 1. Enhanced InsertParser (`src/parser/mysql_insert.rs`)

The existing `InsertParser` already parses INSERT statements and extracts values. We need to:

- Make `ParsedValue` enum public
- Add `values: Vec<ParsedValue>` to `ParsedRow` struct
- Create a schema-less extraction function for the loader

```rust
// New public interface
#[derive(Debug, Clone)]
pub enum ParsedValue {
    Null,
    Integer(i64),
    BigInteger(i128),
    String { value: String },
    Hex(Vec<u8>),
    Other(Vec<u8>),  // decimals, floats, expressions
}

pub struct InsertValues {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub rows: Vec<Vec<ParsedValue>>,
}

pub fn parse_insert_values(stmt: &[u8]) -> Result<InsertValues>
```

#### 2. BatchManager (`src/duckdb/batch.rs`)

A new module to manage batched inserts:

```rust
struct InsertBatch {
    table: String,
    columns: Option<Vec<String>>,
    rows: Vec<Vec<ParsedValue>>,
    statements: Vec<String>,      // Original SQL for fallback
    rows_per_statement: Vec<usize>,
}

struct BatchManager {
    batches: HashMap<(String, Option<Vec<String>>), InsertBatch>,
    max_rows_per_batch: usize,  // Default: 10,000
}

impl BatchManager {
    fn queue_insert(&mut self, table: &str, columns: Option<Vec<String>>, 
                    rows: Vec<Vec<ParsedValue>>, original_sql: String) -> Option<InsertBatch>;
    fn flush_all(&mut self, conn: &Connection, stats: &mut ImportStats) -> Result<()>;
}
```

#### 3. Appender Integration (`src/duckdb/loader.rs`)

Modified `load_statements` flow:

```rust
fn load_statements(&self, ...) -> Result<()> {
    let mut batch_mgr = BatchManager::new(10_000);
    
    while let Some(stmt) = parser.next_statement() {
        match stmt_type {
            StatementType::Insert => {
                // Try fast path first
                if !self.try_queue_for_bulk(&stmt, &mut batch_mgr)? {
                    // Fallback to current behavior
                    self.execute_insert_statement(&stmt, stats)?;
                }
                
                // Flush if batch is full
                batch_mgr.flush_ready_batches(&self.conn, stats)?;
            }
            // ... other statement types unchanged
        }
    }
    
    // Final flush
    batch_mgr.flush_all(&self.conn, stats)?;
}
```

#### 4. Transactional Fallback

Critical for handling constraint violations without data loss:

```rust
fn flush_batch(conn: &Connection, batch: &InsertBatch, stats: &mut ImportStats) -> Result<()> {
    conn.execute("BEGIN TRANSACTION", [])?;
    
    let appender = match conn.appender(&batch.table) {
        Ok(app) => app,
        Err(_) => {
            conn.execute("ROLLBACK", [])?;
            return fallback_execute(conn, batch, stats);
        }
    };
    
    for row in &batch.rows {
        let params = map_values_to_params(row);
        if appender.append_row(params).is_err() {
            drop(appender);
            conn.execute("ROLLBACK", [])?;
            return fallback_execute(conn, batch, stats);
        }
    }
    
    drop(appender);
    match conn.execute("COMMIT", []) {
        Ok(_) => {
            stats.rows_inserted += batch.rows.len() as u64;
            stats.insert_statements += batch.statements.len() as u64;
        }
        Err(_) => {
            conn.execute("ROLLBACK", [])?;
            fallback_execute(conn, batch, stats)?;
        }
    }
    
    Ok(())
}
```

#### 5. Type Mapping

Map `ParsedValue` to DuckDB types:

```rust
fn map_values_to_params(row: &[ParsedValue]) -> Vec<duckdb::types::Value> {
    row.iter().map(|v| match v {
        ParsedValue::Null => duckdb::types::Value::Null,
        ParsedValue::Integer(n) => duckdb::types::Value::BigInt(*n),
        ParsedValue::BigInteger(n) => duckdb::types::Value::Text(n.to_string()),
        ParsedValue::String { value } => duckdb::types::Value::Text(value.clone()),
        ParsedValue::Hex(bytes) => duckdb::types::Value::Blob(bytes.clone()),
        ParsedValue::Other(raw) => {
            let s = String::from_utf8_lossy(raw).to_string();
            if let Ok(f) = s.parse::<f64>() {
                duckdb::types::Value::Double(f)
            } else {
                duckdb::types::Value::Text(s)
            }
        }
    }).collect()
}
```

## Alternative Approaches Considered

### 1. Tree-sitter for SQL Parsing

**Pros:**
- Incremental parsing (sub-millisecond updates)
- Proper AST with full syntax coverage
- Used in production at GitHub

**Cons:**
- Requires native C bindings or WebAssembly
- 8MB+ library size for SQL grammar
- Overkill for our use case (just extracting VALUES)
- No INSERT-specific value extraction built-in

**Verdict:** Not recommended for v1. Consider if we need broader SQL transformation capabilities later.

### 2. sqlparser-rs

**Pros:**
- Pure Rust, no native dependencies
- Complete SQL AST for multiple dialects
- Handles complex expressions, subqueries

**Cons:**
- 3.3x slower than hand-written parser (per benchmarks)
- Heavy allocation overhead for large dumps
- Over-parses (we only need VALUES, not full AST)

**Verdict:** Consider for DDL parsing or complex SQL transformations. For VALUES extraction, our regex-based approach is faster.

### 3. Single Large Transaction

**Attempted:** Wrapping all statements in `BEGIN TRANSACTION` / `COMMIT`

**Result:** Failed. FK constraint violations abort the entire transaction, losing all subsequent data.

**Verdict:** Per-batch transactions with fallback is the correct approach.

## Implementation Plan

### Phase 1: Core Infrastructure (1-2 days)

1. **Refactor InsertParser** (1-3 hours)
   - Make `ParsedValue` public
   - Add `values: Vec<ParsedValue>` to `ParsedRow`
   - Create `parse_insert_values()` helper

2. **Create BatchManager** (3-4 hours)
   - Implement `InsertBatch` struct
   - Implement queue and flush logic
   - Add unit tests

3. **Integrate with DumpLoader** (3-4 hours)
   - Add `try_queue_for_bulk()` method
   - Implement `flush_batch()` with Appender
   - Implement transactional fallback

### Phase 2: Type Mapping & Edge Cases (1 day)

1. **Type mapping implementation** (2-3 hours)
   - Handle all `ParsedValue` variants
   - Test with various column types

2. **Edge case handling** (2-3 hours)
   - Complex column lists
   - MSSQL N'...' strings
   - MySQL hex literals
   - Binary/BLOB data

### Phase 3: Testing & Benchmarking (1 day)

1. **Unit tests** (2-3 hours)
   - BatchManager tests
   - Type mapping tests
   - Fallback behavior tests

2. **Integration tests** (2-3 hours)
   - MySQL dumps with various INSERT formats
   - SQLite dumps
   - MSSQL dumps
   - Large file stress tests

3. **Benchmarking** (1-2 hours)
   - Update `profile-memory.sh` results
   - Compare MySQL vs PostgreSQL performance
   - Document improvements

### Phase 4: Optional Enhancements

1. **Unify PostgreSQL COPY with Appender** (2-3 hours)
   - Map `CopyValue` to `ParsedValue`
   - Use same BatchManager for COPY blocks

2. **Schema-aware type mapping** (3-4 hours)
   - Use table schema for precise type conversion
   - Optimize DATE/TIMESTAMP parsing

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| InsertParser fails on edge cases | Medium | Fallback to per-statement execution |
| Appender constraint errors | High | Transactional rollback + fallback |
| Column order mismatches | Medium | Only use Appender for simple column lists |
| Memory spike from large batches | Low | Cap at 10,000 rows per batch |
| Performance regression | Medium | Extensive benchmarking before merge |

## Success Metrics

1. **Performance:** MySQL/SQLite INSERT processing within 2x of PostgreSQL COPY performance (currently 6x slower)
2. **Correctness:** All existing tests pass; constraint violations handled gracefully
3. **Memory:** Peak RSS unchanged or improved
4. **Fallback rate:** <5% of statements using fallback path on typical dumps

## Open Questions

1. Should we use Arrow RecordBatch for even faster appending? (Available in duckdb-rs but more complex)
2. Should we add a `--bulk-insert` flag to opt-in to this behavior?
3. How to handle `ON DUPLICATE KEY UPDATE` / `ON CONFLICT` statements?

## References

- [DuckDB Appender API](https://duckdb.org/docs/stable/data/appender.html)
- [duckdb-rs Appender example](https://github.com/duckdb/duckdb-rs/blob/main/crates/duckdb/examples/appender.rs)
- [sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs)
- [tree-sitter-sql](https://github.com/DerekStride/tree-sitter-sql)
