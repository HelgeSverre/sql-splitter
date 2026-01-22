# Convert Command Feasibility Analysis

**Status**: Analysis  
**Date**: 2025-12-20

## Overview

This document analyzes the feasibility of implementing a `convert` command that translates SQL dumps between MySQL, PostgreSQL, and SQLite dialects.

## Scope

**Supported conversions:**

- MySQL ↔ PostgreSQL
- MySQL ↔ SQLite
- PostgreSQL ↔ SQLite

**Total conversion pairs:** 6 (each direction counts separately)

## Dialect Differences Matrix

### 1. Identifier Quoting

| Dialect    | Style                      | Example        |
| ---------- | -------------------------- | -------------- |
| MySQL      | Backticks                  | \`table_name\` |
| PostgreSQL | Double quotes              | "table_name"   |
| SQLite     | Double quotes or backticks | "table_name"   |

**Conversion complexity:** Low ✅

- Simple regex replacement
- Must handle escaped quotes inside identifiers

### 2. String Escaping

| Dialect    | Style                | Example                |
| ---------- | -------------------- | ---------------------- |
| MySQL      | Backslash escapes    | `'it\'s'` or `'it''s'` |
| PostgreSQL | Double single quotes | `'it''s'`              |
| SQLite     | Double single quotes | `'it''s'`              |

**Conversion complexity:** Medium ⚠️

- MySQL → PostgreSQL: Convert `\'` to `''`
- Must not break other backslash escapes (`\n`, `\t`)
- Binary data escaping differs

### 3. Data Types

| MySQL                | PostgreSQL         | SQLite                | Notes                 |
| -------------------- | ------------------ | --------------------- | --------------------- |
| `INT AUTO_INCREMENT` | `SERIAL`           | `INTEGER PRIMARY KEY` | Auto-increment        |
| `TINYINT(1)`         | `BOOLEAN`          | `INTEGER`             | Boolean               |
| `DATETIME`           | `TIMESTAMP`        | `TEXT`                | Timestamps            |
| `DOUBLE`             | `DOUBLE PRECISION` | `REAL`                | Floats                |
| `BLOB`               | `BYTEA`            | `BLOB`                | Binary                |
| `TEXT`               | `TEXT`             | `TEXT`                | Same ✓                |
| `VARCHAR(n)`         | `VARCHAR(n)`       | `TEXT`                | SQLite ignores length |
| `ENUM('a','b')`      | `VARCHAR` + CHECK  | `TEXT`                | Enums                 |
| `JSON`               | `JSONB`            | `TEXT`                | JSON                  |
| `UNSIGNED`           | (none)             | (none)                | MySQL-only            |

**Conversion complexity:** High ⚠️

- Requires parsing CREATE TABLE statements
- Type mapping tables per conversion direction
- Some types have no direct equivalent (ENUM, UNSIGNED)

### 4. Auto-Increment

| Dialect    | Syntax                                     |
| ---------- | ------------------------------------------ |
| MySQL      | `INT AUTO_INCREMENT`                       |
| PostgreSQL | `SERIAL` or `GENERATED ALWAYS AS IDENTITY` |
| SQLite     | `INTEGER PRIMARY KEY` (implicit ROWID)     |

**Conversion complexity:** Medium ⚠️

- Must also handle sequences in PostgreSQL
- MySQL has `AUTO_INCREMENT=N` table option

### 5. Boolean Literals

| Dialect    | True            | False            |
| ---------- | --------------- | ---------------- |
| MySQL      | `1` or `TRUE`   | `0` or `FALSE`   |
| PostgreSQL | `TRUE` or `'t'` | `FALSE` or `'f'` |
| SQLite     | `1`             | `0`              |

**Conversion complexity:** Medium ⚠️

- Context-dependent (need to know column type)
- In INSERT VALUES, must parse which column is boolean

### 6. INSERT Syntax

| Feature          | MySQL                                     | PostgreSQL                  | SQLite                      |
| ---------------- | ----------------------------------------- | --------------------------- | --------------------------- |
| Multi-row INSERT | ✅                                        | ✅                          | ✅                          |
| Extended INSERT  | `INSERT INTO t VALUES (...),(...),(...);` | Same                        | Same                        |
| COPY command     | ❌                                        | `COPY ... FROM stdin`       | ❌                          |
| ON DUPLICATE KEY | `ON DUPLICATE KEY UPDATE`                 | `ON CONFLICT ... DO UPDATE` | `ON CONFLICT ... DO UPDATE` |

**Conversion complexity:** High ⚠️

- COPY → INSERT conversion requires parsing tab-separated data
- INSERT → COPY conversion is optional but useful
- Conflict handling syntax completely different

### 7. Transaction & Session Settings

| MySQL                       | PostgreSQL                      | SQLite                     |
| --------------------------- | ------------------------------- | -------------------------- |
| `SET NAMES utf8mb4;`        | `SET client_encoding = 'UTF8';` | (none)                     |
| `SET FOREIGN_KEY_CHECKS=0;` | `SET CONSTRAINTS ALL DEFERRED;` | `PRAGMA foreign_keys=OFF;` |
| `LOCK TABLES`               | (not in dumps)                  | (not applicable)           |
| `/*!40101 ... */`           | (none)                          | (none)                     |

**Conversion complexity:** Medium ⚠️

- Strip MySQL conditional comments
- Map session variables to equivalents
- Some have no equivalent (just remove)

### 8. Index Syntax

| Feature      | MySQL                          | PostgreSQL                    | SQLite |
| ------------ | ------------------------------ | ----------------------------- | ------ |
| CREATE INDEX | `CREATE INDEX idx ON t (col);` | Same                          | Same   |
| FULLTEXT     | `FULLTEXT INDEX`               | `CREATE INDEX ... USING gin`  | `FTS5` |
| SPATIAL      | `SPATIAL INDEX`                | `CREATE INDEX ... USING gist` | ❌     |
| USING        | `USING BTREE`                  | `USING btree`                 | (none) |

**Conversion complexity:** Medium ⚠️

- Basic indexes are straightforward
- Fulltext/spatial require significant rewriting or removal

### 9. Comments

| Dialect    | Inline              | Conditional       |
| ---------- | ------------------- | ----------------- |
| MySQL      | `-- `, `#`, `/* */` | `/*!40101 ... */` |
| PostgreSQL | `-- `, `/* */`      | (none)            |
| SQLite     | `-- `, `/* */`      | (none)            |

**Conversion complexity:** Low ✅

- Strip MySQL conditional comments or convert to regular comments
- `#` comments rare in dumps

### 10. Special Features

| Feature           | MySQL            | PostgreSQL           | SQLite           |
| ----------------- | ---------------- | -------------------- | ---------------- |
| ENUM types        | ✅ Native        | Requires CREATE TYPE | ❌               |
| Triggers          | Different syntax | Different syntax     | Different syntax |
| Stored procedures | `DELIMITER`      | `$$` blocks          | Not supported    |
| Views             | Similar          | Similar              | Similar          |
| Partitioning      | Yes              | Yes                  | No               |

**Conversion complexity:** Very High ❌

- Triggers/procedures need complete rewriting
- May need to skip or warn

## Feasibility Assessment by Conversion Pair

### MySQL → PostgreSQL

| Aspect               | Difficulty | Notes                            |
| -------------------- | ---------- | -------------------------------- |
| Identifier quoting   | Easy       | Backticks → double quotes        |
| String escaping      | Medium     | `\'` → `''`                      |
| Data types           | Hard       | Many mappings needed             |
| AUTO_INCREMENT       | Medium     | → SERIAL                         |
| INSERT syntax        | Easy       | Mostly compatible                |
| COPY conversion      | N/A        | Could generate COPY from INSERTs |
| Conditional comments | Easy       | Strip them                       |

**Overall: Feasible with limitations** ⚠️

### PostgreSQL → MySQL

| Aspect             | Difficulty | Notes                      |
| ------------------ | ---------- | -------------------------- |
| Identifier quoting | Easy       | Double quotes → backticks  |
| String escaping    | Easy       | Already compatible         |
| Data types         | Hard       | SERIAL → AUTO_INCREMENT    |
| COPY → INSERT      | Hard       | Parse stdin data format    |
| Dollar-quoting     | Medium     | Convert to regular strings |

**Overall: Feasible, COPY is main challenge** ⚠️

### MySQL → SQLite

| Aspect             | Difficulty | Notes                      |
| ------------------ | ---------- | -------------------------- |
| Identifier quoting | Easy       | Backticks → double quotes  |
| Data types         | Medium     | Most map directly          |
| AUTO_INCREMENT     | Medium     | → INTEGER PRIMARY KEY      |
| Session settings   | Easy       | Strip or convert to PRAGMA |

**Overall: Most feasible conversion** ✅

### SQLite → MySQL

| Aspect         | Difficulty | Notes                                |
| -------------- | ---------- | ------------------------------------ |
| Data types     | Medium     | Add type lengths                     |
| Auto-increment | Medium     | INTEGER PRIMARY KEY → AUTO_INCREMENT |

**Overall: Feasible** ✅

### PostgreSQL → SQLite

| Aspect         | Difficulty | Notes                        |
| -------------- | ---------- | ---------------------------- |
| COPY → INSERT  | Hard       | Parse tab-separated data     |
| Data types     | Medium     | SERIAL → INTEGER PRIMARY KEY |
| Dollar-quoting | Medium     | Convert to regular strings   |

**Overall: Feasible, COPY is challenge** ⚠️

### SQLite → PostgreSQL

| Aspect         | Difficulty | Notes           |
| -------------- | ---------- | --------------- |
| Data types     | Easy       | Most compatible |
| Auto-increment | Medium     | → SERIAL        |

**Overall: Feasible** ✅

## Implementation Strategy

### Phase 1: Core Infrastructure

```rust
pub trait DialectConverter {
    fn convert_identifier(&self, ident: &str) -> String;
    fn convert_string_literal(&self, lit: &str) -> String;
    fn convert_data_type(&self, dtype: &str) -> String;
    fn convert_create_table(&self, stmt: &str) -> String;
    fn convert_insert(&self, stmt: &str) -> String;
}

pub struct MySqlToPostgres;
pub struct PostgresToMySql;
// ... etc
```

### Phase 2: Statement-Level Conversion

1. Parse each statement type
2. Apply appropriate transformations
3. Stream output (don't buffer entire file)

### Phase 3: COPY Handling (PostgreSQL)

- PostgreSQL → Other: Parse COPY data block, emit INSERTs
- Other → PostgreSQL: Optionally batch INSERTs into COPY

## Required Scope (Full Coverage)

**Non-negotiable — all must be implemented:**

### Statement Conversion

- ✅ Identifier quoting conversion (all dialects)
- ✅ String escape normalization (`\'` ↔ `''`)
- ✅ Complete data type mapping (all types, not just common)
- ✅ AUTO_INCREMENT ↔ SERIAL ↔ INTEGER PRIMARY KEY
- ✅ COPY ↔ INSERT bidirectional conversion
- ✅ Session/header settings conversion
- ✅ Conditional comment handling

### Data Type Mapping (Complete)

- ✅ All integer types (TINYINT, SMALLINT, INT, BIGINT)
- ✅ All float types (FLOAT, DOUBLE, DECIMAL, NUMERIC)
- ✅ All string types (CHAR, VARCHAR, TEXT, MEDIUMTEXT, LONGTEXT)
- ✅ All binary types (BINARY, VARBINARY, BLOB, BYTEA)
- ✅ All date/time types (DATE, TIME, DATETIME, TIMESTAMP, INTERVAL)
- ✅ Boolean types (TINYINT(1) ↔ BOOLEAN ↔ INTEGER)
- ✅ JSON/JSONB types
- ✅ UUID types
- ✅ ENUM types (convert to CHECK constraints or VARCHAR)
- ✅ SET types (MySQL → VARCHAR with validation)
- ✅ Array types (PostgreSQL → JSON or error)
- ✅ UNSIGNED modifier handling

### Constraint Conversion

- ✅ PRIMARY KEY (all syntaxes)
- ✅ FOREIGN KEY with all actions (CASCADE, SET NULL, etc.)
- ✅ UNIQUE constraints
- ✅ CHECK constraints
- ✅ DEFAULT values (including functions like NOW(), CURRENT_TIMESTAMP)
- ✅ NOT NULL constraints

### Index Conversion

- ✅ Basic indexes (BTREE)
- ✅ Unique indexes
- ✅ Composite indexes
- ✅ FULLTEXT indexes (convert or emit warning with alternative)
- ✅ SPATIAL indexes (convert or emit warning with alternative)
- ✅ Partial indexes (PostgreSQL)

### PostgreSQL COPY Handling (Critical)

```
COPY table_name (col1, col2, col3) FROM stdin;
value1	value2	value3
value4	value5	value6
\.
```

Must convert to:

```sql
INSERT INTO table_name (col1, col2, col3) VALUES
('value1', 'value2', 'value3'),
('value4', 'value5', 'value6');
```

**COPY parsing requirements:**

- Tab-separated value parsing
- NULL handling (`\N` literal)
- Escape sequence handling (`\\`, `\t`, `\n`)
- Binary COPY format detection (error, not supported)
- Multi-megabyte COPY blocks (streaming, not buffering)

### Triggers & Procedures

- ⚠️ **Warn and skip** with clear message
- Output original as comment for manual conversion
- `--strict` mode: fail if triggers/procedures found

## Estimated Effort (Full Coverage)

| Component                               | Effort        |
| --------------------------------------- | ------------- |
| Core converter architecture             | 3 hours       |
| Identifier quoting (all pairs)          | 1 hour        |
| String escape conversion                | 2 hours       |
| **Complete data type mapping**          | 6 hours       |
| CREATE TABLE full parsing               | 6 hours       |
| Constraint conversion                   | 4 hours       |
| Index conversion                        | 3 hours       |
| **COPY ↔ INSERT conversion**            | 8 hours       |
| INSERT statement handling               | 2 hours       |
| Session/header conversion               | 2 hours       |
| ENUM/SET handling                       | 3 hours       |
| Default value conversion                | 2 hours       |
| **Testing all 6 pairs (comprehensive)** | 10 hours      |
| Edge case handling                      | 4 hours       |
| **Total**                               | **~56 hours** |

## Risks & Mitigations

| Risk                              | Mitigation                                       |
| --------------------------------- | ------------------------------------------------ |
| Lossy conversion (ENUM, UNSIGNED) | Convert to closest equivalent + emit warning     |
| Edge cases in syntax              | Comprehensive test suite with real-world dumps   |
| Data truncation                   | Validate data fits in target type, warn if not   |
| Performance overhead              | Stream processing, don't buffer large statements |
| Large test matrix (6 pairs)       | Shared test fixtures, automated regression tests |

## Validation Strategy

### Roundtrip Testing

```bash
# Convert MySQL → PostgreSQL → MySQL
sql-splitter convert original.sql -o pg.sql --to postgres
sql-splitter convert pg.sql -o roundtrip.sql --to mysql

# Diff should show only formatting differences, not data loss
sql-splitter diff original.sql roundtrip.sql --data-only
```

### Real-World Dump Testing

- Test against dumps from major CMSs (WordPress, Drupal)
- Test against framework migrations (Laravel, Rails, Django)
- Test against large production-like dumps (1GB+)

## Recommendation

**Feasibility: YES — Full coverage is achievable**

The `convert` command with complete coverage is feasible. The main complexity is in:

1. **COPY parsing** — Must handle PostgreSQL's tab-separated stdin format
2. **Data type completeness** — Every type must have a mapping
3. **Testing breadth** — 6 conversion pairs require thorough testing

**Implementation order:**

1. Core architecture + identifier quoting
2. Complete data type mapping (all types)
3. COPY ↔ INSERT conversion (critical for PostgreSQL)
4. Constraint and index conversion
5. ENUM/SET special handling
6. Comprehensive test suite

**Quality bar:**

- Zero data loss for supported types
- Clear warnings for unsupported features (triggers, procedures)
- `--strict` mode fails on anything that can't be converted accurately
- Roundtrip conversion produces semantically equivalent output
