# Diff Command Design

**Status**: Implemented (v1.9.0 core, v1.9.1 enhanced)  
**Date**: 2025-12-20  
**Last Updated**: 2025-12-21

## Overview

The `diff` command compares two SQL dump files and shows schema and/or data differences. It can generate migration SQL to transform one dump into another.

## Implementation Status

### v1.9.0 (Released)

| Feature | Status |
|---------|--------|
| Schema comparison (tables, columns, PKs, FKs) | ✅ Done |
| Data comparison (row counts: added/removed/modified) | ✅ Done |
| Memory-bounded PK tracking | ✅ Done |
| Output formats (text, json, sql) | ✅ Done |
| Table filters (--tables, --exclude) | ✅ Done |
| Modes (--schema-only, --data-only) | ✅ Done |
| All 3 dialects (MySQL, PostgreSQL, SQLite) | ✅ Done |
| PostgreSQL COPY data parsing | ✅ Done |
| Compressed input (.gz, .bz2, .xz, .zst) | ✅ Done |

### v1.9.1 (Released)

| Feature | Status |
|---------|--------|
| `--verbose` with sample collection | ✅ Done |
| `--primary-key` override | ✅ Done |
| `--ignore-order` for column order | ✅ Done |
| Index diff (non-PK indexes) | ✅ Done |
| `--ignore-columns` glob patterns | ✅ Done |
| `--allow-no-pk` / warnings | ✅ Done |

### Deferred

| Feature | Reason |
|---------|--------|
| Data migration SQL (INSERT/UPDATE/DELETE) | Requires full row storage |
| Rename detection | Heuristic matching adds complexity |
| External-sort for huge tables | Only needed for 100M+ row tables |
| Three-way merge | Future scope |

---

## Command Interface

```bash
# Compare two dumps (schema + data summary)
sql-splitter diff old.sql new.sql

# Schema-only comparison
sql-splitter diff old.sql new.sql --schema-only

# Data-only comparison
sql-splitter diff old.sql new.sql --data-only

# Compare specific tables
sql-splitter diff old.sql new.sql --tables users,orders

# Generate migration SQL
sql-splitter diff old.sql new.sql --format sql --output migration.sql

# Output as JSON for programmatic use
sql-splitter diff old.sql new.sql --format json

# Ignore specific tables
sql-splitter diff old.sql new.sql --exclude cache,sessions,logs

# Show row-level details with PK samples
sql-splitter diff old.sql new.sql --verbose

# Override primary key for data comparison
sql-splitter diff old.sql new.sql --primary-key users:email,orders:order_id

# Ignore column order differences
sql-splitter diff old.sql new.sql --ignore-order

# Ignore specific columns in comparison
sql-splitter diff old.sql new.sql --ignore-columns "*.updated_at,*.created_at"

# Handle tables without primary key
sql-splitter diff old.sql new.sql --allow-no-pk
```

---

## CLI Options

| Flag | Description | Default | Status |
|------|-------------|---------|--------|
| `-o, --output` | Output file (default: stdout) | stdout | ✅ |
| `-t, --tables` | Compare specific table(s) only | all | ✅ |
| `--exclude` | Exclude tables from comparison | none | ✅ |
| `--schema-only` | Compare schema only, ignore data | false | ✅ |
| `--data-only` | Compare data only, ignore schema | false | ✅ |
| `-f, --format` | Output format: `text`, `sql`, `json` | text | ✅ |
| `-v, --verbose` | Show PK samples for changes | false | ✅ |
| `--primary-key` | Override PK for data comparison | auto-detect | ✅ |
| `-d, --dialect` | SQL dialect | auto-detect | ✅ |
| `-p, --progress` | Show progress bar | false | ✅ |
| `--max-pk-entries` | Max PK entries to track | 10000000 | ✅ |
| `--ignore-order` | Ignore column order differences | false | ✅ |
| `--ignore-columns` | Glob patterns for columns to ignore | none | ✅ |
| `--allow-no-pk` | Don't skip tables without PK | false | ✅ |

---

## Enhanced Features (v1.9.1)

### 1. `--verbose` with Sample Collection

**Purpose:** Show actual PK values that were added/removed/modified, not just counts.

**Behavior:**
- Collects up to 100 sample PKs per category (added, removed, modified)
- Only collects samples when `--verbose` flag is set
- Samples are stored during scanning, formatted in output
- Works with all output formats (text, json, sql)

**Text Output Example:**
```
Data Changes:
  Table 'users': +45 rows, -12 rows, ~89 modified
    Added PKs: 101, 102, 103, 104, 105... (+40 more)
    Removed PKs: 5, 12, 23, 45... (+8 more)
    Modified PKs: 1, 7, 15, 22... (+85 more)
```

**JSON Output Example:**
```json
{
  "data": {
    "users": {
      "added_count": 45,
      "removed_count": 12,
      "modified_count": 89,
      "sample_added_pks": ["101", "102", "103", "104", "105"],
      "sample_removed_pks": ["5", "12", "23", "45"],
      "sample_modified_pks": ["1", "7", "15", "22"]
    }
  }
}
```

**Implementation Notes:**
- Store `Vec<String>` for each sample category (PK formatted as string)
- Limit to `sample_size` (default 100) to bound memory
- Composite PKs formatted as `(val1, val2)`

---

### 2. `--primary-key` Override

**Purpose:** Specify which column(s) to use as primary key for data comparison, overriding schema-detected PK.

**Use Cases:**
- Table has no PK defined but has a logical key (e.g., `email` is unique)
- Table has composite PK but you want to compare by single column
- Schema parsing missed the PK
- Testing with different key strategies

**Syntax:**
```bash
# Single table override
--primary-key users:email

# Multiple tables
--primary-key users:email,orders:order_id

# Composite key
--primary-key audit_logs:user_id+timestamp
```

**Behavior:**
- Overrides apply only to specified tables
- Tables not specified use schema-detected PK
- Error if specified column doesn't exist in table
- Composite keys use `+` separator

**Implementation Notes:**
- Parse into `HashMap<String, Vec<String>>` (table -> columns)
- Apply in `DataDiffer` when determining PK columns
- Validate column existence against schema

---

### 3. `--ignore-order` for Column Order

**Purpose:** Ignore column position changes when comparing schemas.

**Problem:**
```sql
-- Old: CREATE TABLE users (id INT, name VARCHAR, email VARCHAR)
-- New: CREATE TABLE users (id INT, email VARCHAR, name VARCHAR)
```

Without `--ignore-order`: Reports as schema modification (columns reordered)
With `--ignore-order`: No schema changes reported

**Behavior:**
- Affects schema comparison only, not data
- Compares columns as sets, not ordered lists
- Still detects added/removed/type-changed columns

**Implementation Notes:**
- In `compare_tables()`, compare column sets instead of ordered vectors
- Add flag to `DiffConfig`
- Simple change: use HashSet comparison for column names

---

### 4. Index Diff (Non-PK Indexes)

**Purpose:** Detect added/removed/modified indexes beyond primary key.

**Current State:** Only compares PK and FK changes.

**Enhanced Comparison:**
```
Schema Changes:
  ~ Table 'users':
    + Index 'idx_users_phone' on (phone)
    - Index 'idx_users_legacy' on (old_column)
    ~ Index 'idx_users_email': (email) → (email, created_at)
    + Unique index 'uq_users_email' on (email)
```

**Index Properties Compared:**
- Index name
- Column list (ordered)
- Unique vs non-unique
- Index type (BTREE, HASH, FULLTEXT, GIN, etc.) where detectable

**SQL Statements Parsed:**
```sql
-- Inline in CREATE TABLE
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email),
    UNIQUE INDEX uq_email (email)
);

-- Standalone CREATE INDEX
CREATE INDEX idx_users_email ON users (email);
CREATE UNIQUE INDEX uq_users_email ON users (email);

-- PostgreSQL
CREATE INDEX idx_users_email ON users USING btree (email);
CREATE INDEX idx_users_search ON users USING gin (search_vector);
```

**Implementation Notes:**
- Extend `SchemaBuilder` to parse CREATE INDEX statements
- Add `indexes: Vec<IndexDef>` to `TableSchema`
- Add `IndexDef { name, columns, is_unique, index_type }`
- Compare indexes in `compare_tables()` similar to FKs

---

### 5. `--ignore-columns` Glob Patterns

**Purpose:** Exclude certain columns from both schema and data comparison.

**Use Cases:**
- Ignore auto-updated timestamps: `--ignore-columns "*.updated_at,*.created_at"`
- Ignore audit columns: `--ignore-columns "*.modified_by,*.version"`
- Ignore specific table's column: `--ignore-columns "users.last_login"`
- Ignore all columns with pattern: `--ignore-columns "*.*_hash"`

**Syntax:**
```bash
# Multiple patterns (comma-separated)
--ignore-columns "*.updated_at,*.created_at,users.last_login"

# Glob patterns supported
*.updated_at     # Any table, column named updated_at
users.*          # All columns in users table (unusual but valid)
*.*_at           # Any column ending in _at
audit_*.action   # Column 'action' in tables starting with audit_
```

**Effect on Schema Comparison:**
- Ignored columns not reported as added/removed/modified
- If column is ignored, its type changes are also ignored

**Effect on Data Comparison:**
- When computing row digest, skip ignored columns
- Two rows differing only in ignored columns are considered identical
- PK columns cannot be ignored (error if attempted)

**Implementation Notes:**
- Use `glob` crate for pattern matching
- Parse patterns into list at startup
- Filter columns before comparison in schema diff
- Filter values before hashing in data diff
- Error if pattern would ignore a PK column

---

### 6. `--allow-no-pk` / No-PK Handling

**Purpose:** Control behavior for tables without a primary key.

**Current Behavior:** Tables without PK are silently skipped in data comparison.

**Enhanced Behavior:**
- Default: Warn about tables without PK, skip data comparison for them
- `--allow-no-pk`: Use all columns as composite key (slow, may have issues)
- Error if `--data-only` and all tables lack PK

**Warning Output:**
```
Warning: Table 'audit_logs' has no primary key, skipping data comparison
Warning: Table 'temp_data' has no primary key, skipping data comparison
```

**With `--allow-no-pk`:**
```
Note: Table 'audit_logs' has no primary key, using all columns as key
```

**JSON Output:**
```json
{
  "warnings": [
    {"table": "audit_logs", "message": "No primary key, data comparison skipped"}
  ]
}
```

**Implementation Notes:**
- Add warnings collection to `DiffResult`
- In `DataDiffer`, emit warning when skipping table
- With `--allow-no-pk`, use all column indices as PK

---

## Output Formats

### Text (Default) — Human Readable

```
Comparing: old.sql → new.sql

Schema Changes:
  + Table 'audit_logs' (new)
  - Table 'legacy_data' (removed)
  ~ Table 'users':
    + Column 'phone' VARCHAR(20)
    - Column 'fax' VARCHAR(20)
    ~ Column 'email': VARCHAR(100) → VARCHAR(255)
    + Index 'idx_users_phone' (phone)

Data Changes:
  Table 'users': +45 rows, -12 rows, ~89 modified
    [verbose: Added PKs: 101, 102, 103...]
  Table 'products': +120 rows, ~34 modified

Warnings:
  - Table 'logs' has no primary key, data comparison skipped

Summary:
  Tables: 2 added, 1 removed, 5 modified
  Rows: 165 added, 12 removed, 123 modified
```

### SQL — Migration Script

```sql
-- Migration: old.sql → new.sql
-- Generated: 2025-12-21 12:00:00

-- New table: audit_logs
CREATE TABLE `audit_logs` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `action` VARCHAR(50),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Removed table: legacy_data
DROP TABLE IF EXISTS `legacy_data`;

-- Modified table: users
ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20);
ALTER TABLE `users` DROP COLUMN `fax`;
ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR(255);

-- New index
CREATE INDEX `idx_users_phone` ON `users` (`phone`);

-- Removed index
DROP INDEX `idx_users_legacy` ON `users`;
```

### JSON — Programmatic Use

```json
{
  "schema": {
    "tables_added": [{
      "name": "audit_logs",
      "columns": [{"name": "id", "col_type": "INT", "is_primary_key": true}]
    }],
    "tables_removed": ["legacy_data"],
    "tables_modified": [{
      "table_name": "users",
      "columns_added": [{"name": "phone", "col_type": "VARCHAR(20)"}],
      "columns_removed": [{"name": "fax", "col_type": "VARCHAR(20)"}],
      "columns_modified": [{
        "name": "email",
        "old_type": "VARCHAR(100)",
        "new_type": "VARCHAR(255)"
      }],
      "indexes_added": [{"name": "idx_users_phone", "columns": ["phone"]}],
      "indexes_removed": []
    }]
  },
  "data": {
    "users": {
      "old_row_count": 100,
      "new_row_count": 133,
      "added_count": 45,
      "removed_count": 12,
      "modified_count": 89,
      "sample_added_pks": ["101", "102", "103"],
      "sample_removed_pks": ["5", "12"],
      "sample_modified_pks": ["1", "7"]
    }
  },
  "warnings": [
    {"table": "logs", "message": "No primary key, data comparison skipped"}
  ],
  "summary": {
    "tables_added": 1,
    "tables_removed": 1,
    "tables_modified": 1,
    "rows_added": 165,
    "rows_removed": 12,
    "rows_modified": 123,
    "truncated": false
  }
}
```

---

## Comparison Logic

### Schema Comparison

#### Tables
- **Added**: Table exists in new, not in old
- **Removed**: Table exists in old, not in new
- **Modified**: Same table name, different definition

#### Columns
Compare by column name within each table:
- **Added**: Column in new, not in old
- **Removed**: Column in old, not in new
- **Modified**: Same name, different type/constraints

**Attributes compared:**
- Data type (including size)
- NULL/NOT NULL
- DEFAULT value (future)
- AUTO_INCREMENT/SERIAL (future)

#### Indexes
Compare by index name:
- **Added**: Index in new, not in old
- **Removed**: Index in old, not in new
- **Modified**: Same name, different columns or properties

#### Constraints
- PRIMARY KEY changes
- FOREIGN KEY changes
- UNIQUE constraints (via indexes)

### Data Comparison

#### Primary Key Detection

1. Use `--primary-key` override if specified for table
2. Parse CREATE TABLE for PRIMARY KEY
3. Use first UNIQUE index if no PK (future)
4. Warn and skip if no PK can be determined
5. With `--allow-no-pk`, use all columns

#### Row Matching

Match rows by primary key value:
```
Old: (1, 'alice@old.com', 'Alice')
New: (1, 'alice@new.com', 'Alice Smith')
→ Modified: id=1
```

#### Change Detection

| Old | New | Result |
|-----|-----|--------|
| Row exists | Row exists, same digest | No change |
| Row exists | Row exists, different digest | Modified |
| Row exists | Row missing | Removed |
| Row missing | Row exists | Added |

---

## Implementation Architecture

### File Structure

```
src/
├── cmd/
│   └── diff.rs              # CLI handler
├── differ/
│   ├── mod.rs               # Public API, DiffConfig
│   ├── schema.rs            # Schema comparison
│   ├── data.rs              # Data comparison with memory budget
│   └── output/
│       ├── mod.rs
│       ├── text.rs          # Text formatter
│       ├── sql.rs           # SQL migration generator
│       └── json.rs          # JSON formatter
├── schema/
│   ├── mod.rs               # Schema, TableSchema, Column
│   └── ddl.rs               # SchemaBuilder, DDL parsing
└── pk.rs                    # Shared PK hashing
```

### Memory Budget

| Setting | Default | Rationale |
|---------|---------|-----------|
| `max_pk_entries_global` | 10M | ~160MB for PK+digest maps |
| `max_pk_entries_per_table` | 5M | Prevent single table domination |
| `sample_size` | 100 | Verbose mode sample limit |

**Memory Calculation:**
- 16 bytes per entry (8-byte PkHash + 8-byte RowDigest)
- 10M entries ≈ 160MB base + HashMap overhead (~2x) ≈ 320MB max

---

## Edge Cases

### 1. Column Order Changes
```sql
-- Old: (id, name, email)
-- New: (id, email, name)
```
With `--ignore-order`: No change
Without: Currently reports as separate add/remove (should not report position changes)

### 2. Case Sensitivity
- Table/column names compared case-insensitively

### 3. Renamed Tables/Columns
Cannot auto-detect. Shows as remove + add.

### 4. No Primary Key
- Default: Warn and skip data comparison
- `--allow-no-pk`: Use all columns as key
- `--primary-key table:col`: Use specified column

### 5. Large Tables
Memory limits with `--max-pk-entries`. When exceeded:
- Continue counting rows
- Mark table as truncated
- Report count-based estimates

### 6. Ignored Columns in PK
Error if `--ignore-columns` would ignore a PK column.

---

## Testing Strategy

### Unit Tests
- Schema parsing for each dialect
- Column comparison logic
- Index comparison logic
- Glob pattern matching for `--ignore-columns`
- PK override parsing

### Integration Tests
- All features across MySQL, PostgreSQL, SQLite
- Verbose output with samples
- Primary key override
- Ignore order behavior
- Index diff detection
- Ignore columns patterns
- No-PK table handling

### Edge Case Tests
- Empty tables
- Tables with no PK
- Composite PKs
- Unicode data
- NULL values
- Large tables (truncation)

---

## Example Workflows

### 1. Pre-Deployment Review
```bash
# Compare staging vs production schema, ignoring timestamps
sql-splitter diff prod.sql staging.sql --schema-only --ignore-columns "*.updated_at"

# Generate migration
sql-splitter diff prod.sql staging.sql --format sql -o migration.sql
```

### 2. Audit Data Changes
```bash
# What changed between backups? Show sample PKs
sql-splitter diff monday.sql friday.sql --data-only --verbose --format json
```

### 3. Compare Tables Without PK
```bash
# Specify which column to use as key
sql-splitter diff old.sql new.sql --primary-key logs:timestamp+message
```

### 4. Ignore Audit Columns
```bash
# Compare data ignoring auto-updated fields
sql-splitter diff old.sql new.sql --ignore-columns "*.updated_at,*.modified_by,*.version"
```

---

## Effort Estimates (v1.9.1 Features)

| Feature | Effort |
|---------|--------|
| `--verbose` samples | 2h |
| `--primary-key` override | 2h |
| `--ignore-order` | 1h |
| Index diff | 4h |
| `--ignore-columns` glob | 3h |
| `--allow-no-pk` | 1h |
| Testing (all dialects) | 6h |
| **Total** | **~19h** |

---

## Future Enhancements

1. **Rename detection**: Heuristic matching for renamed tables/columns
2. **Data migration SQL**: Generate INSERT/UPDATE/DELETE for data changes
3. **External-sort**: Disk-based sorting for tables with 100M+ rows
4. **Three-way merge**: Common ancestor comparison
5. **Semantic comparison**: Ignore formatting in DEFAULT values
6. **Interactive mode**: Approve/reject each change

---

## Related Documents

- [Implementation Plan](DIFF_IMPLEMENTATION_PLAN.md)
- [Roadmap](../ROADMAP.md)
- [Schema Module](../../src/schema/mod.rs)
- [Validate Command](../../src/validate/mod.rs) - Similar PK hashing pattern
