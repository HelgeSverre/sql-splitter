# Diff Command Design

**Status**: Draft  
**Date**: 2025-12-20

## Overview

The `diff` command compares two SQL dump files and shows schema and/or data differences. It can generate migration SQL to transform one dump into another.

## Command Interface

```bash
# Compare two dumps (schema + data summary)
sql-splitter diff old.sql new.sql

# Schema-only comparison
sql-splitter diff old.sql new.sql --schema-only

# Data-only comparison
sql-splitter diff old.sql new.sql --data-only

# Compare specific table
sql-splitter diff old.sql new.sql --table users

# Generate migration SQL
sql-splitter diff old.sql new.sql --output migration.sql

# Output as JSON for programmatic use
sql-splitter diff old.sql new.sql --format json

# Ignore specific tables
sql-splitter diff old.sql new.sql --exclude cache,sessions,logs

# Show row-level details
sql-splitter diff old.sql new.sql --table users --verbose
```

## CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output file for migration SQL | stdout |
| `-t, --table` | Compare specific table(s) only | all |
| `--exclude` | Exclude tables from comparison | none |
| `--schema-only` | Compare schema only, ignore data | false |
| `--data-only` | Compare data only, ignore schema | false |
| `--format` | Output format: `text`, `sql`, `json` | text |
| `--verbose` | Show detailed row-level changes | false |
| `--primary-key` | Override PK for data comparison | auto-detect |
| `-d, --dialect` | SQL dialect | auto-detect |
| `-p, --progress` | Show progress bar | false |
| `--ignore-order` | Ignore column order differences | false |
| `--ignore-whitespace` | Ignore whitespace in definitions | true |

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
  Table 'users':
    + 45 rows added
    - 12 rows removed
    ~ 89 rows modified
  
  Table 'products':
    + 120 rows added
    ~ 34 rows modified

Summary:
  Tables: 2 added, 1 removed, 5 modified
  Rows: 165 added, 12 removed, 123 modified
```

### SQL — Migration Script

```sql
-- Migration: old.sql → new.sql
-- Generated: 2025-12-20 12:00:00

-- Schema Changes

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
CREATE INDEX `idx_users_phone` ON `users` (`phone`);

-- Data Changes

-- Table: users (45 inserts, 12 deletes, 89 updates)
INSERT INTO `users` (id, email, name) VALUES
(101, 'new1@example.com', 'New User 1'),
(102, 'new2@example.com', 'New User 2');

DELETE FROM `users` WHERE id IN (5, 12, 23);

UPDATE `users` SET email = 'updated@example.com', name = 'Updated Name' WHERE id = 7;
```

### JSON — Programmatic Use

```json
{
  "old_file": "old.sql",
  "new_file": "new.sql",
  "schema": {
    "tables_added": ["audit_logs"],
    "tables_removed": ["legacy_data"],
    "tables_modified": {
      "users": {
        "columns_added": [{"name": "phone", "type": "VARCHAR(20)"}],
        "columns_removed": [{"name": "fax", "type": "VARCHAR(20)"}],
        "columns_modified": [{
          "name": "email",
          "old_type": "VARCHAR(100)",
          "new_type": "VARCHAR(255)"
        }],
        "indexes_added": [{"name": "idx_users_phone", "columns": ["phone"]}]
      }
    }
  },
  "data": {
    "users": {
      "rows_added": 45,
      "rows_removed": 12,
      "rows_modified": 89
    }
  },
  "summary": {
    "tables_added": 2,
    "tables_removed": 1,
    "tables_modified": 5,
    "rows_added": 165,
    "rows_removed": 12,
    "rows_modified": 123
  }
}
```

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
- DEFAULT value
- AUTO_INCREMENT/SERIAL
- Character set/collation (MySQL)

#### Indexes
Compare by index name or column combination:
- **Added/Removed/Modified**
- Compare: columns, unique, type (BTREE, FULLTEXT, etc.)

#### Constraints
- PRIMARY KEY changes
- FOREIGN KEY changes
- UNIQUE constraints
- CHECK constraints

### Data Comparison

#### Primary Key Detection

1. Parse CREATE TABLE for PRIMARY KEY
2. Use first UNIQUE index if no PK
3. Use `--primary-key` override if specified
4. Error if no PK can be determined

#### Row Matching

Match rows by primary key value:

```
Old: (1, 'alice@old.com', 'Alice')
New: (1, 'alice@new.com', 'Alice Smith')
→ Modified: id=1, email changed, name changed
```

#### Change Detection

| Old | New | Result |
|-----|-----|--------|
| Row exists | Row exists, same | No change |
| Row exists | Row exists, different | Modified |
| Row exists | Row missing | Removed |
| Row missing | Row exists | Added |

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── diff.rs              # CLI handler
├── differ/
│   ├── mod.rs               # Public API
│   ├── schema.rs            # Schema comparison
│   ├── data.rs              # Data comparison
│   ├── parser.rs            # Schema extraction
│   ├── matcher.rs           # Row matching
│   └── output/
│       ├── text.rs          # Text formatter
│       ├── sql.rs           # SQL generator
│       └── json.rs          # JSON formatter
```

### Key Types

```rust
pub struct DiffConfig {
    pub old_file: PathBuf,
    pub new_file: PathBuf,
    pub dialect: SqlDialect,
    pub tables: Option<Vec<String>>,
    pub exclude: Vec<String>,
    pub schema_only: bool,
    pub data_only: bool,
    pub format: DiffFormat,
    pub verbose: bool,
    pub primary_key_override: Option<Vec<String>>,
    pub progress: bool,
}

pub enum DiffFormat {
    Text,
    Sql,
    Json,
}

pub struct DiffResult {
    pub schema: SchemaDiff,
    pub data: DataDiff,
}

pub struct SchemaDiff {
    pub tables_added: Vec<TableDef>,
    pub tables_removed: Vec<String>,
    pub tables_modified: Vec<TableModification>,
}

pub struct TableModification {
    pub table_name: String,
    pub columns_added: Vec<ColumnDef>,
    pub columns_removed: Vec<ColumnDef>,
    pub columns_modified: Vec<ColumnChange>,
    pub indexes_added: Vec<IndexDef>,
    pub indexes_removed: Vec<IndexDef>,
    pub indexes_modified: Vec<IndexChange>,
}

pub struct DataDiff {
    pub tables: HashMap<String, TableDataDiff>,
}

pub struct TableDataDiff {
    pub rows_added: Vec<Row>,      // Or just count if not verbose
    pub rows_removed: Vec<PkValue>,
    pub rows_modified: Vec<RowChange>,
    pub added_count: u64,
    pub removed_count: u64,
    pub modified_count: u64,
}

pub struct RowChange {
    pub pk: PkValue,
    pub changes: Vec<ColumnValueChange>,
}
```

### Algorithm

#### Two-Pass Approach

**Pass 1: Schema Extraction**
1. Parse old file, extract all CREATE TABLE statements
2. Parse new file, extract all CREATE TABLE statements
3. Compare schemas, build SchemaDiff

**Pass 2: Data Comparison**
1. For each table in both files:
   - Build hash map of old rows by PK
   - Stream new file, compare each row
   - Track added/modified/removed

#### Memory Considerations

For large tables, can't hold all rows in memory:

**Option A: Two-file streaming (for sorted dumps)**
- Assumes rows sorted by PK
- Merge-sort comparison

**Option B: Chunked hashing**
- Hash rows into buckets by PK
- Compare buckets independently
- Spill large buckets to temp files

**Option C: Database-backed (future)**
- Load both into SQLite temp DB
- Use SQL for comparison

## Edge Cases

### 1. Column Order Changes
```sql
-- Old: (id, name, email)
-- New: (id, email, name)
```
With `--ignore-order`: No change
Without: Report as modified

### 2. Case Sensitivity
```sql
-- Old: CREATE TABLE Users
-- New: CREATE TABLE users
```
Handle per-dialect (MySQL case-insensitive on some platforms)

### 3. Renamed Tables
Cannot auto-detect renames. Shows as remove + add.
Future: `--renames "old_name:new_name"`

### 4. Renamed Columns
Same as tables. Future: heuristic matching by type/position.

### 5. No Primary Key
- Error by default
- `--primary-key "col1,col2"` override
- `--allow-no-pk` to skip data comparison for that table

### 6. Large Tables
Memory limits for data comparison:
- `--max-rows 100000` to limit comparison
- `--sample-data` to compare random sample

### 7. Binary Data
Skip BLOB/BYTEA columns in detailed diff, just note "binary data differs"

## Performance Considerations

### Schema Comparison
- Fast: only parses CREATE statements
- O(tables * columns)

### Data Comparison
- Memory: O(rows in smaller table) for hash approach
- Time: O(rows in both tables)
- Disk: May need temp storage for large tables

### Targets

| File Size | Target Time |
|-----------|-------------|
| 100 MB | < 5 seconds |
| 1 GB | < 30 seconds |
| 10 GB | < 5 minutes |

## Testing Strategy

### Unit Tests
- Schema parsing for each dialect
- Column comparison logic
- Index comparison logic
- Row matching algorithms

### Integration Tests
- Known diff scenarios with expected output
- Roundtrip: apply migration.sql, diff should be empty
- All three dialects
- Large file handling

### Edge Case Tests
- Empty tables
- Tables with no PK
- Unicode data
- Binary data
- NULL values

## Example Workflows

### 1. Pre-Deployment Review

```bash
# Compare staging vs production schema
sql-splitter diff prod_backup.sql staging_dump.sql --schema-only

# Generate and review migration
sql-splitter diff prod.sql staging.sql --output migration.sql
cat migration.sql  # Review before applying
```

### 2. Audit Data Changes

```bash
# What changed between backups?
sql-splitter diff backup_monday.sql backup_friday.sql --data-only --format json > changes.json
```

### 3. Validate Migration

```bash
# After migration, verify no unintended changes
sql-splitter diff expected.sql actual.sql
# Should show no differences
```

### 4. Generate Sync Script

```bash
# Sync dev database from production subset
sql-splitter diff dev.sql prod_subset.sql --output sync.sql
mysql dev_db < sync.sql
```

## Estimated Effort

| Component | Effort |
|-----------|--------|
| CLI and config | 2 hours |
| Schema parser (CREATE TABLE) | 6 hours |
| Schema comparison | 4 hours |
| Data comparison (hash-based) | 8 hours |
| Text output formatter | 3 hours |
| SQL migration generator | 6 hours |
| JSON output formatter | 2 hours |
| Memory-efficient large file handling | 6 hours |
| Testing | 8 hours |
| **Total** | **~45 hours** |

## Future Enhancements

1. **Rename detection**: Heuristic matching for renamed tables/columns
2. **Ignore patterns**: `--ignore-columns "*.updated_at"`
3. **Semantic comparison**: Ignore formatting differences in DEFAULT values
4. **Patch format**: Standard diff/patch output
5. **Three-way merge**: Common ancestor comparison
6. **Interactive mode**: Approve/reject each change

## Related

- [Split Command](../../src/cmd/split.rs)
- [Convert Feature](CONVERT_FEASIBILITY.md)
- [Merge Feature](MERGE_FEATURE.md)
