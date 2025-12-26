# Migration Generation Feature Design

**Status**: Planning (v1.15.0)
**Date**: 2025-12-24
**Priority**: High

## Overview

The `migrate` command analyzes schema differences between two SQL dumps and generates migration scripts (ALTER TABLE, CREATE INDEX, etc.) to transform the old schema into the new one. It extends the existing `diff` command with actionable migration generation.

## Problem Statement

Current `diff` command shows **what changed** but doesn't generate **how to apply changes**.

Users need to:
- **Generate migrations automatically** — Convert schema diffs into ALTER statements
- **Plan production deployments** — Review migration steps before applying
- **Generate rollback scripts** — Create reverse migrations for safety
- **Detect breaking changes** — Identify migrations that could cause downtime
- **Support multiple dialects** — Generate migrations for MySQL, PostgreSQL, SQLite, MSSQL

Current workarounds require manual SQL writing or ORM migration generators that require framework setup.

## Command Interface

```bash
# Generate migration from schema diff
sql-splitter migrate old.sql new.sql -o migration.sql

# Generate with rollback script
sql-splitter migrate old.sql new.sql -o migration.sql --rollback rollback.sql

# Check for breaking changes only
sql-splitter migrate old.sql new.sql --breaking-changes

# Dry run (show migration without writing)
sql-splitter migrate old.sql new.sql --dry-run

# Generate for specific dialect
sql-splitter migrate old.sql new.sql -o migration.sql --dialect postgres

# Generate data migration helpers
sql-splitter migrate old.sql new.sql -o migration.sql --with-data

# Split into multiple migration files
sql-splitter migrate old.sql new.sql -o migrations/ --split

# Generate only for specific tables
sql-splitter migrate old.sql new.sql --tables "users,orders"
```

## CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Migration file path | stdout |
| `--rollback` | Generate rollback script | none |
| `-d, --dialect` | Target SQL dialect | auto-detect |
| `--breaking-changes` | Show only breaking changes | false |
| `--dry-run` | Preview without writing files | false |
| `--with-data` | Include data migration helpers | false |
| `--split` | Split into separate migration files | false |
| `--tables` | Only migrate specific tables | all |
| `--safe` | Skip breaking changes, warn instead | false |
| `--format` | Output format: `sql`, `json` | sql |

## Migration Types

### 1. Table Operations

#### Add Table
```sql
-- Migration
CREATE TABLE new_table (
  id INT PRIMARY KEY,
  name VARCHAR(255)
);

-- Rollback
DROP TABLE new_table;
```

#### Drop Table (Breaking)
```sql
-- Migration
DROP TABLE old_table;

-- Rollback
CREATE TABLE old_table (
  -- original schema
);
```

#### Rename Table
```sql
-- Migration
ALTER TABLE old_name RENAME TO new_name;

-- Rollback
ALTER TABLE new_name RENAME TO old_name;
```

### 2. Column Operations

#### Add Column
```sql
-- Migration
ALTER TABLE users ADD COLUMN phone VARCHAR(20);

-- Rollback
ALTER TABLE users DROP COLUMN phone;
```

#### Drop Column (Breaking)
```sql
-- Migration
ALTER TABLE users DROP COLUMN deprecated_field;

-- Rollback
ALTER TABLE users ADD COLUMN deprecated_field TEXT;
```

#### Rename Column
```sql
-- Migration (PostgreSQL)
ALTER TABLE users RENAME COLUMN old_name TO new_name;

-- Migration (MySQL)
ALTER TABLE users CHANGE old_name new_name VARCHAR(255);

-- Rollback
ALTER TABLE users RENAME COLUMN new_name TO old_name;
```

#### Modify Column Type (Potentially Breaking)
```sql
-- Migration
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;

-- Rollback
ALTER TABLE users ALTER COLUMN age TYPE INT;
```

#### Add NOT NULL Constraint (Breaking)
```sql
-- Migration (safe with default)
ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '';

-- Migration (unsafe, needs data migration)
ALTER TABLE users ALTER COLUMN email SET NOT NULL;

-- Rollback
ALTER TABLE users ALTER COLUMN email DROP NOT NULL;
```

### 3. Constraint Operations

#### Add Primary Key
```sql
-- Migration
ALTER TABLE users ADD PRIMARY KEY (id);

-- Rollback
ALTER TABLE users DROP PRIMARY KEY;
```

#### Add Foreign Key
```sql
-- Migration
ALTER TABLE orders
  ADD CONSTRAINT fk_user
  FOREIGN KEY (user_id)
  REFERENCES users(id);

-- Rollback
ALTER TABLE orders DROP CONSTRAINT fk_user;
```

#### Add Unique Constraint (Potentially Breaking)
```sql
-- Migration
ALTER TABLE users ADD UNIQUE (email);

-- Rollback
ALTER TABLE users DROP INDEX email; -- MySQL
-- or
DROP INDEX users_email_key; -- PostgreSQL
```

### 4. Index Operations

#### Add Index
```sql
-- Migration
CREATE INDEX idx_users_email ON users(email);

-- Rollback
DROP INDEX idx_users_email;
```

#### Drop Index
```sql
-- Migration
DROP INDEX idx_old_field;

-- Rollback
CREATE INDEX idx_old_field ON users(old_field);
```

## Breaking Change Detection

### Definite Breaking Changes

| Change | Impact | Mitigation |
|--------|--------|------------|
| Drop table | Data loss | Backup required |
| Drop column | Data loss | Backup required |
| Add NOT NULL without DEFAULT | Insert failures | Add DEFAULT or backfill |
| Reduce column size | Data truncation | Validate data first |
| Add UNIQUE on non-unique data | Constraint violations | Deduplicate first |
| Change column type (incompatible) | Type errors | Cast or backfill |

### Potentially Breaking Changes

| Change | Impact | Safe Condition |
|--------|--------|----------------|
| Add NOT NULL with DEFAULT | None | If DEFAULT is acceptable |
| Increase column size | None | Always safe |
| Add index | Performance during creation | Use CONCURRENTLY (PostgreSQL) |
| Add foreign key | Validation failures | If data already valid |

### Detection Output

```bash
sql-splitter migrate old.sql new.sql --breaking-changes

# Output:
Breaking Changes Detected:

1. DROP TABLE sessions
   Impact: All data in 'sessions' will be lost
   Mitigation: Export data before migration

2. ALTER TABLE users DROP COLUMN phone
   Impact: 'phone' data will be permanently deleted
   Mitigation: Backup column data if needed

3. ALTER TABLE users ALTER COLUMN email SET NOT NULL
   Impact: Rows with NULL email will fail constraint
   Mitigation: Backfill NULL values before migration
   Suggested: UPDATE users SET email = 'unknown@example.com' WHERE email IS NULL;

4. ALTER TABLE products ADD UNIQUE (sku)
   Impact: Duplicate SKUs will cause constraint violation
   Mitigation: Find duplicates: SELECT sku, COUNT(*) FROM products GROUP BY sku HAVING COUNT(*) > 1;
```

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── migrate.rs         # CLI handler
├── migrate/
│   ├── mod.rs             # Public API
│   ├── analyzer.rs        # Schema diff analysis
│   ├── generator.rs       # Migration SQL generation
│   ├── breaking.rs        # Breaking change detection
│   ├── rollback.rs        # Rollback script generation
│   ├── dialect/
│   │   ├── mysql.rs       # MySQL-specific syntax
│   │   ├── postgres.rs    # PostgreSQL-specific syntax
│   │   ├── sqlite.rs      # SQLite-specific syntax
│   │   └── mssql.rs       # MSSQL-specific syntax
│   └── output.rs          # Migration file writer
```

### Key Types

```rust
pub struct MigrationConfig {
    pub old_schema: PathBuf,
    pub new_schema: PathBuf,
    pub output: Option<PathBuf>,
    pub rollback: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub breaking_only: bool,
    pub dry_run: bool,
    pub with_data: bool,
    pub split: bool,
    pub tables: Option<Vec<String>>,
    pub safe_mode: bool,
}

pub struct Migration {
    pub operations: Vec<MigrationOp>,
    pub breaking_changes: Vec<BreakingChange>,
}

pub enum MigrationOp {
    CreateTable { table: TableDef },
    DropTable { name: String },
    RenameTable { old: String, new: String },
    AddColumn { table: String, column: ColumnDef },
    DropColumn { table: String, column: String },
    ModifyColumn { table: String, old: ColumnDef, new: ColumnDef },
    AddPrimaryKey { table: String, columns: Vec<String> },
    DropPrimaryKey { table: String },
    AddForeignKey { table: String, fk: ForeignKey },
    DropForeignKey { table: String, name: String },
    AddIndex { table: String, index: Index },
    DropIndex { table: String, name: String },
}

pub struct BreakingChange {
    pub operation: MigrationOp,
    pub severity: Severity,
    pub impact: String,
    pub mitigation: String,
    pub suggested_sql: Option<String>,
}

pub enum Severity {
    Critical,  // Data loss
    High,      // Constraint violations likely
    Medium,    // Performance impact
    Low,       // Minimal risk
}
```

## Dialect-Specific Syntax

### MySQL

```sql
-- Rename column
ALTER TABLE users CHANGE old_name new_name VARCHAR(255);

-- Modify column
ALTER TABLE users MODIFY COLUMN age BIGINT;

-- Add column after specific column
ALTER TABLE users ADD COLUMN phone VARCHAR(20) AFTER email;
```

### PostgreSQL

```sql
-- Rename column
ALTER TABLE users RENAME COLUMN old_name TO new_name;

-- Modify column
ALTER TABLE users ALTER COLUMN age TYPE BIGINT;

-- Create index concurrently (non-blocking)
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);

-- Add constraint with validation
ALTER TABLE users ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID;
ALTER TABLE users VALIDATE CONSTRAINT fk_user;
```

### SQLite

**Limitations:**
- Cannot drop columns (requires table recreation)
- Cannot modify column types (requires table recreation)
- Limited ALTER TABLE support

```sql
-- Add column (supported)
ALTER TABLE users ADD COLUMN phone TEXT;

-- Rename table (supported)
ALTER TABLE old_name RENAME TO new_name;

-- Drop/modify column (requires recreation)
-- 1. Create new table with new schema
CREATE TABLE users_new (id INTEGER PRIMARY KEY, email TEXT);
-- 2. Copy data
INSERT INTO users_new SELECT id, email FROM users;
-- 3. Drop old table
DROP TABLE users;
-- 4. Rename new table
ALTER TABLE users_new RENAME TO users;
```

## Data Migration Helpers

With `--with-data` flag, generate helper comments/scripts:

```sql
-- Migration
ALTER TABLE users ADD COLUMN full_name VARCHAR(255);

-- Data migration helper
-- Option 1: Concatenate existing columns
-- UPDATE users SET full_name = first_name || ' ' || last_name;

-- Option 2: Default value
-- UPDATE users SET full_name = 'Unknown';

ALTER TABLE users ALTER COLUMN email SET NOT NULL;

-- Data migration helper
-- Backfill NULL values before adding constraint:
-- UPDATE users SET email = 'noemail@example.com' WHERE email IS NULL;
```

## Migration Ordering

Operations must be ordered to avoid dependency issues:

1. **Drop foreign keys** (remove dependencies)
2. **Drop indexes** (free resources)
3. **Modify/drop columns**
4. **Modify/drop tables**
5. **Create tables** (establish new structures)
6. **Add columns**
7. **Add indexes**
8. **Add foreign keys** (establish new dependencies)

## Rollback Generation

Reverse operations in opposite order:

```sql
-- Forward migration
CREATE TABLE new_table (...);
ALTER TABLE users ADD COLUMN phone VARCHAR(20);
CREATE INDEX idx_phone ON users(phone);

-- Rollback (reverse order)
DROP INDEX idx_phone;
ALTER TABLE users DROP COLUMN phone;
DROP TABLE new_table;
```

## Edge Cases

### 1. Column Rename vs Drop+Add

**Problem:** Can't distinguish between:
- Rename: `old_name` → `new_name`
- Drop + Add: Remove `old_name`, add `new_name`

**Solution:** Heuristic detection
- Same type, position, constraints → likely rename
- Otherwise → drop + add
- Future: Config file to specify renames

### 2. Table Rename vs Drop+Create

**Same heuristic challenge:**
- Similar schema → likely rename
- Otherwise → drop + create

### 3. Data Type Compatibility

```sql
-- Safe
VARCHAR(100) -> VARCHAR(200)  ✓
INT -> BIGINT                 ✓

-- Unsafe
VARCHAR(200) -> VARCHAR(100)  ✗ (truncation)
BIGINT -> INT                 ✗ (overflow)
VARCHAR -> INT                ✗ (cast failure)
```

### 4. Default Value Backfilling

```sql
-- Not NULL safe if has default
ALTER TABLE users ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'active';

-- Not NULL unsafe without default
ALTER TABLE users ADD COLUMN status VARCHAR(20) NOT NULL;
-- Requires: UPDATE users SET status = 'active' WHERE status IS NULL;
```

## Performance Considerations

| Schema Size | Tables | Time |
|-------------|--------|------|
| Small | 50 | < 1s |
| Medium | 200 | < 3s |
| Large | 500 | < 10s |

**Optimizations:**
- Reuse existing diff infrastructure
- Generate migrations during diff traversal
- Template-based SQL generation

## Testing Strategy

### Unit Tests
- Each migration operation type
- Breaking change detection
- Rollback generation
- Dialect-specific syntax

### Integration Tests
- Real schema evolution (WordPress 5.0 → 6.0)
- All four dialects
- Complex changes (renames, type changes)

### Property Tests
- Forward + rollback = identity
- All breaking changes detected

## Example Workflows

### 1. Generate and Review

```bash
sql-splitter migrate v1.sql v2.sql --dry-run
# Review changes

sql-splitter migrate v1.sql v2.sql -o upgrade.sql --rollback downgrade.sql
```

### 2. Safe Production Deployment

```bash
# Check for breaking changes
sql-splitter migrate prod_current.sql prod_target.sql --breaking-changes

# Generate migration
sql-splitter migrate prod_current.sql prod_target.sql -o migration.sql --safe

# Review before applying
cat migration.sql
mysql -u root production < migration.sql
```

### 3. Multi-Environment Deployment

```bash
# Generate for PostgreSQL
sql-splitter migrate old.sql new.sql -o pg_migration.sql --dialect postgres

# Generate for MySQL
sql-splitter migrate old.sql new.sql -o mysql_migration.sql --dialect mysql
```

## Estimated Effort

| Component | Effort |
|-----------|--------|
| Schema diff reuse (from diff command) | 1 hour |
| Migration operation types | 4 hours |
| SQL generation per dialect | 8 hours |
| Breaking change detection | 4 hours |
| Rollback generation | 3 hours |
| Operation ordering | 2 hours |
| Data migration helpers | 3 hours |
| CLI integration | 2 hours |
| Testing | 8 hours |
| Documentation | 3 hours |
| **Total** | **~40 hours** |

## Future Enhancements

1. **Smart Rename Detection** — ML-based heuristics for rename vs drop+add
2. **Multi-Step Migrations** — Generate incremental migrations for large changes
3. **Migration Simulation** — Test migrations on sample data
4. **Migration Metrics** — Estimate execution time and locking
5. **ORM Integration** — Generate Rails/Django/Doctrine migrations
6. **Schema Registry** — Track migration history across versions

## Related Documents

- [Diff Feature](../archived/DIFF_FEATURE.md) — Foundation for diff analysis
- [Graph Feature](GRAPH_FEATURE.md) — Used for dependency ordering
- [Roadmap](../ROADMAP.md)
