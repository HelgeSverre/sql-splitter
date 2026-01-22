# Shard Command Design

**Status**: ✅ Implemented (v1.6.0)  
**Date**: 2025-12-20  
**Source**: Oracle feasibility analysis + real-world schema investigation

## Overview

The `shard` command extracts tenant-specific data from multi-tenant SQL dumps. It resolves FK chains to include all data belonging to a tenant, even from tables that don't have a direct tenant column.

This is a common need in multi-tenant SaaS applications where:

- Most tables have a `company_id` or `tenant_id` column
- Some tables (pivot/junction, child tables) don't have the tenant column directly
- You need to extract one tenant's data for testing, debugging, or migration

**Key differentiator:** Most tools can filter by a column value, but they can't follow FK chains to include related data from tables without that column.

## Real-World Schema Analysis

Analysis of two production Laravel/MySQL multi-tenant applications revealed:

| Metric                      | App 1    | App 2    |
| --------------------------- | -------- | -------- |
| Total tables                | 52       | 125      |
| Tables with `company_id`    | 22 (42%) | 44 (35%) |
| Tables without `company_id` | 30 (58%) | 81 (65%) |

**Tables without direct tenant column fall into categories:**

| Category             | Examples                                  | How to Handle                         |
| -------------------- | ----------------------------------------- | ------------------------------------- |
| **Child tables**     | `invoice_items` → `invoices`              | Follow FK to tenant-owned parent      |
| **Junction/pivot**   | `permission_role`, `role_user`            | Include if either FK hits tenant data |
| **Self-referential** | `comments.parent_id`, `folders.parent_id` | Closure over parent chain             |
| **Lookup/reference** | `permissions`, `roles`, `migrations`      | Include fully or skip                 |
| **System/framework** | `failed_jobs`, `job_batches`, `cache`     | Skip by default                       |

## Command Interface

```bash
# Extract single tenant by company_id
sql-splitter shard dump.sql -o tenant_5.sql \
  --tenant-column company_id \
  --tenant-value 5

# With auto-detection (looks for company_id, tenant_id, organization_id)
sql-splitter shard dump.sql -o tenant_5.sql --tenant-value 5

# Include global lookup tables
sql-splitter shard dump.sql -o tenant_5.sql \
  --tenant-value 5 \
  --include-global lookups

# Specify explicit root tables
sql-splitter shard dump.sql -o tenant_5.sql \
  --tenant-value 5 \
  --root-tables companies,users

# Use config file for complex schemas
sql-splitter shard dump.sql -o tenant_5.sql \
  --tenant-value 5 \
  --config shard.yaml

# Dry run to see what would be extracted
sql-splitter shard dump.sql --tenant-value 5 --dry-run

# Extract multiple tenants to separate files
sql-splitter shard dump.sql -o shards/ \
  --tenant-column company_id \
  --tenant-values 1,2,3,5,8

# Extract by hash partitioning (for parallel loading)
sql-splitter shard dump.sql -o chunks/ \
  --tenant-column id \
  --table companies \
  --hash --partitions 8
```

## CLI Options

| Flag               | Description                                     | Default     |
| ------------------ | ----------------------------------------------- | ----------- |
| `-o, --output`     | Output file or directory                        | stdout      |
| `--tenant-column`  | Column name for tenant key                      | auto-detect |
| `--tenant-value`   | Tenant key value to extract                     | required    |
| `--tenant-values`  | Multiple tenant values (comma-separated)        | —           |
| `--root-tables`    | Explicit tenant root tables                     | auto-detect |
| `--include-global` | Global table handling: `none`, `lookups`, `all` | lookups     |
| `-d, --dialect`    | SQL dialect                                     | auto-detect |
| `-p, --progress`   | Show progress bar                               | false       |
| `--dry-run`        | Show statistics without writing                 | false       |
| `--config`         | YAML config for table classification            | —           |
| `--strict-fk`      | Fail on FK integrity issues                     | false       |
| `--include-schema` | Include CREATE TABLE statements                 | true        |
| `--hash`           | Use hash-based sharding                         | false       |
| `--partitions`     | Number of hash partitions                       | —           |

## How It Works

### High-Level Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SHARD PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Phase 0: Schema Analysis                                               │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ • Parse CREATE TABLE statements                                  │  │
│  │ • Extract columns, PKs, FKs                                      │  │
│  │ • Build dependency graph                                         │  │
│  │ • Classify tables: tenant-root, dependent, junction, global      │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Phase 1: Internal Split                                                │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ • Stream through dump file once                                  │  │
│  │ • Split into per-table temp files: tmp/<table>.data              │  │
│  │ • Write schema to tmp/schema.sql                                 │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Phase 2: Tenant Selection (FK-ordered)                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ • Process tables in topological order (roots first)              │  │
│  │ • For each table:                                                │  │
│  │   - Stream through tmp/<table>.data                              │  │
│  │   - Check tenant column OR FK membership                         │  │
│  │   - Record selected PKs                                          │  │
│  │   - Emit matching rows to output                                 │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  Output: Concatenate schema.sql + selected data in table order          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Table Classification Algorithm

```rust
fn classify_table(
    table: &TableSchema,
    graph: &SchemaGraph,
    config: &ShardConfig,
) -> TableClassification {
    // 1. Check explicit config overrides
    if let Some(role) = config.table_role(&table.name) {
        return role;
    }

    // 2. Check for tenant column
    if table.has_column(&config.tenant_column) {
        return TableClassification::TenantRoot;
    }

    // 3. Check if reachable from tenant roots via FK chain
    if graph.is_reachable_from_tenant_roots(&table.name) {
        // Has FK path to a tenant table
        if table.is_junction_table() {
            return TableClassification::Junction;
        }
        return TableClassification::TenantDependent;
    }

    // 4. Check for known system/framework patterns
    if is_system_table(&table.name) {
        return TableClassification::System;
    }

    // 5. Default to lookup/global
    TableClassification::Lookup
}

fn is_system_table(name: &str) -> bool {
    matches!(name,
        "migrations" | "failed_jobs" | "job_batches" | "jobs" |
        "password_resets" | "sessions" | "cache" | "cache_locks" |
        "telescope_entries" | "telescope_monitoring" |
        "pulse_entries" | "pulse_values" | "pulse_aggregates"
    )
}
```

### FK Chain Resolution

For a table without a tenant column (e.g., `invoice_items`):

```
invoice_items
    └── invoice_id → invoices.id
                         └── company_id = 5 ✓ (tenant match)
```

**Algorithm:**

1. `invoices` is processed first (has `company_id`, is tenant-root)
2. Selected invoice IDs are recorded: `{101, 205, 307, ...}`
3. `invoice_items` is processed later (dependent table)
4. For each row, check: `invoice_id ∈ selected_pks["invoices"]`?
5. If yes, include the row

### Handling Junction Tables

Junction tables like `permission_role`:

```sql
CREATE TABLE permission_role (
    permission_id INT REFERENCES permissions(id),
    role_id INT REFERENCES roles(id)
);
```

Where `roles` has `company_id` but `permissions` is global.

**Inclusion rule:** Include row if ANY FK points to a selected row in a tenant-owned table.

```rust
fn should_include_junction_row(
    row: &Row,
    table: &TableSchema,
    selected_pks: &HashMap<String, PkSet>,
) -> bool {
    for fk in &table.fk_edges {
        if let Some(pks) = selected_pks.get(&fk.to_table) {
            let fk_value = row.get_column(&fk.from_columns);
            if pks.contains(&fk_value) {
                return true;
            }
        }
    }
    false
}
```

### Self-Referential Tables

Tables like `comments.parent_id → comments.id` or `folders.parent_id → folders.id`:

```
comments (id=10, parent_id=5, company_id=1)
    └── parent: comments (id=5, parent_id=2, company_id=1)
        └── parent: comments (id=2, parent_id=NULL, company_id=1)
```

**Algorithm:** Ancestor closure

```rust
fn compute_self_referential_closure(
    table: &str,
    self_fk_column: &str,
    initial_pks: &PkSet,
    table_data: &Path,
) -> PkSet {
    let mut selected = initial_pks.clone();
    let mut needed_parents: PkSet = collect_parent_refs(&selected, self_fk_column);

    while !needed_parents.is_empty() {
        // Scan table file for rows with PK in needed_parents
        let newly_selected = scan_for_pks(table_data, &needed_parents);

        // Add to selected set
        selected.extend(newly_selected.iter().cloned());

        // Find their parents
        let new_parents = collect_parent_refs(&newly_selected, self_fk_column);
        needed_parents = new_parents.difference(&selected).cloned().collect();
    }

    selected
}
```

This may require multiple passes over the table's temp file, but only for self-referential tables.

---

## Configuration File

For complex schemas, use a YAML config:

```yaml
# shard.yaml

tenant:
  column: company_id
  # Optional: explicit root tables (otherwise auto-detected)
  root_tables:
    - companies
    - users

# Table classification overrides
tables:
  # System/framework tables (skip entirely)
  migrations:
    role: system
  failed_jobs:
    role: system
  job_batches:
    role: system
  telescope_entries:
    role: system

  # Lookup tables (include fully or skip)
  permissions:
    role: lookup
    include: true
  roles:
    role: lookup
    include: true # Note: roles might have company_id in some apps

  # Junction tables (explicit classification)
  permission_role:
    role: junction
  role_user:
    role: junction
  taggables:
    role: junction

  # Large tables with special handling
  activity_log:
    role: tenant
    # Future: limit rows
    # max_rows: 1000

  # Self-referential tables (auto-detected, but can be explicit)
  comments:
    self_fk: parent_id
  folders:
    self_fk: parent_id

# Global table handling
include_global: lookups # none | lookups | all
```

---

## Cross-Dialect Considerations

### MySQL / MariaDB

**Characteristics:**

- Backtick-quoted identifiers
- Backslash escapes in strings
- Multi-row INSERT: `INSERT INTO t VALUES (1,'a'), (2,'b'), ...`

**Value parsing:**

```rust
fn parse_mysql_insert_values(stmt: &[u8]) -> Vec<Row> {
    // Handle: strings with \' and \\, NULL, numbers, hex
    // Split by ), (
    // Watch for strings containing ), (
}
```

### PostgreSQL

**Characteristics:**

- Double-quote identifiers (optional)
- `''` for escaped quotes in strings
- COPY FROM stdin format (more common than INSERT)
- Schema-qualified names: `public.users`

**COPY parsing:**

```rust
fn parse_postgres_copy_data(data: &[u8]) -> Vec<Row> {
    // Tab-separated values
    // \N for NULL
    // \\ for backslash, \t for tab, \n for newline
    // Lines terminated by \n
    // Block terminated by \.\n
}
```

### SQLite

**Characteristics:**

- Double-quote or backtick identifiers
- `''` for escaped quotes
- INSERT statements (no COPY)
- Simpler overall

---

## Memory Model

### PK Tracking

```rust
/// Efficient PK value representation
pub enum PkValue {
    Int(i64),          // Most common: auto-increment IDs
    BigInt(i128),      // Large IDs, ULIDs as integers
    Text(Box<str>),    // UUIDs, string PKs
}

/// Composite PK support
pub type PkTuple = SmallVec<[PkValue; 2]>;

/// Per-table set of selected PKs
pub type PkSet = AHashSet<PkTuple>;

/// Sharding state
pub struct ShardState {
    /// Only track PKs for tables that are FK targets
    selected_pks: HashMap<String, PkSet>,

    /// Statistics
    stats: ShardStats,
}
```

### Memory Budget

| Scenario      | Selected Rows | Memory  |
| ------------- | ------------- | ------- |
| Small tenant  | 10,000        | ~400 KB |
| Medium tenant | 100,000       | ~4 MB   |
| Large tenant  | 1,000,000     | ~40 MB  |
| Very large    | 10,000,000    | ~400 MB |

**Guardrails:**

```rust
const DEFAULT_MAX_SELECTED_ROWS: usize = 10_000_000;

if total_selected > config.max_selected_rows {
    return Err(anyhow!(
        "Selected {} rows exceeds limit of {}. \
         This tenant may own most of the data. \
         Use --max-selected-rows to increase limit.",
        total_selected, config.max_selected_rows
    ));
}
```

---

## Performance

### Targets

| Metric           | Target              | Notes                       |
| ---------------- | ------------------- | --------------------------- |
| Schema analysis  | <5s for 100+ tables | Single pass, regex-based    |
| Internal split   | 200+ MB/s           | Reuses existing split logic |
| Tenant selection | 100-150 MB/s        | Row parsing overhead        |
| Memory           | O(selected_rows)    | Not O(file_size)            |

### Optimizations

1. **Single parse of original dump**: Schema + split in one pass
2. **Per-table temp files**: Process in any order (topological)
3. **Lazy value decoding**: Only parse PK/FK/tenant columns
4. **Fast hashing**: `ahash` for PK sets
5. **Streaming writes**: Buffer and flush incrementally

### Disk Usage

Temp files use approximately the same space as the original dump:

- 10 GB dump → ~10-12 GB temp files
- Cleaned up automatically after completion

---

## Edge Cases

### 1. Tenant Owns Most Data

If a single tenant owns 90% of the data:

- PK sets will be large
- Memory usage increases proportionally
- Consider: Is sharding the right approach? Maybe just filter on import.

### 2. Orphaned FK References (Original Data Issues)

```
⚠️ Warning: 15 rows in 'orders' reference non-existent user_id values
   This indicates orphaned data in the original dump.
```

With `--strict-fk`: Command fails.

### 3. Tables Without Primary Key

```
⚠️ Warning: Table 'legacy_audit' has no PRIMARY KEY
   Cannot track FK references for tenant selection.
   Including all rows (may include data from other tenants).
```

### 4. Circular FK References (Rare)

Detected via SCC analysis. Handled by processing entire cycle as a unit with relaxed inclusion rules.

### 5. Very Deep FK Chains

```
a → b → c → d → e → f → company_id
```

Handled correctly by topological ordering. Table `a` is processed last, after all intermediate tables have their PKs recorded.

---

## Output Format

### Single Tenant Output

```sql
-- Sharded from: production.sql
-- Date: 2025-12-20 14:30:00
-- Tenant column: company_id
-- Tenant value: 5
-- Dialect: mysql
--
-- Statistics:
--   Tables processed: 52
--   Tables with data: 38
--   Tables skipped: 7 (system)
--   Global tables included: 3 (lookups)
--   Total rows: 45,678 (from 1,234,567 original)
--   FK integrity: OK

-- Schema

CREATE TABLE `companies` (...);
CREATE TABLE `users` (...);
...

-- Data

INSERT INTO `companies` VALUES (5, 'Acme Corp', ...);

INSERT INTO `users` VALUES
(12, 'alice@acme.com', 5),
(15, 'bob@acme.com', 5),
...;

INSERT INTO `orders` VALUES ...;
```

### Multi-Tenant Output (Directory)

```
shards/
├── tenant_1.sql
├── tenant_2.sql
├── tenant_5.sql
├── tenant_8.sql
└── global/
    ├── schema.sql
    ├── permissions.sql
    └── roles.sql
```

---

## Example Workflows

### 1. Extract Single Tenant for Debugging

```bash
# Customer reported issue, need their data locally
sql-splitter shard prod-backup.sql.gz -o customer_acme.sql \
  --tenant-value 42 \
  --include-global lookups \
  -p  # Show progress
```

### 2. Create Tenant-Specific Test Fixtures

```bash
# Each test tenant gets own fixture
for tenant_id in 1 2 3; do
  sql-splitter shard dump.sql -o "fixtures/tenant_${tenant_id}.sql" \
    --tenant-value $tenant_id \
    --include-global none \
    --exclude activity_log,telescope_entries
done
```

### 3. Migrate Tenant to Separate Database

```bash
# Extract tenant data for migration
sql-splitter shard prod.sql.gz -o migrate_tenant_5.sql \
  --tenant-value 5 \
  --include-schema \
  --include-global all \
  --strict-fk  # Ensure data integrity

# Import to new database
mysql -u root new_tenant_db < migrate_tenant_5.sql
```

### 4. Hash-Based Parallel Loading

```bash
# Split data into 8 chunks for parallel import
sql-splitter shard dump.sql -o chunks/ \
  --table companies \
  --hash \
  --partitions 8

# Load in parallel
for i in {0..7}; do
  mysql -u root db < "chunks/partition_${i}.sql" &
done
wait
```

---

## Implementation Effort

| Component                   | Effort        | Shared With                   |
| --------------------------- | ------------- | ----------------------------- |
| CLI argument parsing        | 2 hours       | —                             |
| Schema graph (FK parsing)   | 6 hours       | `sample --preserve-relations` |
| Table classification        | 3 hours       | `sample`                      |
| Internal split (temp files) | 2 hours       | —                             |
| PK tracking data structures | 2 hours       | `sample`                      |
| Row parsing (INSERT/COPY)   | 4 hours       | `sample`                      |
| Tenant selection logic      | 4 hours       | —                             |
| Junction table handling     | 2 hours       | `sample`                      |
| Self-FK closure             | 3 hours       | `sample`                      |
| Output generation           | 2 hours       | —                             |
| Config file parsing         | 2 hours       | `sample`                      |
| Progress reporting          | 1 hour        | Existing                      |
| Multi-tenant sharding       | 4 hours       | —                             |
| Hash-based sharding         | 3 hours       | —                             |
| Testing                     | 8 hours       | —                             |
| **Total**                   | **~48 hours** |                               |

**Note:** ~20 hours of this is shared with `sample --preserve-relations`, so implementing both features reduces total effort.

---

## Future Enhancements

1. **Streaming multi-tenant**: Extract all tenants in single pass
2. **Key range partitioning**: `--ranges "2024-01-01,2024-06-01,2025-01-01"`
3. **Tenant mapping file**: JSON/YAML mapping tenant_id → shard
4. **Parallel processing**: Process independent tables concurrently
5. **Incremental sharding**: Add new data to existing shards
6. **Compression support**: Output to `.sql.gz`

---

## Risks & Mitigations

| Risk                    | Mitigation                                     |
| ----------------------- | ---------------------------------------------- |
| Tenant owns most data   | Memory limit + clear error message             |
| Complex FK patterns     | Conservative cycle handling + warnings         |
| Dialect variations      | Comprehensive test fixtures for each dialect   |
| Orphaned data in source | Detect and warn; `--strict-fk` for enforcement |
| Very large dumps        | Temp file approach keeps memory bounded        |

---

## Related

- [Sample Feature](SAMPLE_FEATURE.md) — Shares FK graph and PK tracking
- [Split Command](../../src/cmd/split.rs) — Base streaming architecture
- [Merge Feature](MERGE_FEATURE.md) — Combine split files
- [Additional Ideas](ADDITIONAL_IDEAS.md) — Hash sharding, key ranges
