# Sample Command Design

**Status**: ✅ Fully Implemented (v1.5.0)  
**Date**: 2025-12-20  
**Updated**: 2025-12-20 (Full implementation complete)

## Overview

The `sample` command creates reduced datasets from large SQL dumps for development, testing, and CI environments. It extracts a representative subset of data while optionally preserving referential integrity through dependency-aware FK chain resolution.

## Command Interface

```bash
# Sample 10% of each table
sql-splitter sample dump.sql -o dev.sql --percent 10

# Fixed row count per table
sql-splitter sample dump.sql -o dev.sql --rows 1000

# Preserve foreign key relationships (dependency-aware)
sql-splitter sample dump.sql -o dev.sql --rows 500 --preserve-relations

# Start from specific "root" tables and follow FK chains
sql-splitter sample dump.sql -o dev.sql --percent 5 --preserve-relations --root-tables orders

# Sample specific tables only
sql-splitter sample dump.sql -o dev.sql --percent 20 --tables users,posts,comments

# Exclude large tables
sql-splitter sample dump.sql -o dev.sql --percent 10 --exclude cache,logs,sessions

# Include global/lookup tables in full
sql-splitter sample dump.sql -o dev.sql --rows 100 --preserve-relations --include-global lookups

# Reproducible sampling with seed
sql-splitter sample dump.sql -o dev.sql --percent 5 --seed 42

# Different strategies per table
sql-splitter sample dump.sql -o dev.sql --config sample.yaml

# Output statistics only (dry run)
sql-splitter sample dump.sql --percent 10 --dry-run
```

## CLI Options

| Flag                   | Description                                                  | Default     |
| ---------------------- | ------------------------------------------------------------ | ----------- |
| `-o, --output`         | Output SQL file path                                         | stdout      |
| `--percent`            | Sample percentage (1-100)                                    | —           |
| `--rows`               | Fixed row count per table                                    | —           |
| `--preserve-relations` | Maintain foreign key integrity via FK chain resolution       | false       |
| `--root-tables`        | Explicit root tables for sampling (comma-separated)          | auto-detect |
| `--include-global`     | How to handle global/lookup tables: `none`, `lookups`, `all` | lookups     |
| `--tables`             | Only sample these tables (comma-separated)                   | all         |
| `--exclude`            | Exclude these tables (comma-separated)                       | none        |
| `--seed`               | Random seed for reproducibility                              | random      |
| `--config`             | YAML config for per-table settings                           | —           |
| `-d, --dialect`        | SQL dialect: `mysql`, `postgres`, `sqlite`                   | auto-detect |
| `-p, --progress`       | Show progress bar                                            | false       |
| `--dry-run`            | Show statistics without writing                              | false       |
| `--include-schema`     | Include CREATE TABLE statements                              | true        |
| `--strict-fk`          | Fail if any FK integrity issues detected                     | false       |

## Sampling Modes

### 1. Percentage-Based (`--percent`)

Sample N% of rows from each table randomly.

```bash
sql-splitter sample dump.sql -o dev.sql --percent 10
```

**Behavior:**

- Each table gets ~10% of its rows
- Minimum 1 row per table (if table has data)
- Random selection using reservoir sampling

### 2. Fixed Row Count (`--rows`)

Sample exactly N rows from each table.

```bash
sql-splitter sample dump.sql -o dev.sql --rows 1000
```

**Behavior:**

- Each table gets up to 1000 rows
- Tables with fewer rows get all rows
- Random selection

### 3. Dependency-Aware Sampling (`--preserve-relations`)

**This is the key differentiating feature.** Most tools sample tables independently; maintaining a consistent relational slice across FK chains is rare and valuable.

```bash
sql-splitter sample dump.sql -o dev.sql --rows 100 --preserve-relations
```

**What it does:**

- Parses CREATE TABLE statements to build FK dependency graph
- Samples from "root" tables first
- Automatically includes all referenced parent rows
- Recursively follows FK chains to ensure referential integrity
- Result: A consistent subset with no broken foreign key references

**Example FK chain resolution:**

```
orders (100 rows sampled)
  └── users (referenced user_id values automatically included)
  └── products (referenced product_id values automatically included)
       └── categories (referenced category_id values automatically included)
```

#### Starting from Specific Root Tables

```bash
# Sample 5% starting from orders, include all referenced data
sql-splitter sample dump.sql -o subset.sql --percent 5 --preserve-relations --root-tables orders
```

This is useful when you want to extract a coherent slice of data centered around specific entities (e.g., "give me 100 orders with all their related data").

### 4. Config-Based (Advanced)

Per-table sampling strategies via YAML config.

```yaml
# sample.yaml
default:
  percent: 10

# Table classification for --preserve-relations
classification:
  global:
    - migrations
    - permissions
    - roles
  system:
    - failed_jobs
    - job_batches
    - cache
  lookup:
    - countries
    - states
    - currencies

tables:
  users:
    rows: 500

  posts:
    percent: 5

  comments:
    percent: 2

  sessions:
    skip: true

  audit_logs:
    skip: true

  products:
    rows: 100
    # Future: conditional sampling
    # where: "active = 1"
```

```bash
sql-splitter sample dump.sql -o dev.sql --config sample.yaml
```

---

## Dependency-Aware Sampling: Deep Dive

### The Problem

In multi-tenant and relational databases, tables have complex FK relationships:

```
invoice_items → invoices → customers → companies
             ↘ products → categories
```

Simple random sampling breaks these relationships:

- Sampled `invoice_items` may reference `invoices` that weren't sampled
- Import fails with FK constraint violations
- Manual fixing is tedious and error-prone

### The Solution: FK Chain Resolution

Build a dependency graph from schema DDL and use it to:

1. Determine sampling order (topological sort)
2. Track which PK values have been selected
3. Automatically include referenced rows

### Algorithm

```
Phase 0: Schema Analysis (streaming)
├── Parse CREATE TABLE statements
├── Extract column names, types, PK definitions
├── Parse FOREIGN KEY constraints (inline and ALTER TABLE)
├── Build FK dependency graph
└── Classify tables: root, dependent, junction, lookup, system

Phase 1: Internal Split (streaming)
├── Split dump into per-table temp files
└── Separate schema.sql from data files

Phase 2: Dependency-Ordered Selection
├── Process tables in FK-distance order from roots
├── For each table:
│   ├── Stream through table's data file
│   ├── Apply sampling logic (percent/rows)
│   ├── Check FK membership for dependent tables
│   ├── Record selected PK values
│   └── Emit selected rows to output
└── Concatenate schema + selected data
```

### Table Classification

| Classification | Description                                    | Sampling Behavior                             |
| -------------- | ---------------------------------------------- | --------------------------------------------- |
| **Root**       | Has no FK dependencies OR explicitly specified | Sample directly, seed for FK chain            |
| **Dependent**  | References root/dependent tables via FK        | Include if FK points to selected row          |
| **Junction**   | Only FKs + maybe PK (pivot tables)             | Include if any FK hits selected row           |
| **Lookup**     | Global reference data (countries, currencies)  | Include fully with `--include-global lookups` |
| **System**     | Framework tables (migrations, jobs)            | Skip by default                               |

### PK Tracking Data Structure

```rust
/// Efficient primary key value representation
enum PkValue {
    Int(i64),
    BigInt(i128),
    Text(Box<str>),
    Uuid([u8; 16]),
}

/// Support for composite primary keys
type PkTuple = SmallVec<[PkValue; 2]>;

/// Per-table set of selected primary keys
type PkSet = AHashSet<PkTuple>;

/// State maintained during sampling
struct SamplingState {
    /// Selected PKs per table (only for tables that are FK targets)
    selected_pks: HashMap<String, PkSet>,

    /// Statistics
    rows_selected: HashMap<String, u64>,
    rows_scanned: HashMap<String, u64>,
}
```

**Memory considerations:**

- Only track PKs for tables that are FK targets
- Most PKs are integers (~8-16 bytes each)
- 1 million selected rows ≈ 20-40 MB
- Configurable limit with `--max-selected-rows`

---

## Cross-Dialect FK Parsing

### MySQL / MariaDB

```sql
CREATE TABLE `invoice_items` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `invoice_id` INT NOT NULL,
  `product_id` INT NOT NULL,
  CONSTRAINT `fk_invoice` FOREIGN KEY (`invoice_id`) REFERENCES `invoices` (`id`),
  CONSTRAINT `fk_product` FOREIGN KEY (`product_id`) REFERENCES `products` (`id`)
) ENGINE=InnoDB;

-- Or via ALTER TABLE
ALTER TABLE `invoice_items`
  ADD CONSTRAINT `fk_invoice` FOREIGN KEY (`invoice_id`) REFERENCES `invoices` (`id`);
```

**Parsing notes:**

- Backtick-quoted identifiers
- Inline or ALTER TABLE constraints
- Named constraints with `CONSTRAINT name`
- ON DELETE/UPDATE actions (parse but ignore for sampling)

### PostgreSQL

```sql
CREATE TABLE invoice_items (
  id SERIAL PRIMARY KEY,
  invoice_id INTEGER NOT NULL REFERENCES invoices(id),
  product_id INTEGER NOT NULL,
  CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Or via ALTER TABLE
ALTER TABLE invoice_items
  ADD CONSTRAINT fk_invoice FOREIGN KEY (invoice_id) REFERENCES invoices(id);
```

**Parsing notes:**

- Double-quote identifiers (optional)
- Inline `REFERENCES table(col)` shorthand
- Schema-qualified names: `schema.table`
- Extract table name from `schema.table` → `table`

### SQLite

```sql
CREATE TABLE invoice_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  invoice_id INTEGER NOT NULL REFERENCES invoices(id),
  product_id INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);
```

**Parsing notes:**

- Double-quote or backtick identifiers
- No named constraints (anonymous)
- No ALTER TABLE ADD CONSTRAINT (FKs must be inline)
- Simpler parsing overall

### FK Regex Patterns

```rust
// Inline REFERENCES (PostgreSQL, SQLite)
static INLINE_FK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)`?(\w+)`?\s+\w+.*?REFERENCES\s+`?(\w+)`?\s*\(`?(\w+)`?\)")
});

// CONSTRAINT ... FOREIGN KEY (all dialects)
static CONSTRAINT_FK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)FOREIGN\s+KEY\s*\(`?([^)]+)`?\)\s*REFERENCES\s+`?([^\s(`]+)`?\s*\(`?([^)]+)`?\)")
});

// ALTER TABLE ... ADD CONSTRAINT (MySQL, PostgreSQL)
static ALTER_FK_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)ALTER\s+TABLE\s+`?(\w+)`?.*?ADD\s+(?:CONSTRAINT\s+`?\w+`?\s+)?FOREIGN\s+KEY\s*\(`?([^)]+)`?\)\s*REFERENCES\s+`?(\w+)`?\s*\(`?([^)]+)`?\)")
});
```

---

## Handling Edge Cases

### 1. Circular FK References

**Detection:** Use Tarjan's algorithm to find strongly connected components (SCCs).

**Handling:**

- If SCC size = 1 with self-FK: Special self-referential handling
- If SCC size > 1: Process all tables in cycle as a unit

```rust
// For cycles, use conservative inclusion
fn handle_cycle(tables: &[String], state: &mut SamplingState) {
    // Include row if:
    // 1. It matches sampling criteria, OR
    // 2. Any FK within the cycle points to a selected row
    // Warn user about cycle
}
```

**Example:** `comments.parent_id → comments.id`

For self-referential tables:

1. Sample base rows normally
2. Compute ancestor closure (include parents of selected rows)
3. May require multiple passes over the table's temp file

### 2. Tables Without Primary Key

```
⚠️ Warning: Table 'legacy_data' has no PRIMARY KEY
   Cannot track FK references; sampling without relation preservation
```

- Skip PK tracking for this table
- Sample normally (random selection)
- Warn user about potential integrity issues

### 3. Composite Primary Keys

```rust
// Track multi-column PKs as tuples
let pk_tuple: PkTuple = smallvec![
    PkValue::Int(company_id),
    PkValue::Int(order_id),
];
selected_pks.insert(pk_tuple);
```

### 4. Junction Tables

Tables like `permission_role`, `user_roles`, `taggables`:

```sql
CREATE TABLE permission_role (
  permission_id INT REFERENCES permissions(id),
  role_id INT REFERENCES roles(id),
  PRIMARY KEY (permission_id, role_id)
);
```

**Handling:**

- Detect: Table with only FK columns + composite PK
- Include row if ANY FK points to a selected row
- Don't require ALL FKs to match (would be too restrictive)

### 5. Very Small Tables

```rust
fn calculate_sample_size(total_rows: usize, percent: f64) -> usize {
    let target = (total_rows as f64 * percent / 100.0).round() as usize;
    // Always include at least 1 row if table has data
    target.max(1).min(total_rows)
}
```

### 6. Empty Tables

- Include CREATE TABLE in schema
- Skip INSERT generation
- Note in statistics: "0 rows (empty table)"

### 7. Orphaned FK References

During sampling, detect when included rows reference non-included parents:

```rust
struct OrphanStats {
    table: String,
    fk_name: String,
    orphan_count: u64,
}

// After processing, report:
// ⚠️ 15 rows in 'orders' reference non-included 'users' (original data may have orphans)
```

With `--strict-fk`: Fail the command if any orphans detected.

---

## INSERT/COPY Row Parsing

To check FK values and extract PKs, we need to parse individual rows from INSERT and COPY statements.

### MySQL INSERT Parsing

```sql
INSERT INTO `users` (`id`, `name`, `company_id`) VALUES
(1, 'Alice', 5),
(2, 'Bob', 3),
(3, 'Carol', 5);
```

**Parser approach:**

1. Extract column list (if present) to map indices
2. Parse each `(...)` value tuple
3. Handle: strings with escapes, NULLs, numeric types
4. Only decode columns needed for PK/FK checks

```rust
fn parse_insert_values(stmt: &[u8], schema: &TableSchema) -> impl Iterator<Item = Row> {
    // Find VALUES keyword
    // Iterate through (val1, val2, ...) groups
    // Yield decoded rows (only pk/fk columns materialized)
}
```

### PostgreSQL COPY Parsing

```sql
COPY public.users (id, name, company_id) FROM stdin;
1	Alice	5
2	Bob	3
3	Carol	5
\.
```

**Parser approach:**

1. Extract column list from COPY header
2. Split data block by newlines
3. Split each line by `\t`
4. Handle: `\N` (NULL), backslash escapes

```rust
fn parse_copy_data(data: &[u8], schema: &TableSchema) -> impl Iterator<Item = Row> {
    // Split by newlines (excluding final \.)
    // Split each line by \t
    // Decode values, handling \N and escapes
}
```

---

## Implementation Architecture

### Directory Structure

```
src/
├── cmd/
│   └── sample.rs           # CLI handler
├── sampler/
│   ├── mod.rs              # Public API
│   ├── config.rs           # Config parsing (YAML)
│   ├── strategy.rs         # Sampling strategies (percent, rows)
│   ├── reservoir.rs        # Reservoir sampling algorithm
│   └── writer.rs           # Output generation
├── schema/
│   ├── mod.rs              # Schema graph types
│   ├── parser.rs           # DDL parsing for FK extraction
│   ├── graph.rs            # Dependency graph + topological sort
│   └── classifier.rs       # Table classification logic
└── row/
    ├── mod.rs              # Row parsing types
    ├── insert.rs           # INSERT value parsing
    └── copy.rs             # COPY data parsing
```

### Key Types

```rust
/// Schema information for a table
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_indices: Vec<usize>,
    pub fk_edges: Vec<ForeignKeyEdge>,
    pub has_tenant_column: Option<usize>,  // For shard feature
}

pub struct Column {
    pub name: String,
    pub index: usize,
    pub data_type: Option<String>,
}

pub struct ForeignKeyEdge {
    pub constraint_name: Option<String>,
    pub from_columns: Vec<usize>,  // Indices in source table
    pub to_table: String,
    pub to_columns: Vec<String>,   // Column names in target
}

/// Complete schema graph
pub struct SchemaGraph {
    pub tables: HashMap<String, TableSchema>,
    pub fk_graph: HashMap<String, Vec<String>>,  // child -> parents
    pub reverse_graph: HashMap<String, Vec<String>>,  // parent -> children
}

impl SchemaGraph {
    pub fn from_ddl(statements: &[&[u8]], dialect: SqlDialect) -> Self;
    pub fn topological_order(&self) -> Vec<&str>;
    pub fn find_cycles(&self) -> Vec<Vec<&str>>;
    pub fn root_tables(&self) -> Vec<&str>;
    pub fn classify_table(&self, name: &str) -> TableClassification;
}
```

### Reservoir Sampling

```rust
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

pub struct ReservoirSampler<T> {
    sample_size: usize,
    reservoir: Vec<T>,
    count: usize,
    rng: StdRng,
}

impl<T> ReservoirSampler<T> {
    pub fn new(sample_size: usize, seed: Option<u64>) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_entropy(),
        };
        Self {
            sample_size,
            reservoir: Vec::with_capacity(sample_size),
            count: 0,
            rng,
        }
    }

    pub fn add(&mut self, item: T) {
        self.count += 1;

        if self.reservoir.len() < self.sample_size {
            self.reservoir.push(item);
        } else {
            // Algorithm R: Replace with probability k/n
            let j = self.rng.gen_range(0..self.count);
            if j < self.sample_size {
                self.reservoir[j] = item;
            }
        }
    }

    pub fn total_seen(&self) -> usize {
        self.count
    }

    pub fn finish(self) -> Vec<T> {
        self.reservoir
    }
}
```

---

## Output Format

### Header Comment

```sql
-- Sampled from: production.sql
-- Date: 2025-12-20 12:00:00
-- Mode: 10% per table, preserve-relations
-- Seed: 42
-- Dialect: mysql
--
-- Statistics:
--   Tables sampled: 45
--   Tables skipped: 7 (system/excluded)
--   Total rows: 15,234 (from 152,340 original, 10.0%)
--   FK integrity: OK (no orphans)
```

### Schema Section

```sql
-- Schema statements

CREATE TABLE `users` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `email` VARCHAR(255),
  `company_id` INT,
  CONSTRAINT `fk_company` FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`)
);

CREATE INDEX `idx_users_email` ON `users` (`email`);
```

### Data Section (Compact INSERTs)

```sql
-- Data: users (523 rows)

INSERT INTO `users` VALUES
(1, 'alice@example.com', 5),
(5, 'bob@example.com', 5),
(12, 'carol@example.com', 5);

-- Data: orders (1,847 rows)

INSERT INTO `orders` VALUES
...
```

---

## Performance

### Targets

| Metric     | Target           | Notes                                     |
| ---------- | ---------------- | ----------------------------------------- |
| Throughput | 150-200 MB/s     | Slower than split due to parsing overhead |
| Memory     | O(selected_rows) | PK tracking for FK targets only           |
| Startup    | <100ms           | Schema analysis phase                     |

### Optimizations

1. **Streaming architecture**: Never load full file into memory
2. **Lazy row parsing**: Only decode PK/FK columns, skip others
3. **Fast hashing**: `ahash` for PK sets
4. **Temp file I/O**: Per-table files enable random access order
5. **Pre-compiled regexes**: Static initialization via `once_cell`

### Memory Budget

```rust
const DEFAULT_MAX_SELECTED_ROWS: usize = 10_000_000;  // ~200MB for int PKs
const PK_MEMORY_WARNING_THRESHOLD: usize = 5_000_000;

if selected_pks.len() > PK_MEMORY_WARNING_THRESHOLD {
    eprintln!("⚠️ Large selection: {} PKs tracked, memory usage may be high",
              selected_pks.len());
}
```

---

## Testing Strategy

### Unit Tests

- Reservoir sampling: uniform distribution, deterministic with seed
- FK regex parsing: all three dialects, edge cases
- Dependency graph: topological sort, cycle detection
- Row parsing: INSERT values, COPY data, escapes, NULLs

### Integration Tests

```rust
#[test]
fn test_sample_preserves_fk_integrity() {
    // Create dump with FK relationships
    // Sample with --preserve-relations
    // Verify: all FK references in output exist
}

#[test]
fn test_sample_reproducible_with_seed() {
    // Sample twice with same seed
    // Verify: identical output
}

#[test]
fn test_sample_handles_cycles() {
    // Create dump with self-referential table
    // Sample with --preserve-relations
    // Verify: parent rows included for selected children
}
```

### Property-Based Tests

- Output is always valid SQL for the dialect
- Row counts within expected bounds (±5% for reservoir sampling)
- No orphaned FK references when `--preserve-relations`

---

## Example Workflows

### 1. Create Dev Database Fixture

```bash
# Production: 50GB, Dev: ~500MB (1%)
sql-splitter sample prod.sql.gz -o dev-fixture.sql \
  --percent 1 \
  --preserve-relations \
  --seed 12345
```

### 2. CI Test Data

```bash
# Small, fast, reproducible, FK-safe
sql-splitter sample dump.sql -o ci-data.sql \
  --rows 100 \
  --preserve-relations \
  --seed 42
```

### 3. Demo Dataset Starting from Orders

```bash
# 50 orders with all related data (customers, products, etc.)
sql-splitter sample prod.sql -o demo.sql \
  --rows 50 \
  --preserve-relations \
  --root-tables orders \
  --include-global lookups \
  --exclude audit_logs,sessions,cache
```

### 4. Advanced Config

```yaml
# team-sample.yaml
default:
  percent: 5

classification:
  system:
    - failed_jobs
    - job_batches
    - telescope_entries
  lookup:
    - countries
    - currencies
    - permissions

tables:
  users:
    rows: 100 # Always need some users

  analytics_events:
    percent: 0.1 # Huge table, minimal sample

  reference_data:
    percent: 100 # Keep all lookup data

  temp_tables:
    skip: true
```

```bash
sql-splitter sample prod.sql -o team-dev.sql --config team-sample.yaml --preserve-relations
```

---

## Estimated Effort

| Component                                     | Effort        |
| --------------------------------------------- | ------------- |
| CLI and config parsing                        | 2 hours       |
| Reservoir sampling                            | 2 hours       |
| Basic percentage/row sampling                 | 3 hours       |
| Schema graph + FK parsing (shared with shard) | 6 hours       |
| Table classification logic                    | 2 hours       |
| INSERT row parsing (MySQL)                    | 3 hours       |
| COPY data parsing (PostgreSQL)                | 2 hours       |
| Dependency-ordered processing                 | 4 hours       |
| PK tracking + FK membership checks            | 3 hours       |
| Cycle handling (self-FK, SCCs)                | 3 hours       |
| Output generation                             | 2 hours       |
| YAML config support                           | 2 hours       |
| Progress reporting                            | 1 hour        |
| Testing (comprehensive)                       | 8 hours       |
| **Total**                                     | **~43 hours** |

**Note:** The schema graph and FK parsing components are shared with the `shard` command, reducing overall implementation effort when both features are built.

---

## Future Enhancements

1. **Conditional sampling**: `where: "created_at > '2024-01-01'"`
2. **Stratified sampling**: Ensure distribution across column values
3. **Weighted sampling**: Priority to recent/important rows
4. **Incremental sampling**: Add to existing sample
5. **Schema-only mode**: Just CREATE statements, no data
6. **Parallel processing**: Sample multiple tables concurrently (independent tables)

---

## Related

- [Split Command](../../src/cmd/split.rs) — Base streaming architecture
- [Shard Feature](SHARD_FEATURE.md) — Tenant-based extraction (shares FK graph)
- [Merge Feature](MERGE_FEATURE.md) — Combine split files
- [Redact Feature](IDEAS.md#6-redact-command--anonymize-sensitive-data) — Data anonymization
