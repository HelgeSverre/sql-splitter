# Sample Command Design

**Status**: Draft  
**Date**: 2025-12-20

## Overview

The `sample` command creates reduced datasets from large SQL dumps for development, testing, and CI environments. It extracts a representative subset of data while optionally preserving referential integrity.

## Command Interface

```bash
# Sample 10% of each table
sql-splitter sample dump.sql -o dev.sql --percent 10

# Fixed row count per table
sql-splitter sample dump.sql -o dev.sql --rows 1000

# Preserve foreign key relationships
sql-splitter sample dump.sql -o dev.sql --rows 500 --preserve-relations

# Sample specific tables only
sql-splitter sample dump.sql -o dev.sql --percent 20 --tables users,posts,comments

# Exclude large tables
sql-splitter sample dump.sql -o dev.sql --percent 10 --exclude cache,logs,sessions

# Reproducible sampling with seed
sql-splitter sample dump.sql -o dev.sql --percent 5 --seed 42

# Different strategies per table
sql-splitter sample dump.sql -o dev.sql --config sample.yaml

# Output statistics only (dry run)
sql-splitter sample dump.sql --percent 10 --dry-run
```

## CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output SQL file path | stdout |
| `--percent` | Sample percentage (1-100) | — |
| `--rows` | Fixed row count per table | — |
| `--preserve-relations` | Maintain foreign key integrity | false |
| `--tables` | Only sample these tables (comma-separated) | all |
| `--exclude` | Exclude these tables (comma-separated) | none |
| `--seed` | Random seed for reproducibility | random |
| `--config` | YAML config for per-table settings | — |
| `-d, --dialect` | SQL dialect | auto-detect |
| `-p, --progress` | Show progress bar | false |
| `--dry-run` | Show statistics without writing | false |
| `--include-schema` | Include CREATE TABLE statements | true |

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

### 3. Preserve Relations (`--preserve-relations`)

Maintain referential integrity by following foreign key references.

```bash
sql-splitter sample dump.sql -o dev.sql --rows 100 --preserve-relations
```

**Algorithm:**
1. Parse CREATE TABLE statements to build FK dependency graph
2. Start with "root" tables (no FK dependencies)
3. Sample N rows from root tables
4. For each FK reference, include referenced rows
5. Recursively follow FK chains
6. Result: Consistent subset with no broken references

**Example:**
```
orders (100 rows sampled)
  └── users (referenced user_id values included)
  └── products (referenced product_id values included)
       └── categories (referenced category_id values included)
```

### 4. Config-Based (Advanced)

Per-table sampling strategies via YAML config.

```yaml
# sample.yaml
default:
  percent: 10

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
    where: "active = 1"  # Future: conditional sampling
```

```bash
sql-splitter sample dump.sql -o dev.sql --config sample.yaml
```

## Output Format

### Schema Statements

By default, include all CREATE TABLE, CREATE INDEX, etc.

```sql
-- Sampled from: production.sql
-- Date: 2025-12-20 12:00:00
-- Sampling: 10% per table
-- Seed: 42
-- Total rows: 15,234 (from 152,340 original)

CREATE TABLE `users` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `email` VARCHAR(255),
  ...
);

INSERT INTO `users` VALUES
(1, 'user1@example.com', ...),
(5, 'user5@example.com', ...),
...;
```

### Compact INSERT Format

Batch INSERTs for efficiency (matching split command output).

```sql
INSERT INTO `users` VALUES
(1, 'alice@example.com', 'Alice'),
(5, 'bob@example.com', 'Bob'),
(12, 'carol@example.com', 'Carol');
```

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── sample.rs           # CLI handler
├── sampler/
│   ├── mod.rs              # Public API
│   ├── config.rs           # Config parsing
│   ├── strategy.rs         # Sampling strategies
│   ├── reservoir.rs        # Reservoir sampling algorithm
│   ├── relations.rs        # FK dependency analysis
│   └── writer.rs           # Output generation
```

### Key Types

```rust
pub struct SampleConfig {
    pub input: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub mode: SampleMode,
    pub preserve_relations: bool,
    pub tables: Option<Vec<String>>,
    pub exclude: Vec<String>,
    pub seed: Option<u64>,
    pub include_schema: bool,
    pub progress: bool,
}

pub enum SampleMode {
    Percent(f64),           // 0.0 - 100.0
    FixedRows(usize),       // Exact count per table
    Config(SampleTableConfig),
}

pub struct SampleTableConfig {
    pub default_percent: Option<f64>,
    pub default_rows: Option<usize>,
    pub tables: HashMap<String, TableSampleRule>,
}

pub struct TableSampleRule {
    pub percent: Option<f64>,
    pub rows: Option<usize>,
    pub skip: bool,
}

pub struct SampleStats {
    pub tables_sampled: usize,
    pub tables_skipped: usize,
    pub original_rows: u64,
    pub sampled_rows: u64,
    pub original_bytes: u64,
    pub output_bytes: u64,
}
```

### Reservoir Sampling Algorithm

For streaming random sampling without knowing total count upfront:

```rust
pub struct ReservoirSampler {
    sample_size: usize,
    reservoir: Vec<String>,  // Sampled INSERT statements
    count: usize,
    rng: StdRng,
}

impl ReservoirSampler {
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
    
    pub fn add(&mut self, item: String) {
        self.count += 1;
        
        if self.reservoir.len() < self.sample_size {
            self.reservoir.push(item);
        } else {
            // Replace with probability sample_size/count
            let j = self.rng.gen_range(0..self.count);
            if j < self.sample_size {
                self.reservoir[j] = item;
            }
        }
    }
    
    pub fn finish(self) -> Vec<String> {
        self.reservoir
    }
}
```

### FK Dependency Graph

```rust
pub struct ForeignKey {
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
}

pub struct DependencyGraph {
    pub tables: HashSet<String>,
    pub foreign_keys: Vec<ForeignKey>,
    pub dependencies: HashMap<String, Vec<String>>,  // table -> tables it depends on
}

impl DependencyGraph {
    pub fn from_statements(statements: &[Statement]) -> Self { ... }
    
    pub fn root_tables(&self) -> Vec<&str> {
        // Tables with no FK dependencies
    }
    
    pub fn dependents(&self, table: &str) -> Vec<&str> {
        // Tables that reference this table
    }
}
```

### Streaming Architecture

```
Input File → Statement Parser → Per-Table Sampler → Output Writer
                                      ↓
                              ReservoirSampler (per table)
                                      ↓
                              FK Tracker (if --preserve-relations)
```

**Memory efficiency:**
- Don't load entire file
- One ReservoirSampler per table (stores only sampled rows)
- Stream schema statements directly to output
- Two-pass for `--preserve-relations`:
  1. First pass: sample root tables, collect referenced PKs
  2. Second pass: include rows matching collected PKs

## Edge Cases

### 1. Empty Tables
- Include CREATE TABLE, skip INSERT
- Note in statistics

### 2. Tables with No Primary Key
- `--preserve-relations` cannot track references
- Warn and sample normally

### 3. Circular FK References
- Detect cycles in dependency graph
- Sample all tables in cycle together
- Warn user

### 4. Very Small Tables
- `--percent 10` of 5 rows = 0.5 → round to 1
- Always include at least 1 row if table has data

### 5. Multi-Column Primary Keys
- Track composite PK values for relation preservation
- Store as tuple hash

### 6. Self-Referential FKs
- e.g., `parent_id` references same table
- Include parent rows when sampling children

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput | 200+ MB/s (limited by sampling logic) |
| Memory | O(sample_size) per table, not O(file_size) |
| Startup | <50ms for any file size |

**Comparison to split command:**
- Slower due to sampling logic overhead
- Still single-pass for basic sampling
- Two-pass only for `--preserve-relations`

## Testing Strategy

### Unit Tests
- Reservoir sampling correctness
- FK dependency graph building
- Percentage/row calculations
- Config parsing

### Integration Tests
- Roundtrip: sample → import → verify row counts
- FK integrity verification with `--preserve-relations`
- Reproducibility with `--seed`
- All three dialects

### Property-Based Tests
- Sampled output always valid SQL
- Row counts within expected bounds
- Seed produces identical output

## Example Workflows

### 1. Create Dev Database Fixture

```bash
# Production: 50GB, Dev: ~500MB
sql-splitter sample prod.sql.gz -o dev-fixture.sql --percent 1 --seed 12345
```

### 2. CI Test Data

```bash
# Small, fast, reproducible
sql-splitter sample dump.sql -o ci-data.sql --rows 100 --seed 42
```

### 3. Demo Dataset with Relationships

```bash
# Ensure referential integrity for demo
sql-splitter sample prod.sql -o demo.sql \
  --rows 50 \
  --preserve-relations \
  --exclude audit_logs,sessions,cache
```

### 4. Per-Team Sampling Config

```yaml
# team-sample.yaml
default:
  percent: 5
  
tables:
  users:
    rows: 100  # Always need some users
  
  large_analytics_table:
    percent: 0.1  # Huge table, minimal sample
    
  reference_data:
    percent: 100  # Keep all lookup data
    
  temp_tables:
    skip: true
```

```bash
sql-splitter sample prod.sql -o team-dev.sql --config team-sample.yaml
```

## Estimated Effort

| Component | Effort |
|-----------|--------|
| CLI and config parsing | 2 hours |
| Reservoir sampling | 2 hours |
| Basic percentage/row sampling | 3 hours |
| INSERT statement parsing | 4 hours |
| Output generation | 2 hours |
| FK dependency graph | 4 hours |
| Relation preservation (two-pass) | 6 hours |
| YAML config support | 2 hours |
| Progress reporting | 1 hour |
| Testing (comprehensive) | 6 hours |
| **Total** | **~32 hours** |

## Future Enhancements

1. **Conditional sampling**: `where: "created_at > '2024-01-01'"`
2. **Stratified sampling**: Ensure distribution across column values
3. **Weighted sampling**: Priority to recent/important rows
4. **Incremental sampling**: Add to existing sample
5. **Schema-only mode**: Just CREATE statements, no data

## Related

- [Split Command](../src/cmd/split.rs)
- [Merge Feature](./MERGE_FEATURE.md)
- [Redact Feature](./IDEAS.md#6-redact-command--anonymize-sensitive-data)
