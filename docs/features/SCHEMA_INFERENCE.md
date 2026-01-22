# Schema Inference Feature Design

**Status**: Planning (v2.1.0)
**Date**: 2025-12-24
**Priority**: Medium-High

## Overview

The `infer` command reverse-engineers CREATE TABLE statements from data-only dumps (INSERT statements without schema). It analyzes data patterns to infer column types, constraints, indexes, and primary keys.

## Problem Statement

Common scenarios where schema is missing:

1. **CSV exports** — Data without schema definition
2. **Legacy migrations** — Old exports missing DDL
3. **Data-only mysqldump** — `--no-create-info` flag
4. **ETL pipelines** — Receiving raw data files
5. **Database recovery** — Schema lost but data intact

**Current workarounds:**

- Manually write CREATE TABLE (error-prone, tedious)
- Import to database and use `DESCRIBE` (requires DB setup)
- Guess column types (often wrong)

## Command Interface

```bash
# Infer schema from INSERT-only dump
sql-splitter infer data.sql -o schema.sql

# Infer from CSV
sql-splitter infer data.csv --table users --dialect mysql

# Generate both schema and data
sql-splitter infer data.csv -o complete.sql --with-data

# Specify output dialect
sql-splitter infer data.sql --dialect postgres -o schema.sql

# Show inferred schema without writing
sql-splitter infer data.sql --dry-run

# Infer with hints
sql-splitter infer data.sql --primary-key id --index email

# Sample mode (analyze first N rows only)
sql-splitter infer huge.sql --sample 10000 -o schema.sql

# JSON output for programmatic use
sql-splitter infer data.sql --format json
```

## CLI Options

| Flag            | Description                                | Default     |
| --------------- | ------------------------------------------ | ----------- |
| `-o, --output`  | Output file path                           | stdout      |
| `-d, --dialect` | Target SQL dialect                         | mysql       |
| `--table`       | Table name (required for CSV)              | auto-detect |
| `--with-data`   | Include INSERT statements                  | false       |
| `--dry-run`     | Preview without writing                    | false       |
| `--primary-key` | Hint: PK column(s)                         | auto-detect |
| `--index`       | Hint: columns to index                     | none        |
| `--sample`      | Analyze first N rows only                  | all         |
| `--format`      | Output format: `sql`, `json`               | sql         |
| `--strict`      | Fail on ambiguous types                    | false       |
| `--not-null`    | Columns assumed NOT NULL (comma-separated) | auto-detect |

## Inference Strategies

### 1. Column Type Inference

Analyze all values in each column position to determine type:

```
Column 1 values: 1, 2, 3, 4, 5
→ Inferred type: INT

Column 2 values: '2024-01-15', '2024-02-20', '2024-03-10'
→ Inferred type: DATE

Column 3 values: 'user@example.com', 'admin@test.com'
→ Inferred type: VARCHAR(255)
```

#### Type Inference Algorithm

```rust
pub fn infer_column_type(values: &[Value]) -> ColumnType {
    let mut stats = TypeStats::new();

    for value in values {
        if value.is_null() {
            stats.null_count += 1;
            continue;
        }

        // Try parsing as different types
        if is_integer(value) {
            stats.integer_count += 1;
            stats.max_int = max(stats.max_int, parse_int(value));
        } else if is_float(value) {
            stats.float_count += 1;
        } else if is_date(value) {
            stats.date_count += 1;
        } else if is_datetime(value) {
            stats.datetime_count += 1;
        } else if is_boolean(value) {
            stats.boolean_count += 1;
        } else {
            stats.string_count += 1;
            stats.max_length = max(stats.max_length, value.len());
        }
    }

    // Determine type based on statistics
    match stats.dominant_type() {
        DominantType::Integer => {
            if stats.max_int <= i32::MAX {
                ColumnType::Int
            } else {
                ColumnType::BigInt
            }
        }
        DominantType::Float => ColumnType::Double,
        DominantType::Date => ColumnType::Date,
        DominantType::DateTime => ColumnType::DateTime,
        DominantType::Boolean => ColumnType::Boolean,
        DominantType::String => {
            let varchar_size = next_power_of_two(stats.max_length);
            if varchar_size <= 255 {
                ColumnType::Varchar(varchar_size)
            } else {
                ColumnType::Text
            }
        }
    }
}
```

#### Type Hierarchy

If mixed types detected, use most general:

```
INT + FLOAT → DOUBLE
DATE + DATETIME → DATETIME
VARCHAR(50) + VARCHAR(100) → VARCHAR(128)
INT + STRING → VARCHAR (everything can be string)
```

### 2. Primary Key Detection

**Heuristics:**

1. **Column named `id`** — 99% likely to be PK
2. **Auto-incrementing integers** — 1, 2, 3, 4, 5... → PK
3. **Unique values across all rows** — Check for duplicates
4. **Column named `*_id` or `*_pk`** — High probability
5. **First column with unique values** — Common convention

```rust
pub fn detect_primary_key(table: &ParsedTable) -> Option<Vec<String>> {
    // 1. Check for column named "id"
    if let Some(col) = table.columns.iter().find(|c| c.name == "id") {
        if is_unique(col) {
            return Some(vec!["id".to_string()]);
        }
    }

    // 2. Check for auto-increment pattern
    for col in &table.columns {
        if is_auto_increment(col) {
            return Some(vec![col.name.clone()]);
        }
    }

    // 3. Check for unique columns
    for col in &table.columns {
        if is_unique(col) && is_integer_type(col) {
            return Some(vec![col.name.clone()]);
        }
    }

    // 4. Composite key detection (harder, skip for MVP)
    None
}

fn is_auto_increment(col: &Column) -> bool {
    // Check if values are 1, 2, 3, 4, ...
    if !col.values.iter().all(|v| v.is_integer()) {
        return false;
    }

    let ints: Vec<i64> = col.values.iter().map(|v| v.as_int()).collect();
    ints.windows(2).all(|w| w[1] == w[0] + 1) && ints[0] == 1
}
```

### 3. NOT NULL Constraint Detection

```rust
pub fn is_not_null(column: &Column) -> bool {
    // If no NULL values in data, infer NOT NULL
    !column.values.iter().any(|v| v.is_null())
}
```

### 4. Index Suggestions

**Heuristics:**

1. **Foreign key columns** — `user_id`, `order_id` → Index
2. **Columns with high cardinality** — Many unique values → Good index candidate
3. **String columns frequently used in WHERE** — email, username → Index
4. **Composite indexes** — (tenant_id, created_at) for multi-tenant schemas

```rust
pub fn suggest_indexes(table: &ParsedTable) -> Vec<Index> {
    let mut indexes = Vec::new();

    for col in &table.columns {
        // Foreign key column pattern
        if col.name.ends_with("_id") && col.name != "id" {
            indexes.push(Index::new(&col.name));
        }

        // High cardinality string column
        if col.data_type.is_string() && col.cardinality() > 0.8 {
            indexes.push(Index::new(&col.name));
        }
    }

    indexes
}
```

### 5. Foreign Key Inference (Heuristic)

**Pattern matching:**

```
Column: user_id, Type: INT
→ Likely FK to users(id)

Column: product_id, Type: BIGINT
→ Likely FK to products(id)
```

**Validation:**

- Check if referenced table exists in dump
- Check if all values in `user_id` exist in `users.id`

**Note:** This is best-effort; false positives possible.

## Input Formats

### 1. INSERT-only SQL Dump

```sql
INSERT INTO users (id, email, created_at) VALUES
(1, 'alice@example.com', '2024-01-15 10:30:00'),
(2, 'bob@example.com', '2024-01-16 14:20:00');

INSERT INTO orders (id, user_id, total, created_at) VALUES
(1, 1, 99.99, '2024-01-20 09:00:00'),
(2, 1, 149.50, '2024-01-21 11:30:00');
```

**Inferred schema:**

```sql
CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  created_at DATETIME NOT NULL
);

CREATE INDEX idx_users_email ON users(email);

CREATE TABLE orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  total DECIMAL(10, 2) NOT NULL,
  created_at DATETIME NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
```

### 2. CSV Files

```csv
id,email,age,created_at
1,alice@example.com,28,2024-01-15
2,bob@example.com,35,2024-01-16
```

**Command:**

```bash
sql-splitter infer users.csv --table users --dialect mysql
```

**Output:**

```sql
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(255) NOT NULL,
  age INT NOT NULL,
  created_at DATE NOT NULL
);
```

### 3. JSON Lines (Future)

```jsonl
{"id": 1, "email": "alice@example.com", "age": 28}
{"id": 2, "email": "bob@example.com", "age": 35}
```

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── infer.rs           # CLI handler
├── infer/
│   ├── mod.rs             # Public API
│   ├── parser.rs          # Parse INSERT/CSV into rows
│   ├── analyzer.rs        # Column type analysis
│   ├── primary_key.rs     # PK detection
│   ├── constraints.rs     # NOT NULL, UNIQUE detection
│   ├── indexes.rs         # Index suggestions
│   ├── foreign_keys.rs    # FK inference
│   └── generator.rs       # Generate CREATE TABLE SQL
```

### Key Types

```rust
pub struct InferConfig {
    pub input: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub table_name: Option<String>,
    pub with_data: bool,
    pub sample_size: Option<usize>,
    pub primary_key_hint: Option<Vec<String>>,
    pub index_hints: Vec<String>,
    pub not_null_hints: Vec<String>,
    pub strict: bool,
}

pub struct ParsedTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub row_count: usize,
}

pub struct Column {
    pub name: String,
    pub values: Vec<Value>,
    pub inferred_type: ColumnType,
    pub nullable: bool,
    pub unique: bool,
}

pub enum ColumnType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    Char { length: usize },
    Varchar { length: usize },
    Text,
    Date,
    Time,
    DateTime,
    Timestamp,
    Boolean,
    Binary,
    Json,
}

pub struct InferredSchema {
    pub table: TableDef,
    pub primary_key: Option<PrimaryKey>,
    pub indexes: Vec<Index>,
    pub foreign_keys: Vec<ForeignKey>,
    pub confidence: Confidence,
}

pub struct Confidence {
    pub column_types: f32,  // 0.0 - 1.0
    pub primary_key: f32,
    pub foreign_keys: f32,
}
```

## Edge Cases

### 1. Empty Values

```sql
INSERT INTO users (id, email) VALUES (1, '');
```

**Question:** Is empty string `''` equivalent to NULL?

**Solution:**

- Treat `''` as valid string value (NOT NULL)
- Only explicit NULL → nullable

### 2. Mixed NULL and Data

```sql
INSERT INTO users (phone) VALUES ('555-1234'), (NULL), ('555-5678');
```

**Result:** `phone VARCHAR(20) NULL`

### 3. All NULL Values

```sql
INSERT INTO users (metadata) VALUES (NULL), (NULL), (NULL);
```

**Result:** `metadata TEXT NULL` (default to TEXT if no type info)

### 4. Numeric Precision Loss

```
Values: 3.14159265358979323846
→ DECIMAL(20, 18) or DOUBLE?
```

**Solution:**

- Count decimal places
- If > 6 digits after decimal → DECIMAL
- Otherwise → DOUBLE

### 5. Ambiguous Date Formats

```
'2024-01-15'  → DATE
'2024-01-15 10:30:00' → DATETIME
'10:30:00' → TIME
```

**Solution:**

- Regex patterns for each format
- Prefer most specific type (DATETIME > DATE)

### 6. Boolean Ambiguity

```
Values: 0, 1, 0, 1 → BOOLEAN or TINYINT?
```

**Solution:**

- If all values are 0 or 1 → BOOLEAN
- If hint provided (`--boolean status`) → BOOLEAN
- Otherwise → TINYINT (safer)

## Performance Considerations

| Rows      | Columns | Time  |
| --------- | ------- | ----- |
| 1,000     | 10      | < 1s  |
| 100,000   | 50      | < 5s  |
| 1,000,000 | 100     | < 30s |

**Optimizations:**

1. **Sampling:** Analyze first 10,000 rows (configurable)
2. **Lazy evaluation:** Don't parse values until needed
3. **Type pruning:** Eliminate impossible types early
4. **Parallel column analysis:** Analyze columns independently

## Testing Strategy

### Unit Tests

- Type inference for each data type
- PK detection heuristics
- NOT NULL detection
- Index suggestions

### Integration Tests

- Real-world data (WordPress export, Shopify data)
- CSV files
- All dialects

### Golden File Tests

- Known inputs → expected CREATE TABLE output

## Example Workflows

### 1. Recover Schema from Data-Only Export

```bash
# Export was created with --no-create-info
sql-splitter infer data_only.sql -o schema.sql

# Combine schema + data
cat schema.sql data_only.sql > complete.sql
```

### 2. Import CSV into Database

```bash
# Generate schema from CSV
sql-splitter infer users.csv --table users --dialect postgres -o schema.sql

# Convert CSV to INSERT statements
sql-splitter infer users.csv --table users --with-data -o users.sql

# Import
psql -d mydb -f users.sql
```

### 3. Cross-Dialect Migration

```bash
# Infer schema from MySQL dump
sql-splitter infer mysql_data.sql -o schema.sql --dialect postgres

# Now schema is PostgreSQL-compatible
```

## Estimated Effort

| Component               | Effort        |
| ----------------------- | ------------- |
| INSERT/CSV parser       | 4 hours       |
| Type inference engine   | 8 hours       |
| PK detection            | 4 hours       |
| NOT NULL detection      | 2 hours       |
| Index suggestion        | 4 hours       |
| FK inference            | 6 hours       |
| CREATE TABLE generator  | 4 hours       |
| Dialect-specific output | 6 hours       |
| CLI integration         | 3 hours       |
| Testing                 | 8 hours       |
| Documentation           | 3 hours       |
| **Total**               | **~50 hours** |

## Future Enhancements

1. **Machine Learning:** Train model on real schemas to improve inference
2. **Interactive Mode:** Ask user to confirm ambiguous types
3. **Schema Hints File:** YAML config with type overrides
4. **Constraint Inference:** CHECK constraints from value ranges
5. **Enum Detection:** If limited distinct values, suggest ENUM
6. **JSON Schema Inference:** Infer structure of JSON columns
7. **Time Series Detection:** Auto-detect time-series patterns

## Limitations

**Will NOT detect:**

- Complex CHECK constraints
- Triggers
- Stored procedures
- Views
- Partitioning schemes
- Collation settings

**Best-effort only:**

- Foreign keys (requires cross-table validation)
- Composite primary keys (requires correlation analysis)
- Optimal index selection (requires query patterns)

## Related Documents

- [Query Feature](QUERY_FEATURE.md) — Could use inferred schema
- [Convert Feature](../archived/CONVERT_FEASIBILITY.md) — Type mapping reference
- [Roadmap](../ROADMAP.md)
