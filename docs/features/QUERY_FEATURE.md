# Query Command Design

**Status**: Draft  
**Date**: 2025-12-20

## Overview

The `query` command extracts specific data from SQL dumps using SQL-like filtering, without loading into a database. It enables row-level and column-level filtering with streaming performance.

## Command Interface

```bash
# Extract rows matching a condition
sql-splitter query dump.sql --table users --where "created_at > '2024-01-01'" -o recent.sql

# Extract specific columns only
sql-splitter query dump.sql --table users --columns "id,email,name" -o minimal.sql

# Complex filtering with multiple conditions
sql-splitter query dump.sql --table orders --where "total > 1000 AND status = 'completed'"

# Multiple tables
sql-splitter query dump.sql --table users,posts --where "id < 100" -o subset.sql

# Output as CSV instead of SQL
sql-splitter query dump.sql --table users --columns "id,email" --format csv -o users.csv

# Extract all data for a specific user (GDPR)
sql-splitter query dump.sql --where "user_id = 12345" -o user_data.sql

# Count matching rows (dry run)
sql-splitter query dump.sql --table users --where "active = 1" --count
```

## CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output file path | stdout |
| `-t, --table` | Table(s) to query (comma-separated) | all |
| `-w, --where` | Filter condition | none |
| `-c, --columns` | Columns to include (comma-separated) | all |
| `--format` | Output format: `sql`, `csv`, `json` | sql |
| `--count` | Only count matching rows | false |
| `--limit` | Maximum rows to extract | unlimited |
| `--offset` | Skip N rows before extracting | 0 |
| `-d, --dialect` | SQL dialect | auto-detect |
| `-p, --progress` | Show progress bar | false |
| `--include-schema` | Include CREATE TABLE in output | false |

## WHERE Clause Syntax

### Supported Operators

| Operator | Example | Notes |
|----------|---------|-------|
| `=` | `status = 'active'` | Equality |
| `!=`, `<>` | `status != 'deleted'` | Inequality |
| `>`, `>=` | `age >= 18` | Greater than |
| `<`, `<=` | `price < 100` | Less than |
| `LIKE` | `email LIKE '%@gmail.com'` | Pattern match |
| `NOT LIKE` | `name NOT LIKE 'test%'` | Negative pattern |
| `IN` | `status IN ('active', 'pending')` | Set membership |
| `NOT IN` | `id NOT IN (1, 2, 3)` | Set exclusion |
| `IS NULL` | `deleted_at IS NULL` | Null check |
| `IS NOT NULL` | `email IS NOT NULL` | Not null check |
| `BETWEEN` | `age BETWEEN 18 AND 65` | Range |
| `AND` | `a = 1 AND b = 2` | Logical AND |
| `OR` | `a = 1 OR b = 2` | Logical OR |
| `NOT` | `NOT active` | Logical NOT |
| `()` | `(a = 1 OR b = 2) AND c = 3` | Grouping |

### Data Types

| Type | Example | Notes |
|------|---------|-------|
| String | `'value'` | Single quotes |
| Number | `42`, `3.14` | Integer or float |
| NULL | `NULL` | Case insensitive |
| Boolean | `TRUE`, `FALSE`, `1`, `0` | Dialect-aware |
| Date | `'2024-01-15'` | ISO format string |

### Examples

```bash
# String matching
--where "email LIKE '%@company.com'"

# Numeric range
--where "price >= 10 AND price <= 100"

# Date filtering
--where "created_at > '2024-01-01'"

# Multiple conditions
--where "(status = 'active' OR status = 'pending') AND verified = 1"

# NULL handling
--where "deleted_at IS NULL AND email IS NOT NULL"

# Set membership
--where "country IN ('US', 'CA', 'UK')"
```

## Column Projection

Select specific columns to reduce output size:

```bash
# Only id and email
sql-splitter query dump.sql --table users --columns "id,email"

# With aliases (future)
sql-splitter query dump.sql --table users --columns "id,email AS user_email"
```

**Output:**
```sql
-- Original: INSERT INTO users (id, email, name, password, created_at) VALUES ...
-- Projected:
INSERT INTO users (id, email) VALUES
(1, 'alice@example.com'),
(2, 'bob@example.com');
```

## Output Formats

### SQL (Default)

```sql
-- Query: table=users, where=active = 1
-- Matched: 1,234 rows

INSERT INTO users (id, email, name) VALUES
(1, 'alice@example.com', 'Alice'),
(5, 'bob@example.com', 'Bob');
```

### CSV

```csv
id,email,name
1,alice@example.com,Alice
5,bob@example.com,Bob
```

### JSON

```json
[
  {"id": 1, "email": "alice@example.com", "name": "Alice"},
  {"id": 5, "email": "bob@example.com", "name": "Bob"}
]
```

## Implementation Architecture

### Core Components

```
src/
├── cmd/
│   └── query.rs            # CLI handler
├── query/
│   ├── mod.rs              # Public API
│   ├── parser.rs           # WHERE clause parser
│   ├── evaluator.rs        # Condition evaluation
│   ├── projector.rs        # Column projection
│   └── output.rs           # Format writers
```

### Key Types

```rust
pub struct QueryConfig {
    pub input: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub tables: Option<Vec<String>>,
    pub where_clause: Option<String>,
    pub columns: Option<Vec<String>>,
    pub format: OutputFormat,
    pub limit: Option<usize>,
    pub offset: usize,
    pub count_only: bool,
    pub include_schema: bool,
    pub progress: bool,
}

pub enum OutputFormat {
    Sql,
    Csv,
    Json,
}

pub enum Expr {
    Column(String),
    Literal(Value),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    In { expr: Box<Expr>, list: Vec<Value>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr> },
    IsNull { expr: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: String, negated: bool },
}

pub enum BinaryOp {
    Eq, Ne, Lt, Le, Gt, Ge, And, Or,
}

pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

pub struct QueryStats {
    pub rows_scanned: u64,
    pub rows_matched: u64,
    pub tables_queried: usize,
    pub bytes_processed: u64,
    pub bytes_output: u64,
}
```

### INSERT Parsing

Must parse INSERT statements to extract individual rows and column values:

```sql
INSERT INTO users (id, email, name) VALUES
(1, 'alice@example.com', 'Alice'),
(2, 'bob@example.com', 'Bob'),
(3, 'carol@example.com', 'Carol');
```

**Parsing steps:**
1. Extract table name
2. Extract column list
3. Parse VALUES clause row by row
4. For each row, evaluate WHERE condition
5. If matched, project columns and output

### Streaming Architecture

```
Input File → Statement Parser → INSERT Parser → Row Iterator
                                                     ↓
                                            WHERE Evaluator
                                                     ↓
                                            Column Projector
                                                     ↓
                                            Output Writer
```

**Key constraints:**
- Never buffer entire file
- Process one INSERT statement at a time
- Evaluate rows within INSERT as streaming

## Edge Cases

### 1. Extended INSERT Syntax

```sql
INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c');
```

Must handle both named and positional column references.

### 2. Multi-line String Values

```sql
INSERT INTO posts (content) VALUES ('Line 1
Line 2
Line 3');
```

### 3. Escaped Characters

```sql
INSERT INTO users (name) VALUES ('O\'Brien'), ('She said "hello"');
```

### 4. Binary/Blob Data

```sql
INSERT INTO files (data) VALUES (X'48454C4C4F');
```

Skip or pass through unchanged.

### 5. NULL Values

```sql
INSERT INTO users (email) VALUES (NULL);
```

WHERE evaluation must handle NULL semantics (NULL != NULL).

### 6. No Column List

```sql
INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
```

Requires schema knowledge to map positions to column names. Options:
- Error if `--columns` specified without column list in INSERT
- Parse CREATE TABLE to build column map
- Use positional indexing: `--where "$1 = 1"` (future)

## Performance Considerations

### Target Throughput

| Scenario | Target |
|----------|--------|
| Full scan (no WHERE) | 300+ MB/s |
| Simple WHERE | 200+ MB/s |
| Complex WHERE | 100+ MB/s |
| With column projection | Same as above |

### Optimizations

1. **Short-circuit evaluation**: Stop evaluating AND if first condition false
2. **Column pruning**: Only parse needed columns for WHERE evaluation
3. **Regex caching**: Pre-compile LIKE patterns
4. **String interning**: Reuse column names

## Testing Strategy

### Unit Tests
- WHERE clause parser (all operators)
- Expression evaluator (all types)
- Column projector
- Each output format

### Integration Tests
- Real-world dump filtering
- All three dialects
- Large file streaming
- Edge cases (NULLs, escaping, multi-line)

### Property Tests
- Filtered output is valid SQL
- Row count matches expectation
- Column projection preserves data

## Example Workflows

### 1. GDPR Data Export

```bash
# Extract all data for a specific user
sql-splitter query prod.sql --where "user_id = 12345" -o user_12345_data.sql

# Or as JSON for API response
sql-splitter query prod.sql --where "user_id = 12345" --format json
```

### 2. Create Dev Subset

```bash
# Recent active users only
sql-splitter query prod.sql \
  --table users \
  --where "active = 1 AND created_at > '2024-01-01'" \
  --columns "id,email,name" \
  -o dev_users.sql
```

### 3. Debug Specific Records

```bash
# Find problematic orders
sql-splitter query dump.sql \
  --table orders \
  --where "status = 'failed' AND total > 1000" \
  --format json | jq .
```

### 4. Data Migration Filter

```bash
# Only migrate verified accounts
sql-splitter query old_system.sql \
  --table accounts \
  --where "verified = 1 AND deleted_at IS NULL" \
  -o verified_accounts.sql
```

## Estimated Effort

| Component | Effort |
|-----------|--------|
| CLI and config | 2 hours |
| WHERE clause parser | 6 hours |
| Expression evaluator | 4 hours |
| INSERT row parser | 6 hours |
| Column projector | 2 hours |
| SQL output writer | 2 hours |
| CSV output writer | 2 hours |
| JSON output writer | 2 hours |
| Progress reporting | 1 hour |
| Testing | 8 hours |
| **Total** | **~35 hours** |

## Future Enhancements

1. **Aggregations**: `--count`, `--sum "total"`, `--avg "price"`
2. **Sorting**: `--order-by "created_at DESC"`
3. **Joins**: Cross-table filtering based on FK relationships
4. **Subqueries**: `--where "user_id IN (SELECT id FROM users WHERE active = 1)"`
5. **Regular expressions**: `--where "email REGEXP '^[a-z]+@'"

## Related

- [Split Command](../../src/cmd/split.rs)
- [Sample Feature](SAMPLE_FEATURE.md)
- [Redact Feature](REDACT_FEATURE.md)
