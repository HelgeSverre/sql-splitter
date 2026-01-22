# Enum Type Conversion Feature Design

**Status**: Planning (v1.13.0)  
**Date**: 2025-01-22  
**Priority**: Medium  
**Effort**: ~30 hours

## Overview

Enhance the `convert` command to properly convert enum types between PostgreSQL and MySQL dialects bidirectionally, instead of the current lossy conversion to VARCHAR/TEXT.

## Problem Statement

### Current Behavior

| Conversion          | Current                          | Result                                |
| ------------------- | -------------------------------- | ------------------------------------- |
| MySQL → PostgreSQL  | `ENUM('a','b')` → `VARCHAR(255)` | ❌ Loses type safety                  |
| MySQL → SQLite      | `ENUM('a','b')` → `TEXT`         | ⚠️ OK (SQLite has no enums)           |
| PostgreSQL → MySQL  | `CREATE TYPE` skipped            | ❌ Enum columns become `VARCHAR(255)` |
| PostgreSQL → SQLite | `CREATE TYPE` skipped            | ⚠️ OK (SQLite has no enums)           |

### Desired Behavior

| Conversion         | Desired                                        | Result                 |
| ------------------ | ---------------------------------------------- | ---------------------- |
| MySQL → PostgreSQL | `ENUM('a','b')` → `CREATE TYPE` + typed column | ✅ Preserves semantics |
| PostgreSQL → MySQL | `CREATE TYPE` → inline `ENUM()` per column     | ✅ Preserves values    |

## Core Difference: Enum Models

### PostgreSQL: Named Types

```sql
-- Enum is a reusable named type
CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered');

-- Multiple columns can reference the same type
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status order_status NOT NULL DEFAULT 'pending',
  previous_status order_status
);

-- Casts in data statements
INSERT INTO orders (status) VALUES ('pending'::order_status);
```

### MySQL: Inline Per-Column

```sql
-- Enum is defined inline on each column
CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  status ENUM('pending', 'processing', 'shipped', 'delivered') NOT NULL DEFAULT 'pending',
  previous_status ENUM('pending', 'processing', 'shipped', 'delivered')
);

-- No casts needed in data statements
INSERT INTO orders (status) VALUES ('pending');
```

---

## Implementation Design

### Architecture: Enum Registry

Add state tracking to the streaming converter:

```rust
/// Enum registry for tracking type definitions during conversion
pub struct EnumRegistry {
    /// PostgreSQL enum definitions: type_name → ordered labels
    pg_enums_by_name: HashMap<String, Vec<String>>,

    /// MySQL enum signatures: canonical(labels) → generated_pg_type_name
    /// Used for deduplication when converting MySQL → PostgreSQL
    enum_signatures: HashMap<String, String>,

    /// Track which CREATE TYPE statements have been emitted
    emitted_pg_types: HashSet<String>,
}

impl EnumRegistry {
    pub fn new() -> Self { ... }

    /// Register a PostgreSQL enum type (from CREATE TYPE ... AS ENUM)
    pub fn register_pg_enum(&mut self, name: &str, labels: Vec<String>) { ... }

    /// Lookup a PostgreSQL enum type by name
    pub fn get_pg_enum(&self, name: &str) -> Option<&[String]> { ... }

    /// Register/lookup MySQL enum signature, returns PG type name
    pub fn get_or_create_pg_type_for_signature(
        &mut self,
        table: &str,
        column: &str,
        labels: &[String],
    ) -> String { ... }

    /// Check if a PG type was already emitted
    pub fn mark_emitted(&mut self, name: &str) -> bool { ... } // returns true if new
}
```

### Converter State Extension

```rust
pub struct Converter {
    from: SqlDialect,
    to: SqlDialect,
    warnings: WarningCollector,
    strict: bool,
    pending_copy_header: Option<CopyHeader>,
    enum_registry: EnumRegistry,  // NEW
}
```

---

## Conversion: PostgreSQL → MySQL

### Step 1: Parse and Register Enum Definitions

When processing `CREATE TYPE ... AS ENUM`:

```sql
-- Input (PostgreSQL)
CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered');
```

**Action:**

1. Parse enum name: `order_status`
2. Parse labels: `['pending', 'processing', 'shipped', 'delivered']`
3. Register: `enum_registry.register_pg_enum("order_status", labels)`
4. **Skip output** (MySQL has no CREATE TYPE)

### Step 2: Handle ALTER TYPE ... ADD VALUE

```sql
-- Input (PostgreSQL)
ALTER TYPE order_status ADD VALUE 'cancelled' AFTER 'delivered';
```

**Action:**

1. Update registry with new value in correct position
2. **Skip output**
3. **Warning** if type was already used in emitted CREATE TABLE

### Step 3: Rewrite CREATE TABLE Columns

```sql
-- Input (PostgreSQL)
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status order_status NOT NULL DEFAULT 'pending'
);
```

**Action:**

1. Detect column type `order_status`
2. Lookup in registry → `['pending', 'processing', 'shipped', 'delivered']`
3. Replace with inline `ENUM('pending','processing','shipped','delivered')`

```sql
-- Output (MySQL)
CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  status ENUM('pending','processing','shipped','delivered') NOT NULL DEFAULT 'pending'
);
```

### Step 4: Strip Type Casts in Data Statements

```sql
-- Input (PostgreSQL)
INSERT INTO orders (status) VALUES ('pending'::order_status);
UPDATE orders SET status = 'shipped'::order_status WHERE id = 1;
```

**Action:**

1. Detect `'value'::type_name` pattern
2. Check if `type_name` is in enum registry
3. Strip cast: `'value'::order_status` → `'value'`

```sql
-- Output (MySQL)
INSERT INTO orders (status) VALUES ('pending');
UPDATE orders SET status = 'shipped' WHERE id = 1;
```

### Step 5: Handle Unknown Enum Types

When CREATE TABLE references an enum type not in registry:

**Action:**

1. Fallback to `VARCHAR(255)`
2. Add warning: "Unknown enum type 'status_type' - converted to VARCHAR(255)"

---

## Conversion: MySQL → PostgreSQL

### Strategy: Dedupe vs Per-Column Types

| Strategy                | Pros                                | Cons                         |
| ----------------------- | ----------------------------------- | ---------------------------- |
| **Per-column types**    | No semantic coupling, deterministic | Type explosion               |
| **Dedupe by signature** | Fewer types, reusable               | May couple unrelated columns |

**Recommendation:** Default to **per-column types** with deterministic naming.

### Naming Convention

Generate PostgreSQL type names as:

```
enum__{table}__{column}
```

Examples:

- `orders.status` → `enum__orders__status`
- `users.role` → `enum__users__role`

For schema-qualified tables:

- `myschema.orders.status` → `enum__myschema__orders__status`

### Step 1: Parse Inline ENUMs in CREATE TABLE

```sql
-- Input (MySQL)
CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  status ENUM('pending', 'processing', 'shipped') NOT NULL
);
```

**Action:**

1. Extract inline `ENUM('pending', 'processing', 'shipped')`
2. Extract table name: `orders`, column name: `status`
3. Generate PG type name: `enum__orders__status`
4. Emit `CREATE TYPE` before `CREATE TABLE`

### Step 2: Emit Multiple Statements per Input

One MySQL `CREATE TABLE` may produce multiple PostgreSQL statements:

```sql
-- Output (PostgreSQL)
CREATE TYPE enum__orders__status AS ENUM ('pending', 'processing', 'shipped');

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status enum__orders__status NOT NULL
);
```

**Implementation:** `convert_statement()` returns `Vec<Vec<u8>>` instead of `Vec<u8>`.

### Step 3: Handle Multiple Enum Columns

```sql
-- Input (MySQL)
CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  status ENUM('pending', 'shipped'),
  priority ENUM('low', 'medium', 'high')
);
```

**Output:**

```sql
CREATE TYPE enum__orders__status AS ENUM ('pending', 'shipped');
CREATE TYPE enum__orders__priority AS ENUM ('low', 'medium', 'high');

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  status enum__orders__status,
  priority enum__orders__priority
);
```

### Step 4: Handle ALTER TABLE ... MODIFY COLUMN

```sql
-- Input (MySQL)
ALTER TABLE orders MODIFY COLUMN status ENUM('pending', 'shipped', 'cancelled');
```

**Challenge:** PostgreSQL cannot easily modify enum value lists.

**Strategy:**

1. Create new type with new values
2. Emit ALTER TABLE ... ALTER COLUMN ... TYPE new_type USING column::text::new_type
3. Drop old type (if tracked)

**Or (simpler):** Warn and convert to VARCHAR for ALTER cases.

---

## SQLite Behavior

**No change:** Continue converting ENUMs to TEXT for SQLite.

SQLite has no enum support, and TEXT is the appropriate equivalent.

---

## Parsing Considerations

### PostgreSQL Enum Labels

Handle various quoting styles:

```sql
CREATE TYPE my_enum AS ENUM ('simple', 'with ''quote', E'escaped\ttab', $$dollar$$);
```

Parser must:

1. Handle `''` escape for embedded quotes
2. Handle `E'...'` extended string literals (optional - rare in dumps)
3. Handle `$$..$$` dollar quoting (optional - rare for enum values)
4. Preserve exact ordering

### MySQL Enum Labels

Handle MySQL quoting:

```sql
status ENUM('simple', 'with ''quote', 'with\\backslash')
```

Parser must:

1. Handle `''` escape for embedded quotes
2. Handle `\\` for backslash (SQL mode dependent)
3. Handle character set annotations: `ENUM('a','b') CHARACTER SET utf8mb4`

### Regex Patterns

```rust
// Parse PostgreSQL CREATE TYPE enum
static RE_PG_CREATE_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)CREATE\s+TYPE\s+([^\s(]+)\s+AS\s+ENUM\s*\(([^)]+)\)").unwrap()
});

// Parse PostgreSQL ALTER TYPE ADD VALUE
static RE_PG_ALTER_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)ALTER\s+TYPE\s+([^\s]+)\s+ADD\s+VALUE\s+('[^']*(?:''[^']*)*')(?:\s+(BEFORE|AFTER)\s+('[^']*(?:''[^']*)*'))?").unwrap()
});

// Parse enum type cast: 'value'::type_name
static RE_PG_ENUM_CAST: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"'([^']*(?:''[^']*)*)'::([a-zA-Z_][a-zA-Z0-9_]*)").unwrap()
});

// Parse MySQL inline ENUM
static RE_MYSQL_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bENUM\s*\(([^)]+)\)").unwrap()
});
```

### Label Extraction

```rust
/// Parse enum labels from '...' list
fn parse_enum_labels(labels_str: &str) -> Vec<String> {
    let mut labels = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut chars = labels_str.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => {
                if chars.peek() == Some(&'\'') {
                    current.push('\'');
                    chars.next();
                } else {
                    in_quote = false;
                    labels.push(current.clone());
                    current.clear();
                }
            }
            _ if in_quote => current.push(c),
            _ => {} // skip commas, whitespace outside quotes
        }
    }
    labels
}
```

---

## Edge Cases and Gotchas

### 1. Schema-Qualified Type Names

PostgreSQL allows:

```sql
CREATE TYPE myschema.order_status AS ENUM (...);
CREATE TABLE orders (status myschema.order_status);
```

**Solution:** Store normalized qualified names in registry, strip schema on lookup for MySQL.

### 2. Quoted Identifiers

```sql
CREATE TYPE "Order Status" AS ENUM (...);
```

**Solution:** Preserve original quoting for PG output, sanitize for MySQL identifier rules.

### 3. Shared Enum Types Across Tables

PostgreSQL:

```sql
CREATE TYPE status AS ENUM ('active', 'inactive');
CREATE TABLE users (status status);
CREATE TABLE products (status status);
```

MySQL→PG with dedupe:

```sql
-- Single shared type (signature match)
CREATE TYPE enum__users__status AS ENUM ('active', 'inactive');
CREATE TABLE users (status enum__users__status);
CREATE TABLE products (status enum__users__status); -- reuses same type
```

### 4. Out-of-Order Definitions

If CREATE TABLE appears before CREATE TYPE in dump:

**Strategy:**

- Option A: Buffer statements referencing unknown types, emit when type defined (complex)
- Option B: Fallback to VARCHAR + warning (simple, recommended)

### 5. ALTER TYPE After Table Creation

```sql
CREATE TYPE status AS ENUM ('a', 'b');
CREATE TABLE orders (status status);
ALTER TYPE status ADD VALUE 'c';
```

**For PG→MySQL:** The inline ENUM in `orders` won't include 'c'.

**Strategy:** Warn that ALTER TYPE occurred after table using the type.

### 6. COPY Data with Enum Values

PostgreSQL COPY:

```
COPY orders FROM stdin;
1	pending
2	shipped
\.
```

**Action:** Enum values in COPY data are plain text - no modification needed.

### 7. Enum Ordering Matters

Both MySQL and PostgreSQL use list order for comparisons:

```sql
-- 'pending' < 'shipped' if pending comes first in definition
SELECT * FROM orders WHERE status > 'pending';
```

**Critical:** Preserve exact label ordering during conversion.

### 8. Unicode in Labels

```sql
CREATE TYPE emoji_status AS ENUM ('✅ done', '❌ failed', '⏳ pending');
```

**Solution:** Treat as UTF-8 strings, preserve exactly.

### 9. Empty Enum Types

```sql
CREATE TYPE empty_enum AS ENUM ();
```

**Solution:** Handle gracefully (emit empty ENUM() or skip with warning).

---

## Test Cases

### Unit Tests

| Test                                   | Description                                       |
| -------------------------------------- | ------------------------------------------------- |
| `parse_pg_create_enum`                 | Parse CREATE TYPE ... AS ENUM with various labels |
| `parse_pg_alter_enum_add`              | Parse ALTER TYPE ... ADD VALUE                    |
| `parse_pg_alter_enum_add_before_after` | Parse ADD VALUE BEFORE/AFTER                      |
| `parse_mysql_inline_enum`              | Parse ENUM('a','b','c')                           |
| `parse_enum_escaped_quotes`            | Handle `'it''s'` labels                           |
| `parse_enum_unicode`                   | Handle unicode labels                             |
| `parse_enum_empty`                     | Handle empty ENUM()                               |
| `strip_pg_cast`                        | Remove `'value'::type` casts                      |
| `strip_pg_cast_preserves_non_enum`     | Don't strip non-enum casts                        |

### Integration Tests

| Test                           | Description                        |
| ------------------------------ | ---------------------------------- |
| `pg_to_mysql_simple_enum`      | Single enum type used in one table |
| `pg_to_mysql_shared_enum`      | One type used in multiple tables   |
| `pg_to_mysql_multiple_enums`   | Multiple types in one table        |
| `pg_to_mysql_with_data`        | COPY/INSERT with enum values       |
| `pg_to_mysql_with_casts`       | Strip ::type casts in INSERTs      |
| `pg_to_mysql_alter_type`       | Handle ALTER TYPE ADD VALUE        |
| `mysql_to_pg_single_column`    | Single ENUM column                 |
| `mysql_to_pg_multiple_columns` | Multiple ENUM columns in table     |
| `mysql_to_pg_multiple_tables`  | ENUMs across multiple tables       |
| `mysql_to_pg_with_defaults`    | ENUM with DEFAULT value            |
| `any_to_sqlite_enum`           | Verify TEXT conversion             |
| `roundtrip_mysql_pg_mysql`     | Convert both directions            |

### Edge Case Tests

| Test                           | Description                |
| ------------------------------ | -------------------------- |
| `unknown_enum_type_fallback`   | VARCHAR fallback + warning |
| `out_of_order_create_type`     | Type defined after table   |
| `schema_qualified_type`        | Handle schema.typename     |
| `quoted_type_name`             | Handle "Type Name"         |
| `enum_label_with_comma`        | Labels containing commas   |
| `enum_label_with_parenthesis`  | Labels containing `()`     |
| `alter_type_after_use_warning` | Warn on late ALTER TYPE    |

---

## Implementation Plan

### Phase 1: PostgreSQL → MySQL (~12h)

1. **Add EnumRegistry struct** (2h)
   - HashMap for type definitions
   - Methods for registration and lookup

2. **Parse CREATE TYPE ... AS ENUM** (3h)
   - Regex extraction
   - Label parsing with quote handling
   - Registry population

3. **Rewrite CREATE TABLE columns** (3h)
   - Detect enum type references
   - Replace with inline ENUM()
   - Generate correct MySQL quoting

4. **Strip ::type casts** (2h)
   - Detect enum type casts in DML
   - Strip only for known enum types

5. **Handle ALTER TYPE** (2h)
   - Parse ADD VALUE
   - Update registry
   - Warning for late modifications

### Phase 2: MySQL → PostgreSQL (~10h)

1. **Parse inline ENUM() definitions** (2h)
   - Extract from CREATE TABLE
   - Extract column context (table, column name)

2. **Generate CREATE TYPE statements** (3h)
   - Deterministic naming convention
   - Emit before CREATE TABLE
   - Track emitted types

3. **Multi-statement output** (3h)
   - Change converter to return Vec<Vec<u8>>
   - Update runner to handle multiple outputs

4. **Handle deduplication (optional)** (2h)
   - Signature-based type reuse
   - Flag to control behavior

### Phase 3: Testing & Polish (~6h)

1. Unit tests for parsing (2h)
2. Integration tests for conversion (2h)
3. Real-world dump testing (1h)
4. Documentation (1h)

---

## Configuration Options

Consider adding CLI flags:

```bash
# MySQL → PostgreSQL
sql-splitter convert dump.sql --from mysql --to postgres \
  --enum-naming per-column  # or: dedupe

# PostgreSQL → MySQL
sql-splitter convert dump.sql --from postgres --to mysql \
  --strict  # fail on unknown enum types instead of VARCHAR fallback
```

---

## Future Enhancements

1. **MSSQL Support**: SQL Server has no native ENUM; consider CHECK constraints
2. **CHECK Constraint Generation**: Generate `CHECK (status IN ('a','b'))` as alternative
3. **Enum Documentation Comments**: Preserve/generate comments listing valid values
4. **Bidirectional Sync**: Track changes for schema migration generation

---

## Related Files

| File                           | Changes Needed                            |
| ------------------------------ | ----------------------------------------- |
| `src/convert/mod.rs`           | Add EnumRegistry, update convert methods  |
| `src/convert/types.rs`         | Remove VARCHAR fallback, add enum helpers |
| `src/convert/enum_registry.rs` | New file for registry struct              |
| `src/convert/enum_parser.rs`   | New file for parsing helpers              |
| `tests/convert_enum_test.rs`   | New integration tests                     |

---

## References

- [PostgreSQL CREATE TYPE ENUM](https://www.postgresql.org/docs/current/sql-createtype.html)
- [PostgreSQL ALTER TYPE](https://www.postgresql.org/docs/current/sql-altertype.html)
- [MySQL ENUM Type](https://dev.mysql.com/doc/refman/8.0/en/enum.html)
