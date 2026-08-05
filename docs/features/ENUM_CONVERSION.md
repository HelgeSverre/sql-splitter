# Enum Type Conversion Feature Design

**Status**: Planning (v1.18.0)
**Date**: 2025-01-22 (renumbered 2026-05-07: v1.13.0 → v1.14.0; 2026-08-05: expanded
scope beyond `convert` to include schema, graph, differ, render, generate)
**Priority**: Medium
**Effort**: ~40 hours (revised up from 30h to include all downstream surfaces)

## Overview

Make enum types first-class across all sql-splitter surfaces. Today enums are
lossily collapsed to VARCHAR/TEXT everywhere. This feature replaces that with
proper semantic-preserving conversion, schema-level enum storage, and
enum-aware display in graphs, diffs, and DDL rendering.

## Scope: Surfaces That Need Enum Awareness

### Tier 1 — The `convert` command (original design scope)

| Conversion            | Lossy (now)                     | Semantic (target)                              |
| --------------------- | ------------------------------- | ---------------------------------------------- |
| MySQL → PostgreSQL    | `ENUM('a','b')` → `VARCHAR(255)` | `CREATE TYPE enum__t__c AS ENUM ('a','b')`     |
| PostgreSQL → MySQL    | `CREATE TYPE` skipped           | inline `ENUM('a','b')` per column              |
| MySQL → SQLite        | `ENUM('a','b')` → `TEXT`        | No change (SQLite has no enum)                 |
| PostgreSQL → SQLite   | `CREATE TYPE` skipped           | No change (SQLite has no enum)                 |
| MySQL/PG → MSSQL      | `ENUM` → `NVARCHAR(255)`        | No change (MSSQL has no native enum)           |

**Key behaviors to change:**

- `CREATE TYPE ... AS ENUM` currently skipped by `is_postgres_only_feature()`
- `ALTER TYPE ... ADD VALUE` currently skipped by `is_postgres_only_feature()`
- `ENUM(...)` in CREATE TABLE currently blanket-replaced by `RE_ENUM` regex
- `::enum_type` casts currently stripped by generic `strip_postgres_casts`
- `::enum_type` casts in DML not stripped
- Unknown enum type references → VARCHAR fallback + warning
- Multi-statement output needed (one MySQL CREATE TABLE → multiple PG statements)

### Tier 2 — Schema model, graph, differ, render (semantic storage + display)

| Surface | Current | Target |
| ------- | ------- | ------ |
| `schema::ColumnType` | `"enum"` → `ColumnType::Text` | `ColumnType::Enum(Vec<String>)` with member values |
| `graph::format_column_type` | `Text` → `"VARCHAR"` | `Enum` → `"ENUM"` in DOT/Mermaid/JSON/HTML |
| `differ::format_column_type` | `Text` → `"TEXT"` | `Enum` → `"ENUM('a','b',...)"` |
| `differ` schema comparison | `old_col.col_type != new_col.col_type` works but won't detect member-list change | Detects enum member list changes as schema changes |
| `render::ddl` | `map_column_type()` emits VARCHAR | Uses enum-aware type mapping |
| `generate::verify` | `map_column_type()` emission check | Reflects proper enum type expectations |
| `synthetic::SqlTypeFamily` | No `Enum` variant | `SqlTypeFamily::Enum` for profiling + generation |
| `profile::profiler` | Enum → `ColumnKind::Text` | Preserves enum identity |

### Tier 3 — Synthetic generation (future enhancement, out of initial scope)

| Surface | Current | Target (future) |
| ------- | ------- | --------------- |
| `generate` generators | Enum cols get random text generators | `enum_choice` generator picking from declared members |
| `profile` heuristics | No enum detection | Detect low-cardinality text cols as enum candidates |
| `generate::planners` | No enum routing | Type-family routing for enum columns |

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
3. Strip cast: `'value'::type_name` → `'value'`

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

### Strategy: Per-Column Types (default)

| Strategy                | Pros                                | Cons                         |
| ----------------------- | ----------------------------------- | ---------------------------- |
| **Per-column types**    | No semantic coupling, deterministic | Type explosion               |
| **Dedupe by signature** | Fewer types, reusable               | May couple unrelated columns |

**Default:** Per-column types with deterministic naming.
**CLI flag:** `--enum-naming dedupe` for signature-based reuse.

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

**Implementation:** `convert_statement()` returns `Result<Vec<Vec<u8>>>` instead of
`Result<Vec<u8>>`. This is the single largest architectural change — it ripples
through `run()`, all statement handlers, and COPY data processing.

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

**Fallback (simpler):** Warn and convert to VARCHAR for ALTER cases.

---

## SQLite and MSSQL Behavior

**SQLite:** No change. Continue converting ENUMs to TEXT.
**MSSQL:** No change. Continue converting ENUMs to NVARCHAR(255).
  Future: generate CHECK constraint `CHECK (col IN ('a','b','c'))` as alternative.

---

## Architecture: Enum Registry

New struct with state tracking for the streaming converter:

```rust
pub struct EnumRegistry {
    pg_enums_by_name: HashMap<String, Vec<String>>,

    /// canonical(labels) → generated_pg_type_name (for dedup mode)
    enum_signatures: HashMap<String, String>,

    /// Track which CREATE TYPE statements have been emitted
    emitted_pg_types: HashSet<String>,
}

impl EnumRegistry {
    pub fn new() -> Self { ... }
    pub fn register_pg_enum(&mut self, name: &str, labels: Vec<String>) { ... }
    pub fn get_pg_enum(&self, name: &str) -> Option<&[String]> { ... }
    pub fn get_or_create_pg_type_for_signature(
        &mut self, table: &str, column: &str, labels: &[String],
    ) -> String { ... }
    pub fn mark_emitted(&mut self, name: &str) -> bool { ... }
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
    created_tables: HashMap<String, String>,
    deferred: Vec<String>,
    enum_registry: EnumRegistry,  // NEW
    /// CLI flag: per-column (default) or dedupe
    enum_naming: EnumNamingStrategy,  // NEW
}
```

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

// Parse MySQL inline ENUM (must handle nested parens in values)
static RE_MYSQL_ENUM: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bENUM\s*\(([^)]*(?:\([^)]*\)[^)]*)*)\)").unwrap()
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

## Implementation Phases

### Phase 1: Convert Command Core (~14h)

**New files:**
- `src/convert/enum_registry.rs` — `EnumRegistry` struct with HashMap-based tracking
- `src/convert/enum_parser.rs` — Regex patterns + label extraction for `CREATE TYPE`,
  inline `ENUM()`, `ALTER TYPE`, and cast stripping
- `tests/convert_enum_test.rs` — Integration and edge case tests

**Create `EnumRegistry` (2h)**
- HashMap for type definitions
- Methods for registration, lookup, de-duplication

**Parse + register `CREATE TYPE ... AS ENUM` (3h)**
- Regex extraction
- Label parsing with quote handling
- Registry population
- `is_postgres_only_feature()`: remove `CREATE TYPE` and `ALTER TYPE` from the skip
  list when target is MySQL

**Rewrite CREATE TABLE columns (3h)**
- PG→MySQL: detect enum type references, replace with inline `ENUM()`
- MySQL→PG: extract inline ENUMs, generate CREATE TYPE statements, emit before CREATE TABLE

**Strip `::type` casts in DML (2h)**
- Detect enum type casts in INSERT/UPDATE/DELETE
- Strip only for known enum types

**Handle ALTER TYPE (2h)**
- Parse ADD VALUE, update registry
- Warning for late modifications (type already used in emitted table)

**Multi-statement output (2h)**
- Change `convert_statement()` signature from `Result<Vec<u8>>` to `Result<Vec<Vec<u8>>>`
- Update `run()` to handle multiple output statements per input
- Update COPY data processing to match

### Phase 2: Schema Model + Type Mapping (~10h)

**`schema::ColumnType` — Add `Enum(Vec<String>)` variant (1.5h)**
- `src/schema/mod.rs`: add variant to `ColumnType` enum (line 41)
- `src/schema/mod.rs`: extract enum members in `from_sql_type()` at `"enum"` match arm
  (line 95) — parse `ENUM('a','b')` from `source_type`
- All `ColumnType` match arms add `Enum` handling

**`synthetic::SqlTypeFamily` — Add `Enum` variant (1h)**
- `src/synthetic/schema.rs`: add variant to `SqlTypeFamily` enum (line 106)
- `from_column()` and `from_source_type()` — map `ColumnType::Enum` → `SqlTypeFamily::Enum`
- `from_unclassified_source_type()` — sniff for `enum(` in source_type

**`convert::types` — Enum-aware type mapping (2.5h)**
- `mysql_to_postgres()`: replace blanket `RE_ENUM` → `VARCHAR(255)` with registry-driven
  per-column rewrite
- `mysql_to_sqlite()`, `mysql_to_mssql()`: keep TEXT/NVARCHAR (SQLite/MSSQL stay lossy)
- `postgres_to_mysql()`: stop relying on `is_postgres_only_feature()` skip — use
  registry-lookup to emit inline ENUM
- `map_column_type()`: PG↔MySQL no longer classified as lossy for enum
- `is_narrowed_by_conversion()`: PG↔MySQL enum no longer returns true

**`convert::mod` — Remove ENUM warnings (0.5h)**
- `detect_unsupported_features()` (line 1605-1622): stop emitting
  `UnsupportedFeature` warning for ENUM types on PG↔MySQL paths
- Adjust suggestion text; MSSQL + SQLite targets keep their varchar/check-constraint
  suggestion

**`render::ddl` — Enum-aware DDL rendering (1h)**
- `render_column_def()` at `src/render/ddl.rs:178`: `map_column_type()` will now
  return proper enum types instead of VARCHAR for PG↔MySQL
- Verify that PG→MySQL renders inline `ENUM()` and MySQL→PG renders type references

**`generate::verify` — Enum-aware expectation (1h)**
- `src/generate/verify.rs:789`: `map_column_type()` call now produces enum types,
  so verify expectations must handle enum-type columns

**`generate::mod` — Merge render warnings (0.5h)**
- `merge_render_warnings()` at `src/generate/mod.rs:1161`: `LossyConversion` for enum
  no longer fires on PG↔MySQL paths; verify this is correct

**`profile::profiler` — Enum identity preserved (1h)**
- `src/profile/profiler.rs:212`: add `ColumnType::Enum` → `ColumnKind` mapping
- Enum columns should be classified as text-like for profiling purposes

**`parser::mysql_insert` — PK coercion (0.5h)**
- Add `ColumnType::Enum` match arm (line 390); return `None` like `Text` does

### Phase 3: Graph + Differ Display (~4h)

**`graph::view` — `format_column_type` (1h)**
- `src/graph/view.rs:328`: add `ColumnType::Enum(values)` → `"ENUM"` display
- All four graph format outputs (DOT, JSON, Mermaid, HTML) pick up the change
  through `ColumnInfo.col_type` (a `String` produced by `format_column_type`)

**`differ::schema` — `format_column_type` (1h)**
- `src/differ/schema.rs:73`: add `ColumnType::Enum(values)` → `"ENUM('a','b',...)"`
- Schema comparison: `old_col.col_type != new_col.col_type` now detects member-list
  changes as schema changes
- SQL output (`src/differ/output/sql.rs`): verify `col.col_type` serialization

**`redactor::config_generator` — Debug formatting (0.5h)**
- `src/redactor/config_generator.rs:80,84`: `format!("{:?}", col.col_type)` uses
  Debug formatting — adding `Enum(Vec<String>)` changes debug output format
- Verify PII detection still works with the new debug representation

**All `ColumnType` constructors in tests (1.5h)**
- Every test that constructs `Column { col_type: ColumnType::Text, .. }` for enum
  columns needs review
- Key test files: `tests/schema_unit_test.rs`, `tests/parser_unit_test.rs`,
  `src/redactor/matcher.rs`, `src/redactor/rewriter.rs`, `src/differ/data.rs`,
  `benches/redact_bench.rs`
- Some may want `ColumnType::Enum(vec![...])` instead of `Text`

### Phase 4: Testing, Docs & Polish (~8h, runs alongside phases 1-3)

**New tests (5h)**
- 9 unit tests: `parse_pg_create_enum`, `parse_pg_alter_enum_add`,
  `parse_pg_alter_enum_add_before_after`, `parse_mysql_inline_enum`,
  `parse_enum_escaped_quotes`, `parse_enum_unicode`, `parse_enum_empty`,
  `strip_pg_cast`, `strip_pg_cast_preserves_non_enum`
- 12 integration tests: `pg_to_mysql_simple_enum`, `pg_to_mysql_shared_enum`,
  `pg_to_mysql_multiple_enums`, `pg_to_mysql_with_data`, `pg_to_mysql_with_casts`,
  `pg_to_mysql_alter_type`, `mysql_to_pg_single_column`,
  `mysql_to_pg_multiple_columns`, `mysql_to_pg_multiple_tables`,
  `mysql_to_pg_with_defaults`, `any_to_sqlite_enum`, `roundtrip_mysql_pg_mysql`
- 7 edge case tests: `unknown_enum_type_fallback`, `out_of_order_create_type`,
  `schema_qualified_type`, `quoted_type_name`, `enum_label_with_comma`,
  `enum_label_with_parenthesis`, `alter_type_after_use_warning`

**Update existing tests (1.5h)**
- `tests/convert_unit_test.rs:1074-1076` — `CREATE TYPE my_enum` test now expects
  registry registration, not empty output
- `tests/convert_unit_test.rs:1392-1401` — ENUM→VARCHAR lossy test now expects
  proper conversion for PG→MySQL path
- `tests/convert_integration_test.rs:225-259` — `test_convert_enum_warning` needs
  updated assertions
- `tests/generate_filter_test.rs:346-372` — cross-dialect lossy warning test for
  ENUM: PG↔MySQL should NOT warn; PG→SQLite still warns
- `tests/generate_cli_test.rs:891-941` — `lossy_cross_dialect_conversion` tests:
  PG↔MySQL no longer lossy for enum
- `src/convert/types.rs:952-966` — `narrowing_enum_to_a_plain_string` test: update
  for PG↔MySQL non-lossy path

**Documentation (1.5h)**
- `website/src/content/docs/reference/type-mappings.mdx:77-83` — update ENUM section
- `website/src/content/docs/reference/limitations.mdx:60` — remove "rewritten lossily"
- `website/src/content/docs/commands/convert.mdx:141` — update conversion table
- `website/src/content/docs/advanced/troubleshooting.mdx:262` — update ENUM entry
- `website/src/content/docs/advanced/ai-integration.mdx:125` — update example output
- `website/src/content/docs/cookbook/database-migration.mdx:60-65` — update ENUM section
- `skills/sql-splitter/SKILL.md` — add note about enum conversion capability
- `CHANGELOG.md` — add v1.18.0 entry

---

## Complete File Change Map

### New files

| File | Purpose |
| ---- | ------- |
| `src/convert/enum_registry.rs` | `EnumRegistry` struct with HashMap-based state tracking |
| `src/convert/enum_parser.rs` | Regex patterns + label extraction for all enum syntax forms |
| `tests/convert_enum_test.rs` | ~28 new integration + edge case tests |

### Existing files — Must change

#### Convert pipeline (Phase 1)

| File | Lines | Change |
| ---- | ----- | ------ |
| `src/convert/mod.rs` | 33 | Re-export enum types from registry module |
| `src/convert/mod.rs` | 84-100 | Add `EnumRegistry` field to `Converter` struct |
| `src/convert/mod.rs` | 132-143 | `Converter::new()` initializes `EnumRegistry` |
| `src/convert/mod.rs` | 176 | Change `convert_statement()` return type to `Vec<Vec<u8>>` |
| `src/convert/mod.rs` | 186-195 | Dispatch `CreateType` + `AlterType` to enum handlers |
| `src/convert/mod.rs` | 198-265 | `convert_create_table()` — PG↔MySQL enum-aware rewriting |
| `src/convert/mod.rs` | 268-295 | `convert_insert()` — `strip_postgres_casts()` already strips `::type` (generic, no enum changes needed) |
| `src/convert/mod.rs` | 335-383 | `convert_alter_table()` — delegates to `convert_data_types()`, no direct enum impact |
| `src/convert/mod.rs` | 466-489 | `convert_copy()` — no type conversion at all, no enum impact |
| `src/convert/mod.rs` | 492-556 | `convert_other()` — intercept `CREATE TYPE`/`ALTER TYPE` for enum |
| `src/convert/mod.rs` | 594-616 | `is_postgres_only_feature()` — remove `CREATE TYPE`/`ALTER TYPE` (or gate on target dialect) |
| `src/convert/mod.rs` | 1595-1622 | `detect_unsupported_features()` — stop warning on ENUM for enum-aware targets |
| `src/convert/mod.rs` | 1714-1866 | `run()` — handle `Vec<Vec<u8>>` output from `convert_statement()` |
| `src/convert/types.rs` | 30-48 | `map_column_type()` — enum no longer always lossy |
| `src/convert/types.rs` | 61-95 | `is_narrowed_by_conversion()` — PG↔MySQL enum not lossy |
| `src/convert/types.rs` | 178-227 | `mysql_to_postgres()` — replace RE_ENUM blanket with registry-driven rewrite |
| `src/convert/types.rs` | 229-281 | `mysql_to_sqlite()` — keep TEXT (no change) |
| `src/convert/types.rs` | 283-332 | `postgres_to_mysql()` — add inline ENUM emission |
| `src/convert/types.rs` | 403-463 | `mysql_to_mssql()` — keep NVARCHAR (no change) |
| `src/convert/types.rs` | 819-820 | `RE_ENUM` — keep for matching; rewrite logic moves to enum_registry |
| `src/convert/warnings.rs` | 12-32 | Adjust `UnsupportedFeature` suggestion text for remaining lossy targets |

#### Schema model (Phase 2)

| File | Lines | Change |
| ---- | ----- | ------ |
| `src/schema/mod.rs` | 41-58 | Add `Enum(Vec<String>)` variant to `ColumnType` |
| `src/schema/mod.rs` | 95 | `from_sql_type()` — extract enum members when matching `"enum"` |
| `src/synthetic/schema.rs` | 106-117 | Add `Enum` variant to `SqlTypeFamily` |
| `src/synthetic/schema.rs` | 126-134 | `from_column()` — map `ColumnType::Enum` → `SqlTypeFamily::Enum` |
| `src/synthetic/schema.rs` | 145-153 | `from_source_type()` — same mapping |
| `src/synthetic/schema.rs` | 159-168 | `from_unclassified_source_type()` — sniff `enum(` |
| `src/profile/profiler.rs` | 212-218 | Add `ColumnType::Enum` → `ColumnKind::Text` mapping |
| `src/parser/mysql_insert.rs` | 390-393 | Add `ColumnType::Enum` match arm (return `None`) |
| `src/redactor/config_generator.rs` | 80,84 | `format!("{:?}", col.col_type)` — adding `Enum(Vec<String>)` changes Debug output. Verify PII detection still works; may need custom Debug format or column-type-aware display |
| `src/render/ddl.rs` | 178 | `map_column_type()` — now receives enum-aware mapping from convert |
| `src/generate/verify.rs` | 789 | Same `map_column_type()` change |
| `src/generate/mod.rs` | 1161-1168 | `merge_render_warnings()` — verify `LossyConversion` doesn't fire for PG↔MySQL enum |

#### Graph and differ (Phase 3)

| File | Lines | Change |
| ---- | ----- | ------ |
| `src/graph/view.rs` | 328-338 | `format_column_type()` — add `Enum` arm → `"ENUM"` |
| `src/differ/schema.rs` | 73-83 | `format_column_type()` — add `Enum` arm → `"ENUM('a','b',...)"` |

#### Generator family constants (Phase 2 — required for Tier 2 compilation; generators themselves are Tier 3)

| File | Lines | Change |
| ---- | ----- | ------ |
| `src/generate/generators/core.rs` | 213-241 | `coerce_value()` — add `Enum` to `Text\|Uuid\|Other` fallback group at line 237 |
| `src/generate/generators/core.rs` | 245-255 | `ALL_FAMILIES` — add `Enum` |
| `src/generate/generators/mod.rs` | 45-56 | `CONSTANT_DESCRIPTOR.accepts` — add `SqlTypeFamily::Enum` to the hardcoded list |
| `src/generate/generators/observed.rs` | 109-135 | `coerce_sample()` — add `Enum` to `_ =>` fallthrough at line 133 |
| `src/generate/generators/observed.rs` | 159-170 | `ALL_FAMILIES` — add `Enum` |
| `src/generate/value.rs` | 114-129 | `compatible_with()` — add `Enum` to `Text\|Uuid\|Other` group at line 124-125 |
| `src/generate/planners/order_family.rs` | 1691-1705 | `family_of_source_type()` — detect `enum(` → `SqlTypeFamily::Enum` |
| `src/generate/compiler.rs` | 1014-1024 | Generator accepts check — `contains()` handles new variant automatically |
| `src/generate/compiler.rs` | 1168-1178 | Modifier accepts check — `contains()` handles new variant automatically |

#### Test files (Phase 4)

| File | Lines | Change |
| ---- | ----- | ------ |
| `tests/convert_unit_test.rs` | 1074-1076 | CREATE TYPE no longer empty output for PG→MySQL |
| `tests/convert_unit_test.rs` | 1150-1158 | Warning display test wording |
| `tests/convert_unit_test.rs` | 1390-1404 | ENUM→VARCHAR now ENUM→ENUM for PG↔MySQL |
| `tests/convert_integration_test.rs` | 225-259 | Updated assertions |
| `tests/generate_cli_test.rs` | 891-941 | PG↔MySQL enum no longer lossy |
| `tests/generate_filter_test.rs` | 346-372 | Same-dialect still clean; cross-dialect to non-enum target still warns |
| `tests/duckdb_test.rs` | 713-717 | DuckDB ENUM→VARCHAR test unchanged (correct) |
| `src/convert/types.rs` (tests) | 952-966 | Update lossy detection expectations |
| All test files using `ColumnType::Text` | various | Review: some may need `ColumnType::Enum` for accuracy |

#### Docs (Phase 4)

| File | Lines | Change |
| ---- | ----- | ------ |
| `website/src/content/docs/reference/type-mappings.mdx` | 77-83 | Update ENUM section |
| `website/src/content/docs/reference/limitations.mdx` | 60 | Remove "rewritten lossily" |
| `website/src/content/docs/commands/convert.mdx` | 141 | Update conversion table |
| `website/src/content/docs/advanced/troubleshooting.mdx` | 262 | Update ENUM entry |
| `website/src/content/docs/advanced/ai-integration.mdx` | 125 | Update example |
| `website/src/content/docs/cookbook/database-migration.mdx` | 60-65 | Update ENUM section |
| `website/src/content/docs/roadmap.mdx` | 91-101 | Mark as delivered |
| `skills/sql-splitter/SKILL.md` | — | Add note about enum conversion |
| `CHANGELOG.md` | — | Add v1.18.0 entry |

### Files that do NOT need changes

| File | Reason |
| ---- | ------ |
| `src/duckdb/types.rs:254-255` | DuckDB has no native ENUM — VARCHAR conversion is correct |
| `src/duckdb/loader.rs:661` | `RE_COLUMN_TYPE` regex already handles ENUM + parens |
| `src/convert/mod.rs:822-824` | `RE_SET` — SET type stays lossy, no target has SET |
| `src/json_schema.rs:225` | Uses JSON Schema `"enum"` keyword (not SQL ENUM) — unrelated |
| `src/analyzer/` | No type awareness, only statement-level stats |
| `src/validate/` | Validates structural integrity, not data constraints |

---

## CLI Flags

```bash
# MySQL → PostgreSQL: per-column types (default)
sql-splitter convert dump.sql --from mysql --to postgres

# MySQL → PostgreSQL: dedupe by signature
sql-splitter convert dump.sql --from mysql --to postgres \
  --enum-naming dedupe

# PostgreSQL → MySQL
sql-splitter convert dump.sql --from postgres --to mysql

# Strict mode: fail on unknown enum types instead of VARCHAR fallback
sql-splitter convert dump.sql --from postgres --to mysql --strict
```

---

## Key Design Decisions to Confirm

1. **Multi-statement output**: `convert_statement()` returns `Result<Vec<Vec<u8>>>`
   instead of `Result<Vec<u8>>`. This is the biggest architectural ripple —
   affects `run()`, all statement handlers, COPY data processing. Worth it vs.
   buffering CREATE TYPEs and emitting before the next DDL?

2. **Naming convention**: `enum__{table}__{column}` proposed. Deterministic but
   verbose. PostgreSQL has a 63-byte identifier limit. Alternative:
   `{table}_{column}_enum` or `enum_{table}_{column}`.

3. **Deduplication default**: Per-column types (simple, deterministic) vs.
   signature-based dedupe (reuses types across columns with identical member sets)?
   The `--enum-naming dedupe` flag gates the latter.

4. **`ColumnType::Enum(Vec<String>)` or `ColumnType::Enum` with separate field?**
   Storing values in the variant gives match exhaustiveness checking but adds
   size to every ColumnType. A separate `enum_values: Option<Vec<String>>` on
   `Column` avoids the size overhead but loses the compile-time guarantee.

5. **MSSQL CHECK constraint generation**: Generate `CHECK (col IN ('a','b','c'))`
   as an alternative to `NVARCHAR(255)`? Include in this phase or defer?

6. **`map_column_type()` for PG↔MySQL enum**: Should `map_column_type()` handle
   enum mapping directly (stateless), or should it require a `&EnumRegistry`
   parameter? The former is simpler but loses context (e.g., PG type name lookup
   without a registry). The latter is more correct but changes the function
   signature — affecting `render::ddl` and `generate::verify` call sites.

---

## Edge Cases to Handle

### 1. Schema-Qualified Type Names

```sql
CREATE TYPE myschema.order_status AS ENUM (...);
CREATE TABLE orders (status myschema.order_status);
```

Store normalized qualified names in registry, strip schema on lookup for MySQL.

### 2. Quoted Identifiers

```sql
CREATE TYPE "Order Status" AS ENUM (...);
```

Preserve original quoting for PG output, sanitize for MySQL identifier rules.

### 3. Shared Enum Types Across Tables

```sql
CREATE TYPE status AS ENUM ('active', 'inactive');
CREATE TABLE users (status status);
CREATE TABLE products (status status);
```

MySQL→PG with per-column naming: creates `enum__users__status` and
`enum__products__status` — two separate types with identical values.
MySQL→PG with dedupe: creates `enum__users__status` once, reused by `products`.

### 4. Out-of-Order Definitions

If CREATE TABLE appears before CREATE TYPE in dump:
- Option A: Buffer statements referencing unknown types (complex)
- Option B: Fallback to VARCHAR + warning (simple, recommended)

### 5. ALTER TYPE After Table Creation

```sql
CREATE TYPE status AS ENUM ('a', 'b');
CREATE TABLE orders (status status);
ALTER TYPE status ADD VALUE 'c';
```

For PG→MySQL: Inline ENUM in `orders` won't include 'c'. Warn that ALTER TYPE
occurred after table using the type.

### 6. COPY Data with Enum Values

PostgreSQL COPY data contains plain text values — no modification needed.

### 7. Enum Ordering Matters

Both MySQL and PostgreSQL use list order for comparisons. Preserve exact label
ordering during conversion.

### 8. Unicode and Empty Enums

Unicode labels preserved as UTF-8 strings. Empty `ENUM()` handled gracefully
(emit empty ENUM() or skip with warning).

---

## References

- [PostgreSQL CREATE TYPE ENUM](https://www.postgresql.org/docs/current/sql-createtype.html)
- [PostgreSQL ALTER TYPE](https://www.postgresql.org/docs/current/sql-altertype.html)
- [MySQL ENUM Type](https://dev.mysql.com/doc/refman/8.0/en/enum.html)
- [PostgreSQL Identifier Limits](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)
  (63-byte NAMEDATALEN)
