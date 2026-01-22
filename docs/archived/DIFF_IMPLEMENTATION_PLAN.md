# Diff Command Implementation Plan

**Version**: v1.9.1  
**Last Updated**: 2025-12-21  
**Status**: Completed

This document provides a concrete implementation plan for the `diff` command, building on the design in [DIFF_FEATURE.md](DIFF_FEATURE.md).

---

## Executive Summary

The diff command compares two SQL dumps and reports schema + data differences.

**v1.9.0 (Released):** Core functionality

- Schema diff with migration SQL generation
- Data summary (counts per table)
- Memory-bounded operation

**v1.9.1 (In Progress):** Enhanced features

- Verbose mode with PK samples
- Primary key override
- Column order ignoring
- Index diff
- Column ignore patterns
- No-PK table handling

---

## v1.9.0 Completed Scope

| Component                                                   | Status  |
| ----------------------------------------------------------- | ------- |
| Schema comparison (tables, columns, PKs, FKs)               | ✅ Done |
| Schema migration SQL (CREATE/DROP/ALTER)                    | ✅ Done |
| Data row counts (added/removed/modified)                    | ✅ Done |
| Memory-bounded PK tracking                                  | ✅ Done |
| All 3 dialects (MySQL, PostgreSQL, SQLite)                  | ✅ Done |
| Output formats (text, json, sql)                            | ✅ Done |
| CLI flags (--schema-only, --data-only, --tables, --exclude) | ✅ Done |
| PostgreSQL COPY data parsing                                | ✅ Done |
| Compressed input support                                    | ✅ Done |

---

## v1.9.1 Implementation Plan

### Phase 1: `--verbose` with Sample Collection (2h)

**Goal:** Show actual PK values for added/removed/modified rows.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 1.1 Add sample fields to TableDataDiff | 0.5h | `sample_added_pks`, `sample_removed_pks`, `sample_modified_pks` |
| 1.2 Collect samples in DataDiffer | 0.5h | Store first N PKs during scanning |
| 1.3 Format samples in text output | 0.5h | Show PKs in verbose mode |
| 1.4 Format samples in JSON output | 0.25h | Add to JSON structure |
| 1.5 Tests | 0.25h | Unit + integration tests |

**Key Changes:**

```rust
// data.rs
pub struct TableDataDiff {
    // ... existing fields ...
    /// Sample PKs for added rows (only when verbose)
    pub sample_added_pks: Vec<String>,
    /// Sample PKs for removed rows (only when verbose)
    pub sample_removed_pks: Vec<String>,
    /// Sample PKs for modified rows (only when verbose)
    pub sample_modified_pks: Vec<String>,
}
```

**Sample Collection Logic:**

- During `compute_diff()`, collect PKs when iterating through maps
- Format PK as string: single value or `(val1, val2)` for composite
- Limit to `sample_size` (default 100)

---

### Phase 2: `--primary-key` Override (2h)

**Goal:** Allow specifying PK columns for data comparison.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 2.1 CLI argument parsing | 0.5h | Parse `--primary-key table:col,table2:col1+col2` |
| 2.2 Add to DiffConfig | 0.25h | `pk_overrides: HashMap<String, Vec<String>>` |
| 2.3 Apply in DataDiffer | 0.5h | Use override instead of schema PK |
| 2.4 Validate columns exist | 0.25h | Error if column not in schema |
| 2.5 Tests | 0.5h | All dialects |

**Parsing Logic:**

```rust
// Parse "users:email,orders:id+user_id"
fn parse_pk_overrides(s: &str) -> HashMap<String, Vec<String>> {
    s.split(',')
        .filter_map(|pair| {
            let (table, cols) = pair.split_once(':')?;
            let columns: Vec<String> = cols.split('+').map(|s| s.trim().to_string()).collect();
            Some((table.trim().to_string(), columns))
        })
        .collect()
}
```

---

### Phase 3: `--ignore-order` for Column Order (1h)

**Goal:** Ignore column position when comparing schemas.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 3.1 Add flag to DiffConfig | 0.1h | `ignore_column_order: bool` |
| 3.2 Modify schema comparison | 0.5h | Compare as sets, not lists |
| 3.3 Tests | 0.4h | All dialects |

**Current vs New Comparison:**

```rust
// Current: ordered comparison detects position changes
// New with --ignore-order: set comparison

fn compare_tables(old: &TableSchema, new: &TableSchema, ignore_order: bool) {
    if ignore_order {
        // Compare columns by name only, ignore position
        // columns_added = new_names - old_names
        // columns_removed = old_names - new_names
    } else {
        // Current logic (compare in order)
    }
}
```

---

### Phase 4: Index Diff (4h)

**Goal:** Detect added/removed/modified indexes beyond PK.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 4.1 Add IndexDef to schema | 0.5h | `name, columns, is_unique, index_type` |
| 4.2 Parse inline indexes | 1h | `INDEX`, `KEY`, `UNIQUE` in CREATE TABLE |
| 4.3 Parse CREATE INDEX statements | 1h | Standalone statements |
| 4.4 Compare indexes | 0.5h | Added/removed/modified |
| 4.5 Format in outputs | 0.5h | Text, JSON, SQL formatters |
| 4.6 Tests | 0.5h | All dialects |

**Schema Types:**

```rust
#[derive(Debug, Clone, Serialize)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub index_type: Option<String>, // BTREE, HASH, GIN, etc.
}
```

**Parsing Patterns:**

```sql
-- MySQL inline
INDEX idx_name (col1, col2)
KEY idx_name (col1)
UNIQUE INDEX idx_name (col1)

-- PostgreSQL/MySQL standalone
CREATE INDEX idx_name ON table (col1, col2);
CREATE UNIQUE INDEX idx_name ON table (col1);

-- PostgreSQL with type
CREATE INDEX idx_name ON table USING btree (col1);
CREATE INDEX idx_name ON table USING gin (col1);
```

---

### Phase 5: `--ignore-columns` Glob Patterns (3h)

**Goal:** Exclude columns from comparison using glob patterns.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 5.1 CLI parsing | 0.25h | Parse comma-separated patterns |
| 5.2 Glob pattern matching | 0.5h | Match `table.column` against patterns |
| 5.3 Filter in schema comparison | 0.5h | Skip ignored columns |
| 5.4 Filter in data comparison | 1h | Skip ignored columns in digest |
| 5.5 Validate not ignoring PK | 0.25h | Error if pattern matches PK |
| 5.6 Tests | 0.5h | All dialects |

**Pattern Matching:**

```rust
use glob::Pattern;

fn should_ignore_column(table: &str, column: &str, patterns: &[Pattern]) -> bool {
    let full_name = format!("{}.{}", table, column);
    patterns.iter().any(|p| p.matches(&full_name))
}

// Examples:
// "*.updated_at" matches "users.updated_at", "orders.updated_at"
// "users.last_login" matches only "users.last_login"
// "*.*_at" matches any column ending in _at
```

**Data Comparison Changes:**

- When building row digest, exclude ignored column values
- When extracting PK, error if any PK column is ignored

---

### Phase 6: `--allow-no-pk` / No-PK Handling (1h)

**Goal:** Better handling for tables without primary key.

**Tasks:**
| Task | Effort | Description |
|------|--------|-------------|
| 6.1 Add warnings collection | 0.25h | `Vec<Warning>` in DiffResult |
| 6.2 Emit warning when skipping table | 0.25h | "No PK, skipping data comparison" |
| 6.3 Add --allow-no-pk flag | 0.25h | Use all columns as PK |
| 6.4 Tests | 0.25h | All dialects |

**Warning Structure:**

```rust
#[derive(Debug, Serialize)]
pub struct Warning {
    pub table: Option<String>,
    pub message: String,
}

pub struct DiffResult {
    pub schema: Option<SchemaDiff>,
    pub data: Option<DataDiff>,
    pub warnings: Vec<Warning>,
    pub summary: DiffSummary,
}
```

---

### Phase 7: Testing (6h)

**Goal:** Comprehensive test coverage across all dialects.

See "Test Plan" section below.

---

## Test Plan

### Test Matrix

Each feature must be tested across all 3 dialects where applicable:

| Feature                     | MySQL | PostgreSQL | SQLite | Notes                  |
| --------------------------- | ----- | ---------- | ------ | ---------------------- |
| Verbose samples             | ✅    | ✅         | ✅     | All dialects           |
| Verbose + COPY data         | -     | ✅         | -      | PostgreSQL only        |
| PK override single col      | ✅    | ✅         | ✅     | All dialects           |
| PK override composite       | ✅    | ✅         | ✅     | All dialects           |
| PK override invalid col     | ✅    | -          | -      | Error handling         |
| Ignore order - no change    | ✅    | ✅         | ✅     | All dialects           |
| Ignore order - reordered    | ✅    | ✅         | ✅     | All dialects           |
| Index diff - inline         | ✅    | ✅         | ✅     | All dialects           |
| Index diff - CREATE INDEX   | ✅    | ✅         | ✅     | All dialects           |
| Index diff - UNIQUE         | ✅    | ✅         | ✅     | All dialects           |
| Index diff - type (GIN)     | -     | ✅         | -      | PostgreSQL only        |
| Ignore columns - single     | ✅    | ✅         | ✅     | All dialects           |
| Ignore columns - glob       | ✅    | ✅         | ✅     | All dialects           |
| Ignore columns - PK error   | ✅    | -          | -      | Error handling         |
| No-PK warning               | ✅    | ✅         | ✅     | All dialects           |
| Allow-no-pk                 | ✅    | ✅         | ✅     | All dialects           |
| JSON output with new fields | ✅    | -          | -      | One dialect sufficient |
| SQL output with indexes     | ✅    | ✅         | -      | MySQL + PostgreSQL     |

### Integration Tests to Add

```rust
// tests/diff_integration_test.rs

// --- Verbose Mode Tests ---
#[test] fn test_diff_verbose_shows_sample_pks_mysql() { ... }
#[test] fn test_diff_verbose_shows_sample_pks_postgres() { ... }
#[test] fn test_diff_verbose_shows_sample_pks_sqlite() { ... }
#[test] fn test_diff_verbose_composite_pk_format() { ... }
#[test] fn test_diff_verbose_postgres_copy_samples() { ... }
#[test] fn test_diff_verbose_json_includes_samples() { ... }
#[test] fn test_diff_verbose_limits_sample_count() { ... }

// --- Primary Key Override Tests ---
#[test] fn test_diff_pk_override_single_column_mysql() { ... }
#[test] fn test_diff_pk_override_single_column_postgres() { ... }
#[test] fn test_diff_pk_override_single_column_sqlite() { ... }
#[test] fn test_diff_pk_override_composite() { ... }
#[test] fn test_diff_pk_override_invalid_column_error() { ... }
#[test] fn test_diff_pk_override_multiple_tables() { ... }

// --- Ignore Order Tests ---
#[test] fn test_diff_ignore_order_no_change_mysql() { ... }
#[test] fn test_diff_ignore_order_no_change_postgres() { ... }
#[test] fn test_diff_ignore_order_no_change_sqlite() { ... }
#[test] fn test_diff_ignore_order_reordered_columns() { ... }
#[test] fn test_diff_without_ignore_order_detects_reorder() { ... }

// --- Index Diff Tests ---
#[test] fn test_diff_index_added_mysql() { ... }
#[test] fn test_diff_index_added_postgres() { ... }
#[test] fn test_diff_index_added_sqlite() { ... }
#[test] fn test_diff_index_removed() { ... }
#[test] fn test_diff_index_modified_columns() { ... }
#[test] fn test_diff_unique_index() { ... }
#[test] fn test_diff_index_postgres_using_gin() { ... }
#[test] fn test_diff_index_inline_create_table() { ... }
#[test] fn test_diff_index_standalone_create_index() { ... }
#[test] fn test_diff_index_sql_output() { ... }

// --- Ignore Columns Tests ---
#[test] fn test_diff_ignore_columns_single_mysql() { ... }
#[test] fn test_diff_ignore_columns_single_postgres() { ... }
#[test] fn test_diff_ignore_columns_single_sqlite() { ... }
#[test] fn test_diff_ignore_columns_glob_star() { ... }
#[test] fn test_diff_ignore_columns_glob_suffix() { ... }
#[test] fn test_diff_ignore_columns_schema_diff() { ... }
#[test] fn test_diff_ignore_columns_data_diff() { ... }
#[test] fn test_diff_ignore_columns_pk_error() { ... }
#[test] fn test_diff_ignore_columns_multiple_patterns() { ... }

// --- No-PK Handling Tests ---
#[test] fn test_diff_no_pk_warning_mysql() { ... }
#[test] fn test_diff_no_pk_warning_postgres() { ... }
#[test] fn test_diff_no_pk_warning_sqlite() { ... }
#[test] fn test_diff_allow_no_pk_uses_all_columns() { ... }
#[test] fn test_diff_no_pk_json_includes_warning() { ... }
```

### Unit Tests to Add

```rust
// tests/diff_unit_test.rs

// --- Glob Pattern Matching ---
#[test] fn test_ignore_columns_pattern_star_prefix() { ... }
#[test] fn test_ignore_columns_pattern_star_suffix() { ... }
#[test] fn test_ignore_columns_pattern_exact() { ... }
#[test] fn test_ignore_columns_pattern_table_star() { ... }

// --- PK Override Parsing ---
#[test] fn test_parse_pk_override_single() { ... }
#[test] fn test_parse_pk_override_composite() { ... }
#[test] fn test_parse_pk_override_multiple_tables() { ... }
#[test] fn test_parse_pk_override_whitespace() { ... }

// --- Index Parsing ---
#[test] fn test_parse_inline_index_mysql() { ... }
#[test] fn test_parse_inline_unique_index() { ... }
#[test] fn test_parse_create_index_mysql() { ... }
#[test] fn test_parse_create_index_postgres() { ... }
#[test] fn test_parse_create_index_using_clause() { ... }

// --- Sample Collection ---
#[test] fn test_sample_pk_formatting_single() { ... }
#[test] fn test_sample_pk_formatting_composite() { ... }
#[test] fn test_sample_collection_limits() { ... }
```

---

## File Changes Summary

| File                             | Changes                                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------------------------- |
| `src/cmd/diff.rs`                | Add CLI args: `--verbose`, `--primary-key`, `--ignore-order`, `--ignore-columns`, `--allow-no-pk` |
| `src/differ/mod.rs`              | Add fields to DiffConfig, add warnings to DiffResult                                              |
| `src/differ/data.rs`             | Sample collection, PK override, ignore columns in digest, no-PK handling                          |
| `src/differ/schema.rs`           | Index comparison, ignore order, ignore columns in schema diff                                     |
| `src/differ/output/text.rs`      | Verbose samples, index changes, warnings                                                          |
| `src/differ/output/json.rs`      | Sample PKs, indexes, warnings                                                                     |
| `src/differ/output/sql.rs`       | CREATE INDEX, DROP INDEX statements                                                               |
| `src/schema/mod.rs`              | Add `IndexDef`, add `indexes` to TableSchema                                                      |
| `src/schema/ddl.rs`              | Parse INDEX in CREATE TABLE, parse CREATE INDEX                                                   |
| `Cargo.toml`                     | Add `glob` dependency                                                                             |
| `tests/diff_integration_test.rs` | ~40 new tests                                                                                     |
| `tests/diff_unit_test.rs`        | ~15 new tests                                                                                     |

---

## Effort Summary

| Phase     | Tasks           | Effort   |
| --------- | --------------- | -------- |
| Phase 1   | Verbose samples | 2h       |
| Phase 2   | PK override     | 2h       |
| Phase 3   | Ignore order    | 1h       |
| Phase 4   | Index diff      | 4h       |
| Phase 5   | Ignore columns  | 3h       |
| Phase 6   | No-PK handling  | 1h       |
| Phase 7   | Testing         | 6h       |
| **Total** |                 | **~19h** |

---

## Implementation Order

Recommended order for incremental development:

1. **Phase 6: No-PK handling** (1h) - Simple, adds warnings infrastructure
2. **Phase 3: Ignore order** (1h) - Simple flag
3. **Phase 1: Verbose samples** (2h) - High visibility feature
4. **Phase 2: PK override** (2h) - Useful for testing
5. **Phase 5: Ignore columns** (3h) - Needs glob dependency
6. **Phase 4: Index diff** (4h) - Most complex, builds on schema
7. **Phase 7: Testing** (ongoing) - TDD throughout

---

## Dependencies

| Crate  | Version | Purpose                                 |
| ------ | ------- | --------------------------------------- |
| `glob` | 0.3     | Pattern matching for `--ignore-columns` |

---

## Risks & Mitigations

| Risk                     | Mitigation                                       |
| ------------------------ | ------------------------------------------------ |
| Index parsing complexity | Start with common patterns, document limitations |
| Glob pattern edge cases  | Use well-tested `glob` crate                     |
| Sample memory usage      | Hard cap on sample count                         |
| PK override validation   | Validate columns exist before scanning           |

---

## Success Criteria

1. All 6 features implemented and tested
2. All tests pass across 3 dialects
3. Documentation updated
4. No regressions in existing functionality
5. Memory usage remains bounded

---

## Related Documents

- [DIFF_FEATURE.md](DIFF_FEATURE.md) - Feature design document
- [ROADMAP.md](../../ROADMAP.md) - Project roadmap
- [src/schema/ddl.rs](../../src/schema/ddl.rs) - Schema parsing
- [src/differ/data.rs](../../src/differ/data.rs) - Data comparison
