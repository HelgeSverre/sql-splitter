# Feature: Extract Tests to `tests/` Directory

## Overview

Extract all inline `#[cfg(test)] mod tests` blocks from source files into dedicated test files in the `tests/` directory, creating a clear separation between production code and test code.

## Current State

### Test Distribution (211+ tests total)

| Location | Tests | Pattern |
|----------|-------|---------|
| Inline in source files | ~180 | `#[cfg(test)] mod tests { ... }` |
| `src/parser/edge_case_tests.rs` | 83 | Dedicated test module |
| `tests/` directory | 30 | Integration tests |

### Files with Significant Inline Tests

| File | Tests | Lines |
|------|-------|-------|
| `src/parser/mod.rs` | 29 | ~300 lines of tests |
| `src/convert/mod.rs` | 21 | ~250 lines of tests |
| `src/convert/copy_to_insert.rs` | 12 | ~150 lines of tests |
| `src/sample/mod.rs` | 5 | ~100 lines of tests |
| `src/splitter/mod.rs` | 7 | ~80 lines of tests |
| `src/shard/mod.rs` | 6 | ~100 lines of tests |

## Proposed Structure

### Target Organization

```
sql-splitter/
├── src/
│   ├── parser/
│   │   ├── mod.rs              # Core logic only (no tests)
│   │   ├── mysql_insert.rs     # Core logic only
│   │   └── postgres_copy.rs    # Core logic only
│   ├── convert/
│   │   ├── mod.rs              # Core logic only
│   │   ├── copy_to_insert.rs   # Core logic only
│   │   └── types.rs            # Core logic only
│   ├── sample/
│   │   ├── mod.rs              # Core logic only
│   │   ├── config.rs           # Core logic only
│   │   └── reservoir.rs        # Core logic only
│   ├── splitter/
│   │   └── mod.rs              # Core logic only
│   ├── shard/
│   │   └── mod.rs              # Core logic only
│   └── ...
│
└── tests/
    ├── parser_test.rs                 # NEW: Parser unit tests (29 tests)
    ├── parser_edge_cases_test.rs      # MOVED: Edge case tests (83 tests)
    ├── convert_test.rs                # NEW: Convert unit tests (33 tests)
    ├── sample_test.rs                 # NEW: Sample unit tests (13 tests)
    ├── splitter_test.rs               # NEW: Splitter unit tests (7 tests)
    ├── shard_test.rs                  # NEW: Shard unit tests (6 tests)
    ├── schema_test.rs                 # NEW: Schema unit tests (3 tests)
    ├── merger_test.rs                 # NEW: Merger unit tests (3 tests)
    ├── convert_integration_test.rs    # EXISTING: Integration tests
    ├── sample_integration_test.rs     # EXISTING: Integration tests
    ├── fixtures/                      # EXISTING: Test fixtures
    └── data/                          # EXISTING: Generated test data
```

### File Naming Convention

- **`*_test.rs`** - Unit tests for a specific module
- **`*_integration_test.rs`** - End-to-end CLI tests

## Critical Consideration: Public API Requirement

Tests in the `tests/` directory are **integration tests** in Rust. They can only access the **public API** of the crate.

### What This Means

```rust
// src/parser/mod.rs
pub fn parse_sql(...) { ... }        // ✅ Accessible from tests/
fn internal_helper(...) { ... }       // ❌ NOT accessible from tests/
pub(crate) fn crate_only(...) { ... } // ❌ NOT accessible from tests/
```

### Required Changes

Before moving tests, you must:

1. **Make tested functions public** - Functions that tests call directly must be `pub`
2. **Or create public test helpers** - Wrapper functions that expose internal behavior
3. **Or use `#[doc(hidden)]`** - For functions that should be public for testing but not part of the documented API

### Example Refactoring

**Before (inline test accessing private):**
```rust
// src/parser/mod.rs
fn internal_parse_statement(s: &str) -> Statement { ... }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_parse() {
        let result = internal_parse_statement("SELECT 1");  // Works!
        assert!(result.is_valid());
    }
}
```

**After (tests in `tests/` directory):**
```rust
// src/parser/mod.rs
#[doc(hidden)]
pub fn internal_parse_statement(s: &str) -> Statement { ... }
// OR: Make it truly public if it should be part of the API
```

```rust
// tests/parser_test.rs
use sql_splitter::parser::internal_parse_statement;

#[test]
fn test_internal_parse() {
    let result = internal_parse_statement("SELECT 1");
    assert!(result.is_valid());
}
```

## Implementation Plan

### Phase 0: API Preparation

For each module, audit which private functions are tested and decide:
- Should it become part of the public API? → Make it `pub`
- Should it stay internal but testable? → Make it `#[doc(hidden)] pub`
- Should the test change to use public API instead? → Refactor test

### Phase 1: High-Impact Modules

1. **Parser module**
   - Move `src/parser/edge_case_tests.rs` → `tests/parser_edge_cases_test.rs`
   - Extract inline tests → `tests/parser_test.rs`
   - ~112 tests total

2. **Convert module**
   - Extract inline tests → `tests/convert_test.rs`
   - ~33 tests total

### Phase 2: Supporting Modules

3. **Sample module** → `tests/sample_test.rs` (~13 tests)
4. **Splitter module** → `tests/splitter_test.rs` (~7 tests)
5. **Shard module** → `tests/shard_test.rs` (~6 tests)

### Phase 3: Remaining Modules

6. **Schema module** → `tests/schema_test.rs` (~3 tests)
7. **Merger module** → `tests/merger_test.rs` (~3 tests)
8. **Analyzer module** → `tests/analyzer_test.rs` (~2 tests)
9. **Writer module** → `tests/writer_test.rs` (~2 tests)
10. **Cmd module** → `tests/cmd_test.rs` (~4 tests)

## Test File Template

```rust
// tests/parser_test.rs

use sql_splitter::parser::{parse_sql, SqlDialect, Statement};
use sql_splitter::parser::internal_parse_statement;  // #[doc(hidden)]

mod basic_parsing {
    use super::*;

    #[test]
    fn test_simple_select() {
        let result = parse_sql("SELECT 1;", SqlDialect::MySQL);
        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_statement() {
        let stmts = parse_sql("SELECT 1; SELECT 2;", SqlDialect::MySQL).unwrap();
        assert_eq!(stmts.len(), 2);
    }
}

mod mysql_specific {
    use super::*;

    #[test]
    fn test_backtick_identifiers() {
        let result = parse_sql("SELECT `column` FROM `table`;", SqlDialect::MySQL);
        assert!(result.is_ok());
    }
}

mod postgres_specific {
    use super::*;

    #[test]
    fn test_dollar_quoting() {
        let result = parse_sql("SELECT $$text$$;", SqlDialect::PostgreSQL);
        assert!(result.is_ok());
    }
}
```

## Benefits

1. **Complete separation** - Production code contains zero test code
2. **Cleaner source files** - `src/` only contains business logic
3. **Single test location** - All tests discoverable in one directory
4. **Faster compilation** - Source files compile faster without test code
5. **API-focused testing** - Encourages testing through public interfaces
6. **Consistent organization** - Matches common Rust project patterns

## Tradeoffs

| Aspect | Inline Tests | `tests/` Directory |
|--------|--------------|-------------------|
| Access to private members | ✅ Full access | ❌ Public API only |
| Proximity to code | ✅ Same file | ❌ Separate directory |
| Compile time (test build) | Same | ✅ Slightly faster |
| Compile time (release) | Same | Same |
| Encourages public API testing | ❌ | ✅ |
| Test discoverability | ❌ Scattered | ✅ Centralized |

## Migration Checklist

For each module:

- [ ] Identify all tested private functions
- [ ] Decide visibility for each: `pub`, `#[doc(hidden)] pub`, or refactor test
- [ ] Update function visibility in source file
- [ ] Create new test file in `tests/` directory
- [ ] Move test code, updating imports to use crate public API
- [ ] Remove `#[cfg(test)] mod tests` block from source file
- [ ] Run `cargo test` to verify all tests pass
- [ ] Verify test count matches before/after

## Success Metrics

- All 211+ tests pass after extraction
- No `#[cfg(test)]` blocks remain in `src/` (except for test-only dependencies)
- All source files reduced to pure business logic
- Test organization matches proposed structure
