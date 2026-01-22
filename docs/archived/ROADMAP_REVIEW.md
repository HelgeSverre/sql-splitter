# Roadmap Review & Recommendations

**Date**: 2025-12-20  
**Source**: Oracle analysis of roadmap and feature documents

## Overall Assessment

**Verdict**: Roadmap and feature designs are **solid and coherent**, but with some adjustments needed.

| Aspect              | Status       | Notes                                        |
| ------------------- | ------------ | -------------------------------------------- |
| Dependency ordering | ✅ Good      | Minor matrix corrections needed              |
| Effort estimates    | ⚠️ Low       | Feature docs show 30-50% higher than roadmap |
| Scope               | ⚠️ Ambitious | Some features need MVP staging               |
| Technical design    | ✅ Sound     | FK-chain algorithms are correct              |
| Architecture        | ✅ Coherent  | Shared infra approach is right               |
| Testing strategy    | ✅ Good      | Minor additions recommended                  |

---

## Critical Fixes

### 1. Update Dependency Matrix

Current matrix is missing dependencies:

```diff
| Feature | Depends On | Unlocks |
|---------|------------|---------|
| Merge | (none) | — |
| Schema Graph | (none) | Sample, Shard, Validate, Diff |
| Row Parsing | (none) | Sample, Shard, Query, Redact |
- | Diff | Schema Graph | — |
+ | Diff | Schema Graph, Row Parsing | — |
- | Validate | Schema Graph (optional) | — |
+ | Validate | Schema Graph, Row Parsing | — |
- | Convert | Dialect Layer | — |
+ | Convert | Dialect Layer, Row Parsing | — |
```

### 2. Align Effort Estimates

Feature docs show higher effort than roadmap summary:

| Feature       | Roadmap | Feature Doc | Recommendation |
| ------------- | ------- | ----------- | -------------- |
| Test Data Gen | 8-12h   | 27h         | Use 20-25h     |
| Sample        | 24h     | 43h         | Use 35-40h     |
| Shard         | 30h     | 48h         | Use 40-45h     |
| Query         | 24h     | 35h         | Use 30-35h     |
| Redact        | 32h     | 45h         | Use 40h        |
| Diff          | 32h     | 45h         | Use 40h        |
| Convert       | 40h     | 56h         | Use 50h        |

**Total**: ~275h → **~350h** (more realistic)

### 3. Define MVP Scopes

For ambitious features, split into MVP + Full:

**Convert MVP (v2.0)**:

- MySQL ↔ PostgreSQL schema + data
- MySQL ↔ SQLite
- Skip: triggers, procedures, FULLTEXT, SPATIAL, advanced types
- Skip: PostgreSQL arrays, partial indexes

**Convert Full (v2.x)**:

- All type mappings
- ENUM/SET handling
- Complete constraint conversion

**Diff MVP (v2.0)**:

- Schema diff only
- Row counts per table
- Row-level diff for tables < 100K rows

**Diff Full (v2.x)**:

- Chunked hashing for large tables
- Memory-efficient streaming diff

**Redact MVP (v1.8)**:

- Strategies: null, constant, hash, skip
- Glob pattern matching

**Redact Full (v1.x)**:

- Fake data generation
- Locale support
- Mask/shuffle strategies

**Query MVP (v1.8)**:

- WHERE: `=`, `!=`, `<`, `>`, `AND`, `OR`, `IS NULL`, `IN`
- SQL output only

**Query Full (v1.x)**:

- LIKE, BETWEEN, NOT, nested expressions
- CSV/JSON output

---

## Technical Recommendations

### FK Chain Resolution

Current algorithm is correct. Add these safeguards:

1. **Selection explosion control**

   ```rust
   // Fail if sample grows too large
   --max-total-rows 1000000
   --max-expansion-factor 10  // 10× requested sample
   ```

2. **Cycle handling rule (simplest)**
   - For SCCs: if any row selected, include full table
   - Document and warn when cycles detected

3. **Track only needed PKs**
   - Only allocate PK sets for tables that are FK targets
   - Leaf tables don't need PK tracking

### Self-Referential Tables

Current ancestor closure is correct. Add:

- Complexity note: "O(depth × table_size) for multi-pass"
- Optional max closure depth with warning

### Junction Table Heuristics

Make detection more forgiving:

```rust
fn is_junction_table(table: &TableSchema) -> bool {
    // Table with ≥2 FKs to different parents
    // AND no tenant column
    // AND only metadata columns besides FKs (created_at, updated_at, flags)
    // OR explicitly marked in config
}
```

### COPY Parsing

Be explicit about limitations:

- v2.0: Text COPY only, binary COPY → hard error
- Handle escaped newlines in fields (don't naïvely split on `\n`)
- Document: "COPY→INSERT may produce very large INSERT statements"

---

## Version Grouping Recommendations

Consider splitting heavy versions:

**Option A (Current)**:

- v1.8: Query + Redact (56h → likely 80h)
- v1.9: Validate (16h)

**Option B (Recommended)**:

- v1.8: Query MVP (25h)
- v1.9: Redact MVP (30h)
- v1.10: Validate (16h)

**For v2.0**:

- v2.0: Diff (schema + limited data)
- v2.1: Convert MVP
- v2.2: MSSQL
- v2.3: Detect-PII (optional)

---

## Testing Recommendations

1. **Clarify generator purpose**
   - test_data_gen exercises **logical patterns**, not dump quirks
   - Use static fixtures for vendor-specific syntax

2. **Add golden output tests**
   - `input.sql` → `sql-splitter <cmd>` → compare to `expected.sql`
   - Guards against parser/writer regressions

3. **Property tests for FK integrity**
   - "No orphaned FKs after sample/shard with `--strict-fk`"
   - "Redact never leaves original PII for configured columns"

4. **Real-world dump snippets**
   - MySQL conditional comments
   - PostgreSQL COPY with escapes
   - MSSQL GO batches

---

## Architecture Recommendations

### Centralize Parser Early (v1.5)

Create unified `parser` module:

```rust
// src/parser/mod.rs
pub trait StatementIterator { ... }
pub trait InsertRowIterator { ... }
pub trait CopyBlockIterator { ... }
```

Used by: Sample, Shard, Query, Redact, Diff, Convert, Validate

### Unify Schema Types

Single source of truth:

```rust
// src/schema/mod.rs
pub struct TableSchema { ... }
pub struct ColumnDef { ... }
pub struct ForeignKey { ... }
pub struct IndexDef { ... }
```

Used by: Schema Graph, Validate, Diff, Shard

---

## Summary

| Category     | Action                                                      |
| ------------ | ----------------------------------------------------------- |
| Dependencies | Fix matrix to include Row Parsing for Diff/Validate/Convert |
| Estimates    | Inflate by 30-50% or use feature doc numbers                |
| Scope        | Define MVP vs Full for Convert, Diff, Redact, Query         |
| Milestones   | Consider splitting v1.8 (Query+Redact) into two versions    |
| Technical    | Add explosion control, simplify cycle handling              |
| Testing      | Add golden tests, real-world snippets                       |
| Architecture | Centralize parser module early                              |

**No showstoppers found.** The design is sound and the approach is correct.
