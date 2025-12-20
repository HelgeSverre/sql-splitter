# sql-splitter Roadmap

**Version**: 1.3.1 (current)  
**Last Updated**: 2025-12-20  
**Revision**: 2.0 — Reprioritized for core features

This roadmap outlines the feature development plan with dependency-aware ordering and version milestones.

---

## Priority Summary

**High Priority (v1.x):**
1. Test Data Generator — Enables CI testing for all features
2. Merge — Completes split/merge roundtrip
3. Sample — FK-aware data sampling (builds shared infra)
4. Shard — Tenant extraction (reuses Sample infra)
5. Convert — Dialect conversion

**Deferred to v2.x:**
- Query, Redact, Validate, Detect-PII, Diff, MSSQL

---

## Shared Infrastructure

Schema Graph and Row Parsing are built incrementally within Sample/Shard, not as standalone versions:

```
                    ┌─────────────────────────────────────────┐
                    │         SHARED INFRASTRUCTURE           │
                    │    (built incrementally in features)    │
                    ├─────────────────────────────────────────┤
                    │                                         │
                    │  Schema Graph v1 (Sample)               │
                    │  ├─ FK parsing (MySQL)                  │
                    │  ├─ Dependency graph + topo sort        │
                    │  └─ Basic cycle detection               │
                    │           │                             │
                    │           ▼                             │
                    │  Schema Graph v1.5 (Shard)              │
                    │  └─ PostgreSQL FK parsing               │
                    │                                         │
                    │  Row Parsing v1 (Sample)                │
                    │  └─ MySQL INSERT value parsing          │
                    │           │                             │
                    │           ▼                             │
                    │  Row Parsing v1.5 (Shard/Convert)       │
                    │  └─ PostgreSQL COPY parsing             │
                    │                                         │
                    └─────────────────────────────────────────┘
```

---

## Version Milestones

### v1.4.0 — Test Data Generator & CI Foundation
**Target**: 1-2 weeks  
**Theme**: Deterministic fixtures for all integration testing

| Feature | Effort | Status | Notes |
|---------|--------|--------|-------|
| **Test Data Generator** | 20-27h | 🟡 Planned | Synthetic multi-tenant schema |
| ├─ Schema model + types | 4h | | Dialect-agnostic definitions |
| ├─ Generator core + RNG | 3h | | Seed-based reproducibility |
| ├─ Fake data helpers | 2h | | Names, emails, dates |
| ├─ MySQL renderer | 3h | | INSERT statements |
| ├─ PostgreSQL renderer | 4h | | COPY + INSERT |
| ├─ SQLite renderer | 2h | | Double-quote identifiers |
| ├─ CLI binary | 2h | | `gen-fixtures` command |
| └─ Test harness integration | 3h | | `tests/common/` utilities |
| **Static fixtures** | 3h | 🟡 Planned | Edge cases per dialect |

**MVP Scope (v1.4.0):**
- MySQL-only generator
- Single `small` scale (~500 rows)
- Core schema: tenants, users, orders, order_items, one junction, one self-FK
- Enough to test split, merge, sample, shard

**Full Scope (v1.4.x):**
- All 3 dialects
- All 3 scales (small/medium/large)
- Complete 18-table schema from TEST_DATA_GENERATOR.md

**Deliverables:**
- `cargo run -p test_data_gen -- --dialect mysql --scale small --seed 42`
- `tests/fixtures/generated/` with on-demand generation
- Split roundtrip integration tests

---

### v1.5.0 — Merge Command
**Target**: <1 week  
**Theme**: Complete the split/merge roundtrip

| Feature | Effort | Status | Notes |
|---------|--------|--------|-------|
| **Merge command (MVP)** | 6h | 🟡 Planned | Inverse of split |
| ├─ Directory scanning | 1h | | Find .sql files |
| ├─ Streaming concatenation | 2h | | 256KB buffers |
| ├─ `--tables` / `--exclude` | 1h | | Filtering |
| └─ Basic tests | 2h | | Split→merge roundtrip |
| **Merge enhancements** | 4h | 🔵 Optional | |
| ├─ `--order` explicit | 1h | | Manual table order |
| ├─ `--transaction` wrap | 1h | | BEGIN/COMMIT |
| └─ Dialect headers | 2h | | FK checks, encoding |

**MVP Definition:**
- `sql-splitter merge tables/ -o restored.sql`
- Alphabetical ordering
- Split→merge roundtrip produces equivalent output

**Deliverables:**
- `sql-splitter merge tables/ -o restored.sql`
- `sql-splitter merge tables/ --tables users,posts -o partial.sql`
- Integration tests using generator fixtures

---

### v1.6.0 — Sample Command + Shared Infra v1
**Target**: 2-3 weeks  
**Theme**: FK-aware sampling, builds core infrastructure

| Feature | Effort | Status | Notes |
|---------|--------|--------|-------|
| **Schema Graph v1** | 8h | 🟡 Planned | Built for Sample |
| ├─ MySQL FK parsing | 4h | | Inline + ALTER TABLE |
| ├─ Dependency graph | 2h | | Topological sort |
| └─ Cycle detection | 2h | | Conservative SCC handling |
| **Row Parsing v1** | 6h | 🟡 Planned | Built for Sample |
| └─ MySQL INSERT parsing | 6h | | Multi-row, PK/FK extraction |
| **Sample command** | 16h | 🟡 Planned | |
| ├─ CLI + basic modes | 3h | | `--percent`, `--rows` |
| ├─ Reservoir sampling | 2h | | Algorithm R |
| ├─ `--preserve-relations` | 6h | | FK chain resolution |
| ├─ PK tracking | 3h | | AHashSet per table |
| └─ Output generation | 2h | | Compact INSERTs |
| **Testing** | 4h | | Unit + integration |

**Total: ~30h MVP, ~43h Full**

**MVP Definition:**
- `sql-splitter sample dump.sql -o dev.sql --rows 100 --preserve-relations`
- MySQL-only
- No YAML config (CLI flags only)
- Basic table classification (hard-coded patterns)
- No FK orphans on generator fixtures

**Full Scope (v1.6.x):**
- Multi-dialect (PostgreSQL COPY, SQLite)
- YAML config file (`--config sample.yaml`)
- Rich table classification (`--include-global` modes)
- `--dry-run`, progress bar
- Explosion guards (`--max-total-rows`)

**Deliverables:**
- `sql-splitter sample dump.sql -o dev.sql --percent 10`
- `sql-splitter sample dump.sql -o dev.sql --rows 500 --preserve-relations`
- `src/schema/` module (reusable)
- `src/row/` module (reusable)

---

### v1.7.0 — Shard Command + Shared Infra v1.5
**Target**: 2-3 weeks  
**Theme**: Tenant extraction with FK chain resolution

| Feature | Effort | Status | Notes |
|---------|--------|--------|-------|
| **Extend Shared Infra** | 8h | 🟡 Planned | |
| ├─ PostgreSQL FK parsing | 4h | | Extends Schema Graph |
| └─ PostgreSQL COPY parsing | 4h | | Extends Row Parsing |
| **Shard command** | 24h | 🟡 Planned | |
| ├─ CLI + tenant detection | 3h | | Auto-detect company_id |
| ├─ Table classification | 4h | | Root/dependent/junction/global |
| ├─ Internal split to temp | 4h | | Per-table temp files |
| ├─ Tenant selection logic | 6h | | FK-ordered processing |
| ├─ Self-FK closure | 3h | | Ancestor chains |
| └─ Output generation | 4h | | Stats, headers |
| **Testing** | 8h | | Integration + real dumps |

**Total: ~40h MVP, ~48h Full**

**MVP Definition:**
- `sql-splitter shard dump.sql -o tenant_5.sql --tenant-value 5`
- Single tenant extraction
- Auto-detect `tenant_id`/`company_id`
- MySQL-first, best-effort PostgreSQL
- Global lookup tables included by default
- No FK orphans on generator fixtures

**Full Scope (v1.7.x):**
- Multi-tenant (`--tenant-values 1,2,3` → multiple files)
- Hash-based sharding (`--hash --partitions 8`)
- YAML config for classification overrides
- Full PostgreSQL + SQLite support

**Deliverables:**
- `sql-splitter shard dump.sql -o tenant_5.sql --tenant-value 5`
- `sql-splitter shard dump.sql -o shards/ --tenant-values 1,2,3,5`
- FK chain resolution for tables without tenant column

---

### v1.8.0 — Convert Command (MVP)
**Target**: 3-4 weeks  
**Theme**: Dialect conversion for common cases

| Feature | Effort | Status | Notes |
|---------|--------|--------|-------|
| **Convert core** | 20h | 🟡 Planned | |
| ├─ Converter architecture | 3h | | Trait-based per pair |
| ├─ Identifier quoting | 2h | | Backticks ↔ double quotes |
| ├─ String escaping | 2h | | `\'` ↔ `''` |
| ├─ Common type mapping | 6h | | INT, VARCHAR, BOOLEAN, etc. |
| ├─ AUTO_INCREMENT → SERIAL | 2h | | Per-dialect |
| ├─ Session headers | 2h | | Strip/convert |
| └─ Warning system | 3h | | Unsupported features |
| **Conversion pairs (MVP)** | 8h | 🟡 Planned | |
| ├─ MySQL → PostgreSQL | 4h | | INSERT-based |
| └─ MySQL → SQLite | 4h | | Simpler mapping |
| **Testing** | 7h | | Per-pair validation |

**Total: ~35h MVP, ~56h Full**

**MVP Definition:**
- `sql-splitter convert mysql.sql -o postgres.sql --to postgres`
- MySQL → PostgreSQL + MySQL → SQLite
- INSERT-based only (no COPY parsing in MVP)
- Common types only (skip ENUM, SET, UNSIGNED with warning)
- Triggers/procedures: warn and skip

**Full Scope (v2.0.0):**
- All 6 pairs (MySQL ↔ PostgreSQL ↔ SQLite)
- PostgreSQL COPY ↔ INSERT bidirectional
- Complete type mapping (ENUM, arrays, JSONB)
- Full constraint + index conversion
- Roundtrip tests

**Deliverables:**
- `sql-splitter convert mysql.sql -o postgres.sql --to postgres`
- `sql-splitter convert mysql.sql -o sqlite.sql --to sqlite`
- Clear warnings for unsupported features

---

## v2.x — Deferred Features

These features are valuable but lower priority:

### v2.0.0 — Convert Full + Diff
| Feature | Effort | Notes |
|---------|--------|-------|
| Convert Full | 21h | All 6 pairs, COPY handling |
| Diff | 40h | Schema + data comparison |

### v2.1.0 — Query + Redact
| Feature | Effort | Notes |
|---------|--------|-------|
| Query | 30-35h | SQL-like filtering |
| Redact | 40h | Data anonymization |

### v2.2.0 — Validate + Detect-PII
| Feature | Effort | Notes |
|---------|--------|-------|
| Validate | 16h | Dump integrity checking |
| Detect-PII | 8h | Auto-suggest redaction config |

### v2.3.0 — MSSQL Support
| Feature | Effort | Notes |
|---------|--------|-------|
| MSSQL dialect | 24h | Fourth dialect support |

---

## Feature Dependency Matrix

| Feature/Module | Depends On | Unlocks |
|----------------|------------|---------|
| **Test Data Gen** | (none) | All integration tests |
| **Merge** | Split | — |
| **Schema Graph v1** | (built in Sample) | Sample, Shard, future Validate/Diff |
| **Row Parsing v1** | (built in Sample) | Sample, Shard, future Query/Redact/Convert |
| **Sample (basic)** | — | — |
| **Sample --preserve** | Schema Graph v1, Row v1 | Shard |
| **Shard** | Schema Graph v1.5, Row v1.5 | — |
| **Convert MVP** | Row Parsing v1.5 | Convert Full |
| **Query** *(v2.x)* | Row Parsing | — |
| **Redact** *(v2.x)* | Row Parsing | Detect-PII |
| **Validate** *(v2.x)* | Schema Graph, Row Parsing | — |
| **Diff** *(v2.x)* | Schema Graph, Row Parsing | — |
| **MSSQL** *(v2.x)* | Convert | — |

---

## Effort Summary

### Priority Features (v1.4–v1.8)

| Version | Theme | MVP Effort | Full Effort | Duration |
|---------|-------|------------|-------------|----------|
| v1.4.0 | Test Data Gen + CI | ~20h | 27h | 1-2 weeks |
| v1.5.0 | Merge | ~6h | 10h | <1 week |
| v1.6.0 | Sample + Infra v1 | ~30h | 43h | 2-3 weeks |
| v1.7.0 | Shard + Infra v1.5 | ~40h | 48h | 2-3 weeks |
| v1.8.0 | Convert MVP | ~35h | 56h | 3-4 weeks |
| **Total** | | **~131h** | **~184h** | **~10-13 weeks** |

### Deferred Features (v2.x)

| Version | Features | Effort | Duration |
|---------|----------|--------|----------|
| v2.0.0 | Convert Full, Diff | ~61h | 4-5 weeks |
| v2.1.0 | Query, Redact | ~70h | 4-5 weeks |
| v2.2.0 | Validate, Detect-PII | ~24h | 1-2 weeks |
| v2.3.0 | MSSQL | ~24h | 2-3 weeks |

---

## Implementation Order

1. **v1.4.0 — Test Data Generator** ⭐ Start here
   - Enables CI testing for all features
   - Validates multi-tenant patterns on synthetic data

2. **v1.5.0 — Merge** ⭐ Quick win
   - Completes split/merge roundtrip
   - Tests use generator fixtures

3. **v1.6.0 — Sample** ⭐ High value + builds infra
   - Common use case (dev fixtures)
   - Schema Graph + Row Parsing built here

4. **v1.7.0 — Shard** ⭐ Unique differentiator
   - Multi-tenant extraction
   - No other tools do this well
   - Matures shared infrastructure

5. **v1.8.0 — Convert MVP** 
   - Practical cross-dialect conversion
   - Benefits from mature parser types

---

## Test Strategy

### Generator Fixtures

```
tests/
├── fixtures/
│   ├── static/              # Hand-crafted edge cases
│   │   ├── mysql/
│   │   ├── postgres/
│   │   └── sqlite/
│   └── generated/           # .gitignore'd
│       ├── mysql/
│       ├── postgres/
│       └── sqlite/
├── integration/
│   ├── split_merge_test.rs
│   ├── sample_test.rs
│   └── shard_test.rs
└── common/
    └── mod.rs               # Test utilities
```

### Quality Gates

- **Split→Merge roundtrip**: Output is equivalent
- **Sample FK integrity**: No orphaned FKs with `--preserve-relations`
- **Shard FK integrity**: Tenant data is coherent
- **Convert accuracy**: No silent data loss for supported types

---

## Non-Goals (Out of Scope)

- **GUI interface** — CLI only
- **Database connection** — File-based only
- **Binary backup formats** — No .bak (MSSQL)
- **Stored procedure conversion** — Too complex, warn and skip
- **Real-time streaming** — Batch processing only
- **Cloud storage integration** — Use pipes

---

## Related Documents

- [Test Data Generator Design](TEST_DATA_GENERATOR.md)
- [Sample Feature Design](features/SAMPLE_FEATURE.md)
- [Shard Feature Design](features/SHARD_FEATURE.md)
- [Merge Feature Design](features/MERGE_FEATURE.md)
- [Convert Feasibility](features/CONVERT_FEASIBILITY.md)
- [Competitive Analysis](COMPETITIVE_ANALYSIS.md)
- [Roadmap Review](ROADMAP_REVIEW.md)
