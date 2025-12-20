# sql-splitter Roadmap

**Version**: 1.6.0 (current)  
**Last Updated**: 2025-12-20  
**Revision**: 2.2 — Post v1.6.0 release

This roadmap outlines the feature development plan with dependency-aware ordering and version milestones.

---

## Priority Summary

**High Priority (v1.x):**
1. ✅ Test Data Generator — Enables CI testing for all features (v1.4.0)
2. ✅ Merge — Completes split/merge roundtrip (v1.4.0)
3. ✅ Sample — FK-aware data sampling (builds shared infra) (v1.5.0)
4. ✅ Shard — Tenant extraction (reuses Sample infra) (v1.6.0)
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

### v1.4.0 — Test Data Generator & Merge ✅ RELEASED
**Released**: 2025-12-20  
**Theme**: Deterministic fixtures + split/merge roundtrip

| Feature | Status | Notes |
|---------|--------|-------|
| **Test Data Generator** | ✅ Done | `crates/test_data_gen` |
| **Merge command** | ✅ Done | `src/merger/` |

**Delivered:**
- `cargo run -p test_data_gen -- --dialect mysql --scale small --seed 42`
- `sql-splitter merge tables/ -o restored.sql`
- Split→merge roundtrip tests

---

### v1.5.0 — Sample Command + Shared Infra v1
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

**Full Scope (v1.5.x):**
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

### v1.6.0 — Shard Command + Shared Infra v1.5 ✅ RELEASED
**Released**: 2025-12-20  
**Theme**: Tenant extraction with FK chain resolution

| Feature | Status | Notes |
|---------|--------|-------|
| **Extend Shared Infra** | ✅ Done | |
| ├─ PostgreSQL FK parsing | ✅ Done | Extends Schema Graph |
| └─ PostgreSQL COPY parsing | ✅ Done | Extends Row Parsing |
| **Shard command** | ✅ Done | |
| ├─ CLI + tenant detection | ✅ Done | Auto-detect company_id |
| ├─ Table classification | ✅ Done | Root/dependent/junction/global |
| ├─ Internal split to temp | ✅ Done | Per-table temp files |
| ├─ Tenant selection logic | ✅ Done | FK-ordered processing |
| ├─ Self-FK closure | 🟡 Planned | Ancestor chains (v1.6.x) |
| └─ Output generation | ✅ Done | Stats, headers |
| **Testing** | ✅ Done | Unit tests |

**Delivered:**
- `sql-splitter shard dump.sql -o tenant_5.sql --tenant-value 5`
- Auto-detect tenant columns (company_id, tenant_id, etc.)
- Table classification: tenant-root, dependent, junction, lookup, system
- FK chain resolution for dependent tables
- YAML config for table classification overrides
- Supports MySQL, PostgreSQL, and SQLite dialects

**Future (v1.6.x):**
- Multi-tenant (`--tenant-values 1,2,3` → multiple files)
- Hash-based sharding (`--hash --partitions 8`)
- Self-FK closure for hierarchical tables

---

### v1.7.0 — Convert Command (MVP)
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

### Priority Features (v1.4–v1.7)

| Version | Theme | MVP Effort | Full Effort | Duration |
|---------|-------|------------|-------------|----------|
| v1.4.0 | Test Data Gen + Merge | — | — | ✅ Released |
| v1.5.0 | Sample + Infra v1 | — | — | ✅ Released |
| v1.6.0 | Shard + Infra v1.5 | — | — | ✅ Released |
| v1.7.0 | Convert MVP | ~35h | 56h | 3-4 weeks |
| **Total** | | **~35h** | **~56h** | **~3-4 weeks** |

### Deferred Features (v2.x)

| Version | Features | Effort | Duration |
|---------|----------|--------|----------|
| v2.0.0 | Convert Full, Diff | ~61h | 4-5 weeks |
| v2.1.0 | Query, Redact | ~70h | 4-5 weeks |
| v2.2.0 | Validate, Detect-PII | ~24h | 1-2 weeks |
| v2.3.0 | MSSQL | ~24h | 2-3 weeks |

---

## Implementation Order

1. ✅ **v1.4.0 — Test Data Generator + Merge** — Released
   - Enables CI testing for all features
   - Completes split/merge roundtrip

2. ✅ **v1.5.0 — Sample** — Released
   - Common use case (dev fixtures)
   - Schema Graph + Row Parsing built here

3. ✅ **v1.6.0 — Shard** — Released
   - Multi-tenant extraction
   - No other tools do this well
   - Matures shared infrastructure

4. **v1.7.0 — Convert MVP** ⭐ Next up
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
