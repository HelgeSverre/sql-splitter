# sql-splitter Roadmap

**Version**: 1.8.0 (current)  
**Last Updated**: 2025-12-21  
**Revision**: 2.5 — Post v1.8.0 with Validate command

This roadmap outlines the feature development plan with dependency-aware ordering and version milestones.

---

## Priority Summary

**High Priority (v1.x):**
1. ✅ Test Data Generator — Enables CI testing for all features (v1.4.0)
2. ✅ Merge — Completes split/merge roundtrip (v1.4.0)
3. ✅ Sample — FK-aware data sampling (builds shared infra) (v1.5.0)
4. ✅ Shard — Tenant extraction (reuses Sample infra) (v1.6.0)
5. ✅ Convert — Dialect conversion (v1.7.0)
6. ✅ Validate — Dump integrity checking (v1.8.0)

**Next (v1.9+):**
- v1.9.0: Diff — Schema + data comparison
- v1.10.0: Query — SQL-like row filtering
- v1.11.0: Redact — Data anonymization
- v1.12.0: Detect-PII — Auto-suggest redaction config
- v1.13.0: MSSQL — Fourth dialect support

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

### v1.7.0 — Convert Command ✅ RELEASED
**Released**: 2025-12-21  
**Theme**: Full dialect conversion for all 6 pairs with COPY→INSERT support

| Feature | Status | Notes |
|---------|--------|-------|
| **Convert core** | ✅ Done | |
| ├─ Converter architecture | ✅ Done | Streaming, per-statement |
| ├─ Identifier quoting | ✅ Done | Backticks ↔ double quotes |
| ├─ String escaping | ✅ Done | `\'` ↔ `''` |
| ├─ Complete type mapping | ✅ Done | 30+ type mappings |
| ├─ AUTO_INCREMENT ↔ SERIAL | ✅ Done | Bidirectional |
| ├─ Session headers | ✅ Done | Strip MySQL/PostgreSQL/SQLite |
| └─ Warning system | ✅ Done | Unsupported features |
| **PostgreSQL handling** | ✅ Done | |
| ├─ COPY → INSERT conversion | ✅ Done | Tab-separated, NULL handling, escape sequences |
| ├─ ::type cast stripping | ✅ Done | ::regclass, ::text, ::character varying |
| ├─ nextval() removal | ✅ Done | Replaced by AUTO_INCREMENT |
| ├─ DEFAULT now() → CURRENT_TIMESTAMP | ✅ Done | |
| ├─ Schema prefix stripping | ✅ Done | public., pg_catalog., pg_temp. |
| ├─ PostgreSQL-only feature filtering | ✅ Done | CREATE DOMAIN/TYPE/FUNCTION/SEQUENCE, triggers |
| └─ TIMESTAMP WITH TIME ZONE | ✅ Done | → DATETIME |
| **All 6 conversion pairs** | ✅ Done | |
| ├─ MySQL → PostgreSQL | ✅ Done | Full type mapping |
| ├─ MySQL → SQLite | ✅ Done | Full type mapping |
| ├─ PostgreSQL → MySQL | ✅ Done | COPY→INSERT, SERIAL→AUTO_INCREMENT |
| ├─ PostgreSQL → SQLite | ✅ Done | COPY→INSERT, full type mapping |
| ├─ SQLite → MySQL | ✅ Done | REAL→DOUBLE |
| └─ SQLite → PostgreSQL | ✅ Done | BLOB→BYTEA, REAL→DOUBLE PRECISION |
| **Testing** | ✅ Done | 268 tests, real-world verification |

**Delivered:**
- All 6 conversion pairs (MySQL ↔ PostgreSQL ↔ SQLite)
- **COPY → INSERT conversion** with batched inserts (100 rows/INSERT)
- NULL marker handling (`\N` → NULL)
- Escape sequence handling (`\t`, `\n`, `\\`, octal)
- PostgreSQL type cast stripping (::regclass, ::text, etc.)
- Schema prefix removal (public.table → table)
- DEFAULT now() → DEFAULT CURRENT_TIMESTAMP
- nextval() sequence removal (AUTO_INCREMENT handles it)
- PostgreSQL-only feature filtering with warnings (CREATE DOMAIN/TYPE/FUNCTION/SEQUENCE)
- TIMESTAMP WITH TIME ZONE → DATETIME
- Block comment handling at statement start
- Auto-detect source dialect
- Bidirectional type mapping (30+ types)
- Session command stripping for all dialects
- Warnings for unsupported features (ENUM, SET, arrays, INHERITS)
- Real-world verification script (`scripts/verify-realworld.sh`)
- Comprehensive benchmarks (`benches/convert_bench.rs`)

**Remaining low-priority gaps** (rare in practice):
- Array types (warning issued, no conversion)
- EXCLUDE constraints
- Partial indexes (`WHERE` clause in indexes)
- Expression indexes
- INTERVAL types

---

### v1.8.0 — Validate Command ✅ RELEASED
**Released**: 2025-12-21  
**Theme**: Dump integrity checking

| Feature | Status | Notes |
|---------|--------|-------|
| **Validate core** | ✅ Done | |
| ├─ CLI + options | ✅ Done | --strict, --json, --no-fk-checks |
| ├─ SQL syntax validation | ✅ Done | Parser error detection |
| ├─ DDL/DML consistency | ✅ Done | INSERT references existing tables |
| ├─ Encoding validation | ✅ Done | UTF-8 checks with warnings |
| ├─ Duplicate PK detection | ✅ Done | All dialects, with max-rows guard |
| ├─ FK referential integrity | ✅ Done | All dialects, first-5 violations |
| └─ Output formats | ✅ Done | Text + JSON |
| **Multi-dialect support** | ✅ Done | |
| ├─ MySQL INSERT parsing | ✅ Done | |
| ├─ PostgreSQL COPY parsing | ✅ Done | COPY FROM stdin support |
| └─ SQLite INSERT parsing | ✅ Done | Reuses MySQL parser |
| **Testing** | ✅ Done | 38 integration tests |

**Delivered:**
- `sql-splitter validate dump.sql`
- `--strict` flag to fail on warnings
- `--json` flag for CI integration
- `--max-rows-per-table` memory guard (default: 1M rows)
- `--no-fk-checks` to disable heavy data checks
- All 5 validation checks for all 3 dialects
- Compressed file support

**Limitations (documented):**
- FK checks assume parent-before-child insertion order
- Parent-orphan detection deferred to future release

---

## Upcoming Features (v1.9+)

### v1.9.0 — Diff Command
**Theme**: Schema + data comparison

| Feature | Effort | Notes |
|---------|--------|-------|
| Diff | ~40h | Compare two SQL dumps |

**Features:**
- Schema diff (table structure, indexes, constraints)
- Row count comparison
- Row-level diff for tables < 100K rows
- Chunked hashing for large tables

---

### v1.10.0 — Query Command
**Theme**: SQL-like row filtering

| Feature | Effort | Notes |
|---------|--------|-------|
| Query | ~30h | WHERE clause filtering |

**Features:**
- Basic WHERE: `=`, `!=`, `<`, `>`, `AND`, `OR`, `IS NULL`, `IN`
- Table selection
- Output formats: SQL, CSV, JSON

---

### v1.11.0 — Redact Command
**Theme**: Data anonymization

| Feature | Effort | Notes |
|---------|--------|-------|
| Redact | ~40h | Column-based anonymization |

**Strategies:**
- null, constant, hash, mask, shuffle
- Fake data generation (names, emails, etc.)
- Glob pattern matching for column selection

---

### v1.12.0 — Detect-PII Command
**Theme**: Auto-suggest redaction config

| Feature | Effort | Notes |
|---------|--------|-------|
| Detect-PII | ~8h | Scan schema and data |

**Detection:**
- Column name patterns (email, phone, ssn, etc.)
- Data patterns (regex matching)
- Statistical uniqueness

---

### v1.13.0 — MSSQL Support
**Theme**: Fourth dialect

| Feature | Effort | Notes |
|---------|--------|-------|
| MSSQL dialect | ~24h | SQL Server support |

**Features:**
- Parse MSSQL dumps
- Convert to/from MySQL, PostgreSQL, SQLite
- Handle MSSQL-specific syntax

---

## Feature Dependency Matrix

| Feature/Module | Depends On | Unlocks |
|----------------|------------|---------|
| **Test Data Gen** | (none) | All integration tests |
| **Merge** | Split | — |
| **Schema Graph v1** | (built in Sample) | Sample, Shard, Validate, Diff |
| **Row Parsing v1** | (built in Sample) | Sample, Shard, Query, Redact, Convert |
| **Sample (basic)** | — | — |
| **Sample --preserve** | Schema Graph v1, Row v1 | Shard |
| **Shard** | Schema Graph v1.5, Row v1.5 | — |
| **Convert** | Row Parsing v1.5 | MSSQL |
| **Validate** | Schema Graph, Row Parsing | — |
| **Diff** | Schema Graph, Row Parsing | — |
| **Query** | Row Parsing | — |
| **Redact** | Row Parsing | Detect-PII |
| **Detect-PII** | Redact | — |
| **MSSQL** | Convert | — |

---

## Effort Summary

### Priority Features (v1.4–v1.8)

| Version | Theme | Status |
|---------|-------|--------|
| v1.4.0 | Test Data Gen + Merge | ✅ Released |
| v1.5.0 | Sample + Infra v1 | ✅ Released |
| v1.6.0 | Shard + Infra v1.5 | ✅ Released |
| v1.7.0 | Convert MVP | ✅ Released |
| v1.8.0 | Validate | ✅ Released |

### Upcoming Features (v1.9+)

| Version | Features | Effort | Duration |
|---------|----------|--------|----------|
| v1.9.0 | Diff | ~40h | 2-3 weeks |
| v1.10.0 | Query | ~30h | 2 weeks |
| v1.11.0 | Redact | ~40h | 2-3 weeks |
| v1.12.0 | Detect-PII | ~8h | 1 week |
| v1.13.0 | MSSQL | ~24h | 2 weeks |

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

4. ✅ **v1.7.0 — Convert MVP** — Released
   - Practical cross-dialect conversion
   - MySQL → PostgreSQL, MySQL → SQLite

5. ✅ **v1.8.0 — Validate** — Released
   - SQL dump integrity checking
   - DDL/DML consistency, PK/FK validation
   - MySQL-focused with dialect info for others

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

### Active

- [Test Data Generator Design](TEST_DATA_GENERATOR.md)
- [Additional Ideas](features/ADDITIONAL_IDEAS.md)
- [Competitive Analysis](COMPETITIVE_ANALYSIS.md)

### Upcoming Feature Designs

- [Diff Feature](features/DIFF_FEATURE.md) — v1.9.0
- [Query Feature](features/QUERY_FEATURE.md) — v1.10.0
- [Redact Feature](features/REDACT_FEATURE.md) — v1.11.0
- [MSSQL Feasibility](features/MSSQL_FEASIBILITY.md) — v1.13.0

### Archived (Implemented)

Historical documents for completed features in `docs/archived/`:
- SAMPLE_FEATURE.md — v1.5.0
- SHARD_FEATURE.md — v1.6.0
- MERGE_FEATURE.md — v1.4.0
- CONVERT_GAP_ANALYSIS.md — v1.7.0 post-implementation
- CONVERT_FEASIBILITY.md — Pre-implementation analysis
- ROADMAP_REVIEW.md — Pre-implementation recommendations
- TEST_FILE_EXTRACTION.md — Test reorganization
