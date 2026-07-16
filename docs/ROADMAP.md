# sql-splitter Roadmap

**Version**: 1.15.0 (current)
**Last Updated**: 2026-07-15
**Revision**: 3.9 — v1.16.0 zip input implemented (all input commands, single `.sql` member policy)

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
7. ✅ Diff — Schema + data comparison (v1.9.0)
8. ✅ Redact — Data anonymization (v1.10.0)
9. ✅ Graph — ERD generation and FK visualization (v1.11.0)
10. ✅ Query — SQL analytics with DuckDB (v1.12.0)
11. ✅ MSSQL — Fourth dialect support (SQL Server) (v1.12.x)

**Maintenance (v1.13.x):**

- v1.13.0–v1.13.7: Benchmark expansion, JSON schema generation, OG images, dependency bumps, CI fixes, library feature flags, convert bug fixes (no new commands)

**Shipped (v1.14.0):**

- ✅ `-o -` stdout support across streaming commands, redact compressed-input fix, MySQL→SQLite sized auto-increment fix, docs accuracy overhaul

**Shipped (v1.15.0):**

- ✅ `split --compress gzip|zstd|bzip2|xz`, single-file archive output (tar.*/zip), 2.4–4.4× split speedup (parallel pipelined writers, allocation-lean parsing), 12 parser bug fixes

**Shipped (v1.16.0):**

- ✅ `.zip` dump input (single `.sql` member, all input commands); `--io-strategy` auto-tuning for HDDs/slow media

**Next:**

- v1.17.0: Enum Conversion — Proper PG↔MySQL enum type conversion
- v1.18.0: Migrate — Schema migration generation
- v1.19.0: DBML — Import/export DBML schema definitions

**Future (v2.x):**

- v2.0.0: Parallel — Multi-threaded performance
- v2.1.0: Infer — Schema inference from data

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

| Feature                 | Status  | Notes                  |
| ----------------------- | ------- | ---------------------- |
| **Test Data Generator** | ✅ Done | `crates/test_data_gen` |
| **Merge command**       | ✅ Done | `src/merger/`          |

**Delivered:**

- `cargo run -p test_data_gen -- --dialect mysql --scale small --seed 42`
- `sql-splitter merge tables/ -o restored.sql`
- Split→merge roundtrip tests

---

### v1.5.0 — Sample Command + Shared Infra v1

**Target**: 2-3 weeks  
**Theme**: FK-aware sampling, builds core infrastructure

| Feature                   | Effort | Status     | Notes                       |
| ------------------------- | ------ | ---------- | --------------------------- |
| **Schema Graph v1**       | 8h     | 🟡 Planned | Built for Sample            |
| ├─ MySQL FK parsing       | 4h     |            | Inline + ALTER TABLE        |
| ├─ Dependency graph       | 2h     |            | Topological sort            |
| └─ Cycle detection        | 2h     |            | Conservative SCC handling   |
| **Row Parsing v1**        | 6h     | 🟡 Planned | Built for Sample            |
| └─ MySQL INSERT parsing   | 6h     |            | Multi-row, PK/FK extraction |
| **Sample command**        | 16h    | 🟡 Planned |                             |
| ├─ CLI + basic modes      | 3h     |            | `--percent`, `--rows`       |
| ├─ Reservoir sampling     | 2h     |            | Algorithm R                 |
| ├─ `--preserve-relations` | 6h     |            | FK chain resolution         |
| ├─ PK tracking            | 3h     |            | AHashSet per table          |
| └─ Output generation      | 2h     |            | Compact INSERTs             |
| **Testing**               | 4h     |            | Unit + integration          |

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

| Feature                    | Status     | Notes                          |
| -------------------------- | ---------- | ------------------------------ |
| **Extend Shared Infra**    | ✅ Done    |                                |
| ├─ PostgreSQL FK parsing   | ✅ Done    | Extends Schema Graph           |
| └─ PostgreSQL COPY parsing | ✅ Done    | Extends Row Parsing            |
| **Shard command**          | ✅ Done    |                                |
| ├─ CLI + tenant detection  | ✅ Done    | Auto-detect company_id         |
| ├─ Table classification    | ✅ Done    | Root/dependent/junction/global |
| ├─ Internal split to temp  | ✅ Done    | Per-table temp files           |
| ├─ Tenant selection logic  | ✅ Done    | FK-ordered processing          |
| ├─ Self-FK closure         | 🟡 Planned | Ancestor chains (v1.6.x)       |
| └─ Output generation       | ✅ Done    | Stats, headers                 |
| **Testing**                | ✅ Done    | Unit tests                     |

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

| Feature                              | Status  | Notes                                          |
| ------------------------------------ | ------- | ---------------------------------------------- |
| **Convert core**                     | ✅ Done |                                                |
| ├─ Converter architecture            | ✅ Done | Streaming, per-statement                       |
| ├─ Identifier quoting                | ✅ Done | Backticks ↔ double quotes                      |
| ├─ String escaping                   | ✅ Done | `\'` ↔ `''`                                    |
| ├─ Complete type mapping             | ✅ Done | 30+ type mappings                              |
| ├─ AUTO_INCREMENT ↔ SERIAL           | ✅ Done | Bidirectional                                  |
| ├─ Session headers                   | ✅ Done | Strip MySQL/PostgreSQL/SQLite                  |
| └─ Warning system                    | ✅ Done | Unsupported features                           |
| **PostgreSQL handling**              | ✅ Done |                                                |
| ├─ COPY → INSERT conversion          | ✅ Done | Tab-separated, NULL handling, escape sequences |
| ├─ ::type cast stripping             | ✅ Done | ::regclass, ::text, ::character varying        |
| ├─ nextval() removal                 | ✅ Done | Replaced by AUTO_INCREMENT                     |
| ├─ DEFAULT now() → CURRENT_TIMESTAMP | ✅ Done |                                                |
| ├─ Schema prefix stripping           | ✅ Done | public., pg_catalog., pg_temp.                 |
| ├─ PostgreSQL-only feature filtering | ✅ Done | CREATE DOMAIN/TYPE/FUNCTION/SEQUENCE, triggers |
| └─ TIMESTAMP WITH TIME ZONE          | ✅ Done | → DATETIME                                     |
| **All 6 conversion pairs**           | ✅ Done |                                                |
| ├─ MySQL → PostgreSQL                | ✅ Done | Full type mapping                              |
| ├─ MySQL → SQLite                    | ✅ Done | Full type mapping                              |
| ├─ PostgreSQL → MySQL                | ✅ Done | COPY→INSERT, SERIAL→AUTO_INCREMENT             |
| ├─ PostgreSQL → SQLite               | ✅ Done | COPY→INSERT, full type mapping                 |
| ├─ SQLite → MySQL                    | ✅ Done | REAL→DOUBLE                                    |
| └─ SQLite → PostgreSQL               | ✅ Done | BLOB→BYTEA, REAL→DOUBLE PRECISION              |
| **Testing**                          | ✅ Done | 268 tests, real-world verification             |

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

| Feature                     | Status  | Notes                             |
| --------------------------- | ------- | --------------------------------- |
| **Validate core**           | ✅ Done |                                   |
| ├─ CLI + options            | ✅ Done | --strict, --json, --no-fk-checks  |
| ├─ SQL syntax validation    | ✅ Done | Parser error detection            |
| ├─ DDL/DML consistency      | ✅ Done | INSERT references existing tables |
| ├─ Encoding validation      | ✅ Done | UTF-8 checks with warnings        |
| ├─ Duplicate PK detection   | ✅ Done | All dialects, with max-rows guard |
| ├─ FK referential integrity | ✅ Done | All dialects, first-5 violations  |
| └─ Output formats           | ✅ Done | Text + JSON                       |
| **Multi-dialect support**   | ✅ Done |                                   |
| ├─ MySQL INSERT parsing     | ✅ Done |                                   |
| ├─ PostgreSQL COPY parsing  | ✅ Done | COPY FROM stdin support           |
| └─ SQLite INSERT parsing    | ✅ Done | Reuses MySQL parser               |
| **Testing**                 | ✅ Done | 38 integration tests              |

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

### v1.8.1 — Glob Patterns & Agent Skills ✅ RELEASED

**Released**: 2025-12-21  
**Theme**: Batch processing and AI tooling integration

| Feature                  | Status  | Notes                         |
| ------------------------ | ------- | ----------------------------- |
| **Glob pattern support** | ✅ Done | All file-based commands       |
| ├─ validate "\*.sql"     | ✅ Done | Multi-file validation         |
| ├─ analyze "\*_/_.sql"   | ✅ Done | Recursive analysis            |
| ├─ split "\*.sql"        | ✅ Done | Multi-file splitting          |
| └─ convert "\*.sql"      | ✅ Done | Batch conversion              |
| **--fail-fast flag**     | ✅ Done | Stop on first error           |
| **--no-limit flag**      | ✅ Done | Disable row limits            |
| **Multi-dialect PK/FK**  | ✅ Done | Extended to PostgreSQL/SQLite |
| **Agent Skill**          | ✅ Done | agentskills.io spec           |
| **llms.txt**             | ✅ Done | LLM-friendly docs             |

**Delivered:**

- Glob patterns: `sql-splitter validate "dumps/*.sql"`
- `--fail-fast` for CI pipelines
- `--no-limit` to disable memory guards
- PK/FK validation for all 3 dialects
- Agent Skill for 7+ AI coding tools
- llms.txt with installation instructions

---

### v1.8.2 — Sample Memory Optimization ✅ RELEASED

**Released**: 2025-12-21  
**Theme**: Performance optimization and profiling infrastructure

| Feature                        | Status  | Notes                        |
| ------------------------------ | ------- | ---------------------------- |
| **Sample memory optimization** | ✅ Done | 98.5% reduction              |
| ├─ Streaming row processing    | ✅ Done | Temp files instead of memory |
| ├─ PkHashSet (64-bit hashes)   | ✅ Done | Compact PK tracking          |
| └─ Both --percent and --rows   | ✅ Done | All modes optimized          |
| **Profiling infrastructure**   | ✅ Done |                              |
| ├─ profile-memory.sh script    | ✅ Done | Automated profiling          |
| ├─ make profile targets        | ✅ Done | medium, large, mega, giga    |
| └─ Size presets                | ✅ Done | 0.5MB to 10GB                |

**Delivered:**

- 2.9 GB file: 8.2 GB → 114 MB peak RSS
- `make profile` / `make profile-large` / `make profile-mega` / `make profile-giga`
- `scripts/profile-memory.sh` with 8 size presets (tiny to giga)
- Memory profiling documentation in AGENTS.md

---

### v1.9.0 — Diff Command ✅ RELEASED

**Released**: 2025-12-21  
**Theme**: Schema + data comparison

| Feature              | Status  | Notes                      |
| -------------------- | ------- | -------------------------- |
| **Diff command**     | ✅ Done |                            |
| ├─ Schema comparison | ✅ Done | Tables, columns, PKs, FKs  |
| ├─ Data comparison   | ✅ Done | Memory-bounded PK tracking |
| ├─ Output formats    | ✅ Done | text, json, sql            |
| └─ Table filters     | ✅ Done | --tables, --exclude        |

**Delivered:**

- `sql-splitter diff old.sql new.sql`
- Schema diff: tables added/removed, columns, PK/FK changes
- Data diff: rows added/removed/modified per table
- SQL migration output format
- Memory-bounded: 10M PK entries (~160MB max)
- 11 integration tests

---

### v1.10.0 — Redact Command ✅ RELEASED

**Released**: 2025-12-21  
**Theme**: Data anonymization

| Feature                    | Status  | Notes                                                |
| -------------------------- | ------- | ---------------------------------------------------- |
| **Redact command**         | ✅ Done |                                                      |
| ├─ CLI + options           | ✅ Done | --config, --null, --hash, --fake, --mask, --constant |
| ├─ YAML config parsing     | ✅ Done | Rules, defaults, skip_tables                         |
| ├─ Column pattern matching | ✅ Done | Glob patterns (\*.email, users.ssn)                  |
| ├─ 7 redaction strategies  | ✅ Done | null, constant, hash, mask, shuffle, fake, skip      |
| ├─ 25+ fake generators     | ✅ Done | email, name, phone, address, ip, uuid, etc.          |
| ├─ --generate-config       | ✅ Done | Auto-detect PII columns                              |
| ├─ Multi-locale support    | ✅ Done | 8 locales                                            |
| ├─ --seed reproducibility  | ✅ Done | Deterministic fake data                              |
| └─ Streaming architecture  | ✅ Done | ~87MB for 10GB files, ~230 MB/s                      |
| **Documentation**          | ✅ Done |                                                      |
| ├─ Man page                | ✅ Done | sql-splitter-redact.1                                |
| ├─ README                  | ✅ Done |                                                      |
| ├─ llms.txt                | ✅ Done |                                                      |
| └─ SKILL.md                | ✅ Done |                                                      |
| **Testing**                | ✅ Done | verify-realworld.sh integration                      |

**Delivered:**

- `sql-splitter redact dump.sql -o safe.sql --config redact.yaml`
- `sql-splitter redact dump.sql -o safe.sql --null "*.ssn" --hash "*.email" --fake "*.name"`
- `sql-splitter redact dump.sql --generate-config -o redact.yaml`
- All strategies: null, constant, hash, mask, shuffle, fake, skip
- 25+ fake generators with 8 locale support
- Streaming architecture with constant memory usage
- ~230 MB/s throughput on large files

**Note:** Phase 3 (INSERT/COPY rewriting) is stubbed; current implementation passes through statements unchanged. Framework is complete for future implementation.

---

### v1.11.0 — Graph Command ✅ RELEASED

**Released**: 2025-12-26  
**Theme**: ERD generation and FK dependency visualization

| Feature               | Status  | Notes                                     |
| --------------------- | ------- | ----------------------------------------- |
| **Graph command**     | ✅ Done | Full ERD generation                       |
| ├─ ERD-style diagrams | ✅ Done | Tables with columns, types, PK/FK markers |
| ├─ Interactive HTML   | ✅ Done | Dark/light mode, copy Mermaid, panzoom    |
| ├─ DOT format         | ✅ Done | Graphviz ERD-style output                 |
| ├─ Mermaid format     | ✅ Done | erDiagram syntax                          |
| ├─ JSON format        | ✅ Done | Full schema with stats                    |
| ├─ Table filtering    | ✅ Done | --tables, --exclude with glob patterns    |
| ├─ Focus mode         | ✅ Done | --table with --transitive or --reverse    |
| └─ Cycle detection    | ✅ Done | Tarjan's SCC algorithm                    |
| **Order command**     | ✅ Done | Topological FK ordering                   |
| ├─ Safe import order  | ✅ Done | Parents before children                   |
| ├─ --check mode       | ✅ Done | Detect cycles                             |
| └─ --reverse mode     | ✅ Done | For DROP operations                       |

**Delivered:**

- `sql-splitter graph dump.sql -o schema.html` — Interactive HTML ERD
- `sql-splitter graph dump.sql -o schema.dot` — Graphviz DOT ERD
- `sql-splitter graph dump.sql -o schema.mmd --format mermaid` — Mermaid erDiagram
- `sql-splitter graph dump.sql --json` — JSON with full schema details
- `sql-splitter graph dump.sql --cycles-only` — Show circular dependencies
- `sql-splitter graph dump.sql --table orders --transitive` — Focus on dependencies
- `sql-splitter order dump.sql -o ordered.sql` — FK-aware ordering
- `sql-splitter order dump.sql --check` — Cycle detection

**Technical highlights:**

- ERD diagrams show tables with full column details (name, type, PK/FK, nullable)
- HTML viewer with dark/light mode toggle, copy Mermaid button, panzoom
- Handles large schemas (tested with 281 tables, 3104 columns)
- Tarjan's SCC algorithm for cycle detection

---

### v1.12.0 — Query Command ✅ RELEASED

**Released**: 2025-12-26  
**Theme**: SQL analytics with embedded DuckDB

| Feature                 | Status  | Notes                                |
| ----------------------- | ------- | ------------------------------------ |
| **Query command**       | ✅ Done | Full SQL analytics on dump files     |
| ├─ DuckDB integration   | ✅ Done | Bundled, zero dependencies           |
| ├─ Multi-dialect import | ✅ Done | MySQL, PostgreSQL, SQLite            |
| ├─ Interactive REPL     | ✅ Done | .tables, .schema, .describe, .sample |
| ├─ Output formats       | ✅ Done | table, json, jsonl, csv, tsv         |
| ├─ Persistent caching   | ✅ Done | --cache with 400x speedup            |
| ├─ Auto disk mode       | ✅ Done | >2GB dumps use disk storage          |
| ├─ Memory limit         | ✅ Done | --memory-limit flag                  |
| └─ Table filtering      | ✅ Done | --tables flag                        |
| **DuckDB module**       | ✅ Done | Reusable query engine infrastructure |
| ├─ QueryEngine          | ✅ Done | In-memory and disk connections       |
| ├─ DumpLoader           | ✅ Done | Statement parsing and loading        |
| ├─ TypeConverter        | ✅ Done | Cross-dialect type mapping           |
| └─ CacheManager         | ✅ Done | SHA256-based cache keys              |
| **Testing**             | ✅ Done | 119 DuckDB-specific tests            |

**Delivered:**

- `sql-splitter query dump.sql "SELECT COUNT(*) FROM users"` — Single query
- `sql-splitter query dump.sql --interactive` — REPL session
- `sql-splitter query dump.sql "SELECT * FROM orders" -f json -o results.json` — Export
- Full SQL support (JOINs, aggregations, window functions, CTEs)
- Cached queries run 400x faster on repeated access
- Auto-switches to disk mode for dumps >2GB
- Supports compressed input files (.gz, .bz2, .xz, .zst)

**Technical highlights:**

- Zero external dependencies (DuckDB bundled)
- ~15-25 MB binary size increase
- 674 total tests (119 DuckDB-specific)
- Type mapping for all MySQL/PostgreSQL/SQLite types to DuckDB

---

### v1.12.x — MSSQL Support ✅ RELEASED

**Released**: 2025-12-27  
**Theme**: Fourth dialect (SQL Server)

| Feature               | Status  | Notes                                           |
| --------------------- | ------- | ----------------------------------------------- |
| **MSSQL dialect**     | ✅ Done | Full SQL Server support for all commands        |
| ├─ Parser support     | ✅ Done | GO batches, square brackets, IDENTITY           |
| ├─ Schema parsing     | ✅ Done | CLUSTERED/NONCLUSTERED, CONSTRAINT syntax       |
| ├─ Dialect detection  | ✅ Done | Auto-detect from SET ANSI_NULLS, brackets, etc. |
| ├─ Unicode strings    | ✅ Done | N'...' handling                                 |
| └─ DuckDB integration | ✅ Done | IDENTITY stripping, type conversion             |
| **All commands**      | ✅ Done |                                                 |
| ├─ split              | ✅ Done | Splits MSSQL dumps by table                     |
| ├─ merge              | ✅ Done | Merges with MSSQL headers                       |
| ├─ analyze            | ✅ Done | Statistics for MSSQL dumps                      |
| ├─ sample             | ✅ Done | FK-aware sampling                               |
| ├─ shard              | ✅ Done | Tenant extraction with auto-detect              |
| ├─ convert            | ✅ Done | All 12 conversion pairs                         |
| ├─ validate           | ✅ Done | PK/FK validation                                |
| ├─ diff               | ✅ Done | Schema + data comparison                        |
| ├─ redact             | ✅ Done | Data anonymization                              |
| ├─ graph              | ✅ Done | ERD generation                                  |
| ├─ order              | ✅ Done | Topological ordering                            |
| └─ query              | ✅ Done | DuckDB analytics                                |
| **Testing**           | ✅ Done | 29 MSSQL integration tests                      |

**Delivered:**

- Parse MSSQL dumps (SSMS-generated scripts, sqlcmd, Azure Data Studio)
- Convert to/from MySQL, PostgreSQL, SQLite (12 conversion pairs total)
- Handle T-SQL syntax (GO batches, square brackets, IDENTITY)
- Support unicode strings (N'...')
- DuckDB query integration for MSSQL dumps
- Static test fixtures in `tests/fixtures/static/mssql/`

**Out of scope (not planned):**

- bcp file parsing (binary format)
- Native backups (.bak files)
- DACPAC/BACPAC support

---

## Upcoming Features (v1.16+)

---

### v1.16.0 — Zip Input + Adaptive I/O Profiles

**Theme**: Real-world inputs, real-world devices

| Feature            | Effort   | Status  | Notes                                       |
| ------------------ | -------- | ------- | -------------------------------------------- |
| Zip input          | ~6–8h    | ✅ Done | No new deps; `zip` crate already present    |
| Adaptive I/O       | ~2–3 days| ✅ Done | `--io-strategy auto\|ssd\|hdd\|cheap`        |

**Adaptive I/O strategys** — measured 2026-07-15/16: same-spindle split on a
USB HDD runs at 21–33 MB/s with defaults but 54.7 MB/s (2.52×) with
`WRITERS=1` + 64MB buffers; cheap flash wants fewest write *operations*
instead. Design: don't identify the device, respond to it — an fsync probe
picks the opening profile, then a state machine driven by the pipeline's own
backpressure counters (bytes-acked throughput + parser send-stall ratio,
sampled at byte-based epochs) steps between FAST / SLOW_SEEK / SLOW_OPS
profiles with asymmetric hysteresis. Writer count only ever grows (start W=1,
spawn after the device proves fast) so per-table ordering and byte-identical
output are preserved by construction. Full design, implementation phases, and
the deterministic test plan (mock-clock controller tests, throttled-sink
integration tests, cross-profile sha256 golden invariant, real-hardware
acceptance script): [ADAPTIVE_IO_PROFILES.md](features/ADAPTIVE_IO_PROFILES.md)

**Zip input** — shipped. Zip is an archive (multiple members) rather than a
stream compression, and the `zip` crate's reader needs `Read + Seek`, so it
can't just be another `Compression::wrap_reader` decoder. Implementation:

- Two-phase open: parse the central directory seekably (`zip::ZipArchive`,
  which handles zip64 and junk entries like `__MACOSX/` for free) to locate
  the member, then reopen/seek the `File` to `data_start()` and stream the
  tail through `flate2::read::DeflateDecoder` (or a bounded read for stored
  members). Downstream stays an ordinary streaming `Box<dyn Read>` — no
  parser changes. See `src/zip_input.rs`.
- Member policy: exactly one `.sql` member → use it; several → error listing
  them; none → clear error. Encrypted or unsupported-compression members →
  clear error.
- New `Compression::Zip` variant plus `crate::splitter::open_input`/
  `open_input_with_progress` helpers; every `File::open` + `wrap_reader` call
  site across the input commands (`split`, `analyze`, `validate`, `diff`,
  `graph`, `order`, `convert`, `redact`, `query`, `sample`, `shard`) now goes
  through these, including the progress-reader variants. Dialect
  auto-detection (`detect_dialect_from_file`) goes through the same helper
  so it works on zipped dumps too.
- `--compress zip` for per-file *output* stays excluded — archive output
  (`-o dump.zip`) already covers that.
- Feature-gated under the existing `archive` feature; without it, opening a
  `.zip` produces a clear "requires the archive feature" error.

**Deliverables (shipped):**

- `sql-splitter split reflow_latest.sql.zip -o tables/` (and every other
  input command)
- Fixture-zip tests: single member (deflated + stored), junk-entry
  tolerance, multi-member error, no-`.sql`-member error (`tests/zip_input_test.rs`)

---

### v1.18.0 — Migration Generation

**Theme**: Schema evolution tracking

| Feature | Effort | Notes                     |
| ------- | ------ | ------------------------- |
| Migrate | ~40h   | Generate ALTER statements |

**Features:**

- Analyze schema differences
- Generate migration scripts (ALTER TABLE, CREATE INDEX, etc.)
- Multi-dialect migration output
- Rollback script generation
- Breaking change detection

**Deliverables:**

- `sql-splitter migrate old.sql new.sql -o migration.sql`
- `sql-splitter migrate old.sql new.sql --rollback -o rollback.sql`
- `sql-splitter migrate old.sql new.sql --breaking-changes`

---

### v1.19.0 — DBML Import/Export

**Theme**: Schema documentation and interoperability

| Feature                | Effort | Status     | Notes                          |
| ---------------------- | ------ | ---------- | ------------------------------ |
| **DBML Parser**        | 10h    | 🟡 Planned | Recursive descent, full spec   |
| **DBML Export**        | 6h     | 🟡 Planned | Extends `graph` command        |
| ├─ Table/column export | 2h     |            | Full schema details            |
| ├─ Relationship export | 2h     |            | All cardinality types          |
| └─ Index/enum export   | 2h     |            | Including composite            |
| **DBML Import**        | 8h     | 🟡 Planned | Extends `convert` command      |
| ├─ Type mapping        | 3h     |            | All 4 dialects                 |
| ├─ FK generation       | 2h     |            | Inline and standalone          |
| └─ Enum handling       | 3h     |            | Per-dialect strategies         |
| **Testing**            | 5h     | 🟡 Planned | Unit + integration + roundtrip |
| **Documentation**      | 2h     | 🟡 Planned | Man pages, llms.txt            |

**Total: ~35h**

**Use Cases:**

- Export SQL dump schemas to DBML for dbdiagram.io visualization
- Generate SQL DDL from DBML schema-as-code definitions
- Cross-platform schema documentation (human-readable format)

**Deliverables:**

- `sql-splitter graph dump.sql --format dbml -o schema.dbml` — Export
- `sql-splitter convert schema.dbml --to mysql -o schema.sql` — Import
- Support for all 4 dialects (MySQL, PostgreSQL, SQLite, MSSQL)
- Roundtrip testing: SQL → DBML → SQL equivalence

**Design Doc:** [DBML_SUPPORT.md](features/DBML_SUPPORT.md)

---

### v1.17.0 — Enum Type Conversion

**Target**: 2-3 weeks  
**Theme**: Proper bidirectional enum conversion between PostgreSQL and MySQL

| Feature                          | Effort | Status     | Notes                            |
| -------------------------------- | ------ | ---------- | -------------------------------- |
| **Enum Registry**                | 2h     | 🟡 Planned | State tracking across statements |
| **PG → MySQL**                   | 12h    | 🟡 Planned |                                  |
| ├─ Parse CREATE TYPE ... AS ENUM | 3h     |            | Extract type definitions         |
| ├─ Parse ALTER TYPE ADD VALUE    | 2h     |            | Update registry                  |
| ├─ Rewrite CREATE TABLE columns  | 3h     |            | Type ref → inline ENUM           |
| ├─ Strip ::type casts in DML     | 2h     |            | Remove enum casts                |
| └─ Handle unknown types          | 2h     |            | VARCHAR fallback + warning       |
| **MySQL → PG**                   | 10h    | 🟡 Planned |                                  |
| ├─ Parse inline ENUM()           | 2h     |            | Extract from columns             |
| ├─ Generate CREATE TYPE          | 3h     |            | Deterministic naming             |
| ├─ Multi-statement output        | 3h     |            | One input → many outputs         |
| └─ Deduplication (optional)      | 2h     |            | Signature-based reuse            |
| **Testing**                      | 6h     | 🟡 Planned | Unit + integration tests         |

**Total: ~30h**

**Current Behavior (lossy):**

- MySQL → PostgreSQL: `ENUM('a','b')` → `VARCHAR(255)` ❌
- PostgreSQL → MySQL: `CREATE TYPE` skipped, columns become VARCHAR ❌

**New Behavior (semantic-preserving):**

- MySQL → PostgreSQL: `ENUM('a','b')` → `CREATE TYPE enum__table__col AS ENUM ('a','b')` ✅
- PostgreSQL → MySQL: `CREATE TYPE t AS ENUM (...)` → inline `ENUM(...)` per column ✅

**Key Decisions:**

- Naming: `enum__{table}__{column}` (deterministic, collision-safe)
- SQLite: Continue to TEXT (no enum support)
- Unknown types: Fallback to VARCHAR + warning (streaming-compatible)

**Deliverables:**

- Proper enum conversion for PG↔MySQL
- Registry-based state tracking
- `--enum-naming` flag (per-column vs dedupe)
- Comprehensive test coverage

**Design Doc:** [ENUM_CONVERSION.md](features/ENUM_CONVERSION.md)

---

### v2.0.0 — Parallel Processing

**Theme**: Multi-threaded performance

| Feature  | Effort | Notes                  |
| -------- | ------ | ---------------------- |
| Parallel | ~60h   | Multi-core utilization |

**Features:**

- Parallel table splitting
- Parallel conversion
- Parallel validation
- Worker pool architecture
- Configurable thread count

**Performance targets:**

- 4x speedup on 8-core systems
- Linear scaling up to available cores
- Memory-bounded parallel processing

**Deliverables:**

- `sql-splitter split dump.sql -o tables/ --parallel 8`
- `sql-splitter convert dump.sql --parallel 4`
- `sql-splitter validate dump.sql --parallel auto`

---

### v2.1.0 — Schema Inference

**Theme**: Reverse-engineer schemas from data

| Feature | Effort | Notes                     |
| ------- | ------ | ------------------------- |
| Infer   | ~50h   | Generate DDL from INSERTs |

**Features:**

- Type inference from INSERT values
- Primary key detection
- Index suggestion based on data patterns
- Foreign key inference (heuristic)
- NOT NULL constraint detection

**Deliverables:**

- `sql-splitter infer data-only.sql -o schema.sql`
- `sql-splitter infer data.csv --table users --dialect mysql`

---

## Feature Dependency Matrix

| Feature/Module        | Depends On                  | Unlocks                               |
| --------------------- | --------------------------- | ------------------------------------- |
| **Test Data Gen**     | (none)                      | All integration tests                 |
| **Merge**             | Split                       | —                                     |
| **Schema Graph v1**   | (built in Sample)           | Sample, Shard, Validate, Diff         |
| **Row Parsing v1**    | (built in Sample)           | Sample, Shard, Query, Redact, Convert |
| **Sample (basic)**    | —                           | —                                     |
| **Sample --preserve** | Schema Graph v1, Row v1     | Shard                                 |
| **Shard**             | Schema Graph v1.5, Row v1.5 | —                                     |
| **Convert**           | Row Parsing v1.5            | MSSQL, Enum Conversion                |
| **Enum Conversion**   | Convert                     | —                                     |
| **Validate**          | Schema Graph, Row Parsing   | —                                     |
| **Diff**              | Schema Graph, Row Parsing   | —                                     |
| **Query**             | Row Parsing                 | —                                     |
| **Redact**            | Row Parsing                 | Detect-PII                            |
| **Detect-PII**        | Redact                      | —                                     |
| **Graph**             | Schema Graph                | Order, Migrate, DBML                  |
| **Order**             | Schema Graph                | —                                     |
| **DBML**              | Graph, Convert              | —                                     |
| **MSSQL**             | Convert                     | —                                     |
| **Migrate**           | Diff, Schema Graph          | —                                     |
| **Parallel**          | (all commands)              | —                                     |
| **Infer**             | Row Parsing                 | —                                     |

---

## Effort Summary

### Priority Features (v1.4–v1.12)

| Version | Theme                        | Status      |
| ------- | ---------------------------- | ----------- |
| v1.4.0  | Test Data Gen + Merge        | ✅ Released |
| v1.5.0  | Sample + Infra v1            | ✅ Released |
| v1.6.0  | Shard + Infra v1.5           | ✅ Released |
| v1.7.0  | Convert MVP                  | ✅ Released |
| v1.8.0  | Validate                     | ✅ Released |
| v1.8.1  | Glob Patterns + Agent Skills | ✅ Released |
| v1.8.2  | Sample Memory Optimization   | ✅ Released |
| v1.9.0  | Diff                         | ✅ Released |
| v1.9.1  | Diff Enhanced                | ✅ Released |
| v1.9.2  | CLI UX + Man Pages           | ✅ Released |
| v1.10.0 | Redact                       | ✅ Released |
| v1.11.0 | Graph + Order                | ✅ Released |
| v1.12.0 | Query (DuckDB)               | ✅ Released |

### Maintenance (v1.13.x)

| Version | Theme                                                       | Status      |
| ------- | ----------------------------------------------------------- | ----------- |
| v1.12.x | MSSQL                                                       | ✅ Released |
| v1.13.0 | Benchmark expansion, JSON schema gen, OG images             | ✅ Released |
| v1.13.1 | Diff bug fixes (FK formatting, PK truncation)               | ✅ Released |
| v1.13.2 | Dependency bumps + lint fixes                               | ✅ Released |
| v1.13.3 | Release workflow fix (cargo-dist artifact versions)         | ✅ Released |
| v1.13.4 | `rand` 0.10, `fake` 5, dependabot guard for cargo-dist deps | ✅ Released |
| v1.13.5 | `duckdb` 1.10502 (CalVer), `sha2` 0.11, dep bumps           | ✅ Released |
| v1.13.6 | Cargo feature flags for library consumers, Docker images     | ✅ Released |
| v1.13.7 | MySQL→PG convert fixes (COMMENT/AUTO_INCREMENT/UNIQUE KEY)   | ✅ Released |
| v1.14.0 | `-o -` stdout, redact compression fix, docs overhaul         | ✅ Released |

### Upcoming Features (v1.16+)

| Version | Features        | Status      |
| ------- | --------------- | ----------- |
| v1.16.0 | Zip Input + Adaptive I/O | Released |
| v1.17.0 | Enum Conversion | Planned     |
| v1.18.0 | Migrate         | Planned     |
| v1.19.0 | DBML            | Planned     |
| v2.0.0  | Parallel        | Planned     |
| v2.1.0  | Infer           | Planned     |

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

6. ✅ **v1.8.1 — Glob Patterns + Agent Skills** — Released
   - Batch processing with glob patterns
   - Multi-dialect PK/FK validation
   - Agent Skill for AI coding tools

7. ✅ **v1.8.2 — Sample Memory Optimization** — Released
   - 98.5% memory reduction for sample command
   - Memory profiling infrastructure

8. ✅ **v1.9.0 — Diff** — Released
   - Schema + data comparison
   - Memory-bounded PK tracking (10M entries)

9. ✅ **v1.9.1 — Diff Enhanced** — Released
   - Verbose PK samples, PK override, ignore patterns
   - Index diff support

10. ✅ **v1.9.2 — CLI UX + Man Pages** — Released
    - Help headings, examples, aliases
    - Man page generation

11. ✅ **v1.10.0 — Redact** — Released
    - Data anonymization with 7 strategies
    - 25+ fake generators, YAML config
    - ~230 MB/s throughput, constant memory

12. ✅ **v1.11.0 — Graph** — Released
    - ERD generation (HTML, DOT, Mermaid, JSON)
    - Cycle detection with Tarjan's SCC
    - Order command for topological FK ordering
    - Tested with 281 tables, 3104 columns

13. ✅ **v1.12.0 — Query** — Released
    - SQL analytics with embedded DuckDB
    - Multi-dialect import, 5 output formats
    - Interactive REPL with meta-commands
    - Persistent caching with 400x speedup
    - 674 total tests (119 DuckDB-specific)

14. ✅ **v1.12.x — MSSQL** — Released
    - Fourth dialect: SQL Server / T-SQL
    - Full support in all 12 commands
    - GO batch separator, bracket identifiers, IDENTITY
    - Unicode strings (N'...'), CLUSTERED indexes
    - 29 MSSQL integration tests

15. ✅ **v1.13.x — Maintenance Releases** — Released (Jan–Jul 2026)
    - v1.13.0: Benchmark suite expansion (10 tools), JSON schema generation, OG image gen
    - v1.13.1: Diff bug fixes
    - v1.13.2–v1.13.5: Dependency bumps (rand 0.10, fake 5, duckdb CalVer, sha2 0.11), CI fixes
    - v1.13.6: Cargo feature flags (`compression`, `duckdb-query`) for library consumers, Docker Hub/GHCR images
    - v1.13.7: MySQL→PostgreSQL convert fixes (COMMENT/AUTO_INCREMENT/UNIQUE KEY, #64)
    - No new commands or features; planned roadmap features bumped +1 minor version

16. ✅ **v1.14.0 — CLI & Library Polish** — Released (Jul 2026)
    - `-o -` accepted as stdout for merge/sample/shard/convert/redact/order
    - merge status lines moved to stderr when SQL streams to stdout
    - redact now decompresses .gz/.bz2/.xz/.zst input (was a silent no-op)
    - MySQL→SQLite sized AUTO_INCREMENT fix (BIGINTEGER), MergeStats Serialize
    - Docs: Library Usage + Known Limitations pages, full accuracy sweep, Astro 7
    - Planned roadmap features bumped +1 minor version

17. ✅ **v1.16.0 — Zip Input + Adaptive I/O** — Released
    - `.zip` dumps accepted as input across all commands
    - Central-directory locate + streamed deflate (no new deps)
    - Single-`.sql`-member policy; clear errors for multi-member/encrypted

18. 🟡 **v1.17.0 — Enum Conversion** — Planned
    - Proper PG↔MySQL enum type conversion
    - PostgreSQL CREATE TYPE ... AS ENUM → MySQL inline ENUM()
    - MySQL inline ENUM() → PostgreSQL CREATE TYPE
    - Registry-based state tracking for streaming
    - Strip ::type casts in DML statements

19. 🟡 **v1.18.0 — Migrate** — Planned
    - Schema migration generation from diff
    - ALTER TABLE, CREATE INDEX statements
    - Rollback script generation
    - Breaking change detection

20. 🟡 **v1.19.0 — DBML Import/Export** — Planned
    - Export SQL schemas to DBML format
    - Import DBML to SQL DDL (all 4 dialects)
    - Extends `graph` command (export) and `convert` command (import)
    - Integration with dbdiagram.io ecosystem

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
- **DBML roundtrip**: SQL → DBML → SQL produces equivalent schema

---

## Non-Goals (Out of Scope)

- **GUI interface** — CLI only
- **Database connection** — File-based only
- **Binary backup formats** — No .bak (MSSQL)
- **Stored procedure conversion** — Too complex, warn and skip
- **Real-time streaming** — Batch processing only
- **Cloud storage integration** — Use pipes

---

## Ecosystem Integrations (v1.20+)

Strategic integrations beyond core CLI features. See [Integration Roadmap Master](INTEGRATION_ROADMAP_MASTER.md) for full analysis.

| Version | Integration            | Theme                              | Effort |
| ------- | ---------------------- | ---------------------------------- | ------ |
| v1.20.0 | **Parquet Export**     | DuckDB → Parquet/data lake bridge  | 12h    |
| v1.21.0 | **Great Expectations** | Auto-generate data quality suites  | 16h    |
| v1.22.0 | **Atlas**              | SQL dump → HCL schema-as-code      | 20h    |
| v1.23.0 | **dbt**                | Bootstrap dbt projects from dumps  | 28h    |

These follow the core roadmap (v1.16–v2.1) and require user demand validation before committing.

---

## Related Documents

### Active

- [Test Data Generator Design](TEST_DATA_GENERATOR.md)
- [Additional Ideas](features/ADDITIONAL_IDEAS.md)
- [Competitive Analysis](COMPETITIVE_ANALYSIS.md)
- [Integration Opportunities](INTEGRATION_OPPORTUNITIES.md)
- [Integration Roadmap Master](INTEGRATION_ROADMAP_MASTER.md)

### Upcoming Feature Designs

- [Enum Conversion](features/ENUM_CONVERSION.md) — v1.17.0
- [Migrate Feature](features/MIGRATE_FEATURE.md) — v1.18.0
- [DBML Support](features/DBML_SUPPORT.md) — v1.19.0

### Ecosystem Integration Designs (v1.18+)

- [DuckDB Deep Dive](features/DUCKDB_INTEGRATION_DEEP_DIVE.md) — Parquet export (v1.19.0)
- [Great Expectations Deep Dive](features/GREAT_EXPECTATIONS_INTEGRATION_DEEP_DIVE.md) — v1.19.0
- [Atlas Deep Dive](features/ATLAS_INTEGRATION_DEEP_DIVE.md) — v1.20.0
- [dbt Deep Dive](features/DBT_INTEGRATION_DEEP_DIVE.md) — v1.21.0

### Completed Feature Designs (moved to archived after implementation)

- [MSSQL Feasibility](features/MSSQL_FEASIBILITY.md) — v1.12.x (released)

### Archived (Implemented)

Historical documents for completed features in `docs/archived/`:

- QUERY_FEATURE.md — v1.12.0
- DUCKDB_QUERY_FEASIBILITY.md — v1.12.0 feasibility study
- GRAPH_FEATURE.md — v1.11.0
- REDACT_FEATURE.md — v1.10.0
- REDACT_IMPLEMENTATION_PLAN.md — v1.10.0
- DIFF_FEATURE.md — v1.9.0
- DIFF_IMPLEMENTATION_PLAN.md — v1.9.0
- SAMPLE_FEATURE.md — v1.5.0
- SHARD_FEATURE.md — v1.6.0
- MERGE_FEATURE.md — v1.4.0
- CONVERT_GAP_ANALYSIS.md — v1.7.0 post-implementation
- CONVERT_FEASIBILITY.md — Pre-implementation analysis
- ROADMAP_REVIEW.md — Pre-implementation recommendations
- TEST_FILE_EXTRACTION.md — Test reorganization
