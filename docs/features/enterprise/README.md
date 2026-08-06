# Enterprise Database Migration — Design & Architecture

> Implementation planning for adding live database connection support,
> migration planning, data verification, and migration execution to
> sql-splitter.

## Status

**Planning — not yet implemented.** All documents in this directory are
design specifications and architecture proposals.

## Relationship to Other Features

| Feature                            | Scope                                                                                                                                             | Status                     |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `docs/features/enterprise/` (this) | **Live database migration**: connect to source/target DBs, extract schema, stream data, convert types, generate execution plans, verify integrity | Planning                   |
| `docs/features/MIGRATE_FEATURE.md` | **File-based DDL migration**: compare two SQL dump files, generate ALTER statements to transform schema A → schema B                              | Planning (unrelated scope) |

These are complementary but different scopes. The enterprise `migrate`
command would read from live databases and stream data, while the
file-based `migrate` command operates on static dump files. The
enterprise `migrate` command's plan generation phase could absorb the
file-based DDL generation logic from `MIGRATE_FEATURE.md`.

## Document Index

| File                                                                         | Contents                                                                                                                                                                 |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`01-migration-landscape.md`](./01-migration-landscape.md)                   | PlanetScale/Vitess, AWS DMS, CDC patterns, tooling landscape, enterprise pitfalls, managed database constraints                                                          |
| [`02-capability-audit.md`](./02-capability-audit.md)                         | Current sql-splitter architecture mapped against migration needs, detailed gap analysis with code references                                                             |
| [`03-connection-architecture.md`](./03-connection-architecture.md)           | Low-level database connection design — `DbSource`/`DbTarget` traits, crate choices, streaming guarantees, PlanetScale-specific handling                                  |
| [`04-execution-design.md`](./04-execution-design.md)                         | Full `migrate` command design — pipeline, failure-mode catalog (60+ modes), parallel import, staging strategies, chunking, e2e testing, benchmarking, structured logging |
| [`05-review-findings.md`](./05-review-findings.md)                           | Adversarial review — 7 critical findings, 13 high-severity findings, Percona research, updated performance model, implementation sequence                                |
| [`06-cross-industry-patterns.md`](./06-cross-industry-patterns.md)           | Synthesis of 10-agent industry research — Percona, MariaDB, PlanetScale, Neon, ClickHouse, GitHub, AWS, distributed DBs, schema tools, open-source migrants              |
| [`07-managed-service-appendix.md`](./07-managed-service-appendix.md)         | **DEFERRED** — Future managed cloud service architecture (API server, workers, KMS, WebSocket progress), not in implementation plan                                      |
| [`08-implementation-prerequisites.md`](./08-implementation-prerequisites.md) | Refactoring audit — what exists today, what must change before implementation, coherence fixes for design docs, dependency graph                                         |
| [`09-round-1-review-findings.md`](./09-round-1-review-findings.md)           | Three parallel audits — code accuracy (50+ API refs verified), industry claims (12 checked), adversarial edge cases (38 new failure modes found)                         |
| [`10-round-2-review-findings.md`](./10-round-2-review-findings.md)           | Five parallel audits — architecture trace (3 critical gaps found), performance model, security, concurrency, feature flag/build. 6 critical, 18 high, 14 medium          |

## Architecture Overview

```
Source DB ──► DbSource::extract_schema() ──► Schema ──► compare_schemas()
                                                           │
              DbSource::stream_table_rows()                ▼
                     │                            MigrationPlan
                     ▼                                   │
              RowBatch ──► RowTypeConverter ──► DbTarget::insert_rows()
                                                  │
              DbTarget::execute_ddl() ◄───────────┘
                     │
                     ▼
              Verification (SHA-256 checksums)
```

## Key Design Decisions

1. **Synchronous database drivers** (`mysql` v25, `postgres` v0.19) — match
   the existing codebase architecture, no async runtime needed.

2. **Streaming, bounded-memory operation** — rows stream from source to
   target through the executor without buffering full datasets. A 2TB
   migration runs with ~8MB of memory per worker.

3. **Read-only by construction** — `DbSource` trait has no `execute()`
   method. Writes go through a separate `DbTarget` trait, opt-in behind
   `--execute`.

4. **No subprocess orchestration** — sql-splitter is middleware between
   existing tools (`pscale dump`, `mydumper`, `pgloader`), not a
   replacement for them. The executor connects to databases directly,
   never invokes external CLI tools.

5. **Idempotent execution** — `INSERT ... ON DUPLICATE KEY UPDATE` for
   MySQL, `ON CONFLICT (pk_cols) DO NOTHING` for Postgres. Re-running
   a failed migration skips already-imported rows via PK-based dedup.

6. **Enterprise-grade safety** — staging-table strategy for non-transactional
   MySQL DDL, pre-flight checks for connectivity/permissions/charset/FK
   integrity, hazard annotations on every operation, rollback plan
   generation, structured JSON logging with span hierarchy.

## Before Implementation — Prerequisites

See [`08-implementation-prerequisites.md`](./08-implementation-prerequisites.md)
for the full refactoring audit. In short:

| #   | Prerequisite                              | Effort     | Unlocks                              |
| --- | ----------------------------------------- | ---------- | ------------------------------------ |
| P0  | Add `SchemaGraph::topo_levels()`          | ~40 lines  | Parallel import design               |
| P0  | Make `TypeMapper` `pub`                   | 1 line     | `RowTypeConverter`                   |
| P1  | Extract `for_each_statement()` combinator | ~100 lines | Single parser loop for all consumers |
| P1  | Unify COPY pairing across 4 parser loops  | ~80 lines  | Shared file-source path              |
| P2  | Add `migrate` Cargo feature flag          | ~5 lines   | Phase-gated DB drivers               |
| P2  | Add `MIG-*` diagnostic codes              | ~30 lines  | Error handling consistency           |

These are refactoring-only — they don't add migration functionality but
they make the implementation straightforward. After them, `src/migrate/`
fits naturally into the existing architecture.

## Coherence Fixes

Six design-document inconsistencies need resolution before implementation
(see §C-FIX-1 through C-FIX-6 in `08-implementation-prerequisites.md`):
reconcile PlanetScale subprocess position, harmonize phase numbering
across all documents, update `03` for text protocol decision, add missing
CLI flags to `04` §6.8, assign orphan gaps 4-8 to phases, add or remove
`--copy-mode direct` reference.

## Implementation Phases

| Phase | Contents                                                                                                                                                                              | New Code     |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| 0     | `DbSource` trait + `MySqlSource` + `PostgresSource` (schema-only). Text protocol (`query_iter`). Schema via `information_schema` batch queries.                                       | ~600 lines   |
| 1     | `MigrationPlan` generation from `SchemaDiff`. Column rename heuristic. Self-referential FK intra-table ordering. `NOT VALID` for PG constraints. Version-aware keyword checking.      | ~400 lines   |
| 2     | `DbTarget` trait + `MySqlTarget` + `PostgresTarget`. `RowTypeConverter` for cross-dialect data. FK/unique-check disable per worker. `sql_mode` session override. `--binlog-handling`. | ~1,200 lines |
| 3     | `migrate --execute` with live data import. `--chunk-column` with boundary discovery. `--staging-strategy`. Sequential Phase 3b DDL.                                                   | ~800 lines   |
| 4     | Verification: chunk-aware checksums, rowcount mode. Schema-change detection. `--bench` throughput measurement.                                                                        | ~400 lines   |
| 5     | `--parallel` with level-based worker pool. Intra-table parallelism via chunk-column. Partition-aware import.                                                                          | ~600 lines   |

## Related Research

The design documents in this directory are based on:

- 5-agent adversarial codebase review (connection correctness, data integrity,
  operational safety, performance/scalability, integration with existing code)
- 10-agent cross-industry research (Percona, MariaDB, PlanetScale, Neon,
  ClickHouse, GitHub/Stripe/Shopify, AWS, CockroachDB/YugabyteDB/TiDB/SingleStore,
  Flyway/Liquibase/Bytebase/Skeema/Atlas, pgloader/mydumper/pg_chameleon/Debezium)
- Real-world migration case studies (Fathom Analytics 2TB → 1TB, 65B rows;
  Shopify 400TB+ CDC; GitHub 1200+ host MySQL 8.0 upgrade)
- Direct verification of sql-splitter source code references (100% accuracy
  confirmed across all file paths and line numbers)

## Gap Assignment

The capability audit (`02-capability-audit.md`) identifies 8 gaps. Here is
where each is addressed in the implementation plan:

| Gap                           | Phase          | Rationale                                                              |
| ----------------------------- | -------------- | ---------------------------------------------------------------------- |
| 1 (Live DB connection)        | Phase 0–1      | Foundation — `DbSource` trait, MySQL + PG schema extraction            |
| 2 (Migration plan generation) | Phase 1        | Foundation — `MigrationPlan` from `SchemaDiff`, hazard annotations     |
| 3 (Data verification)         | Phase 4        | Depends on live import working first                                   |
| 4 (PlanetScale validation)    | Phase 1        | Schema analysis — low effort, high value. Extends existing `Validator` |
| 5 (Risk assessment)           | Phase 1        | Analysis layer on existing data — `RiskAssessment` struct              |
| 6 (Runbook generation)        | Phase 4        | Template rendering from structured plan output                         |
| 7 (Stored proc cataloging)    | Phase 2        | Needed for cross-dialect migration — catalog what needs to be re-added |
| 8 (Middleware pipeline)       | Already exists | stdin/stdout support is built in; documented as design pattern         |
