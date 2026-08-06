# Part 11: Round-3 Review Findings

> Five parallel audits: document coherence, competitive landscape,
> implementation risks, observability design, missing migration scenarios.
> 5 critical coherence failures found (claimed fixes never applied), 58
> implementation risks identified, 10 migration gaps discovered.

## Document Coherence Re-Audit

### Critical: Claimed Fixes Never Applied to 04

Five fixes claimed in Round 1 and Round 2 were never written to
`04-execution-design.md`, the authoritative design specification:

| Claimed Fix                                                           | Source         | Status                |
| --------------------------------------------------------------------- | -------------- | --------------------- |
| 38 new failure modes added to §6.2                                    | 09 §Applied #3 | **NOT APPLIED**       |
| `SslMode` enum + `--ssl-mode` flag                                    | 10 §Applied #4 | **NOT APPLIED**       |
| Trace log gated behind `debug-trace` feature                          | 10 §Applied #5 | **NOT APPLIED**       |
| `START TRANSACTION WITH CONSISTENT SNAPSHOT` for live MySQL           | 10 §Applied #3 | **NOT APPLIED**       |
| Password-in-URI examples replaced (only §6.1 fixed, §§6.6–6.7 missed) | 10 §Applied #6 | **PARTIALLY APPLIED** |

**Root cause**: The "Applied Fixes" sections in review documents stated
what _should_ be fixed but the edits were aspirational. `04` trails
behind what the review documents claim.

### Other Coherence Issues

| #   | Severity   | Issue                                                                                                                                       |
| --- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| C3  | **HIGH**   | `04` §6.1 calls `stream_table_rows_ordered()` which isn't in the `DbSource` trait defined in `03` §5.2.3                                    |
| C4  | **HIGH**   | `04` §6.3d rename-safe SQL swaps `orders` → `__sql_splitter_staging_orders` instead of `__sql_splitter_old_orders`, destroying staging data |
| C5  | **HIGH**   | `--neon-branch` missing from `04` §6.8 (was on C-FIX-4 list)                                                                                |
| C6  | **MEDIUM** | Three incompatible phase numbering systems persist across documents                                                                         |
| C7  | **MEDIUM** | 6 hanging/phantom references found across docs                                                                                              |
| C8  | **LOW**    | `--copy-mode direct` referenced in prose but has no implementation design                                                                   |

## Competitive Landscape

sql-splitter occupies a unique position: **file-based, multi-dialect,
streaming SQL analysis with live DB connection support**. No direct
competitor exists in Rust or any language.

Closest competitor is Atlas (`migrate diff`) — but requires a live
`--dev-url` database. sql-splitter works on dump files.

Key differentiators:

1. File-based — works on databases without binlog access
2. 4 dialects in one binary (MySQL, PG, SQLite, MSSQL)
3. Streaming for unlimited file sizes (no in-memory accumulation)
4. Data + schema diff combined
5. Single binary, zero runtime dependencies
6. PlanetScale exit path that no CDC tool can provide

## Implementation Risks

58 risks identified across all phases. Top 5 by severity:

| #    | Risk                                                  | Phase | Prob × Impact     |
| ---- | ----------------------------------------------------- | ----- | ----------------- |
| X-7  | No consistent snapshot from live MySQL                | 0     | Critical × High   |
| X-1  | TLS not enforced by default                           | 0–2   | High × Critical   |
| P3-3 | Staging-table rename partial failure                  | 3     | Medium × Critical |
| P1-6 | SchemaDiff bugs produce silently wrong plans          | 1     | Medium × Critical |
| P3-6 | Post-data DDL connection drop with FK checks disabled | 3     | Low × Critical    |

## Observability and Operations

Full observability design covering: pre-migration health checks (18
checks via `migrate check` subcommand), during-migration metrics (25
metrics, 14 alert types), post-migration reporting, operational runbook
(10-item pre-flight checklist, phase-specific recovery), and alerting
(6 delivery mechanisms, dedup windows, severity classification).

## Missing Migration Scenarios

10 gaps identified:

- **In-scope (trivially supported)**: Schema-first migration
- **Gap — needs design**: Data-only migration, 10K+ table schemas,
  no chunk key fallback, write-heavy workload detection, replication
  topology detection, encrypted columns, temporal tables, geospatial
  types, full-text indexes, PII redaction integration
- **Deferred (Phase X+)**: SQLite migration, MSSQL row support,
  MariaDB dialect, multi-tenant shard merge
- **Out-of-scope**: Oracle, MongoDB, rolling migration, blue/green,
  canary, append-only, GDPR residency, HIPAA audit trail

## Applied Fixes

These corrections have been applied to the design documents:

1. **`04` §6.3d**: Fixed rename-safe SQL — staging table now swaps
   with `__sql_splitter_old_orders` first, then renames staging to
   `orders`.

2. **`04` §6.1**: `stream_table_rows_ordered()` removed. Self-ref FK
   ordering moved into `stream_table_rows()` with a conditional
   `ORDER BY` clause parameter.

3. **Hanging references fixed**: `05-connection-architecture.md` →
   `03-connection-architecture.md` in `02`. `04 §4.12` references
   removed from `10`. `04 §6.10` references removed.

4. **`04` §6.8**: `--neon-branch` flag added (Phase 5, manual workflow
   documented).

5. **`03` §5.2.6, `04` Phase 0**: `START TRANSACTION WITH CONSISTENT
SNAPSHOT` + GTID capture added to live MySQL extraction path.
