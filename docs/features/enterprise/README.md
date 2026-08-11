# Enterprise Database Migration Plan

> **Spike status:** The non-default `enterprise-migration-spike` Cargo feature
> contains a fixture-backed spike for selected contracts needed by
> Implementation Phases 1–5: typed values, bounded batches, deterministic
> plans, snapshot-bound keyset paging, journal ordering, ambiguous-commit
> classification, and exact row comparison. It includes an internal live
> PostgreSQL runner and a durable database-side source write fence for the
> currently supported table subset. The fence survives a PostgreSQL restart,
> is re-attested before each execution stage, and is released only after strict
> verification. It can resume the narrow supported PostgreSQL table path across
> the implemented DDL, data, foreign-key, verification, and fence-release
> boundaries. PostgreSQL foreign keys are checked with exact composite `MATCH
> SIMPLE` or `MATCH FULL` null semantics, added without relaxing enforcement,
> and validated by PostgreSQL. It does not implement the complete crash matrix
> or production recovery, and it does not satisfy the complete real-engine
> acceptance gates in [08](./08-implementation-prerequisites.md).
> The feature-gated command also contains a source-only PostgreSQL assessment
> increment. It records typed target-not-assessed state, server-enforced
> read-only evidence, scope estimates, a projected source operation graph, and
> a protected deterministic Markdown report. Its PostgreSQL 15–17 matrix passes
> the current assessment acceptance gates, including blocking-code coverage and
> statement auditing. It remains an experimental feature-gated command, not a
> production release.
> The Implementation Phase 5a throughput increment is also implemented for the
> supported subset: a binary COPY chunk writer with the INSERT diagnostic
> fallback, exported-snapshot pipelined chunk verification, measured
> copy/verification throughput recorded in
> [13](./13-throughput-and-copy-path.md), and an optional plan-bound outage
> policy whose projection is refreshed and enforced at execute preflight
> before any journal or target effect. Its matrices pass on PostgreSQL 15–17.
> The sequential-beta boundary is unchanged: parallel copy remains Phase 9
> design, and none of this is a production support statement.
>
> A PostgreSQL plan adapter, snapshot-bound source reader, native control
> session, create-only pre-data DDL transaction, transactional plain-INSERT
> writer, read-only target verifier, strict finalization path, and explicit
> fence install/attest/release operations are available as internal spike
> contracts. A feature-gated spike command can execute and resume this narrow
> path. See
> [12](./12-postgresql-first-adapter.md) for the exact boundary and support
> matrix.

## Status and product boundary

This directory is a design plan, not a description of implemented behavior.
The first production beta has one explicit boundary:

- It is an **offline, same-dialect, sequential** migration into an **empty,
  migration-owned target**.
- It supports one live dialect first. A second same-dialect path follows.
- A live source must be quiesced behind a verified write fence or read through
  one database-native consistent snapshot for the complete copy and
  verification window.
- An execute run is strict: no skipped, truncated, coerced, or replaced rows.
- The target is create-only. The beta does not drop, narrow, merge, swap, or
  automatically roll back objects.

This plan distinguishes three products:

1. **Plan-only** reads catalogs and emits a deterministic plan. It writes no
   database objects or rows.
2. **Offline bulk copy** is the production beta described above. It has a
   bounded outage or uses a snapshot whose lifetime covers copy and verify.
3. **Online migration** needs change data capture (CDC), replication catch-up,
   cutover coordination, and rollback procedures. It is future work. The plan
   makes no zero-downtime or online-cutover claim.

## Document index

This directory contains 19 documents:

| Document                                            | Purpose                                                               |
| --------------------------------------------------- | --------------------------------------------------------------------- |
| [01](./01-migration-landscape.md)                   | Product boundary, external landscape, and supported patterns          |
| [02](./02-capability-audit.md)                      | Existing capabilities and gaps                                        |
| [03](./03-connection-architecture.md)               | Connection, session, snapshot, value, and catalog contracts           |
| [04](./04-execution-design.md)                      | Canonical executor, CLI, journal, pagination, and verification design |
| [05](./05-review-findings.md)                       | Consolidated safety findings and decisions                            |
| [06](./06-cross-industry-patterns.md)               | Applicable industry patterns and deferred patterns                    |
| [07](./07-managed-service-appendix.md)              | Deferred managed-service design                                       |
| [08](./08-implementation-prerequisites.md)          | Delivery sequence, estimates, and acceptance gates                    |
| [09](./09-round-1-review-findings.md)               | Round-1 resolution ledger                                             |
| [10](./10-round-2-review-findings.md)               | Round-2 resolution ledger                                             |
| [11](./11-round-3-review-findings.md)               | Round-3 resolution ledger                                             |
| [12](./12-postgresql-first-adapter.md)              | First live-dialect decision and plan-only boundary                    |
| [13](./13-throughput-and-copy-path.md)              | COPY fast path, pipelined verification, parallel copy, measured gates |
| [14](./14-managed-source-profiles.md)               | Fence privilege probes, managed profiles, quiesce evidence            |
| [15](./15-assessment-product.md)                    | Standalone read-only assessment product and report                    |
| [16](./16-round-4-review-findings.md)               | Round-4 resolution ledger                                             |
| [Competitive landscape](./competitive-landscape.md) | Tool categories and scoped positioning                                |
| [Observability](./observability-and-operations.md)  | Metrics, logs, alerts, and operations                                 |
| This README                                         | Scope and navigation                                                  |

## Runtime stages

Runtime uses descriptive stage names only:

`preflight → schema extraction → plan → pre-data DDL → data copy → post-data DDL → verification → finalize`

“Implementation Phase N” is reserved for delivery sequence. The canonical
sequence is in [08](./08-implementation-prerequisites.md).

## Core safety decisions

- Connection factories create isolated source, target, verification, control,
  and cancellation sessions. Driver clients are not required to be `Sync`.
- A canonical typed value and exact vendor catalog model precede runtime row
  conversion. `TypeMapper` rewrites SQL type text; it does not convert values.
- Resumable copy uses lexicographic keyset pagination over the complete,
  immutable, non-null unique key tuple.
- Plain `INSERT` is the default. A conflict is fatal unless canonical payload
  equality is completely proven.
- Resume trusts a durable committed-chunk journal, not target-side deduplication.
- Verification compares expected post-conversion source values with target
  values using one versioned canonical encoding and the same snapshot and chunk
  manifest used by copy.
- Tables are created without foreign keys. Complete anti-join checks run before
  constraints are added and validated, including composite, self, and cyclic
  relationships.
- TLS with hostname verification is the default. Source and target credentials
  are separate, and the source account is database-enforced read-only.

## Delivery estimates

These are rough planning ranges pending driver, catalog, and snapshot spikes:

- One-dialect plan-only: **2–4 months**.
- One-dialect sequential production beta: **4–7 months**.
- Two same-dialect paths: **6–10 months**.
- Full advertised cross-dialect, resume, parallel, and hardening scope:
  **10–18 engineer-months**.

No line-count estimate is used as a schedule.
