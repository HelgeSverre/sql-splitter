# Throughput and Copy Path

This document defines the throughput work the offline beta needs before its
bounded-outage claim is credible at enterprise data sizes. It re-sequences the
PostgreSQL COPY fast path and pipelined chunk verification ahead of the second
dialect as [08](./08-implementation-prerequisites.md) Implementation Phase 5a.
Bounded parallel copy remains Implementation Phase 9 scope and is designed
here only so the journal and snapshot contracts account for it. Origin: the 2026-08-11 market-alignment plan
(`docs/superpowers/plans/2026-08-11-enterprise-migration-market-alignment.md`).

## Baseline cost model

The current spike path is sequential:

- **Data copy:** one snapshot read pass in bounded keyset pages, then one
  parameterized `INSERT` round trip per row.
- **Verification:** a full re-read of source and target for every committed
  chunk, plus tail probes past the last committed key.
- **Outage window:** the write fence (or attested quiesce) covers copy and
  verification end to end.

The window is therefore approximately three full data passes plus per-row
network latency amplification on the write pass, single-threaded. That is
correct but not a credible enterprise outage budget. Closing this is an
engineering change inside the existing chunk/journal contract, not an
architecture change.

## COPY fast path

Chunk boundaries and journal semantics do not change: `ChunkPrepared` with the
canonical digest before any write, one explicit target transaction, then
`ChunkCommitted`. Within the chunk transaction, the per-row `INSERT` loop is
replaced by one `COPY ... FROM STDIN (FORMAT binary)` stream over the same
column projection, encoded from the same canonical `DbValue`s.

Rules:

- Generated columns are omitted from the COPY column list. PostgreSQL
  recomputes them and reconciliation compares the complete recomputed row,
  unchanged from the INSERT path.
- Identity-column behavior under COPY for `GENERATED ALWAYS` columns must be
  proven by the live value matrix before the COPY path is enabled for any
  table containing one. Until proven, such tables use the INSERT path.
- A COPY failure aborts the chunk transaction, equivalent to today's chunk
  rollback. Per-row error localization is lost, so a chunk that fails under
  COPY may be retried once through the existing prepared-INSERT path to
  localize the offending row. The INSERT writer is retained as the diagnostic
  path; it is not removed.
- COPY has no conflict handling and needs none: the target is create-only and
  empty, and resume reconciliation continues to trust durable prepared-chunk
  digests, never target-side deduplication.
- Equivalence gate: for every canonical value-matrix case, the COPY path and
  the INSERT path must produce byte-identical verified rows and identical
  canonical digests on every supported PostgreSQL version.

## Pipelined verification

Verification strictness does not change: exhaustive, exact, bound to the same
snapshot and chunk manifest. Pipelining covers chunk-content equality only: a
committed chunk's key interval may be re-read and compared while later chunks
copy, because committed intervals are disjoint. The tail probes and the
extra-row completeness pass of
[08](./08-implementation-prerequisites.md) gate 11 cannot be pipelined —
past the last committed key the target legitimately contains in-flight rows —
so they run after the table's final chunk commits, still inside the fence or
quiesce window.

The second source session binds to the primary snapshot through
`pg_export_snapshot`. Export is available on every supported version; the
real constraint is lifetime: a snapshot is importable only while the
exporting transaction remains open. The exporting transaction is therefore
pinned open until every consumer has executed `SET TRANSACTION SNAPSHOT`
inside a `REPEATABLE READ` transaction before its first query, and snapshot
equality is attested by comparing `pg_current_snapshot()` across sessions.
This pulls part of the snapshot-sharing evidence of Implementation Phase 9
forward for PostgreSQL only. If the exported snapshot cannot be attested,
verification runs sequentially as today.

## Bounded parallel copy

Parallel copy does not amend the beta boundary: the beta remains sequential
as stated in the [README](./README.md), and this section is Implementation
Phase 9 scope. It enters a support statement only when the concurrent crash
matrix passes and the boundary documents are amended in the same change.

After the COPY path and pipelined verification pass their gates:

- Table-level parallelism first: a bounded worker pool where each worker owns
  one table at a time, with its own source session bound to the shared
  exported snapshot and its own target session.
- The journal remains a single hash-linked append stream. Workers submit
  events to one serialized journal writer.
- The at-most-one-`Prepared`-chunk invariant becomes per-table. That is a
  journal contract change: the journal format version is bumped and older
  journals are rejected for automatic resume, consistent with the existing
  versioning posture.
- Intra-table key-range splitting is deferred until table-level parallelism
  passes the complete crash matrix.

## Measured gates

No absolute throughput number is promised in advance, and no unmeasured
speedup claim is made. Before the beta claim:

1. The reproducible container matrix gains fixed synthetic dataset shapes:
   narrow integer rows, wide text rows, and `bytea` rows.
2. Rows/sec and bytes/sec are recorded for the INSERT baseline and the COPY
   path on every supported PostgreSQL version, and the measurements are
   published with the support statement.
3. Execute preflight projects the expected outage window from measured
   throughput and the assessed data size, labeled as an estimate in the
   assessment report ([15](./15-assessment-product.md)).
4. The COPY path passes the same crash, commit-boundary, cancellation, and
   fault-injection matrices as the INSERT path before it becomes the default.

## Recorded measurements

The fixed 10,000-row release-profile harness
(`live_insert_and_copy_throughput_matrix`, reproducible through
`scripts/test-migration-postgres.sh <15|16|17>
live_insert_and_copy_throughput_matrix`) drives the INSERT baseline and the
binary COPY path through the same typed `RowBatch` and target writer and
checks exact target row counts. One recorded run:

| PostgreSQL | Shape     | INSERT rows/s | COPY rows/s | INSERT bytes/s | COPY bytes/s   |
| ---------- | --------- | ------------: | ----------: | -------------: | -------------: |
| 15.18      | narrow    |      9,787.56 |  952,573.74 |     156,601.01 |  15,241,179.76 |
| 15.18      | wide text |      8,328.98 |  134,958.31 |  17,124,382.88 | 277,474,279.04 |
| 15.18      | bytea     |      7,791.54 |   98,960.54 |  16,019,408.73 | 203,462,876.81 |
| 16.14      | narrow    |      9,458.57 |  956,220.95 |     151,337.09 |  15,299,535.26 |
| 16.14      | wide text |      7,839.64 |  136,648.11 |  16,118,303.86 | 280,948,518.82 |
| 16.14      | bytea     |      7,824.83 |  100,650.54 |  16,087,859.32 | 206,937,505.39 |
| 17.10      | narrow    |      9,898.07 |  992,748.76 |     158,369.09 |  15,883,980.23 |
| 17.10      | wide text |      8,519.88 |  141,072.82 |  17,516,868.20 | 290,045,725.23 |
| 17.10      | bytea     |      7,954.77 |  103,282.98 |  16,355,014.65 | 212,349,803.96 |

The same harness measures the exact page-wise verification path: fresh
verification sessions, every canonical row and digest compared, one assessed
data byte per byte verified.

| PostgreSQL | Shape     | Verification rows/s | Verification bytes/s |
| ---------- | --------- | ------------------: | -------------------: |
| 15.18      | narrow    |          211,423.20 |         3,382,771.12 |
| 15.18      | wide text |           96,883.50 |       199,192,476.03 |
| 15.18      | bytea     |           96,208.54 |       197,804,761.60 |
| 16.14      | narrow    |          226,405.27 |         3,622,484.33 |
| 16.14      | wide text |           97,914.30 |       201,311,806.87 |
| 16.14      | bytea     |           97,255.88 |       199,958,098.27 |
| 17.10      | narrow    |          197,124.12 |         3,153,985.99 |
| 17.10      | wide text |           99,584.57 |       204,745,867.81 |
| 17.10      | bytea     |           76,904.98 |       158,116,648.60 |

Environment: Apple M2 Max, 32 GiB RAM, macOS Darwin 24.6.0, Docker Desktop
29.4.0 Linux/arm64 engine, Rust 1.97.1. These are environment-specific
single-run spike measurements, not product guarantees; a production support
statement requires measurements recorded in the supported deployment
environment, and a throughput profile explicitly binds its measurement and
environment references. This satisfies measured-gate items 1 and 2 for the
spike environment. Gate item 3 is implemented on the assessment side:
assessment schema version 3 accepts an optional protected throughput profile
(separate copy and verification rates, PostgreSQL major, measurement time,
validity period, environment reference), computes a conservative
ceiling-summed window over `total_relation_bytes` for each copied physical
`relkind = 'r'` relation exactly once, fails to `NotAssessed` on a missing,
stale, future, incomplete, or wrong-major profile, and recomputes the result
during validation to reject tampering. The execute-preflight
projection follows the plan-bound outage-policy contract below. The gate-4
matrices have passed on PostgreSQL 15, 16, and 17 per the [008]/[010]
mailbox evidence.

## Execute-preflight projection: plan-bound outage policy

The selected contract (mailbox [014]/[017]) preserves one reviewed execution
artifact; execute and resume gain no new profile inputs.

- Plan creation optionally accepts a protected assessment artifact and a
  maximum approved outage in seconds — both together or neither. A plan
  without an outage policy carries no projection and execute enforces
  nothing new.
- Plan creation validates the assessment and its throughput profile and
  embeds a typed reviewed outage policy: assessment digest, source catalog
  fingerprint, the explicit byte basis (PostgreSQL `pg_total_relation_size`
  summed over the copied physical `relkind = 'r'` relation inventory, with
  the exact sorted relation identities bound into the policy), the complete
  throughput profile, reviewed input bytes and projected seconds, and the
  approved maximum. The plan hash covers every field.
- Initial execute refreshes the byte basis from the same source snapshot
  used for catalog attestation, recomputes the projection, and blocks
  before journal creation and before any target effect when the profile is
  stale or incompatible or the refreshed estimate exceeds the approved
  maximum.
- The accepted projection is stored in journal genesis and its digest in
  the resume binding. Resume accepts no replacement assessment, profile,
  basis, or limit. Profile expiry after initial admission never blocks
  recovery: the outage was accepted and has already started, and blocking
  recovery would only extend it.
- The projection is an estimate and is labeled as one wherever rendered.
  Exceeding it at runtime is an observability event, not an integrity
  failure.
