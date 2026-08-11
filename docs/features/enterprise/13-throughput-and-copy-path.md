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
