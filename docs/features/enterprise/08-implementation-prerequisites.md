# Implementation Prerequisites and Delivery Plan

Numbered names in this document are delivery increments. Runtime uses only the
descriptive stages in [04](./04-execution-design.md).

## Implementation Phases

| Delivery sequence                                              | Outcome                                                                                                                                                               | Exit condition                                               |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| **Implementation Phase 1 — Foundations**                       | Versioned identifiers, endpoint/TLS/credential configuration, capabilities, exact vendor catalog, `DbValue`, `ColumnMeta`, bounded `RowBatch`, protected artifact I/O | Adapter-independent vectors and security tests pass          |
| **Implementation Phase 2 — Plan Contract**                     | Immutable plan schema, operation IDs, fingerprints, unsupported-object report, plan hash, plan-only CLI                                                               | One live dialect plan-only works against real engines        |
| **Implementation Phase 3 — Read Path & Consistency**           | Factories/sessions, cancellation, source read-only proof, snapshot token and lifecycle, typed keyset requests                                                         | Sequential snapshot concurrency tests pass for first dialect |
| **Implementation Phase 4 — Sequential Same-Dialect Execution** | Empty-target checks, create-only pre-data DDL, transactional plain INSERT chunks, durable journal, post-data DDL                                                      | First-dialect offline beta execution passes crash tests      |
| **Implementation Phase 5 — Verification & Recovery**           | Canonical comparison, FK anti-joins, constraint validation, ambiguous-commit reconciliation, strict finalize                                                          | No-success-with-skips and resume drift gates pass            |
| **Implementation Phase 5a — PostgreSQL Throughput**            | COPY-protocol chunk writer and pipelined chunk verification over an exported snapshot ([13](./13-throughput-and-copy-path.md))                                        | COPY path passes the same crash and equivalence matrices; measured throughput published |
| **Implementation Phase 5b — Managed Source Profiles**          | Privilege probe suite, execution-time attestation, sequence-stability evidence ([14](./14-managed-source-profiles.md))                                                | Probe, attestation-stop, and sequence-equality matrices pass against at least one managed provider |
| **Implementation Phase 6 — Second Dialect (MySQL)**            | Equivalent catalog, snapshot, DDL, value, and verification adapter; scope below                                                                                       | Same real-engine matrix passes for second dialect            |
| **Implementation Phase 7 — Cross-Dialect**                     | Explicit conversion policies and `RowTypeConverter` over canonical values                                                                                             | Every approved conversion has exact expected-value vectors   |
| **Implementation Phase 8 — Warm Target/Staging**               | Ownership model, merge/conflict policy, staging state machine, write-fence and recovery gates                                                                         | Destructive and metadata-lock fault tests pass               |
| **Implementation Phase 9 — Parallelism**                       | Shareable/exported snapshots, per-worker sessions, deterministic manifests, table-level parallel copy under a per-table prepared-chunk journal format ([13](./13-throughput-and-copy-path.md)) | Snapshot-sharing and concurrent crash matrix passes          |
| **Implementation Phase 10 — Dialect Optimizations**            | Cursor, direct-copy, concurrent-index, partition, and non-PostgreSQL COPY optimizations where proven                                                                  | Optimization is equivalent to baseline canonical output      |

Managed service automation and CDC remain deferred after this sequence.

## Second-dialect scope: MySQL

Implementation Phase 6's dialect is MySQL. Scope for its adapter proof:

- **InnoDB only first.** The consistent read path is one `REPEATABLE READ`
  transaction opened with `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
  Tables using a storage engine without MVCC (MyISAM, MEMORY, and similar)
  are blocking unsupported objects.
- **DDL is outside the snapshot.** MySQL DDL is at most atomic per statement
  (8.0+), and no transaction protects catalog reads against concurrent DDL.
  A fence-equivalent DML/DDL freeze must be proven before live execution.
  Candidate mechanisms are evaluated as
  [14](./14-managed-source-profiles.md)-style profiles with privilege
  probes, not assumed from documentation: `super_read_only` or
  `LOCK TABLES ... READ` for the DML half, paired with a DDL freeze.
  `LOCK INSTANCE FOR BACKUP` blocks only DDL and file-affecting operations
  and explicitly permits DML, so it can only ever be the DDL half of a
  fence equivalent.
- **`AUTO_INCREMENT` counters** are the sequence analogue: outside MVCC,
  captured and restored only under a proven freeze or the
  [14](./14-managed-source-profiles.md) equality re-read rule. Counter reads
  must bypass the cached statistics behind `information_schema.TABLES`
  (`information_schema_stats_expiry = 0`, recorded as evidence): with the
  default 86400-second expiry, two reads can return one stale cached value
  while the live counter advances. Restoration runs after data load, because
  InnoDB clamps `ALTER TABLE ... AUTO_INCREMENT` to the column maximum and
  bumps the counter on explicit-value inserts.
- **Collation binding.** Keyset text keys bind collation identity exactly as
  in PostgreSQL. Case-insensitive or otherwise non-deterministic collations
  are rejected as pagination keys.
- **Version matrix:** MySQL 8.0 and 8.4 LTS over TLS, with the same
  reproducible disposable-container posture as
  [12](./12-postgresql-first-adapter.md).
- **Carried over unchanged:** plan schema, journal, canonical values and
  vectors, verification, and artifact I/O. Implementation Phase 1–5
  contracts are adapter-neutral by construction; MySQL must not fork them.

## Planning ranges

These rough estimates are pending spikes and staffing decisions:

- One-dialect plan-only: **2–4 months**.
- One-dialect sequential production beta: **4–7 months**.
- Two same-dialect paths: **6–10 months**.
- Full advertised cross-dialect/resume/parallel hardening:
  **10–18 engineer-months**.

Driver behavior, exact catalog scope, database versions, test infrastructure,
and review time create substantial uncertainty. Line counts are not schedule
estimates.

## Requirements and acceptance gates

No Implementation Phase exits on unit tests alone. Required gates are:

1. **Real engines:** supported MySQL and PostgreSQL versions with TLS, exact
   catalogs, generated/identity columns, sequences, views, routines, triggers,
   partitions, expression/partial indexes, large values, and unsupported-object
   blocking.
2. **Snapshot concurrency:** concurrent inserts, updates, deletes, and DDL while
   a sequential MySQL or PostgreSQL snapshot runs; output and verification must
   reflect one point, or execution must stop. Write-fence loss must stop.
3. **Commit-boundary failures:** kill/crash before send, during write, before
   prepared-record publication, during commit, during ambiguous commit, after
   target commit before the committed-state transition, during journal fsync,
   and after journal publication. Resume must reconcile the prepared record and
   neither omit nor duplicate a row.
4. **Key pagination matrix:** scalar and composite integers, text with supported
   collations, binary keys, min/max values, exact page boundaries, one-row
   tables, empty tables, nullable rejection, non-unique rejection, key mutation
   rejection, and tables without a suitable key.
5. **Conflict tests:** same key/same canonical payload reconciliation, same
   key/different payload fatal error, secondary unique conflict to a different
   target key, and target trigger mutation. Existence alone never proves
   equality.
6. **Drift and resume:** reject plan, tool/state version, endpoint, snapshot or
   dump manifest, schema fingerprint, conversion policy, operation, and target
   effect drift.
7. **TLS and redaction:** hostname failure, untrusted CA, mTLS, explicit insecure
   audit, separate credentials, read-only source enforcement, malicious
   identifiers, no secrets/rows/SQL literals in logs, `0600` files, symlink and
   replacement races.
8. **FK integrity:** composite, nullable, self-referential, and multi-table cycle
   anti-joins; constraints added only after complete checks and database
   validation.
9. **Canonical vectors:** NULL, Unicode, bytes, integer boundaries, decimal
   scales, float NaN payloads/signed zero/infinities, temporal timezone and
   precision cases, JSON key order/number/duplicate-key cases, column order, and
   complete keys. Vectors are versioned across adapters.
10. **No success with skips:** inject conversion, truncation, replacement,
    constraint, and cancellation failures. Default completion must fail if any
    row was skipped, coerced, truncated, or replaced. Approved transformations
    produce only the distinct approved-transformation status.
11. **Complete target coverage:** extra target tables, extra rows before the
    first key, after the last key, and between committed intervals, plus rows in
    source-empty tables. Every target-only object or row must fail verification.
12. **Plan-to-execution consistency:** modify source data and DDL during plan
    review, break and re-establish a write fence, use nontransactional MySQL
    tables, and change sequence state outside PostgreSQL table MVCC. Execution
    must acquire fresh evidence and fail closed when the reviewed plan no longer
    matches or the consistency contract cannot cover an object.

## Documentation gate

Before each user-facing increment, synchronize the README, architecture,
execution, observability, CLI help, security model, and review ledgers. Future
flags must not appear as current flags before their Implementation Phase.
