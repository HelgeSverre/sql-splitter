# Migration Execution Design

## Runtime contract

The production beta runs these stages sequentially:

1. **preflight**
2. **schema extraction**
3. **plan**
4. **pre-data DDL**
5. **data copy**
6. **post-data DDL**
7. **verification**
8. **finalize**

Plan-only stops after `plan`. Offline bulk copy requires the same dialect, an
empty migration-owned target, a write fence or one verified native snapshot,
and strict verification. Online migration is future CDC work.

## Canonical CLI contract

```text
sql-splitter migrate \
  --source-config source.toml \
  --target-config target.toml \
  --consistency consistent-snapshot \
  --plan-output plan.json

sql-splitter migrate \
  --plan-input plan.json \
  --source-config source.toml \
  --target-config target.toml \
  --approval-ref CHANGE-1234 \
  --execute \
  --strict-verification \
  --state-output migration.state

sql-splitter migrate \
  --resume migration.state \
  --source-config source.toml \
  --target-config target.toml
```

Initial flags:

| Flag                                                | Contract                                                                                                                      |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `--source-config <PATH>`                            | Endpoint, TLS, and credential reference; no inline secret                                                                     |
| `--target-config <PATH>`                            | Separate target endpoint, TLS, and credential reference                                                                       |
| `--plan-output <PATH>`                              | Required durable JSON output for a new plan-only run                                                                          |
| `--plan-input <PATH>`                               | Exact reviewed plan required by a new execute run                                                                             |
| `--approval-ref <ID>`                               | Required external approval/change-record reference bound to the plan hash                                                     |
| `--execute`                                         | Explicit write gate for `--plan-input`; absent means plan-only                                                                |
| `--strict-verification`                             | Required with `--execute`; canonical data and schema checks                                                                   |
| `--state-output <PATH>`                             | Required with `--execute`; new protected journal                                                                              |
| `--resume <PATH>`                                   | Resume the exact recorded migration; mutually exclusive with plan creation, `--plan-input`, `--execute`, and `--state-output` |
| `--consistency <MODE>`                              | `write-fence` or `consistent-snapshot`                                                                                        |
| `--write-fence-ack <ID>`                            | Required for write-fence mode and recorded in state                                                                           |
| `--tls-ca`, `--tls-client-cert`, `--tls-client-key` | Explicit trust and mTLS inputs                                                                                                |
| `--tls-insecure`                                    | Exceptional opt-out with warning and audit record                                                                             |
| `--credential-source`, `--credential-target`        | Protected credential references                                                                                               |
| `--source-dump <PATH>`                              | Immutable completed dump input; mutually exclusive with live `--source-config`                                                |
| `--source-manifest <PATH>`                          | Required identity, completion, file-list, and checksum manifest for `--source-dump`                                           |

No initial flag enables drop, narrowing, staging, target merge, direct server
copy, arbitrary webhooks, parallel extraction, conflict skipping, rowcount-only
verification, or automatic rollback. Future flags are introduced only in their
Implementation Phase and are not accepted before then.

Plan-only creates an immutable plan and does not write to the target. A new
execute run consumes that exact plan through `--plan-input`; it does not
regenerate intent. The runner verifies the plan hash, endpoint identities,
catalog fingerprints, consistency mode, and approval evidence before the first
target write. Resume consumes only the journal, which binds the same plan and
endpoint configuration. It never combines a new plan with old execution state.
`--approval-ref` is a reference to an external approval record, not a claim that
the CLI verifies a digital signature. Signed approvals can be added in a later
implementation phase.

## Preflight and plan

Preflight proves endpoint identity, authenticated TLS, credentials, source
read-only enforcement, target ownership and emptiness, dialect/version,
required capabilities, consistency mode, snapshot lifetime, key suitability,
catalog completeness, and local state storage safety. Unknown is not pass.
Remote free disk is generally unknown unless a documented provider capability
supplies it; database used size is not free space.

The immutable plan records all typed operations, dependencies, hazards,
capabilities, identifiers, expected catalog fingerprints, conversion policy,
and unsupported objects. Operation IDs and a canonical plan hash are stable.
The target must still be empty immediately before pre-data DDL.

## DDL and foreign keys

Pre-data DDL creates new tables, sequences, and required unique indexes without
foreign keys. It never drops or modifies pre-existing objects. Data copy uses
plain `INSERT` in a transaction per committed chunk. Defaults are not used to
hide missing source values.

After copy, the post-data DDL stage first runs complete anti-join checks for
every FK, including composite keys, self-references, cycles, and SQL NULL
semantics. Only after those preconditions pass does it add constraints and
require the database to validate them. The following verification stage performs
canonical source/target comparison and final schema verification. Row ordering
does not solve self-FKs. The default does not disable
`FOREIGN_KEY_CHECKS`, `UNIQUE_CHECKS`, `sql_mode`, triggers, or
`session_replication_role`. Privileged relaxation is a future explicit policy.

PostgreSQL `NOT VALID` can reduce validation work timing, but adding a constraint
still takes locks and `VALIDATE CONSTRAINT` has its own lock and scan behavior:
[PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html).
Concurrent indexes obey their nontransactional restrictions. MySQL DDL implicit
commits are represented as capabilities and journal boundaries.

## Keyset pagination

Resumable tables require a complete immutable, non-null unique key tuple. The
query is lexicographic:

```text
ORDER BY (k1, k2, ... kn)
WHERE (k1, k2, ... kn) > (:last_k1, :last_k2, ... :last_kn)
LIMIT :rows
```

Adapters may expand tuple comparison into equivalent bound predicates. They
must preserve database comparison semantics. The key includes all unique-key
columns; the journal persists the complete last committed tuple.

- Nullable keys are rejected for resumable MVP copy.
- Composite keys use lexicographic comparison, never independent scalar ranges.
- Text keys record collation and server version; binary canonical key values do
  not replace database ordering semantics.
- The beta uses the complete selected unique key in one ascending database order.
  Descending or mixed-direction resumable ordering is deferred.
- A page commits only after every row through its final key commits. The next
  page is strictly greater, so the final boundary appears exactly once.
- Key mutation is prohibited by the write fence/snapshot contract.

No `MIN/MAX` linear ranges or scalar `--chunk-column` are safe defaults. A table
without a suitable key blocks resumable execution. Explicit non-resumable,
single-pass offline handling may be designed later and must not claim resume.

Batching is independently bounded by rows and encoded bytes.

## Journal, commit, and resume

The durable state schema contains:

- migration ID, canonical plan hash, approval reference, tool version, and
  state-schema version;
- source snapshot token evidence or immutable dump manifest ID;
- source and target endpoint identities;
- source and target schema fingerprints;
- conversion policy and canonical encoding version;
- operation IDs and completion state;
- prepared and committed chunk IDs, complete start/final key tuples, row counts,
  and canonical checksums;
- staging state when that future feature exists.

State/report/log files are created with mode `0600`, exclusive creation,
regular-file checks, parent ownership checks, and symlink defenses. The state
file is a versioned append-only frame stream. Its first durable frame is the
journal genesis: it binds the migration ID, plan hash, approval reference,
endpoint identities, consistency-mode evidence, recorded capabilities, and
the accepted outage projection when present
([13](./13-throughput-and-copy-path.md)) before any target effect exists. Each
frame binds its sequence, payload digest, and previous-frame digest, and each
durable transition fsyncs the file. Resume truncates only an incomplete final
frame; internal corruption,
reordering, or deletion fails closed. The in-memory replay projection is bounded
by the reviewed operation graph, one prepared chunk, and one cursor per copied
table. It does not retain the complete chunk manifest. Implementation Phase 9's
table-level parallel copy relaxes this to one prepared chunk per table under a
bumped journal format ([13](./13-throughput-and-copy-path.md)); older journals
are rejected for automatic resume.

`--resume` does not regenerate intent. It loads state, verifies versions, plan
hash, endpoint identities, snapshot/dump identity, schema fingerprints,
conversion policy, and completed target effects. Any drift or unknown evidence
fails closed.

Before sending target writes, the runner durably records a `prepared` chunk with
its exact source snapshot identity, start and final key tuples, row count,
canonical digest, and target transaction intent. It then writes and commits the
target transaction. Only after commit acknowledgement does it durably transition
that same record to `committed`. Initial creation fsyncs the file and parent
directory. Later append transitions fsync the journal file.

A disconnect at commit leaves a durable `prepared` record and is ambiguous:
open a fresh readable target session, compare the complete chunk key interval
and canonical payload with that prepared source record, then either mark it
committed or retry. If equality cannot be proven, stop for manual reconciliation.
Plain `INSERT` is used. Any conflict is fatal unless the runner locates the
actual conflicting target row and proves equality of its complete canonical
payload and complete selected key tuple. PK existence alone is insufficient,
and a secondary unique-key collision with a different key is always fatal.

## Canonical verification

Verification reads the same source snapshot or immutable dump and the exact
committed chunk manifest used by import. It compares expected post-conversion
source rows to readable target rows in key order.

Verification also proves complete target coverage. It compares target namespace
and table inventory with the plan, verifies empty source tables remain empty,
checks before-first and after-last keyspace boundaries, and scans every committed
interval without gaps. Target-only tables or rows, including rows outside source
key intervals, fail verification.

The canonical format is framed and versioned. Each row contains table identity,
ordered column identity, key tuple, value type tag, length, and payload. It
defines:

- a distinct NULL tag;
- Unicode strings as valid Unicode with a specified normalization policy (the
  default preserves code points; no locale folding);
- exact bytes with no text conversion;
- signed and unsigned integer magnitude;
- decimal coefficient and scale with a documented equality policy;
- float width and IEEE bits, with explicit canonical forms for NaN, signed zero,
  and infinities;
- date/time fields, timezone semantics, offset, and declared precision;
- JSON parsed and serialized with decoded object keys in Unicode order,
  preserved array order, exact coefficient-and-exponent number normalization,
  and duplicate-key rejection before canonical framing;
- catalog column order and complete key tuples.

Exact test vectors version this format. XXH3 may be used later as a fast
non-cryptographic integrity checksum, never as a security proof. A cryptographic
digest remains the durable default.

Rowcount is gross progress only. It never authorizes cutover or successful
completion. Missing, extra, changed, skipped, coerced, truncated, or replaced
rows fail default execution. An explicit approved transformation is recorded in
the plan and produces `completed_with_approved_transformations`, distinct from
exact `completed`. There is no successful status with unexplained skips.

## Finalize and cancellation

Finalize writes the signed-off report and marks completion only after strict
data checks, FK anti-joins, database constraint validation, and final schema
fingerprinting pass. It does not cut application traffic over.

On cancellation, stop fetching, cancel the active statement through the control
session, roll back the active chunk, and leave the prior durable boundary
resumable. Never flush buffered rows after cancellation.

## Deferred destructive and online capabilities

Warm targets, staging swaps, automatic rollback, narrowing, and MySQL-only
same-server `INSERT ... SELECT` are later capabilities. Each needs ownership
proof, a durable state machine, write-fence gates, and crash recovery tests.
MySQL `RENAME TABLE` is atomic as a statement but can wait for metadata locks;
it is not a zero-downtime guarantee. Inverse DDL is not data rollback.

Parallelism requires shareable snapshots and follows sequential correctness.
Online migration requires CDC and is outside this executor.

## Security contract

- Authenticated TLS and hostname verification are defaults; CA and mTLS are
  explicit. Insecure mode is conspicuous and audited.
- Source/target credentials are separate. Source read-only is database-enforced.
- Typed identifiers are quoted by the adapter and values are bound parameters.
- Production logs contain no row values, SQL literals, credentials, connection
  strings, or arbitrary SQL. Reports use endpoint identities without secrets.
- State, logs, and reports are protected as described above.
- MVP has local stderr/file alerts only. It does not call arbitrary webhooks.
