# Database Connection Architecture

This is the connection contract for the product boundary in the
[README](./README.md). It does not use shared `DbSource` or `DbTarget` objects.

## Configuration

`EndpointConfig` contains a dialect, host, port, database, and an endpoint
identity that excludes secrets. `TlsConfig` defaults to authenticated TLS with
hostname verification and supports an explicit CA bundle and mTLS certificate
and key. An insecure mode requires an explicit flag, warning, and audit record.

`CredentialRef` refers to a protected environment variable, file descriptor,
OS secret store, or interactive prompt. Source and target references must be
separate. Password-bearing URLs and command-line password values are rejected.
The database source role must have server-enforced read-only permissions; an API
without write methods is not sufficient enforcement.

`CapabilitySet` is probed and recorded in the plan. It includes snapshot,
transaction, DDL atomicity, cursor/COPY, cancellation, identifier, parameter,
and catalog capabilities. Unknown capability values fail closed when required.

## Factories and sessions

```rust
trait SourceConnectionFactory: Send + Sync {
    fn open_catalog(&self) -> Result<Box<dyn CatalogSession>>;
    fn open_reader(&self, snapshot: &SnapshotToken) -> Result<Box<dyn ReadSession>>;
    fn open_control(&self) -> Result<Box<dyn ControlSession>>;
}

trait TargetConnectionFactory: Send + Sync {
    fn open_writer(&self) -> Result<Box<dyn WriteSession>>;
    fn open_verifier(&self) -> Result<Box<dyn VerificationSession>>;
    fn open_control(&self) -> Result<Box<dyn ControlSession>>;
}
```

Factories can be shared. Per-session traits are mutable and need not implement
`Send` or `Sync`; a session belongs to one worker or runner thread. A separate
control connection performs driver-native cancellation. Cancellation asks the
active chunk transaction to roll back. It never flushes a partial batch.

`ReadSession` accepts typed `SelectPage` requests, not SQL fragments:

```rust
struct QualifiedTable { namespace: Identifier, name: Identifier }
struct KeyTuple(Vec<DbValue>);
struct KeysetPage {
    table: QualifiedTable,
    projection: Vec<Identifier>,
    key: Vec<Identifier>,
    after: Option<KeyTuple>,
    limit: u32,
}
```

The dialect renderer quotes typed identifiers and binds values. Raw identifiers,
row values, and predicates are never interpolated. The target verifier is
readable and supports the same typed keyset request for canonical comparison.

## Canonical values and bounded batches

This model is a prerequisite to `RowTypeConverter`:

```rust
enum DbValue {
    Null,
    Bool(bool),
    Signed(i128), Unsigned(u128), Decimal { coefficient: Vec<u8>, scale: i32 },
    Float32(u32), Float64(u64),
    Text(String), Bytes(Vec<u8>), Json(Vec<u8>),
    Date { year: i32, month: u8, day: u8 },
    Time { nanos: i128 },
    Timestamp { local: String, offset_minutes: Option<i16>, precision: u8 },
    Vendor { type_id: String, format: ValueFormat, bytes: Vec<u8> },
}

struct ColumnMeta {
    name: Identifier,
    ordinal: u32,
    vendor_type: String,
    nullable: bool,
    collation: Option<String>,
    precision: Option<u32>,
    scale: Option<i32>,
    timezone_semantics: Option<String>,
}

struct RowBatch {
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<DbValue>>,
    encoded_bytes: usize,
}
```

A batch stops at both configured row and byte limits. One row above the byte
limit gets a dedicated bounded-large-value path or fails before target writes.
Raw source metadata is retained until conversion and verification finish.

`TypeMapper` maps and rewrites SQL type declarations. It cannot convert runtime
values. A later cross-dialect `RowTypeConverter` consumes `DbValue`, source and
target `ColumnMeta`, and an explicit conversion policy.

## Exact vendor catalog

Do not synthesize DDL from the portable schema and then treat it as authoritative.
Each adapter produces an exact, versioned vendor catalog that preserves:

- database/schema namespaces, quoted names, owner, charset, and collation;
- column order, defaults, generated expressions, identity and sequence details;
- primary, unique, check, and foreign keys, including match and update/delete
  actions, deferrability, and validation state;
- index access method, included columns, expressions, predicates, ordering,
  prefix lengths, visibility, and vendor options;
- views, routines, triggers, events, partitions, tablespaces, and dependencies.

Every unrepresented or unsupported object enters an `UnsupportedObjectReport`.
Execution is blocked unless the report is empty for required semantics. Plan-only
may report unsupported objects without executing.

## Snapshot lifecycle and consistency

`SnapshotToken` is opaque, dialect-specific, and bound to endpoint identity,
database identity, snapshot mode, and lifecycle. Plan-only catalog inspection
does not keep a snapshot open across human review. A new execute run acquires
fresh consistency evidence before its first target write, fingerprints the
source again, and requires an exact match with the reviewed plan. The runner
then binds the execution snapshot or write-fence evidence to the journal and
keeps it valid through source-side verification.

MVP modes are:

- `write-fence`: the operator quiesces writes and provides an acknowledgement;
  preflight verifies available database evidence and records the fence window.
- `consistent-snapshot`: one session owns one native snapshot for the sequential
  extraction and verification lifecycle.

Write-fence acknowledgement is supplied at execution, not plan creation. The
operator must prove that the fence remains continuous from execution preflight
through verification. Resume is permitted only while the same fence is still
continuously valid. A fence gap invalidates the migration; a new acknowledgement
cannot continue old execution state.

**MySQL sequential contract:** use a dedicated transaction with a consistent
snapshot at a supported isolation level, establish it before reads, keep the
same session alive, and reject DDL or connection loss. Record server identity,
version, transaction settings, and a position/GTID observation as evidence, not
as a substitute for the snapshot. Every copied table must use a supported
transactional storage engine. Nontransactional, unknown, or mixed-engine tables
block consistent-snapshot mode. Preflight records the exact catalog fingerprint
and a database-level DDL exclusion mechanism or operator-enforced DDL fence.
Catalog fingerprints are checked again before finalize; unknown DDL-exclusion
evidence fails closed.

**PostgreSQL sequential contract:** start one read-only `REPEATABLE READ`
transaction and perform all source-side catalog, data, and verification reads in
that transaction. Target verification uses a separate target verification
session. Record server/database identity and transaction snapshot evidence. Long
snapshot effects are reported during preflight. Sequence state and other values
outside normal table MVCC semantics require a continuous write fence or an
explicit adapter rule; otherwise the affected object is unsupported.

Parallel extraction is deferred. It requires PostgreSQL exported snapshots,
an equivalent proven shareable mechanism, or a stopped replica. MySQL snapshot
sharing must be proven for the selected driver/server; independent transactions
are not equivalent.

## Driver facts

- The synchronous `mysql` crate provides `Pool`. Text protocol returns typed
  protocol fields; it is not mysqldump SQL text. `exec_batch` repeatedly executes
  a statement for parameter sets; it is not one multi-row insert.
- `postgres::Client` is mutable and not `Sync`. `query()` collects rows.
  `query_raw`, server cursors, and `COPY` have different streaming, transaction,
  and buffering contracts and require focused tests.
- PostgreSQL TLS requires a connector dependency such as
  `postgres-native-tls` or `postgres-openssl`; `NoTls` is not production-safe.
- PostgreSQL `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block
  and has invalid-index recovery rules. MySQL DDL can implicitly commit. These
  are explicit adapter capabilities, not hidden transaction behavior.

## PlanetScale input

The core CLI accepts PlanetScale data only as an immutable, completed dump with
a manifest identity and checksums. It does not query live VTGate, spawn `pscale`,
accept `pscale://`, or handle PlanetScale service tokens. A managed-service
subprocess exception is described only in [07](./07-managed-service-appendix.md)
and is deferred.
