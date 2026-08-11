# PostgreSQL First-Adapter Decision

## Decision

PostgreSQL is the first live dialect for enterprise migration work. The adapter
can extract a deterministic plan and now has internal source-reader and target-
writer contracts. The source reader owns one `REPEATABLE READ READ ONLY`
transaction across bounded keyset pages and supports driver-native
cancellation. The target writer uses plain parameterized `INSERT` statements
inside an explicit transaction and a separate read-only verification session.
The target adapter also proves database ownership and emptiness immediately
before an atomic create-only transaction for supported namespaces, ordinary
tables, columns, primary keys, unique constraints, and check constraints.

The feature-gated spike CLI can execute a reviewed same-dialect plan for the
supported table subset. It durably records operation and chunk transitions,
uses bounded keyset pages, and performs exact source/target verification before
finalization. It can resume the implemented table subset from protected state
under an exactly re-attested write fence. It checks, adds, and validates the
supported PostgreSQL foreign-key subset after data verification. This includes
composite, nullable, self-referencing, and cyclic relationships, exact column
order, `MATCH SIMPLE` and `MATCH FULL`, referential actions, and deferrability.
Targeted PostgreSQL 15+ `ON DELETE SET NULL/DEFAULT (column-list)` semantics
fail closed because they are not modeled. It is not production-ready.
Sequences, generated columns, user-defined types, extensions, and other
unsupported semantics fail before DDL.

## Reasons

- One transaction can bind catalog extraction to a repeatable, read-only
  snapshot.
- PostgreSQL exposes typed catalog metadata and canonical definition functions
  such as `pg_get_constraintdef`, `pg_get_indexdef`, `pg_get_functiondef`, and
  `pg_get_triggerdef`.
- The server enforces the transaction's read-only access mode.
- The same driver API can later own table reads and source verification for the
  lifetime of one snapshot.

MySQL follows after this contract works against real PostgreSQL versions. A
MySQL adapter must also prove storage-engine and DDL consistency behavior.

## Initial support matrix

The spike targets PostgreSQL 15, 16, and 17 over TCP with TLS. Each supported
version must pass plan determinism and catalog coverage tests before it becomes
part of a production support statement. PostgreSQL 18 is not included until
its catalog behavior is tested.

The ignored `migration_postgres_plan_test` exercises plan determinism against
TLS-enabled source and empty target databases. The reproducible
`just migration-postgres-live <version>` test creates a disposable TLS container
and verifies snapshot stability during concurrent writes, native query
cancellation, source-role write rejection, create-only DDL and empty-target
rechecks, trusted client-certificate authentication, missing-client-certificate
and wrong-CA rejection, identity-value insertion, and exact binary-protocol round trips for
text, bytes, floats, JSONB, numeric, and temporal values. It also executes and
strictly finalizes a three-row reviewed plan. The write-fence test installs
database-side DML and DDL guards, exits the installer, restarts PostgreSQL,
re-attests exact object identities and guard definitions, executes into a
separate target, and releases the fence after durable completion. This is spike
evidence, not a production support statement. The same matrix injects an
interruption after a durable committed chunk, resumes from the recorded key,
re-runs exact final row verification, and proves that no row is duplicated. An
additional matrix uses the non-default `migration-fault-injection` test feature
and isolated databases to stop after prepared DDL, committed DDL, prepared data,
an applied commit with a lost acknowledgement, complete verification, and fence
release. Every case must resume to the exact expected rows and verified journal.
The network matrix passes the TLS stream through a test-only TCP proxy. One case
drops the COMMIT bytes before PostgreSQL receives them. A second case forwards
COMMIT, proves the rows through a separate direct connection, and drops the
server response. Both cases retain the same durable prepared chunk and recover
without duplicate rows. The fence matrix also releases one generation, installs
a new generation without deleting prior history, and rejects old evidence.
The foreign-key matrix also stops before and after constraint commit, resumes,
and compares exact PostgreSQL catalog metadata and internal constraint-trigger
state. A negative live case proves composite anti-join detection and persists a
manual-reconciliation state for a conflicting target constraint. The separate
feature is not part of the normal spike API. PostgreSQL 15, 16, and 17 pass this
matrix.

The key-pagination matrix validates every read request against an exact,
validated, non-null primary or unique constraint. It covers signed integer
extrema, repeated composite prefixes, `C`-collated Unicode text, binary prefix
keys, empty and one-row tables, exact page boundaries, nullable and non-unique
rejection, and byte-bounded page prefixes. Selected text keys bind the collation
schema, provider, deterministic flag, recorded version, actual provider version,
and PostgreSQL server version through the reviewed catalog and state
fingerprints. A provider-version mismatch blocks execution.

Tables without a key constraint can use a standalone unique index only when it
is a valid, ready, live, immediate, ordinary persistent B-tree index over one or
more non-null physical columns. The supported form has no predicate,
expressions, included columns, custom operator class or collation, ordering
override, NULLS NOT DISTINCT behavior, storage options, tablespace, clustering,
or replica-identity role. The selected key contract is stored in the reviewed
copy operation. Its typed `CREATE UNIQUE INDEX` operation is part of the atomic
pre-data intent and is re-attested before resume. Every other standalone index
form is an explicit execution blocker.

## Configuration and security

Endpoint files contain host, port, database, user, a credential environment
variable name, TLS options, and a connection timeout. They do not contain a
password or connection URL. Certificate and hostname verification are enabled
by default. An explicit `tls.insecure = true` setting is recorded as an
insecure capability in the plan.

The connector supports platform roots, an additional PEM CA certificate, and
PEM client-certificate authentication with a PKCS#8 private key. The client
certificate and key are an atomic configuration pair. On Unix, the key must be
a current-user-owned regular file with no group or world access; symlinks and
permissive modes fail before a connection. Reviewed plans bind source and target TLS
mode, trust-root certificate digest, and client-certificate digest. Fence
artifacts separately bind the administration TLS values, so execution and
resume reject authentication downgrades or certificate replacement.

Example:

```toml
host = "source-db.example.com"
port = 5432
database = "app"
user = "migration_reader"
credential_env = "SQL_SPLITTER_SOURCE_PASSWORD"
connect_timeout_seconds = 10

[tls]
ca_certificate = "/etc/sql-splitter/source-ca.pem"
client_certificate = "/etc/sql-splitter/source-client.pem"
client_private_key = "/etc/sql-splitter/source-client-key.pem"
insecure = false
```

## Current boundary

The adapter inventories ordinary and partitioned tables, columns, sequences,
views, materialized views, constraints, indexes, routines, triggers, row-level
security, and policies. Unsupported semantics remain visible in the plan and
block later execution. Plan generation can succeed so reviewers can inspect
the complete report.

The adapter does not yet prove:

- complete live rejection and drift matrices for unsupported index forms;
- the complete fence failure-injection matrix, including legacy storage upgrades;
- post-data indexes and broader DDL coverage;
- unsupported foreign-key variants beyond the explicitly modeled subset;
- complete real-engine acceptance matrices in CI.

These items remain required by
[08](./08-implementation-prerequisites.md) before an implementation phase is
complete.
