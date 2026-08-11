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
under an exactly re-attested write fence. It is not production-ready. There is
no foreign-key installation or complete crash-reconciliation matrix. Sequences, generated
columns, user-defined types, extensions, and other unsupported semantics fail
before DDL.

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
rechecks, identity-value insertion, and exact binary-protocol round trips for
text, bytes, floats, JSONB, numeric, and temporal values. It also executes and
strictly finalizes a three-row reviewed plan. The write-fence test installs
database-side DML and DDL guards, exits the installer, restarts PostgreSQL,
re-attests exact object identities and guard definitions, executes into a
separate target, and releases the fence after durable completion. This is spike
evidence, not a production support statement. The same matrix injects an
interruption after a durable committed chunk, resumes from the recorded key,
re-runs exact final row verification, and proves that no row is duplicated.

## Configuration and security

Endpoint files contain host, port, database, user, a credential environment
variable name, TLS options, and a connection timeout. They do not contain a
password or connection URL. Certificate and hostname verification are enabled
by default. An explicit `tls.insecure = true` setting is recorded as an
insecure capability in the plan.

The first connector supports platform roots and an additional PEM CA
certificate. Client-certificate authentication is deferred and must be added
before an environment that requires mTLS can be supported.

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
insecure = false
```

## Current boundary

The adapter inventories ordinary and partitioned tables, columns, sequences,
views, materialized views, constraints, indexes, routines, triggers, row-level
security, and policies. Unsupported semantics remain visible in the plan and
block later execution. Plan generation can succeed so reviewers can inspect
the complete report.

The adapter does not yet prove:

- complete table-key suitability and pagination matrices;
- resume coverage at every DDL, chunk, verification, and release boundary;
- fence generation rollover and the complete fence failure-injection matrix;
- post-data indexes and broader DDL coverage;
- foreign-key anti-joins and constraint validation;
- crash recovery or full resume;
- complete real-engine acceptance matrices in CI.

These items remain required by
[08](./08-implementation-prerequisites.md) before an implementation phase is
complete.
