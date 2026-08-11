# PostgreSQL First-Adapter Decision

## Decision

PostgreSQL is the first live dialect for enterprise migration plan-only work.
The initial adapter is read-only. It connects to the source and target with
authenticated TLS, opens `REPEATABLE READ READ ONLY` transactions, extracts
catalog evidence, reports unsupported semantics, and writes a deterministic
reviewed plan. It does not create or modify database objects or rows.

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

The ignored `migration_postgres_plan_test` exercises this matrix against
TLS-enabled source and empty target databases. It checks byte-for-byte plan
determinism, embedded catalog evidence, and protected artifact loading. This is
spike evidence, not a production support statement.

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

## Plan-only boundary

The adapter inventories ordinary and partitioned tables, columns, sequences,
views, materialized views, constraints, indexes, routines, triggers, row-level
security, and policies. Unsupported semantics remain visible in the plan and
block later execution. Plan generation can succeed so reviewers can inspect
the complete report.

The adapter does not yet prove:

- target emptiness or migration ownership;
- table key suitability and row pagination;
- data-copy snapshot lifetime;
- DDL rendering or execution;
- foreign-key anti-joins and constraint validation;
- crash recovery or full resume;
- real-engine acceptance tests in CI.

These items remain required by
[08](./08-implementation-prerequisites.md) before an implementation phase is
complete.
