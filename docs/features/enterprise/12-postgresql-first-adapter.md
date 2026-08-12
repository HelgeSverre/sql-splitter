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
The write-fence path supports logged `smallint`, `integer`, and `bigint`
sequences, including standalone sequences, `serial` ownership, and `GENERATED
ALWAYS` or `BY DEFAULT` identity ownership. It records exact configuration and
`last_value`/`is_called`, copies explicit identity values, and restores each
target sequence through a durable prepared operation. Other generated columns,
user-defined types, extensions, and unsupported semantics fail before DDL.
The same-major PostgreSQL path also supports stored generated columns over
built-in types when every expression dependency is an immutable `pg_catalog`
function, operator, type, or deterministic current-version collation, or
a non-generated column of the same table. The journal binds complete rows,
including generated values. Target inserts omit generated columns, PostgreSQL
recomputes them, and reconciliation compares the complete recomputed row.
The post-data path supports a conservative programmable-object subset. Ordinary
views use a validated typed `CREATE VIEW` AST. SQL scalar functions must use a
parsed SQL-standard `RETURN` body and be immutable, strict, parallel safe,
security invoker, non-leakproof, and free of custom configuration or overloads.
All relation, function, type, operator, collation, namespace, and language
dependencies are resolved through `pg_depend` into typed identities. Raw or
dollar-quoted bodies, unresolved dependencies, materialized views, custom
privileges, and view column privileges fail closed.

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

The same opt-in matrix injects two journal publication failures after the target
commit. One writes and syncs only a strict prefix of the committed frame before
returning `ENOSPC`; resume truncates the tail and reconciles the durable prepared
intent. The other writes and syncs the complete frame but loses the sync
acknowledgement; resume accepts the validated committed frame and does not replay
the target transaction. The Docker harness checks the actual Cargo target,
journal directory, and PostgreSQL container filesystems before loading fixtures.

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
form is an explicit execution blocker. Ordinary non-unique indexes use the same
conservative plain-column B-tree subset. They are created after data copy with a
durable prepared operation before transactional `CREATE INDEX`. Resume accepts
only absence or an exact semantic match; any different observed index requires
manual reconciliation.

PostgreSQL sequence state is outside table MVCC. Write-fence execution remains
the database-enforced path. Implementation Phase 5b also admits `CACHE 1`
sequences for the reviewed external-quiesce profile through the start/end
state-equality and optional full-source re-scan contract in
[14](./14-managed-source-profiles.md). That local PostgreSQL 15–17 matrix is
complete. The Amazon RDS PostgreSQL 16.14 matrix proved the data and sequence
contracts, but a later killed-process test reopened the managed-provider exit
gate because the original CLI could not create or renew the required
external-quiesce artifact. The implemented renewal workflow and pending RDS
revalidation are recorded in
[14](./14-managed-source-profiles.md#recorded-provider-evidence). Fence installation
transfers sequence ownership to the administrator, removes effective `USAGE`
and `UPDATE` from non-superuser login roles, terminates old sessions that can
hold cached values, and records the post-drain state. Resume re-attests this
state and the exact ownership link.
Release restores the original owner and effective ACL. The recovery matrix
stops before and after `setval`, distinguishes the exact initial and desired
states, and requires manual reconciliation for any third state. PostgreSQL 15,
16, and 17 pass this sequence matrix for descending/cyclic, never-called,
identity, and serial sequences.

The generated-column matrix covers a generated column in the middle of the
physical column order, NULL propagation, negative values, prepared-before-write
recovery, applied-commit reconciliation, and target base-row tampering. A
generated pagination key, cross-major migration, mutable or external function,
unknown dependency, generated-on-generated reference, and unsupported mode all
fail closed. PostgreSQL 15, 16, and 17 pass this matrix.

The adapter supports a conservative declarative partition subset: one logged
partitioned root, one built-in integer partition key, one level of logged
ordinary leaves, and typed `RANGE`, `LIST`, `HASH`, or `DEFAULT` bounds. It
creates the root and every leaf in one durable pre-data intent and copies data
exactly once through the root so PostgreSQL performs routing. The write fence
guards the root and every leaf and attests the exact parent, strategy, key, and
bound topology. Final verification compares the logical root and pages every
source and target leaf through `ONLY`, including empty leaves. Expression and
multi-column keys, subpartitions, custom leaf objects/storage, traditional
inheritance, and non-integer bounds fail closed. PostgreSQL 15, 16, and 17 pass
the range/list/hash DDL and resume matrix.

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

Execute and resume install a process `SIGINT` handler on Unix and share one
cancellation token across catalog reads, source paging, target writes,
reconciliation, and verification. Cancellation uses PostgreSQL cancel requests
against the current source and target sessions. An interrupted target chunk is
rolled back explicitly and remains `Prepared` in the durable journal. Resume
then reconciles or retries that exact intent. The live acceptance test observes
an active target INSERT transaction, cancels it, proves that no rows committed,
and resumes 50,000 rows without omission or duplication. A second case blocks
`CREATE INDEX` on a database lock, cancels the active DDL through its control
session, proves that PostgreSQL rolled the index transaction back, and resumes
the same prepared index operation.

The execution state is a protected append-only journal rather than a rewritten
JSON snapshot. Resume replays a hash-linked frame stream into bounded operation
and cursor state. Final verification scans committed chunks once across all
tables, so journal processing is linear in the number of durable events. A
100,000-event replay test verifies the bounded projection. The older spike JSON
state format does not contain the independent table and schema evidence required
by this format and is therefore rejected for automatic resume.

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

The spike also implements the initial source-only assessment contract from
[15](./15-assessment-product.md): one `REPEATABLE READ READ ONLY` transaction,
typed target-not-assessed state, protected JSON and Markdown artifacts, scope
estimates isolated from the stable report body, and an execution-plan type gate
that rejects assessment artifacts. Focused live tests pass on PostgreSQL 15–17.
The current PostgreSQL 15–17 assessment contract passes the statement-audit,
source-only CLI, deterministic-report, artifact-protection, and exhaustive
supported-version blocking-code gates. The command remains feature-gated and
experimental while product packaging and release work remain incomplete.

### Extensions

Extensions remain blocking today. The admission contract for lifting one
specific extension is: the plan records its name, version, and schema; target
preflight proves availability at a compatible version through
`pg_available_extensions`; execution creates the extension with an explicit
version and schema inside the pre-data intent; objects owned by the extension
(per `pg_depend` membership) are excluded from create-only DDL and attested
after creation; and the behavior is proven per extension, per PostgreSQL
version, by the live matrix. The allowlist starts empty. Any extension
without a proven entry fails closed. There is no blanket extension support
claim.

### Privileges (ACLs)

The beta does not migrate grants, and this is no longer silent. Namespace,
relation, routine, and default-privilege findings are elevated from
non-blocking notes to one mandatory reviewed capability, `acl.report_only`.
It appears in the plan and in the assessment report
([15](./15-assessment-product.md)). Approving the plan through the existing
`--approval-ref` review acknowledges every recorded capability, including
this one, and the approval reference is durably bound at journal genesis
([04](./04-execution-design.md)). Migrating a
conservative grant subset is future work; silent omission is removed.

The adapter does not yet prove:

- complete live rejection and drift matrices for unsupported index forms;
- the complete fence failure-injection matrix, including legacy storage upgrades;
- broader post-data DDL coverage;
- the full sequence type, bound, cache, cycle, ACL, cancellation, and server-
  restart acceptance matrix beyond the current exact recovery cases;
- the full generated-column expression, collation, hostile-identifier, large-
  value, cancellation, and network-response-loss matrix beyond the current
  exact recovery cases;
- composite and expression partition keys, broader bound types, subpartitions,
  and the complete partition topology tamper/cancellation/network-loss matrix;
- unsupported foreign-key variants beyond the explicitly modeled subset;
- materialized views and programmable objects outside the strict typed ordinary-
  view and immutable SQL scalar-function subset;
- complete programmable-object collision, privilege, cancellation, and network-
  response-loss matrices beyond the current typed identity and journal recovery
  cases;
- complete real-engine acceptance matrices in CI.

These items remain required by
[08](./08-implementation-prerequisites.md) before an implementation phase is
complete.
