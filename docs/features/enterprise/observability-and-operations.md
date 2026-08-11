# Observability and Operations

Observability supports the offline sequential boundary. It does not claim root
cause from correlation, promise fixed detection latency, or authorize cutover.

## Metric provenance

Every metric records one capability class:

| Class                 | Examples                                                                                 | Availability                                          |
| --------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `runner_local`        | elapsed time, rows/bytes read and committed, batch memory, retries, journal age          | Available from the process                            |
| `sql_derived`         | database version, current schema fingerprint, used database/table size, session settings | Available only when query and privilege are supported |
| `privileged_sql`      | server-wide connection/activity counters, replication or WAL/binlog settings             | Optional; lack of privilege is `unknown`              |
| `provider_api`        | volume free capacity, autosuspend configuration, service events                          | Deferred adapter/service capability                   |
| `external_monitoring` | host disk, CPU, IOPS, network, application write rate                                    | Supplied by operator systems, not inferred by the CLI |

Remote database free disk is generally unknown. PostgreSQL
`pg_database_size()` reports used size, not free space. MySQL table free-space
fields do not prove volume capacity. Reducing batch size can reduce runner memory
or transaction size; it does not free target disk. Unknown never reports pass.

## Runtime-stage events

Events use the descriptive stages:

`preflight → schema extraction → plan → pre-data DDL → data copy → post-data DDL → verification → finalize`

Each event includes timestamp, migration ID, plan hash, stage, operation ID,
table identity when applicable, severity, diagnostic code, and safe numeric
fields. Production events exclude credentials, connection strings, row values,
key values, SQL literals, and arbitrary SQL. Endpoint identities are redacted
stable labels.

State, logs, plans, and reports use protected regular files with mode `0600`,
exclusive creation, ownership checks, symlink defenses, and atomic durable
updates as specified in [04](./04-execution-design.md#journal-commit-and-resume).

## Progress and health

Rowcount and bytes are gross progress. They are not integrity checks. A useful
progress record contains committed chunks, complete tables, current stage,
elapsed time, recent measured throughput, and unknown ETA when there is not
enough stable evidence. No fixed 500 ms refresh or alert promise is made.

The runner can state direct observations such as “target commit latency rose” or
“source read returned timeout.” It must not state an unsupported cause such as
“target disk pressure caused the timeout.” Suggested checks are clearly labeled
as suggestions.

## Operator surface for the beta

The beta ships with a minimum operator surface. Wizards and self-serve flows
remain future work, but they must consume the same versioned data rather than
issuing new queries.

- **Progress.** During data copy and verification, the runner emits the
  progress record above per completed chunk to stderr, and appends the same
  record as JSON lines to a protected progress file, so a later UI needs no
  new privileges and no database access.
- **`monitor`.** A read-only subcommand renders current stage, per-table
  committed chunks, and journal age from the journal snapshot projection. It
  works during a live run and after a crash, and never opens a database
  connection.
- **`abort`.** A request to the running process — the journal is a
  hash-linked single-writer stream, so a second process never appends to it.
  The runner records the operator-intent event (a new event class, which
  bumps the journal format version) and performs the existing cooperative
  cancellation: statement cancel, active chunk rollback. It never releases a
  fence, never finalizes, and leaves the run in a resumable or
  manual-reconciliation state. Fence release remains its own explicit verb.
- **Reports.** The human-readable assessment and plan report is specified in
  [15](./15-assessment-product.md). The final report below is unchanged.

## Alerts in the initial CLI

Initial alert delivery is local stderr plus the protected structured log/report.
There are no arbitrary webhooks. Alerts include:

- snapshot or write-fence validity lost;
- endpoint identity, plan, catalog, or state drift;
- active operation failure or cancellation;
- ambiguous commit requiring reconciliation;
- no committed progress for a configurable observation interval;
- source/target disconnect or TLS/authentication failure;
- local state filesystem write/fsync failure;
- target conflict or canonical verification mismatch;
- any skipped, coerced, truncated, or replaced value;
- FK anti-join or database constraint validation failure;
- required capability changed to unknown or unsupported.

Thresholds are configuration, not universal health facts. Alerts do not skip
verification, lower strictness, or continue after an integrity failure.

## Final report

The final report records product mode, completion status, tool/state/canonical
versions, migration and plan IDs, endpoint identities, snapshot or dump manifest
identity, catalog fingerprints, consistency evidence, operation and chunk
counts, complete canonical verification results, FK validation results,
approved transformations, warnings, and metric provenance.

Valid successful statuses are:

- `completed`: exact required semantics and values matched;
- `completed_with_approved_transformations`: every difference was explicitly in
  the immutable plan and matched expected post-conversion values.

Missing or extra rows, unexplained differences, skips, truncation, coercion,
replacement, unknown required evidence, or incomplete constraints are failures.
Rowcount alone cannot produce a successful status.

## Operational recovery by stage

| Stage                                | Safe response                                                                           |
| ------------------------------------ | --------------------------------------------------------------------------------------- |
| preflight / schema extraction / plan | Stop; no target write should exist                                                      |
| pre-data DDL                         | Stop and reconcile journal with create-only target effects                              |
| data copy                            | Cancel active statement, roll back active chunk, resume from last durable committed key |
| post-data DDL                        | Stop and compare operation state with exact target catalog; do not infer rollback       |
| verification                         | Preserve the same snapshot and manifest; resume only recorded verification work         |
| finalize                             | Re-run final gates and durable report publication; do not cut traffic automatically     |

Cancellation never flushes a partial batch. A lost source snapshot cannot be
recreated and called the same migration; the run fails and needs a new plan/state.

Provider metrics, OpenTelemetry, callback plugins, PagerDuty/Slack delivery, and
managed-service dashboards are future Implementation Phase work.
