# sql-splitter Capability Audit

## Existing strengths

sql-splitter already has streaming SQL file parsing, dialect detection, schema
models and graphs, row parsing for MySQL INSERT and PostgreSQL COPY, bounded
transformation plumbing, diagnostics, and several file-based analysis and
conversion commands. These are useful foundations, not live-migration support.

## Required gaps

| Gap              | Required design                                                                            |
| ---------------- | ------------------------------------------------------------------------------------------ |
| Live connections | Factories and isolated mutable sessions from [03](./03-connection-architecture.md)         |
| Catalog fidelity | Exact vendor catalogs and an unsupported-object report                                     |
| Typed rows       | `DbValue`, `ColumnMeta`, and row-and-byte bounded `RowBatch`                               |
| Planning         | Versioned immutable plan, operation IDs, capabilities, fingerprints, and hash              |
| Consistency      | Write-fence evidence or one native snapshot through copy and verification                  |
| Execution        | Empty-target create-only DDL and plain INSERT transactions                                 |
| Resume           | Durable post-commit chunk journal and fail-closed drift checks                             |
| Verification     | Versioned canonical expected-value comparison on the same manifest                         |
| Security         | Verified TLS, protected credential references, identifier/value safety, and redacted files |
| Operations       | Capability-classified metrics and honest unknown values                                    |

The current portable schema is not sufficient as an exact vendor catalog.
Statement boundary detection also does not imply complete classification and
semantic modeling of routines, views, triggers, events, or partitions.

`TypeMapper` rewrites SQL type text. Making it public does not create runtime
value conversion. Cross-dialect work starts only after the canonical value,
metadata, conversion-policy, and verification contracts exist.

## Reuse boundaries

File parser events can feed immutable dump inputs when the dump has a complete
manifest. Live driver rows do not need to be rendered as mysqldump SQL and
parsed again. Database adapters should map protocol values directly to
`DbValue` while preserving raw metadata.

The current schema graph can support plan dependency analysis. It must not be
used to claim FK-safe row ordering. Initial target tables omit FKs; complete
anti-joins and database validation happen after copy.

## Scope conclusion

Plan-only is a substantial product increment. Sequential same-dialect execution
adds consistency, target transactions, resume, security, and exact verification.
Parallelism and cross-dialect conversion are later increments, not small
extensions. See [08](./08-implementation-prerequisites.md) for estimates and
acceptance gates.
