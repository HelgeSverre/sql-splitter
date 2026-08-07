# Consolidated Review Findings and Decisions

The earlier reviews found unsafe assumptions. The design now applies these
decisions:

| Finding                                               | Decision                                                                      | Applied in                                                                                   |
| ----------------------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Deduplication can hide different payloads             | Plain INSERT; conflict fatal unless full canonical equality is proven         | [04: journal and resume](./04-execution-design.md#journal-commit-and-resume)                 |
| Runtime conversion was confused with type SQL mapping | Canonical `DbValue` and `ColumnMeta` precede `RowTypeConverter`               | [03: canonical values](./03-connection-architecture.md#canonical-values-and-bounded-batches) |
| Session relaxation can silently alter data            | No default FK/unique/sql-mode/trigger relaxation                              | [04: DDL and FKs](./04-execution-design.md#ddl-and-foreign-keys)                             |
| Live reads lacked one consistent point                | Write fence or one native snapshot is mandatory                               | [03: snapshot lifecycle](./03-connection-architecture.md#snapshot-lifecycle-and-consistency) |
| Range chunking omitted or duplicated keys             | Full-tuple lexicographic keyset pagination                                    | [04: keyset](./04-execution-design.md#keyset-pagination)                                     |
| Self-FK ordering was insufficient                     | Load without FKs, anti-join all relationships, then add and validate          | [04: DDL and FKs](./04-execution-design.md#ddl-and-foreign-keys)                             |
| Verification could bless skips                        | No success with skips; canonical expected-value comparison                    | [04: verification](./04-execution-design.md#canonical-verification)                          |
| Target mutation scope was broad                       | Empty migration-owned target and create-only beta                             | [README](./README.md#status-and-product-boundary)                                            |
| TLS and logging were unsafe                           | Verified TLS, protected secret references, no production row/SQL values       | [04: security](./04-execution-design.md#security-contract)                                   |
| PlanetScale paths conflicted                          | Immutable completed dump input only in core CLI                               | [03: PlanetScale](./03-connection-architecture.md#planetscale-input)                         |
| Runtime stage numbers conflicted with delivery        | Descriptive runtime stages; numbered terms reserved for Implementation Phases | [README](./README.md#runtime-stages)                                                         |

Performance figures from case studies remain research context only. They do not
predict this executor. XXH3 is non-cryptographic. Rowcount is progress only.
Missing rows, type coercion, replacement characters, truncation, and skipped
rows are failures unless an exact approved transformation is in the plan; such
a run has a distinct completion status.

Remaining open design work is tracked as later Implementation Phases in
[08](./08-implementation-prerequisites.md), not as beta behavior.
