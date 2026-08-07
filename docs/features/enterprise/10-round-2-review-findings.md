# Round-2 Review Resolution Ledger

| Finding                                             | Status             | Resolution                                                                                                                      |
| --------------------------------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| FK self/cycle handling was unsafe                   | Resolved in design | Tables omit FKs during copy; [04](./04-execution-design.md#ddl-and-foreign-keys) requires complete anti-joins and validation    |
| FK and strict-mode disabling could alter data       | Resolved           | Privileged relaxation is not a beta default                                                                                     |
| Snapshot lifecycle was absent                       | Resolved in design | [03](./03-connection-architecture.md#snapshot-lifecycle-and-consistency)                                                        |
| TLS was optional and credentials appeared in URLs   | Resolved           | [03 configuration](./03-connection-architecture.md#configuration) and [04 security](./04-execution-design.md#security-contract) |
| Trace logs exposed row values and SQL               | Resolved           | Production logs prohibit row values, keys, literals, credentials, and arbitrary SQL                                             |
| Identifier interpolation was unsafe                 | Resolved           | Typed identifiers and bound values are required by [03](./03-connection-architecture.md#factories-and-sessions)                 |
| Shared target could be modified or dropped          | Resolved           | Empty migration-owned, create-only target is mandatory                                                                          |
| Driver concurrency assumptions were wrong           | Resolved           | Factories are shareable; mutable sessions are not required to be `Send + Sync`                                                  |
| Commit ambiguity and crash recovery were incomplete | Resolved in design | [04](./04-execution-design.md#journal-commit-and-resume); kill/crash tests remain mandatory                                     |
| Performance estimates were presented as facts       | Resolved           | Product ranges replace line/throughput claims; benchmarks remain environment-specific                                           |
| Remote disk availability was inferred incorrectly   | Resolved           | [Observability](./observability-and-operations.md#metric-provenance) reports provenance and unknown values                      |

The implementation and real-engine evidence remain open until the acceptance
gates in [08](./08-implementation-prerequisites.md) pass.
