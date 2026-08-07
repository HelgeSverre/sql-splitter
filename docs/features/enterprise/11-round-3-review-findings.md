# Round-3 Review Resolution Ledger

| Finding                                                  | Status                   | Resolution                                                                                                                        |
| -------------------------------------------------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| Runtime and delivery phase numbers conflicted            | Resolved                 | Runtime uses descriptive stages; only [08](./08-implementation-prerequisites.md) uses “Implementation Phase N”                    |
| Connection traits and driver APIs were inaccurate        | Resolved in plan         | [03](./03-connection-architecture.md) uses factories/sessions and corrects Pool, `Client`, query, protocol, batch, and TLS claims |
| Idempotency meant unsafe deduplication                   | Resolved                 | Plain INSERT plus committed-chunk journal and canonical conflict reconciliation                                                   |
| State omitted identity and drift evidence                | Resolved in design       | [04 journal schema](./04-execution-design.md#journal-commit-and-resume)                                                           |
| Staging/direct copy/destructive DDL were mixed into MVP  | Resolved                 | Deferred with ownership, state-machine, and write-fence gates                                                                     |
| Verification examples allowed missing rows as matches    | Resolved                 | Missing or skipped rows fail; approved transformations use a distinct status                                                      |
| Observability made unsupported causal and latency claims | Resolved                 | [Observability](./observability-and-operations.md) limits claims to observations and provenance                                   |
| CLI flags differed across documents                      | Resolved                 | [04](./04-execution-design.md#canonical-cli-contract) is canonical; future flags are absent until their Implementation Phase      |
| Managed service appeared in MVP                          | Resolved                 | [07](./07-managed-service-appendix.md) is explicitly deferred and prerequisite-gated                                              |
| Acceptance tests were incomplete                         | Resolved as requirements | [08 acceptance gates](./08-implementation-prerequisites.md#requirements-and-acceptance-gates)                                     |
| Universal and unique marketing claims were unsupported   | Resolved                 | [Competitive landscape](./competitive-landscape.md) uses scoped category comparisons                                              |

Implementation remains open. These statuses mean the documentation now states a
safe contract; they do not mean the feature exists.
