# Round-1 Review Resolution Ledger

This ledger records the current documentation state. It does not claim complete
or “100%” source accuracy.

| Finding                                                      | Status             | Resolution                                                                                                                                                   |
| ------------------------------------------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Routine/view/trigger classification was overstated           | Resolved           | [02](./02-capability-audit.md) requires exact catalog support and does not infer it from statement boundaries                                                |
| `TypeMapper` was treated as runtime conversion               | Resolved           | [03](./03-connection-architecture.md#canonical-values-and-bounded-batches) separates SQL text mapping from `DbValue` conversion                              |
| PlanetScale timeout/live path was contradictory              | Resolved           | [03](./03-connection-architecture.md#planetscale-input) permits immutable completed dump input only                                                          |
| No consistent snapshot                                       | Resolved in design | [03](./03-connection-architecture.md#snapshot-lifecycle-and-consistency) defines sequential MySQL/PostgreSQL contracts; implementation tests remain required |
| NULL/composite/collation pagination was unspecified          | Resolved in design | [04](./04-execution-design.md#keyset-pagination) defines complete tuple keyset rules                                                                         |
| JSON, float, temporal, and byte verification was unspecified | Resolved in design | [04](./04-execution-design.md#canonical-verification) defines versioned framing; exact vectors remain an implementation gate                                 |
| Edge cases lacked executable acceptance criteria             | Resolved           | [08](./08-implementation-prerequisites.md#requirements-and-acceptance-gates) provides the test matrix                                                        |

Open work is implementation, real-engine validation, and maintenance of vendor
facts. No issue in this ledger changes the offline same-dialect beta boundary.
