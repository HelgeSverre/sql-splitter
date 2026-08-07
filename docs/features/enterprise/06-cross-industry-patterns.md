# Cross-Industry Migration Patterns

## Patterns adopted now

- Separate assessment from execution. Plan-only is the first deliverable.
- Keep schema extraction, plan, DDL, copy, and verification as distinct runtime
  stages with durable operation identities.
- Use a database-native snapshot or a write fence for coherent bulk copy.
- Keep batches bounded by rows and bytes.
- Resume from a durable committed-work journal, not conflict suppression.
- Compare canonical values from the same source point and chunk manifest.
- Create constraints after copy and validate every relationship.
- Probe capabilities and fail closed when a required fact is unknown.

The runtime sequence is:

`preflight → schema extraction → plan → pre-data DDL → data copy → post-data DDL → verification → finalize`

## Patterns deferred

- Parallel read and write paths require shareable snapshots and worker-session
  isolation.
- Adaptive sizing can tune byte-and-row batch limits after correctness tests. It
  does not change keyset boundaries.
- Staging and atomic-name swaps require an ownership model, durable state
  machine, metadata-lock analysis, and write fence.
- Warm-target merge requires explicit conflict semantics. It is not resumable
  empty-target copy.
- Same-server MySQL direct copy is a dialect optimization with separate
  permissions and recovery contracts.
- Managed-provider automation belongs in the deferred service layer.
- CDC is required for online migration and low-downtime cutover. A bulk-copy
  loop must not be marketed as online.

## Patterns rejected for the beta

Do not disable integrity checks or strict modes by default. Do not use linear
`MIN/MAX` ranges, scalar chunk columns, conflict-ignore SQL, rowcount-only
cutover gates, inverse DDL as data rollback, arbitrary webhook delivery, or
unverified estimates for remote free disk.

Industry tools and vendor case studies provide useful patterns, but product
editions and commands change. External tool facts are scoped in
[01](./01-migration-landscape.md) and
[competitive-landscape](./competitive-landscape.md).
