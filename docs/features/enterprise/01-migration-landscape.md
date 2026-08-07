# Enterprise Migration Landscape

## Scope used by this plan

The first production beta is offline, same-dialect, sequential, and create-only
on an empty migration-owned target. One live dialect ships first, then the
second. The source is quiesced by a verified write fence or held under one
database-native consistent snapshot through strict verification. Plan-only does
not write. Online migration is deferred until a CDC design exists.

## Migration patterns

| Pattern                                               | Initial status     | Consistency requirement                                           |
| ----------------------------------------------------- | ------------------ | ----------------------------------------------------------------- |
| Catalog inspection and plan-only                      | Planned first      | Catalog fingerprint and stable endpoint identity                  |
| Immutable completed dump to empty same-dialect target | Planned beta input | Verified dump manifest and checksums                              |
| Live same-dialect source to empty target              | Planned beta input | Write fence or one native snapshot                                |
| Cross-dialect                                         | Deferred           | Typed conversion policy and canonical expected-value verification |
| Parallel extraction                                   | Deferred           | Shareable/exported snapshot or stopped replica                    |
| Warm-target merge or staging swap                     | Deferred           | Ownership, state machine, and write-fence gates                   |
| Online or zero-downtime cutover                       | Deferred           | CDC, catch-up, cutover, and rollback protocol                     |

Schema-change tools, logical dump tools, CDC systems, and database migration
services solve different parts of this space. DDL tools do not imply consistent
data copy. Bulk dump tools do not provide an online cutover. CDC products need
an initial snapshot and operational control plane.

## Corrected external facts

- Oracle removed `mysqlpump` in MySQL 8.4. Use supported alternatives and do
  not plan a new workflow around it:
  [MySQL 8.4 removed features](https://dev.mysql.com/doc/refman/8.4/en/added-deprecated-removed.html).
- Oracle describes MySQL 8.4 as an LTS release. MySQL 8.0 reaching Premier
  Support end is not the same as all support ending; Sustaining Support terms
  remain distinct:
  [MySQL release model](https://dev.mysql.com/doc/refman/8.4/en/mysql-releases.html),
  [Oracle lifetime support](https://www.oracle.com/us/support/library/lifetime-support-technology-069183.pdf).
- PostgreSQL `NOT VALID` and later validation reduce when existing rows are
  checked, but lock acquisition and validation scans still matter:
  [ALTER TABLE](https://www.postgresql.org/docs/current/sql-altertable.html).
- A multi-table MySQL `RENAME TABLE` is atomic as a statement, but metadata
  locks can delay it. It is not a downtime guarantee:
  [RENAME TABLE](https://dev.mysql.com/doc/refman/8.4/en/rename-table.html).
- Neon has pooled and direct connection endpoints. Pooling mode and autosuspend
  behavior depend on configuration and plan; the migration must probe rather
  than infer from a hostname:
  [Neon connection pooling](https://neon.com/docs/connect/connection-pooling),
  [Neon scale to zero](https://neon.com/docs/introduction/scale-to-zero).
- PlanetScale export/import and AWS DMS capabilities vary by product version,
  endpoint, and migration type. Treat vendor documentation as the source for a
  selected deployment, not as a universal guarantee:
  [PlanetScale database imports](https://planetscale.com/docs/imports/database-imports),
  [AWS DMS limitations](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Limitations.html).

The core CLI accepts PlanetScale only as immutable completed dump input. It does
not use live VTGate cursors, invoke `pscale`, accept `pscale://`, or manage
service tokens.

## Risks retained from research

Large objects, generated columns, expression and partial indexes, collations,
identity/sequence state, routines, views, triggers, partitions, FK cycles,
long-lived snapshots, database connection limits, and target storage remain
material risks. The exact vendor catalog and unsupported-object report turn
these into plan gates. Estimates and throughput are environment-specific; no
case-study number is a product performance claim.

## Delivery order

Delivery uses the Implementation Phases in [08](./08-implementation-prerequisites.md).
Runtime uses only: preflight, schema extraction, plan, pre-data DDL, data copy,
post-data DDL, verification, and finalize.
