# Competitive Landscape: Database Migration Tools

This comparison is category-level research. Product editions, supported
endpoints, and command names change. Confirm the selected version from vendor
documentation before a migration.

## Categories

| Category                    | Examples                                                                                | Primary responsibility                          | Difference from this plan                                                        |
| --------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------- |
| Versioned schema migration  | Flyway, Liquibase                                                                       | Apply ordered changesets and track history      | This plan copies an existing database; it is not a changeset registry            |
| Declarative schema diff     | Atlas, Skeema, Bytebase workflows                                                       | Compare desired and actual schema, review DDL   | This plan also requires consistent bulk data and canonical verification          |
| Logical bulk copy           | `mysqldump`, MySQL Shell dump/load, mydumper/myloader, `pg_dump`/`pg_restore`, pgloader | Export and load schema/data                     | Format, consistency, conversion, and resume behavior are tool-specific           |
| CDC and replication         | Debezium, native logical replication, Vitess VReplication                               | Capture changes and approach online cutover     | CDC is deferred and required before any online claim                             |
| Cloud migration service     | AWS DMS, Google Database Migration Service, Azure Database Migration Service            | Managed endpoint-specific migration workflows   | Coverage and constraints depend on source, target, mode, region, and edition     |
| Integrated vendor migration | CockroachDB MOLT, YugabyteDB Voyager, TiDB tools                                        | Assess, copy, and verify into one vendor target | They are target-specific platforms; this plan starts same-dialect                |
| Data comparison             | pt-table-checksum, pg_comparator, vendor verification tools                             | Detect drift under stated assumptions           | This plan defines expected post-conversion canonical values and journal identity |

Command distinctions matter. Atlas schema inspection/diff commands, Flyway
versioned migration commands, and bulk-copy commands do not provide identical
products merely because each uses the word “migration.” Likewise, open-source
and paid editions can expose different capabilities.

## Scoped positioning

The proposed product is a local, plan-first migration runner for dump and live
database inputs. Its first production beta is offline, same-dialect,
sequential, and empty-target only. It can build on sql-splitter's file parser,
schema analysis, and dialect support, but those foundations do not yet provide
live migration.

Potential later differentiation is one versioned plan/journal/verification
contract across supported adapters. This is a design goal, not a claim that no
other tool offers similar functions. Do not market the beta as online,
zero-downtime, universally portable, uniquely complete, or faster than another
tool without reproducible evidence.

## External references

- [Flyway documentation](https://documentation.red-gate.com/flyway)
- [Liquibase documentation](https://docs.liquibase.com/)
- [Atlas documentation](https://atlasgo.io/docs/)
- [MySQL Shell dump and load](https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utilities-dump-instance-schema.html)
- [PostgreSQL pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html)
- [AWS DMS limitations](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Limitations.html)
- [CockroachDB MOLT](https://www.cockroachlabs.com/docs/molt/)
- [YugabyteDB Voyager](https://docs.yugabyte.com/preview/yugabyte-voyager/)

Oracle removed `mysqlpump` in MySQL 8.4, so it is not a current default
alternative: [removed features](https://dev.mysql.com/doc/refman/8.4/en/added-deprecated-removed.html).
