# Part 8: Cross-Industry Migration Patterns

> Synthesis of 10 parallel research subagents across Percona, MariaDB,
> PlanetScale, Neon, ClickHouse, GitHub/Stripe/Shopify, AWS, distributed
> DBs (CockroachDB/YugabyteDB/TiDB/SingleStore), schema management tools
> (Flyway/Liquibase/Bytebase/Skeema/Atlas), and open-source migration
> tools (pgloader/mydumper/pg_chameleon/Debezium).

## 8.1 Universal Migration Pipeline

Every database vendor, schema management tool, and migration utility converges
on the same pipeline structure:

```
Schema Analysis → Data Movement → Verification → Cutover → Post-Migration
     ↑                ↑              ↑           ↑            ↑
  Compatibility   Bulk + CDC    Checksums +   Dual-write    Index/constraint
  report first    (2 phases)    row counts    + shadow read  rebuild + stats
```

**Implication for sql-splitter**: The designed pipeline (extract schemas →
generate plan → execute DDL → migrate data → post-data DDL → verify)
matches industry consensus. The industry validation confirms we have the right
stages in the right order.

## 8.2 Schema Compatibility Reports: Universally Step 1

Every tool generates a compatibility report **before moving any data**:

| Tool        | Report Name               | What It Checks                                      |
| ----------- | ------------------------- | --------------------------------------------------- |
| CRDB MOLT   | Schema Analysis           | Unsupported features, DDL rewrites needed           |
| YB Voyager  | `analyze-schema`          | Unsupported DDL, manual intervention flags          |
| MySQL Shell | `checkForServerUpgrade()` | Deprecated features, removed functions              |
| pgloader    | Dry-run mode              | Type casting warnings, feature incompatibilities    |
| Bytebase    | SQL Review                | 100+ linting rules across dialects                  |
| Atlas       | `migrate lint`            | Destructive changes, table locks, policy violations |

**What sql-splitter already has**: `validate` command checks syntax, encoding,
PK/FK integrity. Missing: cross-dialect feature compatibility report.

**Action**: Add a `validate --target-dialect postgres` mode that runs the
converter in dry-run and produces a compatibility report: which DDL statements
will be rewritten, which types will be lossy, which features are unsupported.

## 8.3 Adaptive Chunk Sizing (Industry Standard)

| Tool              | Mechanism                                                                              | Default            |
| ----------------- | -------------------------------------------------------------------------------------- | ------------------ |
| pt-table-checksum | `--chunk-time` dynamically adjusts chunk size so query completes in target time window | 0.5s               |
| mydumper          | `--rows` with auto-doubling: if query takes <1s, double chunk; if >2s, halve           | 10000              |
| AWS DMS           | `parallel-load` with partition-aware segment splitting                                 | Auto               |
| ClickPipes        | Configurable snapshot partition rows                                                   | ~1GB per partition |

**What the proposed design includes**: Static `--chunk-size` (default 100,000). Missing:
adaptive sizing based on query execution time.

**Action**: Add `--chunk-time <seconds>` flag. The boundary discovery from §6.3c
should also support `--chunk-time` mode: start with a small chunk, measure its
query duration, scale up/down to hit the target time window. This replaces the
need for manual `--chunk-size` tuning on unfamiliar databases.

## 8.4 Staging Tables + Atomic Swaps (Universal Safe Pattern)

| Source          | Pattern                                                              |
| --------------- | -------------------------------------------------------------------- |
| ClickHouse      | `CREATE staging` → INSERT → `MOVE PARTITION` (instant metadata op)   |
| MySQL (general) | `RENAME TABLE a TO old, staging TO a` (single atomic statement)      |
| PlanetScale     | Deploy request → VReplication keeps shadow table for 30 min → revert |
| gh-ost          | Ghost table → cut-over atomic swap                                   |
| pgloader        | Drop constraints → load data → recreate constraints                  |

**What the proposed design includes**: `--staging-strategy rename-safe` in §6.3d.

**Gap**: The ClickHouse pattern of partition-based swaps (metadata-level
operation, instant regardless of data size) is not represented. For ClickHouse
targets with monthly partitioning, sql-splitter should detect partition
boundaries and offer partition-level imports with atomic swaps.

**Action**: Add `--staging-strategy partition-swap` for ClickHouse targets.
Detect `PARTITION BY` in the target schema and import data partition-by-partition.

## 8.5 Checksum-Based Integrity Verification (Universal)

| Tool                      | Checksum Method                                                                   | Used For                                |
| ------------------------- | --------------------------------------------------------------------------------- | --------------------------------------- |
| mydumper                  | `CHECKSUM TABLE` (MySQL native) + CRC32 of schema definitions + CRC32 of routines | Backup verification                     |
| Flyway                    | Migration checksum stored in `flyway_schema_history` table                        | Detecting migration file modification   |
| Liquibase                 | MD5 checksum per changeset in `DATABASECHANGELOG` table                           | Change detection on re-run              |
| Atlas                     | Fingerprint hash of entire schema state                                           | Ensuring plan applies to expected state |
| Percona pt-table-checksum | CRC32 per chunk, stored in `percona.checksums` table                              | Replication drift detection             |

**Key insight from mydumper docs**: `CHECKSUM TABLE` returns **different values**
across MySQL vendors/versions even with identical data. Cross-vendor migration
cannot rely on database-native checksums. Must use application-level checksums
(SHA-256 or XXH3) that are database-independent.

**What the proposed design includes**: SHA-256 streaming table checksums in §6.1 Phase 4.
Aligned with industry practice. Application-level checksums (not DB-native) are
the correct choice.

**Action**: Add `--verify-checksum-algorithm xxh3` as a faster alternative to
SHA-256 for large datasets. XXH3 is 5-10× faster than SHA-256 and
cryptographically sufficient for migration verification (not security).

## 8.6 Separate Thread Pools for Schema, Data, and Index Phases

myloader decomposes restore into three tunable phases:

| Phase           | Thread Pool                  | Purpose                                          |
| --------------- | ---------------------------- | ------------------------------------------------ |
| Schema creation | Single-threaded (sequential) | Creates tables, must be ordered                  |
| Data loading    | Multi-threaded (parallel)    | Bulk INSERT, no FK constraints                   |
| Index creation  | Multi-threaded (parallel)    | `--innodb-optimize-keys AFTER_IMPORT_ALL_TABLES` |

**What the proposed design includes**: Phase 1 DDL (sequential), Phase 2 data (parallel
via `--parallel`), Phase 3b post-data DDL (sequential). This decomposition
matches myloader's architecture exactly.

**Gap**: Index creation is currently part of Phase 3b (sequential). Per
myloader's approach, index creation can be parallelized after all data is
loaded (indexes don't depend on each other, unlike constraints).

**Action**: Split Phase 3b into: 3b1 (CREATE CONSTRAINT / ADD FK — sequential,
dependency-ordered) and 3b2 (CREATE INDEX — parallel, independent per table).
Add `--parallel-indexes N` flag.

## 8.7 MariaDB-Specific Migration Requirements

MariaDB differs from MySQL in ways a migration tool must handle:

| Difference                                | Impact                                                                                                   |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| GTID format incompatible                  | `Domain:ServerID:Sequence` vs MySQL's `UUID:Sequence`; cannot replicate directly; must use file+position |
| JSON stored as `LONGTEXT` alias           | Binary JSON from MySQL cannot be loaded; must convert to TEXT                                            |
| `caching_sha2_password` not portable      | Requires `mysql_native_password` for replication user                                                    |
| `utf8mb4_0900_*` collations missing       | Default collation differs; JOIN indexes may break                                                        |
| `.frm` files required                     | MySQL 8.0+ removed them; MariaDB needs them — no in-place upgrade from MySQL 8.0                         |
| `mariadb-upgrade` must run post-migration | System tables need conversion; skipped = corrupt `mysql.proc`                                            |
| `explicit_defaults_for_timestamp` differs | Default OFF in MariaDB vs ON in MySQL → different TIMESTAMP behavior                                     |
| TRIGGER limit (10.1)                      | Only one trigger per event per table (MySQL allows multiple)                                             |
| `INSERT IGNORE` also emits warnings       | Different from MySQL's silent IGNORE behavior                                                            |

**Action**: Add a `--target-dialect mariadb` to the `convert` and `migrate`
commands. Pre-flight checks for: JSON column detection (must convert to TEXT),
collation mismatch warnings, trigger count warnings, `.frm` compatibility note,
and `mariadb-upgrade` reminder in the runbook output.

## 8.8 RDS/Aurora-Specific Constraints

| Constraint                                                        | Impact                                                |
| ----------------------------------------------------------------- | ----------------------------------------------------- |
| Binlog retention requires `CALL mysql.rds_set_configuration(...)` | Must be run manually (not a SQL variable)             |
| Enhanced binlog (Aurora 8.0) incompatible with CDC tools          | Cannot use DMS or standard binlog readers             |
| DMS does NOT use GTIDs                                            | Custom CDC must handle position-based only            |
| `AUTO_INCREMENT` not migrated by DMS                              | Must sync manually                                    |
| `ON DELETE/ON UPDATE CASCADE` not replicated via binlog           | FK cascades on source produce orphaned rows on target |
| Aurora Serverless v1: no CDC                                      | Full load only                                        |
| RDS Proxy pins sessions for >16KB statements                      | Migration tools must use direct connections           |
| IAM auth tokens expire every 15 min                               | Connection pools must handle token refresh            |

**Action**: Pre-flight checks for RDS sources: validate binlog retention is
configured, warn about cascade operation orphan risk, detect Aurora Serverless
v1 and refuse CDC mode. For RDS targets: warn about binlog disk impact (already
in §7 HF-5), detect RDS Proxy in the connection path and suggest direct
connection, detect IAM auth and warn about 15-min token expiry.

## 8.9 The Last Generation Has Passed; Log-Based CDC Is Universal

Every company abandoned trigger-based CDC in favor of log-based approaches:

| Company     | Old Approach             | New Approach                       |
| ----------- | ------------------------ | ---------------------------------- |
| GitHub      | pt-osc (triggers)        | gh-ost (binlog, triggerless)       |
| Shopify     | Longboat (batch queries) | Debezium + Kafka (binlog CDC)      |
| PlanetScale | N/A                      | VReplication (built-in binlog CDC) |
| ClickHouse  | Manual batch             | ClickPipes (managed binlog CDC)    |
| SingleStore | N/A                      | Flow (managed CDC)                 |

**Implication**: sql-splitter should NOT attempt CDC or replication-based
migration. This is a separate product category (Debezium, ClickPipes, DMS,
VReplication). The tool's value is in the **analysis, validation, conversion,
ordering, and verification** of dump-based migrations — the phases before and
after data movement. Leave the CDC to specialized tools.

## 8.10 Schema-As-Code: All Serious Tools Use Declarative

| Tool      | Approach                                                  | Key Difference                         |
| --------- | --------------------------------------------------------- | -------------------------------------- |
| Skeema    | `CREATE TABLE` files in git → diff against live           | No migration history table             |
| Flyway    | Versioned SQL files + checksum tracking                   | Requires `flyway_schema_history` table |
| Atlas     | HCL/SQL schema files + fingerprint hash + schema registry | Plan stored centrally, fetched by hash |
| Liquibase | XML/YAML changelog + checksum tracking                    | Multi-format, DDL/DML in same file     |
| Bytebase  | Policy-enforced SQL review + change workflow              | 100+ linting rules, approval gates     |

**What sql-splitter does**: sql-splitter is a **migration analysis and execution
tool**, not a schema management tool. It reads dumps and live databases, produces
diffs, generates plans, and executes them. It does not store schema history or
enforce migration versioning.

**Decision**: Do not add schema-as-code or migration versioning features. This
is a separate product category (Flyway/Liquibase/Atlas/Bytebase). sql-splitter
should integrate with these tools (read their output, generate compatible SQL)
but not replace them.

## 8.11 Rule-Based Schema Safety (Adopt Bytebase's Model)

Bytebase's 100+ SQL linting rules, organized into three layers, are the most
comprehensive model:

| Layer        | What It Checks                                             | sql-splitter Status                               |
| ------------ | ---------------------------------------------------------- | ------------------------------------------------- |
| **Linting**  | Syntax errors, typos, dialect mistakes                     | Covered by `validate` command                     |
| **Semantic** | Missing PKs, no FKs, naming conventions, table-locking DDL | Partial (PK/FK checks exist, no locking analysis) |
| **Policy**   | Risk-tiered routing; dangerous ops → human review          | Not applicable (CLI tool, no approval workflow)   |

**Action**: Extend the PlanetScale validation profile (§4.4) into a general
"target dialect" profile system. For each target dialect, add semantic rules
from Bytebase's rule set that are relevant to migration:

| Rule                             | Applies To                  | Check                                               |
| -------------------------------- | --------------------------- | --------------------------------------------------- |
| `NO_PRIMARY_KEY`                 | All targets                 | Error for PlanetScale, warning otherwise            |
| `NO_UNIQUE_KEY`                  | PlanetScale, TiDB           | Error                                               |
| `UNSUPPORTED_CHARSET`            | PlanetScale, MariaDB        | Error for missing collations                        |
| `TABLE_REQUIRES_LOCK`            | MySQL 5.7                   | Warning for DDL that locks the table                |
| `CONSTRAINT_MISSING_NAME`        | All targets                 | Warning (unnamed constraints are hard to reference) |
| `USES_ENGINE_MYISAM`             | PlanetScale, RDS Aurora     | Error                                               |
| `USES_PARTITION_BY`              | SQLite                      | Error                                               |
| `USES_GENERATED_COLUMN`          | MySQL < 5.7, MariaDB < 10.2 | Warning                                             |
| `RESERVED_KEYWORD_AS_IDENTIFIER` | Cross-version               | Warning (target version has new reserved words)     |

## 8.12 Neon Branching for Migration Testing (Adopt Immediately)

Neon's copy-on-write branching is the killer feature for migration testing:

```bash
# Create a branch from production for migration testing
neonctl branches create --project-id myproject --name test-migration

# Run migration against the branch (copy-on-write, costs nothing unless data diverges)
sql-splitter migrate \
  --source mysql://source/db \
  --target "postgres://user:endpoint=ep-test;password@host/db" \
  --execute

# Verify results on the branch
sql-splitter migrate --verify checksum --target ...

# If migration succeeds: promote branch or re-run against production
# If migration fails: delete branch (instant, no data copy to clean up)
neonctl branches delete test-migration
```

**Action**: Document this pattern as the recommended pre-production testing
workflow for Postgres targets. Add a `--neon-branch` flag that creates a
branch, runs the migration, and either promotes or reports results. This
is a Phase 4 feature but should be documented as a manual workflow now.

## 8.13 The "Divorce" Pattern (Fathom + Industry)

Fathom's SingleStore → ClickHouse + PlanetScale split is not unique. The
industry trend is to separate OLTP and OLAP workloads:

| Customer              | Split                                                | Benefits                               |
| --------------------- | ---------------------------------------------------- | -------------------------------------- |
| Fathom                | SingleStore → ClickHouse (OLAP) + PlanetScale (OLTP) | 10× cost reduction, faster analytics   |
| Riskified             | Aurora PG → CockroachDB                              | 83% lower p99 latency, 4.5× throughput |
| Global diagnostics co | 6 DBs → 3 (SingleStore consolidation)                | Simplified architecture                |

**Implication**: sql-splitter's ability to handle **multi-target** migrations
(source → two different targets) is valuable. The `migrate` command should
support:

```bash
sql-splitter migrate \
  --source mysql://source/db \
  --target-olap clickhouse://target/db \
  --target-oltp mysql://planetscale/db \
  --table-map analytics_*:olap users,sites,subscriptions:oltp
```

This is a Phase 5 feature but the architecture should accommodate it.

## 8.14 Key Missing Features After Cross-Industry Review

| #   | Feature                                                               | When    | Based On                               |
| --- | --------------------------------------------------------------------- | ------- | -------------------------------------- |
| F1  | `validate --target-dialect postgres` compatibility report             | Phase 1 | Universal schema analysis pattern      |
| F2  | `--chunk-time <seconds>` adaptive chunk sizing                        | Phase 3 | pt-table-checksum, mydumper            |
| F3  | `--staging-strategy partition-swap` for ClickHouse                    | Phase 4 | ClickHouse atomic partition operations |
| F4  | `--verify-checksum-algorithm xxh3` for faster verification            | Phase 4 | Industry checksum practices            |
| F5  | `--parallel-indexes N` for post-data index creation                   | Phase 3 | myloader thread-pool decomposition     |
| F6  | `--target-dialect mariadb` with MariaDB-specific checks               | Phase 2 | MariaDB compatibility research         |
| F7  | RDS pre-flight checks (binlog retention, cascade warning, IAM expiry) | Phase 2 | AWS RDS/Aurora research                |
| F8  | Neon branching integration (`--neon-branch`)                          | Phase 5 | Neon copy-on-write branching           |
| F9  | Rule-based dialect profiles (extend Bytebase's 100+ rules)            | Phase 1 | Bytebase, Atlas rule engines           |
| F10 | Multi-target migration with table-to-target mapping                   | Phase 5 | Fathom-style OLTP/OLAP split           |

## 8.15 What NOT to Build (Industry Confirmation)

Several ideas from the original brainstorming are explicitly rejected by
industry practice:

| Idea                                   | Why Not                                                 | Industry Consensus                                                                        |
| -------------------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Built-in CDC/change capture            | Separate product category (Debezium, DMS, VReplication) | All log-based CDC tools are specialized, complex, and operate at the infrastructure layer |
| Schema versioning / migration history  | Separate product category (Flyway, Liquibase, Atlas)    | Schema management tools have 20+ years of specialized development                         |
| SQL linting engine with 100+ rules     | Already exists (Bytebase, Atlas)                        | Better to integrate than to rebuild                                                       |
| Automatic shard key selection          | Requires workload analysis, not schema analysis         | All distributed DB vendors have their own advisor tools                                   |
| Trigger-based CDC                      | Dead end; log-based is universally preferred            | GitHub, Shopify, PlanetScale all abandoned triggers                                       |
| Mixing DDL and DML in same transaction | Impossible on MySQL; dangerous illusion elsewhere       | Skeema and Liquibase explicitly separate them                                             |
