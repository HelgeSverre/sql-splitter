# Enterprise Database Migration Landscape

This document surveys the technical landscape for enterprise-scale database
migration: PlanetScale (Vitess) exports, AWS DMS, cross-dialect tooling, online
schema change patterns, and common pitfalls at scale. It documents what each
source/target platform does and does not support, establishes the constraints
that migration tooling must operate within, and defines a prioritised feature
roadmap for migration capabilities.

---

## 2.1 PlanetScale Deep Dive

PlanetScale is a managed MySQL platform built on Vitess, Google's
open-source database clustering system originally developed for YouTube.

### Architecture

```
┌──────────────────────────────────────────────┐
│                  Application                  │
└──────────────────┬───────────────────────────┘
                   │ MySQL wire protocol
┌──────────────────▼───────────────────────────┐
│                  VTGate (3 per branch)         │
│         Connection pooling, query routing      │
└────────┬─────────┬─────────┬─────────────────┘
         │         │         │
    ┌────▼───┐ ┌───▼───┐ ┌───▼───┐
    │VTTablet│ │VTTablet│ │VTTablet│
    │ +MySQL │ │ +MySQL │ │ +MySQL │
    │  8.0   │ │  8.0   │ │  8.0   │
    └────────┘ └────────┘ └────────┘
         │         │         │
    ┌────▼─────────▼─────────▼────┐
    │       Topology Service       │
    └──────────────────────────────┘
```

### Branch Model

- **Production branches**: 1 primary + 2 replicas + 3 VTGates across 3 AZs
- **Development branches**: Single MySQL node + single VTGate; billed per-millisecond
- **Schema-only branch creation**: Development branches copy only the schema, not data
- **Read-only regions**: Additional replica pools for geo-distributed reads

### Sharding

- Uses **Primary Vindex** (sharding key) — Vitess hashes the vindex to route rows to shards
- Supports **Lookup Vindexes** for secondary-key cross-shard lookups
- **FK constraints unsupported** in sharded environments (Vitess limitation)
- Recommended when databases exceed ~250 GB or hit vertical throughput limits

### What PlanetScale DOES NOT Support

This is the critical list for migration tooling:

| Feature                     | Status                                                               | Migration Impact                                                                            |
| --------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Stored procedures           | Not supported                                                        | Must rewrite as application logic or re-add at target                                       |
| Stored functions            | Not supported                                                        | Same as above                                                                               |
| Triggers                    | Not supported                                                        | Must implement in application or re-add at target                                           |
| Events (scheduled SQL)      | Not supported                                                        | Must replace with cron + CLI                                                                |
| Foreign key constraints     | Disabled by default; must enable per-database in settings            | FK constraint names change on every deployment (hash-suffixed); not in sharded environments |
| `LOAD DATA INFILE`          | Not supported                                                        | Must use INSERT-based import                                                                |
| `CREATE/DROP DATABASE`      | Not supported                                                        | Cannot create logical DBs within a PlanetScale DB                                           |
| `ALTER TABLE RENAME COLUMN` | Destructive                                                          | Must create new column, copy data, drop old                                                 |
| `KILL` query                | Not supported as SQL; `pscale branch connections kill` works via CLI | Must use PlanetScale CLI, not MySQL protocol, to terminate queries                          |
| `WITH RECURSIVE` CTEs       | Vitess source shows CTE support in semantic analyzer; may be stable  | Test on target Vitess/PlanetScale version before relying on this feature                    |
| `JSON_TABLE`                | Not supported                                                        | Other JSON functions work                                                                   |
| `SET GLOBAL time_zone`      | Fixed to UTC                                                         | Timezone-aware apps must handle this                                                        |
| Binary log access           | Not provided externally                                              | No CDC/DMS/Debezium outbound                                                                |
| Views                       | Detected but NOT imported                                            | Must recreate manually post-migration                                                       |
| Non-InnoDB engines          | Not supported                                                        | Only InnoDB                                                                                 |
| Tables without unique key   | Not allowed                                                          | ALL tables must have a unique, non-null key                                                 |
| Direct DDL on production    | Blocked when safe migrations enabled                                 | All DDL must go through deploy requests                                                     |
| FK constraint stability     | Names change on every deploy (suffixed with hash)                    | Breaks app logic referencing constraint names                                               |

### PlanetScale Export Path

```
pscale database dump <DATABASE> <BRANCH> --output <dir>
```

Key characteristics:

- Output: mydumper-compatible per-table SQL files (`-schema.sql` + `.sql`)
- `--threads` for parallelism (default 16)
- `--tables`, `--columns`, `--wheres` for filtering
- `--replica`, `--rdonly` for source node selection
- `--keyspace`, `--shard` for shard-specific dumps
- `--output-format` supports `sql` (default, mydumper-compatible), `json`, and `csv`
- No binary log streaming — this is a snapshot-only export

### PlanetScale Schema Change Workflow (Deploy Requests)

1. Enable safe migrations on production (blocks direct DDL)
2. Create a dev branch off production (schema only, no data)
3. Make schema changes on the dev branch
4. Create a deploy request — PlanetScale auto-generates a schema diff
5. Review the diff (additions in green, deletions in red; linting checks)
6. Deploy via Vitess Online DDL (gh-ost style: shadow table, copy, sync, brief cutover)
7. Revert available within 30-minute window (not for instant deployments)

Deployment modes:

- **Standard**: Non-blocking online schema change
- **Gated**: Migration runs but doesn't cut over until approved (for long-running migrations)
- **Instant**: Uses MySQL `ALGORITHM=INSTANT` for supported operations (not revertable)

### PlanetScale Inbound Import (Dashboard-Driven)

PlanetScale's import tool provides zero-downtime migration from external MySQL:

Requirements on source:

- MySQL 5.7-8.0 or MariaDB
- Internet-accessible
- `gtid_mode=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`
- `PROCESS`, `REPLICATION SLAVE`, `REPLICATION CLIENT`, `RELOAD`, plus table SELECT/INSERT/UPDATE/DELETE on target schemas

Import phases:

1. Connect & validate (connectivity, config, schema compatibility)
2. Full table copy (configurable parallelism, up to 100 concurrent connections; secondary indexes can be deferred for speed)
3. Binary log replication keeps databases in sync
4. Traffic switch (optional: replica first, then primary; warns about split-brain)
5. Complete (detach external, stop reverse replication, remove credentials)

Import considerations:

- With FK: all tables must be imported together; uses a long-running transaction; use a replica as source
- Views are not imported — must be recreated manually
- Charsets: `utf8`, `utf8mb4`, `utf8mb3`, `latin1`, `ascii` only
- All tables must have unique non-null keys

### Verified Accuracy

The following points have been verified against PlanetScale documentation, CLI
behaviour, and the Vitess source tree as of August 2026:

| Claim                                                                                | Verification                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--replica` and `--rdonly` flags for source node selection in `pscale database dump` | Correct. `--read-only-region` is a `pscale branch` flag for adding read-only replica regions; it is not part of the dump command. Use `--replica` or `--rdonly` when dumping from a replica.                                                                      |
| KILL query not supported as SQL                                                      | Correct. PlanetScale does not support the MySQL `KILL [connection_id]` statement. The CLI alternative `pscale branch connections kill` works and is the documented way to terminate queries.                                                                      |
| `WITH RECURSIVE` CTE support in Vitess semantic analyzer                             | Confirmed. The Vitess source tree includes CTE support in the semantic analyzer (parser and AST). This feature may work on current PlanetScale versions but should be tested on the target Vitess/PlanetScale version before relying on it in migration planning. |

---

## 2.2 AWS Database Migration Service (DMS)

DMS is a cloud replication service that moves data between database endpoints.

### Architecture

A replication instance (EC2-based) connects to source and target endpoints
and runs a replication task that reads from source, optionally transforms data,
and writes to target.

### MySQL Source Requirements

- `binlog_format = ROW` (mandatory for CDC)
- `binlog_row_image = FULL`
- `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges for CDC
- Sufficient binlog retention (RDS: `CALL mysql.rds_set_configuration('binlog retention hours', 24)`)
- For RDS: GTID-based replication preferred

### Critical DMS Limitations

| Limitation                                 | Impact                                                                                             |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| AUTO_INCREMENT not migrated                | Must pre-create target tables with it; post-load sync needed                                       |
| ON DELETE/ON UPDATE CASCADE not replicated | InnoDB doesn't generate binlog events for cascaded FK operations; target gets orphaned rows        |
| No trigger/view/procedure migration        | Only tables and PKs are migrated                                                                   |
| No GTID-based replication                  | DMS doesn't use GTIDs even if source has them                                                      |
| 4 GB binlog transaction limit              | Large bulk operations can fail                                                                     |
| No computed/virtual columns                | Skipped entirely                                                                                   |
| Invisible columns not migrated             | Must be made visible before migration                                                              |
| LOB truncation risk                        | MEDIUMBLOB/LONGBLOB/MEDIUMTEXT/LONGTEXT skipped if `SupportLobs=false`                             |
| TIMESTAMP → UTC conversion on target       | DATETIME values NOT converted — mixed timezone semantics                                           |
| Aurora MySQL replicas as CDC source        | Not supported; only full-load from replicas                                                        |
| Data loss with replica sources             | Transactions committed to primary before task start but not yet replicated to the replica are lost |

**AWS now recommends "homogeneous data migrations" (native tool-based) over DMS
for same-engine migrations.** DMS may still apply to MySQL → Aurora or
cross-platform migrations, but cannot be used for PlanetScale exits (no
binlog access).

---

## 2.3 Migration Patterns by Source/Target

### MySQL → MySQL (Homogeneous)

| Pattern                  | Tools                                            | Downtime           | Best For                |
| ------------------------ | ------------------------------------------------ | ------------------ | ----------------------- |
| Logical dump/restore     | `mysqldump`, `mydumper`/`myloader`               | Minutes to hours   | < 100 GB databases      |
| Parallel dump/restore    | `mydumper` with `--rows` chunking, `myloader`    | Minutes to hours   | 100 GB - 1 TB           |
| Replication tail         | `mysqldump --master-data=2` + native replication | Seconds to minutes | Zero/near-zero downtime |
| AWS DMS                  | Full load + CDC task                             | Minutes            | AWS-to-AWS migrations   |
| **PlanetScale outbound** | **`pscale database dump` only**                  | Hours+             | Any size; no CDC option |

For PlanetScale → anything, the export path is always file-based:
`pscale database dump` → per-table SQL files → restore on target.
No CDC, no replication tail, no zero-downtime option from PlanetScale's side.

### MySQL → PostgreSQL (Heterogeneous)

**pgloader** is the dominant open-source tool:

```bash
pgloader mysql://user:pass@source/dbname postgresql://user:pass@target/dbname
```

What it does:

- Discovers source schema automatically
- Maps MySQL types to PostgreSQL types using configurable casting rules
- Streams data via COPY protocol (fast bulk load)
- Handles zero dates → NULL conversion
- Drops indexes/constraints before load, recreates after (faster)
- Supports partial migrations, encoding overrides, schema rewrites

**AWS DMS + Schema Conversion Tool (SCT)** for AWS targets:

- SCT assesses schema complexity, converts DDL, functions, procedures
- DMS handles full load + CDC for data migration
- SCT can convert stored procedures with varying success rates

Key data type mapping challenges:

- `TINYINT(1)` → `BOOLEAN` (semantic mismatch)
- `UNSIGNED BIGINT` > 2^63-1 → `NUMERIC(20)` (value overflow)
- `ENUM` → custom PG enum type or `VARCHAR` + `CHECK`
- `DATETIME` → `TIMESTAMP WITHOUT TIME ZONE`
- `0000-00-00` dates → NULL (invalid in PG)
- `ON UPDATE CURRENT_TIMESTAMP` → trigger (no built-in PG equivalent)
- `utf8` vs `utf8mb4` → PG `UTF8` is full UTF-8; MySQL metadata may lie about actual encoding

### PostgreSQL → PostgreSQL

| Pattern             | Tools                                                    | Notes                                                                       |
| ------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------- |
| Dump/restore        | `pg_dump`/`pg_restore`                                   | Directory format for parallel (`-j N`); Custom format for selective restore |
| Logical replication | `CREATE PUBLICATION` / `CREATE SUBSCRIPTION`             | Initial copy + ongoing CDC; DDL not replicated; sequences not replicated    |
| pg_upgrade          | `pg_upgrade --link`                                      | In-place major version upgrades; near-instant with hard links               |
| Large DBs           | Parallel `pg_dump -Fd -j 8` + parallel `pg_restore -j 8` | Directory format only                                                       |

### Zero-Downtime Strategies

1. **Replication-based**: Full dump with binlog/WAL position → restore → start replication → wait for catch-up → brief read-only window → cut over. Typical downtime: seconds to minutes.

2. **Dual-write (application-level)**: App writes to both old and new DBs simultaneously → backfill historical data → verify → switch reads → remove old writes. Zero downtime if done correctly.

3. **CDC/log-based streaming**: Tools (DMS, Debezium, Maxwell, VReplication) read the change log and apply to target. Continuous sync until cutover.

4. **Blue/green deployment**: Create "green" environment → replicate from "blue" → test with production traffic mirroring → DNS cutover → keep blue for rollback.

---

## 2.4 Tooling Landscape

### MySQL Logical Dump Tools

| Tool                  | Parallel           | Snapshot Consistent               | Use Case                            |
| --------------------- | ------------------ | --------------------------------- | ----------------------------------- |
| `mysqldump`           | No                 | Yes (`--single-transaction`)      | < 10 GB                             |
| `mydumper`/`myloader` | Yes, multithreaded | Yes (FTWRL + consistent snapshot) | 10 GB - 1 TB+                       |
| `mysqlpump`           | Yes (MySQL 5.7+)   | Yes                               | Oracle MySQL only                   |
| MySQL Shell dump      | Yes                | Yes                               | Parallel dump/load, compression, S3 |

### PostgreSQL Logical Dump Tools

| Tool                   | Parallel | Features                                              |
| ---------------------- | -------- | ----------------------------------------------------- |
| `pg_dump` (`-Fd -j N`) | Yes      | Custom/directory/tar/plain formats, selective restore |
| `pg_dumpall`           | No       | Entire cluster (all DBs + globals)                    |
| `pg_restore -j N`      | Yes      | Parallel restore of custom/directory format           |

### Online Schema Change Tools (DDL Without Downtime)

| Tool                                  | Mechanism                                                                                              | MySQL       | PG  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------ | ----------- | --- |
| **gh-ost** (GitHub)                   | **Triggerless** — reads binlog directly, creates ghost table, replays binlog events, atomic table swap | Yes         | No  |
| **pt-online-schema-change** (Percona) | Trigger-based — creates ghost table, uses triggers to capture changes                                  | Yes         | No  |
| **Vitess Online DDL**                 | VReplication-based — `ALTER VITESS_MIGRATION ...` declarative commands                                 | Vitess only | No  |

gh-ost features (relevant for migration tooling design):

- Pausable: truly suspends writes on master
- Dynamic control: interactive commands via Unix socket
- Throttling: based on replication lag, query load, custom conditions
- Cut-over control: `--postpone-cut-over-flag-file` to defer table swap
- Hooks: external scripts at migration lifecycle points
- Limitations: no FK support, requires unique key, requires `binlog_format=ROW`

### Schema Conversion Tools

| Tool                      | Heterogeneous | Features                                                              |
| ------------------------- | ------------- | --------------------------------------------------------------------- |
| **pgloader**              | Yes           | MySQL/SQLite/MSSQL → PostgreSQL; one-command; streaming COPY          |
| **AWS SCT**               | Yes           | GUI + CLI; converts schemas, procedures, triggers; assessment reports |
| **ora2pg**                | Yes           | Oracle → PostgreSQL; comprehensive, mature                            |
| **DMS Schema Conversion** | Yes           | AWS-managed SCT; integrated with DMS                                  |

### Schema Diffing and Migration Generation

| Tool                        | Dialect    | Approach                                                                                              |
| --------------------------- | ---------- | ----------------------------------------------------------------------------------------------------- |
| **Skeema**                  | MySQL      | Declarative, file-per-object git repo; information_schema introspection; empirical ALTER verification |
| **schemadiff** (Vitess)     | MySQL      | Library, AST-based diff with semantic awareness; online DDL hints                                     |
| **pg-schema-diff** (Stripe) | PostgreSQL | Library+CLI; declarative; zero-downtime native PG operations; hazard annotations                      |
| **apgdiff**                 | PostgreSQL | Java; dump file comparison; CREATE-to-ALTER generation                                                |
| **migra**                   | PostgreSQL | Live DB comparison; auto-generates SQL                                                                |
| **Bytebase**                | Multi-DB   | GUI platform for schema change governance; 200+ SQL linting rules                                     |

### Data Validation Tools

| Tool                  | Dialect    | Method                                                                                    |
| --------------------- | ---------- | ----------------------------------------------------------------------------------------- |
| **pt-table-checksum** | MySQL      | Chunked checksum comparison between replication source and replicas; `CRC32`/`MD5`/`SHA1` |
| **VDiff** (Vitess)    | Vitess     | Logical diff between source and target tables during/after migration                      |
| **pg_comparator**     | PostgreSQL | Table comparison with row-level diff output                                               |
| **Custom checksums**  | Any        | `SELECT MD5(GROUP_CONCAT(MD5(CONCAT_WS(...))))` per table on both sides                   |

---

## 2.5 Common Pitfalls at Scale

### Large Tables (Billions of Rows)

1. **Single-threaded dump takes days**: Use mydumper with `--rows` chunking or MySQL Shell parallel dump
2. **Restore time dominates**: Disable FK checks, unique checks, binary logging, autocommit during restore
3. **Disk space**: Plan for dump files + target DB + binlogs + temp space. 500 GB DB may need 1-1.5 TB
4. **Index rebuild**: Creating indexes post-load is often faster than maintaining during load (pgloader does this)
5. **Network bandwidth**: Terabytes over WAN = bottleneck; consider physical transfer or Direct Connect
6. **CDC catch-up**: If source has high write throughput, CDC consumer must keep up

### Foreign Key Constraints

1. **DMS doesn't replicate CASCADE operations**: InnoDB doesn't log cascaded changes; target gets orphaned rows
2. **PlanetScale FK constraint names change on every deploy**: Hash-suffixed (e.g., `pchild_fk_5vtaqz7kepok6wa91vryrkrje`); breaks application logic
3. **pgloader strategy**: Drop FKs before load, create + validate after data load
4. **Import ordering**: FK-aware topological sort is essential for correct restore

### Auto-Increment Resets

1. **DMS doesn't migrate AUTO_INCREMENT**: Must pre-create or post-sync
2. **Sequence values must be synced**: `ALTER TABLE ... AUTO_INCREMENT = N` (MySQL) / `SELECT setval('seq', max(id))` (PG)
3. **Gap risk during cutover**: If writes continue during cutover, auto-increment values consumed on old DB aren't on new DB

### Character Set and Collation

1. **MySQL metadata may lie**: Declared charset can mismatch actual data; MySQL doesn't enforce
2. **`utf8` vs `utf8mb4`**: MySQL `utf8` = 3-byte (no emoji); `utf8mb4` = true UTF-8. Always validate
3. **Collation differences**: PG collations are OS-provided; case-insensitive collations need `CITEXT` extension or `ILIKE`
4. **Sort order differences**: Test sorting, GROUP BY, DISTINCT with non-ASCII data before cutover

### Binary Log and Replication

1. **ROW format mandatory** for CDC and gh-ost
2. **`binlog_row_image=FULL`** for complete row images on UPDATE/DELETE
3. **RDS binlog retention is aggressive**: Must configure explicitly
4. **PlanetScale provides no binlog access**: The critical limitation — no CDC outbound

### PlanetScale-Specific Export Considerations

When dumping from PlanetScale:

1. **Every table has a unique key** (enforced) — good for migration, simplifies replication setup at target
2. **`utf8mb4_0900_ai_ci` collation** on new databases — migrate to older MySQL versions requires collation conversion
3. **No stored routines to migrate** — PlanetScale doesn't allow them, so no conversion needed
4. **FK constraint names will be hash-suffixed** — rename to predictable names at target
5. **No views** — PlanetScale doesn't import them; user must recreate
6. **Sharded data** — `pscale database dump` unifies shard output; VSchema config has no standard MySQL equivalent
7. **No binlog** — cannot use CDC, DMS CDC, Debezium, or any log-based tooling for PlanetScale outbound

### PostgreSQL-Specific Considerations

1. **DDL is transactional** in PG — `CREATE TABLE`, `ALTER TABLE` roll back on error (MySQL DDL is implicitly committed)
2. **`pg_dump` holds a snapshot** for entire dump duration — WAL bloat and replication lag on busy databases
3. **Parallel dump opens N+1 connections** — ensure `max_connections` accommodates
4. **Sequences not replicated** via logical replication — must sync before cutover
5. **Default expressions** — MySQL `DEFAULT (expression)` and `ON UPDATE CURRENT_TIMESTAMP` have no direct PG equivalent
6. **Zero-value TIMESTAMP** — MySQL allows `0000-00-00 00:00:00`; PG rejects it

---

## 2.6 Managed MySQL/Postgres Target Constraints

Managed database platforms impose specific constraints that migration tooling
must encode as pre-flight checks. The following constraints are verified against
Laravel Cloud, Neon, and AWS RDS documentation as of August 2026. Similar
constraints apply to other managed offerings (PlanetScale itself, DigitalOcean
Managed Databases, Fly.io Postgres).

### General Platform Constraints

1. **Backups are internal snapshots** — not downloadable `mysqldump` files. To
   download data: restore backup to a temp cluster, enable public endpoint,
   export with `mydumper`/`mysqldump`, delete the temp cluster.

2. **No cross-region database copy/restore** provided by most platforms. Must
   export from source region, import to target region manually.

3. **Storage can only increase, never decrease** — with cooldown periods between
   increases (typically 6 hours). Plan long-term storage needs before importing.

4. **Public endpoint must be enabled** for external import tools to connect.
   Disable after migration.

5. **Size up for import, size down after.** CPU/memory can scale freely (unlike
   storage). Temporarily scale to a larger tier for fast imports, then scale down.

6. **No superuser access** on managed platforms. This restricts operations like
   `SET GLOBAL`, `SUPER`-privileged `KILL`, and direct binary log manipulation.

### Managed Platform Comparison

| Constraint                 | MySQL (Laravel Cloud)              | Serverless Postgres (Neon)        | RDS (Private Cloud) |
| -------------------------- | ---------------------------------- | --------------------------------- | ------------------- |
| Storage range              | 5 GB–1,000 GB                      | 5 GB–1,000 GB                     | 20 GB–1,000 GB      |
| Storage decrease           | Never                              | Never                             | Never               |
| Storage increase frequency | Once per 6 hours                   | Once per 6 hours                  | Once per 6 hours    |
| Scale-to-zero              | Flex only (wake in few hundred ms) | After 5 min idle (wake in ~500ms) | No                  |
| Connection limit           | 51–3,264 (RAM-based)               | 104–4,000 (CU-based)              | 630–20K             |
| Proxy layer                | 10k read multiplexing              | PgBouncer 10k pooled              | N/A                 |
| PITR                       | No (daily snapshots only)          | Yes                               | Yes (up to 30 days) |
| Backup format              | Internal snapshots only            | Neon internal                     | RDS snapshots       |
| Superuser                  | No                                 | No                                | No (managed)        |
| Public endpoint            | Must toggle on for import          | Always available                  | Must toggle on      |
| Same-region requirement    | Must match compute                 | Must match compute                | Must match VPC      |

### MySQL-Specific Constraints

- **No PITR** on some platforms — only daily automated snapshots and manual snapshots.
- **MySQL 8.0 reaches end-of-life April 2026.** 8.0 → 8.4 upgrade is
  irreversible, takes several minutes of downtime, and requires FK strictness
  checks (referenced columns must have UNIQUE keys) and reserved keyword
  migration (`MANUAL`, `PARALLEL`, `QUALIFY`, `TABLESAMPLE`).
- **`mydumper` is the recommended tool** for parallel MySQL import.
- **`pgloader` is the recommended tool** for MySQL → Postgres migration.

### Serverless Postgres (Neon) Constraints

- **Scale-to-zero**: Compute suspends after idle period, drops all connections
  and prepared statements. Migration tooling must use direct (non-pooled)
  connections and either disable auto-suspend or implement reconnection loops.
- **PgBouncer limitations**: no `SET`/`RESET` persistence across transactions,
  no `LISTEN`/`NOTIFY`, no `WITH HOLD CURSOR`, no SQL-level `PREPARE`/`DEALLOCATE`,
  no temporary tables, no session-level advisory locks on pooled connections.
  Use direct connections (not pooled) for migration import operations.

---

## Migration Feature Priorities

Features ranked by migration impact and implementation feasibility. Timeline
estimates are excluded; priorities reflect dependency ordering and value-to-effort
ratio.

| Priority | Feature                                             | Rationale                                                    |
| -------- | --------------------------------------------------- | ------------------------------------------------------------ |
| **P0**   | Live database connections                           | Foundation for all other features; unlocks live-to-live diff |
| **P0**   | Migration plan generator                            | Diff engine exists; orchestration layer on top               |
| **P1**   | PlanetScale-aware validation                        | Schema metadata exists; rule checks are additive             |
| **P1**   | Migration data verification (chunked)               | Row processing infrastructure exists                         |
| **P1**   | Pre-migration risk assessment                       | Analysis layer on existing data                              |
| **P1**   | Migration preset configurations                     | Config layer; productises accumulated knowledge              |
| **P2**   | Runbook/documentation generation                    | Template rendering from structured output                    |
| **P2**   | Stored routine tracking/preservation                | Detection is easy; full parsing is harder                    |
| **P2**   | External tool integration (pscale, mysqldump, etc.) | Subprocess wrappers                                          |
| **P3**   | Schema drift detection (continuous)                 | Built on live DB connections + diff                          |
| **P3**   | Online schema change integration (gh-ost)           | Significant scope; external tool dependency                  |
| **P3**   | MSSQL support for sample/shard/redact               | Dialect completeness                                         |
| **P4**   | Charset/collation conversion utilities              | Niche; most DBs are utf8mb4/UTF8                             |

### P0: Live Database Connections

Add database connections as an input source alongside file-based reading.
Schema extraction from `information_schema` enables pre-migration assessment
against live databases without requiring a dump file. Data streaming via
`SELECT * FROM table` with server-side cursors feeds into the existing INSERT
row pipeline for live-to-file and live-to-live operations.

### P0: Migration Plan Generator

Build on the existing schema diff engine to produce structured migration plans:
pre-data DDL, data migration, post-data DDL, cleanup, and finalization phases.
Each phase is annotated with hazards (data loss, downtime required, long-running,
irreversible) based on table row counts and schema complexity. Output formats
include raw DDL SQL, structured JSON/YAML plans, Markdown runbooks, and
CI/CD pipeline JSON.

### P1: PlanetScale-Aware Schema Validation

Add validation profiles that check dialect-specific rules: unique key
requirements, engine support, charset/collation allowlists, stored procedure
detection, and unsupported feature flags. Profiles include `planetscale`,
`aurora-mysql`, `standard-mysql`, and `cloud-postgres`.

### P1: Migration Data Verification

Two modes: table-level checksums for fast pass/fail, and chunked PK-range
comparison for large tables with detailed mismatch reporting. Complements
planner output as a post-migration verification step.

### P1: Migration Preset Configurations

Package migration knowledge as reusable YAML configuration files encoding
pre-flight commands, validation rules, conversion settings, migration plan
parameters, verification methods, and post-migration steps. A single
`--preset` flag replaces manual command chaining.

### Implementation Phasing

**Phase 1 (Foundation)**: Database source trait, schema-only reading,
`--db` flag on analyze/validate/graph commands, PlanetScale validation profile,
migration plan struct and JSON plan output.

**Phase 2 (Core)**: Data streaming from database connections, chunked
verification, hazard annotation, rollback plans, pre/post-flight checks,
`migrate` command with preset support.

**Phase 3 (Productionization)**: Runbook/report generation, routine/view
cataloging, subprocess integration, execution logging, resumability support,
additional validation profiles.

**Phase 4 (Ecosystem)**: MSSQL row-level support, charset/collation utilities,
CI/CD integration, online schema change detection (flag tables requiring
gh-ost/pt-osc, do not integrate the tools directly).

### Scope Boundary

The tool generates migration plans and verifies their execution. It does not
orchestrate external tools (`pscale`, `mydumper`, `pgloader`, `psql`) as
subprocesses. The `--execute` flag is bounded to running generated DDL on a
target database with a direct connection. Pipeline orchestration, if needed,
should be a separate tool consuming sql-splitter as a library.
