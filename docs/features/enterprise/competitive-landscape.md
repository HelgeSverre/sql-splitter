# Competitive Landscape Analysis — Database Migration Tooling

This document evaluates sql-splitter's competitive position against each category of database migration tool. Claims about competitor tools are verified against documentation, source code, or GitHub READMEs (Context7, August 2026).

---

## Executive Summary

**sql-splitter's unique market position**: The only multi-dialect, file-based CLI tool that can inspect, diff, convert, validate, and generate migration plans from SQL dumps — without requiring live database connections, a JVM, a Kafka cluster, or vendor lock-in.

**Closest competitor**: Atlas (`atlas schema diff` / `atlas migrate diff`) overlaps on schema diff → migration generation, but requires a live `--dev-url` database. sql-splitter works on dump files from any source, including platforms that block binlog access (PlanetScale).

**Key gap no one fills**: Generating structured, hazard-annotated migration plans from two SQL dump files, with cross-dialect conversion, rollback scripts, and data verification — in a single zero-dependency binary.

**No Rust tool exists in this space.** Every major migration tool is Go, Java, Python, or Common Lisp. sql-splitter is uniquely positioned as the Rust-native SQL migration CLI.

---

## 1. Schema Migration Tools

### 1.1 Flyway

| Aspect                           | Details                                                                                                                                                                |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Java (JVM required)                                                                                                                                                    |
| **Approach**                     | Version-continuous: numbered SQL/Java migration files tracked in version control                                                                                       |
| **Dialects**                     | 50+ (via JDBC)                                                                                                                                                         |
| **Key strengths**                | Industry standard; CI/CD integration; rollback support; extensive documentation                                                                                        |
| **Limitations**                  | No dump-file diffing; requires JVM; migrations must be hand-authored or generated via `diff-changelog` (Pro only); no heterogeneous data conversion                    |
| **sql-splitter overlap**         | Minimal. Flyway applies hand-crafted migrations; sql-splitter inspects and generates them. sql-splitter's `migrate` output (ALTER scripts) could be consumed by Flyway |
| **sql-splitter differentiation** | Works on dump files; no JVM required; multi-dialect conversion; streaming architecture; schema graph + FK-aware ordering                                               |
| **Verified claims**              | Context7 confirms Flyway's diff functionality, rollback commands, and changeset format                                                                                 |

### 1.2 Liquibase

| Aspect                           | Details                                                                                                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Java (JVM required)                                                                                                                                |
| **Approach**                     | Changelog-based (XML/YAML/JSON/SQL); tracks applied changesets in a `DATABASECHANGELOG` table                                                      |
| **Dialects**                     | 50+ (via JDBC)                                                                                                                                     |
| **Key strengths**                | `diff` and `diff-changelog` commands compare live DBs; cross-database changelogs; rollback tags; multiple output formats                           |
| **Limitations**                  | Requires live JDBC connections for diff; JVM dependency; complex XML changelog format; no file-based workflow; no data-level migration             |
| **sql-splitter overlap**         | Liquibase's `diff-changelog` is the closest overlap: schema diff → migration script output. sql-splitter does this from files instead of live DBs  |
| **sql-splitter differentiation** | File-based; no JVM; streaming for unlimited file sizes; cross-dialect conversion; data diff included; PlanetScale-compatible (no JDBC requirement) |
| **Integration potential**        | sql-splitter's `migrate` output could target Liquibase XML/YAML changelog format. Documented in `INTEGRATION_OPPORTUNITIES.md`                     |

### 1.3 Atlas (Ariga)

| Aspect                           | Details                                                                                                                                                                                                                                                  |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go                                                                                                                                                                                                                                                       |
| **Approach**                     | Declarative schema-as-code (HCL); diff engine between any two schema states; `schema diff`, `migrate diff`, `schema apply`                                                                                                                               |
| **Dialects**                     | MySQL, PostgreSQL, SQLite, MariaDB, MSSQL, TiDB, CockroachDB, ClickHouse                                                                                                                                                                                 |
| **Key strengths**                | Clean declarative model; migration linting; safety checks; `schema fmt`; Kubernetes operator; excellent diagramming                                                                                                                                      |
| **Limitations**                  | Requires `--dev-url` (live database or Docker container) for diff; community edition blocks schema test/plan/push and migrate checkpoint/down/rebase/rm/edit/push/test; no data diff; no heterogeneous conversion                                        |
| **sql-splitter overlap**         | **Highest overlap.** Both diff schemas → generate migration scripts. Both support multiple dialects. Both have schema graphs                                                                                                                             |
| **sql-splitter differentiation** | No `--dev-url` required (works on files); data diff included; heterogeneous conversion between all 4 dialects; streaming architecture for unlimited files; DuckDB query engine; Rust binary (no Docker/Go runtime needed); full PlanetScale exit support |
| **Integration potential**        | sql-splitter could export Atlas HCL schemas. Atlas could consume sql-splitter's migration plans. Documented in `INTEGRATION_OPPORTUNITIES.md`                                                                                                            |
| **Verified claims**              | Context7 confirms community edition blocked commands, `--dev-url` requirement, and supported dialects                                                                                                                                                    |

### 1.4 Skeema

| Aspect                           | Details                                                                                                                                                              |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go                                                                                                                                                                   |
| **Approach**                     | Declarative: file-per-object, pure-SQL repo; `skeema diff` compares file system to live DB; `skeema push` applies changes                                            |
| **Dialects**                     | MySQL, MariaDB only                                                                                                                                                  |
| **Key strengths**                | 20+ linter rules; pt-osc/gh-ost integration; safety guardrails; "empirical ALTER verification" (tests generated DDL); trusted by GitHub                              |
| **Limitations**                  | MySQL/MariaDB only; no PostgreSQL/SQLite/MSSQL; paid Premium edition for views/triggers/events; no heterogeneous conversion; no data migration; no file-to-file diff |
| **sql-splitter overlap**         | Both do schema diffing and linting. Both can generate ALTER statements                                                                                               |
| **sql-splitter differentiation** | 4 dialects (not 1); works on dump files; data diff; cross-dialect conversion; DuckDB analytics; all features in single binary (vs. premium tier for views/triggers)  |

### 1.5 Bytebase

| Aspect                           | Details                                                                                                                                                                               |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go (GUI platform, not a CLI)                                                                                                                                                          |
| **Approach**                     | Centralized schema change governance: GitOps, RBAC, approval workflows, 200+ SQL linting rules                                                                                        |
| **Dialects**                     | MySQL, PostgreSQL, TiDB, Snowflake, ClickHouse, MongoDB, MSSQL, Oracle, Spanner, BigQuery                                                                                             |
| **Key strengths**                | Full platform (GUI + API); CI/CD integration; multi-environment; VCS integration; Terraform provider                                                                                  |
| **Limitations**                  | GUI platform (not CLI-first); requires deployment of Bytebase server; no dump-file workflow; no cross-dialect conversion                                                              |
| **sql-splitter overlap**         | Bytebase's schema change workflow overlaps conceptually with what sql-splitter's `migrate` could enable, but Bytebase is a platform (GUI + server) while sql-splitter is a CLI binary |
| **sql-splitter differentiation** | Zero infrastructure (no server, no GUI); file-based; streaming for unlimited files; PlanetScale-aware; cross-dialect conversion                                                       |

---

## 2. Data Migration Tools

### 2.1 pgloader

| Aspect                           | Details                                                                                                                                                                                      |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Common Lisp                                                                                                                                                                                  |
| **Approach**                     | Streaming data loader; auto-discovers source schema; maps types; loads via PostgreSQL COPY protocol                                                                                          |
| **Dialects**                     | Source: MySQL, SQLite, MSSQL, CSV, fixed-width, etc. Target: PostgreSQL only                                                                                                                 |
| **Key strengths**                | One-command migration (`pgloader mysql://... postgresql://...`); drops/rebuilds indexes for speed; configurable CAST rules; handles zero dates; mature (10+ years)                           |
| **Limitations**                  | PostgreSQL target only (one-directional); no schema diff/generation; no migration script output; no PG → MySQL; ignores views/triggers/procs; Common Lisp runtime; no PlanetScale source     |
| **sql-splitter overlap**         | Both do MySQL → PostgreSQL type conversion. sql-splitter's `convert` command overlaps with pgloader's schema+type conversion                                                                 |
| **sql-splitter differentiation** | Bidirectional (6 conversion pairs, not 1 direction); schema diff + migration plan generation; data verification; 4 dialects; file-based (no live DB needed); single binary (no Lisp runtime) |
| **Integration potential**        | sql-splitter's `convert` output → pgloader for streaming import. sql-splitter's validation can verify pgloader output                                                                        |

### 2.2 mydumper / myloader

| Aspect                           | Details                                                                                                                                    |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Language**                     | C                                                                                                                                          |
| **Approach**                     | Parallel MySQL dump and restore with chunked table extraction                                                                              |
| **Dialects**                     | MySQL only                                                                                                                                 |
| **Key strengths**                | Parallel; snapshot-consistent; `--rows` chunking for large tables; compression; mature (recommended by PlanetScale docs and Laravel Cloud) |
| **Limitations**                  | MySQL only; no schema migration generation; no cross-dialect; no validation                                                                |
| **sql-splitter overlap**         | sql-splitter's `split` command generates per-table SQL in the same format. Both handle parallel output                                     |
| **sql-splitter differentiation** | 4 dialects; schema diff; data validation; cross-dialect conversion; migration plan generation; synthetic data; DuckDB analytics            |

### 2.3 Fivetran / Airbyte

| Aspect                           | Details                                                                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **Approach**                     | SaaS ELT platforms. Extract from source → load to data warehouse/destination. 300+ connectors (Airbyte). CDC support (Fivetran) |
| **Key strengths**                | Managed; scheduling; wide connector coverage; incremental sync; monitoring                                                      |
| **Limitations**                  | SaaS only (not a CLI tool); data pipelines, not DDL migration; no schema diff; no migration plan generation; latency dependent  |
| **sql-splitter overlap**         | Minimal. sql-splitter operates on SQL dumps; these are ETL/ELT platforms                                                        |
| **sql-splitter differentiation** | CLI-first; DDL/schema focus; migration plan generation; offline dump processing; zero-cost open source                          |
| **Integration potential**        | sql-splitter-prepared SQL dumps could be ingested via Airbyte connector. Documented in `INTEGRATION_OPPORTUNITIES.md`           |

---

## 3. CDC / Replication Tools

### 3.1 Debezium

| Aspect                           | Details                                                                                                                                                                                                           |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Java (Kafka Connect plugin)                                                                                                                                                                                       |
| **Approach**                     | Reads database change logs (binlog/WAL) → emits change events to Kafka topics                                                                                                                                     |
| **Dialects**                     | MySQL, PostgreSQL, MongoDB, SQL Server, Oracle, Db2, Cassandra, Vitess, Spanner                                                                                                                                   |
| **Key strengths**                | Low-latency CDC; Kafka integration; schema registry; monitoring; battle-tested                                                                                                                                    |
| **Limitations**                  | CDC only (not migration); requires Kafka + Zookeeper; no schema migration generation; no file/dump support; PostgreSQL UTF-8 encoding only; Spanner connector has limitations on streaming, routing, PG interface |
| **sql-splitter overlap**         | Minimal. sql-splitter does not do CDC. For migration verification, sql-splitter's checksums provide a post-hoc alternative to CDC-based verification                                                              |
| **sql-splitter differentiation** | Snapshot-based (not CDC); works without infrastructure; one binary; PlanetScale-compatible (Debezium cannot connect to PlanetScale — no binlog access)                                                            |
| **Verified claims**              | Context7 confirms UTF-8 limitation for PG connector, Spanner streaming/routing/PG interface limitations                                                                                                           |

### 3.2 Maxwell / Vitess VReplication / ClickPipes

| Tool                    | Key characteristic                                      | Overlap with sql-splitter               |
| ----------------------- | ------------------------------------------------------- | --------------------------------------- |
| **Maxwell**             | Java, MySQL binlog → JSON to Kafka                      | None (CDC only, MySQL only)             |
| **Vitess VReplication** | Powers Vitess online DDL, resharding, materialize views | None (Vitess subsystem, not standalone) |
| **ClickPipes**          | ClickHouse native CDC from Kafka → ClickHouse           | None (ClickHouse-specific)              |

---

## 4. Database Comparison Tools

### 4.1 pt-table-checksum (Percona Toolkit)

| Aspect                           | Details                                                                                                               |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Perl                                                                                                                  |
| **Approach**                     | Chunked checksum comparison between replication source and replicas; CRC32/MD5/SHA1                                   |
| **Dialects**                     | MySQL only                                                                                                            |
| **Key strengths**                | Checks replication consistency; fast (chunked); non-blocking on replicas                                              |
| **Limitations**                  | MySQL only; replica-to-source comparison only; no migration script generation; no schema diff                         |
| **sql-splitter overlap**         | sql-splitter's planned data verification (chunked checksums with PK-range partitioning) is the same algorithm pattern |
| **sql-splitter differentiation** | 4 dialects; schema diff; migration plan generation; file-based (no live DB); check mode + detail mode                 |

### 4.2 migra (DEPRECATED) / results

| Aspect                           | Details                                                                                                                                                                 |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Status**                       | **migra is deprecated.** Replaced by `results` (same author, repository `djrobstep/results`)                                                                            |
| **Approach**                     | Schema diff between two PostgreSQL databases → auto-generated migration SQL                                                                                             |
| **Dialects**                     | PostgreSQL only                                                                                                                                                         |
| **Key strengths**                | Used to be the simplest PG schema diff tool; magically generated ALTER scripts                                                                                          |
| **Limitations**                  | Deprecated; PostgreSQL only; no data diff; no file support; no multi-dialect                                                                                            |
| **sql-splitter overlap**         | **migra was the closest design precedent for sql-splitter's `migrate` command**: two schemas → ALTER scripts. But migra was PG-only, live-DB only, and is now abandoned |
| **sql-splitter differentiation** | 4 dialects; dump files (not live DBs); data diff; still actively maintained                                                                                             |

### 4.3 pg_comparator

| Aspect       | Details                                                                    |
| ------------ | -------------------------------------------------------------------------- |
| **Approach** | Table comparison with row-level diff output for PostgreSQL                 |
| **Overlap**  | sql-splitter's data diff is more comprehensive (schema + data in one tool) |

---

## 5. All-in-One Migration Platforms

### 5.1 CockroachDB MOLT

| Aspect                           | Details                                                                                                                                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go                                                                                                                                                                                                      |
| **Components**                   | Schema Conversion Tool (web-based), MOLT Fetch (bulk data), MOLT Replicator (CDC), MOLT Verify                                                                                                          |
| **Sources**                      | PostgreSQL, MySQL, Oracle, MSSQL                                                                                                                                                                        |
| **Target**                       | CockroachDB only                                                                                                                                                                                        |
| **Key strengths**                | Complete migration toolkit; schema conversion + data fetch + CDC replication + verification in one platform; PG 11–16, MySQL 5.7–8.4                                                                    |
| **Limitations**                  | CockroachDB target only (vendor lock-in); schema conversion is web-based (not CLI); no PlanetScale source                                                                                               |
| **sql-splitter overlap**         | MOLT's "schema conversion + fetch + verify" pipeline is conceptually closest to sql-splitter's "validate + convert + diff + verify" pipeline. Both are integration platforms spanning multiple concerns |
| **sql-splitter differentiation** | Target-agnostic (any MySQL/PG/SQLite/MSSQL, not just CockroachDB); all features in one CLI (not web UI + separate tools); streaming dump processing; 4 dialect targets (not 1); no vendor lock-in       |
| **Verified claims**              | Context7 confirms MOLT Schema Conversion Tool supports PG/MySQL/Oracle/MSSQL and can target CockroachDB Cloud directly                                                                                  |

### 5.2 YugabyteDB Voyager

| Aspect                           | Details                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go/C                                                                                                                                                            |
| **Approach**                     | Open-source data migration engine: offline + live migration, parallel processing, progress monitoring                                                           |
| **Sources**                      | PostgreSQL (offline + live), MySQL (offline only), Oracle (offline + live)                                                                                      |
| **Target**                       | YugabyteDB only                                                                                                                                                 |
| **Key strengths**                | Live migration for PostgreSQL/Oracle; parallel import; schema assessment; known-issue documentation (spatial types, numeric precision, table inheritance)       |
| **Limitations**                  | YugabyteDB target only; MySQL is offline-only (no live); no MSSQL support; no PlanetScale source; PG table inheritance unsupported; spatial types not supported |
| **sql-splitter overlap**         | Voyager's assessment → migration → verify pipeline is conceptually similar to sql-splitter's validate → convert → verify pipeline                               |
| **sql-splitter differentiation** | Target-agnostic; 4 dialects (including MSSQL and SQLite); streaming architecture; migration plan generation; synthetic data; zero vendor lock-in                |
| **Verified claims**              | Context7 confirms MySQL is offline-only, limitations on spatial types, numeric precision/scale issues, and PG table inheritance unsupported                     |

### 5.3 TiDB DM / TiDB Lightning

| Aspect                           | Details                                                                                                                                                                                                        |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Language**                     | Go                                                                                                                                                                                                             |
| **Approach**                     | DM: full + incremental replication from MySQL/MariaDB to TiDB. Lightning: high-speed bulk import for >1 TB datasets using mydumper format                                                                      |
| **Sources**                      | MySQL/MariaDB (DM), MySQL dump files via mydumper (Lightning)                                                                                                                                                  |
| **Target**                       | TiDB only                                                                                                                                                                                                      |
| **Key strengths**                | Sharded migration support (merge multiple shard sources); does work with Vitess-exported data (MySQL-compatible); Lightning for terabyte-scale imports                                                         |
| **Limitations**                  | TiDB target only; DM requires binlog access on source (TiDB can't pull from PlanetScale); Lightning is a bulk importer, not a migration planner                                                                |
| **sql-splitter overlap**         | Lightning reads mydumper-format files (same format sql-splitter produces). DM's incremental replication pattern is what sql-splitter's enterprise design could move toward (live DB source → stream to target) |
| **sql-splitter differentiation** | Target-agnostic (any MySQL/PG, not just TiDB); works on files (no binlog needed); schema diff; migration plans; 4 dialects; no TiDB dependency                                                                 |
| **Verified claims**              | Context7 confirms DM config for MySQL → TiDB, sharded migration support, and TiDB Lightning's mydumper-compatible input                                                                                        |

---

## 6. Cloud Provider Migration Services

### 6.1 AWS DMS (Database Migration Service)

| Aspect                           | Details                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Type**                         | Managed cloud service                                                                                                                                                                                                                                                                                                                                                           |
| **Approach**                     | Full load + ongoing CDC via replication instance (EC2-based); optional Schema Conversion Tool (SCT)                                                                                                                                                                                                                                                                             |
| **Key strengths**                | Broad source/target support; CDC for near-zero downtime; integrates with SCT for heterogeneous conversion                                                                                                                                                                                                                                                                       |
| **Limitations**                  | 4 GB binlog transaction limit; no triggers/views/procedures migration; no GTID-based replication; AUTO_INCREMENT not migrated; CASCADE FK operations not replicated; invisible columns skipped; LOB truncation risk; TIMESTAMP → UTC conversion mismatch; **no PlanetScale source** (no binlog access); **AWS now recommends native tools over DMS for same-engine migrations** |
| **sql-splitter overlap**         | sql-splitter's planned live DB connection + verify pipeline would provide a similar "compare source and target" capability, but file-based rather than service-based                                                                                                                                                                                                            |
| **sql-splitter differentiation** | No 4 GB transaction limit; works with PlanetScale exports; file-based; open source; one binary; no AWS account needed                                                                                                                                                                                                                                                           |
| **Verified claims**              | MOLT documentation explicitly states data is missing from DMS: AUTO_INCREMENT not migrated, CASCADE operations not replicated, 4 GB binlog limit, "AWS recommends homogeneous data migrations over DMS"                                                                                                                                                                         |

### 6.2 Google DMS / Azure DMS

| Service        | Key characteristic                                                                            | Overlap with sql-splitter                                  |
| -------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| **Google DMS** | Serverless, continuous migration to Cloud SQL/AlloyDB. Sources: MySQL, PG, Oracle, SQL Server | Target-specific (Google Cloud only). No PlanetScale source |
| **Azure DMS**  | Online + offline modes to Azure SQL/MySQL/PostgreSQL/Cosmos DB                                | Target-specific (Azure only)                               |

All cloud DMS services share the same fundamental limitations relative to sql-splitter: vendor-locked targets, require cloud accounts, cannot work with platforms that block binlog access (PlanetScale), and are opaque services rather than inspectable tools.

---

## 7. Is There a Rust Database Migration Tool?

| Tool                                        | Type                                                                                                    | Overlap                                                                                                                                                                                                                          |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **refinery** (rust-db/refinery, 1.7k stars) | Rust library + CLI for running migrations (Flyway-style). Embed SQL files or Rust modules in the binary | **Minimal.** refinery applies hand-authored migrations; sql-splitter generates them. refinery's approach (numbered migration files, applies to live DB) is in a different category. No schema diff, no conversion, no validation |
| **sqlx-cli** (launchbadge/sqlx)             | Rust ORM's migration runner. `sqlx migrate run` applies ordered SQL files                               | Migration runner, not generator. No overlap on sql-splitter's core features                                                                                                                                                      |
| **diesel-cli**                              | Rust ORM's migration runner. Schema.rs generation from live DB                                          | Schema extraction (narrow overlap). Live DB only. No multi-dialect, no migration generation                                                                                                                                      |
| **prisma migrate**                          | TypeScript, not Rust. Schema model → migration files                                                    | ORM-focused. Declarative with Prisma schema language. No overlap on dump-file processing                                                                                                                                         |

**No Rust tool exists in the SQL dump inspection/transformation/migration-planning space.** sql-splitter is the first.

---

## 8. Closest Competitor to sql-splitter's Proposed `migrate` Command

The proposed `migrate` command takes two SQL dumps (or a dump + live DB) and produces:

1. A comparison of schema + data differences
2. An ordered, hazard-annotated migration plan (DDL + data migration steps)
3. Rollback scripts
4. Breaking change detection
5. Output as SQL, JSON, or Markdown runbook

### Direct Competition

| Tool                           | How close                                                           | Key differentiator                                             |
| ------------------------------ | ------------------------------------------------------------------- | -------------------------------------------------------------- |
| **Atlas `migrate diff`**       | Closest functional match. Takes two schema states → migration files | Requires `--dev-url` (live DB or Docker). No dump-file support |
| **Liquibase `diff-changelog`** | Also generates migration scripts from two DBs                       | JVM-based; live DBs only; No file-based workflow               |
| **migra (deprecated)**         | Was the closest conceptual match: "like diff but for PG schemas"    | PG-only, live DB only, unmaintained since Sep 2022             |
| **CockroachDB MOLT**           | Closest in pipeline breadth (schema conversion + fetch + verify)    | CRDB-only target; web-based schema conversion; vendor lock-in  |

### sql-splitter's Differentiating Factors for `migrate`

1. **Works on dump files** — no live DB required. This is the single biggest differentiator. Every competitor requires a live connection for at least one side of the diff
2. **Streaming** — works on arbitrarily large dumps (tested with 100 GB+). Atlas/Liquibase must load the full schema into memory
3. **4 dialects** — MySQL, PostgreSQL, SQLite, MSSQL in one binary. The closest competitor (Atlas) also has multi-dialect, but is live-DB dependent
4. **Data diff** — compares row-level data between two dumps. Atlas doesn't diff data; Liquibase Pro does but via live DB
5. **PlanetScale exit path** — the only tool that can compare a PlanetScale export dump against a target without requiring binlog access
6. **Zero infrastructure** — single binary, no Docker, no JVM, no Kafka, no cloud account
7. **Hazard annotations** — migration plans include data loss warnings, downtime requirements, irreversibility flags, and index rebuild estimates

---

## 9. Market Gap

### The gap no one fills

**A tool that takes a SQL dump — from a managed service that doesn't expose binlogs, from a CI artifact, from a snapshot — and produces a complete, validated, verifiable migration plan with cross-dialect conversion, without requiring access to the source database.**

### Why this gap matters

1. **PlanetScale/Vitess exits**: PlanetScale blocks binlog access. AWS DMS, Debezium, Maxwell, and all CDC-based tools cannot extract from PlanetScale. The only export path is `pscale database dump` (file-based). sql-splitter is the only tool that can read that dump and produce a migration plan for any target

2. **Managed MySQL/PostgreSQL platforms**: Laravel Cloud, Neon, Fly.io Postgres, and similar platforms only provide internal snapshots (not downloadable). To migrate out, you restore a backup to a temp cluster and export — producing dump files that need processing. sql-splitter operates directly on those files

3. **CI/CD pipelines**: Migration artifacts stored as CI pipeline outputs can be diffed, validated, and converted without a live database. sql-splitter is pipeline-native — YAML generation is already in scope

4. **Vendor-agnostic migration**: Every "all-in-one" tool (CockroachDB MOLT, YugabyteDB Voyager, TiDB DM/Lightning) targets a specific destination. AWS/Google/Azure DMS targets their respective clouds. sql-splitter targets any MySQL, PostgreSQL, SQLite, or MSSQL database — open source or managed

5. **Offline/air-gapped environments**: Single binary with no external dependencies works in environments without internet access, Docker, JVM, or cloud connectivity

### Who needs this

| Persona                                                 | Use case                                                                    |
| ------------------------------------------------------- | --------------------------------------------------------------------------- |
| **Migration engineer** leaving a managed MySQL platform | Generate migration plan from dump file; validate; verify                    |
| **DevOps/platform engineer** in CI/CD                   | Diff production schema dump against dev; detect drift                       |
| **Site reliability engineer**                           | Verify backup integrity; compare snapshots across regions                   |
| **DBA in large enterprise**                             | Plan database version upgrades; assess risk of schema changes               |
| **Consultant** performing database migrations           | Convert + validate + diff in one tool; no need to install multiple runtimes |

---

## 10. Competitive Positioning Recommendations

### Primary positioning

> **sql-splitter is the SQL migration Swiss Army knife for databases you don't have full access to.**

### Messaging pillars

1. **File-first, live-optional**: Works where Atlas, Liquibase, Flyway, and DMS can't — on dump files, CI artifacts, and exports from managed platforms

2. **Vendor-agnostic**: Target any MySQL, PostgreSQL, SQLite, or MSSQL database. No lock-in to CockroachDB, YugabyteDB, TiDB, AWS, GCP, or Azure

3. **Zero infrastructure, one binary**: scp to the target machine and run. No JVM, no Docker, no Kafka, no cloud account

4. **Multi-dialect from day one**: MySQL ↔ PostgreSQL ↔ SQLite ↔ MSSQL in a single tool. 12 conversion pairs. 4 dialects for every command

5. **Streaming, not bounded**: Process 500 GB dumps the same way as 50 MB dumps — constant memory, no spills to disk unless required

### Secondary positioning (for Rust ecosystem)

> **The only Rust-native SQL dump processing toolkit.** Extensible as a library, usable as a CLI, embeddable in Rust applications that need multi-dialect SQL parsing, streaming, and reference data generation.

### What not to position against

- **CDC/replication tools** (Debezium, Maxwell, VReplication) — sql-splitter is snapshot-based, not CDC
- **ELT platforms** (Fivetran, Airbyte) — different category entirely
- **Bytebase** — different delivery model (platform vs. CLI)

### Recommended competitive edge to invest in

1. **Live DB connection** (P0 in roadmap) — unlocks the ability to diff dump ↔ live DB, which no tool currently combines with dump file support
2. **Migration plan generation** (P0) — makes sql-splitter the first file-based migration plan generator
3. **PlanetScale validation** (P1) — uniquely valuable for the fastest-growing pain point (PlanetScale exits)
4. **Chunked data verification** (P1) — complements the migration plan with trust verification
5. **Integration format exports** (Atlas HCL, Liquibase XML, Flyway SQL) — positions sql-splitter as the "generator" that feeds into established migration runners

---

## Appendix: Tool Quick Reference Table

| Tool               | Language | Dialects                                          | Live DB required? | Schema diff | Data diff | Cross-dialect | Dump file support           | Open source                           |
| ------------------ | -------- | ------------------------------------------------- | ----------------- | ----------- | --------- | ------------- | --------------------------- | ------------------------------------- |
| **sql-splitter**   | Rust     | MySQL, PG, SQLite, MSSQL                          | No (optional)     | ✅          | ✅        | ✅ 6 pairs    | ✅ Full                     | ✅ MIT/Apache-2.0                     |
| Atlas              | Go       | MySQL, PG, SQLite, MariaDB, MSSQL, TiDB, CRDB, CH | Yes (`--dev-url`) | ✅          | ❌        | ❌            | ❌                          | ✅ Community / ❌ Enterprise features |
| Flyway             | Java     | 50+ (JDBC)                                        | Yes (for diff)    | ✅          | ❌        | ❌            | ❌                          | ✅ Community / ❌ Teams/Enterprise    |
| Liquibase          | Java     | 50+ (JDBC)                                        | Yes (for diff)    | ✅          | ✅ Pro    | ❌            | ❌                          | ✅ OSS / ❌ Pro                       |
| Skeema             | Go       | MySQL, MariaDB                                    | Yes               | ✅          | ❌        | ❌            | ❌                          | ✅ Community / ❌ Premium             |
| pgloader           | CL       | → PG only                                         | Yes (source)      | ❌          | ❌        | ✅ (→PG)      | ❌                          | ✅                                    |
| mydumper           | C        | MySQL                                             | Yes               | ❌          | ❌        | ❌            | ❌ (generates dump files)   | ✅                                    |
| Debezium           | Java     | 12+                                               | Yes (binlog/WAL)  | ❌          | ❌        | ❌            | ❌                          | ✅                                    |
| MOLT               | Go       | → CRDB only                                       | Yes               | ✅ (web)    | ✅        | ✅ (→CRDB)    | Partial (Fetch reads dumps) | ✅                                    |
| YB Voyager         | Go/C     | → YB only                                         | Yes               | ✅          | ✅        | ✅ (→YB)      | Partial                     | ✅                                    |
| TiDB DM            | Go       | → TiDB only                                       | Yes               | ❌          | ❌        | ❌            | Partial (Lightning)         | ✅                                    |
| migra (deprecated) | Python   | PG only                                           | Yes (both)        | ✅          | ❌        | ❌            | ❌                          | ✅ (deprecated)                       |
| AWS DMS            | Service  | Broad                                             | Yes               | ❌          | ❌        | With SCT      | ❌                          | ❌ (managed)                          |

---

_Claims about competitors verified against Context7 documentation (Atlas, Flyway, Liquibase, Debezium, pgloader, CockroachDB MOLT, YugabyteDB Voyager, TiDB) and GitHub READMEs (Skeema, refinery, migra), August 2026._
