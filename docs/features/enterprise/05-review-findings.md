# Part 7: Review Findings and Mitigations

> Five-agent adversarial review of the migration execution design
> (§6), cross-referenced with Percona blog research. Every finding has
> a mitigation integrated into the design.

## 7.1 Critical Findings — Must Fix Before MVP

### CF-1: INSERT IGNORE Swallows Data Errors Silently

**Source**: Operational review (§5) + Correctness review (§2, §4)

`INSERT IGNORE` (MySQL) and `ON CONFLICT DO NOTHING` (Postgres) skip rows
for **any** error: PK conflicts, UNIQUE violations, NOT NULL violations,
CHECK constraint failures, type conversion errors, FK violations in strict
mode — all silently become "nothing." The migration reports success while
data is missing. There is no way to distinguish "valid duplicate row" from
"corrupted row that failed CHECK constraint."

Additionally, on tables **without a unique key**, `INSERT IGNORE` is
semantically identical to `INSERT` — re-running the migration **doubles
all rows** with no error or warning.

**Mitigation (three-part)**:

1. **Switch to `INSERT ... ON DUPLICATE KEY UPDATE`** for PK-based dedup
   only. This only skips duplicate PKs, not invalid data. Invalid rows fail
   with proper error codes.

```sql
-- Instead of (broad, swallows everything):
INSERT IGNORE INTO orders (id, name, total) VALUES (1, 'A', 100.0);

-- Use (narrow, only dedup PK conflicts):
INSERT INTO orders (id, name, total) VALUES (1, 'A', 100.0)
ON DUPLICATE KEY UPDATE id = VALUES(id);
-- Row succeeds: PK 1 already exists → no-change update, no data loss
-- Row fails: CHECK constraint violation → ERROR 3819 (visible, logged)
```

2. **Pre-flight unique-key check**: Every table to be migrated must have at
   least one unique constraint. If not, idempotency is disabled for that
   table and the user is warned. For MySQL→MySQL without unique keys,
   re-running the migration is unsafe.

3. **Add `--strict-insert` flag**: Uses regular `INSERT` (no IGNORE, no ON
   DUPLICATE) — the migration fails fast on the first problem row instead
   of silently skipping. Good for debugging and pre-production verification
   runs.

### CF-2: No Runtime Row Type Converter Exists

**Source**: Correctness review (§3)

The design says cross-dialect data migration routes rows through
`TypeMapper::convert()`. But `TypeMapper` operates on SQL **text** strings
(via regex) — it converts column types in CREATE TABLE DDL, not row
**values** at runtime. The `convert` module's `convert_insert` method only
does identifier quoting + string escaping — no type mapping on values.

The T1–T8 failure modes in §6.2 (TINYINT→BOOLEAN coercion, UNSIGNED BIGINT
overflow, zero-date conversion, DECIMAL precision, JSON validity, float
tolerance) are **all unimplementable** with the current codebase. There is
no infrastructure for runtime value coercion.

**Mitigation**: New `RowTypeConverter` module:

```rust
// src/migrate/convert.rs (new)
pub struct RowTypeConverter {
    column_converters: Vec<ColumnConverter>,
    dialect_pair: (SqlDialect, SqlDialect),
}

enum ColumnConverter {
    /// No conversion needed (same type on both dialects)
    Passthrough,
    /// TINYINT(1) → BOOLEAN: 0→false, non-zero→true
    TinyIntToBool,
    /// UNSIGNED BIGINT → NUMERIC(20): detect overflow
    UnsignedBigIntToNumeric,
    /// DATETIME '0000-00-00...' → NULL
    ZeroDateToNull,
    /// ENUM → VARCHAR: passthrough value as string
    EnumToVarchar,
    /// DECIMAL precision: check range, truncate if exceeds target
    DecimalPrecisionCheck { max_precision: u8, max_scale: u8 },
    /// JSON → JSONB: validate and compact
    JsonValidate,
}

impl RowTypeConverter {
    /// Build converter from source and target tables in the migration plan
    pub fn from_plan(plan: &MigrationPlan) -> Self;

    /// Convert a batch of rows, collecting per-row errors
    pub fn convert_batch(&self, batch: &mut RowBatch) -> Vec<ConvertError>;
}
```

This is a substantial new domain module — not a Phase 4 refinement. It must
be built as part of the core data import path.

### CF-3: SQL_MODE Differences Cause Silent Row Loss

**Source**: Connection review (§6.1)

If the target has MySQL strict mode ON and the source has values that
violate constraints (zero dates, out-of-range values, invalid defaults),
`INSERT IGNORE` silently skips the row. The migration reports success but
data is missing. The verification catches this only after the entire
migration completes.

**Mitigation**:

1. **Pre-flight**: Query `SELECT @@sql_mode` on source and target. Warn if
   they differ. Specifically flag `STRICT_TRANS_TABLES` differences.

2. **Session-level override**: On the target connection, temporarily set
   `SET sql_mode = ''` during data import (removes strict mode, converts
   errors to warnings). After import, restore the original sql_mode.

3. **Warning capture**: After every INSERT batch, run `SHOW WARNINGS LIMIT
1000` and log all warnings as `MIG-ROW-WARNING` diagnostics. These
   include: truncated values, out-of-range values adjusted, invalid dates
   coerced to zero, division by zero.

4. **Post-import check**: After data import, compare source and target row
   counts per table. If the target has fewer rows than the source (counting
   only rows that should have been imported given idempotent dedup),
   surfaces it as a verification failure.

```sql
-- Before data import:
SET @old_sql_mode = @@sql_mode;
SET sql_mode = '';

-- After data import:
SET sql_mode = @old_sql_mode;
```

### CF-4: Throughput Model Inflated 3-8×

**Source**: Performance review (§1)

The design claims MySQL single-connection INSERT throughput of "~25 MB/s"
and 4 workers → "~80 MB/s." Realistic numbers for the `mysql` crate
INSERT path (text protocol, per-statement batching) are **5-12 MB/s**
single-connection. At 12 MB/s, a 2TB migration on 4 workers takes **~48
hours** — not the sub-day figure implied.

Fathom achieved 700K-1M rows/sec by using cross-DB `INSERT INTO target
SELECT * FROM source` — server-internal transfer with no client-side data
movement. The sql-splitter design uses per-connection INSERT, where every
byte crosses the client twice (source→client→target). This is 10× slower.

**Mitigation**:

1. **Add `--copy-mode direct` for cross-DB optimization**: When source and
   target are the same database type and on the same server (or connected
   via FOREIGN DATA WRAPPER), use `INSERT INTO target.table SELECT * FROM
source.table`. This is the Fathom path. Requires schema alignment
   first (matching column names, types).

2. **Realistic throughput documentation**: Model throughput as 5-15 MB/s per
   connection for MySQL INSERT, 40-80 MB/s for PostgreSQL COPY. Provide a
   duration estimator: `estimated_hours = source_size_gb * 1024 / (throughput_mbps * 3600)`.

3. **Benchmark tooling**: Add `--bench` flag that measures actual INSERT
   throughput against the target before migration begins (10K test rows,
   timed). Reports: "Measured INSERT throughput: 8.2 MB/s. Estimated
   migration time for 2.1 TB: 73 hours with 1 connection, 18 hours with 4."

### CF-5: Neon (Serverless Postgres) Scale-to-Zero + PgBouncer Kills Migration

**Source**: Connection review (§3.3, §3.4)

Neon (Serverless Postgres) suspends compute after 5 minutes of inactivity
(scale-to-zero). During suspension, all connections are closed, session
state is lost, and prepared statements are dropped. When compute wakes, the
reconnected session has no context. Additionally, PgBouncer in `transaction`
mode means `SET` statements do not persist between transactions — `SET
session_replication_role = replica` (to disable triggers) lasts for one
transaction only.

**Mitigation**:

1. **Pre-flight**: If target is Neon (Serverless Postgres) (detected via
   `SELECT current_setting('neon.version')`), either require
   `--neon-disable-suspend` (which calls the Neon API to set
   `suspend_timeout_seconds=0`), or refuse to execute.

2. **Direct connections only**: Neon (Serverless Postgres) migration
   requires direct (non-pooled) connections. Pooled connections (PgBouncer)
   are rejected during pre-flight with a clear error.

3. **Per-transaction session setup**: Before every INSERT transaction, emit:

```sql
SET session_replication_role = 'replica';
SET idle_in_transaction_session_timeout = 0;
```

4. **Reconnection loop**: On connection drop, the worker reconnects,
   re-applies session settings, and continues from the last committed chunk
   (chunk-level idempotency, not row-level).

### CF-6: VTGate Kills Long-Running SELECTs — Mandatory Chunking

**Source**: Operational review (§8)

PlanetScale VTGate kills individual queries that exceed a timeout (~60s
scalar, ~300s team/enterprise). A `SELECT * FROM huge_table` running for
2 hours will be killed regardless of connection keepalive. The design's C7
suggests keepalive queries, which only prevent **connection** idle timeout,
not **query** execution timeout.

**Mitigation**:

1. **For PlanetScale sources, `--chunk-column` is mandatory.** Pre-flight
   check detects PlanetScale (VTGate-specific error or hostname pattern),
   and requires `--chunk-column` for any table estimated to exceed 60s of
   query time. Without it, the migration refuses to start.

2. **Auto-detect chunk column**: If `--chunk-column` is not specified, the
   executor queries `EXPLAIN SELECT * FROM table LIMIT 1` to find the
   table's sort key, and uses that as the default chunk column.

3. **Cap parallel workers**: For PlanetScale sources, `--parallel` is
   capped at 2 regardless of level size. VTGate query multiplexing means
   parallel `SELECT` cursors compete for the same underlying resources.

### CF-7: Parallel Workers Must Enforce FK_DISABLE

**Source**: Operational review (§4)

The parallel import design (§6.3b) does not explicitly state that each
worker connection executes `SET FOREIGN_KEY_CHECKS = 0` before data import.
Without this, INSERTs into child tables fail because parent rows haven't
been imported yet. The existing `transform_common.rs` emits this for file
output, but the live-DB path has no equivalent guarantee.

**Mitigation**:

1. **Every worker connection** executes session-level FK disable on startup:

```sql
-- MySQL
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;          -- for idempotency performance
SET autocommit = 0;             -- batch commits

-- PostgreSQL
SET session_replication_role = 'replica';  -- disables triggers + FK checks
```

2. **Worker drain on failure**: When any worker fails, all workers in the
   current level are signaled to stop via `AtomicBool`. They finish their
   current batch (to avoid partial INSERTs), then drain. No new batches
   are started after the shutdown signal.

3. **Orphan check after aborted level**: After an aborted level, run FK
   integrity checks on affected tables before proceeding to the next level:

```sql
-- Check for orphaned child rows
SELECT child_table.*
FROM child_table
LEFT JOIN parent_table ON child_table.parent_id = parent_table.id
WHERE parent_table.id IS NULL
LIMIT 100;
```

If orphans are found, log them and offer the user the choice to delete
them or stop.

---

## 7.2 High-Severity Findings — Fix Before Production

### HF-1: Column Rename Detected as DROP + ADD (Data Loss)

**Source**: Correctness review (§1)

`compare_schemas()` compares columns by name. A renamed column is reported
as "old column dropped, new column added" — losing the column's data
during migration. The `differ/schema.rs:357-361` `compare_columns` function
has no rename detection heuristic.

**Mitigation**: Add a rename heuristic to `compare_tables()`:

```rust
// After identifying unmatched old and new columns:
// If an old column and a new column have:
//   1. Same ordinal position (or adjacent)
//   2. Same data type (ColumnType matches)
//   3. Different names
// → Flag as "possible rename" with a warning.
//   Replace DROP old + ADD new with RENAME COLUMN in the migration plan.
```

This is a heuristic, not a guarantee — two independent DROP/ADD operations
with the same type at the same position could be falsely detected as a
rename. The warning makes the ambiguity explicit.

### HF-2: Self-Referential FK Tables Need Intra-Table Row Ordering

**Source**: Correctness review (§2)

`SchemaGraph` excludes self-referential FK edges at `graph.rs:51`. The
table-level topo-sort handles multi-table ordering correctly. But within a
self-referential table (`employees.manager_id → employees.id`), rows must
be imported in the correct order: rows with `manager_id IS NULL` first,
then rows referencing those, etc. Otherwise INSERTs fail because the
referenced `manager_id` row doesn't exist yet on the target.

**Mitigation**: Before importing a self-referential table:

1. Build an intra-table dependency graph: for each row, check if its FK
   value references a PK that exists in the same batch or earlier batches.
2. Sort rows within the table: NULL FK rows first, then rows referencing
   previously-inserted rows. This is a topological sort within a single
   table.
3. If the self-referential FK is configured with `ON DELETE SET NULL` or
   `ON UPDATE CASCADE`, the import is safe in any order (FK is enforced
   after import, not during).

```sql
-- Safe: NULL FK rows first, then rows with valid references
INSERT INTO employees (id, manager_id, name)
SELECT id, manager_id, name
FROM source_employees
ORDER BY CASE WHEN manager_id IS NULL THEN 0 ELSE 1 END, id;
```

### HF-3: Post-Data DDL Must Run Sequentially, Not Through Worker Pool

**Source**: Correctness review (§6) + Operational review (§4)

The pipeline diagram conflates "Phase 3" (data import) and post-data DDL
(done). Post-data DDL (ADD CONSTRAINT, CREATE INDEX, ADD FK) must run on a
**single connection, sequentially**, after all data import is complete.
If dispatched through the parallel worker pool, multiple workers would
attempt to add constraints to different tables simultaneously — some of
which might fail because the constraint references a table that another
worker hasn't finished importing.

**Mitigation**:

1. Add explicit **Phase 3b: Post-data DDL** between data import and
   verification. Runs sequentially. Single connection.

2. For FK constraints on PostgreSQL, use `NOT VALID` to avoid table scans:

```sql
ALTER TABLE orders ADD CONSTRAINT fk_orders_users
  FOREIGN KEY (user_id) REFERENCES users(id) NOT VALID;
-- Then validate later (can be done online):
ALTER TABLE orders VALIDATE CONSTRAINT fk_orders_users;
```

3. For large indexes, use `CREATE INDEX CONCURRENTLY` on PostgreSQL to
   avoid blocking writes (if target is live).

### HF-4: Verification Must Use Same Chunking as Data Import

**Source**: Performance review (§5) + Operational review (§5)

Verification streams all rows from source and target, computing SHA-256
checksums. This takes ~22 hours for a 2TB database (reading 2TB × 2
sources). If the connection drops at 95%, the checksum is lost and must
restart from row 1. The design's chunk-column infrastructure (§6.3c) is
not applied to verification.

**Mitigation**:

1. **Verification uses the same chunk boundaries as data import.** Each
   chunk's checksum is computed independently. If verification fails
   mid-chunk, only that chunk is re-computed.

2. **Chunk-level checksums are written to state file.** On resume, the
   executor skips chunks already verified.

3. **Add `--verify-mode rowcount`** as a fast alternative. Compares
   `SELECT COUNT(*)` on source vs target per table. This is what Fathom
   used — index-only scans, seconds per table. Use full checksums only
   when deep verification is needed.

### HF-5: Binlog Disk Space 2× Underestimated

**Source**: Operational review (§6)

Every INSERT writes to the MySQL binlog on the target. A 2TB import
generates ~2TB of additional binlog data. Combined with the 2TB of
actual data, that's 4TB of disk needed. The design's pre-flight disk
check only accounts for data size, not binlog overhead, doubling the
actual requirement.

On managed MySQL, storage can only increase, never decrease, and
only once per 6 hours. A 2TB import + 2TB binlog = 4TB → if the user
provisioned 3TB based on the estimate, the import fails midway with a
disk-full error that cannot be recovered without increasing storage and
waiting 6 hours.

**Mitigation**:

1. **Pre-flight disk estimate**: `estimated_disk = data_size × 2` for
   MySQL targets with binlog enabled. Warn prominently.

2. **Binlog handling flag**: `--binlog-handling` with options:
   - `log` (default): use `SET SESSION sql_log_bin = 0` if available
     (requires SUPER or SYSTEM_VARIABLES_ADMIN — not available on
     managed MySQL; services typically don't grant SUPER privilege).
   - `purge`: set `SET GLOBAL binlog_expire_logs_seconds = 3600` before
     import (requires SUPER).
   - `warn-only`: estimate disk needs and warn, but don't change settings.

3. **For managed MySQL specifically**: document that binlog space
   cannot be avoided (no SUPER privilege, no GLOBAL variable access).
   Recommend provisioning storage at 2× data size. Pre-flight check
   queries `information_schema.TABLES.DATA_LENGTH` for source data size
   and compares against `information_schema.TABLES.DATA_FREE` + remaining
   volume capacity on target.

### HF-6: mysql Crate Binary Protocol vs Text Protocol

**Source**: Connection review (§1.2, §1.3)

The design uses `exec_iter()` which uses MySQL's **binary** protocol
(prepared statements). This means: (a) each unique `SELECT * FROM table`
creates a prepared statement on the server, (b) BIT/ENUM/SET/GEOMETRY types
have surprising binary representations that differ from text dumps, (c) no
server-side cursor support limits chunking.

**Mitigation**: Switch to `query_iter()` (text protocol):

```rust
// Instead of (binary protocol, prepared statements):
let mut result = self.conn.exec_iter(&query)?;

// Use (text protocol, same format as mysqldump):
let mut result = self.conn.query_iter(&query)?;
```

Text protocol returns human-readable text — identical to what `mysqldump`
produces. Performance difference for bulk extraction is negligible (text
is actually faster for decoding since it uses the same code path as the
file parser). No server-side prepared statement exhaustion.

### HF-7: Error Type Must Follow GenerateError Pattern

**Source**: Integration review (§5)

The migration executor needs both fatal errors (connection refused) and
non-fatal diagnostics (row-level type conversion warnings). The existing
codebase has two patterns: `anyhow::Result` (linear, stringy) and
`GenerateError` wrapping `DiagnosticBag` (structured, multi-error).

**Mitigation**: Model after `GenerateError`:

```rust
pub struct MigrateOutcome {
    pub plan: MigrationPlan,
    pub stats: MigrateStats,
    pub diagnostics: DiagnosticBag,
}

pub enum MigrateError {
    Diagnostics(DiagnosticBag),     // structured multi-error
    Io(std::io::Error),             // connection failures
    Config(String),                 // invalid CLI arguments
}
```

The `run()` function returns `Result<MigrateOutcome, MigrateError>`.
Non-fatal warnings are collected in `MigrateOutcome.diagnostics`. Fatal
errors are in `MigrateError`. This matches the generate module's
established pattern at `src/generate/mod.rs:330`.

---

## 7.3 Percona Research Insights

### PI-1: SAFE_NO_LOCK Consistency Verification

**Source**: Percona blog — "MyDumper Locking Mechanisms Revisited" (July 2026)

MyDumper's `SAFE_NO_LOCK` mode captures the binlog/GTID position at the
start of a parallel snapshot, lets all threads establish consistent reads
via `START TRANSACTION WITH CONSISTENT SNAPSHOT`, then compares binlog
positions after all threads sync. If they diverge → immediately aborts
instead of silently producing an inconsistent backup.

**Application**: When sql-splitter reads schema from a live MySQL source via
`information_schema`, it should capture the GTID position before and after
schema extraction. If the schema changed during extraction (detected by
comparing `information_schema.TABLES.UPDATE_TIME` divergence or schema
hash), emit a `MIG-SCHEMA-CHANGED` warning. Don't silently continue with
a snapshot of a moving target.

### PI-2: pt-archiver Chunked Processing for Large Tables

**Source**: Percona blog — "Extending pt-archiver with a Partition-Aware Plug-in" (June 2026)

pt-archiver processes rows in configurable chunks with `--limit`,
`--commit-each`, avoiding massive transactions, replication lag, and lock
escalation. The partition-aware plugin drops entire partitions (metadata-only
operation) instead of executing row-by-row DELETEs.

**Application**:

1. The chunk size in sql-splitter's `--chunk-size` should default to a
   conservative 100,000 rows, not 1,000. MyDumper's default `--rows` is
   10,000 for InnoDB tables with integer PKs — the design's 10,000 row
   batches for INSERT output align with this practice.
2. For RANGE-partitioned tables, detect partition boundaries and offer to
   export/import by partition rather than row-by-row. This is a Phase 4
   optimization but should be flagged in the plan as "partition-aware
   migration possible."

### PI-3: MySQL 8.0 EOL Confirmed — Version Detection Critical

**Source**: Percona blog — "Still on MySQL 5.7 or 8.0?" (July 2026)

Percona Server for MySQL 8.0 reached EOL in April 2026. Percona offers
Extended Lifecycle Support (out-of-schedule CVE fixes). Production systems
are still predominantly on 8.0.

MySQL 9.7 is the LTS release following 8.0. Each major version adds
reserved keywords and removes deprecated features.

**Application**: The pre-flight check must query `SELECT VERSION()` on
source and target, and if the source version > target version with
breaking changes:

1. Flag reserved keywords in column/table names that changed between
   versions.
2. Warn about `utf8mb3` removal (MySQL 8.0 default `utf8` is `utf8mb3`;
   MySQL 8.4+ `utf8` is `utf8mb4`).
3. Detect `FLOAT(M,D)` / `DOUBLE(M,D)` with non-standard precision (removed
   in 8.0.17, deprecated in 8.4).

### PI-4: PostgreSQL NOT VALID Constraints for Large Migrations

**Source**: Percona migration guide (2023) + correctness review

PostgreSQL supports `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` —
creating an FK or CHECK constraint without validating existing data. Then
`ALTER TABLE ... VALIDATE CONSTRAINT` validates existing rows without
blocking writes (requires `SHARE UPDATE EXCLUSIVE` lock, not `ACCESS
EXCLUSIVE`).

**Application**: Phase 3b (post-data DDL) should use `NOT VALID` for FK
constraints on PostgreSQL targets, then validate after. This avoids a
full table scan during the migration window. The validation can run
asynchronously after the migration is "complete."

### PI-5: Safe FK Disable/Restore Pattern

**Source**: Synthesized from multiple Percona and review sources

The established pattern for bulk data loads with FKs:

```
1. SET FOREIGN_KEY_CHECKS = 0;
2. Import all data in FK order (parents before children)
3. SET FOREIGN_KEY_CHECKS = 1;
4. Run validation: SELECT * FROM child LEFT JOIN parent ON ...
   WHERE parent.id IS NULL;
5. If orphans found: either fix or document
```

**Application**: Explicitly document this as the pattern sql-splitter
follows. The executor emits step 1 at worker startup, step 2 during
data import, and steps 3-5 during verification. The `--verify fk` flag
runs orphan checks even if `--verify checksum` is not requested.

---

## 7.4 Updated Failure Mode Catalog

Additional failure modes from the reviews, not in the original §6.2:

| #   | Failure                                                                  | Detection                                                                       | Recovery                                                                                                                                                                     |
| --- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P13 | `exec_iter()` exhausts server prepared statement cache (binary protocol) | MySQL error 1461 "Can't create more than max_prepared_stmt_count statements"    | Switch to `query_iter()` text protocol (CF-1 mitigation).                                                                                                                    |
| P14 | Source buffer pool eviction from full table scan                         | Buffer pool hit rate drops below 50% during migration                           | Use `--chunk-column` to convert full scans to index range scans. Pre-flight: if `--chunk-column` not set and `estimated_source_read_gb` > 10, warn about buffer pool impact. |
| P15 | Neon PgBouncer session reset between INSERT batches                      | `SET` statement effects lost; triggers fire on target; FK checks re-enabled     | Per-transaction SET preamble (CF-5 mitigation). Reject pooled connections during pre-flight.                                                                                 |
| P16 | mydumper SAFE_NO_LOCK detects inconsistency mid-dump                     | Binlog position divergence across parallel dump threads                         | sql-splitter reads already-dumped files. If reading from a live dump-in-progress, detect inconsistent metadata and abort.                                                    |
| P17 | MySQL 8.0 → 8.4 reserved keyword collision                               | Column named `MANUAL`, `PARALLEL`, `QUALIFY`, or `TABLESAMPLE` in source schema | Pre-flight version comparison + keyword scan against target's reserved word list.                                                                                            |
| P18 | `ON CONFLICT DO NOTHING` fails syntactically without conflict arbiter    | PostgreSQL error 42601                                                          | Pre-flight unique-key check (CF-1 mitigation part 2).                                                                                                                        |
| P19 | INSERT batch exceeds `max_allowed_packet` (large BLOB rows)              | MySQL error 1153 "Got a packet bigger than max_allowed_packet"                  | Dynamic batch sizing: estimate wire-size before sending; split batch if exceeds server limit. Query `@@max_allowed_packet` at connection start.                              |
| P20 | Chunk column has skewed distribution (one value = 90% of data)           | Per-worker throughput diverges 10:1                                             | Skew detection during boundary discovery (§6.3c). Subdivide oversized chunks.                                                                                                |

---

## 7.5 Updated Performance Model

Realistic throughput numbers (replaces §6.3b performance table):

| Target                       | Single conn INSERT | 4 workers INSERT | 8 workers INSERT | PostgreSQL COPY |
| ---------------------------- | ------------------ | ---------------- | ---------------- | --------------- |
| MySQL 8.0 gp3 3000 IOPS      | 5-10 MB/s          | 12-25 MB/s       | 18-35 MB/s       | N/A             |
| MySQL 8.4 gp3 16000 IOPS     | 8-15 MB/s          | 20-40 MB/s       | 30-55 MB/s       | N/A             |
| MySQL 8.4 io2 64000 IOPS     | 10-20 MB/s         | 30-55 MB/s       | 45-80 MB/s       | N/A             |
| PostgreSQL 17 gp3 3000 IOPS  | N/A                | N/A              | N/A              | 30-60 MB/s      |
| PostgreSQL 17 io2 64000 IOPS | N/A                | N/A              | N/A              | 60-120 MB/s     |

**2TB migration duration estimate** (MySQL 8.4, gp3 16000, 4 workers):

```
Throughput: 20-40 MB/s
Duration: 2,097,152 MB / 20-40 MB/s = 14.6-29.1 hours
With verification (checksum): adds 8-16 hours (reading 2TB × 2 sources)
With verification (rowcount): adds ~5 minutes
```

These are measured targets, not estimates. The `--bench` flag validates
them against the actual target before migration begins.

---

## 7.6 Architecture Decision: No `pscale` Subprocess in Core Executor

The reviews consistently surface PlanetScale as requiring special treatment:
mandatory chunking, VTGate query timeouts, diverse source access patterns.
The design already separated PlanetScale into `PlanetScaleSource` (§5.2.6)
as a file-based wrapper around `pscale database dump` output.

**Decision**: The core executor never invokes `pscale` as a subprocess.
It reads PlanetScale data from mydumper-compatible dump files. The
`pscale database dump` step is a pre-condition, documented in the
migration runbook, but not automated by sql-splitter. This keeps the
executor's dependency surface clean and avoids the operational complexity
of managing PlanetScale CLI auth, subprocess timeouts, and dump-file disk
space provisioning.

The `pscale://` URI scheme (described in §5.2.6) is a convenience wrapper
for local development, not a production migration feature.

---

## 7.7 Updated Implementation Sequencing

The findings reorder the implementation phases:

| Phase       | Contents                                                                                                                                                                                                                                    | Dependency  |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| **Phase 0** | `DbSource` trait + `MySqlSource` + `PostgresSource` (schema-only). Uses `query_iter()` text protocol. Schema extraction via `information_schema` batch queries (NOT per-table).                                                             | None        |
| **Phase 1** | `MigrationPlan` generation from `SchemaDiff`. Column rename heuristic. Self-referential FK ordering. `NOT VALID` for PG constraints. Version-aware keyword checking.                                                                        | Phase 0     |
| **Phase 2** | `DbTarget` trait + `MySqlTarget` + `PostgresTarget`. `RowTypeConverter` for cross-dialect data. FK/unique-check disable per worker connection. `INSERT ... ON DUPLICATE KEY` idempotency. `sql_mode` session override. `--binlog-handling`. | Phase 0     |
| **Phase 3** | `migrate --execute` with live data import. `--chunk-column` with boundary discovery and skew detection. `--staging-strategy`. Sequential Phase 3b DDL.                                                                                      | Phase 1 + 2 |
| **Phase 4** | Verification: chunk-aware checksums, rowcount mode. Percona-style SAFE_NO_LOCK schema-change detection. `--bench` throughput measurement.                                                                                                   | Phase 3     |
| **Phase 5** | `--parallel` with level-based worker pool. Intra-table parallelism via chunk-column. Partition-aware import. `--binlog-handling purge`.                                                                                                     | Phase 3     |
