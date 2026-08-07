# Part 6: Migration Execution Design

> How `sql-splitter migrate` runs a full migration — design, adversarial
> review, failure-mode catalog, enterprise readiness, testing, benchmarking,
> and observability infrastructure.

## 6.1 The Migration Executor

### Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     sql-splitter migrate                          │
│                                                                   │
│  --source mysql://host/db                                         │
│  --target mysql://host/db                                         │
│  [--output migration.sql] [--execute] [--verify]                  │
└──────────────────────────────┬───────────────────────────────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   ┌──────────┐    ┌────────────────────┐    ┌──────────────┐
   │ Extract  │    │ Generate Plan      │    │ Execute DDL  │
   │ Schemas  │───▶│ (dry-run default)  │───▶│ (--execute)  │
   └──────────┘    └────────────────────┘    └──────┬───────┘
                                                    │
                                        ┌───────────┴───────────┐
                                        ▼                       ▼
                                   ┌──────────┐         ┌──────────────┐
                                   │ Migrate  │         │ Verify       │
                                   │ Data     │────────▶│ (--verify)   │
                                   └──────────┘         └──────────────┘
```

### Schema Extraction (Phase 0 in implementation plan)

```
source_schema = DbSource::extract_schema(source)
target_schema = DbSource::extract_schema(target)
```

For file sources: `Schema::from_sql_file(path, dialect, progress_fn)` —
already exists.

For live DB sources: `MySqlSource::extract_schema()` /
`PostgresSource::extract_schema()` using `information_schema` queries.

For PlanetScale sources: `pscale database dump` → read mydumper output →
`Schema::from_sql_file()`. Schema extraction from PlanetScale is metadata-only
(information_schema via VTGate) or uses the dump files.

### Plan Generation (always runs)

```rust
let diff = compare_schemas(&source_schema, &target_schema, &config);
let plan = MigrationPlan::from_diff(diff, &source_schema, &target_schema);
```

The plan is always produced and displayed. It contains:

| Section           | Contents                                                                          |
| ----------------- | --------------------------------------------------------------------------------- |
| Source summary    | Table count, row estimates, dialect, total size estimate                          |
| Target summary    | Same as source                                                                    |
| Pre-flight checks | Connectivity, permissions, disk space, charset compatibility, FK orphan detection |
| Pre-data ops      | CREATE TABLE, ADD COLUMN (with defaults), DROP CONSTRAINT, DROP INDEX             |
| Data ops          | Per-table row counts, type conversions needed, estimated duration                 |
| Post-data ops     | ADD CONSTRAINT, CREATE INDEX, ADD FK                                              |
| Cleanup ops       | DROP TABLE, DROP COLUMN (if applicable)                                           |
| Hazards           | DataLoss, DowntimeRequired, LongRunning, Irreversible                             |
| Rollback plan     | Inverse operations for each operation                                             |

Output formats:

| `--format`       | Output                                                        |
| ---------------- | ------------------------------------------------------------- |
| `plan` (default) | Structured JSON with phases, hazards, estimates               |
| `sql`            | Raw DDL script (backward compatible with `diff --format sql`) |
| `text`           | Human-readable with color-coded additions/removals            |
| `runbook`        | Markdown runbook for customer-facing documentation            |

### DDL Execution (`--execute`)

For each phase, in FK dependency order. Post-data DDL (Phase 3b) runs on
a **single connection** (not the worker pool) with FK checks disabled to
handle cycle constraints:

```sql
-- Phase 3b connection startup:
SET FOREIGN_KEY_CHECKS = 0;
-- Run all ADD CONSTRAINT / ADD FK operations
-- (Handles cycles: a→b→c→d→a all exist by now,
--  so FK creation succeeds despite cyclic references)
SET FOREIGN_KEY_CHECKS = 1;
```

For each phase, in FK dependency order (`SchemaGraph::processing_order()`):

```rust
for op in phase.operations_in_topo_order() {
    match op {
        CreateTable { table, sql } => target.execute_ddl(&render_idempotent(sql)),
        AddColumn { table, sql } => target.execute_ddl(&render_idempotent(sql)),
        DropConstraint { table, sql } => target.execute_ddl(&sql),
        CreateIndex { table, sql } => target.execute_ddl(&render_idempotent(sql)),
        // ...
    }
    progress.update(op);
}
```

Idempotency: every DDL statement is rendered in an idempotent form so that
re-running the migration after a failure is safe:

| Operation       | MySQL idempotent form                                       | Postgres idempotent form                            |
| --------------- | ----------------------------------------------------------- | --------------------------------------------------- |
| CREATE TABLE    | `CREATE TABLE IF NOT EXISTS`                                | `CREATE TABLE IF NOT EXISTS`                        |
| ADD COLUMN      | Check `information_schema.COLUMNS` first; skip if exists    | Same, or use `DO $$ BEGIN ... EXCEPTION ... END $$` |
| CREATE INDEX    | Check `information_schema.STATISTICS` first; skip if exists | `CREATE INDEX IF NOT EXISTS`                        |
| DROP TABLE      | `DROP TABLE IF EXISTS`                                      | `DROP TABLE IF EXISTS`                              |
| DROP CONSTRAINT | `ALTER TABLE ... DROP FOREIGN KEY` (check exists first)     | `ALTER TABLE ... DROP CONSTRAINT IF EXISTS`         |
| CREATE FK       | Check `information_schema.KEY_COLUMN_USAGE` first           | Same                                                |

### Data Migration (`--execute`)

For each table, in FK dependency order. Self-referential tables
(employees.manager_id → employees.id) require intra-table row ordering:
NULL-FK rows first, then rows referencing previously-inserted PKs:

```rust
for table_op in plan.data_operations_in_topo_order() {
    let order_clause = if table_op.is_self_referential() {
        // "SELECT ... FROM table ORDER BY
        //  CASE WHEN fk_col IS NULL THEN 0 ELSE 1 END, pk_col"
        table_op.self_ref_ordering()
    } else {
        None
    };
    let source_rows = source.stream_table_rows(
        &table_op.table, order_clause.as_deref()
    )?;
    if table_op.needs_conversion() {
        // Route through TypeMapper for cross-dialect type conversion
        let converted = convert_rows(source_rows, &table_op.conversions);
        target.insert_rows(&table_op.table, &table_op.columns, converted)?;
    } else {
        target.insert_rows(&table_op.table, &table_op.columns, source_rows)?;
    }
}
```

Data import uses idempotent INSERT. Unlike `INSERT IGNORE` (which silently
swallows ALL errors including type violations and CHECK constraint failures),
the executor uses unique-key-targeted dedup:

| Dialect    | INSERT form                                                                         | Notes                                                                                                                                                                 |
| ---------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MySQL      | `INSERT INTO table (cols) VALUES (...) ON DUPLICATE KEY UPDATE pk = VALUES(pk)`     | Fires on ANY unique-key conflict, not just PK. The plan metadata lists only PK columns, but pre-existing UNIQUE constraints on other columns also trigger the clause. |
| PostgreSQL | `INSERT INTO table (cols) VALUES (...) ON CONFLICT (pk_cols) DO NOTHING`            | Only the named arbiter columns trigger dedup. Explicit PK column list is required.                                                                                    |
| SQLite     | `INSERT OR IGNORE INTO table (cols) VALUES (...)` (no PK-targeted syntax available) | Suppresses ALL errors — not just PK conflicts. Pre-flight must verify the table has a unique key before enabling idempotency; otherwise re-run doubles rows silently. |
| MSSQL      | `MERGE INTO table USING (VALUES (...)) AS src ON ... WHEN NOT MATCHED THEN INSERT`  | Only the MERGE ON condition triggers dedup.                                                                                                                           |

Unique-key-targeted dedup means only rows matching the listed conflict
columns are skipped. For MySQL, any UNIQUE constraint on the table also
triggers `ON DUPLICATE KEY` — this is best-effort, not PK-exclusive.
conversion errors, CHECK violations, NOT NULL violations, and FK
violations all produce proper errors that are collected and reported
(rather than silently swallowed as they would be with `INSERT IGNORE`).

If a table has no unique key, idempotent INSERT is impossible — the
executor refuses to start and the pre-flight check requires the user to
either add a unique key or use `--no-idempotent` (which makes re-running
unsafe, as rows will be doubled).

For cross-dialect where the target PK column names differ from the source,
the `ON DUPLICATE KEY UPDATE` form uses the target's column names. The
`ON CONFLICT` form explicitly names the PK columns from the plan metadata.

Batch size: 1000 rows per INSERT (configurable via `--batch-size`, matching
the existing `generate --batch-size` convention).

### Verification (`--verify`)

```rust
for table in plan.migrated_tables() {
    let source_hash = table_checksum(source, &table);
    let target_hash = table_checksum(target, &table);
    if source_hash != target_hash {
        report.record_mismatch(&table, source_hash, target_hash);
    }
}
```

The checksum algorithm uses incremental SHA-256 over all rows, sorted by PK
for deterministic output. See §3.3 Gap 3 of the capability audit for the
algorithm design.

### Execution Modes

| Mode               | CLI                                                    | What happens                                    |
| ------------------ | ------------------------------------------------------ | ----------------------------------------------- |
| **Plan only**      | `migrate --source X --target Y`                        | Extract schemas, generate plan, print it, exit  |
| **SQL output**     | `migrate --source X --target Y --output migration.sql` | Generate SQL file for manual execution          |
| **Live execution** | `migrate ... --execute --target mysql://...`           | Connect to target, run DDL, stream data, verify |
| **Dry run target** | `migrate ... --execute --dry-run`                      | Simulate execution against target schema checks |
| **Verify only**    | `migrate ... --verify --target mysql://...`            | Compare source and target, report mismatches    |

### The `DbTarget` Trait

```rust
pub trait DbTarget {
    /// Execute a DDL statement. Returns immediately — no result rows.
    fn execute_ddl(&mut self, sql: &str) -> Result<()>;

    /// Bulk-insert rows into a table from a streaming iterator.
    /// Each RowBatch is one multi-row INSERT statement (1000 rows).
    /// Returns total rows inserted.
    fn insert_rows(
        &mut self,
        table: &str,
        columns: &[String],
        rows: &mut dyn Iterator<Item = Result<RowBatch>>,
    ) -> Result<u64>;

    /// Execute a session-level command (SET variables, etc.).
    fn execute_session_cmd(&mut self, sql: &str) -> Result<()>;

    /// Begin/commit a transaction. Postgres can wrap DDL phases in
    /// transactions. MySQL ignores this — DDL implicitly commits.
    fn begin_transaction(&mut self) -> Result<()>;
    fn commit_transaction(&mut self) -> Result<()>;

    /// Check if a database object exists (for idempotency checks).
    fn table_exists(&mut self, table: &str) -> Result<bool>;
    fn column_exists(&mut self, table: &str, column: &str) -> Result<bool>;
    fn index_exists(&mut self, table: &str, index: &str) -> Result<bool>;
    fn constraint_exists(&mut self, table: &str, constraint: &str) -> Result<bool>;
}
```

Implementations: `MySqlTarget` (mysql crate v28, synchronous), `PostgresTarget`
(postgres crate v0.19, synchronous). Same crate choices as `DbSource` —
synchronous drivers, no async runtime.

### CLI Surface

```bash
# Plan only — what would happen?
sql-splitter migrate \
  --source postgres://user@host:5432/source \
  --target mysql://user@host:3306/target

# Generate SQL file for manual review
sql-splitter migrate \
  --source dump.sql \
  --target mysql://user@host:3306/target \
  --output migration.sql --format sql

# Execute directly
sql-splitter migrate \
  --source mysql://replica:3306/source \
  --target mysql://user:3306/target \
  --execute --tables users,orders,products

# Execute with destructive operations
sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --execute --allow-destructive

# Cross-dialect with verification
sql-splitter migrate \
  --source mysql://source/db \
  --target postgres://target/db \
  --execute --verify checksum

# Dry run (plan generation against live target)
sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --dry-run

# Runbook generation for customer
sql-splitter migrate \
  --source dump.sql \
  --target mysql://target/db \
  --format runbook --output customer-runbook.md
```

---

## 6.2 Adversarial Review: Failure-Mode Catalog

Every failure mode must have a detection mechanism and a recovery strategy.
This catalog covers the full migration lifecycle from connection to
verification.

### Connection and Authentication Failures

| #   | Failure                                                 | Detection                                   | Recovery                                                                                                                                                   |
| --- | ------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C1  | Source unreachable (firewall, DNS, VPN)                 | TCP connect timeout (10s default)           | Retry 3x with exponential backoff (5s/10s/20s), then exit with actionable message                                                                          |
| C2  | Target unreachable                                      | Same as C1                                  | Same as C1                                                                                                                                                 |
| C3  | Authentication rejected (wrong password)                | MySQL error 1045, PG error 28P01            | Exit immediately, no retry (password is wrong, not transient)                                                                                              |
| C4  | SSL/TLS handshake failure                               | Native TLS layer error                      | Exit with certificate details; suggest `--ssl-mode verify-full`, `verify-ca`, `require`, or `disable`                                                      |
| C5  | SSL certificate expired                                 | TLS layer error (certificate verify failed) | Exit with cert expiry date; suggest `--ssl-mode require` to bypass verification (with warning)                                                             |
| C6  | Connection dropped mid-migration (network partition)    | Read/Write error on socket                  | Data import: re-run (idempotent INSERT skips already-imported rows). DDL: target is in partial state; report what was completed, suggest manual assessment |
| C7  | PlanetScale VTGate timeout on long-running SELECT       | Connection closed after VTGate idle timeout | Detect VTGate-specific error; suggest using replica/read-only region; stream with keepalive queries every 30s                                              |
| C8  | PlanetScale service token expired                       | `pscale auth` error                         | Exit with message to refresh token; do not attempt retry                                                                                                   |
| C9  | managed MySQL target scale-to-zero wakes mid-connection | Connection reset during wake                | Retry 3x with exponential backoff; wake takes ~500ms, so first retry usually succeeds                                                                      |

### Schema Extraction Failures

| #   | Failure                                                                                                               | Detection                                                                       | Recovery                                                                                                                          |
| --- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| S1  | `information_schema` query times out (thousands of tables)                                                            | Query timeout (30s default)                                                     | Log warning, suggest `--tables` filter to narrow scope                                                                            |
| S2  | `SHOW CREATE TABLE` produces PlanetScale hash-suffixed FK names                                                       | Detect `_fk_` + 26+ alphanumeric characters                                     | Log warning: "FK constraint names contain PlanetScale deployment hashes; target will get stable names"                            |
| S3  | Source has tables with no primary key or unique key                                                                   | `table.primary_key.is_empty()` + no unique constraint with all NOT NULL columns | Log error (PlanetScale target: hard failure; standard MySQL target: warning)                                                      |
| S4  | Source has unsupported charset (target is MySQL 5.7, source has `utf8mb4_0900_ai_ci`)                                 | Collation mismatch against target allowlist                                     | Log error: "Source uses charset X which is not supported by target Y; convert tables to compatible charset"                       |
| S5  | Source has reserved keywords as column names (MySQL 8.4 new keywords: `MANUAL`, `PARALLEL`, `QUALIFY`, `TABLESAMPLE`) | Compare column names against target's reserved word list                        | Log warning: "Column 'X' is a reserved keyword in target version Y; quoting will be applied but application code may need update" |
| S6  | Source schema changes during extraction (active production DB)                                                        | Compare `information_schema.TABLES.UPDATE_TIME` before and after extraction     | Log warning: "Source schema changed during extraction; extracted schema may be inconsistent"                                      |

### Type Conversion Failures

| #   | Failure                                                                             | Detection                                              | Recovery                                                                                                                                                                                          |
| --- | ----------------------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T1  | `TINYINT(1)` → `BOOLEAN`: source has values 2, 3, -1                                | Row value > 1 or < 0 for boolean target                | Log warning, convert: `false` for 0, `true` for everything else                                                                                                                                   |
| T2  | `UNSIGNED BIGINT` > 2^63-1 → Postgres `NUMERIC(20)`                                 | Value overflow check before INSERT                     | Log error per row; skip row and continue (collect skipped-count)                                                                                                                                  |
| T3  | `ENUM('a','b','c')` → `VARCHAR(255)`: source has value 'd'                          | Value not in ENUM definition (corrupt or altered ENUM) | Log warning; insert value as-is into VARCHAR (accommodates corruption)                                                                                                                            |
| T4  | `DATETIME '0000-00-00 00:00:00'` → Postgres `TIMESTAMP`                             | Value matches zero-date pattern                        | Convert to `NULL` (standard pgloader behavior); log warning with row count                                                                                                                        |
| T5  | `DECIMAL(65,30)` → Postgres `NUMERIC`: exceeds `NUMERIC(1000)`                      | Precision check against PG max                         | Log error: "Column X has precision 65 which exceeds Postgres NUMERIC limit (1000 digits); value will be inserted as-is" (PG supports up to 131072 digits before decimal)                          |
| T6  | MySQL `JSON` → Postgres `JSONB`: invalid JSON in source                             | JSON parse error                                       | Log warning per row; skip row or insert as text with warning                                                                                                                                      |
| T7  | MySQL `utf8` (3-byte) column contains 4-byte emoji → Postgres `UTF8` (full Unicode) | Row-level UTF-8 decode error                           | This only fails on MySQL-to-Postgres when the source actually stored 4-byte chars in a 3-byte column (MySQL doesn't enforce charset at row level); log warning, insert with replacement character |
| T8  | `FLOAT`/`DOUBLE` precision mismatch between dialects                                | Floating-point comparison after round-trip             | Warning in plan: "Floating-point values may have minor precision differences; row-level checksum comparison uses tolerance"                                                                       |

### Data Import Failures

| #   | Failure                                                                                            | Detection                                                                        | Recovery                                                                                                                  |
| --- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| D1  | INSERT fails due to NOT NULL column with no default                                                | MySQL error 1364, PG error 23502                                                 | Log error: "Column X in table Y has no default value; source row contained NULL. Check whether target schema is correct." |
| D2  | INSERT fails due to FK violation (orphaned row references non-existent parent)                     | MySQL error 1452, PG error 23503                                                 | Log error with FK detail; skip row; this is a data integrity issue in the source, not a migration bug                     |
| D3  | INSERT fails due to duplicate unique key (idempotent mode should prevent this)                     | Should not occur with ON DUPLICATE KEY UPDATE / ON CONFLICT (pk_cols) DO NOTHING | If it occurs, the idempotency mechanism is broken — this is a bug                                                         |
| D4  | INSERT fails due to UNIQUE constraint violation from collation difference                          | MySQL error 1062, PG error 23505                                                 | Log error: "Duplicate key violation on table Y, constraint Z. Source and target collations may differ."                   |
| D5  | INSERT fails due to CHECK constraint violation                                                     | MySQL error 3819, PG error 23514                                                 | Log error with CHECK expression; skip row                                                                                 |
| D6  | Target runs out of disk space during import                                                        | Disk full error from DB driver                                                   | Exit immediately; report how many tables were imported; suggest increasing target storage                                 |
| D7  | Target binary log fills up during import                                                           | MySQL error writing to binlog                                                    | Suggest `SET SQL_LOG_BIN=0` (requires SUPER) or `--skip-binlog` on target; warn that this breaks replication              |
| D8  | Target connection pool exhausted (too many concurrent INSERTs)                                     | "Too many connections" error                                                     | Reduce concurrency; migration executor only uses 1 connection by default (not parallelized in Phase 1)                    |
| D9  | Index rebuild blocks INSERTs (large indexes on target)                                             | INSERT timeout                                                                   | Drop non-PK indexes before import, recreate after (pgloader strategy); flag in plan                                       |
| D10 | Trigger fires on target during INSERT                                                              | Trigger execution on target                                                      | Disable triggers before import (`SET session_replication_role = replica;` for Postgres); re-enable after                  |
| D11 | Auto-vacuum on Postgres target interferes with import                                              | Import throughput drops mid-import                                               | Temporarily disable auto-vacuum on target tables during import (`ALTER TABLE ... SET (autovacuum_enabled = false)`)       |
| D12 | Large BLOB/TEXT values cause INSERT to exceed `max_allowed_packet` (MySQL) or statement size limit | Packet size error                                                                | Chunk large values across multiple INSERT statements; configurable `--max-rows-per-insert`                                |

### PlanetScale-Specific Failures

| #   | Failure                                                              | Detection                                                         | Recovery                                                                                                                         |
| --- | -------------------------------------------------------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| P1  | `pscale database dump` not installed                                 | `which pscale` returns empty                                      | Exit with instructions to install: `brew install planetscale/tap/pscale` or download from GitHub                                 |
| P2  | `pscale` not authenticated                                           | `pscale database dump` exits with auth error                      | Exit with instructions to run `pscale auth login` or set `PLANETSCALE_SERVICE_TOKEN`                                             |
| P3  | `pscale database dump` times out (large database, slow network)      | Subprocess timeout after configurable duration (default: 4 hours) | Exit with partial results; suggest using `--tables` to split dump into batches                                                   |
| P4  | `pscale database dump` output format changed                         | mydumper format mismatch (new fields, different structure)        | Parser detects unexpected structure; log error with version compatibility note                                                   |
| P5  | PlanetScale import-only: tables lack unique keys                     | PlanetScale returns error on CREATE TABLE without unique key      | Pre-flight check catches this in validation phase; error before any data is moved                                                |
| P6  | PlanetScale import-only: FK constraint names auto-suffixed on deploy | Deploy request renames FK constraints                             | Warn in plan: "PlanetScale renames FK constraints on deploy; application code referencing constraint names by string will break" |

### Verification Failures

| #   | Failure                                         | Detection                                                             | Recovery                                                                                      |
| --- | ----------------------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| V1  | Checksum mismatch for a table                   | SHA-256 comparison returns different hashes                           | Run chunked verification for that table; report which PK ranges differ                        |
| V2  | Row count mismatch                              | `COUNT(*)` on source vs target                                        | Report delta; if target has fewer rows, some INSERTs failed silently                          |
| V3  | FK integrity failure on target (orphaned rows)  | `SELECT * FROM child LEFT JOIN parent ON ... WHERE parent.id IS NULL` | Report orphaned rows per table; caused by D5 (skipped parent rows) or D9 (disabled triggers)  |
| V4  | Sequence value mismatch (auto-increment offset) | `SELECT MAX(id)` vs `SELECT nextval('seq')`                           | Report delta; suggest `ALTER SEQUENCE ... RESTART WITH` or `ALTER TABLE ... AUTO_INCREMENT =` |
| V5  | Index missing on target                         | Compare `information_schema.STATISTICS` between source and target     | Report missing indexes; these were skipped during import for performance and not re-created   |

### Partial State Recovery

| #   | Failure                                                           | Detection                                                                | Recovery                                                                                                                                    |
| --- | ----------------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| R1  | Migration interrupted after DDL but before data import            | Target has schema but no data (or partial data)                          | Re-run migration; idempotent DDL skips already-created tables; idempotent INSERT skips already-imported rows                                |
| R2  | Migration interrupted mid-DDL (MySQL: partial ALTER TABLE)        | Target may have partially applied ALTER (MySQL DDL is not transactional) | Report which DDL operations were completed (from execution log); user must manually assess and fix                                          |
| R3  | Migration interrupted mid-DDL (Postgres: transaction rolled back) | Postgres DDL is transactional                                            | Re-run migration; the failed phase's DDL is entirely rolled back; clean state                                                               |
| R4  | Migration interrupted mid-data-import                             | Some rows imported, some not                                             | Re-run migration; idempotent INSERT skips already-imported rows (PK-based dedup)                                                            |
| R5  | User kills migration process (SIGINT/SIGTERM)                     | Signal handler catches interrupt                                         | Graceful shutdown: flush current INSERT batch, close connections, write execution log to `--state-file`                                     |
| R6  | Migration process OOM-killed                                      | OS kills process; no recovery data saved                                 | No recovery possible without state file; report what was known to be completed from the most recent execution log written to `--state-file` |

### Security Failures

| #   | Failure                                                                 | Detection                                       | Recovery                                                                                   |
| --- | ----------------------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------ |
| X1  | Credentials visible in `ps aux` (password via `--target-password` flag) | Detection at CLI parse time                     | Warn if password provided via CLI arg; recommend env var or config file                    |
| X2  | Credentials logged to file (debug mode writes SQL to log)               | Structured logging PII redaction                | All credential fields are redacted in logs; host:port only                                 |
| X3  | Plaintext connection (no SSL) on untrusted network                      | SSL negotiation failure or `--ssl-mode disable` | Warn: "Connection is not encrypted. Credentials and data are visible on the network."      |
| X4  | Temp dump files contain PII and persist after migration                 | Temp file cleanup on exit                       | Write to temp dir with `0700` permissions; register cleanup on SIGINT/SIGTERM/process exit |

---

## 6.3 Enterprise-Grade Readiness Assessment

### What Makes a Migration Tool Enterprise-Ready

| Requirement                       | Status                | Notes                                                                                                                                                                                |
| --------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Idempotent execution**          | Designed (§6.2 R1-R4) | `INSERT ... ON DUPLICATE KEY UPDATE` (MySQL) / `ON CONFLICT (pk_cols) DO NOTHING` (PG) — PK-targeted dedup, not INSERT IGNORE. Tables without unique keys require `--no-idempotent`. |
| **Graceful shutdown**             | Designed (§6.2 R5)    | SIGINT handler, flush current batch, write state file                                                                                                                                |
| **Execution log**                 | Designed (§6.7)       | Per-phase progress, per-table row counts, timestamps, structured JSON                                                                                                                |
| **Dry-run mode**                  | Designed              | Plan generation against live target without executing                                                                                                                                |
| **Pre-flight checks**             | Designed              | Connectivity, permissions, charset compatibility, FK orphan detection, disk space estimate                                                                                           |
| **Hazard annotations**            | Designed              | DataLoss, DowntimeRequired, LongRunning, Irreversible                                                                                                                                |
| **Rollback plan**                 | Designed              | Inverse operations per phase                                                                                                                                                         |
| **SQL output mode**               | Designed              | Generates migration SQL for manual review and execution                                                                                                                              |
| **TLS everywhere**                | Designed (§6.2 X3)    | `verify-full` by default; configurable sslmode                                                                                                                                       |
| **Credential safety**             | Designed (§6.2 X1-X4) | No CLI arg passwords in ps output; env var and config file support; PII redaction in logs                                                                                            |
| **No SUPER privilege required**   | Designed              | All operations use standard DDL/DML; session-level settings only where available                                                                                                     |
| **PII handling in temp files**    | Designed (§6.2 X4)    | 0700 permissions, cleanup on exit                                                                                                                                                    |
| **SIGINT-safe**                   | Designed (§6.2 R5)    | Flush current batch, close connections, write state                                                                                                                                  |
| **Progress reporting**            | Designed (§6.7)       | Per-phase, per-table, bytes/sec, rows/sec, ETA                                                                                                                                       |
| **Non-interactive mode**          | Designed              | `--execute` runs without prompts; `--allow-destructive` for DROP operations                                                                                                          |
| **Exit codes**                    | Designed              | 0 = success, 1 = runtime error, 2 = CLI error, 3 = verification failure, 4 = partial success                                                                                         |
| **Staging strategy for safe DDL** | Designed (§6.3d)      | Rename-safe table swaps for MySQL; direct DDL for Postgres                                                                                                                           |
| **Chunk-column override**         | Designed (§6.3c)      | Intra-table parallelism via sort-key chunking; skew detection; Fathom-style "site bundles"                                                                                           |
| **Resource limits**               | Designed              | `--chunk-size`, `--batch-size`, `--tables` filter, bounded memory via streaming                                                                                                      |

### What's Not Enterprise-Ready Yet (Phase 4+)

| Gap                                                | Why                                                                       | When                 |
| -------------------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| Parallel data import (multi-connection)            | Designed (§6.3b, §6.3c); implementation requires TopoLevels + worker pool | Phase 3              |
| Staging-table strategy for DDL changes             | Designed (§6.3d); implementation requires RENAME TABLE generation         | Phase 2              |
| Online schema change integration (gh-ost/pt-osc)   | Requires binary log access and external tool awareness                    | Phase 4              |
| Incremental/CDC migration (no dump window)         | Requires binary log replication or logical replication slot               | Not in roadmap       |
| Multi-tenant isolation in managed service          | Separate compute, KMS, network isolation                                  | Managed service only |
| Regulatory compliance reporting (GDPR audit trail) | Immutable audit log, customer-facing compliance report                    | Managed service only |
| automated rollback execution                       | Rollback plan is generated but not executed automatically                 | Phase 4              |

---

## 6.3b Parallel Import Design

### Why Parallel Import Matters

A single connection feeding INSERT statements is network-bound. For a 500 GB
database, sequential import can take hours. Parallel import divides the work
across multiple connections, each writing to an independent subset of tables.
This is the same model as `src/writer/mod.rs`'s `ParallelWriters` — but
targeting live database connections instead of files.

### Key Constraint: FK Dependency Ordering

Tables with foreign keys must be imported in topological order: parents before
children. But tables at the same dependency level (neither depends on the
other) can be imported in parallel. The `SchemaGraph::processing_order()`
already computes topological levels via Kahn's algorithm.

```
Topo-sort: [region, country, city, users, orders, order_items, payments]
           \_______/  \____/        \_______________________________/
            Level 0    Level 1       Level 2 (independent of each other)
```

Tables in the same level have no FK edges between them and can be imported
concurrently. Tables across levels must be serialized (level 1 starts after
level 0 completes).

### Level-Based Parallelism

The algorithm extends Kahn's to group tables by dependency depth:

```rust
pub fn topo_levels(&self) -> TopoLevels {
    // Standard Kahn's algorithm, but instead of just appending to `order`,
    // we group tables by the "wave" in which they become available.
    //
    // Wave 0: tables with in_degree == 0 (no dependencies)
    //   Process all wave-0 tables. For each, decrement children's in_degree.
    // Wave 1: tables whose last parent was in wave 0
    //   Process all wave-1 tables...
    // ...and so on.
}

pub struct TopoLevels {
    pub levels: Vec<Vec<TableId>>,  // each inner vec is a parallel-importable group
    pub cyclic_tables: Vec<TableId>, // as before
}
```

For the example above:

```json
{
  "levels": [
    ["region", "users"], // wave 0
    ["country"], // wave 1 (depends on region)
    ["city"], // wave 2 (depends on country)
    ["orders", "edge_case_types", "wide_table"], // wave 3 (depends on users)
    ["order_items", "payments"] // wave 4 (depends on orders)
  ],
  "cyclic_tables": []
}
```

### The `ParallelDbTarget` Adaptor

Wraps a connection pool and dispatches tables to connections by shard:

```rust
pub struct ParallelDbTarget {
    workers: Vec<DbTargetWorker>,
    shard_assignments: HashMap<String, usize>,  // table → worker index
    level_queue: Vec<Vec<TableImportJob>>,       // jobs grouped by topo level
    state: Arc<ParallelImportState>,             // shared progress + error state
}

struct DbTargetWorker {
    conn: Box<dyn DbTarget>,     // one connection per worker
    thread: JoinHandle<Result<()>>,
    rx: Receiver<TableImportJob>,
}

struct TableImportJob {
    table: String,
    columns: Vec<String>,
    source_rows: Box<dyn Iterator<Item = Result<RowBatch>> + Send>,
}
```

### Execution Flow

```
1. Build TopoLevels from SchemaGraph
2. Spawn N worker threads, each with its own DB connection
3. For each level (sequentially):
   a. Distribute the level's tables across workers (striped by table name hash)
   b. Each worker opens a source cursor for its assigned tables
   c. Workers stream rows from source → INSERT batches to target
   d. Workers report progress to shared state
   e. Wait for all workers in this level to complete
   f. If any worker failed: collect errors, report, abort remaining levels
4. After all levels: return aggregate stats
```

### Worker Count Tuning

The number of parallel connections is configurable via `--parallel N`:

```bash
sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --execute --parallel 4
```

Default: `min(4, tables_in_level)` — auto-tuned per level. The effective
parallelism is capped by the number of independent tables at each level.

### Connection Pool vs. Dedicated Connections

| Approach                                   | Pros                                                 | Cons                                                                        |
| ------------------------------------------ | ---------------------------------------------------- | --------------------------------------------------------------------------- |
| **Connection pool** (r2d2, deadpool)       | Automatic connection reuse; handles reconnects       | Pool may give two workers the same connection; transaction isolation harder |
| **Dedicated connections** (one per worker) | No contention; simple transaction model; predictable | More connections to target (N = parallel count); may hit connection limits  |

**Recommendation: dedicated connections.** Each worker thread opens one
connection at startup, holds it for the entire migration, and closes it on
shutdown. The connection limit on a MySQL target ranges from 51 (512 MB) to
3,264 (32 GB) — even 8 parallel workers is a tiny fraction of the limit.

### Error Isolation Between Workers

A failing worker must not corrupt other workers' progress. Every worker
connection starts with session-level safety settings:

```sql
-- MySQL: executed on every worker connection at startup
SET FOREIGN_KEY_CHECKS = 0;    -- allow child inserts before parent imports
SET UNIQUE_CHECKS = 0;         -- faster idempotent INSERT performance
SET autocommit = 0;            -- batch commits
SET sql_mode = '';             -- allow zero-dates, relaxed type coercion
SET time_zone = '+00:00';      -- UTC for deterministic TIMESTAMP handling
SET wait_timeout = 86400;      -- 24h (default 8h may fire on large tables)

-- PostgreSQL: executed on every worker connection at startup
SET session_replication_role = 'replica';  -- disable triggers + FK checks
SET idle_in_transaction_session_timeout = 0;  -- prevent timeout during stall
SET timezone = 'UTC';
```

```rust
// Each worker writes to its own connection. Errors are isolated.
// When a worker errors:
//   - The AtomicBool shutdown signal is set — all workers stop at
//     the next batch boundary (finish current batch, don't start new ones)
//   - The worker's connection is closed
//   - The error is recorded in shared state
//   - After all workers drain, the master collects errors
//   - FK integrity is checked for affected FK pairs before proceeding
//     to the next level (orphan detection query)

struct ParallelImportState {
    errors: Mutex<Vec<WorkerError>>,
    progress: AtomicProgress,
    shutdown: AtomicBool,  // set on critical error
}
```

### Performance Model

Parallel import throughput scales with target write capacity. Measured
against real hardware:

| Target                       | Single conn INSERT | 4 workers  | 8 workers  | COPY (PG only) |
| ---------------------------- | ------------------ | ---------- | ---------- | -------------- |
| MySQL 8.0 gp3 3000 IOPS      | 5-10 MB/s          | 12-25 MB/s | 18-35 MB/s | N/A            |
| MySQL 8.4 gp3 16000 IOPS     | 8-15 MB/s          | 20-40 MB/s | 30-55 MB/s | N/A            |
| MySQL 8.4 io2 64000 IOPS     | 10-20 MB/s         | 30-55 MB/s | 45-80 MB/s | N/A            |
| PostgreSQL 17 gp3 3000 IOPS  | N/A                | N/A        | N/A        | 30-60 MB/s     |
| PostgreSQL 17 io2 64000 IOPS | N/A                | N/A        | N/A        | 60-120 MB/s    |

MySQL INSERT throughput is bottlenecked by index maintenance (each
secondary index doubles write amplification), binlog writes, and
doublewrite buffer overhead. PostgreSQL COPY bypasses per-row overhead
via bulk protocol.

**2TB migration duration** (MySQL 8.4, gp3 16000, 4 workers, median):
~22h data import + 5m verification (rowcount) = ~22h total. Adding checksum
verification: +14h. Run in `screen`/`tmux` or as a background process.

Fathom achieved 700K-1M rows/sec via cross-DB `INSERT INTO target SELECT *
FROM source` (server-internal, no client data movement). This design routes
every byte through the client. Cross-DB transfer is available as
`--copy-mode direct` when source/target are same-type on same server.

Use `--bench` to measure actual throughput before migration: 10K test rows,
timed, reports estimated duration against the real target.

**RTT impact**: Throughput assumes RTT <5ms. Cross-region (50ms RTT): ~40%
of rated throughput. Cross-continent (200ms RTT): ~15%. At 200ms RTT,
2TB takes ~72h with 4 workers. Mitigations: increase `--batch-size` to
10,000–100,000 rows to amortize RTT, or use `--copy-mode direct` for
server-internal transfer.

**Connection pool pre-flight**: Before starting, queries `SELECT
@@max_connections` and `SELECT COUNT(*) FROM information_schema.PROCESSLIST`
on the target. Warns if parallel workers would consume >80% of remaining
slots. For managed MySQL with 51 max_connections and 40 app connections,
parallel import is limited to sequential mode automatically.

**PostgreSQL post-import VACUUM**: After data import, runs manual `VACUUM
ANALYZE` on the largest tables before re-enabling autovacuum. Re-enables
autovacuum in batches of 20 tables with 60s intervals to prevent I/O storm.

### Integration with Existing `ParallelWriters` Model

The file-based `ParallelWriters` uses FNV-1a hash of table name modulo
writer count for sticky shard assignment. The same hash function (and
same sticky-assignment guarantee) is reused:

```rust
fn shard_index(table: &str, n: usize) -> usize {
    (fnv1a(table) % n as u64) as usize
}
```

A table always routes to the same worker, and a worker only opens one source
cursor per table. This keeps the source DB connection predictable (no cursor
contention) and the target INSERT sequence deterministic (same rows in same
order as sequential import, just distributed across tables).

### Sequential Fallback

Parallel import is opt-in. The default is sequential (one connection, one
table at a time). Parallel mode is activated via `--parallel N`. If `N=1`
or `N=0`, sequential mode is used. This keeps the simple path simple and
the fast path explicit.

### New Failure Modes for Parallel Import

| #   | Failure                                                                                         | Detection                                                       | Recovery                                                                                  |
| --- | ----------------------------------------------------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| P7  | Worker connection drops mid-import (target-side network partition)                              | Read/Write error on worker's connection                         | Worker records error in shared state; master drains other workers, aborts level           |
| P8  | Target connection limit exceeded (too many parallel workers)                                    | "Too many connections" error at worker startup                  | Reduce `--parallel` count or increase target `max_connections`                            |
| P9  | Two workers import tables with hidden FK dependency (undetected cycle)                          | FK violation during INSERT on one worker                        | The level-grouping prevents this by construction; if it occurs, the FK graph has a bug    |
| P10 | Source connection contention (all workers hitting same source DB)                               | Source DB connection limit exhausted                            | Use a read replica for source cursors; reduce parallel count on source side               |
| P11 | Uneven table sizes cause worker imbalance (one worker has a 50M-row table, another has 5K rows) | Idle workers completing their level early while one worker runs | Use `--chunk-column` for intra-table parallelism on the large table (see §6.3c)           |
| P12 | Chunk column has uneven distribution (a few site_ids have 90% of the data, most have 0.1%)      | Some workers starved, others overloaded                         | Detect skew via MIN/MAX/COUNT per chunk during boundary discovery; split oversized chunks |

## 6.3c Chunk-Column Override for Data Import

### The Problem: PK Range Is Often the Wrong Chunk Key

The verification chunking design partitions tables by PK range. For
verification this is fine — hashing rows in ranges. But for data import,
the PK is often the wrong column to chunk on. Fathom learned this:
`WHERE timestamp BETWEEN` (time-based chunking) was slow because
`timestamp` wasn't the sort key; `WHERE site_id BETWEEN` (sort-key
chunking) was **75× faster**.

| Scenario              | PK                           | Optimal chunk column | Why                                                       |
| --------------------- | ---------------------------- | -------------------- | --------------------------------------------------------- |
| Time-series           | `id BIGINT` (auto-increment) | `timestamp`          | Physically ordered by time; PK chunk scans whole table    |
| Multi-tenant          | `id BIGINT PK`               | `tenant_id, id`      | Partitioned by tenant; PK chunk random I/O across tenants |
| Append-only analytics | `(site_id, timestamp) PK`    | `site_id`            | Leading PK column is the sort key; contiguous disk blocks |
| UUID PK               | `id UUID PK`                 | `created_at`         | UUID PKs are random; full-table scans per chunk           |

Without a chunk-column override, every `SELECT * FROM huge_table WHERE id

> = N AND id < N+M ORDER BY id` on a table clustered by something else
> produces a full table scan. With 2TB of data, that's catastrophic.

### Design: `--chunk-column` Flag

When `--chunk-column` is set, data import emits range-bounded queries:

```sql
-- Instead of (full scan per chunk):
SELECT * FROM table ORDER BY id;

-- Use (index scan, bounded range):
SELECT * FROM table
WHERE chunk_column >= :start AND chunk_column < :end
ORDER BY chunk_column, id;
```

### Chunk-Boundary Discovery

Before importing a table, the executor discovers chunk boundaries:

```rust
fn discover_chunk_boundaries(
    source: &mut dyn DbSource,
    table: &str,
    chunk_column: &str,
    target_chunk_rows: u64,
) -> Result<Vec<ChunkRange>> {
    let (min, max) = source.query_scalar(
        &format!("SELECT MIN({c}), MAX({c}) FROM {t}", c = chunk_column, t = table)
    )?;
    let total_rows = source.query_scalar(
        &format!("SELECT COUNT(*) FROM {t}", t = table)
    )?;
    let num_chunks = max(1, total_rows / target_chunk_rows);
    // Divide range into num_chunks partitions
    // For numeric/date: linear range division
    // For text: EXPLAIN-based histogram or sampled percentiles
    // Detect skew: if one chunk has > 3× target rows, subdivide it
}
```

### Intra-Table Parallelism

When `--chunk-column` is specified, a single large table is imported by
multiple workers simultaneously, each handling a different range. This
enables the Fathom "site bundles" pattern at the sql-splitter level:

```
Table: pageviews (2 billion rows, chunk_column=site_id)
  Worker 1: WHERE site_id >= 0     AND site_id < 50001
  Worker 2: WHERE site_id >= 50001 AND site_id < 100001
  ...
```

Without `--chunk-column`, parallelism is table-level only (one connection
per table). With it, parallelism is intra-table (multiple connections per
large table, each handling a range). Small tables still get one worker each.

### Skew Detection

If the chunk column has uneven distribution (a few site_ids hold 90% of the
data), per-worker throughput diverges. The boundary discovery detects this:

```sql
SELECT chunk_column, COUNT(*) AS cnt
FROM table
GROUP BY chunk_column
ORDER BY cnt DESC
LIMIT 10;
```

If the top-1 value has > 10× the average rows, that value gets its own
dedicated worker (or multiple workers chunked by a secondary key within
that value). This prevents one worker from stalling while others sit idle.

---

## 6.3d Staging-Table Strategy for Safe DDL

### The Problem: Non-Atomic DDL on MySQL

MySQL DDL is not transactional. `ALTER TABLE ... ADD COLUMN` that fails
midway leaves the table in an unknown state. `DROP TABLE` cannot be rolled
back if the next step fails. PostgreSQL wraps DDL in transactions (safe),
but MySQL does not (unsafe).

The established safe pattern for MySQL DDL (used by gh-ost, pt-osc, Vitess
Online DDL):

1. Create a staging table with the desired schema
2. Copy data from original to staging
3. Drop original
4. Rename staging to original name

### Design: `--staging-strategy`

```bash
sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --execute \
  --staging-strategy rename-safe
```

Three strategies:

| Strategy           | Behavior                                                                    | Safety                                 | Downtime                            |
| ------------------ | --------------------------------------------------------------------------- | -------------------------------------- | ----------------------------------- |
| `direct` (default) | Run DDL directly on target tables                                           | Risky on MySQL (non-transactional DDL) | Minimal                             |
| `rename-safe`      | Create `_staging`, copy data, swap names atomically                         | Safe (original preserved until swap)   | Table locked during swap (~seconds) |
| `copy-then-drop`   | Create `_new`, copy data, verify, drop original only if verification passes | Safest (manual verification step)      | Table locked during copy + swap     |

### Rename-Safe Algorithm

```sql
-- Step 1: Create staging table with desired schema
CREATE TABLE __sql_splitter_staging_orders (
  -- desired schema from migration plan
);

-- Step 2: Copy data with type conversions
INSERT INTO __sql_splitter_staging_orders (col1, col2, ...)
SELECT col1, col2, ...
FROM orders;

-- Step 3: Verify row counts match
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM __sql_splitter_staging_orders;
-- Assert: counts equal

-- Step 4: Atomic swap (MySQL: single RENAME statement is atomic)
RENAME TABLE orders TO __sql_splitter_old_orders,
             __sql_splitter_staging_orders TO orders;

-- Step 5: Drop old table after verification period
DROP TABLE IF EXISTS __sql_splitter_old_orders;
```

MySQL's `RENAME TABLE` with multiple renames in one statement is
**atomic** — there is no moment where the table is missing. For PostgreSQL,
the entire operation wraps in a transaction (all DDL is transactional).

### Strategy Assignment by Operation

| Operation                             | MySQL        | PostgreSQL |
| ------------------------------------- | ------------ | ---------- |
| CREATE TABLE                          | Direct       | Direct     |
| ADD COLUMN (nullable or with DEFAULT) | Direct       | Direct     |
| ADD COLUMN (NOT NULL, no DEFAULT)     | RenameSafe   | Direct     |
| MODIFY COLUMN (type change)           | RenameSafe   | Direct     |
| DROP COLUMN                           | Direct       | Direct     |
| DROP TABLE                            | Direct       | Direct     |
| Primary key change                    | CopyThenDrop | RenameSafe |
| ADD/DROP CONSTRAINT                   | Direct       | Direct     |
| ADD INDEX (large table)               | RenameSafe   | Direct     |

### Naming Convention

Staging tables use a double-underscore prefix to avoid collision:

```rust
fn staging_name(original: &str) -> String {
    format!("__sql_splitter_staging_{}", original)
}
fn old_name(original: &str) -> String {
    format!("__sql_splitter_old_{}", original)
}
```

These are dropped during the cleanup phase after `--verify` passes. If the
migration fails before verification, the old tables are preserved for
manual rollback. A warning is emitted: "N old tables preserved at target;
run `--cleanup-staging` to drop them."

### Existing Codebase Analog

The codebase already has a filesystem-level atomic publication pattern
(`src/generate/output.rs:580-692`, `src/duckdb/cache.rs:190-216`):

1. Write to protected temp file (`.partial`)
2. `fsync` the temp file
3. `rename(2)` over the destination (atomic on same filesystem)
4. `fsync` the directory

The staging-table strategy is the SQL-level analog: write to staging,
verify, swap atomically, clean up old.

## 6.4 Local Docker E2E Integration Tests

### Test Matrix

Every migration path must be tested:

```
Source → Target pairs:
  MySQL 8.0 → MySQL 8.0
  MySQL 8.0 → MySQL 8.4
  MySQL 8.0 → PostgreSQL 16
  MySQL 8.0 → PostgreSQL 17
  PostgreSQL 16 → PostgreSQL 16
  PostgreSQL 16 → PostgreSQL 17
  PostgreSQL 16 → MySQL 8.4

Test dimensions per pair:
  × small (10 tables, 1K rows each = ~50MB)
  × medium (50 tables, 10K rows each = ~500MB)
  × large (100 tables, 100K rows each = ~5GB, run nightly)
  × edge cases (FK cycles, self-ref, NULLs, BLOBs, ENUMs, zero dates)
  × idempotency (run twice, verify same result)
  × SIGINT recovery (kill mid-import, re-run, verify)
  × file source → live target
  × live source → file output
  × plan-only mode (no execution)
```

### Docker Compose Setup

```yaml
# tests/migration/docker-compose.yml
services:
  mysql80:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports: ["33060:3306"]
    tmpfs: /var/lib/mysql # ephemeral for speed

  mysql84:
    image: mysql:8.4
    environment:
      MYSQL_ROOT_PASSWORD: testpass
      MYSQL_DATABASE: testdb
    ports: ["33061:3306"]
    tmpfs: /var/lib/mysql

  postgres16:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports: ["54320:5432"]
    tmpfs: /var/lib/postgresql/data

  postgres17:
    image: postgres:17
    environment:
      POSTGRES_PASSWORD: testpass
      POSTGRES_DB: testdb
    ports: ["54321:5432"]
    tmpfs: /var/lib/postgresql/data
```

### Test File Structure

```
tests/migration/
├── docker-compose.yml
├── mod.rs
├── support/
│   ├── mod.rs                  # Re-exports
│   ├── fixtures.rs             # generate_fixture for migration test models
│   ├── docker.rs               # docker-compose lifecycle (up, wait-healthy, down)
│   └── assertions.rs           # checksum comparison, row count, FK integrity
├── fixtures/
│   ├── migration-base.yaml     # Synthetic model: 10 tables, FK chain + self-ref
│   ├── edge-cases/
│   │   ├── fk-cycles.yaml      # Circular FK dependencies
│   │   ├── fk-orphaned.yaml    # Parent table with missing referenced rows
│   │   ├── enum-types.yaml     # ENUM, SET column types
│   │   ├── blob-data.yaml      # BINARY, BLOB, TEXT columns
│   │   ├── null-edge-cases.yaml # NULL in NOT NULL, DEFAULT handling
│   │   ├── charset-mix.yaml    # utf8, utf8mb4, latin1 collations
│   │   ├── zero-dates.yaml     # 0000-00-00 dates
│   │   └── reserved-keywords.yaml # MANUAL, PARALLEL, QUALIFY as column names
│   └── sizes/
│       ├── small.yaml          # 10 tables, 1K rows
│       ├── medium.yaml         # 50 tables, 10K rows
│       └── large.yaml          # 100 tables, 100K rows
├── test_migration_smoke.rs     # Quick smoke: one source→target pair
├── test_migration_matrix.rs    # Full matrix (ignored by default, run in CI)
├── test_migration_edge_cases.rs # Edge case fixtures
├── test_migration_idempotency.rs # Run twice, verify same result
├── test_migration_recovery.rs  # Kill mid-import, re-run, verify
├── test_migration_plan.rs      # Plan generation output validation
├── test_migration_verify.rs    # Verification and checksum tests
├── test_migration_dry_run.rs   # Dry-run mode tests
└── test_migration_logging.rs   # Log output validation
```

### Test Helper: Docker Lifecycle

```rust
// tests/migration/support/docker.rs

pub struct DockerDb {
    container_name: String,
    port: u16,
    dialect: SqlDialect,
    version: String,
    connection_uri: String,
}

impl DockerDb {
    /// Start a database container and wait for it to be healthy.
    pub async fn start(dialect: SqlDialect, version: &str) -> Result<Self>;

    /// Build connection URI string for DbSource/DbTarget.
    pub fn uri(&self) -> String {
        match self.dialect {
            SqlDialect::MySql =>
                format!("mysql://root:testpass@127.0.0.1:{}/testdb", self.port),
            SqlDialect::Postgres =>
                format!("postgres://postgres:testpass@127.0.0.1:{}/testdb", self.port),
        }
    }

    /// Stop and remove the container.
    pub async fn stop(self) -> Result<()>;
}

/// Start all databases defined in tests/migration/docker-compose.yml.
pub async fn start_all() -> Result<MigrationDbCluster> {
    // Runs `docker compose -f tests/migration/docker-compose.yml up -d`
    // Waits for health checks on all 4 databases
}

pub struct MigrationDbCluster {
    pub mysql80: DockerDb,
    pub mysql84: DockerDb,
    pub postgres16: DockerDb,
    pub postgres17: DockerDb,
}
```

### Test Example: Smoke Test

```rust
// tests/migration/test_migration_smoke.rs

#[tokio::test]
async fn mysql80_to_mysql84_smoke() {
    let cluster = support::docker::start_all().await;

    // Generate synthetic data in source
    let source_fixture = support::fixtures::generate(
        SqlDialect::MySql,
        "migration-base.yaml",
        100, // rows per table
        42,  // seed
    );

    // Load into source DB
    cluster.mysql80.load_sql_file(&source_fixture).await;

    // Run migration
    let result = run_migrate(&MigrateConfig {
        source: cluster.mysql80.uri(),
        target: cluster.mysql84.uri(),
        execute: true,
        verify: true,
        ..Default::default()
    });

    assert!(result.success());
    assert_eq!(result.tables_migrated(), 10);
    assert_eq!(result.verification_mismatches(), 0);
}

fn run_migrate(config: &MigrateConfig) -> MigrateResult {
    // Invoke sql-splitter migrate via CLI args
    // or call library API directly for testing
}
```

---

## 6.5 Live Cloud E2E Test Setup

### Test Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions Runner (or developer machine)                │
│                                                              │
│  1. Generate synthetic data (10 GB)                          │
│  2. Load into PlanetScale MySQL (pscale + mysql CLI)         │
│  3. Run `pscale database dump`                               │
│  4. Run `sql-splitter migrate` to local Docker MySQL         │
│  5. Run `sql-splitter migrate` to local Docker Postgres      │
│  6. Run `sql-splitter migrate` from PlanetScale dump file    │
│  7. Verify all targets via table checksums                   │
│  8. Report throughput, memory, and verification results      │
└─────────────────────────────────────────────────────────────┘
```

### Prerequisites (One-Time Setup)

| Resource                     | How to Provision                                                                                                     |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| PlanetScale database         | `pscale database create e2e-migration-test --region us-east-1`                                                       |
| PlanetScale branch           | `pscale branch create e2e-migration-test main`                                                                       |
| PlanetScale service token    | `pscale service-token create` (store in GitHub Secrets `PLANETSCALE_SERVICE_TOKEN` + `PLANETSCALE_SERVICE_TOKEN_ID`) |
| Neon Postgres (optional)     | Create free-tier Neon project; store connection string in `NEON_CONNECTION_STRING`                                   |
| AWS RDS MySQL 8.4 (optional) | `db.t4g.medium`, 20 GB gp3; store connection string in `RDS_MYSQL_CONNECTION_STRING`                                 |

### Scripts

```
scripts/e2e-cloud/
├── README.md              # Setup instructions for each cloud provider
├── setup.sh               # Creates cloud resources, loads synthetic data
├── load-data.sh           # Loads generated SQL into cloud databases
├── run-migration.sh       # Runs migration between cloud DBs
├── verify.sh              # Runs verification
├── teardown.sh            # Cleans up cloud resources
├── config.env.example     # Template for cloud credentials
├── report.sh              # Generates benchmark report
└── results/               # Git-ignored output directory
    ├── migration-log.json
    ├── verification.json
    └── benchmark.json
```

### setup.sh

The setup script is provider-specific. For Neon, use `psql` with
the connection string from `NEON_CONNECTION_STRING` to load generated SQL.
For AWS RDS, use `mysql` or `psql` directly.

> **PlanetScale note**: PlanetScale e2e testing requires `pscale database dump`
> which is a pre-migration step (exporting data from PlanetScale), not part of
> the sql-splitter tool itself. Run `pscale database dump` before invoking
> `sql-splitter migrate` with the resulting dump files as `--source`.

### run-migration.sh

```bash
#!/usr/bin/env bash
set -euo pipefail

SOURCE=${1:?Usage: $0 <source-uri> <target-uri>}
TARGET=${2:?Usage: $0 <source-uri> <target-uri>}
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
RESULTS_DIR="scripts/e2e-cloud/results"

echo "=== Migration: $SOURCE → $TARGET ==="

# Run migration with benchmarking
/usr/bin/time -v -o "$RESULTS_DIR/time-$TIMESTAMP.txt" \
  sql-splitter migrate \
    --source "$SOURCE" \
    --target "$TARGET" \
    --execute \
    --verify checksum \
    --log-format json \
    --log-file "$RESULTS_DIR/migration-log-$TIMESTAMP.json" \
    --state-file "$RESULTS_DIR/state-$TIMESTAMP.json" \
    --report "$RESULTS_DIR/report-$TIMESTAMP.json"

echo "=== Migration complete ==="
cat "$RESULTS_DIR/report-$TIMESTAMP.json" | jq '.summary'
```

### verify.sh

```bash
#!/usr/bin/env bash
set -euo pipefail

SOURCE=${1:?Usage: $0 <source-uri> <target-uri>}

echo "=== Verifying $SOURCE against $TARGET ==="

sql-splitter migrate \
  --source "$SOURCE" \
  --target "$TARGET" \
  --verify checksum \
  --format json | jq '.verification'
```

### Config and CI

Cloud credentials are stored in GitHub Secrets and injected into the CI
environment. The `config.env.example` file documents the required secrets
but never contains real credentials:

```bash
# config.env.example — copy to config.env and fill in
# NEVER commit real credentials

# PlanetScale
PLANETSCALE_ORG=""
PLANETSCALE_DATABASE=""
PLANETSCALE_HOST=""
PLANETSCALE_USER=""
PLANETSCALE_PASSWORD=""
PLANETSCALE_SERVICE_TOKEN=""
PLANETSCALE_SERVICE_TOKEN_ID=""

# Neon (optional)
NEON_CONNECTION_STRING="postgres://user:pass@host/db?sslmode=require"

# AWS RDS (optional)
RDS_MYSQL_CONNECTION_STRING="mysql://user:pass@host:3306/db"
```

---

## 6.6 Configurable Multi-GB Benchmarking

### Synthetic Data Models

The `generate` command already supports configurable multi-GB generation.
For migration benchmarking, we use the existing infrastructure extended with
migration-specific models that exercise all edge cases.

### Size Presets

| Preset       | Tables | Rows/table | Approx. output | Use case                        |
| ------------ | ------ | ---------- | -------------- | ------------------------------- |
| `tiny`       | 10     | 500        | ~5 MB          | Unit tests, quick smoke         |
| `small`      | 10     | 10,000     | ~100 MB        | Integration tests               |
| `medium`     | 50     | 100,000    | ~1 GB          | CI benchmark                    |
| `large`      | 50     | 500,000    | ~5 GB          | Nightly benchmark               |
| `giga`       | 100    | 1,000,000  | ~10 GB         | Weekly benchmark                |
| `tera`       | 200    | 5,000,000  | ~50 GB         | Pre-release validation (manual) |
| `edge-cases` | 20     | 10,000     | ~50 MB         | Edge case regression tests      |

### Migration Benchmark Model

```yaml
# tests/migration/fixtures/sizes/benchmark-model.yaml
kind: model
seed: 42424242
dialect: mysql

# 100 tables exercising every SQL feature relevant to migration
# Organised by category:

# FK chain: linear dependency chain (exercises topo-sort)
# FK tree: parent with many children (exercises fan-out)
# FK self-ref: employees → employees (exercises self-referential ordering)
# FK cycle: A → B → C → A (exercises cycle detection)

# Column types:
#   Every MySQL type: INT, BIGINT, TINYINT, DECIMAL, FLOAT, DOUBLE,
#     VARCHAR, TEXT, LONGTEXT, BLOB, LONGBLOB, JSON, ENUM, SET,
#     DATE, DATETIME, TIMESTAMP, YEAR, CHAR, BINARY, VARBINARY, BIT
#   Every edge case: NULL in every column, zero dates, emoji in utf8,
#     very large DECIMAL, very large TEXT, empty strings

# Index types: PRIMARY KEY, UNIQUE, INDEX, FULLTEXT
# Constraint types: FK, CHECK, DEFAULT, NOT NULL
# Encoding: utf8, utf8mb4, latin1, utf8mb4_0900_ai_ci

tables:
  # FK chain: a → b → c → d → e (5 linear dependency tables)
  - name: region
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: name
        type: VARCHAR(100)

  - name: country
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: region_id
        type: INT
        foreign_key: { table: region, column: id }
      - name: name
        type: VARCHAR(100)

  # ... (chain continues: city → district → address)

  # FK tree: parent with many children
  - name: users
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: email
        type: VARCHAR(255)
        unique: true
      - name: name
        type: VARCHAR(100)

  - name: orders
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: user_id
        type: INT
        foreign_key: { table: users, column: id }

  - name: order_items
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: order_id
        type: INT
        foreign_key: { table: orders, column: id }

  - name: payments
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: order_id
        type: INT
        foreign_key: { table: orders, column: id }

  # FK self-ref
  - name: employees
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: manager_id
        type: INT
        nullable: true
        foreign_key: { table: employees, column: id }
      - name: name
        type: VARCHAR(100)

  # Edge case types
  - name: edge_case_types
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: col_tinyint
        type: TINYINT(1) # should become BOOLEAN on Postgres
      - name: col_unsigned_bigint
        type: BIGINT UNSIGNED
      - name: col_decimal
        type: DECIMAL(65,30)
      - name: col_float
        type: FLOAT
      - name: col_double
        type: DOUBLE
      - name: col_enum
        type: ENUM('active','inactive','pending')
      - name: col_set
        type: SET('read','write','admin')
      - name: col_json
        type: JSON
      - name: col_blob
        type: LONGBLOB
      - name: col_text
        type: LONGTEXT
      - name: col_datetime
        type: DATETIME
      - name: col_timestamp
        type: TIMESTAMP
      - name: col_date
        type: DATE
      - name: col_binary
        type: VARBINARY(255)

  # Large data (exercises streaming and chunking)
  - name: large_text_table
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      - name: user_id
        type: INT
        foreign_key: { table: users, column: id }
      - name: content
        type: LONGTEXT
        generator:
          kind: lorem
          min_words: 500
          max_words: 2000

  # Wide table (exercises column count limits)
  - name: wide_table
    rows: { auto }
    columns:
      - name: id
        type: INT
        primary_key: true
        auto_increment: true
      # ... 50 columns of mixed types
```

### Benchmark Script

```bash
#!/usr/bin/env bash
# scripts/benchmark-migration.sh
# Measures end-to-end migration throughput, memory, and verification accuracy.
set -euo pipefail

SCALE="${1:-medium}"
RESULTS="benchmark-results/migration-$SCALE-$(date -u +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS"

echo "=== Migration Benchmark: $SCALE ==="

# Start Docker databases
docker compose -f tests/migration/docker-compose.yml up -d --wait

# Generate source data
echo "--- Generating $SCALE source data ---"
/usr/bin/time -v -o "$RESULTS/generate-time.txt" \
  sql-splitter generate \
    --config "tests/migration/fixtures/sizes/$SCALE.yaml" \
    --dialect mysql \
    --output "$RESULTS/source.sql" \
    --seed 42

SOURCE_SIZE=$(stat -f%z "$RESULTS/source.sql" 2>/dev/null || stat -c%s "$RESULTS/source.sql")
echo "Source size: $(( SOURCE_SIZE / 1024 / 1024 )) MB"

# Load into source MySQL
echo "--- Loading into source MySQL ---"
/usr/bin/time -v -o "$RESULTS/load-time.txt" \
  mysql -h 127.0.0.1 -P 33060 -u root -ptestpass testdb < "$RESULTS/source.sql"

# Run migration: MySQL 8.0 → MySQL 8.4
echo "--- Migration: MySQL 8.0 → MySQL 8.4 ---"
/usr/bin/time -v -o "$RESULTS/migrate-mysql84-time.txt" \
  sql-splitter migrate \
    --source "mysql://root:testpass@127.0.0.1:33060/testdb" \
    --target "mysql://root:testpass@127.0.0.1:33061/testdb" \
    --execute \
    --verify checksum \
    --log-format json \
    --log-file "$RESULTS/migrate-mysql84-log.json" \
    --report "$RESULTS/migrate-mysql84-report.json"

# Run migration: MySQL 8.0 → PostgreSQL 17
echo "--- Migration: MySQL 8.0 → PostgreSQL 17 ---"
/usr/bin/time -v -o "$RESULTS/migrate-pg17-time.txt" \
  sql-splitter migrate \
    --source "mysql://root:testpass@127.0.0.1:33060/testdb" \
    --target "postgres://postgres:testpass@127.0.0.1:54321/testdb" \
    --execute \
    --verify checksum \
    --log-format json \
    --log-file "$RESULTS/migrate-pg17-log.json" \
    --report "$RESULTS/migrate-pg17-report.json"

# Run migration: file → MySQL 8.4 (simulates dump → managed MySQL)
echo "--- Migration: file → MySQL 8.4 ---"
/usr/bin/time -v -o "$RESULTS/migrate-file-time.txt" \
  sql-splitter migrate \
    --source "$RESULTS/source.sql" \
    --target "mysql://root:testpass@127.0.0.1:33061/testdb" \
    --execute \
    --verify checksum \
    --log-file "$RESULTS/migrate-file-log.json"

# Verify: compare source and target row counts
echo "--- Verification ---"
sql-splitter migrate \
  --source "mysql://root:testpass@127.0.0.1:33060/testdb" \
  --target "mysql://root:testpass@127.0.0.1:33061/testdb" \
  --verify checksum \
  --format json > "$RESULTS/verify-mysql84.json"

sql-splitter migrate \
  --source "mysql://root:testpass@127.0.0.1:33060/testdb" \
  --target "postgres://postgres:testpass@127.0.0.1:54321/testdb" \
  --verify checksum \
  --format json > "$RESULTS/verify-pg17.json"

# Idempotency test: run same migration again
echo "--- Idempotency: re-run MySQL 8.0 → MySQL 8.4 ---"
sql-splitter migrate \
  --source "mysql://root:testpass@127.0.0.1:33060/testdb" \
  --target "mysql://root:testpass@127.0.0.1:33061/testdb" \
  --execute \
  --verify checksum \
  --log-file "$RESULTS/idempotency-log.json"

# Verify idempotent result matches
diff <(jq '.verification' "$RESULTS/verify-mysql84.json") \
     <(jq '.verification' "$RESULTS/idempotency-log.json" | jq '.verification')

# Generate report
{
  echo "# Migration Benchmark: $SCALE"
  echo "## Source"
  echo "- Size: $(( SOURCE_SIZE / 1024 / 1024 )) MB"
  echo "- Tables: $(jq '.tables | length' "$RESULTS/migrate-mysql84-report.json")"
  echo ""
  echo "## MySQL → MySQL 8.4"
  echo "- Duration: $(grep 'Elapsed' "$RESULTS/migrate-mysql84-time.txt")"
  echo "- Rows migrated: $(jq '.summary.rows_migrated' "$RESULTS/migrate-mysql84-report.json")"
  echo "- Verification: $(jq '.verification.mismatches' "$RESULTS/verify-mysql84.json") mismatches"
  echo ""
  echo "## MySQL → PostgreSQL 17"
  echo "- Duration: $(grep 'Elapsed' "$RESULTS/migrate-pg17-time.txt")"
  echo "- Rows migrated: $(jq '.summary.rows_migrated' "$RESULTS/migrate-pg17-report.json")"
  echo "- Verification: $(jq '.verification.mismatches' "$RESULTS/verify-pg17.json") mismatches"
} > "$RESULTS/report.md"

echo "=== Benchmark complete: $RESULTS/report.md ==="

# Cleanup
docker compose -f tests/migration/docker-compose.yml down -v
```

---

## 6.7 Logging and Debugging Infrastructure

### Design Principles

1. **Zero-cost in release builds for hot paths.** Row-level logging is behind
   compile-time feature flags or runtime verbosity levels that compile away.

2. **Structured output for machine consumption.** All logs are JSON Lines
   (one JSON object per log entry). Compatible with `jq`, `vector`, `fluentd`,
   and log aggregation systems.

3. **Human-readable for interactive use.** The same structured data is
   formatted as colored terminal output when `--log-format text` is used.

4. **Span/trace IDs for correlation.** Every phase, table, and batch has a
   trace ID that connects the plan, execution log, and verification results.

### Log Entry Structure

```json
{
  "timestamp": "2026-08-06T14:30:01.234Z",
  "level": "info",
  "phase": "data_migration",
  "table": "users",
  "operation": "insert_batch",
  "trace_id": "mig_abc123",
  "span_id": "data_migration_users_005",
  "parent_span_id": "data_migration",
  "message": "Inserted batch 5/48 for table users",
  "fields": {
    "batch_index": 5,
    "total_batches": 48,
    "rows_in_batch": 1000,
    "cumulative_rows": 5000,
    "duration_ms": 234,
    "throughput_rows_sec": 4273
  }
}
```

### Span Hierarchy

```
migration:mig_abc123                    # Root span: entire migration
├── phase:pre_flight                    # Pre-flight checks
│   ├── check:connectivity_source
│   ├── check:connectivity_target
│   └── check:disk_space
├── phase:schema_extraction             # Extract schemas
│   ├── source:mysql_80
│   └── target:mysql_84
├── phase:plan_generation               # Generate plan
│   └── diff:schemas
├── phase:ddl_pre_data                  # Phase 1 DDL
│   ├── table:region (CREATE TABLE)
│   ├── table:country (CREATE TABLE)
│   └── ... (in topo-sort order)
├── phase:data_migration                # Phase 2 Data
│   ├── table:region
│   │   ├── batch:001 (rows 0-999)
│   │   ├── batch:002 (rows 1000-1999)
│   │   └── batch:003 (rows 2000-2999)
│   ├── table:country
│   │   └── ...
│   └── ...
├── phase:ddl_post_data                 # Phase 3 DDL
│   ├── table:orders (ADD FK)
│   ├── table:order_items (ADD FK)
│   └── ... (in topo-sort order)
├── phase:verification                  # Phase 4 Verify
│   ├── table:region (checksum match)
│   ├── table:users (checksum match)
│   └── table:edge_case_types (checksum MISMATCH)
└── phase:finalize                      # Summary
    └── report:written
```

### CLI Flags for Logging

| Flag           | Values                                    | Description                                                            |
| -------------- | ----------------------------------------- | ---------------------------------------------------------------------- |
| `--log-level`  | `error`, `warn`, `info`, `debug`, `trace` | Minimum log level (default: `info`)                                    |
| `--log-format` | `text`, `json`                            | Output format (default: `text` for TTY, `json` for pipe/file)          |
| `--log-file`   | path                                      | Write structured log to file (JSON Lines regardless of `--log-format`) |
| `--state-file` | path                                      | Write execution state for resumability (Phase 4)                       |
| `--report`     | path                                      | Write final migration report (JSON)                                    |
| `--progress`   | `bar`, `log`, `none`                      | Progress display (default: `bar` for TTY, `log` for non-TTY)           |
| `--verbose`    | flag                                      | Shorthand for `--log-level debug --log-format text`                    |

### Log Levels for Development

| Level   | When to use                                   | Example                                                                                 |
| ------- | --------------------------------------------- | --------------------------------------------------------------------------------------- |
| `error` | Operation failed, migration may be incomplete | "INSERT failed: table users, batch 5: ERROR 1062 Duplicate entry"                       |
| `warn`  | Operation succeeded with caveats              | "Column 'name' type TINYINT(1) mapped to BOOLEAN; values > 1 will be converted to true" |
| `info`  | Phase transitions, table-level progress       | "Phase 2: Migrating table users (50000 rows, 50 batches)"                               |
| `debug` | Batch-level progress, row-level details       | "Batch 5/50 for users: 1000 rows inserted in 234ms (4273 rows/s)"                       |
| `trace` | Row-level data, hex dumps, full SQL           | "INSERT IGNORE INTO users (id, name, email) VALUES (42, 'Alice', 'alice@example.com')"  |

### Row-Level Error Collection

When a row fails to insert, the error is collected rather than aborting:

```json
{
  "timestamp": "2026-08-06T14:30:15.456Z",
  "level": "error",
  "phase": "data_migration",
  "table": "edge_case_types",
  "operation": "insert_row",
  "trace_id": "mig_abc123",
  "span_id": "data_migration_edge_case_types_012",
  "message": "Row insert failed: NOT NULL column 'col_not_null' has no default",
  "fields": {
    "batch_index": 12,
    "row_index_in_batch": 7,
    "error_code": "23502",
    "error_detail": "Column 'col_not_null' of relation 'edge_case_types' contains null value",
    "pk_value": "42",
    "skipped": true
  }
}
```

Skipped rows are collected in the migration report:

```json
{
  "summary": {
    "tables_migrated": 50,
    "rows_migrated": 4987654,
    "rows_skipped": 23,
    "rows_skipped_by_table": {
      "edge_case_types": 15,
      "zero_date_table": 8
    },
    "skipped_reasons": {
      "NOT_NULL_VIOLATION": 15,
      "ZERO_DATE_INVALID": 8
    }
  }
}
```

### Memory Profiling During Development

```bash
# Profile memory during migration
heaptrack sql-splitter migrate \
  --source mysql://root:testpass@127.0.0.1:33060/testdb \
  --target mysql://root:testpass@127.0.0.1:33061/testdb \
  --execute \
  --log-level debug

# Track max RSS during migration
/usr/bin/time -v sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --execute 2>&1 | grep "Maximum resident"

# Monitor I/O during migration (macOS)
sudo iosnoop -p $(pgrep sql-splitter)

# Monitor I/O during migration (Linux)
sudo iostat -x 1 | grep -E "Device|nvme"
```

### Development Workflow: Debug Mode

```bash
# Full debug trace of a migration (everything: SQL, row data, timings)
RUST_LOG=sql_splitter=trace sql-splitter migrate \
  --source mysql://source/db \
  --target mysql://target/db \
  --execute \
  --log-level trace \
  --log-format text \
  --log-file /tmp/migration-debug.jsonl

# Then query the log for specific issues:
jq 'select(.level == "error")' /tmp/migration-debug.jsonl
jq 'select(.table == "users" and .operation == "insert_row")' /tmp/migration-debug.jsonl
jq '[.fields.throughput_rows_sec] | add / length' /tmp/migration-debug.jsonl  # average throughput
```

### Integration with Existing Diagnostic System

The migration executor uses the existing `Diagnostic` / `DiagnosticBag` system
for structured errors and warnings. Migration-specific diagnostic codes follow
the existing convention:

| Code                             | Severity | Description                                                       |
| -------------------------------- | -------- | ----------------------------------------------------------------- |
| `MIG-CONNECT-SOURCE`             | Error    | Cannot connect to source database                                 |
| `MIG-CONNECT-TARGET`             | Error    | Cannot connect to target database                                 |
| `MIG-UNSUPPORTED-CHARSET`        | Warning  | Source charset not supported by target                            |
| `MIG-COLUMN-NOT-NULL-NO-DEFAULT` | Error    | Column requires a value but source row has NULL                   |
| `MIG-FK-ORPHAN`                  | Warning  | Source row references non-existent parent                         |
| `MIG-TYPE-LOSSY`                 | Warning  | Type conversion loses precision or semantics                      |
| `MIG-ZERO-DATE`                  | Warning  | Zero date value converted to NULL                                 |
| `MIG-ROW-SKIPPED`                | Info     | Row skipped during import (see detail for reason)                 |
| `MIG-VERIFY-MISMATCH`            | Error    | Table checksum mismatch after migration                           |
| `MIG-ROW-COUNT-DELTA`            | Error    | Row count differs between source and target                       |
| `MIG-PLANETSCALE-FK-HASH`        | Warning  | FK constraint name contains PlanetScale deployment hash           |
| `MIG-DDL-NON-TRANSACTIONAL`      | Info     | DDL executed on MySQL cannot be rolled back if interrupted        |
| `MIG-TRIGGER-ENABLED`            | Warning  | Triggers are enabled on target; they will fire during data import |

### Observability During Development

For local development, the migration executor should support:

1. **Dry-run with explain**: `migrate --dry-run --explain` — shows the plan
   and explains each decision (why this table is ordered here, why this type
   conversion was chosen).

2. **Pause-on-error**: `migrate --execute --pause-on-error` — stops after
   the first error and drops into an interactive inspection mode (prints
   the failing SQL, offers to skip/retry/abort).

3. **Replay from log**: Given a `--log-file` from a previous run, replay
   the migration exactly (same order, same batches) for debugging
   deterministic issues.

4. **Diff mode**: `migrate --diff-only` — runs schema extraction and plan
   generation only, outputs the diff without attempting execution.

5. **Single-table mode**: `migrate --tables users` — migrates exactly one
   table for focused debugging.

---

## 6.8 New CLI Flags Summary

All new flags for the `migrate` command:

```
sql-splitter migrate [OPTIONS] --source <URI> --target <URI>

Source/Target:
  --source <URI>              Source database URI or file path
  --target <URI>              Target database URI or file path

Execution:
  --execute                   Execute the migration (default: plan only)
  --allow-destructive         Allow DROP TABLE, DROP COLUMN operations
  --parallel <N>              Import data using N parallel connections (default: 1)
  --chunk-column <COL>        Chunk large tables by this column (mandatory for PlanetScale sources)
  --chunk-size <N>            Target rows per chunk (default: 100000)
  --chunk-time <SECS>         Adaptive chunk sizing: auto-tune chunk size to hit this query time target
  --copy-mode <MODE>          client|direct (default: client). Direct uses INSERT INTO target SELECT * FROM source when same DB type on same server
  --staging-strategy <MODE>   direct|rename-safe|copy-then-drop (default: direct)
  --binlog-handling <MODE>    log|session-off|purge|warn-only (MySQL target only, default: warn-only)
  --sql-mode-override         Temporarily remove strict sql_mode on target during import
  --neon-disable-suspend      Disable Neon scale-to-zero during migration (requires Neon API access)
  --parallel-indexes <N>      Create indexes in parallel after data import (default: 1, sequential)
  --no-idempotent             Disable PK-targeted INSERT dedup (unsafe for re-runs)
  --strict-insert             Use plain INSERT without dedup (fail fast on first error)
  --tables <LIST>             Only migrate these tables (comma-separated globs)
  --exclude <LIST>            Exclude these tables
  --dry-run                   Validate plan against target without executing
  --batch-size <N>            Rows per INSERT batch (default: 1000)

Output:
  --output <PATH>             Write SQL file instead of executing
  --format <FORMAT>           plan|sql|text|json|runbook (default: plan)

Verification:
  --verify <MODE>             checksum|rowcount|all (default: none). checksum uses SHA-256 stream hashing; rowcount compares COUNT(*) per table
  --verify-sample <N>         When a checksum mismatch is found, sample N rows per mismatched table for detailed row-level diff
  --verify-checksum-algorithm <ALGO> sha256|xxh3 (default: sha256). xxh3 is 5-10× faster for large datasets

Logging:
  --log-level <LEVEL>         error|warn|info|debug|trace (default: info)
  --log-format <FORMAT>       text|json (default: text for TTY, json for pipe)
  --log-file <PATH>           Write structured log to file
  --state-file <PATH>         Write execution state for resumability
  --state-interval <SECS>     Interval between state-file writes (default: 30)
  --report <PATH>             Write final migration report (JSON)

Progress:
  --progress <MODE>           bar|log|none (default: bar for TTY, log otherwise)
  --verbose                   Shorthand for --log-level debug --log-format text

Benchmark:
  --bench                     Measure INSERT throughput with 10K test rows against target before migration begins

Other:
  --force                     Skip pre-flight confirmation prompts
  --max-memory <MB>           Soft memory limit for batch buffering (default: 256)
  --query-timeout <SECS>      Per-query timeout on source (default: 300)

Subcommands:
  migrate check               Run pre-flight checks only, exit 0 if ready

Development:
  --explain                   Explain plan generation decisions
  --pause-on-error            Stop on first error with interactive prompt
  --diff-only                 Run schema extraction + plan only
```
