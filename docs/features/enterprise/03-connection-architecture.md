# Part 3: Database Connection Architecture

> How sql-splitter reads from live databases.

## 5.1 The Core Problem

sql-splitter currently reads from files. Every command starts with file I/O.
Live database connections are the single feature that unlocks every migration
workflow described in this research. But the architecture choice has
consequences that ripple through the entire codebase.

The question:

1. **How do we add live DB reads to a synchronous, file-based, streaming
   SQL parser and keep it fast?**

This document answers that question.

## 5.2 Local CLI: Adding Live Database Connections

### 5.2.1 The Existing I/O Model

sql-splitter's current architecture is:

```
File path → open_input_opt_progress() → streaming parser → events → processing
```

Everything is synchronous. The parser uses bounded buffers. Memory caps are
enforced (256 MB max DDL, 256 MB max row, 64 MB max header). The writer uses
adaptive I/O profiles and parallel output. There is no async runtime.

Adding database connections means adding a _second_ input source that produces
the same parser events the file path produces, without breaking any of the
existing guarantees.

### 5.2.2 Synchronous vs. Async: The Fork in the Road

|                        | Synchronous Drivers                                                                                      | Async with Tokio Bridge                                                                                   |
| ---------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| **Crates**             | `mysql` (v25), `postgres` (v0.19)                                                                        | `mysql_async` (v0.35), `tokio-postgres` (v0.7)                                                            |
| **Runtime**            | None needed                                                                                              | Tokio `rt-multi-thread` added as dependency                                                               |
| **Connection pooling** | `r2d2_postgres` for PG; `mysql` crate has no native pool                                                 | `mysql_async::Pool`, `tokio-postgres` deadpool/bb8                                                        |
| **Streaming**          | `mysql::Conn::query_iter()` — row-by-row (text protocol); `postgres::Client::copy_out()` — COPY protocol | `mysql_async::Conn::query_iter()` — async row iterator; `tokio_postgres::Client::copy_out()` — async COPY |
| **TLS**                | Both support native TLS + custom CA certs                                                                | Both support native TLS with async handshake                                                              |
| **Fit with codebase**  | Matches existing sync architecture                                                                       | Requires `tokio::runtime::Runtime::block_on()` at the boundary                                            |
| **Risk**               | Lower — no async/sync impedance mismatch                                                                 | Medium — mixing sync parser with async DB I/O needs careful boundary design                               |

**Recommendation: Option A — Synchronous drivers.**

The migration engineer is running a CLI tool on a jump host. They're doing one
database at a time. Connection pooling is nice-to-have, not must-have. A single
synchronous connection per database is simpler to implement, simpler to debug,
and doesn't introduce an async runtime into a codebase that has never needed one.

If connection pooling becomes necessary later (e.g., parallel table extraction
for large schemas), a Tokio bridge can be added behind the same `DbSource`
trait without changing any callers.

### 5.2.3 The `DbSource` Trait

```rust
/// A live database that can produce schema and streaming row data.
///
/// This mirrors the file-based input in `src/splitter/mod.rs` but
/// fetches from a live connection instead of reading files.
pub trait DbSource: Send + Sync {
    /// Extract the full schema (tables, columns, types, constraints, indexes).
    /// Does NOT stream data — only DDL metadata.
    fn extract_schema(&mut self) -> Result<Schema>;

    /// Stream rows from a table as INSERT events.
    /// Returns an iterator that yields (column_names, row_values) tuples.
    /// The iterator is lazy — rows are pulled from the database on demand,
    /// bounded by the driver's fetch buffer, not loaded into a Vec.
    fn stream_table_rows(
        &mut self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<RowBatch>> + '_>>;
}

struct RowBatch {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>, // Chunked for efficiency, configurable batch size
}
```

Design decisions:

1. **Schema extraction never streams data.** It runs `information_schema`
   queries or `SHOW CREATE TABLE` / `pg_dump --schema-only`. This is fast
   (seconds) and doesn't risk memory exhaustion.

2. **Data streaming is chunked, not row-at-a-time.** The driver already
   fetches in chunks internally. A `RowBatch` avoids the overhead of a
   trait call per row while keeping memory bounded (configurable batch size,
   default 10,000 rows, ~4–8 MB).

3. **No `dump(&self) -> PathBuf`.** Writing a full dump to a temp file before
   processing is a regression from the existing streaming architecture. A
   500 GB dump produces a 500 GB temp file. If the migration engineer's
   laptop doesn't have 500 GB free, the tool crashes. Instead, the streaming
   path pipes rows directly from the database driver through the parser event
   pipeline without intermediate files.

### 5.2.4 MySQL Implementation (`mysql` crate)

```rust
use mysql::prelude::*;
use mysql::*;

struct MySqlSource {
    conn: mysql::Conn,
    database: String,
}

impl DbSource for MySqlSource {
    fn extract_schema(&mut self) -> Result<Schema> {
        // Use information_schema — more portable than SHOW CREATE TABLE,
        // works across VTGate (PlanetScale), RDS, Cloud SQL, and bare metal.
        //
        // information_schema.TABLES → table names, engines, collations
        // information_schema.COLUMNS → column names, types, nullability, defaults
        // information_schema.KEY_COLUMN_USAGE → primary keys, unique keys, FKs
        // information_schema.STATISTICS → indexes
        //
        // The existing SchemaBuilder (src/schema/ddl.rs) can be reused here
        // by feeding it synthesized CREATE TABLE statements from the
        // information_schema data. This avoids writing a parallel schema
        // parser for live DB introspection.
        todo!()
    }

    fn stream_table_rows(
        &mut self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<RowBatch>> + '_>> {
        // mysql::Conn::query_iter() streams rows using the text protocol.
        // Text protocol returns human-readable text — identical to what
        // mysqldump produces. Avoids binary protocol edge cases with
        // BIT/ENUM/SET types and server-side prepared statement exhaustion.
        //
        // We batch them into RowBatch(10_000 rows) to avoid trait call
        // overhead per row.
        let query = format!("SELECT * FROM `{}`", table);
        let mut result = self.conn.query_iter(&query)?;

        let columns: Vec<String> = result
            .columns()
            .iter()
            .map(|c| c.name_str().to_string())
            .collect();

        Ok(Box::new(BatchingIter {
            result,
            columns,
            batch_size: 10_000,
        }))
    }
}
```

**PlanetScale-specific handling:**

When the source is PlanetScale (detected by `mysql` connection through
VTGate), the safe path is:

1. **Schema extraction**: Use `information_schema` queries (VTGate-compatible,
   safer than `SHOW CREATE TABLE` which can include PlanetScale-specific
   constraints and hash-suffixed FK names).

2. **Data extraction**: Invoke `pscale database dump` as a subprocess OR
   require the migration engineer to provide a pre-dumped directory. Direct
   `SELECT *` streaming through VTGate is unreliable for production databases
   — VTGate internally pools connections and may interfere with long-running
   queries. See §5.2.6 below for the PlanetScale subprocess wrapper.

3. **FK constraint names**: When extracting schema from PlanetScale, FK
   constraint names include hash suffixes. The schema extraction should detect
   hash-suffixed names (`_fk_` followed by ~26 alphanumeric characters) and
   either warn or offer to rename them to stable names for the target.

### 5.2.5 PostgreSQL Implementation (`postgres` crate)

```rust
use postgres::{Client, NoTls};

struct PostgresSource {
    client: postgres::Client,
    database: String,
}

impl DbSource for PostgresSource {
    fn extract_schema(&mut self) -> Result<Schema> {
        // Two approaches:
        //
        // Approach 1: information_schema queries (portable, but misses
        //   PG-specific features like inheritance, partitioning, extensions).
        //
        // Approach 2 (recommended): Invoke pg_dump --schema-only as a
        //   subprocess and parse its output. This captures everything PG
        //   knows about the schema, including extensions, partitions,
        //   inheritance, and custom types. The output is standard SQL that
        //   sql-splitter's existing parser already handles.
        //
        // For Serverless Postgres (Neon): pg_dump --schema-only works
        // normally. Use direct connections (not pooled via PgBouncer) to
        // avoid PgBouncer transaction-mode limitations.
        //
        // For AWS RDS Postgres: pg_dump --schema-only works normally.
        todo!()
    }

    fn stream_table_rows(
        &mut self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<RowBatch>> + '_>> {
        // Two approaches:
        //
        // Approach 1: SELECT * FROM table with server-side cursor.
        //   DECLARE cursor CURSOR FOR SELECT * FROM table;
        //   FETCH 10000 FROM cursor; -- repeated
        //   Server-side cursors keep the result set on the server, not in
        //   client memory. But they require a transaction to stay open.
        //
        // Approach 2: COPY table TO STDOUT (binary or text).
        //   client.copy_out("COPY table TO STDOUT"). This is the fastest
        //   path for bulk extraction — PG's own dump tool uses it.
        //   Binary format is faster but dialect-specific; text format is
        //   compatible with sql-splitter's parser.
        //
        // Recommendation: COPY text format for bulk extraction, server-side
        // cursor for selective extraction (WHERE clauses, subset of columns).
        todo!()
    }
}
```

### 5.2.6 PlanetScale Subprocess Wrapper

> **Design pattern, not automatic integration.** This section describes how
> sql-splitter _could_ wrap the `pscale` CLI as a subprocess for bulk data
> extraction. It is not implemented and is not part of the Phase 1 plan.
> The migration engineer remains responsible for the PlanetScale dump step.

For PlanetScale sources, direct database connections are unreliable for bulk
data extraction because VTGate is not designed for long-running `SELECT *`
queries. The core executor never invokes `pscale` as a subprocess (see
`05-review-findings.md` §7.6 for the architecture decision).

Instead, PlanetScale data comes from mydumper dump files produced by
`pscale database dump`. The dump step is a pre-condition, documented in the
migration runbook, but not automated by sql-splitter:

```bash
# Run before invoking sql-splitter:
pscale database dump <DATABASE> <BRANCH> --output /path/to/dump/
```

sql-splitter reads the resulting per-table `.sql` files through the existing
file-based parser — the same path as any other file source. This keeps the
executor's dependency surface clean.

### 5.2.7 Integrating with the Existing Pipeline

The existing `splitter/mod.rs` uses `open_input_opt_progress()` to produce a
`Box<dyn BufRead>`. The live database path needs to produce the same parser
events that the file path produces, but from a database connection instead of
a file.

Two integration approaches:

**Approach 1: Event-level integration (recommended).**

Add an `InputSource` enum to the splitter:

```rust
enum InputSource {
    File(PathBuf),
    Database(Box<dyn DbSource>),
    // PlanetScale: data comes from mydumper dump files (File variant),
    // not from a live database connection. See §5.2.6.
}

// In splitter:
match input_source {
    InputSource::File(path) => {
        // existing file path — unchanged
    }
    InputSource::Database(db) => {
        let schema = db.extract_schema()?;
        for table in schema.iter() {
            let rows = db.stream_table_rows(&table.name)?;
            for batch in rows {
                for row in batch.rows {
                    emit(ParserEvent::InsertRow { table, row });
                }
            }
        }
    }
}
```

**Approach 2: SQL generation (simpler, less efficient).**

Generate `CREATE TABLE` and `INSERT` SQL from the live database, feed it
through the existing parser. This reuses 100% of the existing pipeline but
loses the efficiency of direct row streaming.

**Recommendation**: Start with Approach 2 for schema-only extraction (fast,
low risk, reuses everything). Add Approach 1 for data streaming when
performance measurements show the SQL-generation overhead is a bottleneck.

### 5.2.8 Safety Boundaries

Live database connections introduce new failure modes:

| Failure Mode                                              | Handling                                                                                    |
| --------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| Connection refused / timeout                              | Exponential backoff retry (3 attempts, 5s/10s/20s), then exit with actionable error message |
| Authentication failure                                    | Exit immediately, print available auth methods from error                                   |
| TLS handshake failure                                     | Exit with "check your SSL configuration" and the server's TLS details                       |
| Query killed mid-stream (PlanetScale VTGate timeout)      | Log warning, skip table, report in migration assessment                                     |
| Schema extraction timeout (>30s for metadata)             | Log warning about large schema, suggest filtering                                           |
| Disk full during subprocess dump                          | Exit before starting migration, check available space                                       |
| Subprocess crash (pscale dump segfault)                   | Capture stderr, report to user, clean up work_dir                                           |
| Schema mismatch (source schema changed during extraction) | Log warning with timestamp comparison                                                       |

All database connections are **read-only**. No DDL, no DML, no writes. This is
enforced at the trait level (`&mut self` for cursor state but no write methods
exposed). The `DbSource` trait has no `execute()` method. If writes are needed
for the target database (importing data), that goes through a separate
`DbTarget` trait that is opt-in behind a CLI flag (`--execute`).

## 5.3 Managed Cloud Service Architecture

This section has been deferred. See `07-managed-service-appendix.md` for the
speculative managed cloud service design. The primary focus is the local CLI
with live database connections.

## 5.4 Net-new Dependencies

Adding database connections to sql-splitter requires:

| Dependency                 | Purpose                             | License        | Approx. compile time impact |
| -------------------------- | ----------------------------------- | -------------- | --------------------------- |
| `mysql` v25                | Sync MySQL driver                   | MIT/Apache 2.0 | ~30s (includes native TLS)  |
| `postgres` v0.19           | Sync PostgreSQL driver              | MIT/Apache 2.0 | ~20s                        |
| `ssh2` v0.9 (optional)     | SSH tunneling (behind feature flag) | MIT            | ~15s                        |
| `r2d2_postgres` (optional) | PG connection pooling               | MIT            | ~5s                         |

All are well-maintained, widely used crates. None require an async runtime.
None conflict with existing dependencies.

## 5.5 What We Explicitly Do NOT Build

The following are out of scope for Phase 1 and Phase 2:

1. **ORM-style query building.** The `DbSource` trait exposes raw SQL
   streaming. No query builder, no schema migration DSL, no model layer.

2. **Database writes.** All connections are read-only. Writes go through
   a separate, opt-in `DbTarget` trait that is only activated with
   `--execute`. This is a safety boundary, not a future feature.

3. **SSH tunneling in Phase 1.** The SSH tunnel is a convenience for
   jump-host workflows. Phase 1 assumes the database is directly reachable
   (or the user handles SSH tunneling externally). `ssh2` integration is
   Phase 2.

4. **Connection pooling.** Phase 1 uses single connections. Pooling is
   added in Phase 2 if performance measurements show it's needed.

5. **PlanetScale API integration.** The PlanetScale source uses `pscale
database dump` as a subprocess. Direct pscale API calls (for auth,
   branch management, deploy request inspection) are not supported. The
   PlanetScale relationship is managed by the migration engineer, not
   by sql-splitter.

6. **Multi-source concurrent connections.** One source database at a time.
   Multi-source (e.g., diff between live DB and file dump) requires two
   sources and is different from the core extraction path. It can be built
   on top of the `DbSource` trait later.
