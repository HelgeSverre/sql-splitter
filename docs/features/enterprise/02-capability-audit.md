# sql-splitter Capability Audit

## Current Architecture

### Entry Points and Command Layer

- `src/main.rs` — Binary entry point
- `src/lib.rs` — Library API
- `src/cmd/mod.rs` — `Commands` enum (16 commands) and dispatch
- `src/cmd/common.rs`, `src/cmd/glob_util.rs` — Shared command plumbing

### SQL Input, Parsing, and Output

- `src/parser/mod.rs` — Dialect detection and streaming SQL parser
  - `detect_dialect()` reads up to 8KB from header (transparently decompressing)
  - Supports MySQL, Postgres, SQLite, MSSQL
  - `read_statement()` buffers whole statements (used by diff, validate, redact)
  - `visit_events()` streams INSERT rows and COPY data (used by sample, shard)
  - Bounded memory: buffer drains already-returned data on each refill
  - Safety caps: max DDL 256MB, max header 64MB, max row 256MB
- `src/splitter/mod.rs` — Coordinates input decoding, parser events, table filtering, archive/compression, writer pipeline
- `src/writer/mod.rs` — `ParallelWriters` with hash-sharded table→writer assignment, per-file compression, adaptive I/O profiles
  - Output formats: raw `.sql`, `.sql.gz`, `.sql.bz2`, `.sql.xz`, `.sql.zst`
  - Archive output: `.tar`, `.tar.gz`, `.tar.zst`, `.tar.bz2`, `.tar.xz`, `.zip`
  - I/O profiles: Ssd (parallel, small buffers), SlowSeek (single writer, large writes), SlowOps (fewest, largest operations)
  - Feedback controller: monitors throughput and backpressure, retunes profile mid-run
- `src/archive.rs`, `src/zip_input.rs` — Feature-gated archive input and output

### Schema and Transformation Domains

- `src/schema/` — DDL parsing into shared schema model and dependency graph
  - `ddl.rs`: `SchemaBuilder` parses CREATE TABLE, ALTER TABLE, CREATE INDEX, PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
  - `mod.rs`: `Schema`, `TableSchema`, `Column`, `ColumnType`, `ForeignKey`
  - `graph.rs`: `SchemaGraph` with topological sort (Kahn's), ancestor/descendant queries, cycle detection
  - `build.rs`: `Schema::from_sql_file()` — streams SQL file, transparently decompresses, builds schema from DDL only
- `src/transform_common.rs` — FK-aware transformations: disables/enables FK checks during import, handles COPY data pairing
- Domain modules: `src/analyzer/`, `src/merger/`, `src/convert/`, `src/validate/`, `src/differ/`, `src/redactor/`, `src/sample/`, `src/shard/`, `src/graph/`
- `src/duckdb/` — Optional DuckDB query feature

### Synthetic Data Generation Pipeline

Staged library+CLI: schema → profiling → inference → compilation → plan → generation → rendering → verification.

### Features and Dialects

- `duckdb-query`: DuckDB integration and `query` command
- `compression`: Compressed input and per-file compressed output
- `archive`: Archive support (implies compression)
- `man-pages`: Hidden man-page generator
- Default build includes DuckDB, compression, and archives

## Command-by-Command Migration Relevance

| Command      | Current Capability                                 | Migration Relevance                                                                                          | Gaps                                                                                                       |
| ------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| **split**    | Split dump into per-table files                    | PlanetScale exports are per-table; splitting is the first step for analysis                                  | Already capable                                                                                            |
| **analyze**  | Display table statistics                           | Pre-migration assessment: table sizes, row counts, column types                                              | Add charset/collation reporting, FK complexity scoring                                                     |
| **merge**    | Merge split SQL files into single dump             | Prepare DDL-only or data-only merged files for target import                                                 | Add `--fk-order` flag to reorder merged output by FK dependency (uses existing `SchemaGraph::topo_sort`)   |
| **sample**   | Create reduced dataset preserving FK relationships | Test migration on a subset; validate tooling without full data                                               | Add row-level MSSQL processing (parser supports MSSQL DDL; sample/shards need MSSQL INSERT handling)       |
| **shard**    | Extract tenant-specific data                       | Multi-tenant → single-tenant migration; PlanetScale shard extraction                                         | Add row-level MSSQL processing (same gap as sample)                                                        |
| **convert**  | Convert between MySQL/PostgreSQL/SQLite/MSSQL      | MySQL→Postgres migration; type mapping; identifier quoting                                                   | Add COPY-to-INSERT as an option; improve zero-date handling; add charset conversion                        |
| **validate** | Validate syntax, encoding, data integrity          | Pre-migration validation: check dump for issues before attempting migration                                  | Add PlanetScale schema rules (must have unique keys, no procedures, etc.); add target compatibility checks |
| **diff**     | Compare two dumps: schema + data                   | Migration verification: compare source and target schemas; verify data integrity                             | Add data migration SQL generation; add live-DB-to-dump comparison                                          |
| **redact**   | Redact PII from SQL dumps                          | Sanitize production data for migration testing                                                               | Add MSSQL support                                                                                          |
| **generate** | Generate synthetic data from model                 | Create test data that matches production schema for migration testing                                        | Profile-driven model inference already exists                                                              |
| **graph**    | Generate ERD from SQL dump                         | Visual migration planning; dependency visualization                                                          | Already capable                                                                                            |
| **order**    | Output dump with tables in topological FK order    | Ensure correct import order on target (children before parents for DROP, parents before children for CREATE) | Already capable (`--reverse` flag for DROP ordering)                                                       |
| **query**    | Query SQL dumps using DuckDB                       | Ad-hoc analysis of dump content; complex validation queries                                                  | Already capable                                                                                            |
| **N/A**      | **Live database connection**                       | Extract schema and data directly from source DB                                                              | **MISSING — critical gap**                                                                                 |
| **N/A**      | **Migration plan generation**                      | Produce ordered, hazard-annotated DDL scripts from schema diff                                               | **MISSING — critical gap**                                                                                 |
| **N/A**      | **Data verification (row-level)**                  | Compare source and target data row-by-row or via checksums                                                   | **MISSING — partially built** (diff has data comparison but no migration verification mode)                |
| **N/A**      | **Runbook generation**                             | Produce human-readable migration documentation                                                               | **MISSING**                                                                                                |
| **N/A**      | **Pre-migration risk assessment**                  | Score a migration for risk factors before execution                                                          | **MISSING**                                                                                                |

## Detailed Gap Analysis

### Gap 1: Live Database Connection (CRITICAL)

**Current state**: sql-splitter reads only from files (plain SQL, gzip, bzip2, xz, zstd, zip archives).

**What's needed**:

- MySQL connection (via `mysql` crate or direct TCP protocol)
- PostgreSQL connection (via `tokio-postgres` or `sqlx`)
- Schema extraction: `information_schema` queries or `SHOW CREATE TABLE` / `pg_dump --schema-only`-equivalent
- Data streaming: `SELECT * FROM table` with streaming/chunking (cursor-based, not buffered)
- PlanetScale-specific: `pscale database dump` subprocess invocation or direct `pscale` API integration

**Architecture**: Add a `DbSource` trait that mirrors the streaming input model.
The critical constraint is that the entire existing pipeline is **streaming and
bounded-memory** — databases must produce parser events lazily, never buffering
full datasets in memory or writing full dumps to temp files.

```rust
/// A live database producing schema metadata and streaming row events.
///
/// Mirrors the file-based `open_input_opt_progress()` → parser → events
/// pipeline, but from a live connection instead of a file.
pub trait DbSource {
    /// Extract schema metadata (fast, seconds). Uses information_schema
    /// or pg_dump --schema-only. Does NOT stream data.
    fn extract_schema(&mut self) -> Result<Schema>;

    /// Stream rows from a table as parser-compatible events.
    /// Lazy — rows are pulled from the driver on demand, bounded by the
    /// driver's fetch buffer (~few MB). No Vec<Row> accumulation.
    fn stream_table_rows(
        &mut self,
        table: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<RowBatch>> + '_>>;
}

struct RowBatch {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>,  // configurable batch size, default 10_000
}
```

Key design decisions:

1. **No `dump() -> PathBuf`.** Writing a full dump to a temp file before
   processing breaks the streaming guarantee. A 500 GB PlanetScale dump
   would need 500 GB of free disk on the migration engineer's machine.
   Instead, rows stream directly from the driver through the parser event
   pipeline — the same way file-based input works.

2. **Schema extraction is separate from data streaming.** `extract_schema()`
   runs `information_schema` queries or `pg_dump --schema-only` (subprocess).
   Metadata-only, fast (seconds for even large schemas). The result is a
   `Schema` struct that feeds directly into `SchemaGraph::from_schema()` and
   the existing analysis pipeline.

3. **Chunked streaming, not row-at-a-time.** `RowBatch` batches 10,000 rows
   to avoid per-row trait call overhead while keeping memory bounded at
   ~4–8 MB per batch.

4. **Synchronous drivers match the existing architecture.** The `mysql`
   crate (v25) and `postgres` crate (v0.19) are synchronous, matching the
   codebase's current I/O model. No async runtime needed. If connection
   pooling becomes necessary later, async drivers can be bridged behind the
   same trait via `tokio::runtime::Runtime::block_on()`.

5. **Read-only by construction.** The `DbSource` trait has no `execute()`
   method. Writes to target databases go through a separate `DbTarget` trait
   that is opt-in behind a CLI flag (`--execute`). This is a safety boundary,
   not a performance optimization.

For the detailed crate choices, PlanetScale subprocess handling, error model,
and connection lifecycle, see `05-connection-architecture.md`.

**Effort**: Medium-high. Two protocol implementations (MySQL + PostgreSQL),
TLS, error handling, streaming result set integration with parser events.
The parser and schema infrastructure already exist — the connection layer
is additive, not rewiring.

### Gap 2: Migration Plan Generation (CRITICAL)

**Current state**: `src/differ/output/sql.rs` generates basic ALTER/CREATE/DROP
statements from schema diffs. But:

- No ordering by dependency (the generated SQL is flat)
- No hazard annotations ("this DROP TABLE will lose data")
- No statement timeouts or lock timeouts
- No zero-downtime strategies (proxy indexes, NOT VALID constraints)
- No data migration included (line 386: "Data changes detected but not included in migration script")
- No rollback plan generation

**What's needed**: A `MigrationPlan` struct that wraps a schema diff with:

```rust
struct MigrationPlan {
    steps: Vec<MigrationStep>,
    hazards: Vec<Hazard>,
    estimated_duration: Option<Duration>,
    rollback_plan: Vec<MigrationStep>,
    pre_checks: Vec<PreCheck>,
    post_checks: Vec<PostCheck>,
}

enum Hazard {
    DataLoss { table: String, rows: u64 },
    DowntimeRequired { reason: String },
    LongRunning { table: String, estimated_rows: u64 },
    Irreversible { reason: String },
    IndexRebuild { table: String, index: String },
    ConstraintValidation { table: String, constraint: String },
}
```

The existing schema graph (`src/schema/graph.rs`) already provides topological
ordering. The existing type converter (`src/convert/types.rs`) provides column
type mapping. What's needed is:

1. Group the diff operations into ordered migration steps
2. Annotate each step with hazards
3. Generate both forward and rollback DDL
4. Estimate duration from table sizes (which `analyze` already collects)

**Effort**: Medium. Most infrastructure exists. This is an orchestration layer
on top of `differ`, `schema`, `convert`, and `analyze`.

### Gap 3: Migration Data Verification (HIGH)

**Current state**: `src/differ/data.rs` compares two dumps via `DataDiffer`:
it scans both files, builds `AHashMap<PkHash, RowDigest>` per table, then
does a map-level join in `compute_diff()`. This works for diffing (where you
want row-level add/modify/remove details) but is **unsuitable for migration
verification** for two reasons:

1. **Memory-bounded to `max_pk_entries`** (default 10M global, 5M per-table).
   A 450 GB database like Sportmonks' PlanetScale deployment has billions of
   rows — the existing differ will truncate.

2. **No chunked mode.** The map-based comparison cannot be parallelized
   across chunks, and the entire comparison runs in one pass with a single
   join at the end.

**What's needed — two modes:**

**Mode A: Table-level checksum (fast, always works).** For each table on
source and target, stream all rows through a hasher and produce a single
hex digest. This requires only one row in memory at a time and uses O(1)
memory per table. Report which tables match and which don't. This is
sufficient for confirming "the migration moved all the data correctly" —
it's the standard pt-table-checksum approach.

**Mode B: Chunked comparison (for mismatch investigation).** When a
table-level checksum shows a mismatch, divide the table into chunks by
PK range, stream-hash each chunk on both sides, and report which chunks
differ. This bounds the data that must be read twice (only the mismatched
chunk, not the whole table) and enables focused debugging.

**Algorithm for Mode A (table checksum):**

```
for each table:
    let mut hasher = Sha256::new();
    for row in source.stream_rows(table):
        hasher.update(hash_row(&row, &schema));
    let source_hash = hasher.finalize();
    // repeat for target
    if source_hash != target_hash: report mismatch
```

`hash_row()` uses the same `hash_pk_values()` + `hash_row_digest()` functions
from `src/differ/data.rs` (already battle-tested). The key difference is the
hasher is **incremental** (SHA-256) rather than **collective** (PkHash map),
so only one row is in memory at a time.

**Algorithm for Mode B (chunked comparison):**

```
let chunks = partition_by_pk_range(table, chunk_size=100_000);
for chunk in chunks:
    let source_hash = stream_hash_rows(source, table, chunk.min_pk, chunk.max_pk);
    let target_hash = stream_hash_rows(target, table, chunk.min_pk, chunk.max_pk);
    if source_hash != target_hash:
        emit MismatchedChunk { table, chunk_range, ... }
```

**Integration with existing code**: `DataDiffer::scan_file()` already
uses `hash_pk_values()` and `hash_row_digest()`. The verification
mode reuses these same hash functions but feeds them into an incremental
SHA-256 rather than a `HashMap<PkHash, u64>`. No new hashing logic needed.

**PlanetScale-specific**: For PlanetScale sources, data comes from
`pscale database dump` output files (per-table `.sql` files). The existing
`visit_events()` parser already reads these. Table-level checksums
produced from the dump files can be compared against checksums produced
from the target MySQL database after import.

**Memory model**:

- Mode A: O(1) per table (one row at a time)
- Mode B: O(1) per chunk (one row at a time)
- No external sort needed (unlike the original "spill-sort-merge" proposal)
- No temp files. No disk I/O beyond reading source/target data.

**Effort**: Low-Medium. The row hashing functions exist in `src/differ/data.rs`.
The incremental SHA-256 wrapper is ~50 lines of Rust. The chunk partitioning
by PK range requires running `SELECT MIN(pk), MAX(pk) FROM table` (or scanning
the dump file to find PK boundaries), which is ~100 lines.

### Gap 4: PlanetScale-Aware Validation and Target Compatibility Validation (HIGH)

**Current state**: `src/validate/mod.rs` checks syntax, encoding, and basic
data integrity via the `Validator` struct which produces `ValidationIssue`
values. No PlanetScale-specific rules.

**How to add rules**: The `Validator` already:

- Collects a `Schema` via `SchemaBuilder` during pass 1
- Has a central `add_issue()` funnel with error codes and severity
- Reports results via `CheckResults` with per-category `CheckStatus` values

Adding PlanetScale rules requires:

1. New error codes (e.g., `"PLANETSCALE_NO_UNIQUE_KEY"`)
2. New `CheckStatus` field on `CheckResults` (e.g., `pub planetscale_rules: CheckStatus`)
3. A post-schema-build analysis pass that inspects the `Schema` struct
4. A CLI flag to enable the profile (`--profile planetscale`)

**What's needed**: A validation mode that checks a dump for PlanetScale
compatibility before attempting import:

| Rule                                | Check                                                                                                       | Implementation                                                                                                                                                                                                                                                                      |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| All tables have unique non-null key | `table.primary_key.is_empty() && !table.unique_constraints.iter().any(\|uc\| all_cols_not_null(uc, table))` | Iterate `table.unique_constraints` + `table.indexes` (filter `is_unique`). For each, verify all columns are `!col.is_nullable`.                                                                                                                                                     |
| No stored procedures                | Parser event: scan for `CREATE PROCEDURE` / `CREATE FUNCTION` / `CREATE TRIGGER` / `CREATE EVENT`           | NOTE: `StatementType` does NOT currently include `CreateProcedure`, `CreateFunction`, `CreateTrigger`, `CreateEvent`, or `CreateView` variants. The parser returns `Unknown` for these. Adding statement-type classification for these is a prerequisite for this rule (see Gap 7). |
| No views                            | Parser event: scan for `CREATE VIEW`                                                                        | Same approach as stored procedures.                                                                                                                                                                                                                                                 |
| Only InnoDB tables                  | `table.create_statement` contains `ENGINE=` clause                                                          | Parse the engine from the CREATE TABLE text. `Schema` doesn't track engine (no field for it) — this is net-new parsing in the builder.                                                                                                                                              |
| Supported charsets only             | `col.collation: Option<String>` on each `Column`                                                            | The `Column` struct already has `collation`. Check against PlanetScale's allowlist (`utf8`, `utf8mb4`, `utf8mb3`, `latin1`, `ascii`).                                                                                                                                               |
| No unsupported SQL                  | Parser event scan for `LOAD DATA INFILE`, `JSON_TABLE`, `:=`                                                | Statement-type classification in the parser.                                                                                                                                                                                                                                        |
| FK compatibility                    | `table.foreign_keys` contains hash-suffixed names                                                           | `ForeignKey.name` is `Option<String>`. Check for PlanetScale hash patterns: `_fk_` followed by ~26 alphanumeric characters.                                                                                                                                                         |
| No `RENAME COLUMN`                  | Detect `ALTER TABLE ... RENAME COLUMN` in generated migration scripts                                       | Post-diff analysis on the migration plan output; not a validation rule but a plan-generation hazard.                                                                                                                                                                                |

The schema parser already tracks primary keys and unique constraints via
`TableSchema`. Adding rule checks against the `Schema` object is
straightforward.

**Effort**: Low. Schema metadata is already available. Adding rule checks
against it is a configuration + reporting layer.

### Gap 5: Pre-Migration Risk Assessment (MEDIUM)

**What's needed**: Before executing a migration, produce a report of risk
factors:

```rust
struct RiskAssessment {
    overall_risk: RiskLevel, // Low, Medium, High, Critical
    factors: Vec<RiskFactor>,
    recommendations: Vec<String>,
}

struct RiskFactor {
    severity: RiskLevel,
    category: RiskCategory, // DataLoss, Downtime, Performance, Compatibility, Irreversible
    description: String,
    affected_objects: Vec<String>,
}
```

Risk factors include:

- Tables without primary keys (unpredictable chunking for gh-ost)
- Tables with large row counts (long migration time)
- Tables with BLOB/TEXT columns (LOB handling issues)
- FK constraint changes (cascading complications)
- ENUM/SET columns (type conversion complexity)
- Cross-dialect migrations (data type mapping risks)
- Version-specific features (MySQL 8.0 features → older MySQL target)

**Effort**: Low-Medium. This is primarily an analysis layer that queries the
existing schema and statistics data, applies rules, and produces structured
output.

### Gap 6: Runbook/Documentation Generation (MEDIUM)

**What's needed**: Transform migration analysis into human-readable
documentation:

```
Migration Runbook: PlanetScale → Managed MySQL/Postgres Target
================================================================

Pre-migration Checklist:
  [ ] Verify source database accessibility
  [ ] Run pre-migration validation (no errors)
  [ ] Notify stakeholders of migration window
  [ ] Take backup of source database

Step 1: Export from PlanetScale
  Command: pscale database dump mydb production --output /tmp/export
  Expected duration: 45 minutes (150 GB)
  Output: 128 table files

Step 2: Pre-import Validation
  Command: sql-splitter validate --dialect mysql --planetscale-check /tmp/export/*
  Check: All tables have unique keys ✓
  Check: No stored procedures ✓
  ...

Step 3: Import to Target MySQL
  Command: myloader --host $DB_HOST --user $DB_USER --password $DB_PASS --database mydb --threads 8 --directory /tmp/export
  Expected duration: 2 hours

Step 4: Post-migration Verification
  Command: sql-splitter verify --source /tmp/export --target $DB_HOST ...
  ...
```

**Effort**: Low. Template-based generation from structured analysis output
(YAML/JSON). The existing `--json` output provides the data; rendering is
a presentation layer.

### Gap 7: Stored Procedure/View/Trigger Cataloging (LOW)

**Current state**: The parser handles `DELIMITER` commands for stored routines
but treats their bodies as opaque statement blobs. Views are not tracked in
the schema graph.

**What's needed for migration**:

- **Detection**: Catalog which procedures, functions, triggers, views, and
  events exist in the dump. NOTE: `StatementType` (src/parser/mod.rs:305)
  does NOT currently include variants for `CreateProcedure`,
  `CreateFunction`, `CreateTrigger`, `CreateEvent`, or `CreateView`
  — the parser returns `Unknown` for these. Adding these variants to the
  enum and statement-type detection logic is a prerequisite for automated
  cataloging (~100 lines of parser changes). Once added, building a catalog
  is about recording these during a pass, not parsing their bodies.

- **Preservation**: Pass routines through the pipeline unchanged. For
  PlanetScale → standard MySQL, the catalog tells the migration engineer
  what needs to be re-applied after import. For MySQL → PostgreSQL, the
  catalog surface-fits what must be manually rewritten.

- **Why not full AST parsing**: The bodies of procedures, functions, and
  triggers contain dialect-specific SQL (MySQL flow control, Postgres
  PL/pgSQL, etc.). Full AST parsing would require building a PL/SQL or
  PL/pgSQL parser — a separate project. Detection + cataloging + pass-through
  covers the migration use case without attempting automated procedural
  code conversion.

**Effort**: Low. Statement-type classification exists. Adding a `RoutineCatalog`
with `Vec<RoutineInfo>` (name, type, raw body text, referenced tables) and
populating it during parser traversal is ~200 lines of Rust.

### Gap 8: Migration Pipeline as Middleware (LOW)

**What's needed**: sql-splitter should operate as middleware in the
migration toolchain — not an orchestration platform that runs subprocesses,
but a filter/transform step that fits between existing tools:

```
pscale database dump → sql-splitter (validate, convert, analyze) → myloader
                   ↑                                        ↑
                   │ Existing tools                          │ Existing tools
                   │ sql-splitter as analysis step           │
```

This means:

- **Read from stdin, write to stdout** for every command (enables Unix pipes)
- **Consistent `--json` output** across all commands (already exists)
- **No subprocess orchestration in Phase 1.** The migration engineer chains
  tools with shell pipes or CI/CD scripts. sql-splitter provides the analysis
  layer in the middle of the chain.

If subprocess integration becomes valuable later, it can be added as a
separate `migrate` command that wraps the chain. But Phase 1 focuses on
the analysis primitives — what sql-splitter is already good at.

**Effort**: Low. The existing file-reading architecture already supports
stdin. Adding stdout output for all commands is a matter of changing the
output path from a file to stdout.

## Strengths to Build On

### What sql-splitter Already Excels At

1. **Multi-dialect parsing**: MySQL, PostgreSQL, SQLite, and MSSQL in one binary. No other Rust tool handles all four with streaming.

2. **Streaming architecture**: `visit_events()` processes arbitrarily large dumps without loading them into memory. This is the foundation for handling enterprise-scale migrations.

3. **Schema graph**: FK dependency tracking with topological sort is already used by `order`, `sample`, `shard`, and `graph` commands. Migration order is a solved problem.

4. **Type conversion**: The `src/convert/types.rs` module has comprehensive bidirectional type mapping across all four dialects.

5. **Diff engine**: Schema and data diffing between two dumps already works. The migration plan generator just needs to wrap this with ordering, hazards, and rollback.

6. **Parallel I/O**: The `ParallelWriters` and adaptive I/O profiles mean sql-splitter already handles high-throughput disk operations — critical for migration performance.

7. **Validation infrastructure**: The validate command checks syntax, encoding, and data integrity. Adding PlanetScale-specific rules is additive.

8. **Single binary**: No runtime dependencies. A migration engineer can `scp` one file to a jump host and run migrations.

9. **JSON output**: Machine-readable output on (almost) every command enables scripting and integration.

10. **Compression and archive handling**: Transparently reads and writes gzip, bzip2, xz, zstd, tar, and zip. Dump files are often compressed — this is already handled.

### What Makes This Uniquely Valuable

The combination of features that would otherwise require a chain of tools:

- Parse → Analyze → Validate → Convert → Diff → Order → Output

A migration engineer armed with sql-splitter could:

1. Read a PlanetScale dump (already compressed/SQL)
2. Analyze table structures and sizes
3. Convert to PostgreSQL (if needed)
4. Diff against target schema
5. Generate an ordered, hazard-annotated migration plan
6. Write the plan as JSON for CI/CD

All in one tool, with streaming, bounded-memory operation on arbitrarily large
dumps. No other tool in the Rust ecosystem — or indeed the open-source database
migration tooling ecosystem — provides this integrated pipeline.

## Existing Code That Directly Enables the Roadmap

| Capability            | Implementation                                                         | Migration Use                                           |
| --------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------- |
| FK topological sort   | `src/schema/graph.rs:111` `topo_sort()`                                | Correct CREATE/INSERT order; `--reverse` for DROP order |
| Schema diff           | `src/differ/schema.rs`                                                 | What changed between source and target                  |
| Data diff             | `src/differ/data.rs`                                                   | Verify no data loss                                     |
| SQL migration script  | `src/differ/output/sql.rs`                                             | Base for migration plan DDL generation                  |
| Type conversion       | `src/convert/types.rs`                                                 | MySQL→Postgres type mapping                             |
| COPY-to-INSERT        | `src/convert/copy_to_insert.rs`                                        | PlanetScale doesn't support COPY; generate INSERT       |
| Session helpers       | `src/transform_common.rs:420` `SET session_replication_role = replica` | Disable FK checks during import                         |
| Dialect detection     | `src/parser/mod.rs:86` `detect_dialect()`                              | Auto-detect source dialect                              |
| Streaming parser      | `src/parser/mod.rs:1132` `visit_events()`                              | Process arbitrarily large dumps                         |
| Parallel writers      | `src/writer/mod.rs:344` `ParallelWriters`                              | Fast output for large datasets                          |
| Adaptive I/O          | `src/writer/profile.rs`, `src/writer/controller.rs`                    | Optimal performance on any storage                      |
| DuckDB queries        | `src/duckdb/mod.rs`                                                    | Complex ad-hoc analysis of dump content                 |
| Schema build from SQL | `src/schema/build.rs:54` `Schema::from_sql_file()`                     | Extract full schema from any dump                       |
| JSON output           | Across all commands                                                    | Machine-readable migration artifacts                    |
