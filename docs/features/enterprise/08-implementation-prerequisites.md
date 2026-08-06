# Implementation Prerequisites: Make Change Easy

> What must happen before `src/migrate/` can exist. Refactoring audit +
> coherence fixes. Ordered by dependency: each item unlocks the next.

## Coherence Fixes Before Implementation

These corrections make the design documents internally consistent so the
implementation matches a single source of truth.

### C-FIX-1: Reconcile PlanetScale subprocess position

**Problem**: `03-connection-architecture.md` §5.2.6 describes a
`PlanetScaleSource` that wraps `pscale database dump` as a subprocess.
`05-review-findings.md` §7.6 adopts the opposite decision — the executor
never invokes `pscale`.

**Fix**: Remove the subprocess wrapper code from `03`'s `PlanetScaleSource`
struct. Replace with: "For PlanetScale sources, data comes from mydumper
dump files produced by `pscale database dump`. The dump step is a
pre-condition, documented in the migration runbook, but not automated by
sql-splitter. See §7.6 of `05-review-findings.md` for the rationale."

### C-FIX-2: Harmonize phase numbering

**Problem**: The architecture diagram in `04-execution-design.md` uses
Phase 0–4. The plan operations table in the same document uses Phase 1–4
for different concepts. The README uses Phase 0–5 for implementation
sequencing. `01-migration-landscape.md` uses Phase 1–4 for milestone
grouping.

**Fix**: Rename all internal phase labels to descriptive names in `04`:

| Old Label (diagram) | Old Label (plan table) | New Label         |
| ------------------- | ---------------------- | ----------------- |
| Phase 0             | —                      | Schema Extraction |
| Phase 1             | Phase 1                | Plan Generation   |
| Phase 2             | Phase 2                | DDL Execution     |
| Phase 3             | —                      | Data Migration    |
| Phase 3b            | Phase 3                | Post-Data DDL     |
| Phase 4             | Phase 4                | Verification      |

Keep the README's Phase 0–5 for implementation sequencing (different
concept: when do we build each component, not what order the executor
runs steps).

### C-FIX-3: Update `03` for text protocol decision

**Problem**: `03-connection-architecture.md`'s `MySqlSource` code uses
`exec_iter()` (binary protocol). `05-review-findings.md` HF-6 requires
`query_iter()` (text protocol). The README says Phase 0 uses text protocol.

**Fix**: Replace `self.conn.exec_iter(&query)?` with
`self.conn.query_iter(&query)?` in `03`'s code example. Add a comment
explaining: "Text protocol returns human-readable text — identical to
mysqldump output. Avoids binary protocol edge cases with BIT/ENUM/SET
types and server-side prepared statement exhaustion."

### C-FIX-4: Add missing CLI flags to `04` §6.8

**Problem**: Five flags from cross-industry research and review findings
are not in the CLI summary. Two flags in §6.8 have no prose description.

**Fix**: Add to §6.8:

- `--chunk-time <seconds>` (from cross-industry §8.3)
- `--parallel-indexes <N>` (from cross-industry §8.6)
- `--neon-branch` (from cross-industry §8.12)
- `--neon-disable-suspend` (from review CF-5)

Add prose for:

- `--verify-sample <N>`: "Sample N rows per mismatched table for detailed
  row-level diff during verification."
- `--profile <PATH>`: removed from the Dev flags — it was a placeholder
  without a design.

### C-FIX-5: Orphan gaps 4–8 from `02`

**Problem**: `02-capability-audit.md` identifies eight gaps. The README
phases only cover Gaps 1–3. Gaps 4–8 (PlanetScale validation, risk
assessment, runbook generation, stored procedure cataloging, middleware
pipeline) have no timeline.

**Fix**: Add a "Gap Assignment" table to the README:

| Gap                           | Assigned Phase                        | Rationale                                 |
| ----------------------------- | ------------------------------------- | ----------------------------------------- |
| 1 (Live DB connection)        | Phase 0–1                             | Foundation                                |
| 2 (Migration plan generation) | Phase 1                               | Foundation                                |
| 3 (Data verification)         | Phase 4                               | Depends on live data import               |
| 4 (PlanetScale validation)    | Phase 1                               | Schema analysis — low effort, high value  |
| 5 (Risk assessment)           | Phase 1                               | Analysis layer on existing data           |
| 6 (Runbook generation)        | Phase 4                               | Template rendering from structured output |
| 7 (Stored proc cataloging)    | Phase 2                               | Needed for cross-dialect migration        |
| 8 (Middleware pipeline)       | Already exists (stdin/stdout support) | Documented as design pattern              |

### C-FIX-6: Add `--copy-mode direct` or remove the reference

**Problem**: `04-execution-design.md` prose references `--copy-mode direct`
for cross-DB optimization, but it's not in the CLI flags.

**Fix**: Add to §6.8: `--copy-mode <MODE>`: `client` (default, data flows
through sql-splitter process) or `direct` (uses INSERT INTO target SELECT

- FROM source when both are same DB type on same server). Add a 3-line
  description: "Direct mode avoids client-side data movement by delegating
  the transfer to the database server via INSERT...SELECT. Only available
  when source and target are the same database type."

---

## Refactoring Prerequisites (Make Change Easy)

These are changes to the existing codebase that make the migration
features straightforward to implement. They don't add migration
functionality — they restructure what already exists so the new code
fits naturally.

### R0: Add `SchemaGraph::topo_levels()` — 40 lines

**Why**: The parallel import design (§6.3b) requires grouping tables by
dependency level. Kahn's algorithm exists in `topo_sort()` but produces a
flat list. `topo_levels()` groups by wave.

**Where**: `src/schema/graph.rs`, new method on `SchemaGraph`

**What**:

```rust
pub struct TopoLevels {
    pub levels: Vec<Vec<TableId>>,
    pub cyclic_tables: Vec<TableId>,
}

impl SchemaGraph {
    pub fn topo_levels(&self) -> TopoLevels {
        // Extends topo_sort() to group by wave:
        // Wave 0: tables with in_degree == 0
        // Process all wave-0 tables, decrement children's in_degree
        // Wave 1: tables whose last parent was in wave 0
        // ... etc.
    }
}
```

**Unlocks**: Phase 5 parallel import. Existing `topo_sort()` callers are
unaffected.

### R1: Make `TypeMapper` methods accessible — 1 line

**Why**: The `RowTypeConverter` needs type mapping information to convert
row values between dialects. The `TypeMapper` struct is already `pub`, but
`map_column_type()` is `pub(crate)`.

**Where**: `src/convert/types.rs`

**What**: Change `pub(crate) fn map_column_type` to `pub fn map_column_type`.

**Unlocks**: `RowTypeConverter` in `src/migrate/`.

### R2: Extract `for_each_statement()` helper — ~100 lines

**Why**: Seven parser loops exist across the codebase with identical
boilerplate: open input, create parser, loop over `read_statement()`,
handle COPY pairing. The migration executor needs to read from files
for schema extraction and from live DBs for data. A shared combinator
avoids an 8th copy-paste.

**Where**: `src/parser/mod.rs` or a new `src/parser/combinators.rs`

**What**:

```rust
pub struct StatementEvent {
    pub stmt_type: StatementType,
    pub table_name: Option<String>,
    pub sql: Vec<u8>,
    pub copy_data: Option<Vec<u8>>, // filled for COPY + data
}

pub fn for_each_statement<R: Read>(
    reader: R,
    dialect: SqlDialect,
    buffer_size: usize,
    mut on_statement: impl FnMut(StatementEvent) -> anyhow::Result<()>,
) -> anyhow::Result<()>;
```

COPY header/data pairing is handled internally — the callback receives
a single `StatementEvent` with both the header and data.

**Unlocks**: `Schema::from_sql_file()`, `Validator`, `Differ`, `DuckDB
loader`, `Converter`, and `Splitter` all use the same combinator instead
of their own loops. The migration executor uses it for file-based sources.

### R3: Unify COPY pairing logic — ~80 lines

**Why**: The `pending_copy_header` / `copy_context` pattern is duplicated
in splitter, validator, differ, and DuckDB loader. Each implementation
is slightly different in how it handles the `COPY ... FROM stdin` case
vs the data that follows.

**Where**: `src/parser/mod.rs`, inside `Parser<R>` or alongside
`for_each_statement()`.

**What**: The COPY pairing is handled inside `for_each_statement()`. The
parser loop stores the last COPY header. When the next `read_statement()`
call returns data bytes (not SQL), it pairs them with the stored header
and delivers `StatementType::Copy` + data as one `StatementEvent`.

**Unlocks**: All file-reading consumers get COPY handling for free. The
migration executor's file-source path inherits it.

### R4: Add `migrate` Cargo feature — ~5 lines

**Why**: The `mysql` and `postgres` crates add significant compile time.
Feature-gating lets users who only need file-based operations skip the
DB drivers.

**Where**: `Cargo.toml`

**What**:

```toml
[features]
migrate = ["dep:mysql", "dep:postgres"]

[dependencies]
mysql = { version = "28", optional = true }
postgres = { version = "0.19", optional = true }
```

This follows the existing `duckdb-query` feature pattern.

**Unlocks**: Phased deployment: file-based migration works without DB
drivers. Live DB connections require `--features migrate`.

### R5: Add migration diagnostic codes — ~30 lines

**Why**: The migration executor needs diagnostic codes (`MIG-*` prefix)
following the existing `GEN-*` pattern in `src/diagnostic/codes.rs`.

**Where**: `src/diagnostic/codes.rs`, new module block

**What**: A subset of the migration-specific codes from the design:

```
MIG-CONNECT-SOURCE, MIG-CONNECT-TARGET, MIG-UNSUPPORTED-CHARSET,
MIG-COLUMN-NOT-NULL-NO-DEFAULT, MIG-FK-ORPHAN, MIG-TYPE-LOSSY,
MIG-ZERO-DATE, MIG-ROW-SKIPPED, MIG-VERIFY-MISMATCH,
MIG-ROW-COUNT-DELTA, MIG-PLANETSCALE-FK-HASH, MIG-DDL-NON-TRANSACTIONAL,
MIG-TRIGGER-ENABLED, MIG-DDL-INCOMPLETE-TABLE, MIG-SCHEMA-CHANGED
```

**Unlocks**: Error handling in `MigrateOutcome` and `MigrateError`.

---

## Implementation Sequence (Make the Easy Change)

After the refactoring prerequisites, implementation follows the README
phases. Each phase builds on the previous.

| Phase | What                                                                                                                                                                                                                                                                                                                                            | New Code     | Depends On       |
| ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ | ---------------- |
| **0** | `DbSource` trait + `MySqlSource` + `PostgresSource` (schema-only). Use `query_iter()` text protocol. Schema extraction via `information_schema` batch queries.                                                                                                                                                                                  | ~600 lines   | R1, R4, R5       |
| **1** | `MigrationPlan` generation from `SchemaDiff`. Column rename heuristic in `compare_tables()`. Self-referential FK intra-table ordering. `NOT VALID` for PG constraints. PlanetScale validation profile in `Validator`. Risk assessment layer.                                                                                                    | ~600 lines   | R0, Phase 0      |
| **2** | `DbTarget` trait + `MySqlTarget` + `PostgresTarget`. `RowTypeConverter` for cross-dialect data. FK/unique-check disable per worker connection. `INSERT ... ON DUPLICATE KEY` idempotency with `--strict-insert` fallback. `sql_mode` session override + `SHOW WARNINGS` capture. `--binlog-handling`. Stored procedure/view/trigger cataloging. | ~1,800 lines | R1, Phase 0      |
| **3** | `migrate` command. File-to-file, file-to-DB, DB-to-file, DB-to-DB modes. `--chunk-column` with boundary discovery and skew detection. `--chunk-time` adaptive sizing. `--staging-strategy rename-safe` for MySQL DDL. Sequential Phase 3b post-data DDL. Neon direct-connection enforcement + `--neon-disable-suspend`.                         | ~1,000 lines | Phase 1, Phase 2 |
| **4** | Verification: chunk-aware SHA-256/XXH3 checksums. Rowcount mode. Schema-change detection (GTID capture before/after). `--bench` throughput measurement. Runbook generation from plan output.                                                                                                                                                    | ~500 lines   | Phase 3          |
| **5** | `--parallel N` with `topo_levels()` worker pool. Intra-table parallelism via chunk-column. `--parallel-indexes N`. Partition-aware import. `--copy-mode direct`.                                                                                                                                                                                | ~800 lines   | R0, Phase 3      |

---

## What Should NOT Be Done (From Cross-Industry Consensus)

These were considered during research and explicitly rejected by industry
practice. Do not build:

1. **Trigger-based CDC or binlog tail replication** — separate product
   category (Debezium, VReplication, ClickPipes, DMS). All major
   companies have abandoned triggers for log-based approaches.

2. **Schema versioning / migration history tracking** — separate product
   category (Flyway, Liquibase, Atlas). 20+ years of specialized
   development in this space.

3. **SQL linting engine with 100+ rules** — exists (Bytebase, Atlas).
   Better to integrate than rebuild.

4. **Automatic shard key / distribution key selection** — requires
   workload analysis, not schema analysis. All distributed DB vendors
   have their own advisor tools.

5. **Mixing DDL and DML in the same transaction** — impossible on MySQL,
   dangerous illusion on PostgreSQL for migration contexts (a 6-hour
   transaction is worse than no transaction).

6. **Subprocess orchestration of `pscale`/`mydumper`/`pgloader`** — these
   are separate tools with their own lifecycle management. sql-splitter
   is middleware between them, not a wrapper around them.

---

## Dependency Graph (What Blocks What)

```
R1 (pub TypeMapper)
 │
 ├─► R5 (diagnostic codes)
 │
 ├─► Phase 0 (DbSource trait)
 │     │
 │     ├─► Phase 2 (DbTarget trait + RowTypeConverter)
 │     │     │
 │     │     └─► Phase 3 (migrate command)
 │     │           │
 │     │           ├─► Phase 4 (verification + benchmarks)
 │     │           │
 │     │           └─► Phase 5 (parallel import)
 │     │
 │     └─► Phase 1 (MigrationPlan + validation + risk assessment)
 │           │
 │           └─► Phase 3 (migrate command)
 │
 ├─► R0 (topo_levels) ──► Phase 5 (parallel import)
 │
 ├─► R2/R3 (for_each_statement + COPY unification)
 │     └─► Phase 0/3 (file-source path in migration executor)
 │
 └─► R4 (migrate feature flag) ──► Phase 0+ (all DB-dependent code)
```

The critical path is R1 → Phase 0 → Phase 2 → Phase 3. Everything else is
parallelizable or deferrable.
