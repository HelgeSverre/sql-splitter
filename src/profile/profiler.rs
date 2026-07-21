//! Stream a SQL dump into a neutral [`DumpProfile`] in a single pass.
//!
//! [`DumpProfiler`] drives the production parser's [`Parser::visit_events`]
//! visitor: DDL statements build schema evidence as they arrive, and each
//! decoded data row is routed — value by value, by column identity — into the
//! per-column bounded sketches from [`crate::profile::sketches`]. Nothing here
//! buffers a whole INSERT/COPY block: the visitor delivers one tuple / one COPY
//! line at a time, so resident memory is bounded by the [`ProfileBudget`], never
//! by the row count.
//!
//! # Depths
//! * [`ProfileDepth::Schema`] reads no values — it counts rows and, per column,
//!   total/null tallies only (exact), producing the portable schema with no
//!   value-derived evidence.
//! * [`ProfileDepth::Basic`] adds the cheap per-column metrics: distinct
//!   estimate, min/max, quantiles, string shape, length, top-k, and a bounded
//!   sample.
//! * [`ProfileDepth::Full`] additionally computes pairwise evidence for the
//!   *bounded* candidate set: declared foreign-key pairs (child→parent coverage)
//!   and same-table temporal orderings. Planner-nominated pairs plug in through
//!   the same [`RelationshipEvidence`] surface.
//!
//! Every depth returns the *same* portable schema and the *same* exact row and
//! null counts; the depths differ only in how much value-derived evidence they
//! attach.

use std::collections::HashSet;
use std::io::Read;
use std::path::Path;

use ahash::AHashMap;

use crate::diagnostic::{codes, Diagnostic};
use crate::parser::mysql_insert::{
    hash_pk_tuple, parse_insert_tuple, InsertRowContext, ParsedRow, PkTuple, PkValue, RowExtraction,
};
use crate::parser::postgres_copy::{parse_copy_columns, CopyParser, ParsedCopyRow};
use crate::parser::{
    detect_dialect_from_file, determine_buffer_size, Parser, ParserEvent, RowFlow, SqlDialect,
    StatementType, SMALL_BUFFER_SIZE,
};
use crate::schema::{Column, ColumnType, Schema, SchemaBuilder, TableId, TableSchema};
use crate::synthetic::schema::PortableSchema;

use super::evidence::{ColumnEvidence, DumpProfile, RelationshipEvidence, TableEvidence};
use super::{ColumnSketches, ProfileBudget, ProfileDepth, ProfileValue};

/// Streams a SQL dump into a [`DumpProfile`]. Build one with
/// [`DumpProfiler::builder`].
#[derive(Debug, Clone)]
pub struct DumpProfiler {
    depth: ProfileDepth,
    budget: ProfileBudget,
    seed: u64,
    dialect: Option<SqlDialect>,
}

impl Default for DumpProfiler {
    fn default() -> Self {
        Self {
            depth: ProfileDepth::Full,
            budget: ProfileBudget::default(),
            seed: 0,
            dialect: None,
        }
    }
}

impl DumpProfiler {
    /// Start configuring a profiler.
    pub fn builder() -> DumpProfilerBuilder {
        DumpProfilerBuilder::default()
    }

    /// Profile a dump on disk. Opens the file through the shared input layer
    /// (transparent decompression / zip), and auto-detects the dialect from the
    /// header unless one was configured on the builder.
    pub fn profile_path(&self, path: &Path) -> anyhow::Result<DumpProfile> {
        let dialect = match self.dialect {
            Some(d) => d,
            None => detect_dialect_from_file(path)?.dialect,
        };
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let buffer_size = determine_buffer_size(file_size);
        let reader = crate::splitter::open_input(path)?;
        self.run(reader, dialect, buffer_size)
    }

    /// Profile a dump from an arbitrary reader. The dialect must be given
    /// explicitly (there is no file header to sniff a compressed input from).
    pub fn profile_reader<R: Read>(
        &self,
        reader: R,
        dialect: SqlDialect,
    ) -> anyhow::Result<DumpProfile> {
        self.run(reader, dialect, SMALL_BUFFER_SIZE)
    }

    fn run<R: Read>(
        &self,
        reader: R,
        dialect: SqlDialect,
        buffer_size: usize,
    ) -> anyhow::Result<DumpProfile> {
        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);
        let mut run = ProfileRun::new(self.depth, self.budget, self.seed, dialect);
        parser.visit_events(|event| run.on_event(event))?;
        Ok(run.finish())
    }
}

/// Builder for [`DumpProfiler`].
#[derive(Debug, Clone, Default)]
pub struct DumpProfilerBuilder {
    inner: DumpProfilerConfig,
}

// A small config carrier so the builder can start from `Default` without
// exposing the profiler's private fields.
#[derive(Debug, Clone)]
struct DumpProfilerConfig {
    depth: ProfileDepth,
    budget: ProfileBudget,
    seed: u64,
    dialect: Option<SqlDialect>,
}

impl Default for DumpProfilerConfig {
    fn default() -> Self {
        let d = DumpProfiler::default();
        Self {
            depth: d.depth,
            budget: d.budget,
            seed: d.seed,
            dialect: d.dialect,
        }
    }
}

impl DumpProfilerBuilder {
    /// How deep to profile (default [`ProfileDepth::Full`]).
    pub fn depth(mut self, depth: ProfileDepth) -> Self {
        self.inner.depth = depth;
        self
    }

    /// Per-column retention budget (default [`ProfileBudget::default`]).
    pub fn budget(mut self, budget: ProfileBudget) -> Self {
        self.inner.budget = budget;
        self
    }

    /// Seed for the reservoir sampler, making retained samples reproducible.
    pub fn seed(mut self, seed: u64) -> Self {
        self.inner.seed = seed;
        self
    }

    /// Force a SQL dialect instead of auto-detecting from the file header.
    pub fn dialect(mut self, dialect: SqlDialect) -> Self {
        self.inner.dialect = Some(dialect);
        self
    }

    /// Finalize the profiler.
    pub fn build(self) -> DumpProfiler {
        DumpProfiler {
            depth: self.inner.depth,
            budget: self.inner.budget,
            seed: self.inner.seed,
            dialect: self.inner.dialect,
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming run state
// ---------------------------------------------------------------------------

/// Coarse column family, derived once from the schema so row observation never
/// has to re-borrow the schema to decide how to interpret a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Bool,
    Int,
    BigInt,
    Decimal,
    DateTime,
    Json,
    Text,
}

impl ColumnKind {
    fn from_column(col: &Column) -> Self {
        if col.source_type.to_ascii_lowercase().contains("json") {
            return ColumnKind::Json;
        }
        match col.col_type {
            ColumnType::Bool => ColumnKind::Bool,
            ColumnType::Int => ColumnKind::Int,
            ColumnType::BigInt => ColumnKind::BigInt,
            ColumnType::Decimal => ColumnKind::Decimal,
            ColumnType::DateTime => ColumnKind::DateTime,
            ColumnType::Text | ColumnType::Uuid | ColumnType::Other(_) => ColumnKind::Text,
        }
    }
}

/// Per-column accumulator: exact total/null tallies at every depth, plus the
/// value sketches at [`ProfileDepth::Basic`] and above.
struct ColumnAccum {
    name: String,
    kind: ColumnKind,
    total: u64,
    nulls: u64,
    sketches: Option<ColumnSketches>,
}

/// A same-table pair of timestamp columns, tracked for temporal ordering
/// (`from <= to`) at [`ProfileDepth::Full`].
struct TemporalPair {
    from: usize,
    to: usize,
    both_present: u64,
    ordered: u64,
}

/// A budget-capped set of 64-bit value hashes. Once full it stops inserting and
/// records that it overflowed, so retention never scales with the row count.
struct BoundedHashSet {
    set: HashSet<u64>,
    cap: usize,
    overflowed: bool,
}

impl BoundedHashSet {
    fn new(cap: usize) -> Self {
        Self {
            set: HashSet::new(),
            cap: cap.max(1),
            overflowed: false,
        }
    }

    fn insert(&mut self, hash: u64) {
        if self.set.len() < self.cap {
            self.set.insert(hash);
        } else if !self.set.contains(&hash) {
            self.overflowed = true;
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.set.contains(&hash)
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// A declared foreign key reduced to the local column ordinals that carry its
/// child values, so coverage can be tracked without re-borrowing the schema per
/// row. `referenced_table_id` is resolved only at `build()`, so we key by
/// `fk_index` and resolve the parent at finish.
struct FkCandidate {
    fk_index: u16,
    ordinals: Vec<usize>,
}

/// Per-table profiling state.
struct TableProfile {
    name: String,
    table_id: TableId,
    row_count: u64,
    columns: Vec<ColumnAccum>,
    // Full-depth relationship tracking (empty otherwise).
    pk_hashes: BoundedHashSet,
    fk_candidates: Vec<FkCandidate>,
    fk_child: AHashMap<u16, BoundedHashSet>,
    temporal: Vec<TemporalPair>,
    /// Whether a `GEN-PROFILE-DECODE-SKIPPED` warning has already been raised for
    /// this table, so a run of undecodable rows warns once, not per row.
    decode_warned: bool,
}

/// Buffered rows for a table whose data arrived before its DDL, retained up to
/// the sample budget for a bounded replay once the schema appears.
struct PendingTable {
    row_count: u64,
    retained: Vec<PendingRow>,
    overflowed: bool,
    header: Option<Vec<u8>>,
    copy_columns: Option<Vec<String>>,
}

enum PendingRow {
    Insert(Vec<u8>),
    Copy(Vec<u8>),
}

/// Column context for the INSERT statement currently being streamed.
struct InsertCtx {
    table_idx: usize,
    context: InsertRowContext,
}

/// Column context for the COPY block currently being streamed.
struct CopyCtx {
    table_idx: usize,
    columns: Vec<String>,
}

struct ProfileRun {
    depth: ProfileDepth,
    budget: ProfileBudget,
    seed: u64,
    dialect: SqlDialect,
    builder: SchemaBuilder,
    tables: Vec<TableProfile>,
    table_index: AHashMap<String, usize>,
    insert_ctx: Option<InsertCtx>,
    copy_ctx: Option<CopyCtx>,
    /// The schema-late COPY table whose block is currently open. `CopyRow`
    /// events carry no table name, so this is the *only* reliable way to route
    /// pending COPY rows — a predicate scan over `pending` would misroute when
    /// two tables both have COPY data before either's DDL.
    pending_copy_key: Option<String>,
    pending: AHashMap<String, PendingTable>,
    warnings: Vec<Diagnostic>,
    next_seed: u64,
}

impl ProfileRun {
    fn new(depth: ProfileDepth, budget: ProfileBudget, seed: u64, dialect: SqlDialect) -> Self {
        Self {
            depth,
            budget,
            seed,
            dialect,
            builder: SchemaBuilder::new(),
            tables: Vec::new(),
            table_index: AHashMap::new(),
            insert_ctx: None,
            copy_ctx: None,
            pending_copy_key: None,
            pending: AHashMap::new(),
            warnings: Vec::new(),
            next_seed: 0,
        }
    }

    fn key(name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn on_event(&mut self, event: ParserEvent<'_>) -> anyhow::Result<RowFlow> {
        match event {
            ParserEvent::Statement(bytes) => self.on_statement(bytes),
            ParserEvent::InsertRow {
                header,
                row,
                first_in_statement,
            } => self.on_insert_row(header, row, first_in_statement),
            ParserEvent::CopyStart(header) => self.on_copy_start(header),
            ParserEvent::CopyRow(line) => self.on_copy_row(line),
            ParserEvent::CopyEnd => {
                self.copy_ctx = None;
                self.pending_copy_key = None;
                Ok(RowFlow::Continue)
            }
        }
    }

    fn on_statement(&mut self, bytes: &[u8]) -> anyhow::Result<RowFlow> {
        let (stmt_type, name) = Parser::<&[u8]>::parse_statement_with_dialect(bytes, self.dialect);
        self.builder.ingest_statement(bytes, self.dialect);
        if stmt_type == StatementType::CreateTable && !name.is_empty() {
            self.register_table(&name);
        }
        Ok(RowFlow::Continue)
    }

    /// Ensure a [`TableProfile`] exists for a freshly created table, then replay
    /// any rows that were buffered before the DDL arrived.
    fn register_table(&mut self, name: &str) {
        let key = Self::key(name);
        if self.table_index.contains_key(&key) {
            return;
        }
        let Some(table_id) = self.builder.schema().get_table_id(name) else {
            return;
        };
        let profile = self.build_table_profile(name, table_id);
        let idx = self.tables.len();
        self.tables.push(profile);
        self.table_index.insert(key.clone(), idx);

        if let Some(pending) = self.pending.remove(&key) {
            self.replay_pending(idx, pending);
        }
    }

    fn build_table_profile(&mut self, name: &str, table_id: TableId) -> TableProfile {
        let schema = self.builder.schema();
        let table = schema
            .table(table_id)
            .expect("table id just resolved from builder");
        let full = self.depth == ProfileDepth::Full;

        let columns: Vec<ColumnAccum> = table
            .columns
            .iter()
            .map(|col| {
                let kind = ColumnKind::from_column(col);
                // A credential-named, string-shaped column never enters the
                // sketches: its raw values (hashes, tokens, keys) must not be
                // retained in evidence. The exact total/null counts tracked
                // separately below are still kept, so null-rate survives.
                let credential = matches!(kind, ColumnKind::Text | ColumnKind::Json)
                    && crate::profile::heuristics::is_credential_name(&col.name);
                let sketches = if self.depth == ProfileDepth::Schema || credential {
                    None
                } else {
                    let s = ColumnSketches::new(&self.budget, self.seed ^ self.next_seed);
                    self.next_seed = self.next_seed.wrapping_add(1);
                    Some(s)
                };
                ColumnAccum {
                    name: col.name.clone(),
                    kind,
                    total: 0,
                    nulls: 0,
                    sketches,
                }
            })
            .collect();

        // Same-table temporal candidate pairs (Full only): ordered pairs of
        // timestamp columns, bounded by the schema's column count.
        let temporal = if full {
            let ts_cols: Vec<usize> = table
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| ColumnKind::from_column(c) == ColumnKind::DateTime)
                .map(|(i, _)| i)
                .collect();
            let mut pairs = Vec::new();
            for (a_pos, &a) in ts_cols.iter().enumerate() {
                for &b in &ts_cols[a_pos + 1..] {
                    pairs.push(TemporalPair {
                        from: a,
                        to: b,
                        both_present: 0,
                        ordered: 0,
                    });
                }
            }
            pairs
        } else {
            Vec::new()
        };

        // Declared FKs reduced to local column ordinals (resolved at CREATE
        // TABLE time; only the *referenced* table id is deferred to `build()`).
        let fk_candidates: Vec<FkCandidate> = if full {
            table
                .foreign_keys
                .iter()
                .enumerate()
                .filter(|(_, fk)| !fk.columns.is_empty())
                .map(|(fk_index, fk)| FkCandidate {
                    fk_index: fk_index as u16,
                    ordinals: fk.columns.iter().map(|c| c.0 as usize).collect(),
                })
                .collect()
        } else {
            Vec::new()
        };

        TableProfile {
            name: name.to_string(),
            table_id,
            row_count: 0,
            columns,
            pk_hashes: BoundedHashSet::new(self.budget.sample_rows),
            fk_candidates,
            fk_child: AHashMap::new(),
            temporal,
            decode_warned: false,
        }
    }

    // --- INSERT path -------------------------------------------------------

    fn on_insert_row(
        &mut self,
        header: &[u8],
        row: &[u8],
        first_in_statement: bool,
    ) -> anyhow::Result<RowFlow> {
        if first_in_statement {
            self.prepare_insert(header);
        }

        // Every delivered `InsertRow` is a real `(...)` tuple, so it counts
        // toward the exact row total for a known table regardless of whether the
        // secondary value decode succeeds. Decode success only governs per-column
        // value/null evidence.
        if let Some(table_idx) = self.insert_ctx.as_ref().map(|c| c.table_idx) {
            match self.decode_insert_row(row) {
                Some((idx, parsed)) => self.observe_insert(idx, &parsed),
                None => {
                    self.tables[table_idx].row_count += 1;
                    self.warn_decode_skipped(table_idx);
                }
            }
        } else {
            // Schema-late: buffer the raw tuple for replay once the DDL arrives.
            let cap = self.budget.sample_rows;
            if let Some(table) = self.pending_insert_target(header) {
                table.push_insert(row, cap);
            }
        }

        Ok(RowFlow::Continue)
    }

    fn decode_insert_row(&self, row: &[u8]) -> Option<(usize, ParsedRow)> {
        let ctx = self.insert_ctx.as_ref()?;
        let table_idx = ctx.table_idx;
        let table_id = self.tables[table_idx].table_id;
        let table = self.builder.schema().table(table_id)?;
        let parsed =
            parse_insert_tuple(row, table, &ctx.context, self.dialect, RowExtraction::Full)?;
        Some((table_idx, parsed))
    }

    fn prepare_insert(&mut self, header: &[u8]) {
        self.insert_ctx = None;
        let (_, name) = Parser::<&[u8]>::parse_statement_with_dialect(header, self.dialect);
        if name.is_empty() {
            return;
        }
        let key = Self::key(&name);
        if let Some(&table_idx) = self.table_index.get(&key) {
            let table_id = self.tables[table_idx].table_id;
            let schema = self.builder.schema();
            if let Some(table) = schema.table(table_id) {
                let context = InsertRowContext::from_header(header, table);
                self.insert_ctx = Some(InsertCtx { table_idx, context });
            }
        } else {
            // Schema-late: remember the first header so the replay can rebuild
            // context (single-layout-per-pending-table assumption — a later
            // INSERT into the same table with a different column list would
            // replay under this one).
            self.pending
                .entry(key)
                .or_insert_with(PendingTable::new)
                .header
                .get_or_insert_with(|| header.to_vec());
        }
    }

    /// Look up (creating if needed) the pending buffer for the INSERT whose
    /// header names an as-yet-unknown table.
    fn pending_insert_target(&mut self, header: &[u8]) -> Option<&mut PendingTable> {
        let (_, name) = Parser::<&[u8]>::parse_statement_with_dialect(header, self.dialect);
        if name.is_empty() {
            return None;
        }
        let key = Self::key(&name);
        let entry = self.pending.entry(key).or_insert_with(PendingTable::new);
        // Single-layout-per-pending-table: the first header wins for replay.
        entry.header.get_or_insert_with(|| header.to_vec());
        Some(entry)
    }

    // --- COPY path ---------------------------------------------------------

    fn on_copy_start(&mut self, header: &[u8]) -> anyhow::Result<RowFlow> {
        self.copy_ctx = None;
        self.pending_copy_key = None;
        let header_str = String::from_utf8_lossy(header);
        let (_, name) = Parser::<&[u8]>::parse_statement_with_dialect(header, self.dialect);
        if name.is_empty() {
            return Ok(RowFlow::Continue);
        }
        let columns = parse_copy_columns(&header_str);
        let key = Self::key(&name);
        if let Some(&table_idx) = self.table_index.get(&key) {
            self.copy_ctx = Some(CopyCtx { table_idx, columns });
        } else {
            // Schema-late COPY: record which table this open block belongs to so
            // its rows route unambiguously, and stash the column layout for
            // replay (single-layout-per-pending-table assumption).
            let entry = self
                .pending
                .entry(key.clone())
                .or_insert_with(PendingTable::new);
            entry.copy_columns.get_or_insert(columns);
            self.pending_copy_key = Some(key);
        }
        Ok(RowFlow::Continue)
    }

    fn on_copy_row(&mut self, line: &[u8]) -> anyhow::Result<RowFlow> {
        if let Some(table_idx) = self.copy_ctx.as_ref().map(|c| c.table_idx) {
            // Known table. A blank line inside a COPY block is padding, not a
            // row, so it must not count; every genuine data line counts toward
            // the exact total whether or not its values decode.
            let single_col = self.tables[table_idx].columns.len() == 1;
            if !copy_line_is_data(line, single_col) {
                return Ok(RowFlow::Continue);
            }
            match self.decode_copy_row(line) {
                Some((idx, parsed)) => self.observe_copy(idx, &parsed),
                None => {
                    self.tables[table_idx].row_count += 1;
                    self.warn_decode_skipped(table_idx);
                }
            }
        } else if let Some(key) = self.pending_copy_key.clone() {
            // Schema-late COPY: route by the explicitly tracked open-block key,
            // never a predicate scan (which would misroute with two pending
            // COPY tables). Buffer genuine data lines only. The COPY header's
            // column list gives the arity even before the CREATE TABLE, so a
            // single-column block correctly treats a blank line as an
            // empty-string row rather than padding.
            let cap = self.budget.sample_rows;
            if let Some(table) = self.pending.get_mut(&key) {
                let single_col = table
                    .copy_columns
                    .as_ref()
                    .is_some_and(|columns| columns.len() == 1);
                if copy_line_is_data(line, single_col) {
                    table.push_copy(line, cap);
                }
            }
        }
        Ok(RowFlow::Continue)
    }

    /// Raise `GEN-PROFILE-DECODE-SKIPPED` once per table when a delivered row
    /// failed value decoding: it is still counted in the exact row total, but
    /// contributes no per-column evidence.
    fn warn_decode_skipped(&mut self, table_idx: usize) {
        let table = &mut self.tables[table_idx];
        if table.decode_warned {
            return;
        }
        table.decode_warned = true;
        self.warnings.push(Diagnostic::warning(
            &codes::PROFILE_DECODE_SKIPPED,
            format!("tables.{}", table.name),
            "rows failed value decoding; they are counted in the exact row total but \
             contributed no per-column evidence",
        ));
    }

    fn decode_copy_row(&self, line: &[u8]) -> Option<(usize, ParsedCopyRow)> {
        let ctx = self.copy_ctx.as_ref()?;
        let table_idx = ctx.table_idx;
        let table_id = self.tables[table_idx].table_id;
        let table = self.builder.schema().table(table_id)?;
        let parsed = decode_copy_line(line, table, ctx.columns.clone())?;
        Some((table_idx, parsed))
    }

    // --- Row observation ---------------------------------------------------

    fn observe_insert(&mut self, table_idx: usize, row: &ParsedRow) {
        let ncols = self.tables[table_idx].columns.len();
        self.observe_row(
            table_idx,
            ncols,
            |ord| row.get_column_value(ord),
            row.pk.as_ref(),
        );
    }

    fn observe_copy(&mut self, table_idx: usize, row: &ParsedCopyRow) {
        let ncols = self.tables[table_idx].columns.len();
        self.observe_row(
            table_idx,
            ncols,
            |ord| row.get_column_value(ord),
            row.pk.as_ref(),
        );
    }

    /// The single per-row hot path shared by INSERT and COPY: tally total/null,
    /// feed the value sketches, and (at Full depth) update FK/PK/temporal
    /// evidence.
    fn observe_row<'r>(
        &mut self,
        table_idx: usize,
        ncols: usize,
        value: impl Fn(usize) -> Option<&'r PkValue>,
        pk: Option<&PkTuple>,
    ) {
        let full = self.depth == ProfileDepth::Full;
        let cap = self.budget.sample_rows;
        let table = &mut self.tables[table_idx];
        table.row_count += 1;

        for (ord, acc) in table.columns.iter_mut().enumerate().take(ncols) {
            let Some(v) = value(ord) else { continue };
            acc.total += 1;
            if v.is_null() {
                acc.nulls += 1;
            }
            if let Some(sketches) = &mut acc.sketches {
                observe_pk_value(sketches, acc.kind, v);
            }
        }

        if !full {
            return;
        }

        if let Some(pk) = pk {
            table.pk_hashes.insert(hash_pk_tuple(pk));
        }
        // Child FK values, hashed the same way parent PKs are, so coverage is a
        // plain set-membership test at finish.
        for candidate in &table.fk_candidates {
            let mut tuple = PkTuple::new();
            let mut complete = true;
            for &ord in &candidate.ordinals {
                match value(ord) {
                    Some(v) if !v.is_null() => tuple.push(v.clone()),
                    _ => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete && !tuple.is_empty() {
                table
                    .fk_child
                    .entry(candidate.fk_index)
                    .or_insert_with(|| BoundedHashSet::new(cap))
                    .insert(hash_pk_tuple(&tuple));
            }
        }
        for pair in &mut table.temporal {
            if let (Some(PkValue::Text(a)), Some(PkValue::Text(b))) =
                (value(pair.from), value(pair.to))
            {
                pair.both_present += 1;
                if a <= b {
                    pair.ordered += 1;
                }
            }
        }
    }

    // --- Schema-late replay ------------------------------------------------

    fn replay_pending(&mut self, table_idx: usize, pending: PendingTable) {
        let table_id = self.tables[table_idx].table_id;

        // Replay every retained row through the normal decode/observe path.
        for row in &pending.retained {
            match row {
                PendingRow::Insert(bytes) => {
                    let parsed = pending.header.as_ref().and_then(|header| {
                        let schema = self.builder.schema();
                        let table = schema.table(table_id)?;
                        let ctx = InsertRowContext::from_header(header, table);
                        parse_insert_tuple(bytes, table, &ctx, self.dialect, RowExtraction::Full)
                    });
                    if let Some(parsed) = parsed {
                        self.observe_insert(table_idx, &parsed);
                    }
                }
                PendingRow::Copy(bytes) => {
                    let cols = pending.copy_columns.clone().unwrap_or_default();
                    let parsed = {
                        let schema = self.builder.schema();
                        schema
                            .table(table_id)
                            .and_then(|table| decode_copy_line(bytes, table, cols))
                    };
                    if let Some(parsed) = parsed {
                        self.observe_copy(table_idx, &parsed);
                    }
                }
            }
        }

        // The retained sample may be smaller than what actually streamed by:
        // fix the row count to the true total and flag the coverage gap.
        let observed = self.tables[table_idx].row_count;
        if pending.row_count > observed {
            self.tables[table_idx].row_count = pending.row_count;
        }
        if pending.overflowed {
            self.warnings.push(Diagnostic::warning(
                &codes::PROFILE_SCHEMA_LATE,
                format!("tables.{}", self.tables[table_idx].name),
                format!(
                    "had {} data rows before its DDL; only {} were retained for value profiling \
                     (counts remain exact)",
                    pending.row_count,
                    pending.retained.len()
                ),
            ));
        }
    }

    // --- Finalize ----------------------------------------------------------

    fn finish(mut self) -> DumpProfile {
        // Resolve FK references now that all DDL has been seen.
        let schema = std::mem::take(&mut self.builder).build();

        // Map table id -> profile index, so FK coverage can reach the parent's
        // retained PK set.
        let mut id_to_idx: AHashMap<u32, usize> = AHashMap::new();
        for (idx, t) in self.tables.iter().enumerate() {
            id_to_idx.insert(t.table_id.0, idx);
        }

        // Any pending table that never got its DDL: surface its exact row count
        // and warn that its columns could not be profiled.
        let leftovers: Vec<(String, PendingTable)> = self.pending.drain().collect();
        for (key, pending) in leftovers {
            self.warnings.push(Diagnostic::warning(
                &codes::PROFILE_SCHEMA_LATE,
                format!("tables.{key}"),
                format!(
                    "had {} data rows but no CREATE TABLE was seen; it could not be profiled",
                    pending.row_count
                ),
            ));
        }

        let depth = self.depth;
        let relationships = self.compute_relationships(&schema, &id_to_idx);

        let tables = self
            .tables
            .into_iter()
            .enumerate()
            .map(|(idx, table)| {
                finish_table(table, relationships.get(&idx).cloned().unwrap_or_default())
            })
            .collect();

        // The profiler already built the full DDL schema in this same pass;
        // capture it as a portable snapshot so downstream inference never has
        // to re-read the dump to recover it.
        let portable = PortableSchema::from_runtime(&schema, self.dialect);

        DumpProfile {
            depth,
            schema: portable,
            tables,
            warnings: self.warnings,
        }
    }

    /// Build the Full-depth relationship evidence per table index.
    fn compute_relationships(
        &self,
        schema: &Schema,
        id_to_idx: &AHashMap<u32, usize>,
    ) -> AHashMap<usize, Vec<RelationshipEvidence>> {
        let mut out: AHashMap<usize, Vec<RelationshipEvidence>> = AHashMap::new();
        if self.depth != ProfileDepth::Full {
            return out;
        }

        for (idx, table) in self.tables.iter().enumerate() {
            let Some(table_schema) = schema.table(table.table_id) else {
                continue;
            };
            let mut rels = Vec::new();

            // Declared foreign keys: child -> parent coverage over bounded sets.
            for (fk_index, fk) in table_schema.foreign_keys.iter().enumerate() {
                let Some(child) = table.fk_child.get(&(fk_index as u16)) else {
                    continue;
                };
                if child.is_empty() {
                    continue;
                }
                let Some(parent_id) = fk.referenced_table_id else {
                    continue;
                };
                let Some(&parent_idx) = id_to_idx.get(&parent_id.0) else {
                    continue;
                };
                let parent = &self.tables[parent_idx].pk_hashes;
                let matched = child.set.iter().filter(|h| parent.contains(**h)).count();
                let coverage = matched as f64 / child.set.len() as f64;
                let bounded = child.overflowed || parent.overflowed;
                rels.push(RelationshipEvidence {
                    from_column: fk.column_names.join(","),
                    to_table: fk.referenced_table.clone(),
                    to_column: fk.referenced_columns.join(","),
                    coverage,
                    confidence: if bounded { 0.5 } else { 1.0 },
                });
            }

            // Same-table temporal orderings.
            for pair in &table.temporal {
                if pair.both_present == 0 {
                    continue;
                }
                let coverage = pair.ordered as f64 / pair.both_present as f64;
                rels.push(RelationshipEvidence {
                    from_column: table.columns[pair.from].name.clone(),
                    to_table: table.name.clone(),
                    to_column: table.columns[pair.to].name.clone(),
                    coverage,
                    confidence: coverage,
                });
            }

            if !rels.is_empty() {
                out.insert(idx, rels);
            }
        }
        out
    }
}

/// True when a raw COPY line is a genuine data row, matching
/// [`CopyParser::parse_line`]'s own skip rules: the `\.` terminator and (for
/// multi-column tables) a blank padding line are *not* rows. Lets the exact row
/// count be decided independently of whether the value decode succeeds.
fn copy_line_is_data(line: &[u8], empty_is_row: bool) -> bool {
    let line = if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    };
    !(line == b"\\." || (line.is_empty() && !empty_is_row))
}

/// Decode one COPY data line into a [`ParsedCopyRow`] with full column identity,
/// building a fresh (bounded, per-line) parser context bound to the schema.
fn decode_copy_line(
    line: &[u8],
    table: &TableSchema,
    columns: Vec<String>,
) -> Option<ParsedCopyRow> {
    let (parser, empty_is_row) = CopyParser::new(b"")
        .with_schema(table)
        .with_column_order(columns)
        .with_extraction(RowExtraction::Full)
        .prepared();
    parser.parse_line(line, empty_is_row)
}

/// Fold one already-decoded column value into its sketches, interpreting it
/// through the schema-derived [`ColumnKind`].
fn observe_pk_value(sketches: &mut ColumnSketches, kind: ColumnKind, value: &PkValue) {
    match value {
        PkValue::Null => sketches.observe(ProfileValue::Null),
        PkValue::Int(n) => observe_integer(sketches, kind, *n as i128),
        PkValue::BigInt(n) => observe_integer(sketches, kind, *n),
        PkValue::Text(s) => observe_text(sketches, kind, s),
    }
}

fn observe_integer(sketches: &mut ColumnSketches, kind: ColumnKind, n: i128) {
    match kind {
        ColumnKind::Bool if n == 0 || n == 1 => {
            sketches.observe(ProfileValue::Boolean(n == 1));
        }
        ColumnKind::Decimal => sketches.observe(ProfileValue::Decimal { minor: n, scale: 0 }),
        _ => match i64::try_from(n) {
            Ok(v) => sketches.observe(ProfileValue::Integer(v)),
            Err(_) => sketches.observe(ProfileValue::Decimal { minor: n, scale: 0 }),
        },
    }
}

fn observe_text(sketches: &mut ColumnSketches, kind: ColumnKind, s: &str) {
    match kind {
        ColumnKind::DateTime => sketches.observe(ProfileValue::DateTime(s)),
        ColumnKind::Json => sketches.observe(ProfileValue::Json(s)),
        ColumnKind::Bool => match parse_bool(s) {
            Some(b) => sketches.observe(ProfileValue::Boolean(b)),
            None => sketches.observe(ProfileValue::Text(s)),
        },
        ColumnKind::Decimal => match parse_decimal(s) {
            Some((minor, scale)) => sketches.observe(ProfileValue::Decimal { minor, scale }),
            None => sketches.observe(ProfileValue::Text(s)),
        },
        ColumnKind::Int | ColumnKind::BigInt => match s.parse::<i128>() {
            Ok(n) => observe_integer(sketches, kind, n),
            Err(_) => sketches.observe(ProfileValue::Text(s)),
        },
        ColumnKind::Text => sketches.observe(ProfileValue::Text(s)),
    }
}

/// Parse a boolean rendered as text (`t`/`true`/`1`/`yes` and their negatives).
fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "yes" | "y" => Some(true),
        "f" | "false" | "no" | "n" => Some(false),
        _ => None,
    }
}

/// Parse a plain fixed-point decimal string into `(minor, scale)` such that the
/// value equals `minor * 10^-scale`. Rejects anything that is not sign + digits
/// with at most one decimal point (scientific notation, currency, etc.).
fn parse_decimal(s: &str) -> Option<(i128, u8)> {
    let s = s.trim();
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((a, b)) => (a, b),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let mut minor: i128 = digits.parse().ok()?;
    if neg {
        minor = -minor;
    }
    let scale = u8::try_from(frac_part.len()).ok()?;
    Some((minor, scale))
}

/// Finalize one table's accumulated state into neutral evidence.
fn finish_table(table: TableProfile, relationships: Vec<RelationshipEvidence>) -> TableEvidence {
    let columns = table
        .columns
        .into_iter()
        .map(finish_column)
        .collect::<Vec<_>>();
    let confidence = if table.row_count > 0 { 1.0 } else { 0.0 };
    TableEvidence {
        table: table.name,
        row_count: Some(table.row_count),
        columns,
        relationships,
        confidence,
    }
}

fn finish_column(acc: ColumnAccum) -> ColumnEvidence {
    match acc.sketches {
        Some(sketches) => {
            let mut ev = sketches.finish();
            ev.name = acc.name;
            ev
        }
        None => {
            // Schema depth: exact counts, no value-derived evidence.
            let null_rate = if acc.total == 0 {
                0.0
            } else {
                acc.nulls as f64 / acc.total as f64
            };
            // Ad-hoc confidence: rises toward 1.0 with the non-null observation
            // count (mirrors `ColumnSketches::confidence`). Both paths share
            // this intentionally heuristic scale.
            let non_null = acc.total.saturating_sub(acc.nulls);
            ColumnEvidence {
                name: acc.name,
                total_count: acc.total,
                null_count: acc.nulls,
                null_rate,
                distinct_estimate: 0.0,
                sample_values: Vec::new(),
                truncated_sample_count: 0,
                boolean: None,
                numeric: None,
                decimal_scale: None,
                string_shape: None,
                top_k: Vec::new(),
                timestamp_range: None,
                json_valid_rate: None,
                confidence: 1.0 - 1.0 / (1.0 + non_null as f64),
            }
        }
    }
}

impl PendingTable {
    fn new() -> Self {
        Self {
            row_count: 0,
            retained: Vec::new(),
            overflowed: false,
            header: None,
            copy_columns: None,
        }
    }

    fn push_insert(&mut self, row: &[u8], cap: usize) {
        self.row_count += 1;
        if self.retained.len() < cap {
            self.retained.push(PendingRow::Insert(row.to_vec()));
        } else {
            self.overflowed = true;
        }
    }

    fn push_copy(&mut self, line: &[u8], cap: usize) {
        self.row_count += 1;
        if self.retained.len() < cap {
            self.retained.push(PendingRow::Copy(line.to_vec()));
        } else {
            self.overflowed = true;
        }
    }
}
