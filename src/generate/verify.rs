//! Post-generation verification: reparse protected output and audit it before
//! publication.
//!
//! `--verify` renders SQL to a protected temporary file, then this module
//! **reparses that file with the production parser** ([`Parser::visit_events`],
//! the same visitor the profiler drives) and audits the generated rows against
//! the declared [`GenerationPlan`]. Only if the full [`VerificationReport`]
//! passes does the caller atomically publish the temp file over the real
//! destination; a failed audit leaves the prior destination untouched.
//!
//! # What is checked, and how honestly
//!
//! Every reported check carries a [`CheckStatus`]:
//!
//! * [`CheckStatus::Exact`] — a machine-checkable invariant evaluated on every
//!   row: row counts, arity, non-null, primary-key/unique uniqueness,
//!   foreign-key and composite-key membership, planner equations/state
//!   invariants ([`PlannerPredicate`]s from the compiled planners), and
//!   cross-table family sums ([`FamilySumCheck`]s).
//! * [`CheckStatus::Sampled`] — an approximate distribution comparison against a
//!   recorded tolerance. A sampled check is **never** relabeled exact.
//! * [`CheckStatus::NotChecked`] — a capability the verifier could not evaluate
//!   (e.g. a table whose DDL never appeared). It is reported as its own status
//!   and never counted as a silent success.
//!
//! [`VerificationReport::passed`] is `true` only when no check failed; a
//! `NotChecked` capability is surfaced but does not, on its own, fail the run
//! unless it means a required exact check could not run.
//!
//! # Bounded membership
//!
//! Uniqueness and membership indexes hold exact keys in memory under a byte
//! budget and spill their key bytes to a [`ProtectedSpool`] once the budget is
//! exceeded. Spilled indexes external-sort bounded runs, then use adjacent-key
//! and linear merge-join checks without retaining an entry per key in memory.
//! Dense integer key domains stay in memory.

use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use smallvec::SmallVec;

use crate::parser::mysql_insert::{
    hash_pk_tuple, parse_insert_tuple, visit_insert_rows_with, InsertRowContext, PkTuple, PkValue,
    RowExtraction,
};
use crate::parser::postgres_copy::{parse_copy_columns, CopyParser};
use crate::parser::{Parser, ParserEvent, RowFlow, SqlDialect, StatementType};
use crate::schema::{Schema, SchemaBuilder, TableSchema};

use super::output::{ProtectedSpool, SpoolReader, SpooledRow, TempConfig};
use super::plan::{ColumnOwner, CompiledRelationship, GenerationPlan, PlannedTable};
use super::registry::{PlannerPredicate, PredicateGuard};
use super::value::{GenerateError, GeneratedValue};

/// Nanoseconds per second; timestamps render at second precision, so equations
/// are checked modulo one second.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// The default in-memory byte budget for one membership/uniqueness index before
/// it spills its key bytes to a protected spool.
const DEFAULT_MEMBERSHIP_BUDGET: usize = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Report types
// ---------------------------------------------------------------------------

/// How thoroughly a single check was evaluated. A check is never reported under
/// a status stronger than the evidence supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckStatus {
    /// Evaluated exactly on every row.
    Exact,
    /// Compared approximately against a recorded tolerance.
    Sampled,
    /// The verifier could not evaluate this capability.
    NotChecked,
}

/// One audited invariant and its outcome. `name` is a stable slug (e.g.
/// `primary_key:orders`, `planner_equation:sessions`) so callers and tests can
/// locate a specific check; `detail` never contains source or generated values.
#[derive(Debug, Clone)]
pub struct CheckOutcome {
    /// Stable, greppable check name.
    pub name: String,
    /// How the check was evaluated.
    pub status: CheckStatus,
    /// Whether the check passed. A [`CheckStatus::NotChecked`] check is neither a
    /// pass nor a failure of the invariant; `passed` is `false` for it so it can
    /// never be mistaken for a green exact check.
    pub passed: bool,
    /// A value-free description of the outcome.
    pub detail: String,
}

/// The full audit of a generated SQL file.
#[derive(Debug, Clone, Default)]
pub struct VerificationReport {
    /// Every check performed, in table/kind order.
    pub checks: Vec<CheckOutcome>,
}

impl VerificationReport {
    /// Whether every evaluated check passed. A `NotChecked` capability does not
    /// by itself fail the report; only an actual failed check does.
    pub fn passed(&self) -> bool {
        self.checks
            .iter()
            .all(|check| check.passed || check.status == CheckStatus::NotChecked)
    }

    /// The checks that failed, most useful for diagnostics.
    pub fn failures(&self) -> impl Iterator<Item = &CheckOutcome> {
        self.checks
            .iter()
            .filter(|check| !check.passed && check.status != CheckStatus::NotChecked)
    }

    /// Whether a check whose name contains `needle` failed. Convenience for
    /// tests asserting a specific named exact check caught a corruption.
    pub fn failed(&self, needle: &str) -> bool {
        self.failures().any(|check| check.name.contains(needle))
    }

    /// The status of the first check whose name contains `needle`, if any.
    pub fn status_of(&self, needle: &str) -> Option<CheckStatus> {
        self.checks
            .iter()
            .find(|check| check.name.contains(needle))
            .map(|check| check.status)
    }

    fn record(&mut self, name: String, status: CheckStatus, passed: bool, detail: String) {
        self.checks.push(CheckOutcome {
            name,
            status,
            passed,
            detail,
        });
    }
}

/// An approximate categorical-distribution expectation the verifier compares a
/// generated column against, labeled [`CheckStatus::Sampled`]. Fractions are of
/// the table's total rows; `tolerance` is the maximum absolute deviation any
/// category may drift before the sampled check fails.
#[derive(Debug, Clone)]
pub struct DistributionExpectation {
    /// The `table.column` the distribution is measured on.
    pub table: String,
    /// The column within `table`.
    pub column: String,
    /// Expected `(value, fraction)` pairs; fractions should sum to about 1.0.
    pub categories: Vec<(String, f64)>,
    /// Maximum absolute per-category deviation before the check fails.
    pub tolerance: f64,
}

// ---------------------------------------------------------------------------
// The verifier
// ---------------------------------------------------------------------------

/// Reparses a generated SQL file and audits it against a [`GenerationPlan`].
///
/// The verifier extracts everything it needs from the plan up front into an
/// owned [`PlanSpec`], so it does not borrow the plan — the caller can move the
/// plan into the [`crate::generate::GenerationEngine`] to render, then verify
/// the rendered file against the already-captured spec.
pub struct GenerationVerifier {
    spec: PlanSpec,
    dialect: SqlDialect,
    membership_budget: usize,
    temp: TempConfig,
    distributions: Vec<DistributionExpectation>,
}

impl GenerationVerifier {
    /// A verifier for `plan`, defaulting to the plan's output dialect (or MySQL
    /// when none is pinned).
    pub fn new(plan: &GenerationPlan) -> Self {
        let dialect = plan
            .output
            .dialect
            .or(plan.input_dialect)
            .unwrap_or(SqlDialect::MySql);
        Self {
            spec: PlanSpec::from_plan(plan),
            dialect,
            membership_budget: DEFAULT_MEMBERSHIP_BUDGET,
            temp: TempConfig::default(),
            distributions: Vec::new(),
        }
    }

    /// Override the dialect the generated SQL is parsed with.
    pub fn dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Set the in-memory byte budget for each membership/uniqueness index before
    /// it spills to a protected spool. A small budget exercises the spill path.
    pub fn membership_budget_bytes(mut self, bytes: usize) -> Self {
        self.membership_budget = bytes;
        self
    }

    /// Store protected verification spools in `dir` instead of the OS temp
    /// directory. The directory must already exist.
    pub fn temp_directory(mut self, dir: impl Into<PathBuf>) -> Self {
        self.temp = TempConfig::in_dir(dir);
        self
    }

    /// Add an approximate distribution expectation, checked as
    /// [`CheckStatus::Sampled`].
    pub fn expect_distribution(mut self, expectation: DistributionExpectation) -> Self {
        self.distributions.push(expectation);
        self
    }

    /// Reparse and audit `path`, returning the full report. Parses the file
    /// twice: once to reconstruct the schema from the emitted DDL, once to
    /// audit rows against the plan.
    pub fn verify_path(&self, path: &Path) -> Result<VerificationReport, GenerateError> {
        let schema = self.build_schema(path)?;
        self.audit_rows(path, &schema)
    }

    /// Pass 1: reconstruct the reparsed schema from the file's DDL.
    fn build_schema(&self, path: &Path) -> Result<Schema, GenerateError> {
        let mut builder = SchemaBuilder::new();
        let reader = open_reader(path)?;
        let mut parser = Parser::with_dialect(reader, 1 << 20, self.dialect);
        parser
            .visit_events(|event| {
                if let ParserEvent::Statement(bytes) = event {
                    builder.ingest_statement(bytes, self.dialect);
                }
                Ok(RowFlow::Continue)
            })
            .map_err(parse_error)?;
        Ok(builder.build())
    }

    /// Pass 2: stream rows and evaluate every exact + sampled check.
    fn audit_rows(
        &self,
        path: &Path,
        schema: &Schema,
    ) -> Result<VerificationReport, GenerateError> {
        let mut report = VerificationReport::default();
        let mut audit = Audit::new(
            &self.spec,
            schema,
            self.membership_budget,
            self.temp.clone(),
        );
        audit.register_distributions(&self.distributions);
        audit.check_expected_tables(&mut report);

        let reader = open_reader(path)?;
        let mut parser = Parser::with_dialect(reader, 1 << 20, self.dialect);
        let dialect = self.dialect;
        // The parser borrows the current INSERT/COPY context across rows of one
        // statement; rebuild it whenever a new statement starts. COPY output is
        // audited through the *same* `observe_row` path as INSERT tuples, so a
        // Postgres COPY table gets exactly the same exact checks.
        let mut context: Option<InsertState> = None;
        let mut copy: Option<CopyState<'_>> = None;
        let audit_ref = &mut audit;
        parser
            .visit_events(|event| {
                match event {
                    ParserEvent::InsertRow {
                        header,
                        row,
                        first_in_statement,
                    } => {
                        if first_in_statement {
                            context = InsertState::from_header(header, schema, dialect);
                        }
                        if let Some(state) = &context {
                            if let Some(table) = schema.table(state.table_id_index) {
                                if let Some(parsed) = parse_insert_tuple(
                                    row,
                                    table,
                                    &state.context,
                                    dialect,
                                    RowExtraction::Full,
                                ) {
                                    audit_ref.observe_row(
                                        &state.table_name,
                                        table,
                                        &parsed.all_values,
                                        &parsed.column_map,
                                    )?;
                                } else {
                                    audit_ref.note_undecodable(&state.table_name);
                                }
                            }
                        }
                    }
                    ParserEvent::Statement(bytes) => {
                        // Some dialects (notably MSSQL, whose INSERTs are
                        // separated by `GO` batch markers) are delivered as a
                        // whole INSERT statement rather than streamed one tuple
                        // at a time. Audit those rows through the SAME path by
                        // parsing the statement's tuples here.
                        let (stmt_type, name) =
                            Parser::<&[u8]>::parse_statement_with_dialect(bytes, dialect);
                        if stmt_type == StatementType::Insert && !name.is_empty() {
                            if let Some(table_id) = schema.get_table_id(&name) {
                                if let Some(table) = schema.table(table_id) {
                                    visit_insert_rows_with(
                                        bytes,
                                        table,
                                        dialect,
                                        RowExtraction::Full,
                                        |parsed| {
                                            if parsed.all_values.is_empty() {
                                                audit_ref.note_undecodable(&name);
                                            } else {
                                                audit_ref.observe_row(
                                                    &name,
                                                    table,
                                                    &parsed.all_values,
                                                    &parsed.column_map,
                                                )?;
                                            }
                                            Ok(RowFlow::Continue)
                                        },
                                    )?;
                                }
                            }
                        }
                    }
                    ParserEvent::CopyStart(header) => {
                        copy = CopyState::from_header(header, schema, dialect);
                    }
                    ParserEvent::CopyRow(line) => {
                        if let Some(state) = &copy {
                            if let Some(parsed) =
                                state.parser.parse_line(line, state.empty_line_is_row)
                            {
                                if parsed.all_values.is_empty() {
                                    audit_ref.note_undecodable(&state.table_name);
                                } else {
                                    audit_ref.observe_row(
                                        &state.table_name,
                                        state.table,
                                        &parsed.all_values,
                                        &parsed.column_map,
                                    )?;
                                }
                            }
                        }
                    }
                    ParserEvent::CopyEnd => copy = None,
                }
                Ok(RowFlow::Continue)
            })
            .map_err(parse_error)?;

        audit.finish(&mut report, &self.distributions)?;
        Ok(report)
    }
}

/// The reparsed schema table id plus its per-statement parse context.
struct InsertState {
    table_name: String,
    table_id_index: crate::schema::TableId,
    context: InsertRowContext,
}

impl InsertState {
    fn from_header(header: &[u8], schema: &Schema, dialect: SqlDialect) -> Option<Self> {
        let (_, name) = Parser::<&[u8]>::parse_statement_with_dialect(header, dialect);
        if name.is_empty() {
            return None;
        }
        let table_id = schema.get_table_id(&name)?;
        let table = schema.table(table_id)?;
        Some(Self {
            table_name: name,
            table_id_index: table_id,
            context: InsertRowContext::from_header(header, table),
        })
    }
}

/// Per-`COPY`-block parse context: the reparsed table schema plus a prepared
/// [`CopyParser`] that decodes each `CopyRow` line the same way the profiler
/// does, so COPY rows feed the identical [`Audit::observe_row`] path as INSERT
/// tuples.
struct CopyState<'s> {
    table_name: String,
    table: &'s TableSchema,
    parser: CopyParser<'s>,
    empty_line_is_row: bool,
}

impl<'s> CopyState<'s> {
    fn from_header(header: &[u8], schema: &'s Schema, dialect: SqlDialect) -> Option<Self> {
        let (_, name) = Parser::<&[u8]>::parse_statement_with_dialect(header, dialect);
        if name.is_empty() {
            return None;
        }
        let table_id = schema.get_table_id(&name)?;
        let table = schema.table(table_id)?;
        let columns = parse_copy_columns(&String::from_utf8_lossy(header));
        let (parser, empty_line_is_row) = CopyParser::new(&[])
            .with_schema(table)
            .with_column_order(columns)
            .with_extraction(RowExtraction::Full)
            .prepared();
        Some(Self {
            table_name: name,
            table,
            parser,
            empty_line_is_row,
        })
    }
}

// ---------------------------------------------------------------------------
// Owned plan spec (extracted so the verifier does not borrow the plan)
// ---------------------------------------------------------------------------

/// Everything the audit needs from the compiled plan, owned so the plan itself
/// can be moved into the engine before verification runs.
struct PlanSpec {
    tables: Vec<TableSpec>,
    families: Vec<FamilySpec>,
}

/// One table's declared surface for verification.
struct TableSpec {
    name: String,
    rows: u64,
    /// Columns the compiled engine must render on every row. Database-produced
    /// columns are excluded unless a relationship requires the engine to
    /// materialize that parent key.
    rendered_columns: Vec<String>,
    /// Non-nullable columns whose value must never be `NULL` (database-produced
    /// columns excluded, since they legitimately render nothing).
    non_null_columns: Vec<String>,
    /// The primary key columns (empty when the table has no PK).
    primary_key: Vec<String>,
    /// Uniqueness key groups: the PK plus every UNIQUE constraint / column.
    unique_groups: Vec<Vec<String>>,
    /// Foreign-key relationships to parent tables.
    relationships: Vec<RelSpec>,
    /// Planner invariants the table's planners guarantee.
    predicates: Vec<PlannerPredicate>,
}

/// A child→parent relationship reduced to the names the audit needs.
struct RelSpec {
    name: Option<String>,
    columns: Vec<String>,
    parent_table: String,
    parent_columns: Vec<String>,
}

/// A resolved cross-table family sum invariant.
struct FamilySpec {
    slug: String,
    parent_table: String,
    parent_column: String,
    child_table: String,
    child_column: String,
    child_fk_columns: Vec<String>,
    parent_key_columns: Vec<String>,
}

impl PlanSpec {
    fn from_plan(plan: &GenerationPlan) -> Self {
        let materialized_parent_keys: HashSet<(String, String)> = plan
            .tables
            .iter()
            .flat_map(|table| {
                table.relationships.iter().flat_map(|relationship| {
                    relationship
                        .parent_columns
                        .iter()
                        .map(|column| (relationship.parent_table.clone(), column.clone()))
                })
            })
            .collect();
        let tables = plan
            .tables
            .iter()
            .map(|table| TableSpec::from_table(table, &materialized_parent_keys))
            .collect();
        let families = extract_families(plan);
        Self { tables, families }
    }
}

impl TableSpec {
    fn from_table(
        table: &PlannedTable,
        materialized_parent_keys: &HashSet<(String, String)>,
    ) -> Self {
        let mut unique_groups: Vec<Vec<String>> = Vec::new();
        if !table.schema.primary_key.is_empty() {
            unique_groups.push(table.schema.primary_key.clone());
        }
        for uc in &table.schema.unique_constraints {
            if !uc.columns.is_empty() && !unique_groups.contains(&uc.columns) {
                unique_groups.push(uc.columns.clone());
            }
        }
        for col in &table.schema.columns {
            let single = vec![col.name.clone()];
            if col.unique && !unique_groups.contains(&single) {
                unique_groups.push(single);
            }
        }

        let rendered_columns: Vec<String> = table
            .columns
            .iter()
            .filter(|column| rendered_by_engine(table, column, materialized_parent_keys))
            .map(|column| column.schema.name.clone())
            .collect();
        let non_null_columns = table
            .schema
            .columns
            .iter()
            .filter(|column| !column.nullable && rendered_columns.contains(&column.name))
            .map(|column| column.name.clone())
            .collect();

        let relationships = table
            .relationships
            .iter()
            .map(|rel| RelSpec {
                name: rel.name.clone(),
                columns: rel.columns.clone(),
                parent_table: rel.parent_table.clone(),
                parent_columns: rel.parent_columns.clone(),
            })
            .collect();

        let predicates = table
            .planners
            .iter()
            .flat_map(|planner| planner.verification_predicates())
            .collect();

        Self {
            name: table.name.clone(),
            rows: table.rows,
            rendered_columns,
            non_null_columns,
            primary_key: table.schema.primary_key.clone(),
            unique_groups,
            relationships,
            predicates,
        }
    }
}

/// Whether the compiled engine renders `column` on every row.
fn rendered_by_engine(
    table: &PlannedTable,
    column: &super::plan::PlannedColumn,
    materialized_parent_keys: &HashSet<(String, String)>,
) -> bool {
    match &column.owner {
        ColumnOwner::Generator { kind, .. } => kind != "database_default",
        ColumnOwner::Planner { .. }
        | ColumnOwner::FamilyChild { .. }
        | ColumnOwner::Relationship { .. } => true,
        ColumnOwner::DatabaseDefault | ColumnOwner::GeneratedByDatabase => {
            materialized_parent_keys.contains(&(table.name.clone(), column.schema.name.clone()))
        }
    }
}

/// Resolve every family planner's sum checks into fully-named [`FamilySpec`]s.
fn extract_families(plan: &GenerationPlan) -> Vec<FamilySpec> {
    let mut families = Vec::new();
    for parent in &plan.tables {
        for planner in &parent.planners {
            let Some(child_table) = planner.family_child_table() else {
                continue;
            };
            let checks = planner.family_sum_checks();
            if checks.is_empty() {
                continue;
            }
            let Some(child) = plan.table(child_table) else {
                continue;
            };
            for check in checks {
                let Some(rel) = find_relationship(child, &check.relationship) else {
                    continue;
                };
                families.push(FamilySpec {
                    slug: format!(
                        "family_sum:{}:{}->{}",
                        parent.name, check.parent_column, check.child_column
                    ),
                    parent_table: parent.name.clone(),
                    parent_column: check.parent_column,
                    child_table: child_table.to_string(),
                    child_column: check.child_column,
                    child_fk_columns: rel.columns.clone(),
                    parent_key_columns: rel.parent_columns.clone(),
                });
            }
        }
    }
    families
}

// ---------------------------------------------------------------------------
// Row-by-row audit state
// ---------------------------------------------------------------------------

/// A membership/uniqueness key group indexed while auditing a table.
struct GroupKey {
    /// The columns (by name) forming the key.
    columns: Vec<String>,
    /// The exact/spilling index of observed key tuples.
    index: KeySet,
    /// Whether any duplicate was observed (uniqueness groups only).
    duplicate: bool,
}

/// Per-table audit accumulators.
struct TableState {
    /// The plan's declared table.
    plan_index: usize,
    /// Observed row count.
    rows: u64,
    /// Rows whose values could not be decoded (a renderability failure).
    undecodable: u64,
    /// Non-null violations per column name.
    null_violations: HashMap<String, u64>,
    /// Arity violations (a row that differs from the plan-derived rendered
    /// column set).
    arity_violations: u64,
    /// Uniqueness key groups (PK + unique constraints).
    unique_groups: Vec<GroupKey>,
    /// Membership indexes this table exposes for children (parent-column groups).
    member_groups: Vec<GroupKey>,
    /// Child FK occurrences, one append-only index per declared relationship.
    foreign_keys: Vec<KeySet>,
    /// Planner predicate failures, keyed by a stable check slug.
    predicate_failures: HashMap<String, u64>,
    /// Rows a planner predicate could not evaluate because a required input was
    /// present but unparseable, keyed by the same slug. A non-zero count
    /// downgrades the check to `NotChecked` rather than a silent Exact pass.
    predicate_unevaluated: HashMap<String, u64>,
    /// Sampled category tallies, bounded to categories named by expectations.
    categories: CategoryTallies,
}

/// Expected categorical values and their observed counts. Values absent from
/// `expected` are deliberately never retained, so high-cardinality generated
/// text cannot turn a sampled check into an unbounded map.
#[derive(Default)]
struct CategoryTallies {
    expected: HashMap<String, HashSet<String>>,
    counts: HashMap<String, HashMap<String, u64>>,
}

impl CategoryTallies {
    fn register(&mut self, expectation: &DistributionExpectation) {
        self.expected
            .entry(expectation.column.clone())
            .or_default()
            .extend(
                expectation
                    .categories
                    .iter()
                    .map(|(value, _)| value.clone()),
            );
    }

    fn record(&mut self, column: &str, value: &str) {
        if !self
            .expected
            .get(column)
            .is_some_and(|values| values.contains(value))
        {
            return;
        }
        *self
            .counts
            .entry(column.to_string())
            .or_default()
            .entry(value.to_string())
            .or_insert(0) += 1;
    }
}

/// A single family-sum accumulator across a parent/child pair.
struct FamilyAcc {
    /// A stable slug for the check.
    slug: String,
    /// Parent table name.
    parent_table: String,
    /// Parent aggregate column.
    parent_column: String,
    /// Child table name.
    child_table: String,
    /// Child summed column.
    child_column: String,
    /// Child FK columns pointing back to the parent's referenced columns.
    child_fk_columns: Vec<String>,
    /// Parent referenced columns forming the join key.
    parent_key_columns: Vec<String>,
    /// Parent aggregate records in protected, disk-backed exact storage.
    parent_values: FamilyValueStore,
    /// Individual child values in protected, disk-backed exact storage.
    child_values: FamilyValueStore,
    /// Whether any decode/scale problem made the check inexact.
    inexact: bool,
    /// Exact record reads/writes performed by external sorting and merge.
    operations: u64,
}

/// A family aggregate contribution stored without a per-parent memory entry.
struct FamilyValueRecord {
    hash: u64,
    key: Vec<u8>,
    minor: (i128, u32),
}

/// One exact-key record handled by the bounded external sorter. `payload` is
/// empty for membership keys and carries `(mantissa, scale)` for family sums.
#[derive(Clone)]
struct SortRecord {
    hash: u64,
    key: Vec<u8>,
    payload: Vec<GeneratedValue>,
}

impl SortRecord {
    fn from_spooled(row: SpooledRow) -> io::Result<Self> {
        let mut values = row.values.into_iter();
        let Some(GeneratedValue::Bytes(key)) = values.next() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sorted spool record has an invalid key",
            ));
        };
        Ok(Self {
            hash: row.row_index,
            key,
            payload: values.collect(),
        })
    }

    fn encoded_size(&self) -> usize {
        self.key.len() + self.payload.len() * std::mem::size_of::<GeneratedValue>() + 32
    }
}

fn sort_record_cmp(left: &SortRecord, right: &SortRecord) -> Ordering {
    left.hash
        .cmp(&right.hash)
        .then_with(|| left.key.cmp(&right.key))
}

fn exact_key_cmp(left_hash: u64, left: &[u8], right_hash: u64, right: &[u8]) -> Ordering {
    left_hash.cmp(&right_hash).then_with(|| left.cmp(right))
}

#[derive(Default)]
struct SortStats {
    operations: u64,
}

fn read_sort_record<R: io::Read>(
    reader: &mut SpoolReader<R>,
    stats: &mut SortStats,
) -> io::Result<Option<SortRecord>> {
    let Some(row) = reader.read_row()? else {
        return Ok(None);
    };
    stats.operations += 1;
    SortRecord::from_spooled(row).map(Some)
}

fn write_sort_record(
    spool: &mut ProtectedSpool,
    record: SortRecord,
    stats: &mut SortStats,
) -> io::Result<()> {
    let mut values = Vec::with_capacity(record.payload.len() + 1);
    values.push(GeneratedValue::Bytes(record.key));
    values.extend(record.payload);
    spool.write_row(&SpooledRow {
        table_id: 0,
        row_index: record.hash,
        values,
    })?;
    stats.operations += 1;
    Ok(())
}

fn write_sorted_chunk(
    mut chunk: Vec<SortRecord>,
    temp: &TempConfig,
    stats: &mut SortStats,
) -> io::Result<ProtectedSpool> {
    chunk.sort_unstable_by(sort_record_cmp);
    let mut run = ProtectedSpool::create(temp)?;
    for record in chunk {
        write_sort_record(&mut run, record, stats)?;
    }
    run.flush()?;
    Ok(run)
}

fn merge_sorted_runs(
    mut left: ProtectedSpool,
    mut right: ProtectedSpool,
    temp: &TempConfig,
    stats: &mut SortStats,
) -> io::Result<ProtectedSpool> {
    left.flush()?;
    right.flush()?;
    let mut left_reader = SpoolReader::new(BufReader::new(std::fs::File::open(left.path())?));
    let mut right_reader = SpoolReader::new(BufReader::new(std::fs::File::open(right.path())?));
    let mut left_record = read_sort_record(&mut left_reader, stats)?;
    let mut right_record = read_sort_record(&mut right_reader, stats)?;
    let mut merged = ProtectedSpool::create(temp)?;

    while left_record.is_some() || right_record.is_some() {
        let take_left = match (&left_record, &right_record) {
            (Some(left), Some(right)) => sort_record_cmp(left, right) != Ordering::Greater,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_left {
            if let Some(record) = left_record.take() {
                write_sort_record(&mut merged, record, stats)?;
            }
            left_record = read_sort_record(&mut left_reader, stats)?;
        } else {
            if let Some(record) = right_record.take() {
                write_sort_record(&mut merged, record, stats)?;
            }
            right_record = read_sort_record(&mut right_reader, stats)?;
        }
    }
    merged.flush()?;
    Ok(merged)
}

/// Sort an append-only protected spool with bounded record memory. Runs are
/// merged eagerly like a binary counter, so at most one run per size level is
/// retained and every record participates in `O(log n)` sequential disk I/O.
fn external_sort(
    mut source: ProtectedSpool,
    temp: &TempConfig,
    budget: usize,
    stats: &mut SortStats,
) -> io::Result<Option<ProtectedSpool>> {
    source.flush()?;
    let mut reader = SpoolReader::new(BufReader::new(std::fs::File::open(source.path())?));
    let mut levels: Vec<Option<ProtectedSpool>> = Vec::new();
    let mut chunk = Vec::new();
    let mut used = 0usize;

    while let Some(record) = read_sort_record(&mut reader, stats)? {
        let size = record.encoded_size();
        if !chunk.is_empty() && used.saturating_add(size) > budget {
            let run = write_sorted_chunk(std::mem::take(&mut chunk), temp, stats)?;
            add_sorted_run(&mut levels, run, temp, stats)?;
            used = 0;
        }
        used = used.saturating_add(size);
        chunk.push(record);
    }
    if !chunk.is_empty() {
        let run = write_sorted_chunk(chunk, temp, stats)?;
        add_sorted_run(&mut levels, run, temp, stats)?;
    }

    let mut result = None;
    for run in levels.into_iter().flatten() {
        result = Some(match result {
            Some(existing) => merge_sorted_runs(existing, run, temp, stats)?,
            None => run,
        });
    }
    Ok(result)
}

fn add_sorted_run(
    levels: &mut Vec<Option<ProtectedSpool>>,
    mut run: ProtectedSpool,
    temp: &TempConfig,
    stats: &mut SortStats,
) -> io::Result<()> {
    let mut level = 0usize;
    loop {
        if level == levels.len() {
            levels.push(Some(run));
            return Ok(());
        }
        match levels[level].take() {
            Some(existing) => {
                run = merge_sorted_runs(existing, run, temp, stats)?;
                level += 1;
            }
            None => {
                levels[level] = Some(run);
                return Ok(());
            }
        }
    }
}

/// Protected disk-backed family records. Appends are externally sorted once;
/// final comparison can then merge parents and children in one linear pass.
struct FamilyValueStore {
    temp: TempConfig,
    budget: usize,
    spool: Option<ProtectedSpool>,
    sorted: Option<ProtectedSpool>,
    operations: u64,
}

impl FamilyValueStore {
    fn new(temp: TempConfig, budget: usize) -> Self {
        Self {
            temp,
            budget,
            spool: None,
            sorted: None,
            operations: 0,
        }
    }

    fn append(&mut self, key: &[u8], hash: u64, minor: (i128, u32)) -> io::Result<()> {
        if self.sorted.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot append to a finalized family index",
            ));
        }
        let spool = match &mut self.spool {
            Some(spool) => spool,
            None => self.spool.insert(ProtectedSpool::create(&self.temp)?),
        };
        spool.write_row(&SpooledRow {
            table_id: 0,
            row_index: hash,
            values: vec![
                GeneratedValue::Bytes(key.to_vec()),
                GeneratedValue::Integer(minor.0),
                GeneratedValue::Integer(i128::from(minor.1)),
            ],
        })
    }

    fn finalize(&mut self) -> io::Result<()> {
        if self.sorted.is_some() || self.spool.is_none() {
            return Ok(());
        }
        let mut stats = SortStats::default();
        let Some(spool) = self.spool.take() else {
            return Ok(());
        };
        self.sorted = external_sort(spool, &self.temp, self.budget, &mut stats)?;
        self.operations += stats.operations;
        Ok(())
    }

    fn replay(&self) -> io::Result<FamilyValueReplay> {
        let reader = match &self.sorted {
            Some(spool) => Some(SpoolReader::new(BufReader::new(std::fs::File::open(
                spool.path(),
            )?))),
            None => None,
        };
        Ok(FamilyValueReplay { reader })
    }
}

struct FamilyValueReplay {
    reader: Option<SpoolReader<BufReader<std::fs::File>>>,
}

impl Iterator for FamilyValueReplay {
    type Item = io::Result<FamilyValueRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        match reader.read_row() {
            Ok(Some(row)) => Some(decode_family_value(row)),
            Ok(None) => {
                self.reader = None;
                None
            }
            Err(error) => Some(Err(error)),
        }
    }
}

fn decode_family_value(row: SpooledRow) -> io::Result<FamilyValueRecord> {
    let [GeneratedValue::Bytes(key), GeneratedValue::Integer(mantissa), GeneratedValue::Integer(scale)] =
        row.values.as_slice()
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "family spool record has an invalid shape",
        ));
    };
    let scale = u32::try_from(*scale).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "family spool record has an invalid scale",
        )
    })?;
    Ok(FamilyValueRecord {
        hash: row.row_index,
        key: key.clone(),
        minor: (*mantissa, scale),
    })
}

impl FamilyAcc {
    fn compare(&mut self) -> io::Result<(u64, bool)> {
        self.parent_values.finalize()?;
        self.child_values.finalize()?;

        let mut failed = 0u64;
        let mut missing = false;
        let mut operations = 0u64;
        let mut children = self.child_values.replay()?;
        let mut child = next_family_record(&mut children, &mut operations)?;
        let mut parents = self.parent_values.replay()?;
        let mut parent = next_family_record(&mut parents, &mut operations)?;
        while let Some(first_parent) = parent.take() {
            while child.as_ref().is_some_and(|child| {
                exact_key_cmp(child.hash, &child.key, first_parent.hash, &first_parent.key)
                    == Ordering::Less
            }) {
                child = next_family_record(&mut children, &mut operations)?;
            }

            let mut sum = None;
            while child.as_ref().is_some_and(|child| {
                exact_key_cmp(child.hash, &child.key, first_parent.hash, &first_parent.key)
                    == Ordering::Equal
            }) {
                let Some(contribution) = child.take().map(|child| child.minor) else {
                    break;
                };
                match &mut sum {
                    Some(acc) => add_minor(acc, contribution),
                    None => sum = Some(contribution),
                }
                child = next_family_record(&mut children, &mut operations)?;
            }

            let mut current_parent = first_parent;
            loop {
                match sum {
                    Some(child) if !minor_eq(current_parent.minor, child) => failed += 1,
                    None if current_parent.minor.0 != 0 => {
                        failed += 1;
                        missing = true;
                    }
                    _ => {}
                }

                parent = next_family_record(&mut parents, &mut operations)?;
                let same_key = parent.as_ref().is_some_and(|next| {
                    exact_key_cmp(
                        next.hash,
                        &next.key,
                        current_parent.hash,
                        &current_parent.key,
                    ) == Ordering::Equal
                });
                if !same_key {
                    break;
                }
                let Some(next_parent) = parent.take() else {
                    break;
                };
                current_parent = next_parent;
            }
        }
        self.operations = self.parent_values.operations + self.child_values.operations + operations;
        Ok((failed, missing))
    }
}

fn next_family_record(
    replay: &mut FamilyValueReplay,
    operations: &mut u64,
) -> io::Result<Option<FamilyValueRecord>> {
    let Some(record) = replay.next() else {
        return Ok(None);
    };
    *operations += 1;
    record.map(Some)
}

/// The whole-file audit.
struct Audit<'a> {
    spec: &'a PlanSpec,
    tables: HashMap<String, TableState>,
    families: Vec<FamilyAcc>,
    /// Plan tables whose DDL never appeared in the output.
    missing_tables: Vec<String>,
    /// `table.column` pairs the plan declares but the reparsed DDL lacks.
    missing_columns: Vec<String>,
}

impl<'a> Audit<'a> {
    fn new(
        spec: &'a PlanSpec,
        schema: &Schema,
        membership_budget: usize,
        temp: TempConfig,
    ) -> Self {
        let mut tables = HashMap::new();

        // Expected tables + DDL: every plan table must appear with its columns.
        let mut missing_tables = Vec::new();
        let mut missing_columns = Vec::new();
        for table in &spec.tables {
            match schema.get_table_id(&table.name) {
                None => missing_tables.push(table.name.clone()),
                Some(id) => {
                    if let Some(reparsed) = schema.table(id) {
                        for col in &table.non_null_columns {
                            if reparsed.get_column_id(col).is_none() {
                                missing_columns.push(format!("{}.{}", table.name, col));
                            }
                        }
                    }
                }
            }
        }

        // Which parent-column groups each table must expose for FK membership.
        let mut needed_parent_groups: HashMap<String, Vec<Vec<String>>> = HashMap::new();
        for table in &spec.tables {
            for rel in &table.relationships {
                needed_parent_groups
                    .entry(rel.parent_table.clone())
                    .or_default()
                    .push(rel.parent_columns.clone());
            }
        }

        for (index, table) in spec.tables.iter().enumerate() {
            let unique_groups = table
                .unique_groups
                .iter()
                .map(|cols| GroupKey::new(cols.clone(), membership_budget, &temp))
                .collect();

            let mut member_groups: Vec<GroupKey> = Vec::new();
            if let Some(groups) = needed_parent_groups.get(&table.name) {
                for cols in groups {
                    if !member_groups.iter().any(|g| &g.columns == cols) {
                        member_groups.push(GroupKey::new(cols.clone(), membership_budget, &temp));
                    }
                }
            }
            let foreign_keys = table
                .relationships
                .iter()
                .map(|_| KeySet::new(membership_budget, temp.clone()))
                .collect();

            tables.insert(
                table.name.clone(),
                TableState {
                    plan_index: index,
                    rows: 0,
                    undecodable: 0,
                    null_violations: HashMap::new(),
                    arity_violations: 0,
                    unique_groups,
                    member_groups,
                    foreign_keys,
                    predicate_failures: HashMap::new(),
                    predicate_unevaluated: HashMap::new(),
                    categories: CategoryTallies::default(),
                },
            );
        }

        let families = spec
            .families
            .iter()
            .map(|family| FamilyAcc {
                slug: family.slug.clone(),
                parent_table: family.parent_table.clone(),
                parent_column: family.parent_column.clone(),
                child_table: family.child_table.clone(),
                child_column: family.child_column.clone(),
                child_fk_columns: family.child_fk_columns.clone(),
                parent_key_columns: family.parent_key_columns.clone(),
                parent_values: FamilyValueStore::new(temp.clone(), membership_budget),
                child_values: FamilyValueStore::new(temp.clone(), membership_budget),
                inexact: false,
                operations: 0,
            })
            .collect();

        Audit {
            spec,
            tables,
            families,
            missing_tables,
            missing_columns,
        }
    }

    /// Register the columns each sampled distribution measures so the row pass
    /// tallies their category counts.
    fn register_distributions(&mut self, distributions: &[DistributionExpectation]) {
        for expectation in distributions {
            if let Some(state) = self.tables.get_mut(&expectation.table) {
                state.categories.register(expectation);
            }
        }
    }

    fn note_undecodable(&mut self, table: &str) {
        if let Some(state) = self.tables.get_mut(table) {
            state.rows += 1;
            state.undecodable += 1;
        }
    }

    /// Observe one decoded row of `table`.
    fn observe_row(
        &mut self,
        table_name: &str,
        schema: &TableSchema,
        all_values: &[PkValue],
        column_map: &[Option<usize>],
    ) -> Result<(), GenerateError> {
        let plan_index = match self.tables.get(table_name) {
            Some(state) => state.plan_index,
            None => return Ok(()),
        };
        // Borrow the owned spec (tied to the spec's lifetime, not to `self`), so
        // the row-check reads below do not conflict with the mutable `self`
        // updates that follow.
        let spec = self.spec;
        let planned = &spec.tables[plan_index];

        // Resolve a plan column name to its value in this row.
        let value_of = |name: &str| -> Option<&PkValue> {
            let ord = schema.get_column_id(name)?;
            let val_idx = column_map.get(ord.0 as usize).copied().flatten()?;
            all_values.get(val_idx)
        };

        // Arity is fixed by compiled ownership, not learned from the first row:
        // every non-database-produced column must be present on every row.
        let observed_width = all_values.len();
        let arity_violation = observed_width != planned.rendered_columns.len()
            || planned
                .rendered_columns
                .iter()
                .any(|column| value_of(column).is_none());

        // Collect the failures first (immutable borrows of the spec), then apply
        // them to the table's mutable state.
        let mut null_hits: Vec<String> = Vec::new();
        for col in &planned.non_null_columns {
            if matches!(value_of(col), None | Some(PkValue::Null)) {
                null_hits.push(col.clone());
            }
        }

        // Predicate evaluation. A predicate that holds but whose required inputs
        // include a present-but-unparseable value could not actually be
        // evaluated for this row — record it separately so the check is not a
        // silent Exact pass.
        let mut predicate_hits: Vec<String> = Vec::new();
        let mut predicate_unevaluated_hits: Vec<String> = Vec::new();
        for predicate in &planned.predicates {
            match evaluate_predicate(predicate, table_name, &value_of) {
                Some(slug) => predicate_hits.push(slug),
                None if predicate_unevaluable(predicate, &value_of) => {
                    predicate_unevaluated_hits.push(predicate_slug(predicate, table_name));
                }
                None => {}
            }
        }

        // Uniqueness keys.
        let unique_keys: Vec<Option<(Vec<u8>, u64)>> = {
            let state = &self.tables[table_name];
            state
                .unique_groups
                .iter()
                .map(|group| encode_group(&group.columns, &value_of))
                .collect()
        };
        // Membership keys this table exposes to children.
        let member_keys: Vec<Option<(Vec<u8>, u64)>> = {
            let state = &self.tables[table_name];
            state
                .member_groups
                .iter()
                .map(|group| encode_group(&group.columns, &value_of))
                .collect()
        };

        // FK keys are appended now and checked in one sorted merge after the
        // row pass. NULL composite keys remain optional and are not indexed.
        let foreign_keys: Vec<Option<(Vec<u8>, u64)>> = planned
            .relationships
            .iter()
            .map(|rel| {
                if is_all_null(&rel.columns, &value_of) {
                    None
                } else {
                    encode_group(&rel.columns, &value_of)
                }
            })
            .collect();

        // Family accumulation.
        self.accumulate_families(table_name, &value_of)?;

        // Sampled category tallies.
        let category_hits: Vec<(String, String)> = {
            let state = &self.tables[table_name];
            state
                .categories
                .expected
                .keys()
                .filter_map(|col| {
                    value_of(col)
                        .and_then(text_of)
                        .map(|value| (col.clone(), value))
                })
                .collect()
        };

        // Now apply everything mutably.
        let state = self.tables.get_mut(table_name).expect("table state exists");
        state.rows += 1;
        if arity_violation {
            state.arity_violations += 1;
        }
        for name in null_hits {
            *state.null_violations.entry(name).or_insert(0) += 1;
        }
        for slug in predicate_hits {
            *state.predicate_failures.entry(slug).or_insert(0) += 1;
        }
        for slug in predicate_unevaluated_hits {
            *state.predicate_unevaluated.entry(slug).or_insert(0) += 1;
        }
        for (group, key) in state.unique_groups.iter_mut().zip(unique_keys) {
            if let Some((bytes, hash)) = key {
                if !group
                    .index
                    .insert(&bytes, hash)
                    .map_err(membership_index_error)?
                {
                    group.duplicate = true;
                }
            }
        }
        for (group, key) in state.member_groups.iter_mut().zip(member_keys) {
            if let Some((bytes, hash)) = key {
                group
                    .index
                    .insert(&bytes, hash)
                    .map_err(membership_index_error)?;
            }
        }
        for (index, key) in state.foreign_keys.iter_mut().zip(foreign_keys) {
            if let Some((bytes, hash)) = key {
                index.append(&bytes, hash).map_err(membership_index_error)?;
            }
        }
        for (col, value) in category_hits {
            state.categories.record(&col, &value);
        }
        Ok(())
    }

    /// Accumulate parent totals and child sums for every family this row
    /// participates in (as parent or child).
    fn accumulate_families<'v>(
        &mut self,
        table_name: &str,
        value_of: &impl Fn(&str) -> Option<&'v PkValue>,
    ) -> Result<(), GenerateError> {
        for family in &mut self.families {
            if family.parent_table == table_name {
                if let Some((key, hash)) = encode_group(&family.parent_key_columns, value_of) {
                    if let Some(minor) = value_of(&family.parent_column).and_then(money_minor) {
                        family
                            .parent_values
                            .append(&key, hash, minor)
                            .map_err(family_index_error)?;
                    } else {
                        family.inexact = true;
                    }
                }
            }
            if family.child_table == table_name {
                if let Some((key, hash)) = encode_group(&family.child_fk_columns, value_of) {
                    if let Some(minor) = value_of(&family.child_column).and_then(money_minor) {
                        family
                            .child_values
                            .append(&key, hash, minor)
                            .map_err(family_index_error)?;
                    } else {
                        family.inexact = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// Record the expected-tables / expected-DDL checks.
    fn check_expected_tables(&self, report: &mut VerificationReport) {
        report.record(
            "expected_tables".to_string(),
            CheckStatus::Exact,
            self.missing_tables.is_empty(),
            format!(
                "{} expected table(s) missing from output",
                self.missing_tables.len()
            ),
        );
        report.record(
            "expected_ddl".to_string(),
            CheckStatus::Exact,
            self.missing_columns.is_empty(),
            format!(
                "{} expected column(s) missing from the reparsed DDL",
                self.missing_columns.len()
            ),
        );
    }

    /// Finalize every append-only key index, then evaluate child FK indexes
    /// against their parent membership indexes by sorted merge join.
    /// Returns, per table and per relationship, `Some(missing)` when the parent
    /// membership index was available (so the FK was actually checked) or `None`
    /// when the parent table was not generated (so membership is unverifiable).
    fn finalize_indexes(&mut self) -> Result<Vec<Vec<Option<u64>>>, GenerateError> {
        for state in self.tables.values_mut() {
            for group in &mut state.unique_groups {
                group.duplicate |= group.index.finalize().map_err(membership_index_error)?;
            }
            for group in &mut state.member_groups {
                group.index.finalize().map_err(membership_index_error)?;
            }
            for index in &mut state.foreign_keys {
                index.finalize().map_err(membership_index_error)?;
            }
        }

        let mut failures = Vec::with_capacity(self.spec.tables.len());
        for planned in &self.spec.tables {
            let child = &self.tables[&planned.name];
            let mut relationship_failures = Vec::with_capacity(planned.relationships.len());
            for (relationship, child_keys) in planned.relationships.iter().zip(&child.foreign_keys)
            {
                let parent_keys = self
                    .tables
                    .get(&relationship.parent_table)
                    .and_then(|parent| {
                        parent
                            .member_groups
                            .iter()
                            .find(|group| group.columns == relationship.parent_columns)
                    });
                let missing = match parent_keys {
                    Some(parent_keys) => Some(
                        child_keys
                            .missing_from(&parent_keys.index)
                            .map_err(membership_index_error)?,
                    ),
                    // The parent table was not generated, so its membership index
                    // does not exist: the FK cannot be verified (fail-closed as
                    // NotChecked in `finish`, never a silent green pass).
                    None => None,
                };
                relationship_failures.push(missing);
            }
            failures.push(relationship_failures);
        }
        Ok(failures)
    }

    /// Emit every accumulated check into the report.
    fn finish(
        &mut self,
        report: &mut VerificationReport,
        distributions: &[DistributionExpectation],
    ) -> Result<(), GenerateError> {
        let foreign_key_failures = self.finalize_indexes()?;
        for (plan_index, planned) in self.spec.tables.iter().enumerate() {
            let state = &self.tables[&planned.name];

            // Row count (exact). INSERT and COPY output are audited the same way,
            // so a table that produced zero parseable rows when the plan expected
            // some is always a genuine failure — never masked as NotChecked.
            let expected = planned.rows;
            report.record(
                format!("row_count:{}", planned.name),
                CheckStatus::Exact,
                state.rows == expected,
                format!("expected {expected} rows, observed {}", state.rows),
            );

            // Renderability (undecodable rows).
            if state.undecodable > 0 {
                report.record(
                    format!("renderable:{}", planned.name),
                    CheckStatus::Exact,
                    false,
                    format!("{} row(s) failed to reparse", state.undecodable),
                );
            }

            // Arity.
            report.record(
                format!("arity:{}", planned.name),
                CheckStatus::Exact,
                state.arity_violations == 0,
                format!(
                    "{} row(s) had the wrong column count",
                    state.arity_violations
                ),
            );

            // Non-null (one check per non-nullable column that failed, plus an
            // overall pass marker).
            let null_failed: u64 = state.null_violations.values().sum();
            report.record(
                format!("non_null:{}", planned.name),
                CheckStatus::Exact,
                null_failed == 0,
                format!("{null_failed} unexpected NULL value(s) in non-nullable columns"),
            );

            // Uniqueness (PK + unique constraints).
            for group in &state.unique_groups {
                let is_pk = planned.primary_key == group.columns;
                let kind = if is_pk { "primary_key" } else { "unique" };
                let composite = if group.columns.len() > 1 {
                    "composite_"
                } else {
                    ""
                };
                report.record(
                    format!(
                        "{composite}{kind}:{}:{}",
                        planned.name,
                        group.columns.join(",")
                    ),
                    CheckStatus::Exact,
                    !group.duplicate,
                    if group.duplicate {
                        "duplicate key values found".to_string()
                    } else {
                        "all key values distinct".to_string()
                    },
                );
            }

            // Foreign keys / composite keys.
            for (relationship_index, rel) in planned.relationships.iter().enumerate() {
                let slug = fk_slug(&planned.name, rel);
                let missing = foreign_key_failures
                    .get(plan_index)
                    .and_then(|failures| failures.get(relationship_index))
                    .copied()
                    .unwrap_or(Some(0));
                let (status, passed, detail) = match missing {
                    None => (
                        CheckStatus::NotChecked,
                        false,
                        "parent table not generated; foreign-key membership not verified"
                            .to_string(),
                    ),
                    Some(failed) => (
                        CheckStatus::Exact,
                        failed == 0,
                        format!("{failed} foreign-key value(s) missing from the parent"),
                    ),
                };
                report.record(slug, status, passed, detail);
            }

            // Planner predicates: report each declared predicate so a passing
            // invariant is visible as Exact too.
            let mut declared: Vec<String> = planned
                .predicates
                .iter()
                .map(|predicate| predicate_slug(predicate, &planned.name))
                .collect();
            declared.sort();
            declared.dedup();
            for slug in declared {
                let failed = state.predicate_failures.get(&slug).copied().unwrap_or(0);
                let unevaluated = state.predicate_unevaluated.get(&slug).copied().unwrap_or(0);
                let (status, passed, detail) = if failed > 0 {
                    (
                        CheckStatus::Exact,
                        false,
                        format!("{failed} row(s) violated the planner invariant"),
                    )
                } else if unevaluated > 0 {
                    (
                        CheckStatus::NotChecked,
                        false,
                        format!("{unevaluated} row(s) had unparseable inputs; invariant not verified"),
                    )
                } else {
                    (
                        CheckStatus::Exact,
                        true,
                        "0 row(s) violated the planner invariant".to_string(),
                    )
                };
                report.record(slug, status, passed, detail);
            }
        }

        // Family sum checks. An unparseable/NULL money value makes a family
        // `inexact`: those parent groups cannot be evaluated. But `compare`
        // still evaluates every group it *can*, so a genuine disagreement
        // (`failed > 0`) must fail the check regardless — an inexact row must
        // never mask a real violation. Only when no disagreement is found does
        // `inexact` downgrade the check to `NotChecked` (couldn't fully verify).
        for family in &mut self.families {
            let (failed, missing) = family.compare().map_err(family_index_error)?;
            let (status, detail) = if failed > 0 {
                (
                    CheckStatus::Exact,
                    format!(
                        "{failed} parent aggregate(s) disagreed with the child sum{}",
                        if missing { " (missing children)" } else { "" }
                    ),
                )
            } else if family.inexact {
                (
                    CheckStatus::NotChecked,
                    "family sum not evaluated (unparseable money values)".to_string(),
                )
            } else {
                (
                    CheckStatus::Exact,
                    "0 parent aggregate(s) disagreed with the child sum".to_string(),
                )
            };
            report.record(family.slug.clone(), status, failed == 0, detail);
        }

        // Sampled distributions.
        self.finish_distributions(report, distributions);
        Ok(())
    }

    fn finish_distributions(
        &self,
        report: &mut VerificationReport,
        distributions: &[DistributionExpectation],
    ) {
        for expectation in distributions {
            let Some(state) = self.tables.get(&expectation.table) else {
                report.record(
                    format!("distribution:{}.{}", expectation.table, expectation.column),
                    CheckStatus::NotChecked,
                    false,
                    "table not present in output".to_string(),
                );
                continue;
            };
            let counts = state.categories.counts.get(&expectation.column);
            let total = state.rows.max(1) as f64;
            let mut worst = 0.0f64;
            if let Some(counts) = counts {
                for (value, expected_fraction) in &expectation.categories {
                    let observed = counts.get(value).copied().unwrap_or(0) as f64 / total;
                    worst = worst.max((observed - expected_fraction).abs());
                }
            } else {
                worst = 1.0;
            }
            report.record(
                format!("distribution:{}.{}", expectation.table, expectation.column),
                CheckStatus::Sampled,
                worst <= expectation.tolerance,
                format!(
                    "worst per-category deviation {worst:.4} vs tolerance {:.4}",
                    expectation.tolerance
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Predicate evaluation
// ---------------------------------------------------------------------------

/// Evaluate a planner predicate on one row. Returns the check slug when the row
/// **violates** the predicate, or `None` when it holds (or does not apply).
fn evaluate_predicate<'v>(
    predicate: &PlannerPredicate,
    table: &str,
    value_of: &impl Fn(&str) -> Option<&'v PkValue>,
) -> Option<String> {
    let slug = predicate_slug(predicate, table);
    let violated = match predicate {
        PlannerPredicate::Equation {
            start,
            end,
            duration,
            duration_unit_nanos,
            end_inclusive,
            guard,
        } => {
            if !guard_selects(guard.as_ref(), value_of) {
                false
            } else {
                let (Some(start_ns), Some(dur)) = (
                    value_of(start).and_then(epoch_nanos),
                    value_of(duration).and_then(int_of),
                ) else {
                    return None;
                };
                match value_of(end).and_then(epoch_nanos) {
                    Some(end_ns) => {
                        let span = dur.saturating_mul(*duration_unit_nanos);
                        let expected = start_ns.saturating_add(span) - i128::from(*end_inclusive);
                        floor_sec(expected) != floor_sec(end_ns)
                    }
                    None => true, // guard says closed, but end is null/unparsed
                }
            }
        }
        PlannerPredicate::NullWhen { column, guard } => {
            guard_selects(Some(guard), value_of)
                && !matches!(value_of(column), Some(PkValue::Null) | None)
        }
        PlannerPredicate::NotNullWhen { column, guard } => {
            guard_selects(Some(guard), value_of)
                && matches!(value_of(column), Some(PkValue::Null) | None)
        }
        PlannerPredicate::InRange {
            column,
            min_nanos,
            max_nanos,
        } => match value_of(column).and_then(epoch_nanos) {
            Some(ns) => {
                floor_sec(ns) < floor_sec(*min_nanos) || floor_sec(ns) > floor_sec(*max_nanos)
            }
            None => false,
        },
        PlannerPredicate::CounterSum {
            addends,
            sum,
            guard,
        } => {
            if !guard_selects(guard.as_ref(), value_of) {
                false
            } else {
                let mut total = 0i128;
                let mut ok = true;
                for addend in addends {
                    match value_of(addend).and_then(int_of) {
                        Some(v) => total += v,
                        None => ok = false,
                    }
                }
                match (ok, value_of(sum).and_then(int_of)) {
                    (true, Some(target)) => total != target,
                    _ => false,
                }
            }
        }
        PlannerPredicate::NonNegative { columns } => columns
            .iter()
            .any(|col| matches!(value_of(col).and_then(int_of), Some(v) if v < 0)),
        PlannerPredicate::Ordering {
            earlier,
            later,
            guard,
        } => {
            if !guard_selects(guard.as_ref(), value_of) {
                false
            } else {
                match (
                    value_of(earlier).and_then(epoch_nanos),
                    value_of(later).and_then(epoch_nanos),
                ) {
                    (Some(earlier_ns), Some(later_ns)) => {
                        floor_sec(earlier_ns) > floor_sec(later_ns)
                    }
                    _ => false,
                }
            }
        }
    };
    violated.then_some(slug)
}

/// Whether a guard-selected row has a required predicate input that is present
/// (non-null) but cannot be interpreted as the type the predicate needs. Such a
/// row could not actually be evaluated (as opposed to holding), so the check
/// must be surfaced as `NotChecked` rather than a silent Exact pass over
/// corruption. Predicates that only test presence/nullness (`NullWhen`,
/// `NotNullWhen`) and cases already treated as violations (a missing `end` in an
/// `Equation`) are never "unevaluable" here.
fn predicate_unevaluable<'v>(
    predicate: &PlannerPredicate,
    value_of: &impl Fn(&str) -> Option<&'v PkValue>,
) -> bool {
    let bad_int = |col: &str| {
        matches!(value_of(col), Some(v) if !matches!(v, PkValue::Null) && int_of(v).is_none())
    };
    let bad_ns = |col: &str| {
        matches!(value_of(col), Some(v) if !matches!(v, PkValue::Null) && epoch_nanos(v).is_none())
    };
    match predicate {
        PlannerPredicate::Equation {
            start,
            duration,
            guard,
            ..
        } => guard_selects(guard.as_ref(), value_of) && (bad_ns(start) || bad_int(duration)),
        PlannerPredicate::InRange { column, .. } => bad_ns(column),
        PlannerPredicate::CounterSum {
            addends,
            sum,
            guard,
        } => {
            guard_selects(guard.as_ref(), value_of)
                && (addends.iter().any(|a| bad_int(a)) || bad_int(sum))
        }
        PlannerPredicate::NonNegative { columns } => columns.iter().any(|c| bad_int(c)),
        PlannerPredicate::Ordering {
            earlier,
            later,
            guard,
        } => guard_selects(guard.as_ref(), value_of) && (bad_ns(earlier) || bad_ns(later)),
        PlannerPredicate::NullWhen { .. } | PlannerPredicate::NotNullWhen { .. } => false,
    }
}

/// A stable check slug for a predicate on `table`.
fn predicate_slug(predicate: &PlannerPredicate, table: &str) -> String {
    let kind = match predicate {
        PlannerPredicate::Equation { .. } => "equation",
        PlannerPredicate::NullWhen { .. } => "null_when",
        PlannerPredicate::NotNullWhen { .. } => "not_null_when",
        PlannerPredicate::InRange { .. } => "in_range",
        PlannerPredicate::CounterSum { .. } => "counter_sum",
        PlannerPredicate::NonNegative { .. } => "non_negative",
        PlannerPredicate::Ordering { .. } => "ordering",
    };
    format!("planner_{kind}:{table}")
}

/// Whether a guard selects the current row (a `None` guard selects every row).
fn guard_selects<'v>(
    guard: Option<&PredicateGuard>,
    value_of: &impl Fn(&str) -> Option<&'v PkValue>,
) -> bool {
    match guard {
        None => true,
        Some(PredicateGuard::Flag { column, value }) => {
            matches!(value_of(column).and_then(bool_of), Some(b) if b == *value)
        }
        Some(PredicateGuard::Null { column, is_null }) => {
            matches!(value_of(column), Some(PkValue::Null) | None) == *is_null
        }
        Some(PredicateGuard::Equals { column, value }) => {
            matches!(value_of(column).and_then(text_of), Some(v) if &v == value)
        }
    }
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

/// Find a child table's relationship by declared name.
fn find_relationship<'t>(table: &'t PlannedTable, name: &str) -> Option<&'t CompiledRelationship> {
    table
        .relationships
        .iter()
        .find(|rel| rel.name.as_deref() == Some(name))
}

/// Encode a tuple of a row's column values into canonical key bytes + hash, or
/// `None` if any column is missing from the row.
fn encode_group<'v>(
    columns: &[String],
    value_of: &impl Fn(&str) -> Option<&'v PkValue>,
) -> Option<(Vec<u8>, u64)> {
    let mut tuple: PkTuple = SmallVec::new();
    for col in columns {
        tuple.push(normalize(value_of(col)?));
    }
    let hash = hash_pk_tuple(&tuple);
    Some((encode_key(&tuple), hash))
}

fn is_all_null<'v>(columns: &[String], value_of: &impl Fn(&str) -> Option<&'v PkValue>) -> bool {
    columns
        .iter()
        .all(|c| matches!(value_of(c), Some(PkValue::Null) | None))
}

/// Normalize integer PkValues to `BigInt` so a parent rendered as `Int` and a
/// child parsed as `BigInt` (or vice versa) compare and hash identically.
fn normalize(value: &PkValue) -> PkValue {
    match value {
        PkValue::Int(i) => PkValue::BigInt(i128::from(*i)),
        other => other.clone(),
    }
}

/// Canonical byte encoding of a normalized key tuple, for collision confirmation.
fn encode_key(tuple: &[PkValue]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(tuple.len() as u8);
    for value in tuple {
        match value {
            PkValue::Int(i) => {
                out.push(0);
                out.extend_from_slice(&i.to_le_bytes());
            }
            PkValue::BigInt(i) => {
                out.push(1);
                out.extend_from_slice(&i.to_le_bytes());
            }
            PkValue::Text(s) => {
                out.push(2);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            PkValue::Null => out.push(3),
        }
    }
    out
}

fn int_of(value: &PkValue) -> Option<i128> {
    match value {
        PkValue::Int(i) => Some(i128::from(*i)),
        PkValue::BigInt(i) => Some(*i),
        PkValue::Text(s) => s.trim().parse::<i128>().ok(),
        PkValue::Null => None,
    }
}

fn text_of(value: &PkValue) -> Option<String> {
    match value {
        PkValue::Text(s) => Some(s.to_string()),
        PkValue::Int(i) => Some(i.to_string()),
        PkValue::BigInt(i) => Some(i.to_string()),
        PkValue::Null => None,
    }
}

fn bool_of(value: &PkValue) -> Option<bool> {
    match value {
        PkValue::Int(0) | PkValue::BigInt(0) => Some(false),
        PkValue::Int(1) | PkValue::BigInt(1) => Some(true),
        PkValue::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" => Some(true),
            "0" | "false" | "f" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Parse a rendered timestamp string into epoch nanoseconds (UTC, second
/// precision). Accepts `YYYY-MM-DD HH:MM:SS`, an ISO `T` separator, and a
/// trailing offset.
fn epoch_nanos(value: &PkValue) -> Option<i128> {
    let text = match value {
        PkValue::Text(s) => s.as_ref(),
        _ => return None,
    };
    let text = text.trim().trim_matches('\'');
    // Try an explicit offset first (…%:z), then a naive UTC timestamp.
    for format in ["%Y-%m-%d %H:%M:%S%:z", "%Y-%m-%dT%H:%M:%S%:z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, format) {
            return Some(i128::from(dt.timestamp()) * NANOS_PER_SECOND);
        }
    }
    for format in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(text, format) {
            return Some(i128::from(dt.and_utc().timestamp()) * NANOS_PER_SECOND);
        }
    }
    None
}

fn floor_sec(nanos: i128) -> i128 {
    nanos.div_euclid(NANOS_PER_SECOND) * NANOS_PER_SECOND
}

/// Parse a money value into `(mantissa, scale)` minor units: `12.34` → `(1234,
/// 2)`, integer `5` → `(5, 0)`.
fn money_minor(value: &PkValue) -> Option<(i128, u32)> {
    match value {
        PkValue::Int(i) => Some((i128::from(*i), 0)),
        PkValue::BigInt(i) => Some((*i, 0)),
        PkValue::Text(s) => parse_decimal(s.trim().trim_matches('\'')),
        PkValue::Null => None,
    }
}

fn parse_decimal(text: &str) -> Option<(i128, u32)> {
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1, text),
    };
    match digits.split_once('.') {
        Some((whole, frac)) => {
            let scale = frac.len() as u32;
            let combined = format!("{whole}{frac}");
            combined.parse::<i128>().ok().map(|m| (sign * m, scale))
        }
        None => digits.parse::<i128>().ok().map(|m| (sign * m, 0)),
    }
}

/// Add `add` into a running `(mantissa, scale)` accumulator, rescaling to the
/// larger scale so mixed-scale renderings still sum exactly.
fn add_minor(acc: &mut (i128, u32), add: (i128, u32)) {
    let scale = acc.1.max(add.1);
    // Saturate rather than panic on an absurd/corrupt money value; a saturated
    // accumulator will simply disagree with the real total (fail-closed).
    let a = rescale(acc.0, acc.1, scale).unwrap_or(i128::MAX);
    let b = rescale(add.0, add.1, scale).unwrap_or(i128::MAX);
    *acc = (a.saturating_add(b), scale);
}

/// Scale `mantissa` from `from` to `to` decimal places, or `None` if the
/// widening overflows i128 (an absurdly wide scale gap or huge mantissa).
fn rescale(mantissa: i128, from: u32, to: u32) -> Option<i128> {
    10i128
        .checked_pow(to - from)
        .and_then(|factor| mantissa.checked_mul(factor))
}

/// Whether two `(mantissa, scale)` money values are numerically equal. An
/// unrepresentable (overflowing) rescale compares unequal rather than panicking.
fn minor_eq(a: (i128, u32), b: (i128, u32)) -> bool {
    let scale = a.1.max(b.1);
    match (rescale(a.0, a.1, scale), rescale(b.0, b.1, scale)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

fn fk_slug(child_table: &str, rel: &RelSpec) -> String {
    let composite = if rel.columns.len() > 1 {
        "composite_foreign_key"
    } else {
        "foreign_key"
    };
    let name = rel.name.clone().unwrap_or_else(|| rel.columns.join(","));
    format!("{composite}:{child_table}:{name}")
}

fn open_reader(path: &Path) -> Result<std::fs::File, GenerateError> {
    std::fs::File::open(path).map_err(|error| {
        GenerateError::diagnostic(
            &crate::diagnostic::codes::VERIFY_IO,
            path.display().to_string(),
            format!("cannot open `{}` for verification: {error}", path.display()),
        )
    })
}

fn parse_error(error: anyhow::Error) -> GenerateError {
    match error.downcast::<GenerateError>() {
        Ok(error) => error,
        Err(error) => GenerateError::diagnostic(
            &crate::diagnostic::codes::VERIFY_PARSE,
            "verification",
            error.to_string(),
        ),
    }
}

fn membership_index_error(error: io::Error) -> GenerateError {
    GenerateError::diagnostic(
        &crate::diagnostic::codes::VERIFY_IO,
        "verification.membership",
        format!("membership index I/O failed: {error}"),
    )
}

fn family_index_error(error: io::Error) -> GenerateError {
    GenerateError::diagnostic(
        &crate::diagnostic::codes::VERIFY_IO,
        "verification.family",
        format!("family index I/O failed: {error}"),
    )
}

// ---------------------------------------------------------------------------
// Bounded membership/uniqueness index
// ---------------------------------------------------------------------------

impl GroupKey {
    fn new(columns: Vec<String>, budget: usize, temp: &TempConfig) -> Self {
        Self {
            columns,
            index: KeySet::new(budget, temp.clone()),
            duplicate: false,
        }
    }
}

/// The in-memory hash bucket of exact key bytes (one small-vec of colliding
/// keys per 64-bit hash) held while a [`KeySet`] is under budget.
type KeyBuckets = HashMap<u64, SmallVec<[Box<[u8]>; 1]>>;

enum KeyStorage {
    Memory(KeyBuckets),
    UnsortedSpool(ProtectedSpool),
    SortedMemory(Vec<SortRecord>),
    SortedSpool(ProtectedSpool),
    Empty,
}

/// An exact-in-memory, spill-to-disk collection of key tuples.
///
/// While under `budget` bytes it holds exact key bytes in memory. Once the
/// budget is crossed, insertion becomes append-only. Finalization external
/// sorts bounded chunks and pairwise-merges protected runs; uniqueness and FK
/// membership then use adjacent records or a linear merge join.
struct KeySet {
    budget: usize,
    used: usize,
    temp: TempConfig,
    storage: KeyStorage,
    operations: Cell<u64>,
}

impl KeySet {
    fn new(budget: usize, temp: TempConfig) -> Self {
        Self {
            budget,
            used: 0,
            temp,
            storage: KeyStorage::Memory(HashMap::new()),
            operations: Cell::new(0),
        }
    }

    /// Insert a key; returns `Ok(true)` if newly added, `Ok(false)` if an exact
    /// duplicate was already present.
    fn insert(&mut self, key: &[u8], hash: u64) -> io::Result<bool> {
        let mut spill = false;
        match &mut self.storage {
            KeyStorage::Memory(memory) => {
                let bucket = memory.entry(hash).or_default();
                if bucket.iter().any(|existing| existing.as_ref() == key) {
                    return Ok(false);
                }
                bucket.push(key.into());
                self.used += key.len() + 24;
                spill = self.used > self.budget;
            }
            KeyStorage::UnsortedSpool(spool) => write_key(spool, hash, key)?,
            KeyStorage::SortedMemory(_) | KeyStorage::SortedSpool(_) | KeyStorage::Empty => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot insert into a finalized key index",
                ));
            }
        }
        if spill {
            self.spill()?;
        }
        Ok(true)
    }

    /// Append a key while retaining duplicates, used for child FK occurrences.
    fn append(&mut self, key: &[u8], hash: u64) -> io::Result<()> {
        let mut spill = false;
        match &mut self.storage {
            KeyStorage::Memory(memory) => {
                memory.entry(hash).or_default().push(key.into());
                self.used += key.len() + 24;
                spill = self.used > self.budget;
            }
            KeyStorage::UnsortedSpool(spool) => write_key(spool, hash, key)?,
            KeyStorage::SortedMemory(_) | KeyStorage::SortedSpool(_) | KeyStorage::Empty => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot append to a finalized key index",
                ));
            }
        }
        if spill {
            self.spill()?;
        }
        Ok(())
    }

    /// Move the in-memory keys to a fresh protected spool and switch modes.
    fn spill(&mut self) -> io::Result<()> {
        let storage = std::mem::replace(&mut self.storage, KeyStorage::Empty);
        let KeyStorage::Memory(memory) = storage else {
            self.storage = storage;
            return Ok(());
        };
        let mut spool = ProtectedSpool::create(&self.temp)?;
        for (hash, bucket) in memory {
            for key in bucket {
                write_key(&mut spool, hash, &key)?;
            }
        }
        spool.flush()?;
        self.storage = KeyStorage::UnsortedSpool(spool);
        Ok(())
    }

    /// Finalize the index and report whether any exact duplicate exists.
    fn finalize(&mut self) -> io::Result<bool> {
        let storage = std::mem::replace(&mut self.storage, KeyStorage::Empty);
        self.storage = match storage {
            KeyStorage::Memory(memory) => {
                let mut records: Vec<SortRecord> = memory
                    .into_iter()
                    .flat_map(|(hash, bucket)| {
                        bucket.into_iter().map(move |key| SortRecord {
                            hash,
                            key: key.into_vec(),
                            payload: Vec::new(),
                        })
                    })
                    .collect();
                records.sort_unstable_by(sort_record_cmp);
                KeyStorage::SortedMemory(records)
            }
            KeyStorage::UnsortedSpool(spool) => {
                let mut stats = SortStats::default();
                let sorted = external_sort(spool, &self.temp, self.budget, &mut stats)?;
                self.operations
                    .set(self.operations.get() + stats.operations);
                match sorted {
                    Some(spool) => KeyStorage::SortedSpool(spool),
                    None => KeyStorage::SortedMemory(Vec::new()),
                }
            }
            finalized @ (KeyStorage::SortedMemory(_) | KeyStorage::SortedSpool(_)) => finalized,
            KeyStorage::Empty => KeyStorage::SortedMemory(Vec::new()),
        };

        let mut replay = self.replay()?;
        let mut previous: Option<SortRecord> = None;
        let mut duplicate = false;
        let mut operations = 0u64;
        for record in &mut replay {
            let record = record?;
            operations += 1;
            if previous.as_ref().is_some_and(|previous| {
                exact_key_cmp(previous.hash, &previous.key, record.hash, &record.key)
                    == Ordering::Equal
            }) {
                duplicate = true;
            }
            previous = Some(record);
        }
        self.operations.set(self.operations.get() + operations);
        Ok(duplicate)
    }

    fn replay(&self) -> io::Result<SortedRecordReplay<'_>> {
        match &self.storage {
            KeyStorage::SortedMemory(records) => Ok(SortedRecordReplay::Memory(records.iter())),
            KeyStorage::SortedSpool(spool) => Ok(SortedRecordReplay::Spool(SpoolReader::new(
                BufReader::new(std::fs::File::open(spool.path())?),
            ))),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "key index must be finalized before replay",
            )),
        }
    }

    /// Count child-key occurrences absent from the finalized parent set.
    fn missing_from(&self, parent: &Self) -> io::Result<u64> {
        let mut children = self.replay()?;
        let mut parents = parent.replay()?;
        let mut operations = 0u64;
        let mut parent_record = next_sorted_record(&mut parents, &mut operations)?;
        let mut missing = 0u64;

        while let Some(child) = next_sorted_record(&mut children, &mut operations)? {
            while parent_record.as_ref().is_some_and(|parent| {
                exact_key_cmp(parent.hash, &parent.key, child.hash, &child.key) == Ordering::Less
            }) {
                parent_record = next_sorted_record(&mut parents, &mut operations)?;
            }
            if !parent_record.as_ref().is_some_and(|parent| {
                exact_key_cmp(parent.hash, &parent.key, child.hash, &child.key) == Ordering::Equal
            }) {
                missing += 1;
            }
        }
        self.operations.set(self.operations.get() + operations);
        Ok(missing)
    }

    #[cfg(test)]
    fn sort_operations(&self) -> u64 {
        self.operations.get()
    }

    #[cfg(test)]
    fn spilled_memory_entries(&self) -> usize {
        match &self.storage {
            KeyStorage::Memory(buckets) => buckets.values().map(SmallVec::len).sum(),
            _ => 0,
        }
    }
}

enum SortedRecordReplay<'a> {
    Memory(std::slice::Iter<'a, SortRecord>),
    Spool(SpoolReader<BufReader<std::fs::File>>),
}

impl Iterator for SortedRecordReplay<'_> {
    type Item = io::Result<SortRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(records) => records.next().cloned().map(Ok),
            Self::Spool(reader) => match reader.read_row() {
                Ok(Some(row)) => Some(SortRecord::from_spooled(row)),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            },
        }
    }
}

fn next_sorted_record(
    replay: &mut SortedRecordReplay<'_>,
    operations: &mut u64,
) -> io::Result<Option<SortRecord>> {
    let Some(record) = replay.next() else {
        return Ok(None);
    };
    *operations += 1;
    record.map(Some)
}

fn write_key(spool: &mut ProtectedSpool, hash: u64, key: &[u8]) -> io::Result<()> {
    let row = SpooledRow {
        table_id: 0,
        row_index: hash,
        values: vec![GeneratedValue::Bytes(key.to_vec())],
    };
    spool.write_row(&row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn money_comparison_does_not_overflow_on_absurd_values() {
        // A huge mantissa or a wide scale gap must not panic; an
        // unrepresentable comparison is treated as unequal (fail-closed), never
        // a false match.
        assert!(!minor_eq((i128::MAX, 0), (1, 18)));
        assert!(!minor_eq((1, 0), (1, 40)));
        // Ordinary values still compare correctly.
        assert!(minor_eq((100, 2), (1000, 3)));
        assert!(!minor_eq((100, 2), (200, 2)));
    }

    #[test]
    fn spilled_key_set_does_not_retain_one_memory_entry_per_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut keys = KeySet::new(0, TempConfig::in_dir(dir.path()));

        for key in 0u64..256 {
            let bytes = key.to_le_bytes();
            keys.insert(&bytes, key).unwrap();
        }

        assert_eq!(keys.spilled_memory_entries(), 0);
        assert!(!keys.finalize().unwrap());
    }

    #[test]
    fn spilled_key_sort_and_fk_merge_work_is_subquadratic() {
        let dir = tempfile::tempdir().unwrap();
        let temp = TempConfig::in_dir(dir.path());
        let count = 1_024u64;
        let mut unique = KeySet::new(0, temp.clone());
        let mut parents = KeySet::new(0, temp.clone());
        let mut children = KeySet::new(0, temp);

        for key in 0..count {
            let bytes = key.to_le_bytes();
            unique.insert(&bytes, key).unwrap();
            parents.insert(&bytes, key).unwrap();
            children.append(&bytes, key).unwrap();
        }
        unique
            .insert(&(count - 1).to_le_bytes(), count - 1)
            .unwrap();
        children.append(&count.to_le_bytes(), count).unwrap();

        assert!(unique.finalize().unwrap(), "spilled duplicate was missed");
        assert!(!parents.finalize().unwrap());
        assert!(!children.finalize().unwrap());
        assert_eq!(children.missing_from(&parents).unwrap(), 1);

        let record_io =
            unique.sort_operations() + parents.sort_operations() + children.sort_operations();
        assert!(
            record_io < count * 3 * 32,
            "external sort/merge did {record_io} record operations for {count} keys per set"
        );
    }

    #[test]
    fn spilled_family_merge_work_is_subquadratic() {
        let dir = tempfile::tempdir().unwrap();
        let temp = TempConfig::in_dir(dir.path());
        let count = 512u64;
        let mut family = FamilyAcc {
            slug: "family_sum:orders:total->order_items:total".into(),
            parent_table: "orders".into(),
            parent_column: "total".into(),
            child_table: "order_items".into(),
            child_column: "total".into(),
            child_fk_columns: vec!["order_id".into()],
            parent_key_columns: vec!["id".into()],
            parent_values: FamilyValueStore::new(temp.clone(), 0),
            child_values: FamilyValueStore::new(temp, 0),
            inexact: false,
            operations: 0,
        };

        for key in 0..count {
            family
                .parent_values
                .append(&key.to_le_bytes(), key, (2, 0))
                .unwrap();
        }
        // A corrupt duplicate parent is reported by the independent uniqueness
        // check; the family check must still compare the same child sum to both
        // parent records, matching the exact pre-sort behavior.
        family
            .parent_values
            .append(&0u64.to_le_bytes(), 0, (2, 0))
            .unwrap();
        for key in (0..count).rev() {
            family
                .child_values
                .append(&key.to_le_bytes(), key, (1, 0))
                .unwrap();
            family
                .child_values
                .append(&key.to_le_bytes(), key, (1, 0))
                .unwrap();
        }

        assert_eq!(family.compare().unwrap(), (0, false));
        let records = count * 3 + 1;
        assert!(
            family.operations < records * 32,
            "family external sort/merge did {} record operations for {records} records",
            family.operations
        );
    }

    #[test]
    fn category_tallies_retain_only_configured_values() {
        let mut tallies = CategoryTallies::default();
        tallies.register(&DistributionExpectation {
            table: "events".into(),
            column: "kind".into(),
            categories: vec![("configured".into(), 1.0)],
            tolerance: 0.1,
        });

        for value in 0..10_000 {
            tallies.record("kind", &value.to_string());
        }
        tallies.record("kind", "configured");

        assert_eq!(tallies.counts["kind"].len(), 1);
        assert_eq!(tallies.counts["kind"]["configured"], 1);
    }
}
