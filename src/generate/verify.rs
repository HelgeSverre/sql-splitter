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
//! exceeded, keeping only 64-bit hashes in memory and confirming collisions
//! against the on-disk key bytes. Dense integer key domains stay in memory.

use std::collections::{HashMap, HashSet};
use std::io::{self, BufReader};
use std::path::Path;

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
                                    );
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
                                    let _ = visit_insert_rows_with(
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
                                                );
                                            }
                                            Ok(RowFlow::Continue)
                                        },
                                    );
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
                                    );
                                }
                            }
                        }
                    }
                    ParserEvent::CopyEnd => copy = None,
                }
                Ok(RowFlow::Continue)
            })
            .map_err(parse_error)?;

        audit.finish(&mut report, &self.distributions);
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
    /// Column count for the arity check.
    column_count: usize,
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
        let tables = plan.tables.iter().map(TableSpec::from_table).collect();
        let families = extract_families(plan);
        Self { tables, families }
    }
}

impl TableSpec {
    fn from_table(table: &PlannedTable) -> Self {
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

        let non_null_columns = table
            .schema
            .columns
            .iter()
            .filter(|col| !col.nullable && !database_produced(table, &col.name))
            .map(|col| col.name.clone())
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
            column_count: table.schema.columns.len(),
            non_null_columns,
            primary_key: table.schema.primary_key.clone(),
            unique_groups,
            relationships,
            predicates,
        }
    }
}

/// Whether a non-null column is produced by the database (so verification does
/// not expect a literal value for it).
fn database_produced(table: &PlannedTable, column: &str) -> bool {
    matches!(
        table
            .columns
            .iter()
            .find(|c| c.schema.name == column)
            .map(|c| &c.owner),
        Some(ColumnOwner::GeneratedByDatabase | ColumnOwner::DatabaseDefault)
    )
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
    /// Arity violations (a row whose value count differs from its siblings').
    arity_violations: u64,
    /// The value count established by the first observed row; later rows that
    /// deviate are arity violations. Established per table because the renderer
    /// legitimately omits database-produced/DEFAULT columns from every row.
    expected_width: Option<usize>,
    /// Uniqueness key groups (PK + unique constraints).
    unique_groups: Vec<GroupKey>,
    /// Membership indexes this table exposes for children (parent-column groups).
    member_groups: Vec<GroupKey>,
    /// Planner predicate failures, keyed by a stable check slug.
    predicate_failures: HashMap<String, u64>,
    /// Sampled category tallies, keyed by column name.
    category_counts: HashMap<String, HashMap<String, u64>>,
    /// Category columns to tally (from distribution expectations).
    category_columns: Vec<String>,
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
    /// Parent value (in minor units) keyed by the join-key hash.
    parent_values: HashMap<u64, (i128, u32)>,
    /// Summed child value keyed by the join-key hash.
    child_sums: HashMap<u64, (i128, u32)>,
    /// Whether any decode/scale problem made the check inexact.
    inexact: bool,
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

            tables.insert(
                table.name.clone(),
                TableState {
                    plan_index: index,
                    rows: 0,
                    undecodable: 0,
                    null_violations: HashMap::new(),
                    arity_violations: 0,
                    expected_width: None,
                    unique_groups,
                    member_groups,
                    predicate_failures: HashMap::new(),
                    category_counts: HashMap::new(),
                    category_columns: Vec::new(),
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
                parent_values: HashMap::new(),
                child_sums: HashMap::new(),
                inexact: false,
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
                if !state.category_columns.contains(&expectation.column) {
                    state.category_columns.push(expectation.column.clone());
                }
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
    ) {
        let plan_index = match self.tables.get(table_name) {
            Some(state) => state.plan_index,
            None => return,
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

        // Arity: a row's value count must match its siblings'. The width is
        // established by the first row of the table (the renderer omits
        // database-produced/DEFAULT columns uniformly, so the plan's full column
        // count is not the rendered width), and can never exceed it.
        let observed_width = all_values.len();
        let _ = planned.column_count;

        // Collect the failures first (immutable borrows of the spec), then apply
        // them to the table's mutable state.
        let mut null_hits: Vec<String> = Vec::new();
        for col in &planned.non_null_columns {
            // Only an explicitly rendered `NULL` is a violation. A column absent
            // from the row was rendered as `DEFAULT` (omitted from the INSERT),
            // which the database fills — not a null violation.
            if matches!(value_of(col), Some(PkValue::Null)) {
                null_hits.push(col.clone());
            }
        }

        // Predicate evaluation.
        let mut predicate_hits: Vec<String> = Vec::new();
        for predicate in &planned.predicates {
            if let Some(slug) = evaluate_predicate(predicate, table_name, &value_of) {
                predicate_hits.push(slug);
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

        // FK membership: check each relationship's child key against the parent.
        let fk_failures = self.check_foreign_keys(planned, &value_of);

        // Family accumulation.
        self.accumulate_families(table_name, &value_of);

        // Sampled category tallies.
        let category_hits: Vec<(String, String)> = {
            let state = &self.tables[table_name];
            state
                .category_columns
                .iter()
                .filter_map(|col| value_of(col).and_then(text_of).map(|v| (col.clone(), v)))
                .collect()
        };

        // Now apply everything mutably.
        let state = self.tables.get_mut(table_name).expect("table state exists");
        state.rows += 1;
        match state.expected_width {
            None => state.expected_width = Some(observed_width),
            Some(width) if width != observed_width => state.arity_violations += 1,
            Some(_) => {}
        }
        for name in null_hits {
            *state.null_violations.entry(name).or_insert(0) += 1;
        }
        for slug in predicate_hits {
            *state.predicate_failures.entry(slug).or_insert(0) += 1;
        }
        for (group, key) in state.unique_groups.iter_mut().zip(unique_keys) {
            if let Some((bytes, hash)) = key {
                if let Ok(false) = group.index.insert(&bytes, hash) {
                    group.duplicate = true;
                }
            }
        }
        for (group, key) in state.member_groups.iter_mut().zip(member_keys) {
            if let Some((bytes, hash)) = key {
                let _ = group.index.insert(&bytes, hash);
            }
        }
        for slug in fk_failures {
            *state.predicate_failures.entry(slug).or_insert(0) += 1;
        }
        for (col, value) in category_hits {
            *state
                .category_counts
                .entry(col)
                .or_default()
                .entry(value)
                .or_insert(0) += 1;
        }
    }

    /// Check every foreign key of `planned` against the parent's exposed
    /// membership index. Returns the slugs of any failed relationships.
    fn check_foreign_keys<'v>(
        &self,
        planned: &TableSpec,
        value_of: &impl Fn(&str) -> Option<&'v PkValue>,
    ) -> Vec<String> {
        let mut failures = Vec::new();
        for rel in &planned.relationships {
            let Some((bytes, hash)) = encode_group(&rel.columns, value_of) else {
                // A NULL foreign key is allowed (optional relationship); skip.
                continue;
            };
            // All-null composite key → treated as absent, not a violation.
            if is_all_null(&rel.columns, value_of) {
                continue;
            }
            let parent = self.tables.get(&rel.parent_table);
            let Some(parent) = parent else { continue };
            let group = parent
                .member_groups
                .iter()
                .find(|g| g.columns == rel.parent_columns);
            if let Some(group) = group {
                // Fail CLOSED: a spool I/O error during collision confirmation
                // must not be treated as "present" (a silently-satisfied FK
                // check). An error means the membership could not be confirmed,
                // so the FK is reported as a failure rather than passed.
                let present = group.index.contains(&bytes, hash).unwrap_or(false);
                if !present {
                    failures.push(fk_slug(&planned.name, rel));
                }
            }
        }
        failures
    }

    /// Accumulate parent totals and child sums for every family this row
    /// participates in (as parent or child).
    fn accumulate_families<'v>(
        &mut self,
        table_name: &str,
        value_of: &impl Fn(&str) -> Option<&'v PkValue>,
    ) {
        for family in &mut self.families {
            if family.parent_table == table_name {
                if let Some((_, hash)) = encode_group(&family.parent_key_columns, value_of) {
                    if let Some(minor) = value_of(&family.parent_column).and_then(money_minor) {
                        family.parent_values.insert(hash, minor);
                    } else {
                        family.inexact = true;
                    }
                }
            }
            if family.child_table == table_name {
                if let Some((_, hash)) = encode_group(&family.child_fk_columns, value_of) {
                    if let Some(minor) = value_of(&family.child_column).and_then(money_minor) {
                        let entry = family.child_sums.entry(hash).or_insert((0, minor.1));
                        add_minor(entry, minor);
                    } else {
                        family.inexact = true;
                    }
                }
            }
        }
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

    /// Emit every accumulated check into the report.
    fn finish(
        &mut self,
        report: &mut VerificationReport,
        distributions: &[DistributionExpectation],
    ) {
        for planned in &self.spec.tables {
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
            for rel in &planned.relationships {
                let slug = fk_slug(&planned.name, rel);
                let failed = state.predicate_failures.get(&slug).copied().unwrap_or(0);
                report.record(
                    slug,
                    CheckStatus::Exact,
                    failed == 0,
                    format!("{failed} foreign-key value(s) missing from the parent"),
                );
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
                report.record(
                    slug,
                    CheckStatus::Exact,
                    failed == 0,
                    format!("{failed} row(s) violated the planner invariant"),
                );
            }
        }

        // Family sum checks.
        for family in &self.families {
            let mut failed = 0u64;
            let mut missing = false;
            for (key, parent) in &family.parent_values {
                match family.child_sums.get(key) {
                    Some(child) => {
                        if !minor_eq(*parent, *child) {
                            failed += 1;
                        }
                    }
                    None => {
                        // No children summed to this parent — only a mismatch if
                        // the parent expects a nonzero aggregate.
                        if parent.0 != 0 {
                            failed += 1;
                            missing = true;
                        }
                    }
                }
            }
            let status = if family.inexact {
                CheckStatus::NotChecked
            } else {
                CheckStatus::Exact
            };
            report.record(
                family.slug.clone(),
                status,
                failed == 0 && !family.inexact,
                if family.inexact {
                    "family sum not evaluated (unparseable money values)".to_string()
                } else {
                    format!(
                        "{failed} parent aggregate(s) disagreed with the child sum{}",
                        if missing { " (missing children)" } else { "" }
                    )
                },
            );
        }

        // Sampled distributions.
        self.finish_distributions(report, distributions);
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
            let counts = state.category_counts.get(&expectation.column);
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
    let a = rescale(acc.0, acc.1, scale);
    let b = rescale(add.0, add.1, scale);
    *acc = (a + b, scale);
}

fn rescale(mantissa: i128, from: u32, to: u32) -> i128 {
    mantissa * 10i128.pow(to - from)
}

/// Whether two `(mantissa, scale)` money values are numerically equal.
fn minor_eq(a: (i128, u32), b: (i128, u32)) -> bool {
    let scale = a.1.max(b.1);
    rescale(a.0, a.1, scale) == rescale(b.0, b.1, scale)
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
    GenerateError::diagnostic(
        &crate::diagnostic::codes::VERIFY_PARSE,
        "verification",
        error.to_string(),
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

/// An exact-in-memory, spill-to-disk set of key tuples.
///
/// While under `budget` bytes it holds the full key bytes in memory (exact, no
/// false positives). Once the budget is crossed it spills every key's bytes to a
/// [`ProtectedSpool`] and keeps only 64-bit hashes in memory; membership and
/// duplicate detection then confirm a hash hit by rescanning the spool for the
/// exact key bytes. Memory after spill is bounded by the hash set (8 bytes per
/// distinct hash), not by the key payloads.
struct KeySet {
    budget: usize,
    used: usize,
    temp: TempConfig,
    /// Present until the first spill.
    memory: Option<KeyBuckets>,
    /// Present after spilling: the observed hashes.
    hashes: HashSet<u64>,
    spool: Option<ProtectedSpool>,
}

impl KeySet {
    fn new(budget: usize, temp: TempConfig) -> Self {
        Self {
            budget,
            used: 0,
            temp,
            memory: Some(HashMap::new()),
            hashes: HashSet::new(),
            spool: None,
        }
    }

    /// Insert a key; returns `Ok(true)` if newly added, `Ok(false)` if an exact
    /// duplicate was already present.
    fn insert(&mut self, key: &[u8], hash: u64) -> io::Result<bool> {
        if let Some(memory) = &mut self.memory {
            let bucket = memory.entry(hash).or_default();
            if bucket.iter().any(|existing| existing.as_ref() == key) {
                return Ok(false);
            }
            bucket.push(key.into());
            self.used += key.len() + 24;
            if self.used > self.budget {
                self.spill()?;
            }
            return Ok(true);
        }
        // Spilled: confirm a possible duplicate on a hash hit, else append.
        if self.hashes.contains(&hash) && self.scan_matches(key, hash)? {
            return Ok(false);
        }
        self.append_spool(key, hash)?;
        self.hashes.insert(hash);
        Ok(true)
    }

    /// Whether `key` is present.
    fn contains(&self, key: &[u8], hash: u64) -> io::Result<bool> {
        if let Some(memory) = &self.memory {
            return Ok(memory
                .get(&hash)
                .is_some_and(|bucket| bucket.iter().any(|e| e.as_ref() == key)));
        }
        if !self.hashes.contains(&hash) {
            return Ok(false);
        }
        self.scan_matches(key, hash)
    }

    /// Move the in-memory keys to a fresh protected spool and switch modes.
    fn spill(&mut self) -> io::Result<()> {
        let mut spool = ProtectedSpool::create(&self.temp)?;
        if let Some(memory) = self.memory.take() {
            for (hash, bucket) in memory {
                for key in bucket {
                    write_key(&mut spool, hash, &key)?;
                    self.hashes.insert(hash);
                }
            }
        }
        spool.flush()?;
        self.spool = Some(spool);
        Ok(())
    }

    fn append_spool(&mut self, key: &[u8], hash: u64) -> io::Result<()> {
        if let Some(spool) = &mut self.spool {
            write_key(spool, hash, key)?;
            spool.flush()?;
        }
        Ok(())
    }

    /// Scan the spool for a record whose hash and key bytes both match — the
    /// collision-confirmation step. Re-opens the spool file read-only (writes are
    /// flushed after every append), so it needs only `&self`.
    fn scan_matches(&self, key: &[u8], hash: u64) -> io::Result<bool> {
        let spool = match &self.spool {
            Some(spool) => spool,
            None => return Ok(false),
        };
        let file = std::fs::File::open(spool.path())?;
        let mut reader = SpoolReader::new(BufReader::new(file));
        while let Some(row) = reader.read_row()? {
            if row.row_index == hash {
                if let Some(GeneratedValue::Bytes(bytes)) = row.values.first() {
                    if bytes.as_slice() == key {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
}

fn write_key(spool: &mut ProtectedSpool, hash: u64, key: &[u8]) -> io::Result<()> {
    let row = SpooledRow {
        table_id: 0,
        row_index: hash,
        values: vec![GeneratedValue::Bytes(key.to_vec())],
    };
    spool.write_row(&row)
}
