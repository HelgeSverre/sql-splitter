//! The immutable [`GenerationPlan`]: the compiler's output.
//!
//! A plan is what [`super::compiler::ModelCompiler`] produces from a complete
//! [`crate::synthetic::model::SyntheticModel`] and a set of CLI
//! [`CompileOptions`]. It fixes, once and for all, *which* tables are
//! generated, in *what order*, and with *exactly how many rows* — every later
//! stage reads a plan rather than re-deriving those decisions.
//!
//! Plan fields are public but treated as read-only after construction: the
//! compiler builds a plan and hands it out; nothing mutates it in place. Most
//! types here derive `Debug`/`Clone`; [`PlannedTable`] cannot, because it owns
//! `Box<dyn CompiledPlanner>` trait objects, so it carries a hand-written
//! [`std::fmt::Debug`] that keeps the plan observable without pretending the
//! compiled planners are printable.
//!
//! Several types are deliberately minimal stubs that later tasks fill in:
//! [`PlannedColumn`]/[`ColumnOwner`] gain real owners in Task 10,
//! [`CompiledRelationship`] gains fan-out assignment in Task 13, and
//! [`ExecutionPhase`] gains `Family`/`DeferredConstraints` plus the
//! temp-storage/verification [`PlanEstimates`] in Tasks 22/26.

use std::fmt;

use crate::diagnostic::Diagnostic;
use crate::parser::SqlDialect;
use crate::synthetic::model::{InsertMode, OutputMode};
use crate::synthetic::schema::{PortableColumn, PortableTable};

use super::registry::CompiledPlanner;
use super::seed::SeedRoot;

/// A fully compiled generation plan: the immutable contract every downstream
/// stage reads.
#[derive(Debug)]
pub struct GenerationPlan {
    /// Dialect the source dump was parsed with, if the model recorded one.
    pub input_dialect: Option<SqlDialect>,
    /// Resolved output/rendering configuration.
    pub output: CompiledOutput,
    /// The selected tables, in dependency order (parents before children).
    pub tables: Vec<PlannedTable>,
    /// Generation phases, in execution order. Task 9 emits one
    /// [`ExecutionPhase::Table`] per selected table; Task 22 adds richer phases.
    pub phases: Vec<ExecutionPhase>,
    /// Non-error diagnostics that survive a successful compile: drained merge
    /// warnings plus compile-stage warnings (e.g. a `--max-rows` cap).
    pub diagnostics: Vec<Diagnostic>,
    /// Cost/size estimates. Temp-storage and verification fields are filled by
    /// Tasks 22/26; Task 9 only fills `total_rows`.
    pub estimates: PlanEstimates,
}

impl GenerationPlan {
    /// Look up a planned table by name.
    pub fn table(&self, name: &str) -> Option<&PlannedTable> {
        self.tables.iter().find(|table| table.name == name)
    }
}

/// Resolved output/rendering configuration, carried over from the model's
/// `output:` block with the dialect parsed to a [`SqlDialect`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompiledOutput {
    /// Target render dialect, if the model pinned one (else the input dialect).
    pub dialect: Option<SqlDialect>,
    /// Whether to render schema, data, or both.
    pub mode: Option<OutputMode>,
    /// How PostgreSQL rows are rendered (INSERT vs COPY).
    pub inserts: Option<InsertMode>,
    /// Row batch size for multi-row INSERTs.
    pub batch_size: Option<u32>,
}

/// One unit of generation work, in execution order.
///
/// Task 9 only distinguishes tables; `Family` (correlated multi-table groups)
/// and `DeferredConstraints` arrive with Task 22's family planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionPhase {
    /// Generate a single table's rows, identified by name.
    Table(String),
}

/// A single selected table with its resolved row count and generation surface.
///
/// Cannot derive `Debug`/`Clone`: `planners` holds trait objects. See the
/// module docs for the manual `Debug`.
pub struct PlannedTable {
    /// Table name.
    pub name: String,
    /// Exact, resolved row count.
    pub rows: u64,
    /// The table's portable schema.
    pub schema: PortableTable,
    /// The table's resolved seed.
    pub seed: ResolvedTableSeed,
    /// Every schema column, paired with its owner. Task 9 lists them
    /// [`ColumnOwner::Unowned`]; Task 10 assigns real owners.
    pub columns: Vec<PlannedColumn>,
    /// Compiled relationships to parent tables. Task 9 records the shape;
    /// Task 13 completes fan-out assignment.
    pub relationships: Vec<CompiledRelationship>,
    /// Compiled table-level planners. Populated by Task 22; empty here.
    pub planners: Vec<Box<dyn CompiledPlanner>>,
}

impl fmt::Debug for PlannedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `CompiledPlanner` is not `Debug`, so describe the planners by count
        // rather than dropping the field from the plan's observability.
        f.debug_struct("PlannedTable")
            .field("name", &self.name)
            .field("rows", &self.rows)
            .field("schema", &self.schema)
            .field("seed", &self.seed)
            .field("columns", &self.columns)
            .field("relationships", &self.relationships)
            .field(
                "planners",
                &format_args!("[{} compiled]", self.planners.len()),
            )
            .finish()
    }
}

/// A schema column paired with the operator that owns its values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedColumn {
    /// The column's portable schema.
    pub schema: PortableColumn,
    /// What produces this column's values.
    pub owner: ColumnOwner,
}

/// What produces a column's values.
///
/// Task 9 only ever assigns [`ColumnOwner::Unowned`]; Task 10 introduces the
/// generator/planner/relationship owners and the completeness check that every
/// generated column has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnOwner {
    /// No owner resolved yet (Task 10 resolves ownership).
    Unowned,
}

/// A compiled relationship from a child table to a parent table.
///
/// Task 9 captures the referential shape declared in the model; Task 13
/// completes it with the per-row parent assignment strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRelationship {
    /// The relationship's declared name, if any.
    pub name: Option<String>,
    /// The child columns that carry the foreign key.
    pub columns: Vec<String>,
    /// The referenced parent table.
    pub parent_table: String,
    /// The referenced parent columns.
    pub parent_columns: Vec<String>,
}

/// A table's seed, resolved against the run's root seed and CLI overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedTableSeed {
    /// Inherits the run's root seed; per-table streams derive from it.
    Inherited(SeedRoot),
    /// Pinned to an independent, table-local seed.
    Fixed(SeedRoot),
    /// Opted out of determinism; the runtime draws fresh entropy per run.
    Random,
}

/// Cost and size estimates for a plan.
///
/// `total_rows` is filled by Task 9. `temp_storage_bytes` (family spool state)
/// is filled by Task 22 and `verification_cost` by Task 26; both stay zero
/// here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanEstimates {
    /// Sum of every selected table's resolved row count.
    pub total_rows: u64,
    /// Peak temporary storage for buffered/family state. Filled by Task 22.
    pub temp_storage_bytes: u64,
    /// Estimated verification cost. Filled by Task 26.
    pub verification_cost: u64,
}
