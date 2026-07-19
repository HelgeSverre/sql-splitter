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
//! The plan also fixes column ownership, relationship assignment strategies,
//! execution phases for individual tables and correlated families, and cost
//! estimates used by reporting and execution.

use std::fmt;

use crate::diagnostic::Diagnostic;
use crate::parser::SqlDialect;
use crate::synthetic::model::{InsertMode, OutputMode};
use crate::synthetic::schema::{PortableColumn, PortableTable};

use super::registry::{CompiledGenerator, CompiledModifier, CompiledPlanner};
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
    /// Generation phases, in execution order, for individual tables or
    /// correlated table families.
    pub phases: Vec<ExecutionPhase>,
    /// Non-error diagnostics that survive a successful compile: drained merge
    /// warnings plus compile-stage warnings (e.g. a `--max-rows` cap).
    pub diagnostics: Vec<Diagnostic>,
    /// Cost and size estimates for reporting and resource planning.
    pub estimates: PlanEstimates,
    /// The exact in-memory byte budget for each correlated family's buffered
    /// child rows before it spills to a protected spool. The engine's
    /// `commerce.order_family` execution keys its [`FamilyBuffer`] off this. A
    /// large default keeps ordinary families in memory; a run may pin a small
    /// budget (`--family-budget`) to exercise the spill path.
    pub family_budget_bytes: u64,
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
/// A [`Table`](Self::Table) phase generates one table on its own. A
/// [`Family`](Self::Family) phase generates a correlated group of tables
/// together under a shared memory budget (so cross-table correlations are
/// coherent), and a [`DeferredConstraints`](Self::DeferredConstraints) phase
/// records constraints (e.g. self-referential foreign keys, circular
/// references) that are applied only after the referenced rows exist. The
/// compiler emits `Family` for correlated parent/child generation; its bounded
/// buffering uses [`crate::generate::output::FamilyBuffer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionPhase {
    /// Generate a single table's rows, identified by name.
    Table(String),
    /// Generate a correlated family of tables together under one budget.
    Family(FamilyPhase),
    /// Apply constraints that could only be satisfied after their referents
    /// were generated.
    DeferredConstraints(DeferredConstraints),
}

/// A correlated family of tables generated together under a shared, exact
/// memory budget.
///
/// The budget is fixed at compile time; at run time a
/// [`crate::generate::output::FamilyBuffer`] keeps the family's buffered rows in
/// memory only while under `budget_bytes` and spills to a protected spool the
/// moment a push would cross it, so a family never retains every child row in an
/// unbounded `Vec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyPhase {
    /// A stable name for the family (typically its root parent table).
    pub name: String,
    /// The member tables, in dependency order (parents before children).
    pub tables: Vec<String>,
    /// The exact in-memory byte budget for the family's buffered rows.
    pub budget_bytes: u64,
}

/// Constraints deferred until after the rows they reference are generated —
/// self-referential or circular foreign keys the ordinary parent-before-child
/// ordering cannot satisfy in one pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeferredConstraints {
    /// The tables whose constraints are applied in this phase.
    pub tables: Vec<String>,
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
    /// Every schema column, paired with the owner that produces its values.
    pub columns: Vec<PlannedColumn>,
    /// Compiled relationships to parent tables, including fan-out assignment.
    pub relationships: Vec<CompiledRelationship>,
    /// Compiled table-level planners, in declaration order. A
    /// [`ColumnOwner::Planner`]'s `planner_index` indexes into this vector.
    /// Declared planners are compiled here so ownership can reference and the
    /// runtime can execute them without resolving the registry again.
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

/// A schema column paired with the operator that owns its values and the
/// compiled modifier pipeline applied after generation.
///
/// Cannot derive `Debug`/`Clone`/`PartialEq`: a [`ColumnOwner::Generator`]
/// holds a `Box<dyn CompiledGenerator>` and `modifiers` holds
/// `Box<dyn CompiledModifier>` trait objects. The hand-written [`fmt::Debug`]
/// describes the modifier pipeline by count.
pub struct PlannedColumn {
    /// The column's portable schema.
    pub schema: PortableColumn,
    /// What produces this column's values.
    pub owner: ColumnOwner,
    /// The compiled modifier pipeline, in declared order, applied to the value
    /// after the owner produces it. Only populated for generator-owned columns;
    /// empty otherwise (see `ModelCompiler::compile_modifiers`).
    pub modifiers: Vec<Box<dyn CompiledModifier>>,
}

impl fmt::Debug for PlannedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `CompiledModifier` is not `Debug`; describe the pipeline by count so
        // the plan stays observable without pretending the operators print.
        f.debug_struct("PlannedColumn")
            .field("schema", &self.schema)
            .field("owner", &self.owner)
            .field(
                "modifiers",
                &format_args!("[{} compiled]", self.modifiers.len()),
            )
            .finish()
    }
}

/// What produces a column's values.
///
/// Exactly one owner is assigned to every non-omitted column during
/// compilation. Two generators or planners claiming the same column is a
/// `GEN-COLUMN-OWNER-CONFLICT`; a column that no rule and no structural schema
/// fact can supply is a `GEN-COLUMN-OWNER-MISSING`.
pub enum ColumnOwner {
    /// An explicit column generator produces the value. Holds the compiled
    /// generator so the row hot path never re-resolves the registry.
    Generator {
        /// The resolved generator kind (canonical, not an alias).
        kind: String,
        /// The compiled generator, ready to run per row.
        compiled: Box<dyn CompiledGenerator>,
    },
    /// A table-level planner coordinates this column with others. Indexes
    /// into [`PlannedTable::planners`].
    Planner {
        /// The resolved planner kind.
        kind: String,
        /// The owning planner's index in [`PlannedTable::planners`].
        planner_index: usize,
    },
    /// A cross-table family planner declared on *another* table owns this
    /// column's value (a `commerce.order_family` child column). The engine
    /// fills it from the spooled family child rows rather than a same-table
    /// generator, so it needs no local owner and never renders `DEFAULT`.
    FamilyChild {
        /// The owning planner's kind, for diagnostics.
        planner_kind: String,
    },
    /// A declared relationship supplies this foreign-key column's value.
    Relationship {
        /// The owning relationship's declared name, if any.
        relationship: Option<String>,
    },
    /// The column's own `DEFAULT` expression supplies the value; the renderer
    /// emits `DEFAULT` and the database fills it.
    DatabaseDefault,
    /// The database produces the value itself — an `IDENTITY`/serial sequence,
    /// a `GENERATED` computed column, or a bare integer primary key.
    GeneratedByDatabase,
}

impl fmt::Debug for ColumnOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `CompiledGenerator` is not `Debug`; describe the owner by kind so the
        // plan stays observable without pretending the operator is printable.
        match self {
            ColumnOwner::Generator { kind, .. } => f
                .debug_struct("Generator")
                .field("kind", kind)
                .finish_non_exhaustive(),
            ColumnOwner::Planner {
                kind,
                planner_index,
            } => f
                .debug_struct("Planner")
                .field("kind", kind)
                .field("planner_index", planner_index)
                .finish(),
            ColumnOwner::FamilyChild { planner_kind } => f
                .debug_struct("FamilyChild")
                .field("planner_kind", planner_kind)
                .finish(),
            ColumnOwner::Relationship { relationship } => f
                .debug_struct("Relationship")
                .field("relationship", relationship)
                .finish(),
            ColumnOwner::DatabaseDefault => f.write_str("DatabaseDefault"),
            ColumnOwner::GeneratedByDatabase => f.write_str("GeneratedByDatabase"),
        }
    }
}

/// How a relationship distributes children across parent rows.
///
/// This is the *value-assignment* distribution (which parent each child points
/// to), distinct from the *count* distribution ([`crate::synthetic::model::
/// ChildDistribution`]) that decides how many children a table has. `Weighted`
/// and `Observed` both compile to a bounded histogram over parent row-index
/// buckets at run time; they never enumerate a per-value list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RelationshipDistribution {
    /// Each child maps a uniformly random index into `[0, parent_count)`.
    #[default]
    Uniform,
    /// Child row `r` references parent `r % parent_count`.
    Sequential,
    /// A bounded, seed-derived histogram concentrates children on some parents.
    Weighted,
    /// Same mechanism as [`Self::Weighted`]; named for provenance from an
    /// observed source profile.
    Observed,
}

/// A compiled relationship from a child table to a parent table, including the
/// per-row parent assignment strategy executed by the engine.
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
    /// How children are distributed across parent rows.
    pub distribution: RelationshipDistribution,
    /// Fraction of children (in parts-per-thousand) whose foreign key is `NULL`,
    /// applied only when the child columns are nullable. Stored as an integer so
    /// [`CompiledRelationship`] keeps `Eq`.
    pub null_permille: u16,
}

/// A table's seed, resolved against the run's root seed and CLI overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedTableSeed {
    /// Inherits the run's root seed; per-table streams derive from it.
    Inherited(SeedRoot),
    /// Pinned to an independent, table-local seed.
    Fixed(SeedRoot),
    /// Opted out of deterministic inheritance. The compiler draws and records
    /// a fresh table-local root so compiled operators and runtime relationship
    /// streams consume the same entropy for this run.
    Random(SeedRoot),
}

/// Cost and size estimates for a plan.
///
/// Compilation records `total_rows`; estimates not calculated for the current
/// plan remain zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanEstimates {
    /// Sum of every selected table's resolved row count.
    pub total_rows: u64,
    /// Peak temporary storage for buffered or family state, when estimated.
    pub temp_storage_bytes: u64,
    /// Estimated verification cost, when calculated.
    pub verification_cost: u64,
}
