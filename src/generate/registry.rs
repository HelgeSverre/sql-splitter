//! The typed extension registry: how generators, modifiers, and planners are
//! declared, discovered, and turned into runnable operators.
//!
//! Everything a value operator does is split across two lifetimes:
//!
//! * **Factories** ([`GeneratorFactory`], [`ModifierFactory`],
//!   [`PlannerFactory`]) are the *static* half. Each carries a `&'static`
//!   [descriptor](GeneratorDescriptor) that advertises its `kind`, aliases,
//!   accepted [`SqlTypeFamily`] values, ownership/read declarations, and
//!   determinism/buffering/verification capabilities. A factory validates a
//!   config once and `compile`s it into a runtime operator.
//! * **Compiled operators** ([`CompiledGenerator`], [`CompiledModifier`],
//!   [`CompiledPlanner`]) are the *hot* half. They run once per row (or per
//!   plan) and are deliberately allocation-lean: a generator overwrites a
//!   caller-owned [`GeneratedValue`] rather than returning a fresh one.
//!
//! [`ExtensionRegistry`] owns three [`Catalog`]s (one per operator role).
//! Each catalog keeps a `BTreeMap` of primary kinds — so discovery order is
//! deterministic regardless of registration order — plus a second map from
//! alias to primary kind. Registration keeps the two namespaces disjoint:
//! a primary kind may not be registered twice, and an alias may neither
//! shadow a primary kind nor duplicate another alias.

use std::collections::BTreeMap;

use rand_chacha::ChaCha8Rng;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::{GeneratorConfig, ModifierConfig, PlannerConfig};
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

use super::seed::{SeedRoot, StreamId};
use super::value::{GenerateError, GeneratedValue};

// --- Descriptor value types -------------------------------------------------

/// A single named argument a factory reads out of its config.
///
/// Descriptors carry these so tooling (`--describe`, docs, validation) can
/// list an operator's knobs without compiling it. Argument *type* checking
/// stays with the factory's `compile`, which owns the config's semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgumentSpec {
    /// The config key, e.g. `"value"` or `"min"`.
    pub name: &'static str,
    /// Whether omitting the argument is an error.
    pub required: bool,
    /// One-line description of what the argument controls.
    pub summary: &'static str,
}

/// Which columns an operator reads from or writes to, declared statically so
/// the compiler can reason about ownership before a single row is generated.
///
/// Concrete column *names* are resolved from config at compile time (a
/// planner's `writes` set, a cross-column generator's `reads` set); this enum
/// only classifies the shape of that access.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnScope {
    /// No columns — e.g. a generator that never reads a sibling value.
    None,
    /// Exactly the column the operator is attached to (generators, modifiers).
    OwnColumn,
    /// A set of columns named in the operator's configuration (planners, and
    /// cross-column generators).
    Configured,
}

/// Whether an operator's output is reproducible from its seed alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Determinism {
    /// Same seed and inputs always yield the same value.
    Deterministic,
    /// May draw from a nondeterministic source (wall clock, OS entropy); a
    /// run using this operator is not reproducible.
    NonDeterministic,
}

/// Whether an operator can stream row-by-row or must buffer rows first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Buffering {
    /// Emits each row as it is produced; safe to stream to the writer.
    Streaming,
    /// Requires buffering rows before emitting (e.g. shuffles, global sorts).
    /// The spooling stage (Task 22) keys off this.
    Buffered,
}

/// Whether an operator can verify its own output against the model's
/// expectations (uniqueness, referential integrity, value distributions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// The operator provides no verification hook.
    Unsupported,
    /// The operator can verify generated values against its constraints.
    Supported,
}

/// Static description of a registered generator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratorDescriptor {
    /// Canonical registry name, e.g. `"constant"`.
    pub kind: &'static str,
    /// Alternate names that resolve to this generator, e.g. `["const"]`.
    pub aliases: &'static [&'static str],
    /// One-line human summary.
    pub summary: &'static str,
    /// The config arguments this generator understands.
    pub arguments: &'static [ArgumentSpec],
    /// Column type families this generator can populate.
    pub accepts: &'static [SqlTypeFamily],
    /// Which columns the generator produces.
    pub writes: ColumnScope,
    /// Which sibling columns the generator reads.
    pub reads: ColumnScope,
    /// Whether output is reproducible from the seed.
    pub determinism: Determinism,
    /// Whether the generator can stream or must buffer.
    pub buffering: Buffering,
    /// Whether the generator can verify its own output.
    pub verification: Verification,
}

/// Static description of a registered modifier. Mirrors
/// [`GeneratorDescriptor`], but `accepts` lists the families the modifier can
/// *transform* rather than populate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifierDescriptor {
    /// Canonical registry name, e.g. `"nullable"`.
    pub kind: &'static str,
    /// Alternate names that resolve to this modifier.
    pub aliases: &'static [&'static str],
    /// One-line human summary.
    pub summary: &'static str,
    /// The config arguments this modifier understands.
    pub arguments: &'static [ArgumentSpec],
    /// Column type families this modifier can transform.
    pub accepts: &'static [SqlTypeFamily],
    /// Which columns the modifier writes (typically its own).
    pub writes: ColumnScope,
    /// Which sibling columns the modifier reads.
    pub reads: ColumnScope,
    /// Whether output is reproducible from the seed.
    pub determinism: Determinism,
    /// Whether the modifier can stream or must buffer.
    pub buffering: Buffering,
    /// Whether the modifier can verify its own output.
    pub verification: Verification,
}

/// Static description of a registered planner. Planners operate at table
/// scope rather than on a single column's type, so there is no `accepts`
/// family list; `writes`/`reads` describe the columns the planner coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerDescriptor {
    /// Canonical registry name, e.g. `"relation.children"`.
    pub kind: &'static str,
    /// Alternate names that resolve to this planner.
    pub aliases: &'static [&'static str],
    /// One-line human summary.
    pub summary: &'static str,
    /// The config arguments this planner understands.
    pub arguments: &'static [ArgumentSpec],
    /// Which columns the planner produces (named in its config).
    pub writes: ColumnScope,
    /// Which columns the planner reads (named in its config).
    pub reads: ColumnScope,
    /// Whether the plan is reproducible from the seed.
    pub determinism: Determinism,
    /// Whether the planner can stream or must buffer.
    pub buffering: Buffering,
    /// Whether the planner can verify its own output.
    pub verification: Verification,
}

// --- Compile- and row-time contexts ----------------------------------------

/// Everything a factory needs to `compile` a config into a runtime operator:
/// the table and (for column-scoped operators) the column being compiled, the
/// run's [`SeedRoot`], and the config's YAML path for diagnostics.
///
/// It is `Copy` so factories can freely pass it down without ceremony.
#[derive(Debug, Clone, Copy)]
pub struct CompileContext<'a> {
    table: &'a PortableTable,
    column: Option<&'a PortableColumn>,
    seed: SeedRoot,
    path: &'a str,
}

impl<'a> CompileContext<'a> {
    /// Context for a column-scoped operator (generator or modifier).
    pub fn for_column(
        table: &'a PortableTable,
        column: &'a PortableColumn,
        seed: SeedRoot,
        path: &'a str,
    ) -> Self {
        Self {
            table,
            column: Some(column),
            seed,
            path,
        }
    }

    /// Context for a table-scoped operator (planner), where no single column
    /// owns the compilation.
    pub fn for_table(table: &'a PortableTable, seed: SeedRoot, path: &'a str) -> Self {
        Self {
            table,
            column: None,
            seed,
            path,
        }
    }

    /// The table this operator belongs to.
    pub fn table(&self) -> &'a PortableTable {
        self.table
    }

    /// The column being compiled, if this is a column-scoped operator.
    pub fn column(&self) -> Option<&'a PortableColumn> {
        self.column
    }

    /// The run's root seed, for deriving deterministic RNG streams.
    pub fn seed(&self) -> SeedRoot {
        self.seed
    }

    /// The config's YAML path, for attaching diagnostics to the right place.
    pub fn path(&self) -> &'a str {
        self.path
    }

    /// Derive the deterministic RNG stream for the given identity. Convenience
    /// over `self.seed().stream(id)`.
    pub fn rng(&self, id: StreamId) -> ChaCha8Rng {
        self.seed.stream(id)
    }
}

/// Read access to the other columns of the row currently being generated.
///
/// The engine owns the row buffer; generators and modifiers only borrow it
/// through this trait, so a compiled operator can read a sibling column's
/// value (e.g. deriving `full_name` from `first_name` + `last_name`) without
/// depending on the concrete buffer type.
pub trait RowView {
    /// The value already generated for `column` in this row, if any. Returns
    /// `None` for columns not yet produced or not present.
    fn get(&self, column: &str) -> Option<&GeneratedValue>;
}

/// Per-row context handed to a compiled generator or modifier.
///
/// A compiled operator owns its own per-operator RNG (seeded from
/// [`CompileContext`] at compile time and advanced sequentially per row), so
/// `RowContext` deliberately carries no RNG: it exposes the row index and
/// read-only access to sibling column values, which is all the row-time
/// dependencies a value operator has today. Task 22's spooling can extend it.
pub struct RowContext<'a> {
    row_index: u64,
    columns: &'a dyn RowView,
}

impl<'a> RowContext<'a> {
    /// Build a context for the row at `row_index`, reading siblings from
    /// `columns`.
    pub fn new(row_index: u64, columns: &'a dyn RowView) -> Self {
        Self { row_index, columns }
    }

    /// The zero-based index of the row being generated.
    pub fn row_index(&self) -> u64 {
        self.row_index
    }

    /// The value already generated for a sibling `column` in this row.
    pub fn column(&self, column: &str) -> Option<&GeneratedValue> {
        self.columns.get(column)
    }
}

/// Context handed to a compiled planner. Planners run once per table before
/// any row is generated, so they see the whole table and the run seed.
///
/// The planner *runtime surface* (row-count resolution, parent/child
/// fan-out, spooling hooks) is owned by Task 22; this type is the stable
/// extension point that work hangs off.
#[derive(Debug, Clone, Copy)]
pub struct PlanContext<'a> {
    table: &'a PortableTable,
    seed: SeedRoot,
}

impl<'a> PlanContext<'a> {
    /// Build a planning context for `table` under the run's `seed`.
    pub fn new(table: &'a PortableTable, seed: SeedRoot) -> Self {
        Self { table, seed }
    }

    /// The table being planned.
    pub fn table(&self) -> &'a PortableTable {
        self.table
    }

    /// Derive the deterministic RNG stream for the given identity.
    pub fn rng(&self, id: StreamId) -> ChaCha8Rng {
        self.seed.stream(id)
    }
}

// --- Factory and runtime traits --------------------------------------------

/// Compiles a [`GeneratorConfig`] into a runnable generator.
pub trait GeneratorFactory: Send + Sync {
    /// This generator's static description.
    fn descriptor(&self) -> &'static GeneratorDescriptor;

    /// Validate `config` and build a runtime generator, or return a
    /// [`DiagnosticBag`] describing every problem found.
    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag>;
}

/// How a generator materializes as a random-access key domain when its column
/// is a primary key referenced by a child foreign key. Context-free: the engine
/// supplies the parent table/column/seed. A generator that is not a
/// reproducible random-access key returns `None` from
/// [`CompiledGenerator::key_recipe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRecipe {
    /// A dense integer sequence: parent row `n`'s key is `start + n * step`.
    Dense {
        /// The key of parent row 0.
        start: i128,
        /// The per-row increment.
        step: i128,
    },
    /// A per-row-reseeded random UUID (RFC 4122 v4).
    Uuid,
}

/// A compiled generator: produces one value per row into a caller-owned slot.
pub trait CompiledGenerator: Send {
    /// Overwrite `output` with the value for the current row. Overwriting
    /// (rather than returning) lets the engine reuse the allocation across
    /// rows.
    fn generate(
        &mut self,
        context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError>;

    /// How this generator materializes as a random-access key when its column
    /// is a primary key referenced by a child foreign key. The default is
    /// `None` (not a reproducible random-access key — the engine then reports
    /// `GEN-KEY-DOMAIN-UNSUPPORTED`). A generator whose row `n` value is a
    /// closed-form or per-row-reseeded function of `n` overrides this so the
    /// engine can regenerate any parent row's key by index.
    fn key_recipe(&self) -> Option<KeyRecipe> {
        None
    }
}

/// Compiles a [`ModifierConfig`] into a runnable modifier.
pub trait ModifierFactory: Send + Sync {
    /// This modifier's static description.
    fn descriptor(&self) -> &'static ModifierDescriptor;

    /// Validate `config` and build a runtime modifier, or return a
    /// [`DiagnosticBag`] describing every problem found.
    fn compile(
        &self,
        config: &ModifierConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledModifier>, DiagnosticBag>;
}

/// A compiled modifier: transforms a value in place after generation.
pub trait CompiledModifier: Send {
    /// Transform `value` for the current row in place.
    fn apply(
        &mut self,
        context: &RowContext<'_>,
        value: &mut GeneratedValue,
    ) -> Result<(), GenerateError>;
}

/// Compiles a [`PlannerConfig`] into a runnable planner.
pub trait PlannerFactory: Send + Sync {
    /// This planner's static description.
    fn descriptor(&self) -> &'static PlannerDescriptor;

    /// Validate `config` and build a runtime planner, or return a
    /// [`DiagnosticBag`] describing every problem found.
    fn compile(
        &self,
        config: &PlannerConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag>;
}

/// A compiled planner: coordinates several columns of a table together.
///
/// Planners split their work across two phases. [`plan`](Self::plan) runs once
/// per table before any row (the table-scope extension point Task 22 hangs
/// spooling off). [`generate_row`](Self::generate_row) runs once per row in the
/// engine's row pipeline and *produces* the values of the columns the planner
/// owns — a planner writes several correlated columns together (e.g. an
/// interval's `start`/`end`/`duration`/`open`) rather than one column at a
/// time. The engine maps [`writes`](Self::writes) to row-buffer slots once per
/// table, then scatters each produced value into place; a planner-owned column
/// therefore takes its value from the planner, not from a generator.
pub trait CompiledPlanner: Send {
    /// Contribute this planner's table-level decisions for `context`. Runs once
    /// per table before any row is generated. Default: nothing to do.
    fn plan(&mut self, _context: &PlanContext<'_>) -> Result<(), GenerateError> {
        Ok(())
    }

    /// The columns this planner produces, in the positional order
    /// [`generate_row`](Self::generate_row) writes them. The engine resolves
    /// each name to a row-buffer slot once per table. Default: none (a planner
    /// that contributes only structural decisions, not column values).
    fn writes(&self) -> &[String] {
        &[]
    }

    /// Produce this planner's owned column values for the row at `row_index`,
    /// writing them into `output` positionally aligned with
    /// [`writes`](Self::writes). Runs once per row, before column modifiers and
    /// the sink. Default: no-op (nothing owned).
    fn generate_row(
        &mut self,
        _row_index: u64,
        _output: &mut [GeneratedValue],
    ) -> Result<(), GenerateError> {
        Ok(())
    }

    /// Machine-checkable invariants this planner guarantees over its owned
    /// columns, surfaced so the verification stage (Task 26) can assert them
    /// without knowing the planner's internals. Default: none.
    fn verification_predicates(&self) -> Vec<PlannerPredicate> {
        Vec::new()
    }

    /// The name of the child table this planner coordinates as a cross-table
    /// family, if any. A same-table planner returns `None`; a family planner
    /// (e.g. `commerce.order_family`) names the child table whose rows it
    /// produces alongside its parent columns. The engine spools those child
    /// rows and renders them at the child table's dependency position.
    fn family_child_table(&self) -> Option<&str> {
        None
    }

    /// The child-table relationship (declared on the child) that carries the
    /// foreign key back to this planner's parent, so the engine sets each
    /// spooled child row's FK to the exact parent that produced it. Only
    /// meaningful when [`family_child_table`](Self::family_child_table) is
    /// `Some`.
    fn family_relationship(&self) -> Option<&str> {
        None
    }

    /// The child columns this planner produces, in the positional order of the
    /// per-child value vectors returned by
    /// [`take_family_children`](Self::take_family_children). Only meaningful for
    /// a family planner. Default: none.
    fn child_writes(&self) -> &[String] {
        &[]
    }

    /// Take the child rows produced by the most recent
    /// [`generate_row`](Self::generate_row) call — one value vector per child
    /// line, aligned with [`child_writes`](Self::child_writes). The engine calls
    /// this once per parent row for a family planner and spools the rows.
    /// Default: none.
    fn take_family_children(&mut self) -> Vec<Vec<GeneratedValue>> {
        Vec::new()
    }

    /// Cross-table sum invariants this family planner guarantees between a
    /// parent aggregate column and the child rows that reference each parent
    /// row. Same-table planners (whose invariants are stated as
    /// [`verification_predicates`](Self::verification_predicates)) return none;
    /// a family planner (e.g. `commerce.order_family`) surfaces the equalities
    /// the verification stage (Task 26) checks by grouping child rows on the
    /// declared [`family_relationship`](Self::family_relationship) foreign key.
    /// Default: none.
    fn family_sum_checks(&self) -> Vec<FamilySumCheck> {
        Vec::new()
    }
}

/// A cross-table sum invariant: over the child rows of the
/// [`CompiledPlanner::family_child_table`] that reference a given parent row,
/// the integer value of `child_column` sums exactly to the integer value of the
/// parent's `parent_column`. Money columns are compared in currency minor units,
/// so a verifier normalizes decimal/text renderings to the same fixed scale
/// before summing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilySumCheck {
    /// The named relationship (declared on the child table) whose foreign key
    /// groups child rows under their parent — the same name
    /// [`CompiledPlanner::family_relationship`] returns.
    pub relationship: String,
    /// The parent aggregate column the child values must sum to.
    pub parent_column: String,
    /// The child column summed across every child of a parent row.
    pub child_column: String,
}

/// A machine-checkable invariant a planner guarantees over the columns it owns.
///
/// Predicates are stated over column *names* and integer units so a verifier
/// (Task 26) can evaluate them directly against generated rows, without
/// re-deriving the planner's logic. Timestamp columns are compared as
/// nanoseconds since the Unix epoch; a duration column is an integer count of
/// `duration_unit_nanos`-sized units.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerPredicate {
    /// On every row the `guard` selects (or every row when `guard` is `None`),
    /// the `end` timestamp is a checked-integer function of `start` and
    /// `duration`. The exact relation depends on `end_inclusive`:
    ///
    /// * `end_inclusive == false` (half-open `[start, end)`):
    ///   `end_ns == start_ns + duration * duration_unit_nanos`.
    /// * `end_inclusive == true` (closed `[start, end]`, `end` the last covered
    ///   instant): `end_ns == start_ns + duration * duration_unit_nanos - 1`
    ///   (one nanosecond, the smallest internal unit).
    ///
    /// The two modes are therefore *not* the same predicate: a verifier must
    /// branch on `end_inclusive` and check the exact produced value.
    Equation {
        /// The start timestamp column.
        start: String,
        /// The end timestamp column.
        end: String,
        /// The duration column, an integer count of `duration_unit_nanos` units.
        duration: String,
        /// Nanoseconds per duration unit (e.g. `1_000_000_000` for seconds).
        duration_unit_nanos: i128,
        /// Whether `end` denotes the inclusive last instant of the interval.
        end_inclusive: bool,
        /// The rows the equation applies to; `None` means every row.
        guard: Option<PredicateGuard>,
    },
    /// On every row the `guard` selects, `column` is SQL `NULL`.
    NullWhen {
        /// The column asserted to be `NULL`.
        column: String,
        /// The rows the assertion applies to.
        guard: PredicateGuard,
    },
    /// `column`, compared as epoch nanoseconds, lies within the inclusive
    /// bounds `[min_nanos, max_nanos]` on every non-null row.
    InRange {
        /// The timestamp column bounded.
        column: String,
        /// Inclusive lower bound, epoch nanoseconds.
        min_nanos: i128,
        /// Inclusive upper bound, epoch nanoseconds.
        max_nanos: i128,
    },
    /// On every row the `guard` selects (or every row when `guard` is `None`),
    /// the integer values of `addends` sum exactly to the integer value of
    /// `sum`. Counter planners state their partition equations this way, e.g.
    /// `succeeded + failed == processed` and `processed + pending == total`. A
    /// single-element `addends` therefore asserts a plain equality between two
    /// counter columns (e.g. `processed == total` on completed rows).
    CounterSum {
        /// The integer columns summed on the left-hand side.
        addends: Vec<String>,
        /// The integer column the addends must sum to.
        sum: String,
        /// The rows the equation applies to; `None` means every row.
        guard: Option<PredicateGuard>,
    },
    /// Every listed integer `columns` value is `>= 0` on every row. Counter
    /// planners assert their counters never go negative.
    NonNegative {
        /// The integer columns constrained to be non-negative.
        columns: Vec<String>,
    },
    /// On every row the `guard` selects, `column` is non-`NULL` (the dual of
    /// [`NullWhen`](Self::NullWhen)). Counter planners assert a completed row
    /// carries a completion timestamp.
    NotNullWhen {
        /// The column asserted to be non-`NULL`.
        column: String,
        /// The rows the assertion applies to.
        guard: PredicateGuard,
    },
}

/// A row-level condition selecting which rows a [`PlannerPredicate`] applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateGuard {
    /// Rows where the boolean `column` equals `value`.
    Flag {
        /// The boolean flag column tested.
        column: String,
        /// The value the flag must equal.
        value: bool,
    },
    /// Rows where `column` is `NULL` (`is_null`) or non-`NULL` (`!is_null`).
    Null {
        /// The column tested for nullness.
        column: String,
        /// Whether the guard selects null (`true`) or non-null (`false`) rows.
        is_null: bool,
    },
    /// Rows where the text `column` equals `value`. Counter planners guard
    /// state constraints on the workflow status column (e.g. rows whose
    /// `status` is a particular completed status).
    Equals {
        /// The text column tested.
        column: String,
        /// The value the column must equal.
        value: String,
    },
}

// --- Catalog ----------------------------------------------------------------

/// The `(kind, aliases)` a catalog needs to index any factory, regardless of
/// role. Implemented for each factory trait object so [`Catalog`] can be
/// generic over all three roles.
trait FactoryMeta {
    fn kind(&self) -> &'static str;
    fn aliases(&self) -> &'static [&'static str];
}

impl FactoryMeta for dyn GeneratorFactory {
    fn kind(&self) -> &'static str {
        self.descriptor().kind
    }
    fn aliases(&self) -> &'static [&'static str] {
        self.descriptor().aliases
    }
}

impl FactoryMeta for dyn ModifierFactory {
    fn kind(&self) -> &'static str {
        self.descriptor().kind
    }
    fn aliases(&self) -> &'static [&'static str] {
        self.descriptor().aliases
    }
}

impl FactoryMeta for dyn PlannerFactory {
    fn kind(&self) -> &'static str {
        self.descriptor().kind
    }
    fn aliases(&self) -> &'static [&'static str] {
        self.descriptor().aliases
    }
}

/// One role's worth of registered factories: a deterministic primary-kind map
/// plus an alias→kind map. Generic over the factory trait object so all three
/// roles share the same registration and lookup rules.
struct Catalog<F: ?Sized> {
    /// Human label for diagnostics, e.g. `"generator"`.
    label: &'static str,
    /// Primary kind → factory. `BTreeMap` gives deterministic iteration.
    primary: BTreeMap<String, Box<F>>,
    /// Alias → primary kind. Kept disjoint from `primary`'s keys.
    aliases: BTreeMap<String, String>,
}

impl<F: ?Sized + FactoryMeta> Catalog<F> {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            primary: BTreeMap::new(),
            aliases: BTreeMap::new(),
        }
    }

    /// Register `factory`, validating that its kind and aliases collide with
    /// nothing already installed. All problems are collected before returning
    /// so a factory with several bad aliases reports them all at once, and
    /// nothing is inserted unless validation passes.
    fn register(&mut self, factory: Box<F>) -> Result<(), DiagnosticBag> {
        let kind = factory.kind();
        let aliases = factory.aliases();
        let mut bag = DiagnosticBag::default();

        if self.primary.contains_key(kind) {
            bag.error(
                "GEN-REGISTRY-DUPLICATE",
                format!("registry.{}s.{kind}", self.label),
                format!("duplicate {} kind `{kind}`", self.label),
            );
        } else if self.aliases.contains_key(kind) {
            bag.error(
                "GEN-REGISTRY-DUPLICATE",
                format!("registry.{}s.{kind}", self.label),
                format!(
                    "{} kind `{kind}` is already registered as an alias",
                    self.label
                ),
            );
        }

        let mut seen = BTreeMap::new();
        for &alias in aliases {
            let path = format!("registry.{}s.{kind}.aliases.{alias}", self.label);
            if alias == kind {
                bag.error(
                    "GEN-REGISTRY-ALIAS-DUPLICATE",
                    path,
                    format!("alias `{alias}` duplicates its own kind"),
                );
            } else if self.primary.contains_key(alias) {
                bag.error(
                    "GEN-REGISTRY-ALIAS-SHADOWS-KIND",
                    path,
                    format!("alias `{alias}` shadows an existing {} kind", self.label),
                );
            } else if self.aliases.contains_key(alias) || seen.insert(alias, ()).is_some() {
                bag.error(
                    "GEN-REGISTRY-ALIAS-DUPLICATE",
                    path,
                    format!("duplicate alias `{alias}`"),
                );
            }
        }

        bag.into_result(())?;

        for &alias in aliases {
            self.aliases.insert(alias.to_string(), kind.to_string());
        }
        self.primary.insert(kind.to_string(), factory);
        Ok(())
    }

    /// Resolve `name` as a primary kind first, then as an alias.
    fn get(&self, name: &str) -> Option<&F> {
        if let Some(factory) = self.primary.get(name) {
            return Some(factory.as_ref());
        }
        let kind = self.aliases.get(name)?;
        self.primary.get(kind).map(Box::as_ref)
    }

    /// Iterate factories in deterministic primary-kind order.
    fn iter(&self) -> impl Iterator<Item = &F> + '_ {
        self.primary.values().map(Box::as_ref)
    }
}

// --- ExtensionRegistry ------------------------------------------------------

/// The registry of all generator, modifier, and planner factories available
/// to a compilation. Build an empty one with [`ExtensionRegistry::new`] or the
/// phase's built-in set with [`ExtensionRegistry::standard`].
pub struct ExtensionRegistry {
    generators: Catalog<dyn GeneratorFactory>,
    modifiers: Catalog<dyn ModifierFactory>,
    planners: Catalog<dyn PlannerFactory>,
}

impl ExtensionRegistry {
    /// An empty registry with no factories installed.
    pub fn new() -> Self {
        Self {
            generators: Catalog::new("generator"),
            modifiers: Catalog::new("modifier"),
            planners: Catalog::new("planner"),
        }
    }

    /// A registry preloaded with every factory implemented so far: Task 7's
    /// `constant` exemplar plus the full Phase 1 core catalog (Task 11). Later
    /// phases (Tasks 12/22) register their own catalogs here in turn.
    pub fn standard() -> Self {
        let mut registry = Self::new();
        registry
            .register_generator(Box::new(super::generators::ConstantFactory))
            .expect("built-in generator kinds are collision-free");
        super::generators::register_all(&mut registry);
        super::generators::semantic::register_all(&mut registry);
        super::generators::observed::register_all(&mut registry);
        registry
            .register_generator(Box::new(super::generators::relation::ForeignKeyFactory))
            .expect("built-in generator kinds are collision-free");
        registry
            .register_generator(Box::new(super::generators::relation::CompositeKeyFactory))
            .expect("built-in generator kinds are collision-free");
        registry
            .register_planner(Box::new(super::planners::TemporalIntervalFactory))
            .expect("built-in planner kinds are collision-free");
        registry
            .register_planner(Box::new(super::planners::ProgressCountersFactory))
            .expect("built-in planner kinds are collision-free");
        registry
            .register_planner(Box::new(super::planners::OrderFamilyFactory))
            .expect("built-in planner kinds are collision-free");
        registry
    }

    /// Register a generator factory.
    pub fn register_generator(
        &mut self,
        factory: Box<dyn GeneratorFactory>,
    ) -> Result<(), DiagnosticBag> {
        self.generators.register(factory)
    }

    /// Register a modifier factory.
    pub fn register_modifier(
        &mut self,
        factory: Box<dyn ModifierFactory>,
    ) -> Result<(), DiagnosticBag> {
        self.modifiers.register(factory)
    }

    /// Register a planner factory.
    pub fn register_planner(
        &mut self,
        factory: Box<dyn PlannerFactory>,
    ) -> Result<(), DiagnosticBag> {
        self.planners.register(factory)
    }

    /// Resolve a generator by primary kind or alias.
    pub fn generator(&self, name: &str) -> Option<&dyn GeneratorFactory> {
        self.generators.get(name)
    }

    /// Resolve a modifier by primary kind or alias.
    pub fn modifier(&self, name: &str) -> Option<&dyn ModifierFactory> {
        self.modifiers.get(name)
    }

    /// Resolve a planner by primary kind or alias.
    pub fn planner(&self, name: &str) -> Option<&dyn PlannerFactory> {
        self.planners.get(name)
    }

    /// Iterate generator factories in deterministic primary-kind order.
    pub fn generators(&self) -> impl Iterator<Item = &(dyn GeneratorFactory + 'static)> + '_ {
        self.generators.iter()
    }

    /// Iterate modifier factories in deterministic primary-kind order.
    pub fn modifiers(&self) -> impl Iterator<Item = &(dyn ModifierFactory + 'static)> + '_ {
        self.modifiers.iter()
    }

    /// Iterate planner factories in deterministic primary-kind order.
    pub fn planners(&self) -> impl Iterator<Item = &(dyn PlannerFactory + 'static)> + '_ {
        self.planners.iter()
    }
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
