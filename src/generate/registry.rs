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

/// A compiled planner: contributes table-level structural decisions.
///
/// The concrete planning behavior is defined by Task 22; this method is the
/// stable extension point compiled planners hang that behavior off.
pub trait CompiledPlanner: Send {
    /// Contribute this planner's decisions for the table in `context`.
    fn plan(&mut self, context: &PlanContext<'_>) -> Result<(), GenerateError>;
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

    /// A registry preloaded with every factory implemented so far. As later
    /// phases land their catalogs (Tasks 11/12/22), they register here.
    pub fn standard() -> Self {
        let mut registry = Self::new();
        registry
            .register_generator(Box::new(super::generators::ConstantFactory))
            .expect("built-in generator kinds are collision-free");
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
