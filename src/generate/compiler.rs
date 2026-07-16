//! The [`ModelCompiler`]: turns a complete model plus CLI options into an
//! immutable [`GenerationPlan`].
//!
//! Compilation is two concerns woven together:
//!
//! * **Selection** — `--tables`/`--exclude` globs choose the tables to
//!   generate; a selected table whose required parent was excluded is a
//!   `GEN-EXCLUDED-DEPENDENCY` error reporting the full dependency path.
//! * **Exact counts** — every selected table gets a single resolved integer
//!   row count. Roots resolve from their model count through the global
//!   `--scale`/`--rows` control, a per-table `--table-rows`/`--table-scale`
//!   override, and finally the `--max-rows` cap. Relationship children derive
//!   from their *final* parent count times the stored fan-out mean — the
//!   global control is never applied a second time — then take their own
//!   override and cap. Fractional targets are stochastically rounded on a
//!   stable per-table [`StreamId::rounding`] stream, so a run is reproducible.
//!
//! Errors are gathered, not short-circuited: a model with a count conflict and
//! three excluded dependencies reports all four. Warnings (e.g. a `--max-rows`
//! cap) survive a successful compile via [`GenerationPlan::diagnostics`].

use std::collections::{BTreeMap, BTreeSet};

use glob::Pattern;
use rand::RngExt;

use crate::diagnostic::{DiagnosticBag, Severity};
use crate::synthetic::model::{
    ChildDistribution, RelationshipModel, RowsModel, SyntheticModel, TableModel, TableSeed,
};

use super::plan::{
    ColumnOwner, CompiledOutput, CompiledRelationship, ExecutionPhase, GenerationPlan,
    PlanEstimates, PlannedColumn, PlannedTable, ResolvedTableSeed,
};
use super::registry::ExtensionRegistry;
use super::seed::{SeedRoot, StreamId};

/// A single per-table count override from the CLI (`--table-rows` or
/// `--table-scale`).
#[derive(Debug, Clone, PartialEq)]
pub struct TableCountOverride {
    /// The table the override targets.
    pub table: String,
    /// Whether it pins an absolute count or a table-local scale.
    pub kind: TableCountKind,
}

/// The two shapes of a per-table count override.
#[derive(Debug, Clone, PartialEq)]
pub enum TableCountKind {
    /// `--table-rows`: an absolute row count that is never re-scaled.
    Rows(u64),
    /// `--table-scale`: a table-local scale that replaces the global control
    /// on a root, or multiplies the derived count on a child.
    Scale(f64),
}

impl TableCountOverride {
    /// A `--table-rows` override pinning `table` to an absolute count.
    pub fn rows(table: impl Into<String>, rows: u64) -> Self {
        Self {
            table: table.into(),
            kind: TableCountKind::Rows(rows),
        }
    }

    /// A `--table-scale` override applying `scale` to `table`.
    pub fn scale(table: impl Into<String>, scale: f64) -> Self {
        Self {
            table: table.into(),
            kind: TableCountKind::Scale(scale),
        }
    }
}

/// The CLI-supplied knobs a compile honors, mapping one-to-one onto the
/// generation command's count and selection flags.
#[derive(Debug, Clone, Default)]
pub struct CompileOptions {
    /// Run root seed (`--seed`); overrides the model's `seed`.
    pub seed: Option<u64>,
    /// Global multiplicative scale (`--scale`); conflicts with `rows`.
    pub scale: Option<f64>,
    /// Global absolute root count (`--rows`); conflicts with `scale`.
    pub rows: Option<u64>,
    /// Upper bound applied to every table last (`--max-rows`).
    pub max_rows: Option<u64>,
    /// Per-table count overrides (`--table-rows`/`--table-scale`).
    pub table_rows: Vec<TableCountOverride>,
    /// Table-selection globs (`--tables`); empty selects every table.
    pub tables: Vec<String>,
    /// Table-exclusion globs (`--exclude`).
    pub exclude: Vec<String>,
}

/// Compiles a [`SyntheticModel`] into a [`GenerationPlan`].
///
/// Holds the [`ExtensionRegistry`] that later stages (column generators,
/// planners) resolve against; Task 9 itself only reads structure and counts.
pub struct ModelCompiler {
    registry: ExtensionRegistry,
}

impl ModelCompiler {
    /// A compiler backed by an explicit registry.
    pub fn new(registry: ExtensionRegistry) -> Self {
        Self { registry }
    }

    /// A compiler backed by [`ExtensionRegistry::standard`].
    pub fn standard() -> Self {
        Self::new(ExtensionRegistry::standard())
    }

    /// The registry this compiler resolves operators against.
    pub fn registry(&self) -> &ExtensionRegistry {
        &self.registry
    }

    /// Compile `model` under `options`, gathering every independent error
    /// before returning `Err`. On success, warnings survive in
    /// [`GenerationPlan::diagnostics`].
    pub fn compile(
        &self,
        model: SyntheticModel,
        options: CompileOptions,
    ) -> Result<GenerationPlan, DiagnosticBag> {
        let mut bag = DiagnosticBag::default();

        if options.scale.is_some() && options.rows.is_some() {
            bag.error(
                "GEN-COUNT-CONTROL-CONFLICT",
                "options",
                "`--scale` and `--rows` are mutually exclusive global row controls",
            );
        }

        let overrides = self.collect_table_overrides(&options, &mut bag);
        let selected = self.select_tables(&model, &options, &mut bag);
        let parents = required_parents(&model);
        self.report_excluded_dependencies(&selected, &parents, &mut bag);

        let root_seed = options.seed.or(model.seed);
        let rounding_root = SeedRoot::new(root_seed.unwrap_or(0));
        let order = topo_order(&selected, &parents);

        let mut resolved: BTreeMap<String, u64> = BTreeMap::new();
        let mut tables: Vec<PlannedTable> = Vec::with_capacity(order.len());
        let mut phases: Vec<ExecutionPhase> = Vec::with_capacity(order.len());

        for name in &order {
            let table = &model.tables[name];
            let count = self.resolve_count(
                &model,
                name,
                table,
                &options,
                overrides.get(name),
                rounding_root,
                &resolved,
                &mut bag,
            );
            resolved.insert(name.clone(), count);
            tables.push(build_planned_table(name, table, count, root_seed));
            phases.push(ExecutionPhase::Table(name.clone()));
        }

        let estimates = PlanEstimates {
            total_rows: resolved.values().sum(),
            ..PlanEstimates::default()
        };

        if bag.has_errors() {
            return Err(bag);
        }

        // Drain warnings so they reach the CLI/JSON report even though the
        // compile succeeded. (A merge, when wired ahead of the compiler, feeds
        // its warning-only bag through this same channel.)
        let diagnostics = bag
            .diagnostics
            .into_iter()
            .filter(|diagnostic| diagnostic.severity == Severity::Warning)
            .collect();

        Ok(GenerationPlan {
            input_dialect: model
                .source
                .as_ref()
                .and_then(|source| source.dialect.parse().ok()),
            output: compile_output(&model),
            tables,
            phases,
            diagnostics,
            estimates,
        })
    }

    /// Fold the flat `--table-rows`/`--table-scale` list into a per-table
    /// view, reporting `--table-rows` + `--table-scale` on the same table as
    /// `GEN-TABLE-COUNT-CONFLICT`.
    fn collect_table_overrides(
        &self,
        options: &CompileOptions,
        bag: &mut DiagnosticBag,
    ) -> BTreeMap<String, TableOverride> {
        let mut map: BTreeMap<String, TableOverride> = BTreeMap::new();
        for over in &options.table_rows {
            let entry = map.entry(over.table.clone()).or_default();
            match over.kind {
                TableCountKind::Rows(rows) => entry.rows = Some(rows),
                TableCountKind::Scale(scale) => entry.scale = Some(scale),
            }
        }
        for (table, over) in &map {
            if over.rows.is_some() && over.scale.is_some() {
                bag.error(
                    "GEN-TABLE-COUNT-CONFLICT",
                    format!("tables.{table}"),
                    format!(
                        "table `{table}` has both `--table-rows` and `--table-scale`; they are mutually exclusive"
                    ),
                );
            }
        }
        map
    }

    /// Apply the `--tables`/`--exclude` globs to choose the generated set.
    fn select_tables(
        &self,
        model: &SyntheticModel,
        options: &CompileOptions,
        bag: &mut DiagnosticBag,
    ) -> BTreeSet<String> {
        let includes = compile_globs(&options.tables, bag);
        let excludes = compile_globs(&options.exclude, bag);
        model
            .tables
            .keys()
            .filter(|name| {
                let included =
                    includes.is_empty() || includes.iter().any(|pattern| pattern.matches(name));
                let excluded = excludes.iter().any(|pattern| pattern.matches(name));
                included && !excluded
            })
            .cloned()
            .collect()
    }

    /// Report, for each selected table, any required dependency that is not in
    /// the selected set — with the full path from the table to the excluded
    /// dependency. Never re-adds an excluded table.
    fn report_excluded_dependencies(
        &self,
        selected: &BTreeSet<String>,
        parents: &BTreeMap<String, BTreeSet<String>>,
        bag: &mut DiagnosticBag,
    ) {
        for start in selected {
            let mut reported: BTreeSet<String> = BTreeSet::new();
            let mut visited: BTreeSet<String> = BTreeSet::new();
            let mut stack: Vec<Vec<String>> = vec![vec![start.clone()]];
            while let Some(path) = stack.pop() {
                let node = path.last().expect("path is never empty");
                let Some(node_parents) = parents.get(node) else {
                    continue;
                };
                for parent in node_parents {
                    let mut extended = path.clone();
                    extended.push(parent.clone());
                    if selected.contains(parent) {
                        if visited.insert(parent.clone()) {
                            stack.push(extended);
                        }
                    } else if reported.insert(parent.clone()) {
                        bag.error(
                            "GEN-EXCLUDED-DEPENDENCY",
                            format!("tables.{start}"),
                            format!(
                                "table `{start}` requires excluded table `{parent}` via dependency path {}",
                                extended.join(" -> ")
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Resolve one table's exact row count, appending any diagnostics.
    #[allow(clippy::too_many_arguments)]
    fn resolve_count(
        &self,
        model: &SyntheticModel,
        name: &str,
        table: &TableModel,
        options: &CompileOptions,
        over: Option<&TableOverride>,
        rounding_root: SeedRoot,
        resolved: &BTreeMap<String, u64>,
        bag: &mut DiagnosticBag,
    ) -> u64 {
        match &table.rows {
            RowsModel::RelationChildren {
                parent,
                distribution,
                ..
            } => self.resolve_child_count(
                name,
                parent,
                distribution,
                options,
                over,
                rounding_root,
                resolved,
                bag,
            ),
            rows => self.resolve_root_count(model, name, rows, options, over, rounding_root, bag),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_root_count(
        &self,
        model: &SyntheticModel,
        name: &str,
        rows: &RowsModel,
        options: &CompileOptions,
        over: Option<&TableOverride>,
        rounding_root: SeedRoot,
        bag: &mut DiagnosticBag,
    ) -> u64 {
        if matches!(rows, RowsModel::Observed { .. }) && !has_observed_provenance(model, name) {
            bag.error(
                "GEN-ROWS-OBSERVED-MISSING",
                format!("tables.{name}.rows"),
                format!(
                    "table `{name}` uses `rows.kind: observed` but the model has no attached source or profile to resolve the observed count from"
                ),
            );
        }

        let base = declared_count(rows).unwrap_or(0) as f64;

        // Global control (scale XOR rows); conflict already reported upstream.
        let mut target = match (options.scale, options.rows) {
            (Some(scale), _) => base * scale,
            (None, Some(rows)) => rows as f64,
            (None, None) => base,
        };

        // Root per-table override: absolute rows win outright; a table-scale
        // replaces the global control and re-scales the untouched base.
        match over.map(TableOverride::effective) {
            Some(EffectiveOverride::Rows(rows)) => target = rows as f64,
            Some(EffectiveOverride::Scale(scale)) => target = base * scale,
            None | Some(EffectiveOverride::Conflict) => {}
        }

        let count = stochastic_round(target, rounding_root, name);
        apply_max_rows(count, options.max_rows, name, bag)
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_child_count(
        &self,
        name: &str,
        parent: &str,
        distribution: &ChildDistribution,
        options: &CompileOptions,
        over: Option<&TableOverride>,
        rounding_root: SeedRoot,
        resolved: &BTreeMap<String, u64>,
        bag: &mut DiagnosticBag,
    ) -> u64 {
        // A missing parent means it was excluded/cyclic; that error is already
        // recorded, so resolve to zero and let the compile fail.
        let Some(&parent_count) = resolved.get(parent) else {
            return 0;
        };
        let (mean, min, _max) = distribution_bounds(distribution);

        // Children derive from the FINAL parent count and stored fan-out; the
        // global control is deliberately NOT reapplied (no double-scale).
        let derived = parent_count as f64 * mean;
        let target = match over.map(TableOverride::effective) {
            Some(EffectiveOverride::Rows(rows)) => rows as f64,
            Some(EffectiveOverride::Scale(scale)) => derived * scale,
            None | Some(EffectiveOverride::Conflict) => derived,
        };

        let count = stochastic_round(target, rounding_root, name);
        let count = apply_max_rows(count, options.max_rows, name, bag);

        let minimum = (parent_count as f64 * min).ceil() as u64;
        if count < minimum {
            bag.error(
                "GEN-CHILD-COUNT-IMPOSSIBLE",
                format!("tables.{name}.rows"),
                format!(
                    "table `{name}` resolves to {count} rows, but its {parent_count} parents in `{parent}` each require at least {min} child(ren) ({minimum} total)"
                ),
            );
        }
        count
    }
}

/// The per-table override view folded from the flat CLI list.
#[derive(Debug, Default, Clone, Copy)]
struct TableOverride {
    rows: Option<u64>,
    scale: Option<f64>,
}

/// The override actually applied once conflicts are accounted for.
enum EffectiveOverride {
    Rows(u64),
    Scale(f64),
    Conflict,
}

impl TableOverride {
    fn effective(&self) -> EffectiveOverride {
        match (self.rows, self.scale) {
            (Some(_), Some(_)) => EffectiveOverride::Conflict,
            (Some(rows), None) => EffectiveOverride::Rows(rows),
            (None, Some(scale)) => EffectiveOverride::Scale(scale),
            (None, None) => EffectiveOverride::Conflict,
        }
    }
}

/// Cap `count` at `max_rows` (the last step of count resolution), emitting a
/// surviving warning when the cap actually reduces the count.
fn apply_max_rows(count: u64, max_rows: Option<u64>, name: &str, bag: &mut DiagnosticBag) -> u64 {
    match max_rows {
        Some(max) if count > max => {
            bag.warning(
                "GEN-MAX-ROWS-CAPPED",
                format!("tables.{name}.rows"),
                format!("table `{name}` row count capped from {count} to {max} by `--max-rows`"),
            );
            max
        }
        _ => count,
    }
}

/// Stochastically round `value` to an integer on the table's stable
/// `rows.rounding` stream: round up with probability equal to the fraction.
fn stochastic_round(value: f64, root: SeedRoot, table: &str) -> u64 {
    if value <= 0.0 {
        return 0;
    }
    let floor = value.floor();
    let fraction = value - floor;
    let mut count = floor as u64;
    if fraction > 0.0 {
        let mut rng = root.stream(StreamId::rounding(table));
        if rng.random::<f64>() < fraction {
            count += 1;
        }
    }
    count
}

/// The declared row count for the count-carrying `RowsModel` variants;
/// `relation.children` carries no standalone count (it derives from a parent).
fn declared_count(rows: &RowsModel) -> Option<u64> {
    match rows {
        RowsModel::Fixed { count }
        | RowsModel::Observed { count }
        | RowsModel::Scale { count, .. } => Some(*count),
        RowsModel::RelationChildren { .. } => None,
    }
}

/// The `(mean, min, max)` fan-out bounds of any child distribution.
fn distribution_bounds(distribution: &ChildDistribution) -> (f64, f64, f64) {
    match distribution {
        ChildDistribution::Observed { mean, min, max }
        | ChildDistribution::Fixed { mean, min, max }
        | ChildDistribution::Uniform { mean, min, max }
        | ChildDistribution::Poisson { mean, min, max }
        | ChildDistribution::Histogram { mean, min, max } => (*mean, *min, *max),
    }
}

/// Whether an `observed` row count has a source or profile to resolve from.
fn has_observed_provenance(model: &SyntheticModel, table: &str) -> bool {
    model.source.is_some()
        || model
            .profiles
            .keys()
            .filter_map(|key| key.split_once('.'))
            .any(|(profiled_table, _)| profiled_table == table)
}

/// Required-parent edges per table: the `relation.children` parent plus every
/// declared relationship's referenced table.
fn required_parents(model: &SyntheticModel) -> BTreeMap<String, BTreeSet<String>> {
    model
        .tables
        .iter()
        .map(|(name, table)| {
            let mut parents: BTreeSet<String> = table
                .relationships
                .iter()
                .map(|relationship| relationship.references.table.clone())
                .collect();
            if let RowsModel::RelationChildren { parent, .. } = &table.rows {
                parents.insert(parent.clone());
            }
            // A table never depends on itself for ordering purposes.
            parents.remove(name);
            (name.clone(), parents)
        })
        .collect()
}

/// Topologically order the selected tables, parents before children, using
/// only edges between selected tables. Deterministic via sorted `BTreeMap`s;
/// any tables left over from a cycle are appended in name order.
fn topo_order(
    selected: &BTreeSet<String>,
    parents: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    let mut children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in selected {
        let selected_parents: Vec<&String> = parents
            .get(name)
            .into_iter()
            .flatten()
            .filter(|parent| selected.contains(*parent))
            .collect();
        in_degree.insert(name.clone(), selected_parents.len());
        for parent in selected_parents {
            children
                .entry(parent.clone())
                .or_default()
                .push(name.clone());
        }
    }

    // Ready tables are drained smallest-name-first. The queue is kept sorted
    // descending and popped from the back so the emitted order is stable and
    // alphabetical within each dependency level.
    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|(_, &degree)| degree == 0)
        .map(|(name, _)| name.clone())
        .collect();
    queue.sort();
    queue.reverse();
    let mut order: Vec<String> = Vec::with_capacity(selected.len());
    while let Some(name) = queue.pop() {
        order.push(name.clone());
        if let Some(kids) = children.get(&name) {
            for kid in kids {
                let degree = in_degree.get_mut(kid).expect("child has an in-degree");
                *degree -= 1;
                if *degree == 0 {
                    queue.push(kid.clone());
                }
            }
        }
        queue.sort();
        queue.reverse();
    }

    // Append any table trapped in a cycle so the plan still lists it.
    for name in selected {
        if !order.contains(name) {
            order.push(name.clone());
        }
    }
    order
}

/// Build a [`PlannedTable`] with resolved count, seed, unowned columns, and
/// compiled relationships. Planners stay empty until Task 22.
fn build_planned_table(
    name: &str,
    table: &TableModel,
    rows: u64,
    root_seed: Option<u64>,
) -> PlannedTable {
    let columns = table
        .schema
        .columns
        .iter()
        .map(|column| PlannedColumn {
            schema: column.clone(),
            owner: ColumnOwner::Unowned,
        })
        .collect();
    let relationships = table
        .relationships
        .iter()
        .map(compile_relationship)
        .collect();
    PlannedTable {
        name: name.to_string(),
        rows,
        schema: table.schema.clone(),
        seed: resolve_seed(&table.seed, root_seed),
        columns,
        relationships,
        planners: Vec::new(),
    }
}

/// Capture a declared relationship's referential shape. Task 13 completes it
/// with per-row parent assignment.
fn compile_relationship(relationship: &RelationshipModel) -> CompiledRelationship {
    CompiledRelationship {
        name: relationship.name.clone(),
        columns: relationship.columns.clone(),
        parent_table: relationship.references.table.clone(),
        parent_columns: relationship.references.columns.clone(),
    }
}

/// Resolve a table's seed against the run's root seed.
fn resolve_seed(seed: &TableSeed, root_seed: Option<u64>) -> ResolvedTableSeed {
    match seed {
        TableSeed::Inherit => match root_seed {
            Some(root) => ResolvedTableSeed::Inherited(SeedRoot::new(root)),
            None => ResolvedTableSeed::Random,
        },
        TableSeed::Random => ResolvedTableSeed::Random,
        TableSeed::Fixed(seed) => ResolvedTableSeed::Fixed(SeedRoot::new(*seed)),
    }
}

/// Carry the model's `output:` block into the plan, parsing the render dialect.
fn compile_output(model: &SyntheticModel) -> CompiledOutput {
    CompiledOutput {
        dialect: model.output.dialect.as_ref().and_then(|d| d.parse().ok()),
        mode: model.output.mode,
        inserts: model.output.inserts,
        batch_size: model.output.batch_size,
    }
}

/// Compile a list of glob strings once, reporting `GEN-INVALID-GLOB` for any
/// that fail to parse.
fn compile_globs(patterns: &[String], bag: &mut DiagnosticBag) -> Vec<Pattern> {
    patterns
        .iter()
        .filter_map(|pattern| match Pattern::new(pattern) {
            Ok(compiled) => Some(compiled),
            Err(error) => {
                bag.error(
                    "GEN-INVALID-GLOB",
                    "options",
                    format!("invalid table glob `{pattern}`: {error}"),
                );
                None
            }
        })
        .collect()
}
