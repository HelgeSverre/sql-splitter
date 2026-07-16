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
//! * **Ownership, types, and dependencies** — every column gets exactly one
//!   owner: an explicit generator, a table planner, a declared relationship, a
//!   database default, or the database itself (identity/serial, computed, or a
//!   bare integer primary key). Two owners claiming one column is a
//!   `GEN-COLUMN-OWNER-CONFLICT`; a column no rule or structural schema fact can
//!   supply is a `GEN-COLUMN-OWNER-MISSING`. Generators/modifiers are
//!   type-checked against their descriptor's accepted families, and the
//!   column/planner read→write graph is scanned for cycles
//!   (`GEN-COLUMN-CYCLE`), except a cycle a single planner owns end-to-end.
//!
//! Errors are gathered, not short-circuited: a model with a count conflict and
//! three excluded dependencies reports all four. Warnings (e.g. a `--max-rows`
//! cap) survive a successful compile via [`GenerationPlan::diagnostics`].

use std::collections::{BTreeMap, BTreeSet};

use glob::Pattern;
use rand::RngExt;

use crate::diagnostic::{DiagnosticBag, Severity, SourceLocation};
use crate::synthetic::model::{
    ChildDistribution, InferenceMode, PlannerConfig, RelationshipModel, RowsModel, SyntheticModel,
    TableModel, TableSeed,
};
use crate::synthetic::schema::{PortableColumn, SqlTypeFamily};

use super::plan::{
    ColumnOwner, CompiledOutput, CompiledRelationship, ExecutionPhase, GenerationPlan,
    PlanEstimates, PlannedColumn, PlannedTable, ResolvedTableSeed,
};
use super::registry::{ColumnScope, CompileContext, CompiledPlanner, ExtensionRegistry};
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
        let (order, cyclic) = topo_order(&selected, &parents);
        if !cyclic.is_empty() {
            // Tables left unordered by the Kahn drain form a mutual
            // relation-children dependency cycle: each derives its count from a
            // parent that in turn derives from it. Resolving them would silently
            // yield zero rows, so this is a Task-9 count error. (Distinct from
            // the column-generation cycle Task 10 owns and the FK deferral
            // Task 22 owns.)
            bag.error(
                "GEN-ROWS-CYCLE",
                "tables",
                format!(
                    "relation-children row counts form a dependency cycle among tables: {}; each table's count derives from a parent that ultimately derives from it",
                    cyclic.join(", ")
                ),
            );
        }

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
            tables.push(self.plan_table(
                name,
                table,
                count,
                root_seed,
                model.defaults.inference,
                &mut bag,
            ));
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

    /// Build a [`PlannedTable`]: resolve seed and relationships, compile every
    /// declared planner, assign exactly one owner to each column, type-check
    /// generators/modifiers, and report read/write dependency cycles. All
    /// diagnostics are appended to `bag`.
    fn plan_table(
        &self,
        name: &str,
        table: &TableModel,
        rows: u64,
        root_seed: Option<u64>,
        inference: InferenceMode,
        bag: &mut DiagnosticBag,
    ) -> PlannedTable {
        let seed = resolve_seed(&table.seed, root_seed);
        let compile_seed = seed_root_of(&seed);
        let relationships: Vec<CompiledRelationship> = table
            .relationships
            .iter()
            .map(compile_relationship)
            .collect();

        let planners = self.compile_planners(name, table, compile_seed, bag);
        let claims = collect_claims(table, &planners);

        let columns: Vec<PlannedColumn> = table
            .schema
            .columns
            .iter()
            .map(|column| {
                let owner = self.resolve_owner(
                    name,
                    table,
                    column,
                    claims.get(&column.name).map(Vec::as_slice).unwrap_or(&[]),
                    &planners,
                    compile_seed,
                    inference,
                    bag,
                );
                self.check_modifiers(name, table, column, bag);
                PlannedColumn {
                    schema: column.clone(),
                    owner,
                }
            })
            .collect();

        report_dependency_cycles(name, table, &columns, &planners, bag);

        // On a successful compile every planner resolved and compiled, so this
        // filter keeps them all in declaration order and a `ColumnOwner::
        // Planner`'s `planner_index` stays valid. If any planner failed, the
        // bag holds an error and the plan is discarded before it is observed.
        let compiled_planners = planners.into_iter().filter_map(|p| p.compiled).collect();

        PlannedTable {
            name: name.to_string(),
            rows,
            schema: table.schema.clone(),
            seed,
            columns,
            relationships,
            planners: compiled_planners,
        }
    }

    /// Resolve and compile every declared planner, collecting `GEN-PLANNER-*`
    /// diagnostics and the read/write column sets each planner declares.
    fn compile_planners(
        &self,
        table_name: &str,
        table: &TableModel,
        seed: SeedRoot,
        bag: &mut DiagnosticBag,
    ) -> Vec<PlannerInfo> {
        let relationship_names = relationship_names(table);
        table
            .planners
            .iter()
            .enumerate()
            .map(|(index, config)| {
                let path = format!("tables.{table_name}.planners[{index}]");
                let Some(factory) = self.registry.planner(&config.kind) else {
                    bag.error(
                        "GEN-PLANNER-UNKNOWN",
                        path,
                        format!("no planner registered for kind `{}`", config.kind),
                    );
                    return PlannerInfo::unresolved(&config.kind);
                };
                let descriptor = factory.descriptor();

                let writes = match descriptor.writes {
                    ColumnScope::Configured => configured_columns(config, &["columns", "writes"]),
                    _ => Vec::new(),
                };
                let reads = match descriptor.reads {
                    ColumnScope::Configured => configured_columns(config, &["reads"]),
                    _ => Vec::new(),
                };

                if let Some(referenced) = string_arg(config, "relationship") {
                    if !relationship_names.iter().any(|n| n == referenced) {
                        bag.error(
                            "GEN-RELATIONSHIP-UNKNOWN",
                            format!("{path}.relationship"),
                            format!(
                                "planner `{}` references relationship `{referenced}`, which is not declared on table `{table_name}`",
                                config.kind
                            ),
                        );
                    }
                }

                let context = CompileContext::for_table(&table.schema, seed, &path);
                let compiled = match factory.compile(config, &context) {
                    Ok(compiled) => Some(compiled),
                    Err(errors) => {
                        bag.diagnostics.extend(errors.diagnostics);
                        None
                    }
                };

                PlannerInfo {
                    kind: descriptor.kind.to_string(),
                    writes,
                    reads,
                    compiled,
                }
            })
            .collect()
    }

    /// Assign the single owner for `column`, given its recorded claimants.
    #[allow(clippy::too_many_arguments)]
    fn resolve_owner(
        &self,
        table_name: &str,
        table: &TableModel,
        column: &PortableColumn,
        claimants: &[Claim],
        planners: &[PlannerInfo],
        seed: SeedRoot,
        inference: InferenceMode,
        bag: &mut DiagnosticBag,
    ) -> ColumnOwner {
        if claimants.len() > 1 {
            let diagnostic = bag.error(
                "GEN-COLUMN-OWNER-CONFLICT",
                format!("tables.{table_name}.columns.{}", column.name),
                format!(
                    "tables.{table_name}.columns.{} is produced by more than one owner",
                    column.name
                ),
            );
            diagnostic.related = claimants
                .iter()
                .map(|claim| SourceLocation {
                    path: claim.path.clone(),
                    description: claim.description.clone(),
                })
                .collect();
            diagnostic.help = Some(format!(
                "keep a single owner for `{}`: remove the column generator or drop it from the planner mapping",
                column.name
            ));
            return ColumnOwner::GeneratedByDatabase;
        }

        if let Some(claim) = claimants.first() {
            return match &claim.source {
                ClaimSource::Generator => {
                    self.compile_generator(table_name, table, column, seed, bag)
                }
                ClaimSource::Planner { index } => ColumnOwner::Planner {
                    kind: planners[*index].kind.clone(),
                    planner_index: *index,
                },
            };
        }

        self.infer_structural_owner(table_name, table, column, inference, bag)
    }

    /// Resolve, type-check, and compile the explicit generator on `column`.
    fn compile_generator(
        &self,
        table_name: &str,
        table: &TableModel,
        column: &PortableColumn,
        seed: SeedRoot,
        bag: &mut DiagnosticBag,
    ) -> ColumnOwner {
        let path = format!("tables.{table_name}.columns.{}.generator", column.name);
        let config = table.columns[&column.name]
            .generator
            .as_ref()
            .expect("generator claim implies a generator rule");

        let Some(factory) = self.registry.generator(&config.kind) else {
            bag.error(
                "GEN-GENERATOR-UNKNOWN",
                path,
                format!("no generator registered for kind `{}`", config.kind),
            );
            return ColumnOwner::GeneratedByDatabase;
        };
        let descriptor = factory.descriptor();

        if !descriptor.accepts.contains(&column.family) {
            bag.error(
                "GEN-GENERATOR-TYPE",
                path,
                format!(
                    "generator `{}` cannot produce column `{}` of type family {:?}",
                    config.kind, column.name, column.family
                ),
            );
            return ColumnOwner::GeneratedByDatabase;
        }

        let context = CompileContext::for_column(&table.schema, column, seed, &path);
        match factory.compile(config, &context) {
            Ok(compiled) => ColumnOwner::Generator {
                kind: descriptor.kind.to_string(),
                compiled,
            },
            Err(errors) => {
                bag.diagnostics.extend(errors.diagnostics);
                ColumnOwner::GeneratedByDatabase
            }
        }
    }

    /// Owner for a column with no generator and no planner claim, from
    /// structural schema facts alone (no data observation). A column that no
    /// structural rule can supply is `GEN-COLUMN-OWNER-MISSING`.
    fn infer_structural_owner(
        &self,
        table_name: &str,
        table: &TableModel,
        column: &PortableColumn,
        inference: InferenceMode,
        bag: &mut DiagnosticBag,
    ) -> ColumnOwner {
        // Database-supplied values, in precedence order. These are pure schema
        // facts, so they resolve identically under `disabled` and `schema`.
        if column.generated || column.identity {
            return ColumnOwner::GeneratedByDatabase;
        }
        if foreign_key_columns(table).contains(&column.name) {
            return ColumnOwner::Relationship {
                relationship: relationship_of(table, &column.name),
            };
        }
        if column.default_sql.is_some() {
            return ColumnOwner::DatabaseDefault;
        }
        // A bare integer primary key is a database sequence by convention.
        if column.primary_key
            && matches!(
                column.family,
                SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
            )
        {
            return ColumnOwner::GeneratedByDatabase;
        }

        // Nothing structural applies. Richer name/constraint heuristics under
        // `schema` inference arrive in Task 20; for now both modes report the
        // column as unowned so a run never invents values silently.
        let note = match inference {
            InferenceMode::Schema => {
                " (schema inference has no rule for it yet; richer heuristics arrive later)"
            }
            InferenceMode::Disabled => "",
        };
        bag.error(
            "GEN-COLUMN-OWNER-MISSING",
            format!("tables.{table_name}.columns.{}", column.name),
            format!(
                "column `{}` on table `{table_name}` has no generator, planner, relationship, or database default to produce its value{note}",
                column.name
            ),
        );
        ColumnOwner::GeneratedByDatabase
    }

    /// Type-check each modifier declared on `column` against the registry.
    fn check_modifiers(
        &self,
        table_name: &str,
        table: &TableModel,
        column: &PortableColumn,
        bag: &mut DiagnosticBag,
    ) {
        let Some(rule) = table.columns.get(&column.name) else {
            return;
        };
        for (index, modifier) in rule.modifiers.iter().enumerate() {
            let path = format!(
                "tables.{table_name}.columns.{}.modifiers[{index}]",
                column.name
            );
            let Some(factory) = self.registry.modifier(&modifier.kind) else {
                bag.error(
                    "GEN-MODIFIER-UNKNOWN",
                    path,
                    format!("no modifier registered for kind `{}`", modifier.kind),
                );
                continue;
            };
            if !factory.descriptor().accepts.contains(&column.family) {
                bag.error(
                    "GEN-MODIFIER-TYPE",
                    path,
                    format!(
                        "modifier `{}` cannot transform column `{}` of type family {:?}",
                        modifier.kind, column.name, column.family
                    ),
                );
            }
        }
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
/// only edges between selected tables. Deterministic via sorted `BTreeMap`s.
///
/// Returns `(order, cyclic)`: `order` is the tables that could be sequenced
/// parents-first, and `cyclic` is any tables trapped in a mutual dependency
/// cycle (in name order). The caller turns a non-empty `cyclic` into a
/// `GEN-ROWS-CYCLE` error rather than resolving those tables to zero rows.
fn topo_order(
    selected: &BTreeSet<String>,
    parents: &BTreeMap<String, BTreeSet<String>>,
) -> (Vec<String>, Vec<String>) {
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

    // Any selected table the Kahn drain never reached is trapped in a cycle.
    let cyclic: Vec<String> = selected
        .iter()
        .filter(|name| !order.contains(name))
        .cloned()
        .collect();
    (order, cyclic)
}

/// A declared planner after resolution: its resolved kind, the column sets it
/// reads and writes, and its compiled form (absent if resolution or compile
/// failed — the corresponding diagnostic is already recorded).
struct PlannerInfo {
    kind: String,
    writes: Vec<String>,
    reads: Vec<String>,
    compiled: Option<Box<dyn CompiledPlanner>>,
}

impl PlannerInfo {
    /// A planner whose kind did not resolve in the registry.
    fn unresolved(kind: &str) -> Self {
        Self {
            kind: kind.to_string(),
            writes: Vec::new(),
            reads: Vec::new(),
            compiled: None,
        }
    }
}

/// One recorded claim of ownership over a column, for conflict reporting.
struct Claim {
    source: ClaimSource,
    path: String,
    description: Option<String>,
}

/// Who a [`Claim`] comes from.
enum ClaimSource {
    /// The column's own explicit generator rule.
    Generator,
    /// A planner, by its index in the table's compiled planner list.
    Planner { index: usize },
}

/// Record every ownership claim over each column: an explicit generator plus
/// each planner that names the column in its `columns`/`writes` set. A column
/// with more than one claim is a `GEN-COLUMN-OWNER-CONFLICT`.
fn collect_claims(table: &TableModel, planners: &[PlannerInfo]) -> BTreeMap<String, Vec<Claim>> {
    let mut claims: BTreeMap<String, Vec<Claim>> = BTreeMap::new();
    for (name, rule) in &table.columns {
        if rule.generator.is_some() {
            claims.entry(name.clone()).or_default().push(Claim {
                source: ClaimSource::Generator,
                path: format!("columns.{name}.generator"),
                description: None,
            });
        }
    }
    for (index, planner) in planners.iter().enumerate() {
        for column in &planner.writes {
            claims.entry(column.clone()).or_default().push(Claim {
                source: ClaimSource::Planner { index },
                path: format!("planners[{index}]"),
                description: Some(planner.kind.clone()),
            });
        }
    }
    claims
}

/// The column names a planner config names under any of `keys`. Each key's
/// value may be a sequence of names or a mapping whose values are names (the
/// `role: column` planner mapping shape).
fn configured_columns(config: &PlannerConfig, keys: &[&str]) -> Vec<String> {
    let mut columns = Vec::new();
    for key in keys {
        match config.args.get(*key) {
            Some(serde_yaml_ng::Value::Sequence(items)) => columns.extend(
                items
                    .iter()
                    .filter_map(serde_yaml_ng::Value::as_str)
                    .map(str::to_string),
            ),
            Some(serde_yaml_ng::Value::Mapping(map)) => columns.extend(
                map.values()
                    .filter_map(serde_yaml_ng::Value::as_str)
                    .map(str::to_string),
            ),
            _ => {}
        }
    }
    columns
}

/// A single string-valued planner argument, if present.
fn string_arg<'a>(config: &'a PlannerConfig, key: &str) -> Option<&'a str> {
    config.args.get(key).and_then(serde_yaml_ng::Value::as_str)
}

/// Every declared relationship name on a table, from both the generation
/// relationships and the portable schema's foreign keys.
fn relationship_names(table: &TableModel) -> Vec<String> {
    table
        .relationships
        .iter()
        .filter_map(|relationship| relationship.name.clone())
        .chain(
            table
                .schema
                .relationships
                .iter()
                .filter_map(|relationship| relationship.name.clone()),
        )
        .collect()
}

/// Every column that participates in a declared foreign key, from both the
/// generation relationships and the portable schema.
fn foreign_key_columns(table: &TableModel) -> BTreeSet<String> {
    table
        .relationships
        .iter()
        .flat_map(|relationship| relationship.columns.iter().cloned())
        .chain(
            table
                .schema
                .relationships
                .iter()
                .flat_map(|relationship| relationship.columns.iter().cloned()),
        )
        .collect()
}

/// The name of the first relationship that covers `column`, if any.
fn relationship_of(table: &TableModel, column: &str) -> Option<String> {
    table
        .relationships
        .iter()
        .find(|relationship| relationship.columns.iter().any(|c| c == column))
        .and_then(|relationship| relationship.name.clone())
        .or_else(|| {
            table
                .schema
                .relationships
                .iter()
                .find(|relationship| relationship.columns.iter().any(|c| c == column))
                .and_then(|relationship| relationship.name.clone())
        })
}

/// The [`SeedRoot`] a resolved seed compiles operators against; a `Random`
/// table has no fixed root, so operators compile against the zero seed and
/// draw fresh entropy at run time.
fn seed_root_of(seed: &ResolvedTableSeed) -> SeedRoot {
    match seed {
        ResolvedTableSeed::Inherited(root) | ResolvedTableSeed::Fixed(root) => *root,
        ResolvedTableSeed::Random => SeedRoot::new(0),
    }
}

/// A node in a table's column/planner dependency graph.
#[derive(Clone, PartialEq, Eq)]
enum DepNode {
    Column(String),
    Planner(usize),
}

/// Build the read/write dependency graph and report each strongly connected
/// component that forms a real cycle as `GEN-COLUMN-CYCLE` — unless a single
/// registered planner owns every column in it (the planner resolves those
/// values jointly).
fn report_dependency_cycles(
    table_name: &str,
    table: &TableModel,
    columns: &[PlannedColumn],
    planners: &[PlannerInfo],
    bag: &mut DiagnosticBag,
) {
    let mut nodes: Vec<DepNode> = columns
        .iter()
        .map(|column| DepNode::Column(column.schema.name.clone()))
        .collect();
    nodes.extend((0..planners.len()).map(DepNode::Planner));

    let index_of = |node: &DepNode| nodes.iter().position(|candidate| candidate == node);
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    let add_edge = |from: &DepNode, to: &DepNode, edges: &mut Vec<Vec<usize>>| {
        if let (Some(from), Some(to)) = (index_of(from), index_of(to)) {
            edges[from].push(to);
        }
    };

    // A generator that reads sibling columns depends on them: read -> column.
    for planned in columns {
        if matches!(planned.owner, ColumnOwner::Generator { .. }) {
            for read in generator_reads(table, &planned.schema.name) {
                add_edge(
                    &DepNode::Column(read),
                    &DepNode::Column(planned.schema.name.clone()),
                    &mut edges,
                );
            }
        }
    }
    // A planner depends on the columns it reads and produces the ones it
    // writes: read -> planner -> write.
    for (index, planner) in planners.iter().enumerate() {
        for read in &planner.reads {
            add_edge(
                &DepNode::Column(read.clone()),
                &DepNode::Planner(index),
                &mut edges,
            );
        }
        for write in &planner.writes {
            add_edge(
                &DepNode::Planner(index),
                &DepNode::Column(write.clone()),
                &mut edges,
            );
        }
    }

    for component in strongly_connected_components(&edges) {
        let self_loop = component.len() == 1 && edges[component[0]].contains(&component[0]);
        if component.len() <= 1 && !self_loop {
            continue;
        }
        let cycle_columns: Vec<String> = component
            .iter()
            .filter_map(|&node| match &nodes[node] {
                DepNode::Column(name) => Some(name.clone()),
                DepNode::Planner(_) => None,
            })
            .collect();
        if owned_by_single_planner(&cycle_columns, columns) {
            continue;
        }
        bag.error(
            "GEN-COLUMN-CYCLE",
            format!("tables.{table_name}.columns"),
            format!(
                "columns on table `{table_name}` form a read/write dependency cycle: {}",
                cycle_columns.join(", ")
            ),
        );
    }
}

/// The sibling columns a compiled column generator reads, from its config
/// `reads` sequence (only meaningful when the generator declares
/// `reads: Configured`).
fn generator_reads(table: &TableModel, column: &str) -> Vec<String> {
    table
        .columns
        .get(column)
        .and_then(|rule| rule.generator.as_ref())
        .map(|config| match config.args.get("reads") {
            Some(serde_yaml_ng::Value::Sequence(items)) => items
                .iter()
                .filter_map(serde_yaml_ng::Value::as_str)
                .map(str::to_string)
                .collect(),
            _ => Vec::new(),
        })
        .unwrap_or_default()
}

/// Whether every column in a cycle is owned by one and the same planner.
fn owned_by_single_planner(cycle_columns: &[String], columns: &[PlannedColumn]) -> bool {
    let mut planner: Option<usize> = None;
    for name in cycle_columns {
        let owner = columns
            .iter()
            .find(|planned| &planned.schema.name == name)
            .map(|planned| &planned.owner);
        match owner {
            Some(ColumnOwner::Planner { planner_index, .. }) => {
                if planner.is_some_and(|existing| existing != *planner_index) {
                    return false;
                }
                planner = Some(*planner_index);
            }
            _ => return false,
        }
    }
    planner.is_some()
}

/// Tarjan's strongly connected components over an adjacency list. Returns one
/// vector of node indices per component.
fn strongly_connected_components(edges: &[Vec<usize>]) -> Vec<Vec<usize>> {
    struct State<'a> {
        edges: &'a [Vec<usize>],
        index: usize,
        indices: Vec<Option<usize>>,
        low: Vec<usize>,
        on_stack: Vec<bool>,
        stack: Vec<usize>,
        components: Vec<Vec<usize>>,
    }

    fn connect(state: &mut State, v: usize) {
        state.indices[v] = Some(state.index);
        state.low[v] = state.index;
        state.index += 1;
        state.stack.push(v);
        state.on_stack[v] = true;

        for &w in &state.edges[v] {
            match state.indices[w] {
                None => {
                    connect(state, w);
                    state.low[v] = state.low[v].min(state.low[w]);
                }
                Some(w_index) if state.on_stack[w] => {
                    state.low[v] = state.low[v].min(w_index);
                }
                Some(_) => {}
            }
        }

        if state.low[v] == state.indices[v].expect("v was assigned an index") {
            let mut component = Vec::new();
            while let Some(w) = state.stack.pop() {
                state.on_stack[w] = false;
                component.push(w);
                if w == v {
                    break;
                }
            }
            state.components.push(component);
        }
    }

    let n = edges.len();
    let mut state = State {
        edges,
        index: 0,
        indices: vec![None; n],
        low: vec![0; n],
        on_stack: vec![false; n],
        stack: Vec::new(),
        components: Vec::new(),
    };
    for v in 0..n {
        if state.indices[v].is_none() {
            connect(&mut state, v);
        }
    }
    state.components
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
