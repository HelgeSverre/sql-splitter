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
    ColumnOwner, CompiledOutput, CompiledRelationship, ExecutionPhase, FamilyPhase, GenerationPlan,
    PlanEstimates, PlannedColumn, PlannedTable, RelationshipDistribution, ResolvedTableSeed,
};
use super::registry::{
    ColumnScope, CompileContext, CompiledModifier, CompiledPlanner, ExtensionRegistry,
};
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
    /// The exact in-memory byte budget for each correlated family's buffered
    /// child rows (`--family-budget`). `None` uses a large default; a small
    /// value forces the family spill path. Byte-for-byte output is independent
    /// of this budget — only where child rows are held changes.
    pub family_budget_bytes: Option<u64>,
}

/// The default per-family in-memory budget when none is pinned.
const DEFAULT_FAMILY_BUDGET_BYTES: u64 = 64 * 1024 * 1024;

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
        // Ordering edges (`All`) include every foreign key so a present parent is
        // still generated before its child; only *required* edges (non-null FKs,
        // relation.children parents, polymorphic targets) make an excluded
        // dependency a hard `GEN-EXCLUDED-DEPENDENCY`. A nullable FK to an
        // excluded table is detached (with a warning) in `plan_table` instead.
        let all_parents = parent_edges(&model, EdgeKind::All);
        let required = parent_edges(&model, EdgeKind::Required);
        self.report_excluded_dependencies(&selected, &required, &mut bag);
        // Preserve original DDL byte-for-byte only when the ENTIRE source schema
        // survives selection; once any table is excluded, every retained table is
        // rendered from its normalized schema (see `plan_table`).
        let schema_changed = selected.len() < model.tables.len();

        let root_seed = options.seed.or(model.seed);
        let rounding_root = SeedRoot::new(root_seed.unwrap_or(0));
        let (order, cyclic) = topo_order(&selected, &all_parents);
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

        let family_ctx = self.collect_family_facts(&model, &mut bag);
        let family_budget_bytes = options
            .family_budget_bytes
            .unwrap_or(DEFAULT_FAMILY_BUDGET_BYTES);

        let mut resolved: BTreeMap<String, u64> = BTreeMap::new();
        let mut tables: Vec<PlannedTable> = Vec::with_capacity(order.len());

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
                &model,
                name,
                table,
                count,
                root_seed,
                model.defaults.inference,
                &family_ctx,
                &resolved,
                &selected,
                !schema_changed,
                &mut bag,
            ));
        }

        let phases = build_phases(&order, &family_ctx, family_budget_bytes);

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
            family_budget_bytes,
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
    #[allow(clippy::too_many_arguments)]
    fn plan_table(
        &self,
        model: &SyntheticModel,
        name: &str,
        table: &TableModel,
        rows: u64,
        root_seed: Option<u64>,
        inference: InferenceMode,
        family_ctx: &FamilyContext,
        resolved: &BTreeMap<String, u64>,
        selected: &BTreeSet<String>,
        preserve_raw: bool,
        bag: &mut DiagnosticBag,
    ) -> PlannedTable {
        let seed = resolve_seed(&table.seed, root_seed);
        let compile_seed = seed_root_of(&seed);
        let mut relationships: Vec<CompiledRelationship> = table
            .relationships
            .iter()
            .map(compile_relationship)
            .collect();
        // Foreign keys declared only in the portable schema (the common shape
        // for FKs recovered from a parsed dump) are compiled too, so the engine
        // can assign their values. A generation relationship on the same columns
        // wins, since it can carry an explicit assignment distribution.
        for schema_rel in &table.schema.relationships {
            if relationships
                .iter()
                .any(|existing| existing.columns == schema_rel.columns)
            {
                continue;
            }
            relationships.push(CompiledRelationship {
                name: schema_rel.name.clone(),
                columns: schema_rel.columns.clone(),
                parent_table: schema_rel.referenced_table.clone(),
                parent_columns: schema_rel.referenced_columns.clone(),
                distribution: RelationshipDistribution::default(),
                null_permille: 0,
            });
        }
        fold_foreign_key_generators(name, table, &mut relationships, bag);

        // Detach any compiled relationship whose parent table was excluded, so
        // the output never references an absent table. A required (non-null) FK
        // to an excluded table already produced a `GEN-EXCLUDED-DEPENDENCY`
        // error; a nullable one is deliberately detachable, so warn (and let
        // `--strict` promote it) rather than fail.
        relationships.retain(|relationship| {
            if selected.contains(&relationship.parent_table) {
                return true;
            }
            if !fk_columns_all_non_null(table, &relationship.columns) {
                bag.warning(
                    "GEN-DETACHED-DEPENDENCY",
                    format!("tables.{name}"),
                    format!(
                        "optional relationship `{}` on table `{name}` references excluded table `{}`; its foreign key is detached and omitted from the rendered DDL",
                        relationship.name.as_deref().unwrap_or("(unnamed)"),
                        relationship.parent_table
                    ),
                );
            }
            false
        });

        let planners =
            self.compile_planners(model, name, table, compile_seed, family_ctx, resolved, bag);
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
                    family_ctx,
                    compile_seed,
                    inference,
                    bag,
                );
                let modifiers =
                    self.compile_modifiers(name, table, column, &owner, compile_seed, bag);
                PlannedColumn {
                    schema: column.clone(),
                    owner,
                    modifiers,
                }
            })
            .collect();

        report_dependency_cycles(name, table, &columns, &planners, bag);

        // On a successful compile every planner resolved and compiled, so this
        // filter keeps them all in declaration order and a `ColumnOwner::
        // Planner`'s `planner_index` stays valid. If any planner failed, the
        // bag holds an error and the plan is discarded before it is observed.
        let compiled_planners = planners.into_iter().filter_map(|p| p.compiled).collect();

        // The normalized portable schema the renderer sees: drop every foreign
        // key to an absent table, and any index that references an object that no
        // longer exists (columns are never dropped here, so the index guard is a
        // no-op today but keeps the invariant explicit). Clear the raw
        // `create_statement` whenever the schema set changed, so a filtered run
        // renders normalized DDL instead of stale original DDL.
        let present_columns: BTreeSet<&str> = table
            .schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        let mut schema = table.schema.clone();
        schema
            .relationships
            .retain(|relationship| selected.contains(&relationship.referenced_table));
        schema.indexes.retain(|index| {
            index
                .columns
                .iter()
                .all(|c| present_columns.contains(c.as_str()))
        });
        if !preserve_raw {
            schema.create_statement = None;
        }

        PlannedTable {
            name: name.to_string(),
            rows,
            schema,
            seed,
            columns,
            relationships,
            planners: compiled_planners,
        }
    }

    /// Resolve and compile every declared planner, collecting `GEN-PLANNER-*`
    /// diagnostics and the read/write column sets each planner declares.
    #[allow(clippy::too_many_arguments)]
    fn compile_planners(
        &self,
        model: &SyntheticModel,
        table_name: &str,
        table: &TableModel,
        seed: SeedRoot,
        family_ctx: &FamilyContext,
        resolved: &BTreeMap<String, u64>,
        bag: &mut DiagnosticBag,
    ) -> Vec<PlannerInfo> {
        let relationship_names = relationship_names(table);
        // Cross-table FK-side planners (junction/tenant/polymorphic) reference
        // OTHER tables' keys but the planner only sees its own table at compile.
        // The compiler has the whole model plus the resolved counts (parents
        // precede children in dependency order), so it injects the referenced
        // tables' resolved counts and dense key recipes as facts — the same
        // pattern the family planner uses, driven off the `cross_table`
        // capability rather than any specific `kind`.
        let relation_facts = build_relation_facts(model, table, resolved);
        table
            .planners
            .iter()
            .enumerate()
            .map(|(index, config)| {
                let path = format!("tables.{table_name}.planners[{index}]");
                // A cross-table family planner (`children:`) carries its
                // relationship on the *child* table; the compiler injected the
                // child facts to validate it there, so skip the same-table
                // relationship check that would otherwise falsely reject it.
                let is_family = config.args.contains_key("children");
                let is_cross_table = self
                    .registry
                    .planner(&config.kind)
                    .is_some_and(|factory| factory.descriptor().cross_table);
                let injected;
                let config = if let Some(facts) = family_ctx.facts.get(&(table_name.to_string(), index)) {
                    let mut cloned = config.clone();
                    cloned.args.insert(
                        super::planners::order_family::FAMILY_FACTS_KEY.to_string(),
                        facts.clone(),
                    );
                    injected = cloned;
                    &injected
                } else if !is_family
                    && (is_cross_table || config.args.contains_key("relationship"))
                {
                    // A planner that references another table's (or its own
                    // self-FK) key domain by relationship name: inject the
                    // resolved parent counts and dense key recipes so it can
                    // produce valid keys. This covers the FK-side cross-table
                    // planners (junction/tenant/polymorphic) and the same-table
                    // `hierarchy.tree`, which derives its emitted parent_id key
                    // from the referenced primary key's generator.
                    let mut cloned = config.clone();
                    cloned.args.insert(
                        super::planners::structural::RELATION_FACTS_KEY.to_string(),
                        relation_facts.clone(),
                    );
                    injected = cloned;
                    &injected
                } else {
                    config
                };
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

                if !is_family {
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
        family_ctx: &FamilyContext,
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

        // A cross-table family planner (declared on another table) may own this
        // column; that ownership was recorded when its parent table was planned.
        if let Some(external) = family_ctx
            .external_owners
            .get(&(table_name.to_string(), column.name.clone()))
        {
            return ColumnOwner::FamilyChild {
                planner_kind: external.planner_kind.clone(),
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
        // HARD schema facts — genuine database-supplied values, honored in BOTH
        // `disabled` and `schema` modes because they are declared in the DDL,
        // not inferred by convention.
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

        // CONVENTION — a bare integer primary key (no identity/serial flag, no
        // explicit owner) is treated as a database sequence. This is inference,
        // not a declared fact, so it runs ONLY under `schema` mode; under
        // `disabled` every generated column needs an explicit owner, so a bare
        // PK falls through to `GEN-COLUMN-OWNER-MISSING`.
        //
        // PLACEHOLDER: `GeneratedByDatabase` here means "the DB fills it", but a
        // plain integer PK has no sequence to fill from. Once the `sequence`
        // generator is registered (Task 11) and real inference lands (Task 20),
        // a schema-mode bare PK should receive a sequence generator that renders
        // an actual value. Closing that gap is not this task's job.
        if matches!(inference, InferenceMode::Schema)
            && column.primary_key
            && matches!(
                column.family,
                SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
            )
        {
            return ColumnOwner::GeneratedByDatabase;
        }

        // Nothing structural applies. Richer name/constraint heuristics under
        // `schema` inference arrive in Task 20; for now the column is reported
        // as unowned so a run never invents values silently.
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

    /// Type-check every modifier declared on `column`, and compile the pipeline
    /// into runnable operators (in declared order) for a generator-owned column
    /// so the engine can apply it after generation.
    ///
    /// Modifiers are compiled only for [`ColumnOwner::Generator`] columns: a
    /// `DatabaseDefault`/`GeneratedByDatabase`/`Relationship`/`Planner` column
    /// does not run a per-column generator pipeline (its value is a database
    /// placeholder, a materialized key, or planner-coordinated). Type-checking
    /// still runs for every column so a misapplied modifier is reported
    /// regardless of owner.
    fn compile_modifiers(
        &self,
        table_name: &str,
        table: &TableModel,
        column: &PortableColumn,
        owner: &ColumnOwner,
        seed: SeedRoot,
        bag: &mut DiagnosticBag,
    ) -> Vec<Box<dyn CompiledModifier>> {
        let Some(rule) = table.columns.get(&column.name) else {
            return Vec::new();
        };
        let is_generator = matches!(owner, ColumnOwner::Generator { .. });
        let mut compiled: Vec<Box<dyn CompiledModifier>> = Vec::new();
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
                continue;
            }
            if !is_generator {
                continue;
            }
            let context = CompileContext::for_column(&table.schema, column, seed, &path);
            match factory.compile(modifier, &context) {
                Ok(operator) => compiled.push(operator),
                Err(errors) => bag.diagnostics.extend(errors.diagnostics),
            }
        }
        compiled
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

/// The kind name of a child fan-out distribution, so a family planner can honor
/// its *shape* (not just its bounds) when drawing per-order line counts.
fn distribution_kind(distribution: &ChildDistribution) -> &'static str {
    match distribution {
        ChildDistribution::Observed { .. } => "observed",
        ChildDistribution::Fixed { .. } => "fixed",
        ChildDistribution::Uniform { .. } => "uniform",
        ChildDistribution::Poisson { .. } => "poisson",
        ChildDistribution::Histogram { .. } => "histogram",
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

/// Which parent edges [`parent_edges`] collects.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EdgeKind {
    /// Every ordering edge: relation.children parents, polymorphic targets, and
    /// every foreign key regardless of nullability.
    All,
    /// Only edges whose absence is fatal: relation.children parents, polymorphic
    /// targets, and non-null foreign keys. A nullable FK is detachable, so it is
    /// not required.
    Required,
}

/// Whether every column of a foreign key is declared `NOT NULL`; a nullable
/// component makes the whole relationship optional (detachable). An unknown
/// column is treated conservatively as required.
fn fk_columns_all_non_null(table: &TableModel, columns: &[String]) -> bool {
    columns.iter().all(|column| {
        table
            .schema
            .columns
            .iter()
            .find(|candidate| &candidate.name == column)
            .map(|candidate| !candidate.nullable)
            .unwrap_or(true)
    })
}

/// Parent edges per table for the requested [`EdgeKind`]: the `relation.children`
/// parent plus every declared relationship's referenced table (generation
/// relationships and portable-schema foreign keys). Under [`EdgeKind::Required`]
/// a nullable foreign key is omitted, since it may be detached rather than
/// forcing its parent to be retained.
fn parent_edges(model: &SyntheticModel, kind: EdgeKind) -> BTreeMap<String, BTreeSet<String>> {
    model
        .tables
        .iter()
        .map(|(name, table)| {
            let generation = table
                .relationships
                .iter()
                .map(|relationship| (relationship.references.table.clone(), &relationship.columns));
            let schema =
                table.schema.relationships.iter().map(|relationship| {
                    (relationship.referenced_table.clone(), &relationship.columns)
                });
            let mut parents: BTreeSet<String> = BTreeSet::new();
            for (parent, columns) in generation.chain(schema) {
                if kind == EdgeKind::All || fk_columns_all_non_null(table, columns) {
                    parents.insert(parent);
                }
            }
            if let RowsModel::RelationChildren { parent, .. } = &table.rows {
                parents.insert(parent.clone());
            }
            // A polymorphic planner references its target tables by name (with no
            // `relationships:` entry), so those targets are ordering dependencies
            // too: their counts must be resolved before this table is planned.
            for planner in &table.planners {
                if let Some(serde_yaml_ng::Value::Sequence(items)) = planner.args.get("targets") {
                    for item in items {
                        if let Some(target) =
                            item.get("table").and_then(serde_yaml_ng::Value::as_str)
                        {
                            parents.insert(target.to_string());
                        }
                    }
                }
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
        // A single planner claims each column once, even if it lists the column
        // under more than one config key (`columns` and `writes`); otherwise it
        // would falsely conflict with itself.
        let mut claimed: BTreeSet<&str> = BTreeSet::new();
        for column in &planner.writes {
            if !claimed.insert(column.as_str()) {
                continue;
            }
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

// --- Cross-table family (commerce.order_family) -----------------------------

/// The child-facts a parent-scoped family planner cannot see on its own,
/// gathered by the compiler (which has the whole model) and injected into the
/// planner config, plus the child-column ownership the family planner asserts
/// across the table boundary.
#[derive(Default)]
struct FamilyContext {
    /// `(parent_table, planner_index)` → injected child facts.
    facts: BTreeMap<(String, usize), serde_yaml_ng::Value>,
    /// `(child_table, child_column)` → the family planner that owns it.
    external_owners: BTreeMap<(String, String), ExternalChildOwner>,
    /// Every discovered parent→child family membership.
    families: Vec<FamilyMembership>,
}

/// A child column owned by a family planner declared on another table.
struct ExternalChildOwner {
    planner_kind: String,
}

/// One discovered parent/child family, used to emit the [`ExecutionPhase::Family`].
struct FamilyMembership {
    parent: String,
    child: String,
}

/// Whether a planner participates in the cross-table family child-spool pre-pass:
/// its descriptor advertises the `cross_table` capability AND it declares a
/// `children:` table (it produces that child's rows alongside its own). This is
/// driven off the registered capability rather than any specific planner `kind`,
/// so a new family planner joins the pre-pass by setting `cross_table` and
/// declaring `children`. Cross-table planners that only *reference* other tables
/// (junction/tenant/polymorphic FK-side planners) set `cross_table` too but do
/// not declare `children`, so they receive parent-key facts in
/// [`ModelCompiler::inject_relation_facts`] instead of spooling children here.
fn is_family_producer(registry: &ExtensionRegistry, config: &PlannerConfig) -> bool {
    registry
        .planner(&config.kind)
        .is_some_and(|factory| factory.descriptor().cross_table)
        && config.args.contains_key("children")
}

impl ModelCompiler {
    /// Scan the model for cross-table family planners, validate the child-side
    /// facts each one cannot see (child existence and ownership conflicts on the
    /// child columns it claims), and gather the facts to inject plus the child
    /// column ownership the planner asserts. The parent-side and remaining
    /// child-side validation runs in the planner's own `compile`.
    ///
    /// Which planners take part is decided by the [`is_family_producer`]
    /// capability check, not a hardcoded `kind`.
    fn collect_family_facts(
        &self,
        model: &SyntheticModel,
        bag: &mut DiagnosticBag,
    ) -> FamilyContext {
        collect_family_facts(&self.registry, model, bag)
    }
}

fn collect_family_facts(
    registry: &ExtensionRegistry,
    model: &SyntheticModel,
    bag: &mut DiagnosticBag,
) -> FamilyContext {
    let mut ctx = FamilyContext::default();
    for (parent_name, table) in &model.tables {
        for (index, config) in table.planners.iter().enumerate() {
            if !is_family_producer(registry, config) {
                continue;
            }
            let child_name = config.args.get("children").and_then(|v| v.as_str());
            let child = child_name.and_then(|name| model.tables.get(name).map(|t| (name, t)));

            let mut facts = serde_yaml_ng::Mapping::new();
            facts.insert("child_found".into(), (child.is_some()).into());

            if let Some((child_name, child_model)) = child {
                let mut columns = serde_yaml_ng::Mapping::new();
                for column in &child_model.schema.columns {
                    columns.insert(
                        column.name.clone().into(),
                        column.source_type.clone().into(),
                    );
                }
                facts.insert(
                    "child_columns".into(),
                    serde_yaml_ng::Value::Mapping(columns),
                );

                if let RowsModel::RelationChildren { distribution, .. } = &child_model.rows {
                    let (mean, min, max) = distribution_bounds(distribution);
                    facts.insert("dist_kind".into(), distribution_kind(distribution).into());
                    facts.insert("dist_mean".into(), mean.into());
                    facts.insert("dist_min".into(), min.into());
                    facts.insert("dist_max".into(), max.into());
                }

                let rel_name = config.args.get("relationship").and_then(|v| v.as_str());
                let rel_on_child = rel_name.is_some_and(|name| {
                    child_relationship_references(child_model, name, parent_name)
                });
                facts.insert("rel_on_child".into(), rel_on_child.into());

                // Record ownership of every child column the planner maps, and
                // report a conflict with any competing generator on the child.
                if let Some(serde_yaml_ng::Value::Mapping(mapping)) =
                    config.args.get("child_columns")
                {
                    for column_name in mapping.values().filter_map(|v| v.as_str()) {
                        let exists = child_model
                            .schema
                            .columns
                            .iter()
                            .any(|column| column.name == column_name);
                        if !exists {
                            continue;
                        }
                        let has_generator = child_model
                            .columns
                            .get(column_name)
                            .is_some_and(|rule| rule.generator.is_some());
                        if has_generator {
                            bag.error(
                                "GEN-COLUMN-OWNER-CONFLICT",
                                format!("tables.{child_name}.columns.{column_name}"),
                                format!(
                                    "column `{column_name}` on table `{child_name}` is produced by both its own generator and the `commerce.order_family` planner on table `{parent_name}`"
                                ),
                            );
                        }
                        ctx.external_owners.insert(
                            (child_name.to_string(), column_name.to_string()),
                            ExternalChildOwner {
                                planner_kind: config.kind.clone(),
                            },
                        );
                    }
                }

                ctx.families.push(FamilyMembership {
                    parent: parent_name.clone(),
                    child: child_name.to_string(),
                });
            }

            ctx.facts.insert(
                (parent_name.clone(), index),
                serde_yaml_ng::Value::Mapping(facts),
            );
        }
    }
    ctx
}

/// The dense integer key recipe `(start, step)` a parent column materializes as
/// — row `n`'s key is `start + n * step`. Mirrors the engine's
/// [`super::engine`] key-domain rules: a `sequence` generator carries its own
/// `start`/`step`, and a bare integer primary key (no generator, not
/// identity/generated) is the database sequence `1, 2, …`. Any other column
/// (a UUID, a text key, a non-key) has no dense integer recipe and returns
/// `None`, so an FK-side cross-table planner referencing it can reject the
/// incompatible key at compile time.
fn dense_key_recipe(table: &TableModel, column: &str) -> Option<(i128, i128)> {
    if let Some(rule) = table.columns.get(column) {
        if let Some(generator) = &rule.generator {
            if generator.kind == "sequence" {
                let start = generator
                    .args
                    .get("start")
                    .and_then(serde_yaml_ng::Value::as_i64)
                    .unwrap_or(0);
                let step = generator
                    .args
                    .get("step")
                    .and_then(serde_yaml_ng::Value::as_i64)
                    .unwrap_or(1);
                return Some((i128::from(start), i128::from(step)));
            }
            // Any other explicit generator is not a dense integer key.
            return None;
        }
    }
    let column = table.schema.columns.iter().find(|c| c.name == column)?;
    if column.primary_key
        && !column.identity
        && !column.generated
        && matches!(
            column.family,
            SqlTypeFamily::Integer | SqlTypeFamily::BigInteger
        )
    {
        return Some((1, 1));
    }
    None
}

/// Build the `__relation_facts` payload injected into an FK-side cross-table
/// planner: for every relationship the child declares, the resolved parent
/// count and dense key recipe; and for every model table, its resolved count
/// and the dense recipe of each of its columns (so a polymorphic planner can
/// resolve a target table referenced only by name). Counts come from the
/// already-resolved parents, which precede the child in dependency order.
fn build_relation_facts(
    model: &SyntheticModel,
    table: &TableModel,
    resolved: &BTreeMap<String, u64>,
) -> serde_yaml_ng::Value {
    use serde_yaml_ng::{Mapping, Value};

    let recipe_map = |recipe: Option<(i128, i128)>| -> Mapping {
        let mut m = Mapping::new();
        match recipe {
            Some((start, step)) => {
                m.insert("dense".into(), true.into());
                m.insert("start".into(), (start as i64).into());
                m.insert("step".into(), (step as i64).into());
            }
            None => {
                m.insert("dense".into(), false.into());
            }
        }
        m
    };

    let mut relationships = Mapping::new();
    let generation = table.relationships.iter().map(|rel| {
        (
            rel.name.clone(),
            rel.references.table.clone(),
            rel.references.columns.clone(),
        )
    });
    let schema = table.schema.relationships.iter().map(|rel| {
        (
            rel.name.clone(),
            rel.referenced_table.clone(),
            rel.referenced_columns.clone(),
        )
    });
    for (name, parent_table, parent_columns) in generation.chain(schema) {
        let Some(name) = name else { continue };
        let parent_column = parent_columns.first().cloned().unwrap_or_default();
        let recipe = model
            .tables
            .get(&parent_table)
            .and_then(|parent| dense_key_recipe(parent, &parent_column));
        let mut entry = recipe_map(recipe);
        entry.insert("parent_table".into(), parent_table.clone().into());
        entry.insert("parent_column".into(), parent_column.into());
        entry.insert(
            "count".into(),
            resolved.get(&parent_table).copied().unwrap_or(0).into(),
        );
        relationships.insert(name.into(), Value::Mapping(entry));
    }

    let mut tables = Mapping::new();
    for (name, target) in &model.tables {
        let count = resolved.get(name).copied().unwrap_or(0);
        let mut keys = Mapping::new();
        for column in &target.schema.columns {
            if let Some(recipe) = dense_key_recipe(target, &column.name) {
                let mut key_entry = recipe_map(Some(recipe));
                // Carry the owning table's row count on each key entry so a
                // polymorphic target resolved by column name still knows its
                // domain size.
                key_entry.insert("count".into(), count.into());
                keys.insert(column.name.clone().into(), Value::Mapping(key_entry));
            }
        }
        let mut entry = Mapping::new();
        entry.insert("count".into(), count.into());
        entry.insert("keys".into(), Value::Mapping(keys));
        // The primary-key column: an explicit table-level PK, else a column
        // flagged `primary_key` (the common single-column-PK shape).
        let primary_key = target.schema.primary_key.first().cloned().or_else(|| {
            target
                .schema
                .columns
                .iter()
                .find(|column| column.primary_key)
                .map(|column| column.name.clone())
        });
        if let Some(primary_key) = primary_key {
            entry.insert("primary_key".into(), primary_key.into());
        }
        tables.insert(name.clone().into(), Value::Mapping(entry));
    }

    let mut root = Mapping::new();
    root.insert("relationships".into(), Value::Mapping(relationships));
    root.insert("tables".into(), Value::Mapping(tables));
    // The child's own resolved count (it is inserted into `resolved` before this
    // table is planned), so a junction planner can reject a row count that
    // exceeds the number of distinct parent pairs.
    root.insert(
        "self_count".into(),
        resolved
            .get(&table.schema.name)
            .copied()
            .unwrap_or(0)
            .into(),
    );
    Value::Mapping(root)
}

/// Whether `child` declares a relationship named `name` that references
/// `parent`, via either a generation relationship or a portable-schema foreign
/// key. A name that resolves to a relationship referencing a *different* table
/// (or is absent) does not count — the family FK must bind the child to its
/// parent.
fn child_relationship_references(child: &TableModel, name: &str, parent: &str) -> bool {
    let generation = child.relationships.iter().any(|relationship| {
        relationship.name.as_deref() == Some(name) && relationship.references.table == parent
    });
    let schema = child.schema.relationships.iter().any(|relationship| {
        relationship.name.as_deref() == Some(name) && relationship.referenced_table == parent
    });
    generation || schema
}

/// Build the execution phases: a family parent and its child collapse into one
/// [`ExecutionPhase::Family`] (the child's own `Table` phase is omitted, since it
/// is generated inside the family), and every other table stays a `Table` phase.
fn build_phases(
    order: &[String],
    family_ctx: &FamilyContext,
    budget_bytes: u64,
) -> Vec<ExecutionPhase> {
    let child_tables: BTreeSet<&str> = family_ctx
        .families
        .iter()
        .map(|family| family.child.as_str())
        .collect();
    let mut phases = Vec::with_capacity(order.len());
    for name in order {
        if child_tables.contains(name.as_str()) {
            continue;
        }
        match family_ctx
            .families
            .iter()
            .find(|family| &family.parent == name)
        {
            Some(family) => phases.push(ExecutionPhase::Family(FamilyPhase {
                name: name.clone(),
                tables: vec![name.clone(), family.child.clone()],
                budget_bytes,
            })),
            None => phases.push(ExecutionPhase::Table(name.clone())),
        }
    }
    phases
}

/// Capture a declared relationship's referential shape and value-assignment
/// distribution. The per-row parent assignment itself is executed by Task 13's
/// engine.
fn compile_relationship(relationship: &RelationshipModel) -> CompiledRelationship {
    CompiledRelationship {
        name: relationship.name.clone(),
        columns: relationship.columns.clone(),
        parent_table: relationship.references.table.clone(),
        parent_columns: relationship.references.columns.clone(),
        distribution: parse_distribution(relationship.distribution.as_deref()),
        null_permille: 0,
    }
}

/// Parse a relationship's assignment distribution name. An absent or
/// unrecognized name falls back to [`RelationshipDistribution::Uniform`].
fn parse_distribution(name: Option<&str>) -> RelationshipDistribution {
    match name {
        Some("sequential") => RelationshipDistribution::Sequential,
        Some("weighted") => RelationshipDistribution::Weighted,
        Some("observed") => RelationshipDistribution::Observed,
        _ => RelationshipDistribution::Uniform,
    }
}

/// Fold the arguments of any explicit `relation.foreign_key` /
/// `relation.composite_key` column generator into the matching relationship, so
/// an inferred FK (owner `Relationship`) and an explicit FK generator (owner
/// `Generator`) converge on the same compiled relationship the engine executes.
/// Recognizes `distribution` (a name) and `null_rate` (a `0.0..=1.0` fraction).
/// An FK generator whose column no relationship covers is a
/// `GEN-FOREIGN-KEY-UNRESOLVED` error: it would otherwise render `DEFAULT`
/// silently at run time.
fn fold_foreign_key_generators(
    table_name: &str,
    table: &TableModel,
    relationships: &mut [CompiledRelationship],
    bag: &mut DiagnosticBag,
) {
    for (column, rule) in &table.columns {
        let Some(generator) = &rule.generator else {
            continue;
        };
        if generator.kind != "relation.foreign_key" && generator.kind != "relation.composite_key" {
            continue;
        }
        let Some(relationship) = relationships
            .iter_mut()
            .find(|relationship| relationship.columns.iter().any(|c| c == column))
        else {
            bag.error(
                "GEN-FOREIGN-KEY-UNRESOLVED",
                format!("tables.{table_name}.columns.{column}.generator"),
                format!(
                    "column `{column}` on table `{table_name}` uses generator `{}` but no relationship declares it as a foreign key; declare a `relationships:` entry covering it",
                    generator.kind
                ),
            );
            continue;
        };
        if let Some(name) = generator.args.get("distribution").and_then(|v| v.as_str()) {
            relationship.distribution = parse_distribution(Some(name));
        }
        if let Some(rate) = generator.args.get("null_rate").and_then(|v| v.as_f64()) {
            relationship.null_permille = (rate.clamp(0.0, 1.0) * 1000.0).round() as u16;
        }
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
