//! Merges a base `model` with a typed `overrides` patch into a complete
//! candidate [`SyntheticModel`].
//!
//! This is a typed, field-wise patch — not a generic YAML merge. Generic
//! YAML merging happens once, at import time, in
//! [`super::config::ConfigLoader`]; by the time a [`SyntheticOverrides`]
//! reaches [`ModelMerger`], both documents are already fully typed and this
//! module only has to decide, per field, whether the patch may replace the
//! base value outright (generation rules, rows, relationships, planners) or
//! may only assert that it still matches an immutable structural fact (the
//! source schema, the source fingerprint). Every table and column override
//! is checked independently, via [`crate::diagnostic::DiagnosticBag`], so a
//! patch with several unrelated mistakes reports all of them at once rather
//! than stopping at the first.

use crate::diagnostic::DiagnosticBag;

use super::model::{
    ColumnRule, FingerprintPolicy, OutputModel, RowsModel, SourceModel, SyntheticModel, TableModel,
    TableSeed,
};
use super::overrides::{
    ColumnRuleOverride, OutputOverride, PortableTableOverride, RootSeedOverride, RowsKind,
    RowsOverride, SyntheticOverrides, TableOverride, TableSeedOverride,
};
use super::schema::PortableTable;

/// Merges a base [`SyntheticModel`] with a typed [`SyntheticOverrides`]
/// patch, producing a complete candidate model or the diagnostics that
/// block it.
pub struct ModelMerger;

impl ModelMerger {
    /// Applies `patch` on top of `base`, field by field.
    ///
    /// Diagnostics are gathered rather than short-circuited: a missing
    /// table, a missing column, and a structural schema mismatch on three
    /// unrelated tables all get reported together.
    ///
    /// On success, the returned `DiagnosticBag` carries any warnings
    /// produced along the way (e.g. a `GEN-SOURCE-FINGERPRINT` warning
    /// under `warn` policy) — it may be empty, but it is never discarded.
    /// This is why the success type is `(SyntheticModel, DiagnosticBag)`
    /// rather than plain `SyntheticModel`: a bag with no error-severity
    /// diagnostic still deserves to be seen by the caller. On failure (any
    /// error-severity diagnostic), `Err(bag)` carries every diagnostic,
    /// warnings included, same as before.
    pub fn merge(
        mut base: SyntheticModel,
        patch: SyntheticOverrides,
    ) -> Result<(SyntheticModel, DiagnosticBag), DiagnosticBag> {
        let mut bag = DiagnosticBag::default();

        match patch.seed {
            RootSeedOverride::Inherit => {}
            RootSeedOverride::Random => base.seed = None,
            RootSeedOverride::Fixed(seed) => base.seed = Some(seed),
        }

        if let Some(output) = patch.output {
            output.apply_to(&mut base.output);
        }

        if let Some(defaults) = patch.defaults {
            base.defaults = defaults;
        }

        if let Some(source) = patch.source {
            apply_source(source, &mut base.source, &mut bag);
        }

        for (name, table_patch) in patch.tables {
            match base.tables.get_mut(&name) {
                Some(table) => {
                    let table_path = format!("tables.{name}");
                    table_patch.apply_to(&table_path, table, &mut bag);
                }
                None => {
                    bag.error(
                        "GEN-MISSING-TABLE",
                        format!("tables.{name}"),
                        "override table does not exist in the source/base model",
                    );
                }
            }
        }

        if bag.has_errors() {
            Err(bag)
        } else {
            Ok((base, bag))
        }
    }
}

/// Applies a `source:` override, which reuses [`SourceModel`] wholesale
/// rather than a partial-override shape (every override field is a
/// meaningful full value, `dialect` included, since `SourceModel::dialect`
/// is not optional).
///
/// The one field this does not treat as a plain replacement is
/// `fingerprint`: a mismatch against the base's recorded fingerprint is
/// diagnosed according to the effective [`FingerprintPolicy`] (the
/// override's own policy if given, else the base's, else
/// [`FingerprintPolicy::Ignore`]) rather than silently dropped. The
/// replacement still happens — fingerprint differences are expected to
/// occur (a dump was re-profiled, say) — but never *without* the caller
/// being told, at least at `warn` severity.
fn apply_source(
    patch: SourceModel,
    base_source: &mut Option<SourceModel>,
    bag: &mut DiagnosticBag,
) {
    let existing_fingerprint = base_source.as_ref().and_then(|s| s.fingerprint.clone());
    let existing_policy = base_source.as_ref().and_then(|s| s.fingerprint_policy);

    if let (Some(existing), Some(patched)) = (&existing_fingerprint, &patch.fingerprint) {
        if existing != patched {
            let policy = patch
                .fingerprint_policy
                .or(existing_policy)
                .unwrap_or(FingerprintPolicy::Ignore);
            let message = format!(
                "override source fingerprint `{patched}` does not match the base model's recorded fingerprint `{existing}`"
            );
            match policy {
                FingerprintPolicy::Ignore => {}
                FingerprintPolicy::Warn => {
                    bag.warning("GEN-SOURCE-FINGERPRINT", "source.fingerprint", message);
                }
                FingerprintPolicy::Require => {
                    bag.error("GEN-SOURCE-FINGERPRINT", "source.fingerprint", message);
                }
            }
        }
    }

    *base_source = Some(patch);
}

impl OutputOverride {
    /// Replaces each present field on `output`; absent fields leave the
    /// base value untouched.
    fn apply_to(self, output: &mut OutputModel) {
        if let Some(dialect) = self.dialect {
            output.dialect = Some(dialect);
        }
        if let Some(mode) = self.mode {
            output.mode = Some(mode);
        }
        if let Some(inserts) = self.inserts {
            output.inserts = Some(inserts);
        }
        if let Some(batch_size) = self.batch_size {
            output.batch_size = Some(batch_size);
        }
    }
}

impl TableOverride {
    /// Applies a per-table patch: seed, schema assertion, rows, per-column
    /// rules, and whole-list relationship/planner replacement.
    fn apply_to(self, table_path: &str, table: &mut TableModel, bag: &mut DiagnosticBag) {
        match self.seed {
            TableSeedOverride::Inherit => {}
            TableSeedOverride::Random => table.seed = TableSeed::Random,
            TableSeedOverride::Fixed(seed) => table.seed = TableSeed::Fixed(seed),
        }

        if let Some(schema_patch) = self.schema {
            schema_patch.check_matches(table_path, &table.schema, bag);
        }

        if let Some(rows_patch) = self.rows {
            rows_patch.apply_to(table_path, &mut table.rows, bag);
        }

        for (column_name, column_patch) in self.columns {
            let column_path = format!("{table_path}.columns.{column_name}");
            if !table.schema.columns.iter().any(|c| c.name == column_name) {
                bag.error(
                    "GEN-MISSING-COLUMN",
                    column_path,
                    format!(
                        "override column `{column_name}` does not exist in table `{}`'s schema",
                        table.schema.name
                    ),
                );
                continue;
            }
            let rule = table
                .columns
                .entry(column_name)
                .or_insert_with(|| ColumnRule {
                    semantic: None,
                    generator: None,
                    modifiers: Vec::new(),
                });
            column_patch.apply_to(rule);
        }

        if let Some(relationships) = self.relationships {
            table.relationships = relationships;
        }
        if let Some(planners) = self.planners {
            table.planners = planners;
        }
    }
}

impl PortableTableOverride {
    /// Asserts that any structural fact the override names still matches
    /// the base/source schema; never mutates `schema`. Overrides cannot
    /// rename a table or redefine its DDL — see the design's "source,
    /// model, and overrides merge predictably" section — so a mismatch
    /// here is always `GEN-SCHEMA-MISMATCH`, not a silent structural
    /// change.
    ///
    /// Column-level structural facts (type, nullability, key, existence)
    /// are not represented in [`PortableTableOverride`] at all — it carries
    /// only `name` and `create_statement` — so the type system already
    /// forecloses column-level structural overrides; this only needs to
    /// guard the two table-level facts that do exist.
    fn check_matches(&self, table_path: &str, schema: &PortableTable, bag: &mut DiagnosticBag) {
        if let Some(name) = &self.name {
            if name != &schema.name {
                bag.error(
                    "GEN-SCHEMA-MISMATCH",
                    format!("{table_path}.schema.name"),
                    format!(
                        "override asserts table name `{name}`, but the source/base schema names it `{}`; overrides cannot rename or otherwise restructure DDL",
                        schema.name
                    ),
                );
            }
        }
        if let Some(create_statement) = &self.create_statement {
            if Some(create_statement) != schema.create_statement.as_ref() {
                bag.error(
                    "GEN-SCHEMA-MISMATCH",
                    format!("{table_path}.schema.create_statement"),
                    "override's create_statement does not match the source/base schema; overrides cannot redefine DDL structure",
                );
            }
        }
    }
}

impl ColumnRuleOverride {
    /// Replaces each present field on `rule`; absent fields leave the base
    /// rule untouched. Generator/modifier changes are ordinary generation
    /// rules, not structural facts, so they are always legal.
    fn apply_to(self, rule: &mut ColumnRule) {
        if let Some(semantic) = self.semantic {
            rule.semantic = Some(semantic);
        }
        if let Some(generator) = self.generator {
            rule.generator = Some(generator);
        }
        if let Some(modifiers) = self.modifiers {
            rule.modifiers = modifiers;
        }
    }
}

impl RowsOverride {
    /// Applies a `rows:` patch. When the override's `kind` matches the
    /// base's current variant, only the fields the override actually
    /// supplies change — the rest are read back off the base variant. When
    /// the override switches to a different `kind`, there is no base value
    /// to fall back on for the fields it omits, so it must supply every
    /// field that variant needs; if it doesn't, this reports
    /// `GEN-INCOMPLETE-ROWS` rather than guessing.
    fn apply_to(self, table_path: &str, rows: &mut RowsModel, bag: &mut DiagnosticBag) {
        let path = format!("{table_path}.rows");
        match self.kind {
            RowsKind::Fixed => {
                let base_count = match rows {
                    RowsModel::Fixed { count } => Some(*count),
                    _ => None,
                };
                match self.count.or(base_count) {
                    Some(count) => *rows = RowsModel::Fixed { count },
                    None => {
                        bag.error(
                            "GEN-INCOMPLETE-ROWS",
                            path,
                            "rows override switches to `fixed` but supplies no `count`, and the base table's rows are not already `fixed`",
                        );
                    }
                }
            }
            RowsKind::Observed => {
                let base_count = match rows {
                    RowsModel::Observed { count } => Some(*count),
                    _ => None,
                };
                let count = self.count.or_else(|| {
                    self.scale
                        .zip(base_count)
                        .map(|(scale, base)| ((base as f64) * scale).round() as u64)
                });
                match count {
                    Some(count) => *rows = RowsModel::Observed { count },
                    None => {
                        bag.error(
                            "GEN-INCOMPLETE-ROWS",
                            path,
                            "rows override switches to `observed` but supplies neither `count` nor a `scale` against an already-`observed` base count",
                        );
                    }
                }
            }
            RowsKind::Scale => {
                let (base_base, base_factor) = match rows {
                    RowsModel::Scale { base, factor, .. } => (Some(*base), Some(*factor)),
                    _ => (None, None),
                };
                match (self.base.or(base_base), self.factor.or(base_factor)) {
                    (Some(base), Some(factor)) => {
                        let count = ((base as f64) * factor).round() as u64;
                        *rows = RowsModel::Scale {
                            base,
                            factor,
                            count,
                        };
                    }
                    _ => {
                        bag.error(
                            "GEN-INCOMPLETE-ROWS",
                            path,
                            "rows override switches to `scale` but is missing `base` and/or `factor`",
                        );
                    }
                }
            }
            RowsKind::RelationChildren => {
                let (base_parent, base_count, base_distribution) = match rows {
                    RowsModel::RelationChildren {
                        parent,
                        count,
                        distribution,
                    } => (
                        Some(parent.clone()),
                        Some(*count),
                        Some(distribution.clone()),
                    ),
                    _ => (None, None, None),
                };
                match (
                    self.parent.or(base_parent),
                    self.count.or(base_count),
                    self.distribution.or(base_distribution),
                ) {
                    (Some(parent), Some(count), Some(distribution)) => {
                        *rows = RowsModel::RelationChildren {
                            parent,
                            count,
                            distribution,
                        };
                    }
                    _ => {
                        bag.error(
                            "GEN-INCOMPLETE-ROWS",
                            path,
                            "rows override switches to `relation.children` but is missing `parent`, `count`, and/or `distribution`",
                        );
                    }
                }
            }
        }
    }
}
