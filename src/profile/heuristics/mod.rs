//! Turn a neutral [`DumpProfile`] into an explicit, self-contained
//! [`SyntheticModel`] through a set of small, registered heuristics.
//!
//! Each heuristic (see the submodules) receives the schema + evidence for one
//! column and proposes zero or more [`Candidate`] rules, each tagged with a
//! [`Precedence`], a [`Confidence`], a stable reason code, and — for the rare
//! rule that embeds literal source values — the literals it would persist.
//! Heuristics never mutate a model and never generate a value; they only
//! propose.
//!
//! The [resolver](ModelInference::resolve_column) applies a strict precedence
//! order and records both the winner and every rejected alternative (without
//! their values) so `--explain` can show *why* a rule won. The precedence order,
//! highest first, is:
//!
//! 1. explicit YAML (reserved for the overrides merge; never proposed here)
//! 2. schema constraint (identity / declared default)
//! 3. **credential guard** (password/token/secret columns are synthetic-only)
//! 4. relationship / planner (declared foreign keys)
//! 5. strong semantic name+shape (email, person, money, ...)
//! 6. observed distribution (categorical, constant, histogram, sample)
//! 7. type fallback (a plain generator for the column's family)
//!
//! The emitted model is self-contained (audit amendment D3): observed row
//! counts are frozen to an integer `count` while keeping `kind: observed`,
//! `defaults.inference` is `disabled`, raw samples are never retained, and any
//! literal an explicit rule genuinely requires stays in that rule and raises a
//! source-derived warning — there is no "safe to store" mode.

mod credential;
mod distribution;
mod planner;
mod relationship;
mod schema;
mod semantic;

use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;

use crate::generate::registry::ExtensionRegistry;
use crate::profile::evidence::{ColumnEvidence, DumpProfile, TableEvidence};
use crate::synthetic::model::{
    ColumnRule, GeneratorConfig, ModelDefaults, ModelKind, ModifierConfig, OutputModel,
    ProfileInference, ProfileMetadata, RelationshipModel, RelationshipReference, RowsModel,
    SourceModel, SyntheticModel, TableModel, TableSeed,
};
use crate::synthetic::schema::{PortableColumn, PortableSchema, PortableTable};

// --- Precedence & confidence -------------------------------------------------

/// Strict precedence class of a proposed rule. Higher classes always beat
/// lower ones regardless of confidence; see the module docs for the full order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
    /// A plain generator for the column's SQL family (lowest).
    TypeFallback,
    /// A distribution replayed from observed evidence.
    ObservedDistribution,
    /// A strong semantic match on name *and* value shape.
    StrongSemantic,
    /// A declared foreign key or nominated planner.
    Relationship,
    /// A synthetic-only credential rule (password/token/secret).
    CredentialGuard,
    /// A rule forced by a schema constraint (identity / default).
    SchemaConstraint,
    /// An explicit YAML rule (reserved for the overrides merge; never proposed
    /// by inference itself). Highest.
    ExplicitYaml,
}

/// How sure a heuristic is about a candidate, used to order candidates that
/// share a [`Precedence`] and to label the decision for `--explain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    Low,
    Medium,
    High,
    Certain,
}

impl Confidence {
    /// Stable lowercase label for [`ProfileInference::confidence`].
    fn label(self) -> &'static str {
        match self {
            Confidence::Low => "low",
            Confidence::Medium => "medium",
            Confidence::High => "high",
            Confidence::Certain => "certain",
        }
    }
}

// --- Candidate ---------------------------------------------------------------

/// A single proposed rule for one column: a typed generator plus the metadata
/// the resolver needs to rank it and explain the outcome.
#[derive(Debug, Clone)]
pub struct Candidate {
    precedence: Precedence,
    confidence: Confidence,
    /// Stable reason code, e.g. `credential_name_guard` or `observed_categorical`.
    reason: &'static str,
    generator: GeneratorConfig,
    semantic: Option<String>,
    /// Literal source values this rule would persist, if any. Non-empty marks
    /// the rule source-derived (raises `GEN-INFER-SOURCE-DERIVED`).
    source_literals: Vec<String>,
}

impl Candidate {
    /// A candidate with no embedded source literals and no semantic tag.
    fn new(
        precedence: Precedence,
        confidence: Confidence,
        reason: &'static str,
        generator: GeneratorConfig,
    ) -> Self {
        Self {
            precedence,
            confidence,
            reason,
            generator,
            semantic: None,
            source_literals: Vec::new(),
        }
    }

    /// Tag this candidate with the semantic annotation persisted on the rule.
    fn with_semantic(mut self, semantic: impl Into<String>) -> Self {
        self.semantic = Some(semantic.into());
        self
    }

    /// Record the literal source values this candidate would persist.
    fn with_source_literals(mut self, literals: Vec<String>) -> Self {
        self.source_literals = literals;
        self
    }

    /// The registry key ordering tuple: the resolver prefers, in order, higher
    /// precedence, then higher confidence, then a lexicographically smaller
    /// generator kind (a stable stand-in for descriptor / registration order).
    fn rank_key(&self) -> (Precedence, Confidence, std::cmp::Reverse<&str>) {
        (
            self.precedence,
            self.confidence,
            std::cmp::Reverse(self.generator.kind.as_str()),
        )
    }
}

// --- Column context ----------------------------------------------------------

/// Everything the heuristics for one column see: its schema, the evidence the
/// profiler gathered for it (absent at schema depth), and the surrounding
/// table.
pub(crate) struct ColumnContext<'a> {
    table: &'a PortableTable,
    column: &'a PortableColumn,
    evidence: Option<&'a ColumnEvidence>,
    row_count: Option<u64>,
}

impl<'a> ColumnContext<'a> {
    fn table(&self) -> &'a PortableTable {
        self.table
    }
    fn column(&self) -> &'a PortableColumn {
        self.column
    }
    fn evidence(&self) -> Option<&'a ColumnEvidence> {
        self.evidence
    }
    fn row_count(&self) -> Option<u64> {
        self.row_count
    }
}

// --- Decisions & result ------------------------------------------------------

/// One rejected alternative recorded for `--explain`. Deliberately carries no
/// values — only the shape of the losing proposal.
#[derive(Debug, Clone)]
pub struct RejectedCandidate {
    pub generator_kind: String,
    pub reason: String,
    pub precedence: Precedence,
    pub confidence: Confidence,
}

/// Why one column ended up with the rule it did, plus the alternatives that
/// lost. Values are never recorded here.
#[derive(Debug, Clone)]
pub struct Decision {
    /// `"table.column"`.
    pub column: String,
    /// The winning candidate's reason code.
    pub reason: String,
    pub confidence: Confidence,
    pub generator_kind: String,
    /// Whether the winning rule embeds source-derived literal values.
    pub source_derived: bool,
    pub rejected: Vec<RejectedCandidate>,
}

/// The output of [`ModelInference::infer`]: an explicit, self-contained model,
/// the per-column decisions, and any warnings raised while inferring.
#[derive(Debug, Clone)]
pub struct InferenceResult {
    pub model: SyntheticModel,
    pub decisions: Vec<Decision>,
    pub warnings: Vec<String>,
    /// `"table.column"` -> the literal source values its winning rule persists.
    source_literals: BTreeMap<String, Vec<String>>,
}

impl InferenceResult {
    /// The decision recorded for `"table.column"`, if any.
    pub fn decision(&self, key: &str) -> Option<&Decision> {
        self.decisions.iter().find(|d| d.column == key)
    }

    /// The winning column rule for `table`/`column`, if the model has one.
    pub fn column_rule(&self, table: &str, column: &str) -> Option<&ColumnRule> {
        self.model.tables.get(table)?.columns.get(column)
    }

    /// The literal source values persisted by `"table.column"`'s winning rule
    /// (empty when the rule embeds none — e.g. a synthetic credential rule).
    pub fn source_literals(&self, key: &str) -> &[String] {
        self.source_literals
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Errors from [`ModelInference::infer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InferenceError {
    /// The profile referenced no table present in the schema.
    NoTables,
}

impl fmt::Display for InferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InferenceError::NoTables => {
                write!(f, "no profiled table matched the provided schema")
            }
        }
    }
}

impl std::error::Error for InferenceError {}

// --- Options -----------------------------------------------------------------

/// Knobs controlling how a profile is turned into a model.
#[derive(Debug, Clone)]
pub struct InferenceOptions {
    /// Whether to attach the removable `profiles` evidence map to the model.
    /// Deleting it never changes generation; a library caller that does not
    /// want the (bounded, non-literal) summaries can turn it off.
    include_profiles: bool,
}

impl Default for InferenceOptions {
    fn default() -> Self {
        Self {
            include_profiles: true,
        }
    }
}

impl InferenceOptions {
    /// Include (or drop) the removable `profiles` evidence map. Mirrors the CLI
    /// emitter's `--profiles`/library `EmitOptions::include_profiles`.
    pub fn include_profiles(mut self, include: bool) -> Self {
        self.include_profiles = include;
        self
    }
}

// --- ModelInference ----------------------------------------------------------

/// Infers explicit generation rules from a dump profile through the registered
/// heuristics. Build one with [`ModelInference::standard`] (the built-in
/// registry + default options) or [`ModelInference::new`].
pub struct ModelInference {
    registry: ExtensionRegistry,
    options: InferenceOptions,
}

impl ModelInference {
    /// A `ModelInference` over the standard generator registry and default
    /// options.
    pub fn standard() -> Self {
        Self {
            registry: ExtensionRegistry::standard(),
            options: InferenceOptions::default(),
        }
    }

    /// A `ModelInference` over a caller-supplied registry and options.
    pub fn new(registry: ExtensionRegistry, options: InferenceOptions) -> Self {
        Self { registry, options }
    }

    /// Infer an explicit, self-contained model from a schema and its profile.
    ///
    /// The `schema` supplies column types, keys, and relationships; the
    /// `profile` supplies the observed value evidence. Tables in the profile
    /// that are absent from the schema are skipped with a warning.
    pub fn infer(
        &self,
        schema: &PortableSchema,
        profile: &DumpProfile,
    ) -> Result<InferenceResult, InferenceError> {
        let mut tables = BTreeMap::new();
        let mut decisions = Vec::new();
        let mut warnings = Vec::new();
        let mut profiles = BTreeMap::new();
        let mut source_literals = BTreeMap::new();

        for table_evidence in &profile.tables {
            let Some(portable) = schema.tables.get(&table_evidence.table) else {
                warnings.push(format!(
                    "GEN-INFER-TABLE-UNKNOWN: profiled table `{}` is not in the schema; skipped",
                    table_evidence.table
                ));
                continue;
            };
            let table_model = self.infer_table(
                portable,
                Some(table_evidence),
                &mut decisions,
                &mut warnings,
                &mut profiles,
                &mut source_literals,
            );
            tables.insert(portable.name.clone(), table_model);
        }

        // Any schema table the profile never mentioned still gets an explicit,
        // type-fallback model so the output stands alone.
        for (name, portable) in &schema.tables {
            if tables.contains_key(name) {
                continue;
            }
            let table_model = self.infer_table(
                portable,
                None,
                &mut decisions,
                &mut warnings,
                &mut profiles,
                &mut source_literals,
            );
            tables.insert(name.clone(), table_model);
        }

        if tables.is_empty() {
            return Err(InferenceError::NoTables);
        }

        let model = SyntheticModel {
            schema_ref: None,
            version: 1,
            kind: ModelKind::Model,
            imports: Vec::new(),
            source: Some(SourceModel {
                dialect: schema.dialect.clone(),
                fingerprint: None,
                fingerprint_policy: None,
            }),
            defaults: ModelDefaults::default(), // inference: disabled
            seed: None,
            output: OutputModel::default(),
            tables,
            profiles: if self.options.include_profiles {
                profiles
            } else {
                BTreeMap::new()
            },
        };

        Ok(InferenceResult {
            model,
            decisions,
            warnings,
            source_literals,
        })
    }

    fn infer_table(
        &self,
        portable: &PortableTable,
        evidence: Option<&TableEvidence>,
        decisions: &mut Vec<Decision>,
        warnings: &mut Vec<String>,
        profiles: &mut BTreeMap<String, ProfileMetadata>,
        source_literals: &mut BTreeMap<String, Vec<String>>,
    ) -> TableModel {
        let row_count = evidence.and_then(|e| e.row_count);
        let mut columns = BTreeMap::new();

        for column in &portable.columns {
            let key = format!("{}.{}", portable.name, column.name);

            // A declared foreign-key column is owned structurally by the
            // compiler (via the table's relationship), so it gets no column
            // generator at all — only a recorded decision.
            if relationship::is_foreign_key_column(portable, &column.name) {
                decisions.push(Decision {
                    column: key,
                    reason: "relationship_declared_fk".to_string(),
                    confidence: Confidence::High,
                    generator_kind: "relation.foreign_key".to_string(),
                    source_derived: false,
                    rejected: Vec::new(),
                });
                continue;
            }

            let column_evidence = evidence.and_then(|e| find_column_evidence(e, &column.name));
            let ctx = ColumnContext {
                table: portable,
                column,
                evidence: column_evidence,
                row_count,
            };

            let mut candidates = Vec::new();
            candidates.extend(schema::candidates(&ctx));
            candidates.extend(credential::candidates(&ctx));
            candidates.extend(semantic::candidates(&ctx));
            candidates.extend(distribution::candidates(&ctx));

            // Drop any proposal whose generator kind is not actually registered
            // (a heuristic must never emit an uncompilable rule).
            candidates
                .retain(|candidate| self.registry.generator(&candidate.generator.kind).is_some());

            let (rule, decision, literals) = resolve_column(&ctx, &key, candidates);

            if !literals.is_empty() {
                warnings.push(format!(
                    "GEN-INFER-SOURCE-DERIVED: `{key}` persists {} source-derived literal value(s) \
                     required by its `{}` rule",
                    literals.len(),
                    decision.generator_kind
                ));
                source_literals.insert(key.clone(), literals);
            }

            if self.options.include_profiles {
                if let Some(meta) = profile_metadata(&ctx, &decision) {
                    profiles.insert(key.clone(), meta);
                }
            }

            decisions.push(decision);
            columns.insert(column.name.clone(), rule);
        }

        // Planner reconnaissance runs at table scope. Nominations remain
        // warnings so inference does not insert planner configuration.
        warnings.extend(planner::nominations(portable, evidence));

        TableModel {
            seed: TableSeed::Inherit,
            rows: match row_count {
                Some(count) => RowsModel::Observed { count },
                None => RowsModel::Fixed { count: 0 },
            },
            schema: portable.clone(),
            columns,
            relationships: declared_relationships(portable),
            planners: Vec::new(),
        }
    }
}

/// Resolve one column's candidates into a rule + decision, applying precedence
/// and recording rejected alternatives (without values).
fn resolve_column(
    ctx: &ColumnContext<'_>,
    key: &str,
    mut candidates: Vec<Candidate>,
) -> (ColumnRule, Decision, Vec<String>) {
    // Always have a floor: a bare type fallback, so a column never goes ruleless.
    if candidates.is_empty() {
        candidates.push(schema::type_fallback(ctx));
    }

    // Highest rank_key wins; stable so ties resolve deterministically.
    let winner_index = candidates
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.rank_key().cmp(&b.rank_key()))
        .map(|(index, _)| index)
        .expect("candidates is non-empty");

    let winner = candidates.swap_remove(winner_index);

    let rejected = candidates
        .iter()
        .map(|candidate| RejectedCandidate {
            generator_kind: candidate.generator.kind.clone(),
            reason: candidate.reason.to_string(),
            precedence: candidate.precedence,
            confidence: candidate.confidence,
        })
        .collect();

    let mut modifiers = Vec::new();
    if let Some(modifier) = sparse_modifier(ctx, &winner) {
        modifiers.push(modifier);
    }

    let decision = Decision {
        column: key.to_string(),
        reason: winner.reason.to_string(),
        confidence: winner.confidence,
        generator_kind: winner.generator.kind.clone(),
        source_derived: !winner.source_literals.is_empty(),
        rejected,
    };

    let rule = ColumnRule {
        semantic: winner.semantic.clone(),
        generator: Some(winner.generator.clone()),
        modifiers,
    };

    (rule, decision, winner.source_literals)
}

/// A `null_rate` modifier replaying an observed null fraction on a nullable
/// column, unless the winning rule already produces `NULL`/`DEFAULT` itself.
fn sparse_modifier(ctx: &ColumnContext<'_>, winner: &Candidate) -> Option<ModifierConfig> {
    if !ctx.column().nullable {
        return None;
    }
    if matches!(winner.generator.kind.as_str(), "null" | "database_default") {
        return None;
    }
    let rate = ctx.evidence()?.null_rate;
    if !(0.005..=0.995).contains(&rate) {
        return None;
    }
    let mut args = BTreeMap::new();
    args.insert("rate".to_string(), yaml(round4(rate)));
    Some(ModifierConfig {
        kind: "null_rate".to_string(),
        args,
    })
}

/// Build the removable evidence summary for one column (bounded, non-literal).
fn profile_metadata(ctx: &ColumnContext<'_>, decision: &Decision) -> Option<ProfileMetadata> {
    let evidence = ctx.evidence()?;
    Some(ProfileMetadata {
        rows: ctx.row_count().unwrap_or(evidence.total_count),
        null_fraction: round4(evidence.null_rate),
        distinct_estimate: evidence.distinct_estimate.round() as u64,
        inference: Some(ProfileInference {
            selected: decision.generator_kind.clone(),
            confidence: decision.confidence.label().to_string(),
            reasons: vec![decision.reason.clone()],
        }),
    })
}

/// Declared foreign keys become explicit model relationships so the output
/// stands alone.
fn declared_relationships(table: &PortableTable) -> Vec<RelationshipModel> {
    table
        .relationships
        .iter()
        .map(|fk| RelationshipModel {
            name: fk.name.clone(),
            columns: fk.columns.clone(),
            references: RelationshipReference {
                table: fk.referenced_table.clone(),
                columns: fk.referenced_columns.clone(),
            },
            distribution: None,
        })
        .collect()
}

fn find_column_evidence<'a>(table: &'a TableEvidence, name: &str) -> Option<&'a ColumnEvidence> {
    table.columns.iter().find(|c| c.name == name)
}

// --- Shared candidate-building helpers (used by the submodules) --------------

/// Serialize a value into a YAML scalar/sequence for a generator argument.
pub(crate) fn yaml<T: Serialize>(value: T) -> serde_yaml_ng::Value {
    serde_yaml_ng::to_value(value).unwrap_or(serde_yaml_ng::Value::Null)
}

/// A generator config with no arguments.
pub(crate) fn generator(kind: &str) -> GeneratorConfig {
    GeneratorConfig {
        kind: kind.to_string(),
        args: BTreeMap::new(),
    }
}

/// A generator config with the given `(key, value)` arguments.
pub(crate) fn generator_with(
    kind: &str,
    args: impl IntoIterator<Item = (&'static str, serde_yaml_ng::Value)>,
) -> GeneratorConfig {
    GeneratorConfig {
        kind: kind.to_string(),
        args: args.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
    }
}

/// A `[{ value, weight }, ...]` YAML sequence for the weighted generators.
pub(crate) fn weighted_entries(entries: &[(String, u64)]) -> serde_yaml_ng::Value {
    let seq = entries
        .iter()
        .map(|(value, weight)| {
            let mut map = serde_yaml_ng::Mapping::new();
            map.insert(yaml("value"), yaml(value));
            map.insert(yaml("weight"), yaml(*weight));
            serde_yaml_ng::Value::Mapping(map)
        })
        .collect();
    serde_yaml_ng::Value::Sequence(seq)
}

/// Round a fraction to four decimal places for stable, readable emission.
fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}
