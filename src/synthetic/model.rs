//! The `kind: model` YAML document: a self-contained synthetic data model.
//!
//! A [`SyntheticModel`] carries a complete normalized schema, an exact row
//! count for every table, and every generation rule needed to produce data
//! without its source dump. [`SyntheticFile`] is the entry point: it reads
//! the `version`/`kind` envelope first, then deserializes into the
//! role-specific, unknown-field-safe struct for that role.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::de::Error as SerdeDeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::overrides::SyntheticOverrides;
use super::schema::PortableTable;

/// The schema version this crate understands. `SyntheticFile::parse_str`
/// rejects any other value so a future breaking change can add an upgrader
/// rather than silently misreading old documents.
const SUPPORTED_VERSION: u32 = 1;

/// A parsed YAML document, tagged by its declared `kind`.
///
/// `parse_str` peeks the `version`/`kind` envelope before committing to a
/// role-specific `#[serde(deny_unknown_fields)]` struct, so a typo in
/// `kind` produces a clear role error instead of a confusing field error.
#[derive(Debug, Clone)]
pub enum SyntheticFile {
    Model(SyntheticModel),
    Overrides(SyntheticOverrides),
}

/// Minimal envelope used only to determine `version` and `kind` before
/// picking which role-specific struct to deserialize into. Unknown fields
/// are intentionally allowed here; the role-specific struct enforces them.
#[derive(Debug, Deserialize)]
struct DocumentEnvelope {
    version: u32,
    kind: String,
}

impl SyntheticFile {
    /// Parse a YAML document, validating its `version`/`kind` envelope
    /// before deserializing into the matching role-specific struct.
    ///
    /// `serde_yaml_ng` rejects duplicate mapping keys while parsing into
    /// [`serde_yaml_ng::Value`], so that check happens for free here.
    pub fn parse_str(input: &str) -> anyhow::Result<Self> {
        let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(input)?;
        let envelope: DocumentEnvelope = serde_yaml_ng::from_value(value.clone())?;

        if envelope.version != SUPPORTED_VERSION {
            return Err(serde_yaml_ng::Error::custom(format!(
                "unsupported version: {} (expected {SUPPORTED_VERSION})",
                envelope.version
            ))
            .into());
        }

        match envelope.kind.as_str() {
            "model" => Ok(SyntheticFile::Model(serde_yaml_ng::from_value(value)?)),
            "overrides" => Ok(SyntheticFile::Overrides(serde_yaml_ng::from_value(value)?)),
            other => Err(serde_yaml_ng::Error::custom(format!(
                "unknown document kind: {other} (expected `model` or `overrides`)"
            ))
            .into()),
        }
    }

    /// Consume this document as a `model`, or error if it is `overrides`.
    pub fn into_model(self) -> anyhow::Result<SyntheticModel> {
        match self {
            SyntheticFile::Model(model) => Ok(model),
            SyntheticFile::Overrides(_) => Err(anyhow::anyhow!(
                "expected a `kind: model` document, found `kind: overrides`"
            )),
        }
    }

    /// Consume this document as `overrides`, or error if it is a `model`.
    pub fn into_overrides(self) -> anyhow::Result<SyntheticOverrides> {
        match self {
            SyntheticFile::Overrides(overrides) => Ok(overrides),
            SyntheticFile::Model(_) => Err(anyhow::anyhow!(
                "expected a `kind: overrides` document, found `kind: model`"
            )),
        }
    }
}

/// Marker type accepting only the literal `kind: model` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelKind {
    Model,
}

/// A complete, self-contained synthetic data model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyntheticModel {
    pub version: u32,
    pub kind: ModelKind,
    #[serde(default)]
    pub imports: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceModel>,
    pub defaults: ModelDefaults,
    #[serde(default)]
    pub seed: Option<u64>,
    pub output: OutputModel,
    pub tables: BTreeMap<String, TableModel>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileMetadata>,
}

/// Provenance and fingerprint policy for the source dump a model was
/// derived from (`source:` in the complete model example).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceModel {
    pub dialect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_policy: Option<FingerprintPolicy>,
}

/// How strictly a model requires the source fingerprint to match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FingerprintPolicy {
    Ignore,
    Warn,
    Require,
}

/// Inherited table/column behavior defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDefaults {
    pub inference: InferenceMode,
}

/// Whether columns without an explicit owner may fall back to schema-based
/// heuristics, or must always be explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceMode {
    Schema,
    Disabled,
}

/// Dialect and renderer defaults (`output:` in the complete model example).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputModel {
    pub dialect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<OutputMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inserts: Option<InsertMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
}

/// Whether rendering emits DDL, rows, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    SchemaAndData,
    SchemaOnly,
    DataOnly,
}

/// How PostgreSQL row output is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InsertMode {
    Auto,
    Insert,
    Copy,
}

/// A single table's complete generation rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableModel {
    #[serde(
        default,
        deserialize_with = "deserialize_table_seed",
        serialize_with = "serialize_table_seed",
        skip_serializing_if = "TableSeed::is_inherit"
    )]
    pub seed: TableSeed,
    pub rows: RowsModel,
    pub schema: PortableTable,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnRule>,
    #[serde(default)]
    pub relationships: Vec<RelationshipModel>,
    #[serde(default)]
    pub planners: Vec<PlannerConfig>,
}

/// A table's seed relative to the model's top-level seed: inherit it,
/// opt out of determinism entirely, or pin an independent seed.
///
/// YAML semantics: omitted means [`TableSeed::Inherit`], `null` means
/// [`TableSeed::Random`], and an integer means [`TableSeed::Fixed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableSeed {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}

impl TableSeed {
    /// Used by `skip_serializing_if` to omit the field for the common case.
    pub fn is_inherit(&self) -> bool {
        matches!(self, TableSeed::Inherit)
    }
}

/// Deserialize the tri-state seed shared by `TableModel::seed`: absent
/// (handled by `#[serde(default)]`) is `Inherit`, `null` is `Random`, and an
/// integer is `Fixed`.
pub(super) fn deserialize_table_seed<'de, D>(deserializer: D) -> Result<TableSeed, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<u64>::deserialize(deserializer)? {
        None => TableSeed::Random,
        Some(seed) => TableSeed::Fixed(seed),
    })
}

pub(super) fn serialize_table_seed<S>(seed: &TableSeed, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match seed {
        TableSeed::Inherit => serializer.serialize_none(),
        TableSeed::Random => serializer.serialize_none(),
        TableSeed::Fixed(value) => serializer.serialize_u64(*value),
    }
}

/// How many rows a table produces, and how those rows are derived.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowsModel {
    Fixed {
        count: u64,
    },
    Observed {
        count: u64,
    },
    Scale {
        base: u64,
        factor: f64,
        count: u64,
    },
    #[serde(rename = "relation.children")]
    RelationChildren {
        parent: String,
        count: u64,
        distribution: ChildDistribution,
    },
}

/// The fan-out distribution used to allocate a relationship child's rows
/// across its parents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChildDistribution {
    Observed { mean: f64, min: f64, max: f64 },
    Fixed { mean: f64, min: f64, max: f64 },
    Uniform { mean: f64, min: f64, max: f64 },
    Poisson { mean: f64, min: f64, max: f64 },
    Histogram { mean: f64, min: f64, max: f64 },
}

/// A single column's generation rule: an optional semantic annotation, an
/// optional generator owner, and an ordered modifier pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator: Option<GeneratorConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modifiers: Vec<ModifierConfig>,
}

/// A registry-resolved generator invocation. `kind` selects the registered
/// generator; the remaining fields are its typed arguments, opaque to the
/// document model and resolved later by `ExtensionRegistry`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratorConfig {
    pub kind: String,
    #[serde(flatten)]
    pub args: BTreeMap<String, serde_yaml_ng::Value>,
}

/// A registry-resolved modifier invocation; see [`GeneratorConfig`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModifierConfig {
    pub kind: String,
    #[serde(flatten)]
    pub args: BTreeMap<String, serde_yaml_ng::Value>,
}

/// A registry-resolved planner invocation; see [`GeneratorConfig`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannerConfig {
    pub kind: String,
    #[serde(flatten)]
    pub args: BTreeMap<String, serde_yaml_ng::Value>,
}

/// A declared generation relationship to another table.
///
/// This covers the common named foreign-key shape used throughout the
/// complete model example. Self-referential, polymorphic, and shaped
/// (tree/junction) relationships are a documented follow-up; see the
/// task report for why they are out of scope here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipModel {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub references: RelationshipReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<String>,
}

/// The `references:` half of a [`RelationshipModel`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipReference {
    pub table: String,
    pub columns: Vec<String>,
}

/// Removable evidence recorded for one profiled column
/// (`profiles."table.column"` in the complete model example). Deleting a
/// model's `profiles` map cannot change generation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileMetadata {
    pub rows: u64,
    pub null_fraction: f64,
    pub distinct_estimate: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference: Option<ProfileInference>,
}

/// The inference explanation attached to a [`ProfileMetadata`] entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileInference {
    pub selected: String,
    pub confidence: String,
    #[serde(default)]
    pub reasons: Vec<String>,
}
