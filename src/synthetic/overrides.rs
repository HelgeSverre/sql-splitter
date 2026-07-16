//! The `kind: overrides` YAML document: a partial patch applied on top of
//! a source dump or a base `model`.
//!
//! Every field is optional. Missing fields mean "leave the source/base
//! value unchanged". Present lists (`relationships`, `planners`) replace
//! the base list wholesale rather than merging element-by-element.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::model::{
    ChildDistribution, GeneratorConfig, InsertMode, ModelDefaults, ModifierConfig, OutputMode,
    PlannerConfig, RelationshipModel, SourceModel,
};

/// Marker type accepting only the literal `kind: overrides` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverridesKind {
    Overrides,
}

/// A partial patch document applied on top of a source dump or base model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyntheticOverrides {
    pub version: u32,
    pub kind: OverridesKind,
    #[serde(default)]
    pub imports: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<ModelDefaults>,
    #[serde(
        default,
        deserialize_with = "deserialize_root_seed_override",
        serialize_with = "serialize_root_seed_override",
        skip_serializing_if = "RootSeedOverride::is_inherit"
    )]
    pub seed: RootSeedOverride,
    pub output: Option<OutputOverride>,
    #[serde(default)]
    pub tables: BTreeMap<String, TableOverride>,
}

/// The top-level seed override: leave the run seed alone, force it random
/// for this run, or pin a replacement.
///
/// YAML semantics mirror [`super::model::TableSeed`]: omitted is
/// [`RootSeedOverride::Inherit`], `null` is [`RootSeedOverride::Random`],
/// and an integer is [`RootSeedOverride::Fixed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RootSeedOverride {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}

impl RootSeedOverride {
    pub fn is_inherit(&self) -> bool {
        matches!(self, RootSeedOverride::Inherit)
    }
}

pub(super) fn deserialize_root_seed_override<'de, D>(
    deserializer: D,
) -> Result<RootSeedOverride, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<u64>::deserialize(deserializer)? {
        None => RootSeedOverride::Random,
        Some(seed) => RootSeedOverride::Fixed(seed),
    })
}

pub(super) fn serialize_root_seed_override<S>(
    seed: &RootSeedOverride,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match seed {
        RootSeedOverride::Inherit | RootSeedOverride::Random => serializer.serialize_none(),
        RootSeedOverride::Fixed(value) => serializer.serialize_u64(*value),
    }
}

/// A partial patch for `output:`; present fields replace the base value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<OutputMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inserts: Option<InsertMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
}

/// A partial patch for one table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOverride {
    #[serde(
        default,
        deserialize_with = "deserialize_table_seed_override",
        serialize_with = "serialize_table_seed_override",
        skip_serializing_if = "TableSeedOverride::is_inherit"
    )]
    pub seed: TableSeedOverride,
    pub rows: Option<RowsOverride>,
    pub schema: Option<PortableTableOverride>,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnRuleOverride>,
    pub relationships: Option<Vec<RelationshipModel>>,
    pub planners: Option<Vec<PlannerConfig>>,
}

/// A table's seed override; see [`RootSeedOverride`] for YAML semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableSeedOverride {
    #[default]
    Inherit,
    Random,
    Fixed(u64),
}

impl TableSeedOverride {
    pub fn is_inherit(&self) -> bool {
        matches!(self, TableSeedOverride::Inherit)
    }
}

pub(super) fn deserialize_table_seed_override<'de, D>(
    deserializer: D,
) -> Result<TableSeedOverride, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match Option::<u64>::deserialize(deserializer)? {
        None => TableSeedOverride::Random,
        Some(seed) => TableSeedOverride::Fixed(seed),
    })
}

pub(super) fn serialize_table_seed_override<S>(
    seed: &TableSeedOverride,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match seed {
        TableSeedOverride::Inherit | TableSeedOverride::Random => serializer.serialize_none(),
        TableSeedOverride::Fixed(value) => serializer.serialize_u64(*value),
    }
}

/// The tag for [`RowsOverride::kind`]; standalone since an override may
/// carry only a subset of the fields a complete [`super::model::RowsModel`]
/// requires (for example, `{ kind: observed, scale: 0.01 }`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowsKind {
    Fixed,
    Observed,
    Scale,
    #[serde(rename = "relation.children")]
    RelationChildren,
}

/// A partial patch for `rows:`. Unlike [`super::model::RowsModel`], a count
/// or distribution may be omitted when the source/base model supplies it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowsOverride {
    pub kind: RowsKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribution: Option<ChildDistribution>,
}

/// A partial patch for `schema:`. Overrides cannot redefine DDL structure
/// (see the design's "source, model, and overrides merge predictably"
/// section), so this only covers non-structural annotations.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableTableOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_statement: Option<String>,
}

/// A partial patch for one column's generation rule.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnRuleOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generator: Option<GeneratorConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifiers: Option<Vec<ModifierConfig>>,
}
