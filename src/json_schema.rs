//! JSON Schema generation for CLI output types.
//!
//! This module provides schema generation for all commands that support --json output,
//! plus the editor-facing `generate-config.schema.json` for the `generate` command's
//! YAML input (see [`generate_config_schema`]). Schemas are generated using the
//! schemars crate and can be exported via the `schema` subcommand.

use schemars::{generate::SchemaSettings, transform::RestrictFormats, JsonSchema, Schema};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

use crate::generate::registry::{ArgumentSpec, ExtensionRegistry};

/// Returns all JSON schemas for commands that support --json output.
/// Uses BTreeMap for deterministic ordering (important for diffable output).
pub fn all_schemas() -> BTreeMap<&'static str, Schema> {
    let settings = SchemaSettings::default().with_transform(RestrictFormats::default());
    let generator = settings.into_generator();

    let mut schemas = BTreeMap::new();

    schemas.insert("generate-config", generate_config_schema());

    schemas.insert(
        "analyze",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::analyze::AnalyzeJsonOutput>(),
    );

    schemas.insert(
        "convert",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::convert::ConvertJsonOutput>(),
    );

    schemas.insert(
        "graph",
        generator
            .clone()
            .into_root_schema_for::<crate::graph::format::json::ErdJson>(),
    );

    schemas.insert(
        "merge",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::merge::MergeJsonOutput>(),
    );

    schemas.insert(
        "redact",
        generator
            .clone()
            .into_root_schema_for::<crate::redactor::RedactStats>(),
    );

    schemas.insert(
        "sample",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::sample::SampleJsonOutput>(),
    );

    schemas.insert(
        "shard",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::shard::ShardJsonOutput>(),
    );

    schemas.insert(
        "split",
        generator
            .clone()
            .into_root_schema_for::<crate::cmd::split::SplitJsonOutput>(),
    );

    schemas.insert(
        "validate",
        generator.into_root_schema_for::<crate::validate::ValidationSummary>(),
    );

    schemas
}

/// Generate a single schema by command name.
pub fn get_schema(command: &str) -> Option<Schema> {
    all_schemas().remove(command)
}

/// Schema-only view of the `generate` YAML root: either a `kind: model` or a
/// `kind: overrides` document. `#[schemars(untagged)]` makes schemars emit a
/// bare `oneOf` over the two variants' own schemas, rather than wrapping them
/// in an externally-tagged envelope — each variant's `kind` field already
/// discriminates. This type exists purely for schema generation; the real
/// parse path is [`crate::synthetic::model::SyntheticFile::parse_value`].
#[derive(JsonSchema)]
#[schemars(untagged)]
#[allow(dead_code)] // constructed only through the derive, never at runtime
enum GenerateConfigRoot {
    Model(crate::synthetic::model::SyntheticModel),
    Overrides(crate::synthetic::overrides::SyntheticOverrides),
}

/// Build the editor-facing JSON Schema for the `generate` command's `kind:
/// model` / `kind: overrides` YAML configs.
///
/// The structural shape (tables, columns, rows, relationships, ...) comes
/// straight from `schemars` deriving over [`GenerateConfigRoot`]. The one
/// part `schemars` cannot see is what a `generator:`/`modifiers:`/`planners:`
/// entry's `{ kind, ...args }` shape actually accepts: `args` is a
/// registry-resolved `BTreeMap<String, serde_yaml_ng::Value>` (see
/// [`crate::synthetic::model::GeneratorConfig`]), so on its own schemars can
/// only describe it as "any object". This function replaces that permissive
/// placeholder with a `oneOf` composed from [`ExtensionRegistry::standard`]'s
/// descriptors — one branch per registered generator/modifier/planner,
/// keying on `kind` (primary name and aliases) and listing that operator's
/// declared [`ArgumentSpec`]s (required ones enforced; see the
/// argument-strictness note below for why the argument set stays open).
///
/// This documents the *standard* registry only: a statically-linked custom
/// registry (extra generators/modifiers/planners compiled in by a
/// downstream embedder) is validated at runtime by the compiler, not by this
/// shipped schema.
///
/// Argument-strictness depth: each branch enumerates its `kind` (rejecting
/// an unregistered generator/modifier/planner kind) and requires any
/// argument [`ArgumentSpec`] marks `required`. Most branches are fully
/// closed (`additionalProperties: false`): an unrecognized argument name is
/// rejected. Two categories stay open (`additionalProperties: true`)
/// because a closed set there would reject committed, runtime-valid
/// fixtures, not because of a general gap in this task:
///
/// * **Every planner branch.** Every standard
///   [`PlannerDescriptor`](crate::generate::registry::PlannerDescriptor) ships an
///   empty `arguments` list even though its `compile` reads real config
///   keys (e.g. `relation.junction_pair` reads `columns`,
///   `left_relationship`, `right_relationship`); this is a metadata gap in
///   `src/generate/planners/**`, out of scope for this schema task.
/// * **The `unique` modifier only.** At least one committed fixture passes
///   `attempts` where `unique`'s `compile` actually reads `max_attempts`
///   (silently inert at runtime — the real parser never closes registry
///   argument maps either); closing this one branch would reject that
///   fixture.
///
/// Every `GeneratorConfig` branch and every other `ModifierConfig` branch
/// (`case`/`clamp`/`format`/`null_rate`/`prefix`/`round`/`suffix`/
/// `truncate`) has no such mismatch in any committed, validated fixture, so
/// those stay closed: an unrecognized argument name (e.g. a typo) is
/// rejected. Per-argument *value types* are still never enforced
/// (`ArgumentSpec` carries no value type).
pub fn generate_config_schema() -> Schema {
    let settings = SchemaSettings::default().with_transform(RestrictFormats::default());
    let generator = settings.into_generator();
    let mut schema = generator.into_root_schema_for::<GenerateConfigRoot>();

    let registry = ExtensionRegistry::standard();

    let generator_branches: Vec<Value> = registry
        .generators()
        .map(|factory| {
            let d = factory.descriptor();
            // Closed: no committed fixture uses an undeclared generator arg.
            operator_branch(d.kind, d.aliases, d.arguments, true)
        })
        .collect();
    let modifier_branches: Vec<Value> = registry
        .modifiers()
        .map(|factory| {
            let d = factory.descriptor();
            // Open only for `unique`, whose descriptor's `max_attempts`
            // diverges from at least one committed fixture's `attempts`.
            let closed = d.kind != "unique";
            operator_branch(d.kind, d.aliases, d.arguments, closed)
        })
        .collect();
    let planner_branches: Vec<Value> = registry
        .planners()
        .map(|factory| {
            let d = factory.descriptor();
            // Open: every standard planner ships an empty `arguments` list
            // despite reading real config keys (see doc comment above).
            operator_branch(d.kind, d.aliases, d.arguments, false)
        })
        .collect();

    let obj = schema.ensure_object();
    obj.insert(
        "$id".to_string(),
        json!("https://sql-splitter.dev/schemas/generate-config.schema.json"),
    );
    obj.insert("title".to_string(), json!("sql-splitter generate config"));
    obj.insert(
        "description".to_string(),
        json!(
            "Schema for the `sql-splitter generate` command's `kind: model` and \
             `kind: overrides` YAML documents. Add `# yaml-language-server: \
             $schema=https://sql-splitter.dev/schemas/generate-config.schema.json` \
             as the first line of a config file for editor validation and completion."
        ),
    );

    let defs = obj
        .get_mut("$defs")
        .and_then(Value::as_object_mut)
        .expect("schemars always emits a $defs object for a type with nested definitions");

    replace_def(defs, "GeneratorConfig", generator_branches);
    replace_def(defs, "ModifierConfig", modifier_branches);
    replace_def(defs, "PlannerConfig", planner_branches);

    schema
}

/// Replace `defs[name]` with `{"oneOf": branches}`, keeping any existing
/// `$defs`-level metadata (there is none today, but this avoids clobbering a
/// future schemars addition such as `"description"`).
fn replace_def(defs: &mut Map<String, Value>, name: &str, branches: Vec<Value>) {
    defs.insert(name.to_string(), json!({ "oneOf": branches }));
}

/// One `oneOf` branch for a single registered operator: `kind` is restricted
/// to the operator's primary name and aliases, each declared argument is
/// accepted (required arguments are required). When `closed` is `true`, no
/// other property is allowed (`additionalProperties: false`); when `false`,
/// extra properties are permitted — see `generate_config_schema`'s doc
/// comment for exactly which branches pass `false` and why.
fn operator_branch(
    kind: &str,
    aliases: &[&str],
    arguments: &[ArgumentSpec],
    closed: bool,
) -> Value {
    let mut kinds = vec![Value::String(kind.to_string())];
    kinds.extend(aliases.iter().map(|alias| Value::String(alias.to_string())));

    let mut properties = Map::new();
    properties.insert("kind".to_string(), json!({ "enum": kinds }));

    let mut required = vec!["kind".to_string()];
    for arg in arguments {
        properties.insert(arg.name.to_string(), json!({ "description": arg.summary }));
        if arg.required {
            required.push(arg.name.to_string());
        }
    }

    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": !closed,
    })
}
