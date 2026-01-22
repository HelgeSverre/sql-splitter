//! JSON Schema generation for CLI output types.
//!
//! This module provides schema generation for all commands that support --json output.
//! Schemas are generated using the schemars crate and can be exported via the `schema` subcommand.

use schemars::{generate::SchemaSettings, transform::RestrictFormats, Schema};
use std::collections::BTreeMap;

/// Returns all JSON schemas for commands that support --json output.
/// Uses BTreeMap for deterministic ordering (important for diffable output).
pub fn all_schemas() -> BTreeMap<&'static str, Schema> {
    let settings = SchemaSettings::default().with_transform(RestrictFormats::default());
    let generator = settings.into_generator();

    let mut schemas = BTreeMap::new();

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

/// List all available schema names.
pub fn schema_names() -> Vec<&'static str> {
    all_schemas().keys().copied().collect()
}
