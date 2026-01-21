//! JSON Schema generation for CLI output types.
//!
//! This module provides schema generation for all commands that support --json output.
//! Schemas are generated using the schemars crate and can be exported via the `schema` subcommand.

use schemars::{schema::RootSchema, schema_for};
use std::collections::BTreeMap;

/// Returns all JSON schemas for commands that support --json output.
/// Uses BTreeMap for deterministic ordering (important for diffable output).
pub fn all_schemas() -> BTreeMap<&'static str, RootSchema> {
    let mut schemas = BTreeMap::new();

    // analyze command
    schemas.insert(
        "analyze",
        schema_for!(crate::cmd::analyze::AnalyzeJsonOutput),
    );

    // convert command
    schemas.insert(
        "convert",
        schema_for!(crate::cmd::convert::ConvertJsonOutput),
    );

    // graph command (uses ErdJson from graph module)
    schemas.insert("graph", schema_for!(crate::graph::format::json::ErdJson));

    // merge command
    schemas.insert("merge", schema_for!(crate::cmd::merge::MergeJsonOutput));

    // redact command (uses RedactStats from redactor module)
    schemas.insert("redact", schema_for!(crate::redactor::RedactStats));

    // sample command
    schemas.insert("sample", schema_for!(crate::cmd::sample::SampleJsonOutput));

    // shard command
    schemas.insert("shard", schema_for!(crate::cmd::shard::ShardJsonOutput));

    // split command
    schemas.insert("split", schema_for!(crate::cmd::split::SplitJsonOutput));

    // validate command (uses ValidationSummary from validate module)
    schemas.insert("validate", schema_for!(crate::validate::ValidationSummary));

    schemas
}

/// Generate a single schema by command name.
pub fn get_schema(command: &str) -> Option<RootSchema> {
    all_schemas().remove(command)
}

/// List all available schema names.
pub fn schema_names() -> Vec<&'static str> {
    all_schemas().keys().copied().collect()
}
