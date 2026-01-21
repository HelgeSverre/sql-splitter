//! JSON Schema generation for CLI output types.
//!
//! This module provides schema generation for all commands that support --json output.
//! Schemas are generated using the schemars crate and can be exported via the `schema` subcommand.

use schemars::{
    schema::{RootSchema, Schema, SchemaObject},
    schema_for,
};
use std::collections::BTreeMap;

/// Non-standard format values that schemars generates for Rust numeric types.
/// These are not part of JSON Schema Draft-07 and cause validation errors with ajv.
const NONSTANDARD_FORMATS: &[&str] = &[
    "uint", "uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float",
    "double",
];

/// Returns all JSON schemas for commands that support --json output.
/// Uses BTreeMap for deterministic ordering (important for diffable output).
pub fn all_schemas() -> BTreeMap<&'static str, RootSchema> {
    let mut schemas = BTreeMap::new();

    // analyze command
    schemas.insert(
        "analyze",
        clean_schema(schema_for!(crate::cmd::analyze::AnalyzeJsonOutput)),
    );

    // convert command
    schemas.insert(
        "convert",
        clean_schema(schema_for!(crate::cmd::convert::ConvertJsonOutput)),
    );

    // graph command (uses ErdJson from graph module)
    schemas.insert(
        "graph",
        clean_schema(schema_for!(crate::graph::format::json::ErdJson)),
    );

    // merge command
    schemas.insert(
        "merge",
        clean_schema(schema_for!(crate::cmd::merge::MergeJsonOutput)),
    );

    // redact command (uses RedactStats from redactor module)
    schemas.insert(
        "redact",
        clean_schema(schema_for!(crate::redactor::RedactStats)),
    );

    // sample command
    schemas.insert(
        "sample",
        clean_schema(schema_for!(crate::cmd::sample::SampleJsonOutput)),
    );

    // shard command
    schemas.insert(
        "shard",
        clean_schema(schema_for!(crate::cmd::shard::ShardJsonOutput)),
    );

    // split command
    schemas.insert(
        "split",
        clean_schema(schema_for!(crate::cmd::split::SplitJsonOutput)),
    );

    // validate command (uses ValidationSummary from validate module)
    schemas.insert(
        "validate",
        clean_schema(schema_for!(crate::validate::ValidationSummary)),
    );

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

/// Clean a schema by removing non-standard format values.
fn clean_schema(mut schema: RootSchema) -> RootSchema {
    clean_schema_object(&mut schema.schema);
    for def in schema.definitions.values_mut() {
        if let Schema::Object(obj) = def {
            clean_schema_object(obj);
        }
    }
    schema
}

/// Recursively clean a schema object by removing non-standard formats.
fn clean_schema_object(obj: &mut SchemaObject) {
    // Remove non-standard format from this object
    if let Some(ref format) = obj.format {
        if NONSTANDARD_FORMATS.contains(&format.as_str()) {
            obj.format = None;
        }
    }

    // Clean nested object properties
    if let Some(ref mut obj_validation) = obj.object {
        for prop in obj_validation.properties.values_mut() {
            if let Schema::Object(prop_obj) = prop {
                clean_schema_object(prop_obj);
            }
        }
        for prop in obj_validation.pattern_properties.values_mut() {
            if let Schema::Object(prop_obj) = prop {
                clean_schema_object(prop_obj);
            }
        }
        if let Some(Schema::Object(ref mut additional)) = obj_validation.additional_properties.as_deref_mut() {
            clean_schema_object(additional);
        }
    }

    // Clean array items
    if let Some(ref mut array_validation) = obj.array {
        if let Some(ref mut items) = array_validation.items {
            match items {
                schemars::schema::SingleOrVec::Single(schema) => {
                    if let Schema::Object(item_obj) = schema.as_mut() {
                        clean_schema_object(item_obj);
                    }
                }
                schemars::schema::SingleOrVec::Vec(schemas) => {
                    for schema in schemas {
                        if let Schema::Object(item_obj) = schema {
                            clean_schema_object(item_obj);
                        }
                    }
                }
            }
        }
    }

    // Clean subschemas (allOf, anyOf, oneOf)
    if let Some(ref mut subschemas) = obj.subschemas {
        if let Some(ref mut all_of) = subschemas.all_of {
            for schema in all_of {
                if let Schema::Object(sub_obj) = schema {
                    clean_schema_object(sub_obj);
                }
            }
        }
        if let Some(ref mut any_of) = subschemas.any_of {
            for schema in any_of {
                if let Schema::Object(sub_obj) = schema {
                    clean_schema_object(sub_obj);
                }
            }
        }
        if let Some(ref mut one_of) = subschemas.one_of {
            for schema in one_of {
                if let Schema::Object(sub_obj) = schema {
                    clean_schema_object(sub_obj);
                }
            }
        }
    }
}
