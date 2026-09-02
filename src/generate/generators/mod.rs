//! Built-in generator factories.
//!
//! This module is the home of the generator catalog. [`ConstantFactory`]
//! provides the minimal factory/compiled exemplar. [`core`] provides the
//! literal and structural generators (`null`, `sequence`,
//! `copy`, `template`, `pattern`, `database_default`, `json_value`), the
//! typed random generators (`integer`, `decimal`, `boolean`, `string`,
//! `bytes`, `uuid`), `choice`/`weighted_choice`, and the built-in modifiers.
//! [`semantic`] provides semantic, temporal, and credential generators;
//! [`relation`] provides relationship-aware generators.

mod core;
pub(crate) mod observed;
pub(crate) mod relation;
pub(crate) mod semantic;

pub(crate) use core::register_all;

use rand_chacha::ChaCha8Rng;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::GeneratorConfig;
use crate::synthetic::schema::{PortableColumn, PortableTable, SqlTypeFamily};

use super::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, Determinism,
    GeneratorDescriptor, GeneratorFactory, RowContext, Verification,
};
use super::seed::StreamId;
use super::value::{GenerateError, GeneratedValue};

// --- Shared helpers ----------------------------------------------------------

/// The target column of a column-scoped operator. Every generator/modifier
/// in this catalog is column-scoped, so the compiler always builds its
/// `CompileContext` with [`CompileContext::for_column`]; a missing column would
/// be a caller bug, not a data problem, hence the `expect`.
pub(super) fn column<'a>(context: &CompileContext<'a>) -> &'a PortableColumn {
    context
        .column()
        .expect("catalog generators and modifiers are column-scoped")
}

/// The deterministic RNG stream for a column-scoped operator, keyed by
/// table, column, and the operator's own kind so two different generators on
/// the same column never share a stream.
pub(super) fn stream(context: &CompileContext<'_>, kind: &str) -> ChaCha8Rng {
    let table = context.table().name.clone();
    let col = column(context).name.clone();
    context.rng(StreamId::column(table, col, kind.to_string()))
}

pub(super) fn find_column<'a>(table: &'a PortableTable, name: &str) -> Option<&'a PortableColumn> {
    table.columns.iter().find(|c| c.name == name)
}

/// Minimal YAML -> string rendering for scalar config values (used for
/// literal template fragments and `display`-style coercion).
pub(super) fn display_yaml(value: &serde_yaml_ng::Value) -> String {
    match value {
        serde_yaml_ng::Value::Null => String::new(),
        serde_yaml_ng::Value::Bool(b) => b.to_string(),
        serde_yaml_ng::Value::Number(n) => n.to_string(),
        serde_yaml_ng::Value::String(s) => s.clone(),
        other => serde_yaml_ng::to_string(other)
            .unwrap_or_default()
            .trim_end()
            .to_string(),
    }
}

pub(super) fn parse_i128(value: &serde_yaml_ng::Value) -> Option<i128> {
    match value {
        serde_yaml_ng::Value::Number(n) => n
            .as_i64()
            .map(i128::from)
            .or_else(|| n.as_f64().map(|f| f as i128)),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<i128>().ok(),
        _ => None,
    }
}

pub(super) fn parse_f64(value: &serde_yaml_ng::Value) -> Option<f64> {
    match value {
        serde_yaml_ng::Value::Number(n) => n.as_f64(),
        serde_yaml_ng::Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

pub(super) fn parse_usize(value: &serde_yaml_ng::Value) -> Option<usize> {
    parse_i128(value).and_then(|n| usize::try_from(n).ok())
}

/// The `constant` generator: emits the same configured value for every row.
///
/// It is the minimal end-to-end exemplar of the factory/compiled split — a
/// factory that reads one optional `value` argument and compiles it into a
/// [`CompiledConstant`] that overwrites the output slot with a fixed value.
pub struct ConstantFactory;

static CONSTANT_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "constant",
    aliases: &["const"],
    summary: "Emits the same configured value for every row.",
    arguments: &[ArgumentSpec {
        name: "value",
        required: false,
        summary: "The constant value to emit; SQL NULL when omitted.",
    }],
    accepts: &[
        SqlTypeFamily::Integer,
        SqlTypeFamily::BigInteger,
        SqlTypeFamily::Decimal,
        SqlTypeFamily::Boolean,
        SqlTypeFamily::Text,
        SqlTypeFamily::Bytes,
        SqlTypeFamily::Uuid,
        SqlTypeFamily::DateTime,
        SqlTypeFamily::Json,
        SqlTypeFamily::Other,
    ],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for ConstantFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &CONSTANT_DESCRIPTOR
    }

    fn compile(
        &self,
        config: &GeneratorConfig,
        context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let value = match config.args.get("value") {
            Some(value) => {
                let Some(column) = context.column() else {
                    let mut bag = DiagnosticBag::default();
                    bag.error(
                        crate::diagnostic::codes::CONSTANT_INVALID_VALUE.code,
                        context.path(),
                        "constant generator requires a target column",
                    );
                    return Err(bag);
                };
                core::coerce_value(value, &column.family).map_err(|message| {
                    let mut bag = DiagnosticBag::default();
                    bag.error(
                        crate::diagnostic::codes::CONSTANT_INVALID_VALUE.code,
                        context.path(),
                        message,
                    );
                    bag
                })?
            }
            None => GeneratedValue::Null,
        };
        Ok(Box::new(CompiledConstant { value }))
    }
}

/// The compiled form of [`ConstantFactory`]: a captured value replayed per row.
struct CompiledConstant {
    value: GeneratedValue,
}

impl CompiledGenerator for CompiledConstant {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        output.clone_from(&self.value);
        Ok(())
    }
}
