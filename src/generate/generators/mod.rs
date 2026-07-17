//! Built-in generator factories.
//!
//! This module is the home of the generator catalog. Task 7 seeded it with a
//! single exemplar, [`ConstantFactory`]. [`core`] adds the rest of the Phase 1
//! catalog: the other literal/structural generators (`null`, `sequence`,
//! `copy`, `template`, `pattern`, `database_default`, `json_value`), the
//! typed random generators (`integer`, `decimal`, `boolean`, `string`,
//! `bytes`, `uuid`), `choice`/`weighted_choice`, and the Phase 1 modifiers.
//! Semantic/temporal/credential generators are Task 12; `relation.foreign_key`
//! is Task 13.

mod core;
pub(crate) mod observed;
pub(crate) mod relation;
pub(crate) mod semantic;

pub(crate) use core::register_all;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::GeneratorConfig;
use crate::synthetic::schema::SqlTypeFamily;

use super::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, Determinism,
    GeneratorDescriptor, GeneratorFactory, RowContext, Verification,
};
use super::value::{GenerateError, GeneratedValue};

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
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        let value = config
            .args
            .get("value")
            .map_or(GeneratedValue::Null, yaml_to_value);
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

/// Minimal YAML → [`GeneratedValue`] mapping for the constant exemplar.
///
/// Only the scalar shapes a hand-authored `value:` is likely to use are
/// mapped; the full, family-aware coercion (decimals with scale, byte
/// literals, typed date/time) is a Task 11 concern.
fn yaml_to_value(value: &serde_yaml_ng::Value) -> GeneratedValue {
    match value {
        serde_yaml_ng::Value::Null => GeneratedValue::Null,
        serde_yaml_ng::Value::Bool(b) => GeneratedValue::Boolean(*b),
        serde_yaml_ng::Value::Number(n) => match n.as_i64() {
            Some(i) => GeneratedValue::Integer(i128::from(i)),
            None => GeneratedValue::Text(n.to_string()),
        },
        serde_yaml_ng::Value::String(s) => GeneratedValue::Text(s.clone()),
        other => GeneratedValue::Text(
            serde_yaml_ng::to_string(other)
                .unwrap_or_default()
                .trim_end()
                .to_string(),
        ),
    }
}
