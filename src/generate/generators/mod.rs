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
        SqlTypeFamily::Enum,
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
