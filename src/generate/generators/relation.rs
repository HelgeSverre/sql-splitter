//! Foreign-key generator markers.
//!
//! `relation.foreign_key` and `relation.composite_key` name a column (or
//! columns) whose value is a reference to a parent row. Unlike ordinary column
//! generators, a foreign key cannot be produced from a per-row seed in
//! isolation: choosing a parent row requires cross-table state (the parent's
//! key domain and row count) that only the [`GenerationEngine`] owns. These
//! factories therefore compile into *markers*: they let an explicit
//! `{ kind: relation.foreign_key, ... }` rule type-check and carry its
//! `distribution` / `null_rate` arguments (folded into the compiled
//! relationship by the compiler), while the engine performs the actual per-row
//! selection through the relationship's [`KeyDomain`]. The compiled marker is
//! never executed as a column generator; if it ever is, it reports an error
//! rather than silently emitting a bogus value.
//!
//! [`GenerationEngine`]: crate::generate::engine::GenerationEngine
//! [`KeyDomain`]: crate::generate::engine::KeyDomain

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::model::GeneratorConfig;
use crate::synthetic::schema::SqlTypeFamily;

use super::super::registry::{
    ArgumentSpec, Buffering, ColumnScope, CompileContext, CompiledGenerator, Determinism,
    GeneratorDescriptor, GeneratorFactory, RowContext, Verification,
};
use super::super::value::{GenerateError, GeneratedValue};

/// Every family a foreign key can plausibly carry: integer ids, UUID/text keys,
/// and the `Other` catch-all for exotic key types.
const FK_FAMILIES: &[SqlTypeFamily] = &[
    SqlTypeFamily::Integer,
    SqlTypeFamily::BigInteger,
    SqlTypeFamily::Text,
    SqlTypeFamily::Uuid,
    SqlTypeFamily::Other,
];

const FK_ARGUMENTS: &[ArgumentSpec] = &[
    ArgumentSpec {
        name: "relationship",
        required: false,
        summary: "Name of the declared relationship this key follows.",
    },
    ArgumentSpec {
        name: "distribution",
        required: false,
        summary: "Parent-assignment distribution: uniform, sequential, weighted, or observed.",
    },
    ArgumentSpec {
        name: "null_rate",
        required: false,
        summary: "Fraction (0.0..=1.0) of children whose key is NULL (nullable columns only).",
    },
];

/// The `relation.foreign_key` marker: a single-column reference to a parent row.
pub struct ForeignKeyFactory;

static FOREIGN_KEY_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "relation.foreign_key",
    aliases: &["foreign_key"],
    summary: "References a parent row; assigned by the generation engine.",
    arguments: FK_ARGUMENTS,
    accepts: FK_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for ForeignKeyFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &FOREIGN_KEY_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(ForeignKeyMarker {
            kind: FOREIGN_KEY_DESCRIPTOR.kind,
        }))
    }
}

/// The `relation.composite_key` marker: a multi-column reference to a parent
/// row where every component is derived from one chosen parent row.
pub struct CompositeKeyFactory;

static COMPOSITE_KEY_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "relation.composite_key",
    aliases: &["composite_key"],
    summary: "References a parent row via a composite key; assigned by the engine.",
    arguments: FK_ARGUMENTS,
    accepts: FK_FAMILIES,
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for CompositeKeyFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COMPOSITE_KEY_DESCRIPTOR
    }

    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(ForeignKeyMarker {
            kind: COMPOSITE_KEY_DESCRIPTOR.kind,
        }))
    }
}

/// A compiled foreign-key marker. The engine assigns FK values directly from the
/// relationship's key domain and never calls this; running it as a plain column
/// generator is a bug, so it reports one instead of emitting a value.
struct ForeignKeyMarker {
    kind: &'static str,
}

impl CompiledGenerator for ForeignKeyMarker {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        _output: &mut GeneratedValue,
    ) -> Result<(), GenerateError> {
        Err(GenerateError::InvalidInput(format!(
            "`{}` is assigned by the generation engine, not executed as a column generator",
            self.kind
        )))
    }
}
