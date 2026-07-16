//! Tests for the synthetic-data-generation diagnostics module and the typed
//! extension registry.

use sql_splitter::diagnostic::{Diagnostic, DiagnosticBag, Severity, SourceLocation};
use sql_splitter::generate::{
    Buffering, ColumnScope, CompileContext, CompiledGenerator, ConstantFactory, Determinism,
    ExtensionRegistry, GeneratedValue, GeneratorDescriptor, GeneratorFactory, RowContext,
    Verification,
};
use sql_splitter::synthetic::{GeneratorConfig, SqlTypeFamily};

#[test]
fn diagnostic_bag_keeps_independent_errors() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    bag.error(
        "GEN-MISSING-COLUMN",
        "tables.users.columns.email",
        "column does not exist",
    );
    assert_eq!(bag.errors().count(), 2);
    assert!(serde_json::to_value(&bag).unwrap()["diagnostics"].is_array());
}

#[test]
fn diagnostic_bag_into_result_errs_when_errors_present() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    assert!(bag.into_result(()).is_err());
}

#[test]
fn diagnostic_bag_into_result_oks_when_only_warnings_present() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "column is never referenced by a generator or planner",
    );
    assert_eq!(bag.into_result(42).unwrap(), 42);
}

#[test]
fn diagnostic_bag_into_result_oks_when_empty() {
    let bag = DiagnosticBag::default();
    assert_eq!(bag.into_result("value").unwrap(), "value");
}

#[test]
fn diagnostic_errors_excludes_warnings() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "unused column",
    );
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");
    assert_eq!(bag.errors().count(), 1);
    assert_eq!(bag.diagnostics.len(), 2);
}

#[test]
fn diagnostic_display_renders_code_path_related_and_help() {
    let diagnostic = Diagnostic {
        code: "GEN-COLUMN-OWNER-CONFLICT".to_string(),
        severity: Severity::Error,
        path: "tables.orders.columns.total".to_string(),
        message: "tables.orders.columns.total is produced by both:".to_string(),
        help: Some(
            "remove the column generator or remove `total` from the planner mapping".to_string(),
        ),
        related: vec![
            SourceLocation {
                path: "columns.total.generator".to_string(),
                description: None,
            },
            SourceLocation {
                path: "planners[0]".to_string(),
                description: Some("commerce.order_family".to_string()),
            },
        ],
    };

    let rendered = diagnostic.to_string();
    let expected = "error[GEN-COLUMN-OWNER-CONFLICT] tables.orders.columns.total\n  \
tables.orders.columns.total is produced by both:\n    \
- columns.total.generator\n    \
- planners[0] (commerce.order_family)\n  \
help: remove the column generator or remove `total` from the planner mapping";

    assert_eq!(rendered, expected);
}

#[test]
fn diagnostic_display_omits_help_and_related_when_absent() {
    let diagnostic = Diagnostic {
        code: "GEN-MISSING-TABLE".to_string(),
        severity: Severity::Error,
        path: "tables.orders".to_string(),
        message: "table does not exist".to_string(),
        help: None,
        related: Vec::new(),
    };

    assert_eq!(
        diagnostic.to_string(),
        "error[GEN-MISSING-TABLE] tables.orders\n  table does not exist"
    );
}

#[test]
fn diagnostic_json_shape_omits_absent_optional_fields() {
    let mut bag = DiagnosticBag::default();
    bag.error("GEN-MISSING-TABLE", "tables.orders", "table does not exist");

    let value = serde_json::to_value(&bag).unwrap();
    let diagnostics = value["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1);

    let first = &diagnostics[0];
    assert_eq!(first["code"], "GEN-MISSING-TABLE");
    assert_eq!(first["severity"], "error");
    assert_eq!(first["path"], "tables.orders");
    assert_eq!(first["message"], "table does not exist");
    assert!(first.get("help").is_none());
    assert!(first.get("related").is_none());
}

#[test]
fn diagnostic_json_roundtrips_through_serde() {
    let mut bag = DiagnosticBag::default();
    bag.warning(
        "GEN-UNUSED-COLUMN",
        "tables.orders.columns.legacy_flag",
        "unused column",
    );

    let json = serde_json::to_string(&bag).unwrap();
    let restored: DiagnosticBag = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.diagnostics, bag.diagnostics);
}

// --- Extension registry -----------------------------------------------------

/// A do-nothing compiled generator used by the collision test factories below.
struct NoopGenerator;

impl CompiledGenerator for NoopGenerator {
    fn generate(
        &mut self,
        _context: &RowContext<'_>,
        output: &mut GeneratedValue,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        *output = GeneratedValue::Null;
        Ok(())
    }
}

/// Builds a static generator descriptor for a test factory.
macro_rules! test_descriptor {
    ($kind:expr, $aliases:expr) => {
        &GeneratorDescriptor {
            kind: $kind,
            aliases: $aliases,
            summary: "test factory",
            arguments: &[],
            accepts: &[SqlTypeFamily::Text],
            writes: ColumnScope::OwnColumn,
            reads: ColumnScope::None,
            determinism: Determinism::Deterministic,
            buffering: Buffering::Streaming,
            verification: Verification::Unsupported,
        }
    };
}

macro_rules! test_factory {
    ($name:ident, $kind:expr, $aliases:expr) => {
        struct $name;
        impl GeneratorFactory for $name {
            fn descriptor(&self) -> &'static GeneratorDescriptor {
                test_descriptor!($kind, $aliases)
            }
            fn compile(
                &self,
                _config: &GeneratorConfig,
                _context: &CompileContext<'_>,
            ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
                Ok(Box::new(NoopGenerator))
            }
        }
    };
}

test_factory!(AlphaGen, "alpha", &["a1", "shared"]);
test_factory!(BetaGen, "beta", &["b1", "shared"]);
test_factory!(ShadowGen, "shadow", &["alpha"]);
test_factory!(ZuluGen, "zulu", &[]);

#[test]
fn registry_rejects_duplicate_kinds_and_resolves_aliases() {
    let mut registry = ExtensionRegistry::new();
    registry
        .register_generator(Box::new(ConstantFactory))
        .unwrap();
    assert_eq!(
        registry.generator("const").unwrap().descriptor().kind,
        "constant"
    );
    let err = registry
        .register_generator(Box::new(ConstantFactory))
        .unwrap_err();
    assert!(err.to_string().contains("GEN-REGISTRY-DUPLICATE"));
}

#[test]
fn registry_resolves_primary_and_alias_to_the_same_factory() {
    let mut registry = ExtensionRegistry::new();
    registry
        .register_generator(Box::new(ConstantFactory))
        .unwrap();
    assert_eq!(
        registry.generator("constant").unwrap().descriptor().kind,
        "constant"
    );
    assert_eq!(
        registry.generator("const").unwrap().descriptor().kind,
        "constant"
    );
    assert!(registry.generator("missing").is_none());
}

#[test]
fn registry_rejects_duplicate_aliases_across_factories() {
    let mut registry = ExtensionRegistry::new();
    registry.register_generator(Box::new(AlphaGen)).unwrap();
    let err = registry.register_generator(Box::new(BetaGen)).unwrap_err();
    assert!(err.to_string().contains("GEN-REGISTRY-ALIAS-DUPLICATE"));
    // The rejected factory must not have been partially installed.
    assert!(registry.generator("beta").is_none());
    assert!(registry.generator("b1").is_none());
}

#[test]
fn registry_rejects_aliases_that_shadow_primary_kinds() {
    let mut registry = ExtensionRegistry::new();
    registry.register_generator(Box::new(AlphaGen)).unwrap();
    let err = registry
        .register_generator(Box::new(ShadowGen))
        .unwrap_err();
    assert!(err.to_string().contains("GEN-REGISTRY-ALIAS-SHADOWS-KIND"));
}

#[test]
fn registry_lists_generators_in_deterministic_kind_order() {
    let mut registry = ExtensionRegistry::new();
    registry.register_generator(Box::new(ZuluGen)).unwrap();
    registry.register_generator(Box::new(AlphaGen)).unwrap();
    registry
        .register_generator(Box::new(ConstantFactory))
        .unwrap();
    let kinds: Vec<&str> = registry
        .generators()
        .map(|factory| factory.descriptor().kind)
        .collect();
    assert_eq!(kinds, vec!["alpha", "constant", "zulu"]);
}

#[test]
fn standard_registry_installs_the_constant_generator() {
    let registry = ExtensionRegistry::standard();
    assert_eq!(
        registry.generator("constant").unwrap().descriptor().kind,
        "constant"
    );
}
