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

// --- Model compiler: selection and exact row counts -------------------------

use sql_splitter::generate::{
    ColumnOwner, CompileOptions, CompiledPlanner, ExecutionPhase, GenerateError, GenerationPlan,
    ModelCompiler, PlanContext, PlannerDescriptor, PlannerFactory, TableCountOverride,
};
use sql_splitter::synthetic::{
    ColumnRule, GeneratorConfig as GenConfig, PlannerConfig, SyntheticFile, SyntheticModel,
};

// --- Task 10 test operators -------------------------------------------------
//
// The standard registry only ships the family-agnostic `constant` generator,
// so ownership/type/cycle validation needs a few narrow test operators: a
// generator that accepts only integer families (to trigger `GEN-GENERATOR-TYPE`
// against a text column), a generator that reads a sibling column (to build
// read/write cycles), and a planner that owns and reads columns named in its
// config (to trigger conflicts and planner-owned cycles).

/// A generator that only accepts integer families.
struct IntegerOnlyFactory;

static INTEGER_ONLY_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "integer",
    aliases: &[],
    summary: "test integer-only generator",
    arguments: &[],
    accepts: &[SqlTypeFamily::Integer, SqlTypeFamily::BigInteger],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::None,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for IntegerOnlyFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &INTEGER_ONLY_DESCRIPTOR
    }
    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(NoopGenerator))
    }
}

/// A generator that reads one sibling column named in its `reads` argument.
struct CopyOfFactory;

static COPY_OF_DESCRIPTOR: GeneratorDescriptor = GeneratorDescriptor {
    kind: "copy_of",
    aliases: &[],
    summary: "test cross-column generator",
    arguments: &[],
    accepts: &[
        SqlTypeFamily::Integer,
        SqlTypeFamily::BigInteger,
        SqlTypeFamily::Text,
    ],
    writes: ColumnScope::OwnColumn,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

impl GeneratorFactory for CopyOfFactory {
    fn descriptor(&self) -> &'static GeneratorDescriptor {
        &COPY_OF_DESCRIPTOR
    }
    fn compile(
        &self,
        _config: &GeneratorConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledGenerator>, DiagnosticBag> {
        Ok(Box::new(NoopGenerator))
    }
}

/// A planner that owns and reads the columns named in its config.
struct TestFamilyFactory;

static TEST_FAMILY_DESCRIPTOR: PlannerDescriptor = PlannerDescriptor {
    kind: "test.family",
    aliases: &[],
    summary: "test column-owning planner",
    arguments: &[],
    writes: ColumnScope::Configured,
    reads: ColumnScope::Configured,
    determinism: Determinism::Deterministic,
    buffering: Buffering::Streaming,
    verification: Verification::Unsupported,
};

struct NoopPlanner;

impl CompiledPlanner for NoopPlanner {
    fn plan(&mut self, _context: &PlanContext<'_>) -> Result<(), GenerateError> {
        Ok(())
    }
}

impl PlannerFactory for TestFamilyFactory {
    fn descriptor(&self) -> &'static PlannerDescriptor {
        &TEST_FAMILY_DESCRIPTOR
    }
    fn compile(
        &self,
        _config: &PlannerConfig,
        _context: &CompileContext<'_>,
    ) -> Result<Box<dyn CompiledPlanner>, DiagnosticBag> {
        Ok(Box::new(NoopPlanner))
    }
}

/// A compiler backed by the standard registry plus the Task 10 test operators.
/// The extra operators are inert for the Task 9 count/selection tests.
fn compiler() -> ModelCompiler {
    let mut registry = ExtensionRegistry::standard();
    registry
        .register_generator(Box::new(IntegerOnlyFactory))
        .unwrap();
    registry
        .register_generator(Box::new(CopyOfFactory))
        .unwrap();
    registry
        .register_planner(Box::new(TestFamilyFactory))
        .unwrap();
    ModelCompiler::new(registry)
}

/// A `GeneratorConfig` for the named generator kind, with no arguments.
fn generator(kind: &str) -> GenConfig {
    GenConfig {
        kind: kind.to_string(),
        args: Default::default(),
    }
}

/// A `test.family` planner whose `columns` mapping claims ownership of `column`.
fn order_planner_owning(column: &str) -> PlannerConfig {
    let mut columns = serde_yaml_ng::Mapping::new();
    columns.insert(
        serde_yaml_ng::Value::from("total"),
        serde_yaml_ng::Value::from(column),
    );
    let mut args = std::collections::BTreeMap::new();
    args.insert(
        "columns".to_string(),
        serde_yaml_ng::Value::Mapping(columns),
    );
    PlannerConfig {
        kind: "test.family".to_string(),
        args,
    }
}

/// A two-table model (`users` with `id`/`email`, `orders` with `id`/`total`)
/// whose non-key columns carry empty rules so tests can attach generators and
/// planners. Every table's `id` is a bare integer primary key (a database
/// sequence), so the baseline model is otherwise owner-complete.
fn invalid_model() -> SyntheticModel {
    let mut model = model_from_yaml(
        r#"
version: 1
kind: model
seed: 7
tables:
  users:
    rows: { kind: fixed, count: 10 }
    schema:
      name: users
      columns:
        - { name: id, type: integer, nullable: false, primary_key: true }
        - { name: email, type: text, nullable: false }
    columns:
      email: {}
  orders:
    rows: { kind: fixed, count: 10 }
    schema:
      name: orders
      columns:
        - { name: id, type: integer, nullable: false, primary_key: true }
        - { name: total, type: integer, nullable: false }
    columns:
      total: {}
"#,
    );
    // Ensure the mutable column entries the headline test indexes exist.
    model
        .tables
        .get_mut("orders")
        .unwrap()
        .columns
        .entry("total".to_string())
        .or_insert_with(empty_rule);
    model
        .tables
        .get_mut("users")
        .unwrap()
        .columns
        .entry("email".to_string())
        .or_insert_with(empty_rule);
    model
}

/// An empty column rule (no semantic, generator, or modifiers).
fn empty_rule() -> ColumnRule {
    ColumnRule {
        semantic: None,
        generator: None,
        modifiers: Vec::new(),
    }
}

/// Parse a `kind: model` YAML document into a [`SyntheticModel`].
fn model_from_yaml(yaml: &str) -> SyntheticModel {
    SyntheticFile::parse_str(yaml)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model")
}

/// A two-table `customers` (root, `fixed`) → `orders` (`relation.children`)
/// model. `mean` is the fan-out; the child's minimum is one per parent so the
/// impossibility check has teeth.
fn customers_orders_model(customers: u64, orders_base: u64, mean: f64) -> SyntheticModel {
    model_from_yaml(&format!(
        r#"
version: 1
kind: model
source:
  dialect: postgres
seed: 7
tables:
  customers:
    rows: {{ kind: fixed, count: {customers} }}
    schema:
      name: customers
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: {orders_base}
      distribution: {{ kind: fixed, mean: {mean:?}, min: 1.0, max: 1000000.0 }}
    schema:
      name: orders
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: customer_id, type: bigint, nullable: false }}
      relationships:
        - {{ columns: [customer_id], referenced_table: customers, referenced_columns: [id] }}
"#
    ))
}

/// A single root `users` table with a `fixed` row count and no source.
fn users_model(users: u64) -> SyntheticModel {
    model_from_yaml(&format!(
        r#"
version: 1
kind: model
seed: 7
tables:
  users:
    rows: {{ kind: fixed, count: {users} }}
    schema:
      name: users
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
"#
    ))
}

#[test]
fn child_counts_are_not_scaled_twice() {
    let model = customers_orders_model(1_000, 4_000, 4.0);
    let options = CompileOptions {
        scale: Some(0.1),
        ..Default::default()
    };
    let plan = compiler().compile(model, options).unwrap();
    assert_eq!(plan.table("customers").unwrap().rows, 100);
    assert_eq!(plan.table("orders").unwrap().rows, 400);
}

#[test]
fn absolute_table_rows_win_and_max_rows_is_last() {
    let options = CompileOptions {
        scale: Some(0.1),
        table_rows: vec![TableCountOverride::rows("users", 500)],
        max_rows: Some(300),
        ..Default::default()
    };
    assert_eq!(
        compiler()
            .compile(users_model(10_000), options)
            .unwrap()
            .table("users")
            .unwrap()
            .rows,
        300
    );
}

#[test]
fn count_scale_and_rows_controls_conflict() {
    let options = CompileOptions {
        scale: Some(0.1),
        rows: Some(500),
        ..Default::default()
    };
    let err = compiler()
        .compile(users_model(10_000), options)
        .unwrap_err();
    assert!(err.to_string().contains("GEN-COUNT-CONTROL-CONFLICT"));
}

#[test]
fn table_rows_and_table_scale_conflict_on_the_same_table() {
    let options = CompileOptions {
        table_rows: vec![
            TableCountOverride::rows("users", 500),
            TableCountOverride::scale("users", 2.0),
        ],
        ..Default::default()
    };
    let err = compiler()
        .compile(users_model(10_000), options)
        .unwrap_err();
    assert!(err.to_string().contains("GEN-TABLE-COUNT-CONFLICT"));
}

#[test]
fn child_table_scale_multiplies_the_derived_count() {
    let options = CompileOptions {
        table_rows: vec![TableCountOverride::scale("orders", 0.5)],
        ..Default::default()
    };
    let plan = compiler()
        .compile(customers_orders_model(1_000, 4_000, 4.0), options)
        .unwrap();
    assert_eq!(plan.table("customers").unwrap().rows, 1_000);
    // 1000 parents * 4.0 fan-out = 4000, then * 0.5 table-scale = 2000.
    assert_eq!(plan.table("orders").unwrap().rows, 2_000);
}

#[test]
fn child_count_derives_from_the_distribution_mean() {
    let plan = compiler()
        .compile(
            customers_orders_model(500, 4_000, 3.0),
            CompileOptions::default(),
        )
        .unwrap();
    assert_eq!(plan.table("orders").unwrap().rows, 1_500);
}

#[test]
fn minimum_child_count_impossibility_is_an_error() {
    // 1000 parents with a minimum of one child each need >= 1000 children;
    // forcing 500 is impossible.
    let options = CompileOptions {
        table_rows: vec![TableCountOverride::rows("orders", 500)],
        ..Default::default()
    };
    let err = compiler()
        .compile(customers_orders_model(1_000, 4_000, 4.0), options)
        .unwrap_err();
    assert!(err.to_string().contains("GEN-CHILD-COUNT-IMPOSSIBLE"));
}

#[test]
fn stochastic_rounding_is_deterministic_before_emission() {
    let model = || {
        model_from_yaml(
            r#"
version: 1
kind: model
seed: 7
tables:
  widgets:
    rows: { kind: fixed, count: 10 }
    schema:
      name: widgets
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
"#,
        )
    };
    let options = || CompileOptions {
        scale: Some(0.15),
        ..Default::default()
    };
    // 10 * 0.15 = 1.5 -> stochastically rounds to 1 or 2, but stably so.
    let first = compiler()
        .compile(model(), options())
        .unwrap()
        .table("widgets")
        .unwrap()
        .rows;
    let second = compiler()
        .compile(model(), options())
        .unwrap()
        .table("widgets")
        .unwrap()
        .rows;
    assert_eq!(first, second);
    assert!(first == 1 || first == 2, "expected 1 or 2, got {first}");
}

#[test]
fn observed_rows_resolve_from_an_attached_source_count() {
    let model = model_from_yaml(
        r#"
version: 1
kind: model
source:
  dialect: mysql
tables:
  events:
    rows: { kind: observed, count: 3200 }
    schema:
      name: events
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
"#,
    );
    let plan = compiler()
        .compile(model, CompileOptions::default())
        .unwrap();
    assert_eq!(plan.table("events").unwrap().rows, 3200);
}

#[test]
fn observed_rows_without_a_source_count_are_an_error() {
    let model = model_from_yaml(
        r#"
version: 1
kind: model
tables:
  events:
    rows: { kind: observed, count: 3200 }
    schema:
      name: events
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
"#,
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.to_string().contains("GEN-ROWS-OBSERVED-MISSING"));
}

#[test]
fn excluded_required_dependency_reports_the_parent_path() {
    let options = CompileOptions {
        exclude: vec!["customers".to_string()],
        ..Default::default()
    };
    let err = compiler()
        .compile(customers_orders_model(1_000, 4_000, 4.0), options)
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("GEN-EXCLUDED-DEPENDENCY"));
    assert!(text.contains("orders"));
    assert!(text.contains("customers"));
}

#[test]
fn max_rows_cap_emits_a_warning_that_survives_a_successful_compile() {
    let options = CompileOptions {
        max_rows: Some(300),
        ..Default::default()
    };
    let plan = compiler().compile(users_model(10_000), options).unwrap();
    assert_eq!(plan.table("users").unwrap().rows, 300);
    assert!(plan
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "GEN-MAX-ROWS-CAPPED"));
}

#[test]
fn phases_and_columns_are_ordered_and_initially_unowned() {
    let plan = compiler()
        .compile(
            customers_orders_model(1_000, 4_000, 4.0),
            CompileOptions::default(),
        )
        .unwrap();
    let phases: Vec<&str> = plan
        .phases
        .iter()
        .map(|phase| match phase {
            ExecutionPhase::Table(name) => name.as_str(),
        })
        .collect();
    assert_eq!(phases, vec!["customers", "orders"]);

    let customers = plan.table("customers").unwrap();
    assert_eq!(customers.columns.len(), 1);
    // The bare bigint primary key is a database sequence.
    assert!(matches!(
        customers.columns[0].owner,
        ColumnOwner::GeneratedByDatabase
    ));
}

#[test]
fn mutual_relation_children_counts_are_a_cycle_error() {
    // `a`'s count derives from `b` and `b`'s from `a`: an unresolvable count
    // cycle that must be reported, not silently resolved to zero rows.
    let model = model_from_yaml(
        r#"
version: 1
kind: model
seed: 7
tables:
  a:
    rows:
      kind: relation.children
      parent: b
      count: 100
      distribution: { kind: fixed, mean: 2.0, min: 1.0, max: 100.0 }
    schema:
      name: a
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: b_id, type: bigint, nullable: false }
  b:
    rows:
      kind: relation.children
      parent: a
      count: 100
      distribution: { kind: fixed, mean: 2.0, min: 1.0, max: 100.0 }
    schema:
      name: b
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: a_id, type: bigint, nullable: false }
"#,
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("GEN-ROWS-CYCLE"));
    assert!(text.contains('a') && text.contains('b'));
}

#[test]
fn self_referential_relationship_is_not_a_cycle() {
    // A self-referential FK (manager_id -> employees) is stripped from the
    // dependency graph; the table's own `fixed` count resolves normally.
    let model = model_from_yaml(
        r#"
version: 1
kind: model
seed: 7
tables:
  employees:
    rows: { kind: fixed, count: 50 }
    schema:
      name: employees
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: manager_id, type: bigint, nullable: true }
    relationships:
      - { columns: [manager_id], references: { table: employees, columns: [id] } }
"#,
    );
    let plan = compiler()
        .compile(model, CompileOptions::default())
        .unwrap();
    assert_eq!(plan.table("employees").unwrap().rows, 50);
}

#[test]
fn root_table_scale_replaces_the_global_scale() {
    // Global `--scale 0.5` AND `--table-scale users=0.1`: the per-table scale
    // REPLACES the global one (10_000 * 0.1 = 1_000), it does not compound to
    // 10_000 * 0.5 * 0.1 = 500.
    let options = CompileOptions {
        scale: Some(0.5),
        table_rows: vec![TableCountOverride::scale("users", 0.1)],
        ..Default::default()
    };
    let plan = compiler().compile(users_model(10_000), options).unwrap();
    assert_eq!(plan.table("users").unwrap().rows, 1_000);
}

/// Type stubs forward-referenced by the plan (`GenerationPlan`) must exist and
/// be constructible so downstream tasks (10, 13, 22) can extend them.
#[allow(dead_code)]
fn plan_type_is_debuggable(plan: &GenerationPlan) -> String {
    format!("{plan:?}")
}

// --- Task 10: ownership, types, and dependency graphs -----------------------

#[test]
fn compiler_reports_all_independent_ownership_and_type_errors() {
    let mut model = invalid_model();
    model
        .tables
        .get_mut("orders")
        .unwrap()
        .columns
        .get_mut("total")
        .unwrap()
        .generator = Some(generator("integer"));
    model
        .tables
        .get_mut("orders")
        .unwrap()
        .planners
        .push(order_planner_owning("total"));
    model
        .tables
        .get_mut("users")
        .unwrap()
        .columns
        .get_mut("email")
        .unwrap()
        .generator = Some(generator("integer"));

    let err = compiler().compile(model, Default::default()).unwrap_err();
    assert!(err.has_code("GEN-COLUMN-OWNER-CONFLICT"));
    assert!(err.has_code("GEN-GENERATOR-TYPE"));
    assert_eq!(err.errors().count(), 2);
}

/// A single-table model with `id` (integer PK) plus the supplied extra column
/// lines and per-column/planner rules, for the focused ownership cases.
fn ownership_model(columns: &str, rules: &str) -> SyntheticModel {
    model_from_yaml(&format!(
        r#"
version: 1
kind: model
seed: 7
tables:
  t:
    rows: {{ kind: fixed, count: 5 }}
    schema:
      name: t
      columns:
        - {{ name: id, type: integer, nullable: false, primary_key: true }}
{columns}
{rules}
"#
    ))
}

#[test]
fn ownership_reports_missing_owner_under_disabled_inference() {
    let model = ownership_model("        - { name: note, type: text, nullable: false }", "");
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.has_code("GEN-COLUMN-OWNER-MISSING"));
    assert_eq!(err.errors().count(), 1);
}

#[test]
fn ownership_reports_generator_type_mismatch() {
    let model = ownership_model(
        "        - { name: label, type: text, nullable: false }",
        "    columns:\n      label:\n        generator: { kind: integer }",
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.has_code("GEN-GENERATOR-TYPE"));
    assert_eq!(err.errors().count(), 1);
}

#[test]
fn ownership_reports_unknown_generator_kind() {
    let model = ownership_model(
        "        - { name: label, type: text, nullable: false }",
        "    columns:\n      label:\n        generator: { kind: nonesuch }",
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.has_code("GEN-GENERATOR-UNKNOWN"));
    assert_eq!(err.errors().count(), 1);
}

#[test]
fn ownership_reports_unresolved_planner_relationship() {
    let model = ownership_model(
        "",
        "    planners:\n      - { kind: test.family, relationship: nope }",
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.has_code("GEN-RELATIONSHIP-UNKNOWN"));
    assert_eq!(err.errors().count(), 1);
}

#[test]
fn ownership_reports_column_read_write_cycle() {
    let model = ownership_model(
        "        - { name: a, type: integer, nullable: false }\n        - { name: b, type: integer, nullable: false }",
        "    columns:\n      a:\n        generator: { kind: copy_of, reads: [b] }\n      b:\n        generator: { kind: copy_of, reads: [a] }",
    );
    let err = compiler()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(err.has_code("GEN-COLUMN-CYCLE"));
    assert_eq!(err.errors().count(), 1);
}

#[test]
fn ownership_allows_cycle_owned_by_one_planner() {
    let model = ownership_model(
        "        - { name: a, type: integer, nullable: false }\n        - { name: b, type: integer, nullable: false }",
        "    planners:\n      - { kind: test.family, columns: { x: a, y: b }, reads: [a, b] }",
    );
    let plan = compiler()
        .compile(model, CompileOptions::default())
        .unwrap();
    let table = plan.table("t").unwrap();
    let a = table.columns.iter().find(|c| c.schema.name == "a").unwrap();
    assert!(matches!(
        a.owner,
        ColumnOwner::Planner {
            planner_index: 0,
            ..
        }
    ));
}

#[test]
fn ownership_allows_database_default_column() {
    let model = ownership_model(
        "        - { name: created_at, type: timestamptz, nullable: false, default_sql: \"now()\" }",
        "",
    );
    let plan = compiler()
        .compile(model, CompileOptions::default())
        .unwrap();
    let table = plan.table("t").unwrap();
    let created = table
        .columns
        .iter()
        .find(|c| c.schema.name == "created_at")
        .unwrap();
    assert!(matches!(created.owner, ColumnOwner::DatabaseDefault));
}
