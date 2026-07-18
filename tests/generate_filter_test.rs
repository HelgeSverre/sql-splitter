//! Integration coverage for DDL filtering and cross-dialect output.
//!
//! Covers the selection/exclusion filter matrix, the required-vs-optional
//! dependency split (`GEN-EXCLUDED-DEPENDENCY` vs the detachable
//! `GEN-DETACHED-DEPENDENCY` warning), same-dialect DDL preservation vs
//! cross-dialect/filtered normalization, lossy type-mapping warnings, and the
//! identity-insert loadability wrappers (MSSQL `SET IDENTITY_INSERT`,
//! PostgreSQL `OVERRIDING SYSTEM VALUE`).

use sql_splitter::diagnostic::Severity;
use sql_splitter::generate::{
    CompileOptions, GenerationEngine, GenerationPlan, ModelCompiler, RenderOptions, SqlRenderer,
};
use sql_splitter::parser::SqlDialect;
use sql_splitter::synthetic::{OutputMode, SyntheticFile};

const DIALECTS: [SqlDialect; 4] = [
    SqlDialect::MySql,
    SqlDialect::Postgres,
    SqlDialect::Sqlite,
    SqlDialect::Mssql,
];

/// Compile `yaml` with the given `--tables`/`--exclude` selection.
fn compile(yaml: &str, tables: &[&str], exclude: &[&str]) -> Result<GenerationPlan, String> {
    let model = SyntheticFile::parse_str(yaml)
        .expect("valid model yaml")
        .into_model()
        .expect("document is a model");
    let options = CompileOptions {
        tables: tables.iter().map(|s| s.to_string()).collect(),
        exclude: exclude.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    ModelCompiler::standard()
        .compile(model, options)
        .map_err(|bag| bag.to_string())
}

/// Compile with no selection (every table), panicking on a compile error.
fn compile_all(yaml: &str) -> GenerationPlan {
    compile(yaml, &[], &[]).expect("model compiles cleanly")
}

/// Render `plan` to SQL under `dialect`/`source_dialect`/`mode`, returning the
/// rendered text and the number of renderer warnings (e.g. lossy conversions).
fn render(
    plan: GenerationPlan,
    dialect: SqlDialect,
    source_dialect: Option<SqlDialect>,
    mode: OutputMode,
) -> (String, usize) {
    let mut buffer: Vec<u8> = Vec::new();
    let options = RenderOptions {
        dialect,
        source_dialect,
        mode,
        no_copy: false,
        batch_size: 1000,
        ..RenderOptions::default()
    };
    let mut renderer = SqlRenderer::new(&mut buffer, options);
    GenerationEngine::new(plan)
        .run(&mut renderer)
        .expect("engine renders the plan");
    let warnings = renderer.warnings().len();
    renderer.finish().expect("renderer flushes");
    (String::from_utf8(buffer).expect("utf-8 sql"), warnings)
}

/// A two-table model whose `orders.customer_id` FK to `customers` is nullable
/// (`optional = true`) or `NOT NULL`, and where each table carries a distinctive
/// raw `create_statement` so DDL preservation is observable.
fn customers_orders(optional: bool) -> String {
    let nullable = optional;
    format!(
        r#"
version: 1
kind: model
source: {{ dialect: mysql }}
defaults: {{ inference: schema }}
seed: 7
tables:
  customers:
    rows: {{ kind: fixed, count: 3 }}
    schema:
      name: customers
      create_statement: "CREATE TABLE customers (id BIGINT) /*RAW-CUSTOMERS*/;"
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
  orders:
    rows: {{ kind: fixed, count: 4 }}
    schema:
      name: orders
      create_statement: "CREATE TABLE orders (id BIGINT, customer_id BIGINT) /*RAW-ORDERS*/;"
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: customer_id, type: bigint, nullable: {nullable} }}
      relationships:
        - {{ name: fk_customer, columns: [customer_id], referenced_table: customers, referenced_columns: [id] }}
"#
    )
}

// --- Selection / exclusion matrix -------------------------------------------

#[test]
fn all_tables_same_dialect_preserves_original_ddl() {
    let plan = compile_all(&customers_orders(false));
    let (sql, _) = render(
        plan,
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    // Nothing was filtered and the dialects match, so the raw DDL survives
    // byte-for-byte instead of being re-rendered from the normalized schema.
    assert!(
        sql.contains("/*RAW-CUSTOMERS*/"),
        "raw customers DDL: {sql}"
    );
    assert!(sql.contains("/*RAW-ORDERS*/"), "raw orders DDL: {sql}");
}

#[test]
fn cross_dialect_output_normalizes_even_when_unfiltered() {
    let plan = compile_all(&customers_orders(false));
    let (sql, _) = render(
        plan,
        SqlDialect::Postgres,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    // A different output dialect discards the raw MySQL DDL and re-renders.
    assert!(!sql.contains("/*RAW"), "cross-dialect kept raw DDL: {sql}");
    assert!(sql.contains("CREATE TABLE \"orders\""));
}

#[test]
fn filtering_forces_normalized_ddl_even_in_the_source_dialect() {
    // Excluding any table means the schema set changed, so every retained table
    // renders from its normalized schema even though the dialect is unchanged.
    let plan = compile(&customers_orders(true), &[], &["customers"]).expect("optional FK detaches");
    let (sql, _) = render(
        plan,
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert!(!sql.contains("/*RAW"), "filtered run kept raw DDL: {sql}");
    assert!(sql.contains("CREATE TABLE `orders`"));
}

#[test]
fn excluded_independent_table_is_absent_from_output() {
    // `customers` has no required dependents once `orders`' FK is optional.
    let plan = compile(&customers_orders(true), &[], &["customers"]).expect("compiles");
    let (sql, _) = render(
        plan,
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert!(!sql.contains("customers"), "excluded table leaked: {sql}");
}

#[test]
fn include_and_exclude_collision_lets_exclude_win() {
    // `orders` appears in both lists; exclusion always wins.
    let plan = compile(
        &customers_orders(true),
        &["orders", "customers"],
        &["orders"],
    )
    .expect("compiles");
    assert!(plan.table("orders").is_none(), "exclude did not win");
    assert!(plan.table("customers").is_some());
}

#[test]
fn exact_names_and_globs_coexist_with_exclude_winning() {
    // Include everything by glob, exclude `orders` by glob: exclude still wins.
    let plan = compile(&customers_orders(true), &["*"], &["ord*"]).expect("compiles");
    assert!(plan.table("orders").is_none());
    assert!(plan.table("customers").is_some());
}

// --- Required vs optional dependency on an excluded table --------------------

#[test]
fn retained_required_fk_to_excluded_table_is_an_error() {
    // A NOT NULL FK cannot be detached: excluding its parent is fatal, with the
    // same stable code used for the selection-time condition.
    let error = compile(&customers_orders(false), &[], &["customers"])
        .expect_err("required FK to an excluded table must fail");
    assert!(error.contains("GEN-EXCLUDED-DEPENDENCY"), "{error}");
    assert!(error.contains("orders") && error.contains("customers"));
}

#[test]
fn optional_detached_fk_is_removed_with_a_strict_promotable_warning() {
    let plan = compile(&customers_orders(true), &[], &["customers"])
        .expect("optional FK detaches instead of failing");

    // The detach is a warning (which `--strict` promotes to exit 1), not an error.
    let detached = plan
        .diagnostics
        .iter()
        .find(|d| d.code == "GEN-DETACHED-DEPENDENCY")
        .expect("a detach warning is recorded");
    assert_eq!(detached.severity, Severity::Warning);

    // The rendered DDL never references the absent parent.
    let (sql, _) = render(
        plan,
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert!(
        !sql.to_lowercase().contains("customers"),
        "detached FK still references the excluded table: {sql}"
    );
    assert!(
        !sql.contains("FOREIGN KEY"),
        "detached FK still rendered: {sql}"
    );
}

// --- Indexes ----------------------------------------------------------------

fn indexed_model() -> &'static str {
    r#"
version: 1
kind: model
source: { dialect: mysql }
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 3 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: email, type: "varchar(255)", nullable: false }
      indexes:
        - { name: idx_email, columns: [email], unique: true }
  orders:
    rows: { kind: fixed, count: 4 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: true }
      indexes:
        - { name: idx_customer, columns: [customer_id] }
      relationships:
        - { name: fk_customer, columns: [customer_id], referenced_table: customers, referenced_columns: [id] }
"#
}

#[test]
fn local_indexes_survive_filtering() {
    // Excluding `customers` detaches the optional FK but keeps `orders`' own
    // local index, since it references only present columns.
    let plan = compile(indexed_model(), &[], &["customers"]).expect("compiles");
    let (sql, _) = render(
        plan,
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert!(sql.contains("idx_customer"), "local index dropped: {sql}");
    assert!(!sql.contains("customers"), "excluded table leaked: {sql}");
}

// --- Generated / default / identity DDL across dialects ---------------------

fn typed_columns_model() -> &'static str {
    r#"
version: 1
kind: model
source: { dialect: postgres }
defaults: { inference: schema }
seed: 7
tables:
  events:
    rows: { kind: fixed, count: 2 }
    schema:
      name: events
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true, identity: true }
        - { name: status, type: "varchar(32)", nullable: false, default_sql: "'new'" }
        - { name: label, type: "varchar(64)", nullable: false }
    columns:
      label: { generator: { kind: text.word } }
"#
}

#[test]
fn identity_and_default_columns_render_per_dialect_clauses() {
    for target in DIALECTS {
        let plan = compile_all(typed_columns_model());
        let (sql, _) = render(
            plan,
            target,
            Some(SqlDialect::Postgres),
            OutputMode::SchemaOnly,
        );
        assert!(
            sql.contains("DEFAULT 'new'"),
            "{target:?} lost default: {sql}"
        );
        let expected_identity = match target {
            SqlDialect::MySql => "AUTO_INCREMENT",
            SqlDialect::Postgres => "GENERATED ALWAYS AS IDENTITY",
            SqlDialect::Mssql => "IDENTITY(1,1)",
            SqlDialect::Sqlite => "",
        };
        if !expected_identity.is_empty() {
            assert!(
                sql.contains(expected_identity),
                "{target:?} missing identity clause: {sql}"
            );
        }
    }
}

// --- Lossy type mapping -----------------------------------------------------

fn enum_model() -> &'static str {
    r#"
version: 1
kind: model
source: { dialect: mysql }
defaults: { inference: schema }
seed: 7
tables:
  widgets:
    rows: { kind: fixed, count: 2 }
    schema:
      name: widgets
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: kind, type: "enum('a','b')", nullable: false }
    columns:
      kind: { generator: { kind: choice, values: [a, b] } }
"#
}

#[test]
fn lossy_enum_mapping_warns_cross_dialect_and_is_clean_same_dialect() {
    // Same dialect: identity mapping, no warning.
    let (_, same_warnings) = render(
        compile_all(enum_model()),
        SqlDialect::MySql,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert_eq!(same_warnings, 0, "same-dialect ENUM should not warn");

    // Cross dialect: ENUM narrows to a plain string and records a lossy warning.
    let (sql, warnings) = render(
        compile_all(enum_model()),
        SqlDialect::Postgres,
        Some(SqlDialect::MySql),
        OutputMode::SchemaOnly,
    );
    assert!(warnings >= 1, "cross-dialect ENUM should warn: {sql}");
    assert!(sql.contains("VARCHAR(255)"), "ENUM not narrowed: {sql}");
}

// --- IDENTITY + explicit values: loadability -------------------------------

fn identity_parent_child_model() -> &'static str {
    r#"
version: 1
kind: model
source: { dialect: postgres }
defaults: { inference: schema }
seed: 7
tables:
  authors:
    rows: { kind: fixed, count: 3 }
    schema:
      name: authors
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true, identity: true }
        - { name: name, type: "varchar(100)", nullable: false }
    columns:
      name: { generator: { kind: person.full_name } }
  books:
    rows:
      kind: relation.children
      parent: authors
      count: 6
      distribution: { kind: fixed, mean: 2.0, min: 1.0, max: 100.0 }
    schema:
      name: books
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true, identity: true }
        - { name: author_id, type: bigint, nullable: false }
        - { name: title, type: "varchar(200)", nullable: false }
    columns:
      title: { generator: { kind: commerce.product_name } }
    relationships:
      - { name: fk_author, columns: [author_id], references: { table: authors, columns: [id] } }
"#
}

#[test]
fn identity_pk_referenced_by_child_loads_on_mssql() {
    let plan = compile_all(identity_parent_child_model());
    let (sql, _) = render(
        plan,
        SqlDialect::Mssql,
        Some(SqlDialect::Postgres),
        OutputMode::SchemaAndData,
    );
    // The referenced identity PK receives explicit values, so its inserts are
    // wrapped in IDENTITY_INSERT ON/OFF; the toggle is balanced.
    assert!(
        sql.contains("SET IDENTITY_INSERT [authors] ON"),
        "missing IDENTITY_INSERT ON: {sql}"
    );
    assert!(
        sql.contains("SET IDENTITY_INSERT [authors] OFF"),
        "missing IDENTITY_INSERT OFF: {sql}"
    );
    assert_eq!(
        sql.matches("SET IDENTITY_INSERT [authors] ON").count(),
        1,
        "IDENTITY_INSERT opened more than once"
    );
    // The explicit id column is present in the authors INSERT column list.
    assert!(sql.contains("INSERT INTO [authors] ([id], [name])"));
}

#[test]
fn identity_pk_referenced_by_child_loads_on_postgres() {
    let plan = compile_all(identity_parent_child_model());
    let (sql, _) = render(
        plan,
        SqlDialect::Postgres,
        Some(SqlDialect::Postgres),
        OutputMode::SchemaAndData,
    );
    // Explicit values for a GENERATED ALWAYS identity need OVERRIDING SYSTEM
    // VALUE, and the identity table must render as INSERT (not COPY) to carry it.
    assert!(
        sql.contains("OVERRIDING SYSTEM VALUE"),
        "missing OVERRIDING SYSTEM VALUE: {sql}"
    );
    assert!(
        sql.contains("INSERT INTO \"authors\" (\"id\", \"name\") OVERRIDING SYSTEM VALUE VALUES"),
        "authors identity insert not wrapped: {sql}"
    );
    assert!(
        !sql.contains("COPY \"authors\""),
        "identity table used COPY, which cannot override the identity: {sql}"
    );
}

#[test]
fn non_referenced_identity_column_needs_no_wrapper() {
    // `books.id` is an identity PK that no child references, so it renders as a
    // database DEFAULT and needs no IDENTITY_INSERT/OVERRIDING wrapper.
    let plan = compile_all(identity_parent_child_model());
    let (sql, _) = render(
        plan,
        SqlDialect::Mssql,
        Some(SqlDialect::Postgres),
        OutputMode::SchemaAndData,
    );
    assert!(
        !sql.contains("SET IDENTITY_INSERT [books]"),
        "books needed no identity wrapper: {sql}"
    );
}

// --- Deferred / self-referential cycle --------------------------------------

fn self_referential_model() -> &'static str {
    r#"
version: 1
kind: model
source: { dialect: postgres }
defaults: { inference: schema }
seed: 7
tables:
  nodes:
    rows: { kind: fixed, count: 5 }
    schema:
      name: nodes
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: parent_id, type: bigint, nullable: true }
      indexes:
        - { name: idx_parent, columns: [parent_id] }
      relationships:
        - { name: fk_parent, columns: [parent_id], referenced_table: nodes, referenced_columns: [id] }
"#
}

#[test]
fn self_referential_table_renders_a_deferred_fk_across_dialects() {
    for target in DIALECTS {
        let plan = compile_all(self_referential_model());
        let (sql, _) = render(
            plan,
            target,
            Some(SqlDialect::Postgres),
            OutputMode::SchemaOnly,
        );
        // The self-FK is emitted as a trailing ALTER TABLE (deferred so the rows
        // can exist first) and still references the present `nodes` table.
        assert!(
            sql.contains("ALTER TABLE") && sql.contains("FOREIGN KEY"),
            "{target:?} did not defer the self FK: {sql}"
        );
    }
}
