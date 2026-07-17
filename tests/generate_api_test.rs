//! Tests for the public [`sql_splitter::generate::Generate`] builder: the
//! single facade that wires config loading, compilation, generation, and
//! rendering together for a complete model.

use std::fs;

use sql_splitter::diagnostic::Severity;
use sql_splitter::generate::{CompileOptions, Generate, GenerateError, RunMode};
use sql_splitter::parser::SqlDialect;

const SIMPLE_MODEL: &str = "tests/fixtures/generate/simple.yaml";
/// A complete model whose `output:` block pins the render dialect to postgres.
const OUTPUT_POSTGRES_MODEL: &str = "tests/fixtures/generate/output_postgres.yaml";
/// A pg_dump-style postgres source dump (declares its dialect via COPY/quoting).
const POSTGRES_DUMP: &str = "tests/fixtures/generate/production_shape_postgres.sql";

#[test]
fn builder_generates_from_a_complete_model() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    let report = Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&output)
        .seed(42)
        .run()
        .unwrap();
    assert!(report.rows_written > 0);
    assert!(fs::read_to_string(output).unwrap().contains("INSERT INTO"));
}

// Dialect resolution precedence: CLI/builder `output_dialect` > model
// `output.dialect` > source/input dialect (preserve-source) > MySQL fallback.

/// (a) A model with `output: { dialect: postgres }` and NO `output_dialect`
/// renders POSTGRES (postgres `COPY`, not a MySQL `INSERT INTO`).
#[test]
fn model_output_dialect_drives_the_rendered_dialect() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    Generate::builder()
        .config(OUTPUT_POSTGRES_MODEL)
        .output(&output)
        .seed(42)
        .run()
        .unwrap();
    let sql = fs::read_to_string(&output).unwrap();
    assert!(sql.contains("COPY "), "expected postgres COPY, got: {sql}");
    assert!(
        !sql.contains("INSERT INTO"),
        "expected no MySQL-shaped INSERT, got: {sql}"
    );
}

/// (b) Profiling a postgres-dialect dump with NO `output_dialect` renders
/// POSTGRES — the render dialect is preserved from the source dump.
#[test]
fn profiling_preserves_the_source_dialect() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    Generate::builder()
        .input(POSTGRES_DUMP)
        .output(&output)
        .seed(42)
        .run()
        .unwrap();
    let sql = fs::read_to_string(&output).unwrap();
    assert!(
        sql.contains("COPY "),
        "expected postgres COPY (preserve-source), got: {sql}"
    );
    assert!(
        !sql.contains("INSERT INTO"),
        "expected no MySQL-shaped INSERT, got: {sql}"
    );
}

/// (c) An explicit `output_dialect(mysql)` OVERRIDES a model's
/// `output: { dialect: postgres }` → MySQL output.
#[test]
fn explicit_output_dialect_overrides_the_model_output_block() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    Generate::builder()
        .config(OUTPUT_POSTGRES_MODEL)
        .output(&output)
        .output_dialect(SqlDialect::MySql)
        .seed(42)
        .run()
        .unwrap();
    let sql = fs::read_to_string(&output).unwrap();
    assert!(
        sql.contains("INSERT INTO"),
        "explicit --dialect mysql should win over the model's postgres output: {sql}"
    );
    assert!(
        !sql.contains("COPY "),
        "expected no postgres COPY once mysql is forced, got: {sql}"
    );
}

/// (d) A model with no `output:` block, no source dialect, and no
/// `output_dialect` still defaults to MySQL (the unchanged fallback).
#[test]
fn no_output_block_no_source_no_dialect_defaults_to_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&output)
        .seed(42)
        .run()
        .unwrap();
    let sql = fs::read_to_string(&output).unwrap();
    assert!(
        sql.contains("INSERT INTO"),
        "expected the MySQL-shaped default, got: {sql}"
    );
    assert!(
        !sql.contains("COPY "),
        "expected no postgres COPY, got: {sql}"
    );
}

#[test]
fn same_seed_reproduces_identical_output() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("first.sql");
    let second_path = dir.path().join("second.sql");

    Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&first_path)
        .seed(7)
        .run()
        .unwrap();
    Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&second_path)
        .seed(7)
        .run()
        .unwrap();

    assert_eq!(
        fs::read_to_string(first_path).unwrap(),
        fs::read_to_string(second_path).unwrap()
    );
}

#[test]
fn check_mode_compiles_but_writes_no_sql() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    let report = Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&output)
        .mode(RunMode::Check)
        .run()
        .unwrap();
    assert_eq!(report.rows_written, 0);
    assert!(!output.exists());
}

#[test]
fn dry_run_mode_reports_the_plan_without_writing_sql() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    let report = Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&output)
        .mode(RunMode::DryRun)
        .run()
        .unwrap();
    assert!(report.rows_written > 0);
    assert!(!output.exists());
}

#[test]
fn a_warning_surfaces_in_the_report_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("synthetic.sql");
    let report = Generate::builder()
        .config(SIMPLE_MODEL)
        .output(&output)
        .compile(CompileOptions {
            max_rows: Some(2),
            ..Default::default()
        })
        .run()
        .unwrap();

    assert!(report
        .diagnostics
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "GEN-MAX-ROWS-CAPPED"
            && diagnostic.severity == Severity::Warning));
}

#[test]
fn overrides_without_a_base_model_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let overrides_path = dir.path().join("overrides.yaml");
    fs::write(
        &overrides_path,
        r#"
version: 1
kind: overrides
"#,
    )
    .unwrap();

    let err = Generate::builder()
        .config(&overrides_path)
        .output(dir.path().join("out.sql"))
        .run()
        .unwrap_err();

    match err {
        GenerateError::InvalidInput(message) => {
            assert!(message.contains("GEN-OVERRIDES-NO-BASE"));
        }
        other => panic!("expected GenerateError::InvalidInput, got {other:?}"),
    }
}

#[test]
fn missing_input_and_config_is_a_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let err = Generate::builder()
        .output(dir.path().join("out.sql"))
        .run()
        .unwrap_err();
    assert!(matches!(err, GenerateError::InvalidInput(_)));
}

#[test]
fn generate_mode_without_output_is_a_shape_error() {
    let err = Generate::builder().config(SIMPLE_MODEL).run().unwrap_err();
    assert!(matches!(err, GenerateError::InvalidInput(_)));
}
