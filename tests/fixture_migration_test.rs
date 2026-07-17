//! Characterization test for Task 31: generates small fixtures with BOTH the
//! old `test_data_gen` crate and the new public `generate` API (via
//! `tests/support/generated_fixture.rs`, loading
//! `tests/fixtures/generate/legacy_fixture.yaml`) and compares schema shape,
//! table counts, and FK validity — the properties the migrated integration
//! tests (`sample_integration_test.rs`, `validate_test.rs`,
//! `mssql_integration_test.rs`, `shard_integration_test.rs`) actually rely
//! on. Byte equality is NOT required or attempted.
//!
//! This is the characterization GATE: Task 32 deletes `crates/test_data_gen`
//! and removes this test once the new fixture is trusted to stand alone.

mod support;

use sql_splitter::analyzer::Analyzer;
use sql_splitter::parser::SqlDialect;
use sql_splitter::validate::{ValidateOptions, ValidationSummary, Validator};
use std::collections::BTreeSet;
use std::io::Write;
use tempfile::NamedTempFile;
use test_data_gen::{Generator, RenderConfig, Renderer, Scale};

use support::generated_fixture::generated_fixture;

/// The old crate's Scale::Small MySQL dump.
fn old_mysql_dump() -> NamedTempFile {
    let mut gen = Generator::new(42, Scale::Small);
    let data = gen.generate();
    let output = Renderer::new(RenderConfig::mysql())
        .render_to_string(&data)
        .unwrap();
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(output.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn validate(path: &std::path::Path, dialect: SqlDialect) -> ValidationSummary {
    Validator::new(ValidateOptions {
        path: path.to_path_buf(),
        dialect: Some(dialect),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    })
    .validate()
    .unwrap()
}

/// Tables every consuming test relies on being present by name.
const EXPECTED_TABLES: &[&str] = &[
    "tenants",
    "users",
    "roles",
    "permissions",
    "role_permissions",
    "user_roles",
    "currencies",
    "categories",
    "products",
    "customers",
    "orders",
    "order_items",
    "projects",
    "tasks",
    "folders",
    "comments",
];

#[test]
fn old_and_new_fixtures_have_the_same_table_contract() {
    let old_dump = old_mysql_dump();
    let new_dump = generated_fixture(SqlDialect::MySql, None, None, 42);

    let old_stats = Analyzer::new(old_dump.path().to_path_buf())
        .with_dialect(SqlDialect::MySql)
        .analyze()
        .unwrap();
    let new_stats = Analyzer::new(new_dump.to_path_buf())
        .with_dialect(SqlDialect::MySql)
        .analyze()
        .unwrap();

    // Exact table counts: both must have every table the migrated tests
    // reference by name (sample/validate/mssql assert on `tenants`, `users`,
    // `orders`, `order_items`, `currencies`, `permissions`, and use
    // `stats.len() >= 10` as a coarse table-count check).
    let old_names: BTreeSet<String> = old_stats.iter().map(|s| s.table_name.clone()).collect();
    let new_names: BTreeSet<String> = new_stats.iter().map(|s| s.table_name.clone()).collect();

    for expected in EXPECTED_TABLES {
        assert!(
            old_names.contains(*expected),
            "old fixture missing expected table `{expected}`"
        );
        assert!(
            new_names.contains(*expected),
            "new fixture missing expected table `{expected}`"
        );
    }

    assert!(
        old_stats.len() >= 10,
        "old fixture should have at least 10 tables, found {}",
        old_stats.len()
    );
    assert!(
        new_stats.len() >= 10,
        "new fixture should have at least 10 tables, found {}",
        new_stats.len()
    );

    // Every table on both sides has at least one row.
    for stat in &old_stats {
        assert!(
            stat.insert_count > 0,
            "old fixture table `{}` has no rows",
            stat.table_name
        );
    }
    for stat in &new_stats {
        assert!(
            stat.insert_count > 0,
            "new fixture table `{}` has no rows",
            stat.table_name
        );
    }
}

#[test]
fn old_and_new_fixtures_are_both_fk_consistent() {
    let old_dump = old_mysql_dump();
    let new_dump = generated_fixture(SqlDialect::MySql, None, None, 42);

    let old_summary = validate(old_dump.path(), SqlDialect::MySql);
    let new_summary = validate(&new_dump, SqlDialect::MySql);

    // The old crate never rendered FOREIGN KEY DDL constraints at all (see
    // crates/test_data_gen/src/renderer.rs::infer_column_type, which infers
    // column types purely from naming heuristics), so its FK check was a
    // structural no-op — always zero issues regardless of data. The new
    // fixture *does* declare real FK constraints (see legacy_fixture.yaml),
    // so this is a strictly more meaningful check than the one it replaces;
    // both must still report zero errors and zero FK violations.
    assert_eq!(
        old_summary.summary.errors, 0,
        "old fixture should validate cleanly"
    );
    assert_eq!(
        new_summary.summary.errors, 0,
        "new fixture should validate cleanly"
    );

    let old_fk_issues = old_summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .count();
    let new_fk_issues = new_summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .count();

    assert_eq!(old_fk_issues, 0, "old fixture should have no FK violations");
    assert_eq!(new_fk_issues, 0, "new fixture should have no FK violations");
}

#[test]
fn new_fixture_supports_every_dialect_the_old_one_did() {
    for dialect in [
        SqlDialect::MySql,
        SqlDialect::Postgres,
        SqlDialect::Sqlite,
        SqlDialect::Mssql,
    ] {
        let dump = generated_fixture(dialect, None, None, 42);
        let summary = validate(&dump, dialect);
        assert_eq!(
            summary.summary.errors, 0,
            "new fixture should validate cleanly for {dialect:?}"
        );
        assert!(
            summary.summary.tables_scanned >= 10,
            "new fixture should scan at least 10 tables for {dialect:?}, found {}",
            summary.summary.tables_scanned
        );
    }
}
