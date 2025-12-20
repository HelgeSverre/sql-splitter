//! Integration tests for the sample command using test_data_gen fixtures.

use sql_splitter::parser::SqlDialect;
use sql_splitter::sample::{run, SampleConfig, SampleMode};
use std::fs;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};
use test_data_gen::{Generator, RenderConfig, Renderer, Scale};

/// Generate a MySQL dump with FK relationships for testing
fn generate_test_dump() -> NamedTempFile {
    let mut gen = Generator::new(42, Scale::Small);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::mysql());
    let output = renderer.render_to_string(&data).unwrap();

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(output.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn test_sample_with_generated_fixtures() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(50),
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        stats.tables_sampled > 0,
        "Should have sampled at least one table"
    );
    assert!(
        stats.total_rows_selected > 0,
        "Should have selected some rows"
    );
    assert!(
        stats.total_rows_selected <= stats.total_rows_seen,
        "Selected rows should not exceed total rows"
    );

    // Check output file was created
    assert!(output_file.exists(), "Output file should exist");

    // Read output and verify it has content
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        content.contains("INSERT INTO"),
        "Output should contain INSERT statements"
    );
    assert!(
        content.contains("CREATE TABLE"),
        "Output should contain CREATE TABLE statements"
    );
}

#[test]
fn test_sample_with_preserve_relations() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_fk.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Rows(10),
        preserve_relations: true,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        stats.tables_sampled > 0,
        "Should have sampled at least one table"
    );

    // When preserving relations, dependent tables may have fewer rows
    // because rows referencing unsampled parents are filtered out
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        content.contains("INSERT INTO"),
        "Output should contain INSERT statements"
    );
}

#[test]
fn test_sample_fixed_rows() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_rows.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Rows(5),
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    // Each table should have at most 5 rows (or less if table has fewer)
    for table_stat in &stats.table_stats {
        assert!(
            table_stat.rows_selected <= 5,
            "Table {} should have at most 5 rows, got {}",
            table_stat.name,
            table_stat.rows_selected
        );
    }
}

#[test]
fn test_sample_with_table_filter() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_filtered.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(100),
        tables_filter: Some(vec!["tenants".to_string()]),
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert_eq!(
        stats.tables_sampled, 1,
        "Should have sampled only one table"
    );
    assert!(
        stats.table_stats.iter().any(|t| t.name == "tenants"),
        "Should have sampled tenants table"
    );
}

#[test]
fn test_sample_with_exclude() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_excluded.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(100),
        exclude: vec!["tenants".to_string()],
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        !stats.table_stats.iter().any(|t| t.name == "tenants"),
        "Should not have sampled tenants table"
    );
}

#[test]
fn test_sample_reproducible_with_seed() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file1 = output_dir.path().join("sampled1.sql");
    let output_file2 = output_dir.path().join("sampled2.sql");

    // Run twice with same seed
    for output_file in [&output_file1, &output_file2] {
        let config = SampleConfig {
            input: dump.path().to_path_buf(),
            output: Some(output_file.clone()),
            dialect: SqlDialect::MySql,
            mode: SampleMode::Rows(5),
            preserve_relations: false,
            seed: 12345,
            ..Default::default()
        };

        run(config).unwrap();
    }

    // Read both outputs
    let content1 = fs::read_to_string(&output_file1).unwrap();
    let content2 = fs::read_to_string(&output_file2).unwrap();

    // Skip the header (which contains timestamp) and compare the rest
    let data1: String = content1
        .lines()
        .filter(|l| !l.starts_with("-- Date:"))
        .collect();
    let data2: String = content2
        .lines()
        .filter(|l| !l.starts_with("-- Date:"))
        .collect();

    assert_eq!(
        data1, data2,
        "Outputs with same seed should be identical (except timestamp)"
    );
}

#[test]
fn test_sample_dry_run() {
    let dump = generate_test_dump();

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: None,
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(50),
        preserve_relations: false,
        seed: 42,
        dry_run: true,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    // Dry run should still compute stats
    assert!(stats.tables_sampled > 0);
    assert!(stats.total_rows_seen > 0);
}

#[test]
fn test_sample_with_yaml_config() {
    use std::io::Write;

    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_config.sql");

    // Create YAML config
    let mut config_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        config_file,
        r#"
default:
  percent: 50

classification:
  lookup:
    - currencies
    - permissions

tables:
  tenants:
    percent: 100
  users:
    rows: 5
"#
    )
    .unwrap();
    config_file.flush().unwrap();

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(10), // Will be overridden by config
        preserve_relations: false,
        seed: 42,
        config_file: Some(config_file.path().to_path_buf()),
        ..Default::default()
    };

    let stats = run(config).unwrap();

    // Check that config was applied
    assert!(stats.tables_sampled > 0);

    // Find tenants stats - should have 100% (3/3 rows)
    let tenants_stats = stats.table_stats.iter().find(|t| t.name == "tenants");
    if let Some(t) = tenants_stats {
        assert_eq!(
            t.rows_seen, t.rows_selected,
            "tenants should have 100% of rows"
        );
    }
}

#[test]
fn test_sample_with_root_tables() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_roots.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Rows(10),
        preserve_relations: true,
        seed: 42,
        root_tables: vec!["orders".to_string()],
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(stats.tables_sampled > 0);
}

#[test]
fn test_sample_with_max_total_rows() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_max.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(100),
        preserve_relations: false,
        seed: 42,
        max_total_rows: Some(50),
        ..Default::default()
    };

    let stats = run(config).unwrap();

    // Should hit the limit or have a warning
    assert!(stats.total_rows_selected <= 100 || !stats.warnings.is_empty());
}

#[test]
fn test_sample_include_global_none() {
    use sql_splitter::sample::GlobalTableMode;

    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_no_global.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(100),
        preserve_relations: false,
        seed: 42,
        include_global: GlobalTableMode::None,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(stats.tables_sampled > 0);
}

#[test]
fn test_sample_no_schema() {
    let dump = generate_test_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_no_schema.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::MySql,
        mode: SampleMode::Percent(100),
        preserve_relations: false,
        seed: 42,
        include_schema: false,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(stats.tables_sampled > 0);

    // Verify no CREATE TABLE in output
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        !content.contains("CREATE TABLE"),
        "Should not contain CREATE TABLE"
    );
    assert!(
        content.contains("INSERT INTO"),
        "Should contain INSERT statements"
    );
}

/// Generate a PostgreSQL dump for testing
fn generate_postgres_dump() -> NamedTempFile {
    let mut gen = Generator::new(42, Scale::Small);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::postgres());
    let output = renderer.render_to_string(&data).unwrap();

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(output.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

/// Generate a SQLite dump for testing
fn generate_sqlite_dump() -> NamedTempFile {
    let mut gen = Generator::new(42, Scale::Small);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::sqlite());
    let output = renderer.render_to_string(&data).unwrap();

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(output.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn test_sample_postgres_dialect() {
    let dump = generate_postgres_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_postgres.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::Postgres,
        mode: SampleMode::Percent(50),
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        stats.tables_sampled > 0,
        "Should have sampled at least one table"
    );
    assert!(
        stats.total_rows_selected > 0,
        "Should have selected some rows"
    );

    // Check output file was created with valid PostgreSQL syntax
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        content.contains("INSERT INTO"),
        "Output should contain INSERT statements"
    );
    assert!(
        content.contains("CREATE TABLE"),
        "Output should contain CREATE TABLE statements"
    );
    // PostgreSQL uses double quotes for identifiers
    assert!(
        content.contains("\""),
        "Output should use double quotes for identifiers"
    );
    // Check that COPY format was properly converted to INSERT VALUES format
    assert!(
        content.contains("VALUES"),
        "Output should contain VALUES clauses"
    );
    // Values should be in parentheses, not tab-separated
    assert!(
        !content.contains("\t1\t") && !content.contains("\t2\t"),
        "Output should not contain tab-separated COPY format"
    );
}

#[test]
fn test_sample_sqlite_dialect() {
    let dump = generate_sqlite_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_sqlite.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::Sqlite,
        mode: SampleMode::Percent(50),
        preserve_relations: false,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        stats.tables_sampled > 0,
        "Should have sampled at least one table"
    );
    assert!(
        stats.total_rows_selected > 0,
        "Should have selected some rows"
    );

    // Check output file was created with valid SQLite syntax
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        content.contains("INSERT INTO"),
        "Output should contain INSERT statements"
    );
    assert!(
        content.contains("CREATE TABLE"),
        "Output should contain CREATE TABLE statements"
    );
    // SQLite uses double quotes for identifiers
    assert!(
        content.contains("\""),
        "Output should use double quotes for identifiers"
    );
}

#[test]
fn test_sample_postgres_preserve_relations() {
    let dump = generate_postgres_dump();
    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("sampled_postgres_fk.sql");

    let config = SampleConfig {
        input: dump.path().to_path_buf(),
        output: Some(output_file.clone()),
        dialect: SqlDialect::Postgres,
        mode: SampleMode::Rows(10),
        preserve_relations: true,
        seed: 42,
        ..Default::default()
    };

    let stats = run(config).unwrap();

    assert!(
        stats.tables_sampled > 0,
        "Should have sampled at least one table"
    );

    // When preserving relations, the output should still be valid
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(
        content.contains("INSERT INTO"),
        "Output should contain INSERT statements"
    );
}
