//! Integration tests that verify JSON output matches JSON schemas.
//!
//! Each command that supports --json output is tested against its corresponding
//! schema in the schemas/ directory.

use jsonschema::Validator;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

fn sql_splitter_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

fn create_temp_sql(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(content.as_bytes())
        .expect("Failed to write temp file");
    file.flush().expect("Failed to flush temp file");
    file
}

fn load_schema(name: &str) -> Validator {
    let schema_path = format!("schemas/{}.schema.json", name);
    let schema_str = fs::read_to_string(&schema_path)
        .unwrap_or_else(|_| panic!("Failed to read schema: {}", schema_path));
    let schema: Value = serde_json::from_str(&schema_str).expect("Invalid schema JSON");
    Validator::new(&schema).expect("Failed to compile schema")
}

fn validate_json_output(output: &std::process::Output, schema_name: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Command failed with stderr: {}",
        stderr
    );

    let json: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("Invalid JSON output: {}\nOutput: {}", e, stdout));

    let schema = load_schema(schema_name);
    let result = schema.validate(&json);

    if let Err(error) = result {
        panic!(
            "JSON output doesn't match {} schema:\n  - {}: {}\n\nOutput was:\n{}",
            schema_name,
            error.instance_path,
            error,
            serde_json::to_string_pretty(&json).unwrap()
        );
    }
}

// =============================================================================
// Analyze Command
// =============================================================================

#[test]
fn test_analyze_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
INSERT INTO orders VALUES (1, 1);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

#[test]
fn test_analyze_empty_file_matches_schema() {
    let file = create_temp_sql("");

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

// =============================================================================
// Validate Command
// =============================================================================

#[test]
fn test_validate_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("validate")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "validate");
}

#[test]
fn test_validate_with_issues_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
INSERT INTO orphans VALUES (1, 'test');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("validate")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout).expect("Invalid JSON");
    let schema = load_schema("validate");

    if let Err(error) = schema.validate(&json) {
        panic!(
            "JSON output doesn't match validate schema:\n  - {}: {}\n\nOutput was:\n{}",
            error.instance_path,
            error,
            serde_json::to_string_pretty(&json).unwrap()
        );
    }
}

// =============================================================================
// Split Command
// =============================================================================

#[test]
fn test_split_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
INSERT INTO orders VALUES (1, 1);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

#[test]
fn test_split_dry_run_json_matches_schema() {
    let sql = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);";
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

// =============================================================================
// Merge Command
// =============================================================================

#[test]
fn test_merge_json_matches_schema() {
    let split_dir = TempDir::new().expect("Failed to create temp dir");
    fs::write(
        split_dir.path().join("users.sql"),
        "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\n",
    )
    .expect("Failed to write file");
    fs::write(
        split_dir.path().join("orders.sql"),
        "CREATE TABLE orders (id INT);\nINSERT INTO orders VALUES (1);\n",
    )
    .expect("Failed to write file");

    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let merged_path = output_dir.path().join("merged.sql");

    let output = sql_splitter_bin()
        .arg("merge")
        .arg(split_dir.path())
        .arg("--output")
        .arg(&merged_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "merge");
}

#[test]
fn test_merge_dry_run_json_matches_schema() {
    let split_dir = TempDir::new().expect("Failed to create temp dir");
    fs::write(split_dir.path().join("test.sql"), "SELECT 1;\n").expect("Failed to write file");

    let output = sql_splitter_bin()
        .arg("merge")
        .arg(split_dir.path())
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "merge");
}

// =============================================================================
// Sample Command
// =============================================================================

#[test]
fn test_sample_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
INSERT INTO users VALUES (4, 'Dave');
INSERT INTO users VALUES (5, 'Eve');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let sample_path = output_dir.path().join("sample.sql");

    let output = sql_splitter_bin()
        .arg("sample")
        .arg(file.path())
        .arg("--output")
        .arg(&sample_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--percent")
        .arg("50")
        .arg("--seed")
        .arg("12345")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "sample");
}

#[test]
fn test_sample_rows_mode_json_matches_schema() {
    let sql = r#"
CREATE TABLE items (id INT PRIMARY KEY);
INSERT INTO items VALUES (1);
INSERT INTO items VALUES (2);
INSERT INTO items VALUES (3);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let sample_path = output_dir.path().join("sample.sql");

    let output = sql_splitter_bin()
        .arg("sample")
        .arg(file.path())
        .arg("--output")
        .arg(&sample_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--rows")
        .arg("2")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "sample");
}

// =============================================================================
// Convert Command
// =============================================================================

#[test]
fn test_convert_json_matches_schema() {
    let sql = r#"
CREATE TABLE `users` (`id` INT AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255));
INSERT INTO `users` VALUES (1, 'Alice');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let converted_path = output_dir.path().join("converted.sql");

    let output = sql_splitter_bin()
        .arg("convert")
        .arg(file.path())
        .arg("--output")
        .arg(&converted_path)
        .arg("--from")
        .arg("mysql")
        .arg("--to")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "convert");
}

#[test]
fn test_convert_dry_run_json_matches_schema() {
    let sql = "CREATE TABLE test (id INT);";
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("convert")
        .arg(file.path())
        .arg("--to")
        .arg("postgres")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "convert");
}

// =============================================================================
// Redact Command
// =============================================================================

#[test]
fn test_redact_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255), name VARCHAR(255));
INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@example.com', 'Bob');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let redacted_path = output_dir.path().join("redacted.sql");

    let output = sql_splitter_bin()
        .arg("redact")
        .arg(file.path())
        .arg("--output")
        .arg(&redacted_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--null")
        .arg("email")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "redact");
}

#[test]
fn test_redact_no_matches_json_matches_schema() {
    let sql = r#"
CREATE TABLE items (id INT PRIMARY KEY, count INT);
INSERT INTO items VALUES (1, 100);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let redacted_path = output_dir.path().join("redacted.sql");

    let output = sql_splitter_bin()
        .arg("redact")
        .arg(file.path())
        .arg("--output")
        .arg(&redacted_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--null")
        .arg("nonexistent")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "redact");
}

// =============================================================================
// Graph Command
// =============================================================================

#[test]
fn test_graph_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

#[test]
fn test_graph_no_relationships_json_matches_schema() {
    let sql = r#"
CREATE TABLE standalone (
    id INT PRIMARY KEY,
    data VARCHAR(255)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

// =============================================================================
// Shard Command
// =============================================================================

#[test]
fn test_shard_json_matches_schema() {
    let sql = r#"
CREATE TABLE tenants (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO tenants VALUES (1, 'Acme');
INSERT INTO tenants VALUES (2, 'Globex');

CREATE TABLE users (id INT PRIMARY KEY, tenant_id INT, name VARCHAR(255));
INSERT INTO users VALUES (1, 1, 'Alice');
INSERT INTO users VALUES (2, 1, 'Bob');
INSERT INTO users VALUES (3, 2, 'Charlie');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let shard_path = output_dir.path().join("shard.sql");

    let output = sql_splitter_bin()
        .arg("shard")
        .arg(file.path())
        .arg("--output")
        .arg(&shard_path)
        .arg("--dialect")
        .arg("mysql")
        .arg("--tenant-column")
        .arg("tenant_id")
        .arg("--tenant-value")
        .arg("1")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "shard");
}

#[test]
fn test_shard_dry_run_json_matches_schema() {
    let sql = r#"
CREATE TABLE data (id INT PRIMARY KEY, org_id INT);
INSERT INTO data VALUES (1, 100);
INSERT INTO data VALUES (2, 200);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("shard")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--tenant-column")
        .arg("org_id")
        .arg("--tenant-value")
        .arg("100")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "shard");
}

// =============================================================================
// PostgreSQL Dialect Tests
// =============================================================================

#[test]
fn test_analyze_postgres_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "analyze");
}

#[test]
fn test_graph_postgres_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id)
);
"#;
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("graph")
        .arg(file.path())
        .arg("--dialect")
        .arg("postgres")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "graph");
}

// =============================================================================
// SQLite Dialect Tests
// =============================================================================

#[test]
fn test_split_sqlite_json_matches_schema() {
    let sql = r#"
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let output = sql_splitter_bin()
        .arg("split")
        .arg(file.path())
        .arg("--output")
        .arg(output_dir.path())
        .arg("--dialect")
        .arg("sqlite")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    validate_json_output(&output, "split");
}

// =============================================================================
// Schema File Validation
// =============================================================================

/// Test that all schema files are valid JSON
#[test]
fn test_all_schema_files_are_valid_json() {
    let schema_files = [
        "analyze",
        "validate",
        "split",
        "merge",
        "sample",
        "convert",
        "redact",
        "graph",
        "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let _: Value = serde_json::from_str(&schema_str)
            .unwrap_or_else(|e| panic!("{} contains invalid JSON: {}", schema_path, e));
    }
}

/// Test that all schema files are valid JSON Schema (can be compiled)
#[test]
fn test_all_schema_files_are_valid_json_schema() {
    let schema_files = [
        "analyze",
        "validate",
        "split",
        "merge",
        "sample",
        "convert",
        "redact",
        "graph",
        "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let schema: Value = serde_json::from_str(&schema_str)
            .unwrap_or_else(|e| panic!("{} contains invalid JSON: {}", schema_path, e));

        Validator::new(&schema).unwrap_or_else(|e| {
            panic!(
                "{} is not a valid JSON Schema: {}",
                schema_path, e
            )
        });
    }
}

/// Test that all schema files have required metadata
#[test]
fn test_all_schema_files_have_metadata() {
    let schema_files = [
        "analyze",
        "validate",
        "split",
        "merge",
        "sample",
        "convert",
        "redact",
        "graph",
        "shard",
    ];

    for name in schema_files {
        let schema_path = format!("schemas/{}.schema.json", name);
        let schema_str = fs::read_to_string(&schema_path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", schema_path, e));

        let schema: Value = serde_json::from_str(&schema_str).unwrap();

        assert!(
            schema.get("$schema").is_some(),
            "{} missing $schema field",
            schema_path
        );
        assert!(
            schema.get("title").is_some(),
            "{} missing title field",
            schema_path
        );
        assert!(
            schema.get("description").is_some(),
            "{} missing description field",
            schema_path
        );
    }
}
