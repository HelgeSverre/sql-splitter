//! Integration tests for --json output across all commands
//!
//! Tests verify that JSON output:
//! - Is valid JSON (can be parsed)
//! - Contains expected fields
//! - Has correct data types

use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

fn create_temp_sql(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn sql_splitter_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

// =============================================================================
// Split Command JSON Tests
// =============================================================================

#[test]
fn test_split_json_output() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');

CREATE TABLE orders (id INT PRIMARY KEY, user_id INT);
INSERT INTO orders VALUES (1, 1);
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().unwrap();

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_file").is_some());
    assert!(json.get("output_dir").is_some());
    assert!(json.get("dialect").is_some());
    assert!(json.get("statistics").is_some());
    assert!(json.get("tables").is_some());

    // Verify statistics
    let stats = &json["statistics"];
    assert!(stats.get("tables_found").is_some());
    assert!(stats.get("statements_processed").is_some());
    assert!(stats.get("bytes_processed").is_some());
    assert!(stats.get("elapsed_secs").is_some());

    // Verify tables array
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&serde_json::json!("users")));
    assert!(tables.contains(&serde_json::json!("orders")));
}

#[test]
fn test_split_json_dry_run() {
    let sql = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);";
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().unwrap();

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Failed to parse JSON");

    assert_eq!(json["dry_run"], true);
}

// =============================================================================
// Analyze Command JSON Tests
// =============================================================================

#[test]
fn test_analyze_json_output() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO users VALUES (4, 'Dave');

CREATE TABLE orders (id INT PRIMARY KEY);
INSERT INTO orders VALUES (1), (2);
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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_file").is_some());
    assert!(json.get("dialect").is_some());
    assert!(json.get("summary").is_some());
    assert!(json.get("tables").is_some());

    // Verify summary
    let summary = &json["summary"];
    assert_eq!(summary["total_tables"], 2);
    assert!(summary["total_inserts"].as_u64().unwrap() > 0);

    // Verify tables array
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);

    // Verify table details
    let users_table = tables.iter().find(|t| t["name"] == "users").unwrap();
    assert!(users_table["inserts"].as_u64().unwrap() >= 2);
    assert!(users_table.get("size_mb").is_some());
}

// =============================================================================
// Merge Command JSON Tests
// =============================================================================

#[test]
fn test_merge_json_output() {
    // Create split files first
    let split_dir = TempDir::new().unwrap();
    std::fs::write(
        split_dir.path().join("users.sql"),
        "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\n",
    )
    .unwrap();
    std::fs::write(
        split_dir.path().join("orders.sql"),
        "CREATE TABLE orders (id INT);\nINSERT INTO orders VALUES (1);\n",
    )
    .unwrap();

    let output_file = TempDir::new().unwrap();
    let merged_path = output_file.path().join("merged.sql");

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_dir").is_some());
    assert!(json.get("output_file").is_some());
    assert!(json.get("statistics").is_some());
    assert!(json.get("tables").is_some());
    assert!(json.get("options").is_some());

    // Verify statistics
    let stats = &json["statistics"];
    assert_eq!(stats["tables_merged"], 2);
    assert!(stats["bytes_written"].as_u64().unwrap() > 0);

    // Verify tables
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
}

#[test]
fn test_merge_json_dry_run() {
    let split_dir = TempDir::new().unwrap();
    std::fs::write(split_dir.path().join("test.sql"), "SELECT 1;\n").unwrap();

    let output = sql_splitter_bin()
        .arg("merge")
        .arg(split_dir.path())
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Failed to parse JSON");

    assert_eq!(json["dry_run"], true);
}

// =============================================================================
// Sample Command JSON Tests
// =============================================================================

#[test]
fn test_sample_json_output() {
    let sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
INSERT INTO users VALUES (4, 'Dave');
INSERT INTO users VALUES (5, 'Eve');
"#;
    let file = create_temp_sql(sql);
    let output_file = TempDir::new().unwrap();
    let sample_path = output_file.path().join("sample.sql");

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
        .arg("42")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_file").is_some());
    assert!(json.get("mode").is_some());
    assert!(json.get("statistics").is_some());
    assert!(json.get("tables").is_some());

    // Verify mode
    let mode = &json["mode"];
    assert_eq!(mode["type"], "percent");
    assert_eq!(mode["value"], 50);

    // Verify statistics
    let stats = &json["statistics"];
    assert!(stats.get("tables_sampled").is_some());
    assert!(stats.get("rows_selected").is_some());
    assert!(stats.get("rows_total").is_some());
    assert!(stats.get("sample_rate_percent").is_some());
}

#[test]
fn test_sample_json_dry_run() {
    let sql = "CREATE TABLE test (id INT); INSERT INTO test VALUES (1);";
    let file = create_temp_sql(sql);

    let output = sql_splitter_bin()
        .arg("sample")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--percent")
        .arg("10")
        .arg("--dry-run")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Failed to parse JSON");

    assert_eq!(json["dry_run"], true);
}

// =============================================================================
// Shard Command JSON Tests
// =============================================================================

#[test]
fn test_shard_json_output() {
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
    let output_file = TempDir::new().unwrap();
    let shard_path = output_file.path().join("shard.sql");

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_file").is_some());
    assert!(json.get("tenant").is_some());
    assert!(json.get("statistics").is_some());
    assert!(json.get("tables").is_some());

    // Verify tenant info
    let tenant = &json["tenant"];
    assert_eq!(tenant["column"], "tenant_id");
    assert_eq!(tenant["value"], "1");

    // Verify statistics
    let stats = &json["statistics"];
    assert!(stats.get("tables_processed").is_some());
    assert!(stats.get("rows_selected").is_some());
    assert!(stats.get("reduction_percent").is_some());
}

// =============================================================================
// Convert Command JSON Tests
// =============================================================================

#[test]
fn test_convert_json_output() {
    let sql = r#"
CREATE TABLE `users` (`id` INT AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255));
INSERT INTO `users` VALUES (1, 'Alice');
"#;
    let file = create_temp_sql(sql);
    let output_file = TempDir::new().unwrap();
    let converted_path = output_file.path().join("converted.sql");

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("input_file").is_some());
    assert!(json.get("conversion").is_some());
    assert!(json.get("statistics").is_some());
    assert!(json.get("warnings").is_some());

    // Verify conversion info
    let conversion = &json["conversion"];
    assert_eq!(conversion["from"], "mysql");
    assert_eq!(conversion["to"], "postgres");

    // Verify statistics
    let stats = &json["statistics"];
    assert!(stats.get("statements_processed").is_some());
    assert!(stats.get("statements_converted").is_some());
    assert!(stats.get("statements_unchanged").is_some());
}

#[test]
fn test_convert_json_dry_run() {
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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Failed to parse JSON");

    assert_eq!(json["dry_run"], true);
}

// =============================================================================
// Validate Command JSON Tests (already exists, verifying it still works)
// =============================================================================

#[test]
fn test_validate_json_output() {
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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect(&format!("Failed to parse JSON: {}", stdout));

    // Verify structure
    assert!(json.get("dialect").is_some());
    assert!(json.get("issues").is_some());
    assert!(json.get("summary").is_some());
    assert!(json.get("checks").is_some());
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_json_output_empty_file() {
    let file = create_temp_sql("");

    let output = sql_splitter_bin()
        .arg("analyze")
        .arg(file.path())
        .arg("--dialect")
        .arg("mysql")
        .arg("--json")
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("Failed to parse JSON");

    assert_eq!(json["summary"]["total_tables"], 0);
}

#[test]
fn test_json_output_with_special_characters() {
    let sql = r#"
CREATE TABLE test (id INT, name VARCHAR(255));
INSERT INTO test VALUES (1, 'Test with "quotes" and ''escapes''');
"#;
    let file = create_temp_sql(sql);
    let output_dir = TempDir::new().unwrap();

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should still be valid JSON
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON with special characters");

    assert!(json.get("tables").is_some());
}
