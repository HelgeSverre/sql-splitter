//! Integration tests for the diff command.

use std::fs;
use std::io::Write;
use std::process::Command;
use tempfile::TempDir;

fn sql_splitter_cmd() -> Command {
    let path = std::env::current_dir()
        .unwrap()
        .join("target/debug/sql-splitter");
    Command::new(path)
}

fn create_temp_file(dir: &TempDir, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let mut file = fs::File::create(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    path
}

#[test]
fn test_diff_basic_schema_changes() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("Column 'email'"),
        "Missing email column in output: {}",
        combined
    );
}

#[test]
fn test_diff_table_added() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY
);

CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("Table 'products'"),
        "Missing products table in output: {}",
        combined
    );
    assert!(
        combined.contains("(new)"),
        "Missing (new) marker: {}",
        combined
    );
}

#[test]
fn test_diff_table_removed() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY
);

CREATE TABLE legacy_data (
    id INT PRIMARY KEY
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("legacy_data"),
        "Missing legacy_data table in output: {}",
        combined
    );
    assert!(
        combined.contains("(removed)"),
        "Missing (removed) marker: {}",
        combined
    );
}

#[test]
fn test_diff_data_changes() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice Updated');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (4, 'David');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Alice modified, Charlie removed, David added
    assert!(
        combined.contains("+1 rows"),
        "Missing added rows: {}",
        combined
    );
    assert!(
        combined.contains("-1 rows"),
        "Missing removed rows: {}",
        combined
    );
    assert!(
        combined.contains("modified"),
        "Missing modified rows: {}",
        combined
    );
}

#[test]
fn test_diff_json_output() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    // Parse as JSON
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    assert!(json.get("schema").is_some(), "Missing schema key");
    assert!(json.get("data").is_some(), "Missing data key");
    assert!(json.get("summary").is_some(), "Missing summary key");

    // Check summary values
    let summary = json.get("summary").unwrap();
    assert_eq!(summary["tables_modified"], 1);
    assert_eq!(summary["rows_added"], 1);
}

#[test]
fn test_diff_sql_output() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

CREATE TABLE products (
    id INT PRIMARY KEY
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--format", "sql", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("ALTER TABLE"),
        "Missing ALTER TABLE: {}",
        combined
    );
    assert!(
        combined.contains("ADD COLUMN"),
        "Missing ADD COLUMN: {}",
        combined
    );
    assert!(
        combined.contains("CREATE TABLE"),
        "Missing CREATE TABLE for new table: {}",
        combined
    );
}

#[test]
fn test_diff_table_filter() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE products (id INT PRIMARY KEY);
INSERT INTO users VALUES (1);
INSERT INTO products VALUES (1);
"#;

    let new_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100));
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
INSERT INTO products VALUES (1);
INSERT INTO products VALUES (2);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--tables", "users"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should only show users, not products
    assert!(
        combined.contains("users"),
        "Should include users table: {}",
        combined
    );
    assert!(
        !combined.contains("products"),
        "Should not include products table: {}",
        combined
    );
}

#[test]
fn test_diff_exclude_filter() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE audit_log (id INT PRIMARY KEY);
INSERT INTO users VALUES (1);
INSERT INTO audit_log VALUES (1);
"#;

    let new_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
CREATE TABLE audit_log (id INT PRIMARY KEY);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
INSERT INTO audit_log VALUES (1);
INSERT INTO audit_log VALUES (2);
INSERT INTO audit_log VALUES (3);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--exclude", "audit_log"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("users"),
        "Should include users: {}",
        combined
    );
    assert!(
        !combined.contains("audit_log"),
        "Should not include audit_log: {}",
        combined
    );
}

#[test]
fn test_diff_no_changes() {
    let dir = TempDir::new().unwrap();

    let sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let old_path = create_temp_file(&dir, "old.sql", sql);
    let new_path = create_temp_file(&dir, "new.sql", sql);

    let output = sql_splitter_cmd()
        .args(["diff"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("0 tables added, 0 removed, 0 modified"),
        "Expected no changes: {}",
        combined
    );
    assert!(
        combined.contains("0 rows added, 0 removed, 0 modified"),
        "Expected no row changes: {}",
        combined
    );
}

#[test]
fn test_diff_postgres_insert_format() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
-- PostgreSQL dump
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let new_sql = r#"
-- PostgreSQL dump
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice Updated');
INSERT INTO users VALUES (2, 'Bob');
INSERT INTO users VALUES (3, 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("+1 rows"),
        "Missing added rows: {}",
        combined
    );
    assert!(
        combined.contains("modified"),
        "Missing modified rows: {}",
        combined
    );
}

#[test]
fn test_diff_output_to_file() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY);
"#;

    let new_sql = r#"
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);
    let output_path = dir.path().join("diff.txt");

    let output = sql_splitter_cmd()
        .args(["diff", "-o"])
        .arg(&output_path)
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    assert!(output.status.success(), "Diff failed");
    assert!(output_path.exists(), "Output file should exist");

    let content = fs::read_to_string(&output_path).unwrap();
    assert!(
        content.contains("Column 'name'"),
        "Output file missing column change"
    );
}

// ============================================================================
// PostgreSQL COPY format tests
// ============================================================================

#[test]
fn test_diff_postgres_copy_data_added() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
3	Charlie
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--data-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("+1 rows"),
        "Should detect 1 added row: {}",
        combined
    );
}

#[test]
fn test_diff_postgres_copy_data_removed() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
3	Charlie
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--data-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("-1 rows"),
        "Should detect 1 removed row: {}",
        combined
    );
}

#[test]
fn test_diff_postgres_copy_data_modified() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice Updated
2	Bob
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--data-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("~1 modified"),
        "Should detect 1 modified row: {}",
        combined
    );
}

#[test]
fn test_diff_postgres_copy_json_output() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

COPY users (id, name) FROM stdin;
1	Alice Updated
2	Bob
3	Charlie
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    let summary = json.get("summary").unwrap();
    assert_eq!(summary["rows_added"], 1, "Should have 1 added row");
    assert_eq!(summary["rows_modified"], 1, "Should have 1 modified row");
}

#[test]
fn test_diff_postgres_copy_multiple_tables() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
\.

COPY orders (id, user_id) FROM stdin;
100	1
101	2
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER
);

COPY users (id, name) FROM stdin;
1	Alice
2	Bob
3	Charlie
\.

COPY orders (id, user_id) FROM stdin;
100	1
102	3
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    let summary = json.get("summary").unwrap();
    // users: +1 (Charlie), orders: +1 (102), -1 (101)
    assert_eq!(summary["rows_added"], 2, "Should have 2 added rows total");
    assert_eq!(summary["rows_removed"], 1, "Should have 1 removed row");
}

#[test]
fn test_diff_postgres_copy_with_nulls() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

COPY users (id, name, email) FROM stdin;
1	Alice	alice@example.com
2	Bob	\N
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);

COPY users (id, name, email) FROM stdin;
1	Alice	alice@new.com
2	Bob	bob@example.com
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    let summary = json.get("summary").unwrap();
    // Both rows modified (email changed)
    assert_eq!(summary["rows_modified"], 2, "Should have 2 modified rows");
}
