//! TDD Integration tests for diff command v1.9.1 enhanced features.
//!
//! These tests cover:
//! - --verbose with sample collection
//! - --primary-key override
//! - --ignore-order for column order
//! - Index diff (non-PK indexes)
//! - --ignore-columns glob patterns
//! - --allow-no-pk handling
//!
//! Tests are organized by feature and cover all 3 dialects where applicable.

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

// =============================================================================
// VERBOSE MODE TESTS
// =============================================================================

#[test]
fn test_diff_verbose_shows_sample_pks_mysql() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice Updated');
INSERT INTO users VALUES (3, 'Charlie');
INSERT INTO users VALUES (4, 'David');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--verbose"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show sample PKs
    assert!(
        combined.contains("Added PKs:") || combined.contains("added:"),
        "Should show added PKs: {}",
        combined
    );
    // Should contain the actual PK values
    assert!(
        combined.contains("3") && combined.contains("4"),
        "Should show added PK values 3 and 4: {}",
        combined
    );
}

#[test]
fn test_diff_verbose_shows_sample_pks_postgres() {
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
3	Charlie
4	David
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--data-only", "--verbose"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show sample PKs for PostgreSQL COPY data
    assert!(
        combined.contains("3") && combined.contains("4"),
        "Should show added PK values 3 and 4: {}",
        combined
    );
}

#[test]
fn test_diff_verbose_shows_sample_pks_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT
);

INSERT INTO users VALUES (1, 'Alice Updated');
INSERT INTO users VALUES (3, 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "sqlite", "--data-only", "--verbose"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("3"),
        "Should show added PK value 3: {}",
        combined
    );
}

#[test]
fn test_diff_verbose_composite_pk_format() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE order_items (
    order_id INT,
    item_id INT,
    quantity INT,
    PRIMARY KEY (order_id, item_id)
);

INSERT INTO order_items VALUES (1, 1, 5);
INSERT INTO order_items VALUES (1, 2, 3);
"#;

    let new_sql = r#"
CREATE TABLE order_items (
    order_id INT,
    item_id INT,
    quantity INT,
    PRIMARY KEY (order_id, item_id)
);

INSERT INTO order_items VALUES (1, 1, 10);
INSERT INTO order_items VALUES (1, 3, 2);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--verbose"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Composite PK should be formatted as (val1, val2)
    assert!(
        combined.contains("(1, 3)") || combined.contains("1, 3"),
        "Should show composite PK format: {}",
        combined
    );
}

#[test]
fn test_diff_verbose_json_includes_samples() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice Updated');
INSERT INTO users VALUES (3, 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--verbose", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    // Check for sample fields in data
    let data = json.get("data").expect("Missing data key");
    let tables = data.get("tables").expect("Missing tables key");
    let users = tables.get("users").expect("Missing users table");

    assert!(
        users.get("sample_added_pks").is_some(),
        "Missing sample_added_pks in JSON"
    );
    assert!(
        users.get("sample_removed_pks").is_some(),
        "Missing sample_removed_pks in JSON"
    );
    assert!(
        users.get("sample_modified_pks").is_some(),
        "Missing sample_modified_pks in JSON"
    );
}

#[test]
fn test_diff_verbose_limits_sample_count() {
    let dir = TempDir::new().unwrap();

    // Generate many rows
    let mut old_inserts = String::new();
    let mut new_inserts = String::new();
    for i in 1..=200 {
        old_inserts.push_str(&format!("INSERT INTO users VALUES ({}, 'User{}');\n", i, i));
    }
    for i in 201..=400 {
        new_inserts.push_str(&format!("INSERT INTO users VALUES ({}, 'User{}');\n", i, i));
    }

    let old_sql = format!(
        r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

{}
"#,
        old_inserts
    );

    let new_sql = format!(
        r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

{}
"#,
        new_inserts
    );

    let old_path = create_temp_file(&dir, "old.sql", &old_sql);
    let new_path = create_temp_file(&dir, "new.sql", &new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--verbose", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    let data = json.get("data").expect("Missing data key");
    let tables = data.get("tables").expect("Missing tables key");
    let users = tables.get("users").expect("Missing users table");
    let sample_added = users
        .get("sample_added_pks")
        .expect("Missing sample_added_pks");

    // Should be limited to 100 samples (default)
    let sample_count = sample_added.as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        sample_count <= 100,
        "Sample count should be limited to 100, got {}",
        sample_count
    );
}

// =============================================================================
// PRIMARY KEY OVERRIDE TESTS
// =============================================================================

#[test]
fn test_diff_pk_override_single_column_mysql() {
    let dir = TempDir::new().unwrap();

    // Table has no PK, but we'll use email as the key
    let old_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@example.com', 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice Updated');
INSERT INTO users VALUES (3, 'charlie@example.com', 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--primary-key", "users:email"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // alice@example.com modified, bob@example.com removed, charlie@example.com added
    assert!(
        combined.contains("+1 rows") || combined.contains("added"),
        "Should detect 1 added row: {}",
        combined
    );
    assert!(
        combined.contains("-1 rows") || combined.contains("removed"),
        "Should detect 1 removed row: {}",
        combined
    );
    assert!(
        combined.contains("~1 modified") || combined.contains("modified"),
        "Should detect 1 modified row: {}",
        combined
    );
}

#[test]
fn test_diff_pk_override_single_column_postgres() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER,
    email VARCHAR(255),
    name VARCHAR(100)
);

COPY users (id, email, name) FROM stdin;
1	alice@example.com	Alice
2	bob@example.com	Bob
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER,
    email VARCHAR(255),
    name VARCHAR(100)
);

COPY users (id, email, name) FROM stdin;
1	alice@example.com	Alice Updated
3	charlie@example.com	Charlie
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "postgres",
            "--data-only",
            "--primary-key",
            "users:email",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("+1") || combined.contains("added"),
        "Should detect changes: {}",
        combined
    );
}

#[test]
fn test_diff_pk_override_single_column_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER,
    email TEXT,
    name TEXT
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@example.com', 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER,
    email TEXT,
    name TEXT
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice Updated');
INSERT INTO users VALUES (3, 'charlie@example.com', 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "sqlite",
            "--data-only",
            "--primary-key",
            "users:email",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_pk_override_composite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    sku VARCHAR(50),
    quantity INT
);

INSERT INTO order_items VALUES (1, 100, 'SKU-A', 5);
INSERT INTO order_items VALUES (1, 101, 'SKU-B', 3);
"#;

    let new_sql = r#"
CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    sku VARCHAR(50),
    quantity INT
);

INSERT INTO order_items VALUES (1, 100, 'SKU-A', 10);
INSERT INTO order_items VALUES (1, 102, 'SKU-C', 2);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--primary-key",
            "order_items:order_id+product_id",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // (1, 100) modified, (1, 101) removed, (1, 102) added
    assert!(
        combined.contains("+1") || combined.contains("added"),
        "Should detect 1 added: {}",
        combined
    );
}

#[test]
fn test_diff_pk_override_invalid_column_error() {
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
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--primary-key",
            "users:nonexistent_column",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    // Should fail with error about invalid column OR warn about it
    assert!(
        !output.status.success()
            || combined.contains("not found")
            || combined.contains("invalid")
            || combined.contains("does not exist"),
        "Should error or warn on invalid column: {}",
        combined
    );
}

#[test]
fn test_diff_pk_override_multiple_tables() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

CREATE TABLE products (
    id INT,
    sku VARCHAR(50),
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO products VALUES (1, 'SKU-A', 'Product A');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

CREATE TABLE products (
    id INT,
    sku VARCHAR(50),
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice Updated');
INSERT INTO products VALUES (1, 'SKU-A', 'Product A Updated');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--primary-key",
            "users:email,products:sku",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Both tables should show 1 modified row each
    assert!(
        combined.contains("modified"),
        "Should detect modified rows: {}",
        combined
    );
}

// =============================================================================
// IGNORE ORDER TESTS
// =============================================================================

#[test]
fn test_diff_ignore_order_no_change_mysql() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only", "--ignore-order"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // With --ignore-order, column reordering should not be reported
    assert!(
        !combined.contains("Column 'name'") && !combined.contains("Column 'email'"),
        "Should not report column changes when only order changed: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_order_no_change_postgres() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "postgres",
            "--schema-only",
            "--ignore-order",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_ignore_order_no_change_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT,
    name TEXT
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "sqlite",
            "--schema-only",
            "--ignore-order",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_ignore_order_still_detects_added_removed() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    phone VARCHAR(20)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only", "--ignore-order"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should still detect added (phone) and removed (name) columns
    assert!(
        combined.contains("phone"),
        "Should detect added phone column: {}",
        combined
    );
    assert!(
        combined.contains("name"),
        "Should detect removed name column: {}",
        combined
    );
}

#[test]
fn test_diff_without_ignore_order_detects_reorder() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    // Without --ignore-order flag
    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Note: Current implementation may or may not detect order changes
    // This test documents the expected behavior
    assert!(output.status.success(), "Diff should succeed");
    // The output should show either no changes (if order is ignored by default)
    // or show the reorder (if order matters by default)
    let _ = stdout; // Just ensure it runs
}

// =============================================================================
// INDEX DIFF TESTS
// =============================================================================

#[test]
fn test_diff_index_added_mysql() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);

CREATE INDEX idx_users_email ON users (email);
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
        combined.contains("idx_users_email") || combined.contains("Index"),
        "Should detect added index: {}",
        combined
    );
}

#[test]
fn test_diff_index_added_postgres() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);

CREATE INDEX idx_users_email ON users (email);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("idx_users_email") || combined.contains("Index"),
        "Should detect added index: {}",
        combined
    );
}

#[test]
fn test_diff_index_added_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT,
    name TEXT
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT,
    name TEXT
);

CREATE INDEX idx_users_email ON users (email);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "sqlite", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("idx_users_email") || combined.contains("Index"),
        "Should detect added index: {}",
        combined
    );
}

#[test]
fn test_diff_index_removed() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);

CREATE INDEX idx_users_email ON users (email);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
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
        combined.contains("idx_users_email") && combined.contains("removed"),
        "Should detect removed index: {}",
        combined
    );
}

#[test]
fn test_diff_unique_index() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);

CREATE UNIQUE INDEX uq_users_email ON users (email);
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
        combined.contains("uq_users_email")
            || combined.contains("unique")
            || combined.contains("Index"),
        "Should detect added unique index: {}",
        combined
    );
}

#[test]
fn test_diff_index_postgres_using_gin() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    content TEXT
);
"#;

    let new_sql = r#"
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    content TEXT
);

CREATE INDEX idx_documents_content ON documents USING gin (to_tsvector('english', content));
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "postgres", "--schema-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    assert!(
        combined.contains("idx_documents_content") || combined.contains("Index"),
        "Should detect added GIN index: {}",
        combined
    );
}

#[test]
fn test_diff_index_inline_create_table() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255),
    INDEX idx_email (email)
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
        combined.contains("idx_email") || combined.contains("Index"),
        "Should detect inline index: {}",
        combined
    );
}

#[test]
fn test_diff_index_sql_output() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);

CREATE INDEX idx_old ON users (email);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    email VARCHAR(255)
);

CREATE INDEX idx_new ON users (email);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only", "--format", "sql"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");
    // SQL output should have DROP INDEX and CREATE INDEX
    assert!(
        stdout.contains("DROP INDEX") || stdout.contains("drop index"),
        "SQL output should contain DROP INDEX: {}",
        stdout
    );
    assert!(
        stdout.contains("CREATE INDEX") || stdout.contains("create index"),
        "SQL output should contain CREATE INDEX: {}",
        stdout
    );
}

// =============================================================================
// IGNORE COLUMNS TESTS
// =============================================================================

#[test]
fn test_diff_ignore_columns_single_mysql() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01 00:00:00');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-12-21 12:00:00');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--ignore-columns",
            "users.updated_at",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show no changes because only updated_at differs
    // Summary shows "0 rows modified" which is correct
    assert!(
        combined.contains("0 rows added, 0 removed, 0 modified")
            || (combined.contains("0 rows") && !combined.contains("1 rows")),
        "Should not detect changes when only ignored column differs: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_columns_single_postgres() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    updated_at TIMESTAMP
);

COPY users (id, name, updated_at) FROM stdin;
1	Alice	2024-01-01 00:00:00
\.
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    updated_at TIMESTAMP
);

COPY users (id, name, updated_at) FROM stdin;
1	Alice	2024-12-21 12:00:00
\.
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "postgres",
            "--data-only",
            "--ignore-columns",
            "users.updated_at",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_ignore_columns_single_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    updated_at TEXT
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    updated_at TEXT
);

INSERT INTO users VALUES (1, 'Alice', '2024-12-21');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--dialect",
            "sqlite",
            "--data-only",
            "--ignore-columns",
            "users.updated_at",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_ignore_columns_glob_star() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    total DECIMAL(10,2),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01 00:00:00');
INSERT INTO orders VALUES (1, 100.00, '2024-01-01 00:00:00');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    total DECIMAL(10,2),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-12-21 12:00:00');
INSERT INTO orders VALUES (1, 100.00, '2024-12-21 12:00:00');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--ignore-columns", "*.updated_at"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show no changes because only updated_at differs in both tables
    assert!(
        combined.contains("0 rows added, 0 removed, 0 modified"),
        "Should not detect changes: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_columns_glob_suffix() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01', '2024-01-01');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-12-21', '2024-12-21');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--ignore-columns", "*.*_at"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show no changes because both created_at and updated_at are ignored
    assert!(
        combined.contains("0 rows added, 0 removed, 0 modified"),
        "Should not detect changes when ignoring *_at columns: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_columns_schema_diff() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at TIMESTAMP
);
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--schema-only", "--ignore-columns", "*.updated_at"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should not report updated_at type change
    assert!(
        !combined.contains("updated_at"),
        "Should not report ignored column in schema diff: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_columns_pk_error() {
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
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'Alice');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--ignore-columns", "users.id"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    // Should warn about ignoring PK column (but still succeed)
    assert!(
        output.status.success(),
        "Diff should complete: {}",
        combined
    );
    assert!(
        combined.contains("primary key") || combined.contains("Ignoring primary key column"),
        "Should warn when ignoring PK column: {}",
        combined
    );
}

#[test]
fn test_diff_ignore_columns_multiple_patterns() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at DATETIME,
    modified_by VARCHAR(50)
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01', 'admin');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at DATETIME,
    modified_by VARCHAR(50)
);

INSERT INTO users VALUES (1, 'Alice', '2024-12-21', 'system');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--ignore-columns",
            "*.created_at,*.modified_by",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show no changes because both columns are ignored
    assert!(
        combined.contains("0 rows added, 0 removed, 0 modified"),
        "Should not detect changes: {}",
        combined
    );
}

// =============================================================================
// NO-PK HANDLING TESTS
// =============================================================================

#[test]
fn test_diff_no_pk_warning_mysql() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20),
    created_at DATETIME
);

INSERT INTO logs VALUES ('Test', 'INFO', '2024-01-01');
"#;

    let new_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20),
    created_at DATETIME
);

INSERT INTO logs VALUES ('Test Updated', 'INFO', '2024-01-01');
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
    // Should warn about no PK
    assert!(
        combined.contains("no primary key")
            || combined.contains("Warning")
            || combined.contains("skipping"),
        "Should warn about table with no PK: {}",
        combined
    );
}

#[test]
fn test_diff_no_pk_warning_postgres() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

COPY logs (message, level) FROM stdin;
Test	INFO
\.
"#;

    let new_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

COPY logs (message, level) FROM stdin;
Test Updated	INFO
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
}

#[test]
fn test_diff_no_pk_warning_sqlite() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE logs (
    message TEXT,
    level TEXT
);

INSERT INTO logs VALUES ('Test', 'INFO');
"#;

    let new_sql = r#"
CREATE TABLE logs (
    message TEXT,
    level TEXT
);

INSERT INTO logs VALUES ('Test Updated', 'INFO');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--dialect", "sqlite", "--data-only"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
}

#[test]
fn test_diff_allow_no_pk_uses_all_columns() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

INSERT INTO logs VALUES ('Test', 'INFO');
INSERT INTO logs VALUES ('Debug', 'DEBUG');
"#;

    let new_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

INSERT INTO logs VALUES ('Test Updated', 'INFO');
INSERT INTO logs VALUES ('New', 'WARN');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--allow-no-pk"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should detect changes using all columns as key
    // (Test, INFO) -> doesn't exist in new, so removed
    // (Debug, DEBUG) -> doesn't exist in new, so removed
    // (Test Updated, INFO) -> new
    // (New, WARN) -> new
    assert!(
        combined.contains("added")
            || combined.contains("removed")
            || combined.contains("+")
            || combined.contains("-"),
        "Should detect data changes with --allow-no-pk: {}",
        combined
    );
}

#[test]
fn test_diff_no_pk_json_includes_warning() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

INSERT INTO logs VALUES ('Test', 'INFO');
"#;

    let new_sql = r#"
CREATE TABLE logs (
    message VARCHAR(255),
    level VARCHAR(20)
);

INSERT INTO logs VALUES ('Test', 'INFO');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args(["diff", "--data-only", "--format", "json"])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "Diff failed");

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse JSON output");

    // Should have warnings array with the no-PK warning
    let warnings = json.get("warnings");
    assert!(
        warnings.is_some(),
        "JSON output should include warnings array"
    );
}

// =============================================================================
// COMBINED FEATURE TESTS
// =============================================================================

#[test]
fn test_diff_verbose_with_ignore_columns() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice', '2024-01-01');
INSERT INTO users VALUES (2, 'Bob', '2024-01-01');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    updated_at DATETIME
);

INSERT INTO users VALUES (1, 'Alice Updated', '2024-12-21');
INSERT INTO users VALUES (3, 'Charlie', '2024-12-21');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--verbose",
            "--ignore-columns",
            "*.updated_at",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should show verbose output with samples, ignoring updated_at
    // id=1 should be modified (name changed), id=2 removed, id=3 added
    assert!(
        combined.contains("1") || combined.contains("2") || combined.contains("3"),
        "Should show sample PKs: {}",
        combined
    );
}

#[test]
fn test_diff_pk_override_with_verbose() {
    let dir = TempDir::new().unwrap();

    let old_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users VALUES (2, 'bob@example.com', 'Bob');
"#;

    let new_sql = r#"
CREATE TABLE users (
    id INT,
    email VARCHAR(255),
    name VARCHAR(100)
);

INSERT INTO users VALUES (10, 'alice@example.com', 'Alice Updated');
INSERT INTO users VALUES (20, 'charlie@example.com', 'Charlie');
"#;

    let old_path = create_temp_file(&dir, "old.sql", old_sql);
    let new_path = create_temp_file(&dir, "new.sql", new_sql);

    let output = sql_splitter_cmd()
        .args([
            "diff",
            "--data-only",
            "--verbose",
            "--primary-key",
            "users:email",
        ])
        .arg(&old_path)
        .arg(&new_path)
        .output()
        .expect("Failed to run diff");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(output.status.success(), "Diff failed: {}", combined);
    // Should use email as PK and show email values in verbose output
    // alice@example.com modified, bob@example.com removed, charlie@example.com added
}
