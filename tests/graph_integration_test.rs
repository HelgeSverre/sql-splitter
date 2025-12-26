//! Integration tests for the graph/order commands (ERD generation).

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn get_binary_path() -> String {
    std::env::var("CARGO_BIN_EXE_sql-splitter")
        .unwrap_or_else(|_| "target/debug/sql-splitter".to_string())
}

fn create_test_dump(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("test.sql");
    fs::write(
        &path,
        r#"
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(255)
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE order_items (
  id INT PRIMARY KEY,
  order_id INT,
  product_id INT,
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  category_id INT,
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE categories (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  parent_id INT,
  FOREIGN KEY (parent_id) REFERENCES categories(id)
);

INSERT INTO users (id, email) VALUES (1, 'alice@example.com');
"#,
    )
    .unwrap();
    path
}

#[test]
fn test_graph_dot_output() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);
    let output = dir.path().join("schema.dot");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("digraph ERD"));
    assert!(content.contains("orders:user_id -> users:id"));
    assert!(content.contains("categories:parent_id -> categories:id")); // self-reference
    assert!(content.contains("🔑 PK")); // Primary key markers
}

#[test]
fn test_graph_mermaid_output() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);
    let output = dir.path().join("schema.mmd");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "mermaid",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("erDiagram"));
    assert!(content.contains("INT id PK"));
    assert!(content.contains("INT user_id FK"));
}

#[test]
fn test_graph_json_output() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);
    let output = dir.path().join("schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["stats"]["table_count"], 5);
    assert_eq!(json["stats"]["relationship_count"], 5); // renamed from edge_count
}

#[test]
fn test_graph_html_output() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);
    let output = dir.path().join("schema.html");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "html",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("<!DOCTYPE html>"));
    assert!(content.contains("mermaid"));
    assert!(content.contains("erDiagram")); // ERD uses erDiagram syntax
}

#[test]
fn test_graph_cycles_only() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);

    let output = Command::new(get_binary_path())
        .args(["graph", dump.to_str().unwrap(), "--cycles-only"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should only contain categories (self-referential)
    assert!(stdout.contains("categories"));
    assert!(!stdout.contains("users [label=")); // users not in cycle
    assert!(stderr.contains("Cycles detected"));
}

#[test]
fn test_graph_table_focus_transitive() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);

    let output = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "--table",
            "order_items",
            "--transitive",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should contain order_items and all its dependencies
    assert!(stdout.contains("order_items"));
    assert!(stdout.contains("orders"));
    assert!(stdout.contains("users"));
    assert!(stdout.contains("products"));
    assert!(stdout.contains("categories"));
}

#[test]
fn test_graph_table_focus_reverse() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);

    let output = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "--table",
            "users",
            "--reverse",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should contain users and tables that depend on it
    assert!(stdout.contains("users"));
    assert!(stdout.contains("orders"));
    assert!(stdout.contains("order_items"));
    // products and categories don't depend on users
    assert!(!stdout.contains("products [label="));
}

#[test]
fn test_order_dry_run() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);

    let output = Command::new(get_binary_path())
        .args(["order", dump.to_str().unwrap(), "--dry-run"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should show topological order
    assert!(stderr.contains("Topological order"));

    // users should come before orders
    let users_pos = stderr.find("users").unwrap_or(usize::MAX);
    let orders_pos = stderr.find("orders").unwrap_or(usize::MAX);
    assert!(users_pos < orders_pos, "users should come before orders");
}

#[test]
fn test_order_check() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);

    let output = Command::new(get_binary_path())
        .args(["order", dump.to_str().unwrap(), "--check"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Check PASSED"));
}

#[test]
fn test_order_output_file() {
    let dir = TempDir::new().unwrap();
    let dump = create_test_dump(&dir);
    let output = dir.path().join("ordered.sql");

    let status = Command::new(get_binary_path())
        .args([
            "order",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    // Content should exist and contain CREATE TABLE statements
    assert!(content.contains("CREATE TABLE"));
}
