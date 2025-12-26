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

// =============================================================================
// Dialect-Specific Graph Tests
// =============================================================================

fn create_mysql_dump(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("mysql_test.sql");
    fs::write(
        &path,
        r#"
CREATE TABLE `users` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `email` VARCHAR(255)
);

CREATE TABLE `orders` (
  `id` INT PRIMARY KEY AUTO_INCREMENT,
  `user_id` INT,
  FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)
);

INSERT INTO `users` (`id`, `email`) VALUES (1, 'alice@example.com');
"#,
    )
    .unwrap();
    path
}

fn create_postgres_dump(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("postgres_test.sql");
    fs::write(
        &path,
        r#"
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "email" VARCHAR(255)
);

CREATE TABLE "orders" (
  "id" SERIAL PRIMARY KEY,
  "user_id" INTEGER,
  FOREIGN KEY ("user_id") REFERENCES "users"("id")
);

INSERT INTO "users" ("id", "email") VALUES (1, 'alice@example.com');
"#,
    )
    .unwrap();
    path
}

fn create_sqlite_dump(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("sqlite_test.sql");
    fs::write(
        &path,
        r#"
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT
);

CREATE TABLE "orders" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER,
  FOREIGN KEY ("user_id") REFERENCES "users"("id")
);

INSERT INTO "users" ("id", "email") VALUES (1, 'alice@example.com');
"#,
    )
    .unwrap();
    path
}

fn create_mssql_dump(dir: &TempDir) -> std::path::PathBuf {
    let path = dir.path().join("mssql_test.sql");
    fs::write(
        &path,
        r#"
SET ANSI_NULLS ON
GO
CREATE TABLE [dbo].[users] (
  [id] INT IDENTITY(1,1) NOT NULL,
  [email] NVARCHAR(255),
  CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
)
GO
CREATE TABLE [dbo].[orders] (
  [id] INT IDENTITY(1,1) NOT NULL,
  [user_id] INT,
  CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([id]),
  CONSTRAINT [FK_orders_users] FOREIGN KEY ([user_id]) REFERENCES [dbo].[users]([id])
)
GO
INSERT INTO [dbo].[users] ([id], [email]) VALUES (1, N'alice@example.com')
GO
"#,
    )
    .unwrap();
    path
}

#[test]
fn test_graph_mysql_dialect() {
    let dir = TempDir::new().unwrap();
    let dump = create_mysql_dump(&dir);
    let output = dir.path().join("mysql_schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
            "--dialect",
            "mysql",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["stats"]["table_count"], 2, "Should detect 2 tables");
    assert_eq!(
        json["stats"]["relationship_count"], 1,
        "Should detect FK relationship"
    );
}

#[test]
fn test_graph_postgres_dialect() {
    let dir = TempDir::new().unwrap();
    let dump = create_postgres_dump(&dir);
    let output = dir.path().join("postgres_schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
            "--dialect",
            "postgres",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["stats"]["table_count"], 2, "Should detect 2 tables");
    assert_eq!(
        json["stats"]["relationship_count"], 1,
        "Should detect FK relationship"
    );
}

#[test]
fn test_graph_sqlite_dialect() {
    let dir = TempDir::new().unwrap();
    let dump = create_sqlite_dump(&dir);
    let output = dir.path().join("sqlite_schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
            "--dialect",
            "sqlite",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["stats"]["table_count"], 2, "Should detect 2 tables");
    assert_eq!(
        json["stats"]["relationship_count"], 1,
        "Should detect FK relationship"
    );
}

#[test]
fn test_graph_mssql_dialect() {
    let dir = TempDir::new().unwrap();
    let dump = create_mssql_dump(&dir);
    let output = dir.path().join("mssql_schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
            "--dialect",
            "mssql",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    assert_eq!(json["stats"]["table_count"], 2, "Should detect 2 tables");
    assert_eq!(
        json["stats"]["relationship_count"], 1,
        "Should detect FK relationship between orders and users"
    );
}

#[test]
fn test_graph_mssql_dot_output() {
    let dir = TempDir::new().unwrap();
    let dump = create_mssql_dump(&dir);
    let output = dir.path().join("mssql_schema.dot");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            dump.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--dialect",
            "mssql",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("digraph ERD"));
    assert!(content.contains("orders:user_id -> users:id"));
    assert!(content.contains("🔑 PK")); // Primary key markers
}

#[test]
fn test_graph_mssql_fixture() {
    let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/simple.sql");

    let dir = TempDir::new().unwrap();
    let output = dir.path().join("mssql_fixture_schema.json");

    let status = Command::new(get_binary_path())
        .args([
            "graph",
            fixture_path.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
            "--format",
            "json",
            "--dialect",
            "mssql",
        ])
        .status()
        .unwrap();

    assert!(status.success());
    assert!(output.exists());

    let content = fs::read_to_string(&output).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    // The fixture has 2 tables (users, orders) with FK relationship
    assert!(
        json["stats"]["table_count"].as_u64().unwrap() >= 2,
        "Should detect at least 2 tables from MSSQL fixture"
    );
}
