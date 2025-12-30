//! Integration tests for the redact command across all dialects.
//!
//! Tests INSERT and COPY statement rewriting with various strategies.

use sql_splitter::parser::SqlDialect;
use sql_splitter::redactor::{RedactConfig, Redactor};
use std::fs;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

// ============================================================================
// MySQL redact tests
// ============================================================================

#[test]
fn test_mysql_redact_insert_null() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `email` VARCHAR(255) NOT NULL,
    `password` VARCHAR(255) NOT NULL,
    `name` VARCHAR(100)
);
INSERT INTO `users` (`id`, `email`, `password`, `name`) VALUES (1, 'alice@example.com', 'secret123', 'Alice');
INSERT INTO `users` (`id`, `email`, `password`, `name`) VALUES (2, 'bob@example.com', 'password456', 'Bob');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.password".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2, "Should redact 2 rows");
    assert_eq!(
        stats.columns_redacted, 2,
        "Should redact 2 password columns"
    );

    let output = fs::read_to_string(&output_file).unwrap();
    assert!(output.contains("NULL"), "Password should be NULL");
    // Original passwords should not appear in redacted output
    assert!(
        !output.contains("secret123"),
        "Original password should not appear"
    );
    assert!(
        !output.contains("password456"),
        "Original password should not appear"
    );
}

#[test]
fn test_mysql_redact_hash_email() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'alice@example.com');
INSERT INTO `users` VALUES (2, 'bob@test.org');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .hash_patterns(vec!["*.email".to_string()])
        .seed(Some(42))
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2);
    assert_eq!(stats.columns_redacted, 2);

    let output = fs::read_to_string(&output_file).unwrap();
    // Original emails should not appear
    assert!(!output.contains("alice@example.com"));
    assert!(!output.contains("bob@test.org"));
}

#[test]
fn test_mysql_redact_multirow_insert() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255),
    `ssn` VARCHAR(11)
);
INSERT INTO `users` VALUES (1, 'a@a.com', '123-45-6789'), (2, 'b@b.com', '987-65-4321'), (3, 'c@c.com', '111-22-3333');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.ssn".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 3, "Should redact 3 rows");
    assert_eq!(stats.columns_redacted, 3, "Should redact 3 ssn columns");

    let output = fs::read_to_string(&output_file).unwrap();
    // SSNs should not appear
    assert!(!output.contains("123-45-6789"));
    assert!(!output.contains("987-65-4321"));
    assert!(!output.contains("111-22-3333"));
}

#[test]
fn test_mysql_redact_preserves_non_matched_columns() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255),
    `name` VARCHAR(100)
);
INSERT INTO `users` VALUES (1, 'alice@example.com', 'Alice Johnson');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.email".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    redactor.run().unwrap();

    let output = fs::read_to_string(&output_file).unwrap();
    // Name should be preserved
    assert!(output.contains("Alice Johnson"), "Name should be preserved");
    // ID should be preserved
    assert!(output.contains("1"), "ID should be preserved");
}

#[test]
fn test_mysql_redact_escaping() {
    // Test that special characters are properly escaped
    let input_content = r#"
CREATE TABLE `messages` (
    `id` INT PRIMARY KEY,
    `content` TEXT,
    `secret` TEXT
);
INSERT INTO `messages` VALUES (1, 'Line1\nLine2', 'secret data');
INSERT INTO `messages` VALUES (2, 'Tab\there', 'more secrets');
INSERT INTO `messages` VALUES (3, 'Quote''s', 'private');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.secret".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 3);
}

// ============================================================================
// PostgreSQL COPY redact tests
// ============================================================================

#[test]
fn test_postgres_redact_copy() {
    let input_content = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    password VARCHAR(255)
);
COPY users (id, email, password) FROM stdin;
1	alice@example.com	secret123
2	bob@test.org	password456
\.
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::Postgres)
        .null_patterns(vec!["*.password".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2, "Should redact 2 COPY rows");
    assert_eq!(
        stats.columns_redacted, 2,
        "Should redact 2 password columns"
    );

    let output = fs::read_to_string(&output_file).unwrap();
    // Passwords should be replaced with \N (NULL marker in COPY)
    assert!(output.contains("\\N"), "Password should be NULL marker \\N");
    assert!(
        !output.contains("secret123"),
        "Original password should not appear"
    );
    assert!(
        !output.contains("password456"),
        "Original password should not appear"
    );
}

#[test]
fn test_postgres_redact_copy_escape_sequences() {
    let input_content = r#"
CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    message TEXT,
    secret TEXT
);
COPY logs (id, message, secret) FROM stdin;
1	Line1\nLine2	secret1
2	Tab\there	secret2
\.
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::Postgres)
        .null_patterns(vec!["*.secret".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2);
}

#[test]
fn test_postgres_redact_insert() {
    // PostgreSQL also supports INSERT statements
    let input_content = r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(100)
);
INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice');
INSERT INTO users (id, email, name) VALUES (2, 'bob@test.org', 'Bob');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::Postgres)
        .null_patterns(vec!["*.email".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2);

    let output = fs::read_to_string(&output_file).unwrap();
    assert!(output.contains("NULL"), "Email should be NULL");
    // Names should be preserved
    assert!(output.contains("Alice") || output.contains("'Alice'"));
}

// ============================================================================
// SQLite redact tests
// ============================================================================

#[test]
fn test_sqlite_redact_insert() {
    let input_content = r#"
CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "email" TEXT,
    "password" TEXT
);
INSERT INTO "users" ("id", "email", "password") VALUES (1, 'alice@example.com', 'secret');
INSERT INTO "users" ("id", "email", "password") VALUES (2, 'bob@test.org', 'password');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::Sqlite)
        .null_patterns(vec!["*.password".to_string()])
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 2);
    assert_eq!(stats.columns_redacted, 2);

    let output = fs::read_to_string(&output_file).unwrap();
    assert!(output.contains("NULL"));
    assert!(!output.contains("secret"));
    assert!(!output.contains("password'")); // 'password' as literal
}

// ============================================================================
// Dry run tests
// ============================================================================

#[test]
fn test_dry_run_does_not_write() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'alice@example.com');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("should_not_exist.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.email".to_string()])
        .dry_run(true)
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert!(stats.tables_processed > 0);
    // File should not be created in dry run mode
    assert!(
        !output_file.exists(),
        "Dry run should not create output file"
    );
}

// ============================================================================
// Skip strategy tests
// ============================================================================

#[test]
fn test_skip_tables() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255)
);
CREATE TABLE `logs` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'user@example.com');
INSERT INTO `logs` VALUES (1, 'log@example.com');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .null_patterns(vec!["*.email".to_string()])
        .exclude(vec!["logs".to_string()]) // Skip logs table
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    // Only users table should be redacted
    assert_eq!(stats.tables_processed, 1);

    let output = fs::read_to_string(&output_file).unwrap();
    // logs email should be preserved (table was skipped)
    assert!(
        output.contains("log@example.com"),
        "Skipped table data should be preserved"
    );
}

// ============================================================================
// Fake strategy tests
// ============================================================================

#[test]
fn test_fake_name_strategy() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `name` VARCHAR(100)
);
INSERT INTO `users` VALUES (1, 'Original Name');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file = output_dir.path().join("redacted.sql");

    let config = RedactConfig::builder()
        .input(input_file.path().to_path_buf())
        .output(Some(output_file.clone()))
        .dialect(SqlDialect::MySql)
        .fake_patterns(vec!["*.name".to_string()])
        .seed(Some(42))
        .build()
        .unwrap();

    let mut redactor = Redactor::new(config).unwrap();
    let stats = redactor.run().unwrap();

    assert_eq!(stats.rows_redacted, 1);

    let output = fs::read_to_string(&output_file).unwrap();
    assert!(
        !output.contains("Original Name"),
        "Original name should be replaced"
    );
}

// ============================================================================
// Reproducibility tests
// ============================================================================

#[test]
fn test_reproducible_with_same_seed() {
    let input_content = r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `email` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'alice@example.com');
INSERT INTO `users` VALUES (2, 'bob@test.org');
"#;

    let mut input_file = NamedTempFile::new().unwrap();
    input_file.write_all(input_content.as_bytes()).unwrap();
    input_file.flush().unwrap();

    let output_dir = TempDir::new().unwrap();
    let output_file1 = output_dir.path().join("redacted1.sql");
    let output_file2 = output_dir.path().join("redacted2.sql");

    for output_file in [&output_file1, &output_file2] {
        let config = RedactConfig::builder()
            .input(input_file.path().to_path_buf())
            .output(Some(output_file.clone()))
            .dialect(SqlDialect::MySql)
            .hash_patterns(vec!["*.email".to_string()])
            .seed(Some(99999))
            .build()
            .unwrap();

        let mut redactor = Redactor::new(config).unwrap();
        redactor.run().unwrap();
    }

    let content1 = fs::read_to_string(&output_file1).unwrap();
    let content2 = fs::read_to_string(&output_file2).unwrap();

    assert_eq!(
        content1, content2,
        "Same seed should produce identical output"
    );
}
