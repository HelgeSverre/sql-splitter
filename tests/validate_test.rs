//! Integration tests for the validate command
//!
//! Tests cover:
//! - Basic validation (valid dump, missing table, duplicate PK, FK violations)
//! - Configuration options (max rows, no-fk-checks, strict mode)
//! - Multi-dialect handling (MySQL, PostgreSQL, SQLite)
//! - Split→Merge→Validate roundtrip tests
//! - Test data generator fixtures for realistic multi-table scenarios

use sql_splitter::merger::Merger;
use sql_splitter::parser::SqlDialect;
use sql_splitter::splitter::Splitter;
use sql_splitter::validate::{ValidateOptions, ValidationSummary, Validator};
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};
use test_data_gen::{Generator, RenderConfig, Renderer, Scale};

fn create_temp_sql(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn validate_sql(content: &str, fk_checks: bool) -> ValidationSummary {
    validate_sql_with_dialect(content, SqlDialect::MySql, fk_checks)
}

fn validate_sql_with_dialect(
    content: &str,
    dialect: SqlDialect,
    fk_checks: bool,
) -> ValidationSummary {
    let file = create_temp_sql(content);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(dialect),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: fk_checks,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    validator.validate().unwrap()
}

fn validate_file(
    path: &std::path::Path,
    dialect: SqlDialect,
    fk_checks: bool,
) -> ValidationSummary {
    let options = ValidateOptions {
        path: path.to_path_buf(),
        dialect: Some(dialect),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: fk_checks,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    validator.validate().unwrap()
}

/// Generate a MySQL dump with test_data_gen
fn generate_mysql_dump(seed: u64, scale: Scale) -> NamedTempFile {
    let mut gen = Generator::new(seed, scale);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::mysql());
    let output = renderer.render_to_string(&data).unwrap();
    create_temp_sql(&output)
}

/// Generate a PostgreSQL dump with test_data_gen
fn generate_postgres_dump_file(seed: u64, scale: Scale) -> NamedTempFile {
    let mut gen = Generator::new(seed, scale);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::postgres());
    let output = renderer.render_to_string(&data).unwrap();
    create_temp_sql(&output)
}

/// Generate a SQLite dump with test_data_gen
fn generate_sqlite_dump_file(seed: u64, scale: Scale) -> NamedTempFile {
    let mut gen = Generator::new(seed, scale);
    let data = gen.generate();
    let renderer = Renderer::new(RenderConfig::sqlite());
    let output = renderer.render_to_string(&data).unwrap();
    create_temp_sql(&output)
}

#[test]
fn test_validate_valid_dump() {
    let sql = r#"
        CREATE TABLE `users` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        INSERT INTO `users` VALUES (1, 'Alice'), (2, 'Bob');
    "#;

    let summary = validate_sql(sql, true);
    assert_eq!(summary.summary.errors, 0);
    assert_eq!(summary.summary.warnings, 0);
    assert_eq!(summary.summary.tables_scanned, 1);
}

#[test]
fn test_validate_missing_table() {
    let sql = r#"
        INSERT INTO `nonexistent_table` VALUES (1, 'test');
    "#;

    let summary = validate_sql(sql, false);
    assert_eq!(summary.summary.errors, 1);

    let issue = &summary.issues[0];
    assert_eq!(issue.code, "DDL_MISSING_TABLE");
    assert!(issue.message.contains("nonexistent_table"));
}

#[test]
fn test_validate_duplicate_pk() {
    let sql = r#"
        CREATE TABLE `users` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        INSERT INTO `users` VALUES (1, 'Alice');
        INSERT INTO `users` VALUES (2, 'Bob');
        INSERT INTO `users` VALUES (1, 'Charlie');
    "#;

    let summary = validate_sql(sql, true);
    assert_eq!(summary.summary.errors, 1);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1);
    assert!(pk_issues[0].message.contains("users"));
}

#[test]
fn test_validate_fk_missing_parent() {
    let sql = r#"
        CREATE TABLE `departments` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        CREATE TABLE `employees` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255),
            `department_id` INT,
            CONSTRAINT `fk_dept` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
        );
        INSERT INTO `departments` VALUES (1, 'Engineering');
        INSERT INTO `employees` VALUES (1, 'Alice', 1);
        INSERT INTO `employees` VALUES (2, 'Bob', 99);
    "#;

    let summary = validate_sql(sql, true);
    assert_eq!(summary.summary.errors, 1);

    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 1);
    assert!(fk_issues[0].message.contains("departments"));
}

#[test]
fn test_validate_no_fk_checks() {
    let sql = r#"
        CREATE TABLE `users` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        INSERT INTO `users` VALUES (1, 'Alice');
        INSERT INTO `users` VALUES (1, 'Bob');
    "#;

    // With FK checks disabled, we shouldn't detect duplicate PK
    let summary = validate_sql(sql, false);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 0);
}

#[test]
fn test_validate_encoding_issue() {
    let mut file = NamedTempFile::new().unwrap();

    // Write some valid SQL first
    file.write_all(b"CREATE TABLE `test` (`id` INT);\n")
        .unwrap();
    // Write invalid UTF-8 bytes
    file.write_all(&[0xFF, 0xFE, 0x00, 0x01]).unwrap();
    file.flush().unwrap();

    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: false,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    // Should have encoding warning
    let encoding_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "ENCODING")
        .collect();
    assert!(!encoding_issues.is_empty() || summary.summary.errors > 0);
}

#[test]
fn test_validate_postgres_with_fk_checks() {
    // PostgreSQL now supports PK/FK validation via INSERT parsing
    let sql = r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255)
        );
        INSERT INTO users VALUES (1, 'Alice');
    "#;

    let file = create_temp_sql(sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::Postgres),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    // PostgreSQL now supports FK checks - should have no errors for valid dump
    assert_eq!(summary.summary.errors, 0);
    assert!(summary.summary.tables_scanned > 0);
}

#[test]
fn test_validate_max_rows_per_table() {
    let sql = r#"
        CREATE TABLE `users` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        INSERT INTO `users` VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
    "#;

    let file = create_temp_sql(sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 3, // Small limit to trigger skip
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    // Should have warning about skipping checks
    let skip_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "PK_CHECK_SKIPPED")
        .collect();
    assert_eq!(skip_issues.len(), 1);
}

#[test]
fn test_validate_no_limit() {
    let mut sql = String::from("CREATE TABLE `items` (`id` INT PRIMARY KEY);\n");
    for i in 1..=100 {
        sql.push_str(&format!("INSERT INTO `items` VALUES ({});\n", i));
    }

    let file = create_temp_sql(&sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: usize::MAX, // No limit (simulating --no-limit)
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    // Should NOT have warning about skipping checks
    let skip_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "PK_CHECK_SKIPPED")
        .collect();
    assert_eq!(skip_issues.len(), 0, "No limit should not skip PK checks");
    assert_eq!(summary.summary.errors, 0);
}

#[test]
fn test_validate_composite_pk() {
    let sql = r#"
        CREATE TABLE `order_items` (
            `order_id` INT,
            `item_id` INT,
            `quantity` INT,
            PRIMARY KEY (`order_id`, `item_id`)
        );
        INSERT INTO `order_items` VALUES (1, 1, 5), (1, 2, 3), (2, 1, 1);
        INSERT INTO `order_items` VALUES (1, 1, 10);
    "#;

    let summary = validate_sql(sql, true);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1);
    assert!(pk_issues[0].message.contains("order_items"));
}

#[test]
fn test_validate_null_fk_allowed() {
    let sql = r#"
        CREATE TABLE `departments` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        CREATE TABLE `employees` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255),
            `department_id` INT,
            CONSTRAINT `fk_dept` FOREIGN KEY (`department_id`) REFERENCES `departments` (`id`)
        );
        INSERT INTO `departments` VALUES (1, 'Engineering');
        INSERT INTO `employees` VALUES (1, 'Alice', 1);
        INSERT INTO `employees` VALUES (2, 'Bob', NULL);
    "#;

    let summary = validate_sql(sql, true);

    // NULL FK should be allowed - no FK violation
    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 0);
}

#[test]
fn test_validate_multiple_tables() {
    let sql = r#"
        CREATE TABLE `a` (`id` INT PRIMARY KEY);
        CREATE TABLE `b` (`id` INT PRIMARY KEY);
        CREATE TABLE `c` (`id` INT PRIMARY KEY);
        INSERT INTO `a` VALUES (1), (2), (3);
        INSERT INTO `b` VALUES (1), (2);
        INSERT INTO `c` VALUES (1);
        INSERT INTO `nonexistent` VALUES (1);
    "#;

    let summary = validate_sql(sql, true);
    assert_eq!(summary.summary.tables_scanned, 3);

    let missing_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DDL_MISSING_TABLE")
        .collect();
    assert_eq!(missing_issues.len(), 1);
}

#[test]
fn test_validate_has_errors() {
    let sql = r#"
        INSERT INTO `missing` VALUES (1);
    "#;

    let summary = validate_sql(sql, false);
    assert!(summary.has_errors());
    assert!(!summary.has_warnings());
}

#[test]
fn test_validate_has_warnings() {
    let sql = r#"
        CREATE TABLE `users` (`id` INT PRIMARY KEY);
        INSERT INTO `users` VALUES (1), (2), (3), (4), (5);
    "#;

    let file = create_temp_sql(sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 3,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    assert!(summary.has_warnings());
}

// =============================================================================
// Tests with test_data_gen fixtures
// =============================================================================

#[test]
fn test_validate_generated_mysql_dump() {
    let dump = generate_mysql_dump(42, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::MySql, true);

    // Generated data should be valid (no errors)
    assert_eq!(
        summary.summary.errors, 0,
        "Generated MySQL dump should have no errors"
    );
    assert!(
        summary.summary.tables_scanned > 0,
        "Should have scanned tables"
    );
    assert!(
        summary.summary.statements_scanned > 0,
        "Should have scanned statements"
    );
}

#[test]
fn test_validate_generated_postgres_dump() {
    let dump = generate_postgres_dump_file(42, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::Postgres, false);

    // PostgreSQL dumps should parse without errors (FK checks skipped)
    assert_eq!(
        summary.summary.errors, 0,
        "Generated PostgreSQL dump should have no errors"
    );
    assert!(
        summary.summary.tables_scanned > 0,
        "Should have scanned tables"
    );
}

#[test]
fn test_validate_generated_sqlite_dump() {
    let dump = generate_sqlite_dump_file(42, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::Sqlite, false);

    // SQLite dumps should parse without errors
    assert_eq!(
        summary.summary.errors, 0,
        "Generated SQLite dump should have no errors"
    );
    assert!(
        summary.summary.tables_scanned > 0,
        "Should have scanned tables"
    );
}

#[test]
fn test_validate_generated_with_fk_checks() {
    let dump = generate_mysql_dump(123, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::MySql, true);

    // Generated data has FK-consistent data, so no FK violations
    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert!(
        fk_issues.is_empty(),
        "Generated data should have no FK violations"
    );
}

#[test]
fn test_validate_generated_multiple_seeds() {
    // Validate with different seeds to ensure robustness
    for seed in [1, 42, 100, 999] {
        let dump = generate_mysql_dump(seed, Scale::Small);
        let summary = validate_file(dump.path(), SqlDialect::MySql, false);
        assert_eq!(
            summary.summary.errors, 0,
            "Seed {} should produce valid dump",
            seed
        );
    }
}

// =============================================================================
// Split → Merge → Validate Roundtrip Tests
// =============================================================================

#[test]
fn test_validate_split_merge_roundtrip_mysql() {
    let dump = generate_mysql_dump(42, Scale::Small);
    let split_dir = TempDir::new().unwrap();
    let merged_file = NamedTempFile::new().unwrap();

    // Split
    let splitter = Splitter::new(dump.path().to_path_buf(), split_dir.path().to_path_buf())
        .with_dialect(SqlDialect::MySql);
    let split_stats = splitter.split().unwrap();
    assert!(split_stats.tables_found > 0, "Should have split tables");

    // Merge
    let merger = Merger::new(
        split_dir.path().to_path_buf(),
        Some(merged_file.path().to_path_buf()),
    )
    .with_dialect(SqlDialect::MySql)
    .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(
        split_stats.tables_found, merge_stats.tables_merged,
        "All split tables should be merged"
    );

    // Validate merged output
    let summary = validate_file(merged_file.path(), SqlDialect::MySql, false);
    assert_eq!(
        summary.summary.errors, 0,
        "Merged output should have no errors"
    );
    assert_eq!(
        summary.summary.tables_scanned, split_stats.tables_found,
        "Should validate same number of tables"
    );
}

#[test]
fn test_validate_split_merge_roundtrip_postgres() {
    let dump = generate_postgres_dump_file(42, Scale::Small);
    let split_dir = TempDir::new().unwrap();
    let merged_file = NamedTempFile::new().unwrap();

    // Split
    let splitter = Splitter::new(dump.path().to_path_buf(), split_dir.path().to_path_buf())
        .with_dialect(SqlDialect::Postgres);
    let split_stats = splitter.split().unwrap();
    assert!(split_stats.tables_found > 0, "Should have split tables");

    // Merge
    let merger = Merger::new(
        split_dir.path().to_path_buf(),
        Some(merged_file.path().to_path_buf()),
    )
    .with_dialect(SqlDialect::Postgres)
    .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(
        split_stats.tables_found, merge_stats.tables_merged,
        "All split tables should be merged"
    );

    // Validate merged output (no FK checks for Postgres)
    let summary = validate_file(merged_file.path(), SqlDialect::Postgres, false);
    assert_eq!(
        summary.summary.errors, 0,
        "Merged PostgreSQL output should have no errors"
    );
}

#[test]
fn test_validate_split_merge_roundtrip_sqlite() {
    let dump = generate_sqlite_dump_file(42, Scale::Small);
    let split_dir = TempDir::new().unwrap();
    let merged_file = NamedTempFile::new().unwrap();

    // Split
    let splitter = Splitter::new(dump.path().to_path_buf(), split_dir.path().to_path_buf())
        .with_dialect(SqlDialect::Sqlite);
    let split_stats = splitter.split().unwrap();
    assert!(split_stats.tables_found > 0, "Should have split tables");

    // Merge
    let merger = Merger::new(
        split_dir.path().to_path_buf(),
        Some(merged_file.path().to_path_buf()),
    )
    .with_dialect(SqlDialect::Sqlite)
    .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(
        split_stats.tables_found, merge_stats.tables_merged,
        "All split tables should be merged"
    );

    // Validate merged output
    let summary = validate_file(merged_file.path(), SqlDialect::Sqlite, false);
    assert_eq!(
        summary.summary.errors, 0,
        "Merged SQLite output should have no errors"
    );
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_validate_empty_file() {
    let summary = validate_sql("", false);
    assert_eq!(summary.summary.errors, 0);
    assert_eq!(summary.summary.tables_scanned, 0);
    assert_eq!(summary.summary.statements_scanned, 0);
}

#[test]
fn test_validate_comments_only() {
    let sql = r#"
        -- This is a comment
        /* Another comment */
        # MySQL comment
    "#;
    let summary = validate_sql(sql, false);
    assert_eq!(summary.summary.errors, 0);
}

#[test]
fn test_validate_schema_only() {
    let sql = r#"
        CREATE TABLE `users` (`id` INT PRIMARY KEY, `name` VARCHAR(255));
        CREATE TABLE `posts` (`id` INT PRIMARY KEY, `user_id` INT);
        CREATE INDEX `idx_user` ON `posts` (`user_id`);
    "#;
    let summary = validate_sql(sql, false);
    assert_eq!(summary.summary.errors, 0);
    assert_eq!(summary.summary.tables_scanned, 2);
}

#[test]
fn test_validate_multiple_fk_violations() {
    let sql = r#"
        CREATE TABLE `categories` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        CREATE TABLE `products` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255),
            `category_id` INT,
            CONSTRAINT `fk_cat` FOREIGN KEY (`category_id`) REFERENCES `categories` (`id`)
        );
        INSERT INTO `categories` VALUES (1, 'Electronics');
        INSERT INTO `products` VALUES (1, 'Phone', 1);
        INSERT INTO `products` VALUES (2, 'Laptop', 99);
        INSERT INTO `products` VALUES (3, 'Tablet', 100);
        INSERT INTO `products` VALUES (4, 'Watch', 101);
    "#;

    let summary = validate_sql(sql, true);

    // Should detect multiple FK violations (up to 5 reported per table)
    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert!(fk_issues.len() >= 1, "Should detect FK violations");
}

#[test]
fn test_validate_string_pk() {
    let sql = r#"
        CREATE TABLE `users` (
            `username` VARCHAR(50) PRIMARY KEY,
            `email` VARCHAR(255)
        );
        INSERT INTO `users` VALUES ('alice', 'alice@example.com');
        INSERT INTO `users` VALUES ('bob', 'bob@example.com');
        INSERT INTO `users` VALUES ('alice', 'alice2@example.com');
    "#;

    let summary = validate_sql(sql, true);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1, "Should detect string PK duplicate");
}

#[test]
fn test_validate_self_referential_fk() {
    let sql = r#"
        CREATE TABLE `employees` (
            `id` INT PRIMARY KEY,
            `name` VARCHAR(255),
            `manager_id` INT,
            CONSTRAINT `fk_manager` FOREIGN KEY (`manager_id`) REFERENCES `employees` (`id`)
        );
        INSERT INTO `employees` VALUES (1, 'CEO', NULL);
        INSERT INTO `employees` VALUES (2, 'Manager', 1);
        INSERT INTO `employees` VALUES (3, 'Employee', 2);
        INSERT INTO `employees` VALUES (4, 'Orphan', 999);
    "#;

    let summary = validate_sql(sql, true);

    // Should detect the orphan reference
    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 1, "Should detect self-ref FK violation");
}

#[test]
fn test_validate_large_rows_warning() {
    // Create a table with many rows to trigger the max_rows warning
    let mut sql = String::from("CREATE TABLE `items` (`id` INT PRIMARY KEY);\n");
    for i in 1..=100 {
        sql.push_str(&format!("INSERT INTO `items` VALUES ({});\n", i));
    }

    let file = create_temp_sql(&sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 50, // Trigger warning
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    let skip_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "PK_CHECK_SKIPPED")
        .collect();
    assert_eq!(skip_issues.len(), 1, "Should warn about skipping checks");
}

#[test]
fn test_validate_json_output_format() {
    let sql = r#"
        CREATE TABLE `test` (`id` INT PRIMARY KEY);
        INSERT INTO `test` VALUES (1);
    "#;

    let file = create_temp_sql(sql);
    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: true, // JSON mode
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let validator = Validator::new(options);
    let summary = validator.validate().unwrap();

    // Verify summary can be serialized to JSON
    let json = serde_json::to_string(&summary).unwrap();
    assert!(json.contains("\"dialect\""));
    assert!(json.contains("\"issues\""));
    assert!(json.contains("\"summary\""));
}

#[test]
fn test_validate_multi_row_insert() {
    let sql = r#"
        CREATE TABLE `users` (`id` INT PRIMARY KEY, `name` VARCHAR(255));
        INSERT INTO `users` VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
        INSERT INTO `users` VALUES (4, 'Dave'), (5, 'Eve');
    "#;

    let summary = validate_sql(sql, true);
    assert_eq!(summary.summary.errors, 0);
}

#[test]
fn test_validate_bigint_pk() {
    let sql = r#"
        CREATE TABLE `events` (
            `id` BIGINT PRIMARY KEY,
            `name` VARCHAR(255)
        );
        INSERT INTO `events` VALUES (9223372036854775807, 'Max BigInt');
        INSERT INTO `events` VALUES (1, 'First');
        INSERT INTO `events` VALUES (9223372036854775807, 'Duplicate');
    "#;

    let summary = validate_sql(sql, true);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1, "Should detect BIGINT PK duplicate");
}

#[test]
fn test_validate_composite_fk() {
    let sql = r#"
        CREATE TABLE `orders` (
            `tenant_id` INT,
            `order_id` INT,
            `name` VARCHAR(255),
            PRIMARY KEY (`tenant_id`, `order_id`)
        );
        CREATE TABLE `order_items` (
            `id` INT PRIMARY KEY,
            `tenant_id` INT,
            `order_id` INT,
            `product` VARCHAR(255),
            CONSTRAINT `fk_order` FOREIGN KEY (`tenant_id`, `order_id`) 
                REFERENCES `orders` (`tenant_id`, `order_id`)
        );
        INSERT INTO `orders` VALUES (1, 100, 'Order A'), (1, 101, 'Order B');
        INSERT INTO `order_items` VALUES (1, 1, 100, 'Widget');
        INSERT INTO `order_items` VALUES (2, 1, 999, 'Invalid');
    "#;

    let summary = validate_sql(sql, true);

    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 1, "Should detect composite FK violation");
}

// =============================================================================
// PostgreSQL-Specific PK/FK Validation Tests
// =============================================================================

#[test]
fn test_validate_postgres_duplicate_pk() {
    let sql = r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255)
        );
        INSERT INTO users VALUES (1, 'Alice');
        INSERT INTO users VALUES (2, 'Bob');
        INSERT INTO users VALUES (1, 'Charlie');
    "#;

    let summary = validate_sql_with_dialect(sql, SqlDialect::Postgres, true);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1, "PostgreSQL should detect duplicate PK");
}

#[test]
fn test_validate_postgres_fk_violation() {
    let sql = r#"
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255)
        );
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            department_id INTEGER,
            CONSTRAINT fk_dept FOREIGN KEY (department_id) REFERENCES departments (id)
        );
        INSERT INTO departments VALUES (1, 'Engineering');
        INSERT INTO employees VALUES (1, 'Alice', 1);
        INSERT INTO employees VALUES (2, 'Bob', 99);
    "#;

    let summary = validate_sql_with_dialect(sql, SqlDialect::Postgres, true);

    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 1, "PostgreSQL should detect FK violation");
}

#[test]
fn test_validate_postgres_generated_with_fk_checks() {
    let dump = generate_postgres_dump_file(42, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::Postgres, true);

    // Generated data should be FK-consistent
    assert_eq!(
        summary.summary.errors, 0,
        "Generated PostgreSQL dump should have no errors with FK checks"
    );
    assert!(summary.summary.tables_scanned > 0);
}

// =============================================================================
// SQLite-Specific PK/FK Validation Tests
// =============================================================================

#[test]
fn test_validate_sqlite_duplicate_pk() {
    let sql = r#"
        CREATE TABLE "users" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT
        );
        INSERT INTO "users" VALUES (1, 'Alice');
        INSERT INTO "users" VALUES (2, 'Bob');
        INSERT INTO "users" VALUES (1, 'Charlie');
    "#;

    let summary = validate_sql_with_dialect(sql, SqlDialect::Sqlite, true);

    let pk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "DUPLICATE_PK")
        .collect();
    assert_eq!(pk_issues.len(), 1, "SQLite should detect duplicate PK");
}

#[test]
fn test_validate_sqlite_fk_violation() {
    let sql = r#"
        CREATE TABLE "categories" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT
        );
        CREATE TABLE "products" (
            "id" INTEGER PRIMARY KEY,
            "name" TEXT,
            "category_id" INTEGER,
            FOREIGN KEY ("category_id") REFERENCES "categories" ("id")
        );
        INSERT INTO "categories" VALUES (1, 'Electronics');
        INSERT INTO "products" VALUES (1, 'Phone', 1);
        INSERT INTO "products" VALUES (2, 'Laptop', 99);
    "#;

    let summary = validate_sql_with_dialect(sql, SqlDialect::Sqlite, true);

    let fk_issues: Vec<_> = summary
        .issues
        .iter()
        .filter(|i| i.code == "FK_MISSING_PARENT")
        .collect();
    assert_eq!(fk_issues.len(), 1, "SQLite should detect FK violation");
}

#[test]
fn test_validate_sqlite_generated_with_fk_checks() {
    let dump = generate_sqlite_dump_file(42, Scale::Small);
    let summary = validate_file(dump.path(), SqlDialect::Sqlite, true);

    // Generated data should be FK-consistent
    assert_eq!(
        summary.summary.errors, 0,
        "Generated SQLite dump should have no errors with FK checks"
    );
    assert!(summary.summary.tables_scanned > 0);
}
