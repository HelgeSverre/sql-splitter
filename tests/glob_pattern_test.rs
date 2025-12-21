//! Integration tests for glob pattern support across commands
//!
//! Tests cover:
//! - Glob pattern expansion (*.sql, **/*.sql)
//! - Multi-file processing for validate, analyze, split, convert
//! - --fail-fast behavior
//! - Error handling for no-match patterns
//! - Edge cases: empty directories, mixed valid/invalid files

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

fn binary_path() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // Remove test binary name
    path.pop(); // Remove deps
    path.push("sql-splitter");
    path
}

fn create_sql_file(dir: &std::path::Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    let mut file = fs::File::create(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    path
}

fn simple_mysql_dump() -> &'static str {
    r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `name` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'Alice'), (2, 'Bob');
"#
}

fn simple_mysql_dump_with_error() -> &'static str {
    r#"
CREATE TABLE `users` (
    `id` INT PRIMARY KEY,
    `name` VARCHAR(255)
);
INSERT INTO `users` VALUES (1, 'Alice');
INSERT INTO `users` VALUES (1, 'Duplicate');
"#
}

fn postgres_dump() -> &'static str {
    r#"
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
"#
}

// =============================================================================
// Validate Command - Glob Pattern Tests
// =============================================================================

#[test]
fn test_validate_glob_single_file_match() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "dump.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args(["validate", &dir.path().join("*.sql").to_string_lossy()])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should succeed: {}", stderr);
    assert!(stderr.contains("Result: PASSED"));
}

#[test]
fn test_validate_glob_multiple_files() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "c.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should succeed: {}", stderr);
    assert!(stderr.contains("Validating 3 files"));
    assert!(stderr.contains("Passed: 3"));
    assert!(stderr.contains("Result: ALL PASSED"));
}

#[test]
fn test_validate_glob_mixed_results() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "good.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "bad.sql", simple_mysql_dump_with_error());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "Should fail due to bad.sql");
    assert!(stderr.contains("Passed: 1"));
    assert!(stderr.contains("Failed: 1"));
    assert!(stderr.contains("Result: SOME FAILED"));
}

#[test]
fn test_validate_glob_fail_fast() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a_bad.sql", simple_mysql_dump_with_error());
    create_sql_file(dir.path(), "b_good.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "c_good.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
            "--fail-fast",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("Failed: 1"));
    assert!(stderr.contains("Passed: 0"), "Should stop after first failure: {}", stderr);
}

#[test]
fn test_validate_glob_no_match() {
    let dir = TempDir::new().unwrap();
    
    let output = Command::new(binary_path())
        .args(["validate", &dir.path().join("*.sql").to_string_lossy()])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("no files match"));
}

#[test]
fn test_validate_glob_json_output() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
            "--json",
        ])
        .output()
        .unwrap();
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["total_files"], 2);
    assert_eq!(json["passed"], 2);
    assert!(json["results"].is_array());
}

#[test]
fn test_validate_glob_recursive() {
    let dir = TempDir::new().unwrap();
    let subdir = dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    
    create_sql_file(dir.path(), "root.sql", simple_mysql_dump());
    create_sql_file(&subdir, "nested.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("**/*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should succeed: {}", stderr);
    assert!(stderr.contains("Validating 2 files"));
}

// =============================================================================
// Analyze Command - Glob Pattern Tests
// =============================================================================

#[test]
fn test_analyze_glob_multiple_files() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "analyze",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Should succeed");
    assert!(stdout.contains("Analyzing 2 files"));
    assert!(stdout.contains("Analysis Summary"));
    assert!(stdout.contains("Succeeded: 2"));
}

#[test]
fn test_analyze_glob_no_match() {
    let dir = TempDir::new().unwrap();
    
    let output = Command::new(binary_path())
        .args(["analyze", &dir.path().join("*.sql").to_string_lossy()])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("no files match"));
}

// =============================================================================
// Split Command - Glob Pattern Tests
// =============================================================================

#[test]
fn test_split_glob_multiple_files() {
    let dir = TempDir::new().unwrap();
    let output_dir = dir.path().join("output");
    
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "split",
            &dir.path().join("*.sql").to_string_lossy(),
            "--output", &output_dir.to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "Should succeed: {}", stdout);
    assert!(stdout.contains("Splitting 2 files"));
    
    assert!(output_dir.join("a").exists());
    assert!(output_dir.join("b").exists());
    assert!(output_dir.join("a").join("users.sql").exists());
    assert!(output_dir.join("b").join("users.sql").exists());
}

#[test]
fn test_split_glob_dry_run() {
    let dir = TempDir::new().unwrap();
    let output_dir = dir.path().join("output");
    
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "split",
            &dir.path().join("*.sql").to_string_lossy(),
            "--output", &output_dir.to_string_lossy(),
            "--dialect", "mysql",
            "--dry-run",
        ])
        .output()
        .unwrap();
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("Splitting 2 files"));
    assert!(stdout.contains("(dry run)"));
    
    assert!(!output_dir.exists());
}

// =============================================================================
// Convert Command - Glob Pattern Tests
// =============================================================================

#[test]
fn test_convert_glob_requires_output_dir_for_multiple_files() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "convert",
            &dir.path().join("*.sql").to_string_lossy(),
            "--to", "postgres",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("Output directory required"));
}

#[test]
fn test_convert_single_file_via_glob_to_stdout() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "convert",
            &dir.path().join("*.sql").to_string_lossy(),
            "--to", "postgres",
        ])
        .output()
        .unwrap();
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success());
    assert!(stdout.contains("CREATE TABLE"));
}

#[test]
fn test_convert_glob_multiple_files() {
    let dir = TempDir::new().unwrap();
    let output_dir = dir.path().join("converted");
    
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "convert",
            &dir.path().join("*.sql").to_string_lossy(),
            "--from", "mysql",
            "--to", "postgres",
            "--output", &output_dir.to_string_lossy(),
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should succeed: {}", stderr);
    assert!(stderr.contains("Converting 2 files"));
    assert!(stderr.contains("Succeeded: 2"));
    
    assert!(output_dir.join("a.sql").exists());
    assert!(output_dir.join("b.sql").exists());
}

#[test]
fn test_convert_glob_dry_run() {
    let dir = TempDir::new().unwrap();
    let output_dir = dir.path().join("converted");
    
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "convert",
            &dir.path().join("*.sql").to_string_lossy(),
            "--from", "mysql",
            "--to", "postgres",
            "--output", &output_dir.to_string_lossy(),
            "--dry-run",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Stderr: {}", stderr);
    assert!(stderr.contains("Converting 2 files"));
    
    assert!(!output_dir.exists());
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

#[test]
fn test_glob_single_file_no_pattern() {
    let dir = TempDir::new().unwrap();
    let file = create_sql_file(dir.path(), "dump.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args(["validate", &file.to_string_lossy()])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success());
    assert!(stderr.contains("Result: PASSED"));
}

#[test]
fn test_glob_nonexistent_single_file() {
    let output = Command::new(binary_path())
        .args(["validate", "/nonexistent/file.sql"])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("does not exist"));
}

#[test]
fn test_glob_pattern_with_question_mark() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "dump1.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "dump2.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "dump10.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("dump?.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success());
    assert!(stderr.contains("Validating 2 files"), "Should match dump1.sql and dump2.sql: {}", stderr);
}

#[test]
fn test_glob_pattern_with_brackets() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "a.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "b.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "c.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("[ab].sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success());
    assert!(stderr.contains("Validating 2 files"), "Should match a.sql and b.sql: {}", stderr);
}

#[test]
fn test_glob_skip_directories() {
    let dir = TempDir::new().unwrap();
    let subdir = dir.path().join("subdir.sql");
    fs::create_dir(&subdir).unwrap();
    create_sql_file(dir.path(), "file.sql", simple_mysql_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success());
    assert!(stderr.contains("Result: PASSED"), "Should only process file.sql, not subdir.sql: {}", stderr);
}

#[test]
fn test_glob_mixed_dialects_with_auto_detect() {
    let dir = TempDir::new().unwrap();
    create_sql_file(dir.path(), "mysql.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "postgres.sql", postgres_dump());
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Auto-detection should work for each file: {}", stderr);
    assert!(stderr.contains("Passed: 2"));
}

#[test]
fn test_validate_strict_mode_with_glob() {
    let dir = TempDir::new().unwrap();
    
    let sql_with_warning = r#"
CREATE TABLE `users` (`id` INT PRIMARY KEY);
INSERT INTO `nonexistent` VALUES (1);
"#;
    
    create_sql_file(dir.path(), "good.sql", simple_mysql_dump());
    create_sql_file(dir.path(), "warning.sql", sql_with_warning);
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
            "--strict",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("Result: SOME FAILED"));
}

// =============================================================================
// --no-limit Flag Tests
// =============================================================================

#[test]
fn test_validate_no_limit_flag() {
    let dir = TempDir::new().unwrap();
    
    let mut sql = String::from("CREATE TABLE `items` (`id` INT PRIMARY KEY);\n");
    for i in 1..=100 {
        sql.push_str(&format!("INSERT INTO `items` VALUES ({});\n", i));
    }
    create_sql_file(dir.path(), "many_rows.sql", &sql);
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("many_rows.sql").to_string_lossy(),
            "--dialect", "mysql",
            "--max-rows-per-table", "10",
            "--no-limit",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should pass: {}", stderr);
    assert!(!stderr.contains("PK_CHECK_SKIPPED"), "--no-limit should prevent skipping");
}

#[test]
fn test_validate_zero_limit_same_as_no_limit() {
    let dir = TempDir::new().unwrap();
    
    let mut sql = String::from("CREATE TABLE `items` (`id` INT PRIMARY KEY);\n");
    for i in 1..=100 {
        sql.push_str(&format!("INSERT INTO `items` VALUES ({});\n", i));
    }
    create_sql_file(dir.path(), "many_rows.sql", &sql);
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("many_rows.sql").to_string_lossy(),
            "--dialect", "mysql",
            "--max-rows-per-table", "0",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "Should pass: {}", stderr);
    assert!(!stderr.contains("PK_CHECK_SKIPPED"), "--max-rows-per-table=0 should prevent skipping");
}

// =============================================================================
// Large Scale Tests
// =============================================================================

#[test]
fn test_glob_many_files() {
    let dir = TempDir::new().unwrap();
    
    for i in 0..10 {
        create_sql_file(dir.path(), &format!("dump_{:02}.sql", i), simple_mysql_dump());
    }
    
    let output = Command::new(binary_path())
        .args([
            "validate",
            &dir.path().join("*.sql").to_string_lossy(),
            "--dialect", "mysql",
        ])
        .output()
        .unwrap();
    
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success());
    assert!(stderr.contains("Validating 10 files"));
    assert!(stderr.contains("Passed: 10"));
}
