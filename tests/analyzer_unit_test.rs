//! Unit tests for analyzer module, extracted from src/analyzer/mod.rs

use sql_splitter::analyzer::Analyzer;
use tempfile::TempDir;

#[test]
fn test_analyzer_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");

    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);\nCREATE TABLE posts (id INT);\nINSERT INTO posts VALUES (1);",
    )
    .unwrap();

    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();

    assert_eq!(stats.len(), 2);

    let users_stats = stats.iter().find(|s| s.table_name == "users").unwrap();
    assert_eq!(users_stats.insert_count, 2);
    assert_eq!(users_stats.create_count, 1);
    assert_eq!(users_stats.statement_count, 3);

    let posts_stats = stats.iter().find(|s| s.table_name == "posts").unwrap();
    assert_eq!(posts_stats.insert_count, 1);
    assert_eq!(posts_stats.create_count, 1);
    assert_eq!(posts_stats.statement_count, 2);
}

#[test]
fn test_analyzer_sorted_by_insert_count() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");

    std::fs::write(
        &input_file,
        b"CREATE TABLE a (id INT);\nINSERT INTO a VALUES (1);\nCREATE TABLE b (id INT);\nINSERT INTO b VALUES (1);\nINSERT INTO b VALUES (2);\nINSERT INTO b VALUES (3);",
    )
    .unwrap();

    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();

    assert_eq!(stats[0].table_name, "b");
    assert_eq!(stats[0].insert_count, 3);
    assert_eq!(stats[1].table_name, "a");
    assert_eq!(stats[1].insert_count, 1);
}

#[test]
fn test_analyzer_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("empty.sql");
    std::fs::write(&input_file, b"").unwrap();
    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();
    assert!(stats.is_empty());
}

#[test]
fn test_analyzer_only_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    std::fs::write(
        &input_file,
        b"INSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);\nINSERT INTO users VALUES (3);",
    )
    .unwrap();
    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].insert_count, 3);
    assert_eq!(stats[0].create_count, 0);
}

#[test]
fn test_analyzer_tracks_total_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    std::fs::write(
        &input_file,
        b"CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);",
    )
    .unwrap();
    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();
    assert_eq!(stats.len(), 1);
    assert!(stats[0].total_bytes > 0);
}

#[test]
fn test_analyzer_with_dialect() {
    use sql_splitter::parser::SqlDialect;
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);",
    )
    .unwrap();
    let analyzer = Analyzer::new(input_file).with_dialect(SqlDialect::Postgres);
    let stats = analyzer.analyze().unwrap();
    assert_eq!(stats.len(), 1);
}

#[test]
fn test_analyzer_unknown_statements_skipped() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    std::fs::write(
        &input_file,
        b"SET NAMES utf8;\nSET FOREIGN_KEY_CHECKS = 0;\nCREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);",
    )
    .unwrap();
    let analyzer = Analyzer::new(input_file);
    let stats = analyzer.analyze().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].statement_count, 2);
}
