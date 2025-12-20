//! Unit tests for merger module, extracted from src/merger/mod.rs

use sql_splitter::merger::Merger;
use sql_splitter::parser::SqlDialect;
use std::collections::HashSet;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_merge_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    // Create input directory with some SQL files
    fs::create_dir_all(&input_dir).unwrap();
    fs::write(
        input_dir.join("users.sql"),
        "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\n",
    )
    .unwrap();
    fs::write(
        input_dir.join("posts.sql"),
        "CREATE TABLE posts (id INT);\n",
    )
    .unwrap();

    // Merge
    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::MySql)
        .with_header(true);

    let stats = merger.merge().unwrap();

    assert_eq!(stats.tables_merged, 2);
    assert!(stats.table_names.contains(&"users".to_string()));
    assert!(stats.table_names.contains(&"posts".to_string()));

    // Verify output
    let content = fs::read_to_string(&output_file).unwrap();
    assert!(content.contains("CREATE TABLE users"));
    assert!(content.contains("CREATE TABLE posts"));
    assert!(content.contains("SET FOREIGN_KEY_CHECKS = 0"));
}

#[test]
fn test_merge_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("users.sql"), "-- users\n").unwrap();
    fs::write(input_dir.join("posts.sql"), "-- posts\n").unwrap();
    fs::write(input_dir.join("comments.sql"), "-- comments\n").unwrap();

    let mut tables = HashSet::new();
    tables.insert("users".to_string());
    tables.insert("posts".to_string());

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_tables(tables)
        .with_header(false);

    let stats = merger.merge().unwrap();

    assert_eq!(stats.tables_merged, 2);
    assert!(!stats.table_names.contains(&"comments".to_string()));
}

#[test]
fn test_merge_with_exclude() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("users.sql"), "-- users\n").unwrap();
    fs::write(input_dir.join("cache.sql"), "-- cache\n").unwrap();
    fs::write(input_dir.join("sessions.sql"), "-- sessions\n").unwrap();

    let mut exclude = HashSet::new();
    exclude.insert("cache".to_string());
    exclude.insert("sessions".to_string());

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_exclude(exclude)
        .with_header(false);

    let stats = merger.merge().unwrap();

    assert_eq!(stats.tables_merged, 1);
    assert!(stats.table_names.contains(&"users".to_string()));
}
