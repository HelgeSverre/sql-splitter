//! Unit tests for cmd module, extracted from src/cmd/merge.rs
//!
//! Note: The internal functions (write_header, transaction_start, etc.) are
//! not publicly exported, so we test through the public Merger API instead.

use sql_splitter::merger::Merger;
use sql_splitter::parser::SqlDialect;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_write_header_mysql() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("t1.sql"), "-- t1\n").unwrap();
    fs::write(input_dir.join("t2.sql"), "-- t2\n").unwrap();
    fs::write(input_dir.join("t3.sql"), "-- t3\n").unwrap();
    fs::write(input_dir.join("t4.sql"), "-- t4\n").unwrap();
    fs::write(input_dir.join("t5.sql"), "-- t5\n").unwrap();

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::MySql)
        .with_header(true);

    merger.merge().unwrap();
    let output = fs::read_to_string(&output_file).unwrap();

    assert!(output.contains("SET NAMES utf8mb4;"));
    assert!(output.contains("SET FOREIGN_KEY_CHECKS = 0;"));
    assert!(output.contains("Tables: 5"));
}

#[test]
fn test_write_header_postgres() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("t1.sql"), "-- t1\n").unwrap();
    fs::write(input_dir.join("t2.sql"), "-- t2\n").unwrap();
    fs::write(input_dir.join("t3.sql"), "-- t3\n").unwrap();

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::Postgres)
        .with_header(true);

    merger.merge().unwrap();
    let output = fs::read_to_string(&output_file).unwrap();

    assert!(output.contains("SET client_encoding = 'UTF8';"));
    assert!(output.contains("Tables: 3"));
}

#[test]
fn test_transaction_wrappers_mysql() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("t1.sql"), "-- t1\n").unwrap();

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::MySql)
        .with_transaction(true)
        .with_header(false);

    merger.merge().unwrap();
    let output = fs::read_to_string(&output_file).unwrap();

    assert!(output.contains("START TRANSACTION"));
    assert!(output.contains("COMMIT"));
}

#[test]
fn test_transaction_wrappers_postgres() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("t1.sql"), "-- t1\n").unwrap();

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::Postgres)
        .with_transaction(true)
        .with_header(false);

    merger.merge().unwrap();
    let output = fs::read_to_string(&output_file).unwrap();

    assert!(output.contains("BEGIN"));
    assert!(output.contains("COMMIT"));
}

#[test]
fn test_transaction_wrappers_sqlite() {
    let temp_dir = TempDir::new().unwrap();
    let input_dir = temp_dir.path().join("tables");
    let output_file = temp_dir.path().join("merged.sql");

    fs::create_dir_all(&input_dir).unwrap();
    fs::write(input_dir.join("t1.sql"), "-- t1\n").unwrap();

    let merger = Merger::new(input_dir, Some(output_file.clone()))
        .with_dialect(SqlDialect::Sqlite)
        .with_transaction(true)
        .with_header(false);

    merger.merge().unwrap();
    let output = fs::read_to_string(&output_file).unwrap();

    assert!(output.contains("BEGIN TRANSACTION"));
    assert!(output.contains("COMMIT"));
}
