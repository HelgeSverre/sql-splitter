//! Unit tests for writer module, extracted from src/writer/mod.rs

use sql_splitter::writer::{TableWriter, WriterPool};
use tempfile::TempDir;

#[test]
fn test_table_writer() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.sql");

    let mut writer = TableWriter::new(&file_path).unwrap();
    writer
        .write_statement(b"CREATE TABLE t1 (id INT);")
        .unwrap();
    writer
        .write_statement(b"INSERT INTO t1 VALUES (1);")
        .unwrap();
    writer.flush().unwrap();

    let content = std::fs::read_to_string(&file_path).unwrap();
    assert!(content.contains("CREATE TABLE t1"));
    assert!(content.contains("INSERT INTO t1"));
}

#[test]
fn test_writer_pool() {
    let temp_dir = TempDir::new().unwrap();
    let mut pool = WriterPool::new(temp_dir.path().to_path_buf());
    pool.ensure_output_dir().unwrap();

    pool.write_statement("users", b"CREATE TABLE users (id INT);")
        .unwrap();
    pool.write_statement("posts", b"CREATE TABLE posts (id INT);")
        .unwrap();
    pool.write_statement("users", b"INSERT INTO users VALUES (1);")
        .unwrap();

    pool.close_all().unwrap();

    // Verify both table files were created
    let users_content = std::fs::read_to_string(temp_dir.path().join("users.sql")).unwrap();
    assert!(users_content.contains("CREATE TABLE users"));
    assert!(users_content.contains("INSERT INTO users"));

    let posts_content = std::fs::read_to_string(temp_dir.path().join("posts.sql")).unwrap();
    assert!(posts_content.contains("CREATE TABLE posts"));
}

#[test]
fn test_table_writer_flush_after_buffer_count() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.sql");
    let mut writer = TableWriter::new(&file_path).unwrap();
    for i in 0..150 {
        writer
            .write_statement(format!("INSERT INTO t VALUES ({});", i).as_bytes())
            .unwrap();
    }
    writer.flush().unwrap();
    let content = std::fs::read_to_string(&file_path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 150);
}

#[test]
fn test_table_writer_write_statement_with_suffix() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.sql");
    let mut writer = TableWriter::new(&file_path).unwrap();
    writer
        .write_statement_with_suffix(b"INSERT INTO t VALUES (1)", b";")
        .unwrap();
    writer.flush().unwrap();
    let content = std::fs::read_to_string(&file_path).unwrap();
    assert!(content.contains("INSERT INTO t VALUES (1);"));
}

#[test]
fn test_writer_pool_creates_output_dir() {
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().join("nested").join("output");
    let pool = WriterPool::new(output_dir.clone());
    pool.ensure_output_dir().unwrap();
    assert!(output_dir.exists());
}

#[test]
fn test_writer_pool_multiple_tables() {
    let temp_dir = TempDir::new().unwrap();
    let mut pool = WriterPool::new(temp_dir.path().to_path_buf());
    pool.ensure_output_dir().unwrap();

    for table in &["users", "posts", "comments", "tags", "categories"] {
        pool.write_statement(
            table,
            format!("CREATE TABLE {} (id INT);", table).as_bytes(),
        )
        .unwrap();
    }
    pool.close_all().unwrap();

    for table in &["users", "posts", "comments", "tags", "categories"] {
        let path = temp_dir.path().join(format!("{}.sql", table));
        assert!(path.exists(), "File for table {} should exist", table);
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains(&format!("CREATE TABLE {}", table)));
    }
}

#[test]
fn test_writer_pool_write_statement_with_suffix() {
    let temp_dir = TempDir::new().unwrap();
    let mut pool = WriterPool::new(temp_dir.path().to_path_buf());
    pool.ensure_output_dir().unwrap();
    pool.write_statement_with_suffix("users", b"INSERT INTO users VALUES (1)", b";")
        .unwrap();
    pool.close_all().unwrap();
    let content = std::fs::read_to_string(temp_dir.path().join("users.sql")).unwrap();
    assert!(content.contains("INSERT INTO users VALUES (1);"));
}
