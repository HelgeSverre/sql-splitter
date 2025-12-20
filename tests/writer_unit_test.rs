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
