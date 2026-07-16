//! Unit tests for writer module, extracted from src/writer/mod.rs

use sql_splitter::splitter::Compression;
use sql_splitter::writer::{
    ParallelWriters, ProfileValues, TableWriter, WriterPool, WriterProfile,
};
use std::sync::Arc;
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

/// Regression for the 2026-07-16 field bug: `bytes_acked` counted bytes
/// landing in the writer's in-RAM coalescing buffer, so on a slow device
/// with big (64 MB) slow-profile buffers the controller saw RAM speed
/// (~600 MB/s on a 77 MB/s drive) instead of device throughput.
///
/// The invariant the fix establishes: `bytes_acked` is incremented only
/// *after* a `write(2)` to the output file returns, so at any moment it is
/// bounded above by the bytes actually on disk. With the old counting this
/// fails immediately — a whole megabyte gets "acked" into the 8 MB buffer
/// while the file is still zero bytes.
#[test]
fn test_bytes_acked_never_exceeds_bytes_on_disk() {
    let temp_dir = TempDir::new().unwrap();
    let out_dir = temp_dir.path().to_path_buf();

    // Small flush_chunk so the producer ships chunks promptly; big file_buf
    // so shipped chunks sit in the writer-side RAM buffer for a long time —
    // the exact configuration that maximized the old inflation.
    let profile = WriterProfile {
        writers: 1,
        flush_chunk: 1024,
        file_buf: 8 * 1024 * 1024,
        stage_cap: 64 * 1024 * 1024,
    };
    let values = Arc::new(ProfileValues::new(&profile));
    let mut writers =
        ParallelWriters::new(out_dir.clone(), 1, 16, Compression::None, values).unwrap();

    let stmt = format!("INSERT INTO t VALUES ('{}');", "x".repeat(200));
    let mut shipped = 0u64;
    for _ in 0..5000 {
        writers.write("t", stmt.as_bytes(), b"");
        shipped += stmt.len() as u64 + 1; // + newline
    }

    let file = out_dir.join("t.sql");
    // Poll while the writer thread drains: the counter must never run ahead
    // of the file. Reading the counter *before* the file size makes the race
    // safe — the file can only have grown in between.
    for _ in 0..50 {
        let acked = writers.stats().bytes_acked;
        let on_disk = std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0);
        assert!(
            acked <= on_disk,
            "bytes_acked ({acked}) ran ahead of bytes on disk ({on_disk}): \
             counting buffered bytes as written"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    // After finish() everything is flushed: the file must contain exactly
    // what was shipped (the counter itself is consumed with the pool).
    writers.finish().unwrap();
    let on_disk = std::fs::metadata(&file).unwrap().len();
    assert_eq!(on_disk, shipped, "flushed output size mismatch");
}
