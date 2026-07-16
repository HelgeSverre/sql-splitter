//! Unit tests for the writer module's production pipeline (`ParallelWriters`).
//!
//! The legacy single-threaded `TableWriter`/`WriterPool` were removed once
//! `split` moved entirely to `ParallelWriters`; the output-shape assertions
//! they carried live on here against the real writer.

use sql_splitter::splitter::Compression;
use sql_splitter::writer::{ParallelWriters, ProfileKind, ProfileValues, WriterProfile};
use std::sync::Arc;
use tempfile::TempDir;

/// A small writer pool with the default (SSD) profile values.
fn pool(dir: &std::path::Path, num_writers: usize) -> ParallelWriters {
    let profile = WriterProfile::for_kind(ProfileKind::Ssd, 4, false);
    let values = Arc::new(ProfileValues::new(&profile));
    ParallelWriters::new(
        dir.to_path_buf(),
        num_writers,
        16,
        Compression::None,
        values,
    )
    .unwrap()
}

#[test]
fn test_single_table_output() {
    let temp_dir = TempDir::new().unwrap();
    let mut writers = pool(temp_dir.path(), 1);

    writers.write("t1", b"CREATE TABLE t1 (id INT);", b"");
    writers.write("t1", b"INSERT INTO t1 VALUES (1);", b"");
    writers.finish().unwrap();

    let content = std::fs::read_to_string(temp_dir.path().join("t1.sql")).unwrap();
    assert_eq!(
        content,
        "CREATE TABLE t1 (id INT);\nINSERT INTO t1 VALUES (1);\n"
    );
}

#[test]
fn test_statements_preserve_input_order_per_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut writers = pool(temp_dir.path(), 4);

    for i in 0..150 {
        writers.write(
            "t",
            format!("INSERT INTO t VALUES ({});", i).as_bytes(),
            b"",
        );
    }
    writers.finish().unwrap();

    let content = std::fs::read_to_string(temp_dir.path().join("t.sql")).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 150);
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(*line, format!("INSERT INTO t VALUES ({});", i));
    }
}

#[test]
fn test_write_with_suffix() {
    let temp_dir = TempDir::new().unwrap();
    let mut writers = pool(temp_dir.path(), 1);

    writers.write("users", b"INSERT INTO users VALUES (1)", b";");
    writers.finish().unwrap();

    let content = std::fs::read_to_string(temp_dir.path().join("users.sql")).unwrap();
    assert_eq!(content, "INSERT INTO users VALUES (1);\n");
}

#[test]
fn test_creates_output_dir() {
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().join("nested").join("output");
    let writers = pool(&output_dir, 1);
    assert!(output_dir.exists());
    writers.finish().unwrap();
}

#[test]
fn test_multiple_tables_get_their_own_files() {
    let temp_dir = TempDir::new().unwrap();
    let mut writers = pool(temp_dir.path(), 4);

    for table in &["users", "posts", "comments", "tags", "categories"] {
        writers.write(
            table,
            format!("CREATE TABLE {} (id INT);", table).as_bytes(),
            b"",
        );
    }
    writers.finish().unwrap();

    for table in &["users", "posts", "comments", "tags", "categories"] {
        let path = temp_dir.path().join(format!("{}.sql", table));
        assert!(path.exists(), "File for table {} should exist", table);
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, format!("CREATE TABLE {} (id INT);\n", table));
    }
}

/// Growing the pool mid-run must not disturb per-table ordering: tables seen
/// before the growth keep their owner thread (sticky shard assignment).
#[test]
fn test_grow_to_preserves_per_table_order() {
    let temp_dir = TempDir::new().unwrap();
    let mut writers = pool(temp_dir.path(), 1);

    for i in 0..50 {
        writers.write(
            "a",
            format!("INSERT INTO a VALUES ({});", i).as_bytes(),
            b"",
        );
    }
    writers.grow_to(4);
    assert_eq!(writers.writer_count(), 4);
    for i in 50..100 {
        writers.write(
            "a",
            format!("INSERT INTO a VALUES ({});", i).as_bytes(),
            b"",
        );
        writers.write(
            "b",
            format!("INSERT INTO b VALUES ({});", i).as_bytes(),
            b"",
        );
    }
    writers.finish().unwrap();

    let a = std::fs::read_to_string(temp_dir.path().join("a.sql")).unwrap();
    let expected: String = (0..100)
        .map(|i| format!("INSERT INTO a VALUES ({});\n", i))
        .collect();
    assert_eq!(a, expected);
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
