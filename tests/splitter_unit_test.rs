use sql_splitter::parser::ContentFilter;
use sql_splitter::splitter::{Compression, Splitter};
use tempfile::TempDir;

#[test]
fn test_splitter_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nCREATE TABLE posts (id INT);\n",
    )
    .unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone());
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert_eq!(stats.statements_processed, 3);

    assert!(output_dir.join("users.sql").exists());
    assert!(output_dir.join("posts.sql").exists());
}

#[test]
fn test_splitter_dry_run() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(&input_file, b"CREATE TABLE users (id INT);").unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone()).with_dry_run(true);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 1);
    assert!(!output_dir.exists());
}

#[test]
fn test_splitter_table_filter() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nCREATE TABLE posts (id INT);\nCREATE TABLE orders (id INT);",
    )
    .unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone())
        .with_table_filter(vec!["users".to_string(), "orders".to_string()]);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert!(output_dir.join("users.sql").exists());
    assert!(!output_dir.join("posts.sql").exists());
    assert!(output_dir.join("orders.sql").exists());
}

#[test]
fn test_splitter_schema_only() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);",
    )
    .unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone())
        .with_content_filter(ContentFilter::SchemaOnly);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 1);
    assert_eq!(stats.statements_processed, 1); // Only CREATE TABLE

    let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(!content.contains("INSERT"));
}

#[test]
fn test_splitter_data_only() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);",
    )
    .unwrap();

    let splitter =
        Splitter::new(input_file, output_dir.clone()).with_content_filter(ContentFilter::DataOnly);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 1);
    assert_eq!(stats.statements_processed, 2); // Only INSERTs

    let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
    assert!(!content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT"));
}

#[test]
fn test_splitter_gzip_compressed() {
    use flate2::write::GzEncoder;
    use flate2::Compression as GzCompression;
    use std::io::Write;

    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql.gz");
    let output_dir = temp_dir.path().join("output");

    // Create gzipped SQL file
    let file = std::fs::File::create(&input_file).unwrap();
    let mut encoder = GzEncoder::new(file, GzCompression::default());
    encoder
        .write_all(b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);")
        .unwrap();
    encoder.finish().unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone());
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 1);
    assert_eq!(stats.statements_processed, 2);
    assert!(output_dir.join("users.sql").exists());
}

#[test]
fn test_compression_detection() {
    assert_eq!(
        Compression::from_path(std::path::Path::new("file.sql")),
        Compression::None
    );
    assert_eq!(
        Compression::from_path(std::path::Path::new("file.sql.gz")),
        Compression::Gzip
    );
    assert_eq!(
        Compression::from_path(std::path::Path::new("file.sql.bz2")),
        Compression::Bzip2
    );
    assert_eq!(
        Compression::from_path(std::path::Path::new("file.sql.xz")),
        Compression::Xz
    );
    assert_eq!(
        Compression::from_path(std::path::Path::new("file.sql.zst")),
        Compression::Zstd
    );
}
