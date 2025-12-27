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

// =============================================================================
// Dialect-Specific Split Tests
// =============================================================================

use sql_splitter::parser::SqlDialect;

#[test]
fn test_splitter_postgres_dialect() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        br#"
CREATE TABLE "users" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255)
);

INSERT INTO "users" ("id", "name") VALUES (1, 'Alice');
INSERT INTO "users" ("id", "name") VALUES (2, 'Bob');

CREATE TABLE "orders" (
    "id" SERIAL PRIMARY KEY,
    "user_id" INTEGER,
    FOREIGN KEY ("user_id") REFERENCES "users"("id")
);

INSERT INTO "orders" ("id", "user_id") VALUES (1, 1);
"#,
    )
    .unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone()).with_dialect(SqlDialect::Postgres);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert!(output_dir.join("users.sql").exists());
    assert!(output_dir.join("orders.sql").exists());

    let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
}

#[test]
fn test_splitter_sqlite_dialect() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_dir = temp_dir.path().join("output");

    std::fs::write(
        &input_file,
        br#"
CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT
);

INSERT INTO "users" VALUES (1, 'Alice');
INSERT INTO "users" VALUES (2, 'Bob');

CREATE TABLE "orders" (
    "id" INTEGER PRIMARY KEY,
    "user_id" INTEGER,
    FOREIGN KEY ("user_id") REFERENCES "users"("id")
);

INSERT INTO "orders" VALUES (1, 1);
"#,
    )
    .unwrap();

    let splitter = Splitter::new(input_file, output_dir.clone()).with_dialect(SqlDialect::Sqlite);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert!(output_dir.join("users.sql").exists());
    assert!(output_dir.join("orders.sql").exists());

    let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
}

#[test]
fn test_splitter_mssql_dialect() {
    let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/simple.sql");
    let temp_dir = TempDir::new().unwrap();
    let output_dir = temp_dir.path().join("output");

    let splitter = Splitter::new(fixture_path, output_dir.clone()).with_dialect(SqlDialect::Mssql);
    let stats = splitter.split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert!(output_dir.join("users.sql").exists());
    assert!(output_dir.join("orders.sql").exists());
}

// =============================================================================
// Dialect-Specific Split→Merge Roundtrip Tests
// =============================================================================

use sql_splitter::merger::Merger;

#[test]
fn test_split_merge_postgres_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let split_dir = temp_dir.path().join("split");
    let merged_file = temp_dir.path().join("merged.sql");

    std::fs::write(
        &input_file,
        br#"
CREATE TABLE "users" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255)
);

INSERT INTO "users" ("id", "name") VALUES (1, 'Alice');

CREATE TABLE "orders" (
    "id" SERIAL PRIMARY KEY,
    "user_id" INTEGER
);

INSERT INTO "orders" ("id", "user_id") VALUES (1, 1);
"#,
    )
    .unwrap();

    // Split
    let splitter =
        Splitter::new(input_file.clone(), split_dir.clone()).with_dialect(SqlDialect::Postgres);
    let split_stats = splitter.split().unwrap();
    assert_eq!(split_stats.tables_found, 2);

    // Merge
    let merger = Merger::new(split_dir.clone(), Some(merged_file.clone()))
        .with_dialect(SqlDialect::Postgres)
        .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(merge_stats.tables_merged, 2);

    // Verify merged content
    let content = std::fs::read_to_string(&merged_file).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
}

#[test]
fn test_split_merge_sqlite_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let split_dir = temp_dir.path().join("split");
    let merged_file = temp_dir.path().join("merged.sql");

    std::fs::write(
        &input_file,
        br#"
CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT
);

INSERT INTO "users" VALUES (1, 'Alice');

CREATE TABLE "orders" (
    "id" INTEGER PRIMARY KEY,
    "user_id" INTEGER
);

INSERT INTO "orders" VALUES (1, 1);
"#,
    )
    .unwrap();

    // Split
    let splitter =
        Splitter::new(input_file.clone(), split_dir.clone()).with_dialect(SqlDialect::Sqlite);
    let split_stats = splitter.split().unwrap();
    assert_eq!(split_stats.tables_found, 2);

    // Merge
    let merger = Merger::new(split_dir.clone(), Some(merged_file.clone()))
        .with_dialect(SqlDialect::Sqlite)
        .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(merge_stats.tables_merged, 2);

    // Verify merged content
    let content = std::fs::read_to_string(&merged_file).unwrap();
    assert!(content.contains("CREATE TABLE"));
    assert!(content.contains("INSERT INTO"));
}

#[test]
fn test_split_merge_mssql_roundtrip() {
    let fixture_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/static/mssql/simple.sql");
    let temp_dir = TempDir::new().unwrap();
    let split_dir = temp_dir.path().join("split");
    let merged_file = temp_dir.path().join("merged.sql");

    // Split
    let splitter = Splitter::new(fixture_path, split_dir.clone()).with_dialect(SqlDialect::Mssql);
    let split_stats = splitter.split().unwrap();
    assert_eq!(split_stats.tables_found, 2);

    // Merge
    let merger = Merger::new(split_dir.clone(), Some(merged_file.clone()))
        .with_dialect(SqlDialect::Mssql)
        .with_header(false);
    let merge_stats = merger.merge().unwrap();
    assert_eq!(merge_stats.tables_merged, 2);

    // Verify merged content
    let content = std::fs::read_to_string(&merged_file).unwrap();
    assert!(content.contains("CREATE TABLE") || content.contains("INSERT INTO"));
}
