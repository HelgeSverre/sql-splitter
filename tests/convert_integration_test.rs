//! Integration tests for the convert command.

use std::fs;
use std::process::Command;
use tempfile::TempDir;

fn sql_splitter() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

#[test]
fn test_convert_mysql_to_postgres_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
CREATE TABLE `users` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `created_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

INSERT INTO `users` (`id`, `name`, `created_at`) VALUES (1, 'John', '2025-01-01 12:00:00');
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("\"users\""),
        "Should have double-quoted identifiers"
    );
    assert!(!result.contains("`"), "Should not have backticks");
    assert!(
        result.contains("SERIAL") || result.contains("INTEGER"),
        "Should convert AUTO_INCREMENT"
    );
    assert!(!result.contains("ENGINE="), "Should strip ENGINE clause");
    assert!(
        result.contains("TIMESTAMP") || result.contains("DATETIME"),
        "Should have timestamp"
    );
}

#[test]
fn test_convert_mysql_to_sqlite_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
CREATE TABLE `products` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `price` DECIMAL(10,2) NOT NULL,
  `data` JSON,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "sqlite",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("\"products\""),
        "Should have double-quoted identifiers"
    );
    assert!(!result.contains("`"), "Should not have backticks");
    assert!(result.contains("INTEGER"), "Should use INTEGER type");
    assert!(result.contains("TEXT"), "Should convert VARCHAR to TEXT");
    assert!(!result.contains("ENGINE="), "Should strip ENGINE clause");
    assert!(!result.contains("CHARSET="), "Should strip CHARSET clause");
}

#[test]
fn test_convert_string_escapes() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
INSERT INTO `messages` (`id`, `text`) VALUES (1, 'It\'s a test');
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(result.contains("It''s a test"), "Should convert \\' to ''");
    assert!(!result.contains("\\'"), "Should not have MySQL escapes");
}

#[test]
fn test_convert_strips_conditional_comments() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
/*!40101 SET NAMES utf8mb4 */;
CREATE TABLE `t` (`id` INT);
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(!result.contains("/*!"), "Should strip conditional comments");
    assert!(!result.contains("SET NAMES"), "Should strip SET NAMES");
}

#[test]
fn test_convert_type_mappings() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
CREATE TABLE `t` (
  `a` TINYINT(1),
  `b` TINYINT(4),
  `c` LONGTEXT,
  `d` BLOB,
  `e` JSON,
  `f` DATETIME
);
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("BOOLEAN"),
        "TINYINT(1) should become BOOLEAN"
    );
    assert!(
        result.contains("SMALLINT"),
        "TINYINT(4) should become SMALLINT"
    );
    assert!(result.contains("TEXT"), "LONGTEXT should become TEXT");
    assert!(result.contains("BYTEA"), "BLOB should become BYTEA");
    assert!(result.contains("JSONB"), "JSON should become JSONB");
    assert!(
        result.contains("TIMESTAMP"),
        "DATETIME should become TIMESTAMP"
    );
}

#[test]
fn test_convert_enum_warning() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
CREATE TABLE `users` (
  `status` ENUM('active', 'inactive')
);
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
            "--progress",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        result.contains("VARCHAR(255)"),
        "ENUM should become VARCHAR(255)"
    );
    assert!(!result.contains("ENUM("), "Should not have ENUM");
    assert!(
        stderr.contains("ENUM") || stderr.contains("Unsupported"),
        "Should warn about ENUM"
    );
}

#[test]
fn test_convert_dry_run() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = "CREATE TABLE `t` (`id` INT);";
    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
            "--dry-run",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);
    assert!(
        !output_file.exists(),
        "Output file should not be created in dry-run mode"
    );
}

#[test]
fn test_convert_same_dialect_error() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");

    fs::write(&input_file, "CREATE TABLE t (id INT);").unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "--from",
            "mysql",
            "--to",
            "mysql",
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "Should fail when dialects are the same"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("same"),
        "Should mention dialects are the same"
    );
}

#[test]
fn test_convert_auto_detect_dialect() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
/*!40101 SET NAMES utf8mb4 */;
CREATE TABLE `users` (`id` INT AUTO_INCREMENT);
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--to",
            "postgres",
            "--progress",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("mysql") || stderr.contains("MySQL"),
        "Should auto-detect MySQL"
    );
}

// PostgreSQL → MySQL tests

#[test]
fn test_convert_postgres_to_mysql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let postgres_sql = r#"
SET client_encoding = 'UTF8';
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL,
  "data" BYTEA,
  "active" BOOLEAN DEFAULT TRUE,
  "created_at" TIMESTAMPTZ NOT NULL
);

INSERT INTO "users" ("id", "name", "data", "active", "created_at") 
VALUES (1, 'John', NULL, TRUE, '2025-01-01 12:00:00');
"#;

    fs::write(&input_file, postgres_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "postgres",
            "--to",
            "mysql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("`users`"),
        "Should have backtick identifiers"
    );
    assert!(
        !result.contains("\"users\""),
        "Should not have double-quoted identifiers"
    );
    assert!(
        result.contains("AUTO_INCREMENT"),
        "Should convert SERIAL to AUTO_INCREMENT"
    );
    assert!(
        result.contains("LONGBLOB"),
        "Should convert BYTEA to LONGBLOB"
    );
    assert!(
        result.contains("TINYINT(1)"),
        "Should convert BOOLEAN to TINYINT(1)"
    );
    assert!(
        result.contains("DATETIME"),
        "Should convert TIMESTAMPTZ to DATETIME"
    );
    assert!(
        !result.contains("SET client_encoding"),
        "Should strip PostgreSQL session commands"
    );
}

#[test]
fn test_convert_postgres_to_mysql_jsonb() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let postgres_sql = r#"
CREATE TABLE "events" (
  "id" SERIAL PRIMARY KEY,
  "payload" JSONB NOT NULL,
  "meta" JSON
);
"#;

    fs::write(&input_file, postgres_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "postgres",
            "--to",
            "mysql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(result.contains("JSON"), "Should have JSON type");
    assert!(!result.contains("JSONB"), "Should convert JSONB to JSON");
}

// PostgreSQL → SQLite tests

#[test]
fn test_convert_postgres_to_sqlite_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let postgres_sql = r#"
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL,
  "data" BYTEA,
  "score" DOUBLE PRECISION,
  "active" BOOLEAN
);
"#;

    fs::write(&input_file, postgres_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "postgres",
            "--to",
            "sqlite",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("INTEGER"),
        "Should convert SERIAL to INTEGER"
    );
    assert!(result.contains("BLOB"), "Should convert BYTEA to BLOB");
    assert!(
        result.contains("REAL"),
        "Should convert DOUBLE PRECISION to REAL"
    );
    assert!(!result.contains("SERIAL"), "Should not have SERIAL");
    assert!(!result.contains("BYTEA"), "Should not have BYTEA");
}

// SQLite → MySQL tests

#[test]
fn test_convert_sqlite_to_mysql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let sqlite_sql = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "name" TEXT NOT NULL,
  "score" REAL,
  "data" BLOB
);

INSERT INTO "users" ("id", "name", "score", "data") VALUES (1, 'John', 99.5, NULL);
"#;

    fs::write(&input_file, sqlite_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "sqlite",
            "--to",
            "mysql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("`users`"),
        "Should have backtick identifiers"
    );
    assert!(result.contains("DOUBLE"), "Should convert REAL to DOUBLE");
    assert!(!result.contains("PRAGMA"), "Should strip SQLite pragmas");
}

// SQLite → PostgreSQL tests

#[test]
fn test_convert_sqlite_to_postgres_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let sqlite_sql = r#"
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "name" TEXT NOT NULL,
  "score" REAL,
  "data" BLOB
);
"#;

    fs::write(&input_file, sqlite_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "sqlite",
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("\"users\""),
        "Should have double-quoted identifiers"
    );
    assert!(
        result.contains("DOUBLE PRECISION"),
        "Should convert REAL to DOUBLE PRECISION"
    );
    assert!(result.contains("BYTEA"), "Should convert BLOB to BYTEA");
    assert!(!result.contains("REAL"), "Should not have REAL");
    assert!(!result.contains(" BLOB"), "Should not have BLOB");
}

// All 12 pairs roundtrip test (basic)

#[test]
fn test_convert_all_pairs_execute_successfully() {
    let test_cases = [
        ("mysql", "postgres", "CREATE TABLE `t` (`id` INT);"),
        ("mysql", "sqlite", "CREATE TABLE `t` (`id` INT);"),
        ("mysql", "mssql", "CREATE TABLE `t` (`id` INT);"),
        ("postgres", "mysql", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("postgres", "sqlite", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("postgres", "mssql", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("sqlite", "mysql", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("sqlite", "postgres", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("sqlite", "mssql", "CREATE TABLE \"t\" (\"id\" INTEGER);"),
        ("mssql", "mysql", "CREATE TABLE [t] ([id] INT)\nGO"),
        ("mssql", "postgres", "CREATE TABLE [t] ([id] INT)\nGO"),
        ("mssql", "sqlite", "CREATE TABLE [t] ([id] INT)\nGO"),
    ];

    for (from, to, sql) in test_cases {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_file = temp_dir.path().join("output.sql");

        fs::write(&input_file, sql).unwrap();

        let output = sql_splitter()
            .args([
                "convert",
                input_file.to_str().unwrap(),
                "-o",
                output_file.to_str().unwrap(),
                "--from",
                from,
                "--to",
                to,
            ])
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "Conversion {} → {} failed: {:?}",
            from,
            to,
            String::from_utf8_lossy(&output.stderr)
        );

        assert!(
            output_file.exists(),
            "Output file should exist for {} → {}",
            from,
            to
        );
    }
}

// MSSQL → Other Dialects tests

#[test]
fn test_convert_mssql_to_postgres_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mssql_sql = r#"SET ANSI_NULLS ON
GO
CREATE TABLE [dbo].[users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [name] NVARCHAR(100),
    [created_at] DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT [PK_users] PRIMARY KEY CLUSTERED ([id])
) ON [PRIMARY]
GO

INSERT INTO [dbo].[users] ([email], [name]) VALUES (N'alice@example.com', N'Alice')
GO
"#;

    fs::write(&input_file, mssql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "mssql",
            "--to",
            "postgres",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("\"users\""),
        "Should have double-quoted identifiers"
    );
    assert!(!result.contains("["), "Should not have brackets");
    assert!(
        result.contains("SERIAL") || result.contains("INTEGER"),
        "Should convert IDENTITY"
    );
    assert!(!result.contains("ON [PRIMARY]"), "Should strip filegroup");
    assert!(
        result.contains("TIMESTAMP"),
        "Should convert DATETIME2 to TIMESTAMP"
    );
}

#[test]
fn test_convert_mssql_to_mysql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mssql_sql = r#"CREATE TABLE [users] (
    [id] INT IDENTITY(1,1) NOT NULL,
    [email] NVARCHAR(255) NOT NULL,
    [active] BIT DEFAULT 1
)
GO
"#;

    fs::write(&input_file, mssql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "mssql",
            "--to",
            "mysql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("`users`"),
        "Should have backtick identifiers"
    );
    assert!(!result.contains("["), "Should not have brackets");
    assert!(
        result.contains("AUTO_INCREMENT"),
        "Should convert IDENTITY to AUTO_INCREMENT"
    );
    assert!(
        result.contains("TINYINT(1)"),
        "Should convert BIT to TINYINT(1)"
    );
}

#[test]
fn test_convert_mssql_to_sqlite_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mssql_sql = r#"CREATE TABLE [products] (
    [id] BIGINT IDENTITY(1,1) NOT NULL,
    [name] NVARCHAR(MAX),
    [price] MONEY,
    [data] VARBINARY(MAX)
)
GO
"#;

    fs::write(&input_file, mssql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "mssql",
            "--to",
            "sqlite",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("\"products\""),
        "Should have double-quoted identifiers"
    );
    assert!(!result.contains("["), "Should not have brackets");
    assert!(
        result.contains("INTEGER") || result.contains("BIGINT"),
        "Should convert to INTEGER or BIGINT"
    );
    assert!(result.contains("TEXT"), "Should convert NVARCHAR(MAX) to TEXT");
    assert!(result.contains("BLOB"), "Should convert VARBINARY(MAX) to BLOB");
}

// Other Dialects → MSSQL tests

#[test]
fn test_convert_mysql_to_mssql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let mysql_sql = r#"
CREATE TABLE `users` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  `active` TINYINT(1) DEFAULT 1,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

INSERT INTO `users` (`id`, `name`, `active`) VALUES (1, 'John', 1);
"#;

    fs::write(&input_file, mysql_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "mysql",
            "--to",
            "mssql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("[users]"),
        "Should have bracket identifiers"
    );
    assert!(!result.contains("`"), "Should not have backticks");
    assert!(
        result.contains("IDENTITY"),
        "Should convert AUTO_INCREMENT to IDENTITY"
    );
    assert!(result.contains("BIT"), "Should convert TINYINT(1) to BIT");
    assert!(!result.contains("ENGINE="), "Should strip ENGINE clause");
}

#[test]
fn test_convert_postgres_to_mssql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let postgres_sql = r#"
CREATE TABLE "users" (
  "id" SERIAL PRIMARY KEY,
  "name" VARCHAR(255) NOT NULL,
  "data" BYTEA,
  "active" BOOLEAN DEFAULT TRUE,
  "created_at" TIMESTAMPTZ NOT NULL
);
"#;

    fs::write(&input_file, postgres_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "postgres",
            "--to",
            "mssql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("[users]"),
        "Should have bracket identifiers"
    );
    assert!(
        result.contains("IDENTITY"),
        "Should convert SERIAL to IDENTITY"
    );
    assert!(
        result.contains("VARBINARY") || result.contains("IMAGE"),
        "Should convert BYTEA to binary type"
    );
    assert!(result.contains("BIT"), "Should convert BOOLEAN to BIT");
}

#[test]
fn test_convert_sqlite_to_mssql_basic() {
    let temp_dir = TempDir::new().unwrap();
    let input_file = temp_dir.path().join("input.sql");
    let output_file = temp_dir.path().join("output.sql");

    let sqlite_sql = r#"
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "name" TEXT NOT NULL,
  "score" REAL,
  "data" BLOB
);
"#;

    fs::write(&input_file, sqlite_sql).unwrap();

    let output = sql_splitter()
        .args([
            "convert",
            input_file.to_str().unwrap(),
            "-o",
            output_file.to_str().unwrap(),
            "--from",
            "sqlite",
            "--to",
            "mssql",
        ])
        .output()
        .unwrap();

    assert!(output.status.success(), "Command failed: {:?}", output);

    let result = fs::read_to_string(&output_file).unwrap();

    assert!(
        result.contains("[users]"),
        "Should have bracket identifiers"
    );
    assert!(
        result.contains("NVARCHAR") || result.contains("VARCHAR"),
        "Should convert TEXT to VARCHAR"
    );
    assert!(result.contains("FLOAT"), "Should convert REAL to FLOAT");
    assert!(
        result.contains("VARBINARY"),
        "Should convert BLOB to VARBINARY"
    );
}
