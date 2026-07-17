//! Integration tests characterizing `shard` output through the visitor-based
//! row-parsing path (Task 17). These pin the rows selected for a tenant so the
//! streaming `for_each_data_row` refactor cannot silently change behavior.

mod support;

use sql_splitter::parser::SqlDialect;
use sql_splitter::shard::{run, GlobalTableMode, ShardConfig};
use std::fs;
use std::io::Write;
use support::generated_fixture::generated_fixture;
use tempfile::{NamedTempFile, TempDir};

/// A small multi-tenant MySQL dump (multi-row INSERTs) with a clear tenant key.
fn multi_tenant_mysql() -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    write!(
        file,
        r#"CREATE TABLE `companies` (
  `id` int NOT NULL,
  `name` varchar(255),
  PRIMARY KEY (`id`)
);

INSERT INTO `companies` VALUES (1, 'Acme'), (2, 'Globex'), (3, 'Initech');

CREATE TABLE `users` (
  `id` int NOT NULL,
  `name` varchar(255),
  `company_id` int,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`)
);

INSERT INTO `users` VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2), (4, 'Dave', 3);
"#
    )
    .unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn shard_mysql_selects_only_tenant_rows() {
    let dump = multi_tenant_mysql();
    let out_dir = TempDir::new().unwrap();
    let out = out_dir.path().join("shard.sql");

    let config = ShardConfig {
        input: dump.path().to_path_buf(),
        output: Some(out.clone()),
        dialect: SqlDialect::MySql,
        tenant_column: Some("company_id".to_string()),
        tenant_value: "1".to_string(),
        include_global: GlobalTableMode::All,
        ..Default::default()
    };

    let stats = run(config).unwrap();
    assert!(stats.total_rows_selected > 0);

    let content = fs::read_to_string(&out).unwrap();
    // Tenant 1's users are present; the other tenants' users are not.
    assert!(
        content.contains("'Alice'"),
        "Alice (tenant 1) should be kept"
    );
    assert!(content.contains("'Bob'"), "Bob (tenant 1) should be kept");
    assert!(
        !content.contains("'Carol'"),
        "Carol (tenant 2) should be excluded"
    );
    assert!(
        !content.contains("'Dave'"),
        "Dave (tenant 3) should be excluded"
    );
}

#[test]
fn shard_mysql_is_reproducible() {
    let dump = multi_tenant_mysql();
    let out_dir = TempDir::new().unwrap();

    let render = |name: &str| {
        let out = out_dir.path().join(name);
        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: Some(out.clone()),
            dialect: SqlDialect::MySql,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            include_global: GlobalTableMode::All,
            ..Default::default()
        };
        run(config).unwrap();
        fs::read_to_string(&out)
            .unwrap()
            .lines()
            .filter(|l| !l.starts_with("-- "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    assert_eq!(render("a.sql"), render("b.sql"));
}

/// Generate a PostgreSQL dump (COPY blocks) so the COPY streaming path is
/// exercised end-to-end through shard.
fn generate_postgres_dump() -> tempfile::TempPath {
    generated_fixture(SqlDialect::Postgres, None, None, 7)
}

#[test]
fn shard_postgres_copy_path_runs() {
    let dump = generate_postgres_dump();
    let out_dir = TempDir::new().unwrap();
    let out = out_dir.path().join("shard_pg.sql");

    let config = ShardConfig {
        input: dump.to_path_buf(),
        output: Some(out.clone()),
        dialect: SqlDialect::Postgres,
        tenant_value: "1".to_string(),
        include_global: GlobalTableMode::All,
        ..Default::default()
    };

    let stats = run(config).unwrap();
    assert!(stats.tables_processed > 0);

    let content = fs::read_to_string(&out).unwrap();
    assert!(content.contains("INSERT INTO"));
    // COPY tab-separated format must have been converted, not passed through.
    assert!(
        !content.contains("\t1\t") && !content.contains("\t2\t"),
        "output should not contain raw tab-separated COPY rows"
    );
}
