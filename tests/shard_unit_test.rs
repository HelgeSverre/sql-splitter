//! Unit tests extracted from shard module

use sql_splitter::parser::SqlDialect;
use sql_splitter::shard::{
    DefaultShardClassifier, GlobalTableMode, ShardConfig, ShardTableClassification, ShardYamlConfig,
};
use std::io::Write;
use tempfile::NamedTempFile;

mod mod_tests {
    use super::*;

    fn create_test_dump() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
CREATE TABLE `companies` (
    `id` int NOT NULL AUTO_INCREMENT,
    `name` varchar(255),
    PRIMARY KEY (`id`)
);

INSERT INTO `companies` VALUES (1, 'Acme Corp'), (2, 'Widgets Inc'), (3, 'Tech Co');

CREATE TABLE `users` (
    `id` int NOT NULL AUTO_INCREMENT,
    `name` varchar(255),
    `company_id` int,
    PRIMARY KEY (`id`),
    FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`)
);

INSERT INTO `users` VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2), (4, 'Dave', 3);

CREATE TABLE `orders` (
    `id` int NOT NULL AUTO_INCREMENT,
    `company_id` int,
    `user_id` int,
    `amount` decimal(10,2),
    PRIMARY KEY (`id`),
    FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`),
    FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
);

INSERT INTO `orders` VALUES (1, 1, 1, 100.00), (2, 1, 1, 200.00), (3, 2, 2, 150.00), (4, 3, 3, 300.00);
"#
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_shard_single_tenant() {
        let dump = create_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::MySql,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0);
        assert!(stats.total_rows_selected > 0);
        // Should include company 1, users Alice and Bob, and their orders
    }

    #[test]
    fn test_shard_dry_run() {
        let dump = create_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::MySql,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0);
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }

    #[test]
    fn test_tenant_column_detection() {
        let dump = create_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::MySql,
            tenant_column: None, // Should auto-detect company_id
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }

    #[test]
    fn test_default_classifier() {
        assert!(DefaultShardClassifier::is_system_table("migrations"));
        assert!(DefaultShardClassifier::is_system_table("failed_jobs"));
        assert!(!DefaultShardClassifier::is_system_table("users"));

        assert!(DefaultShardClassifier::is_lookup_table("countries"));
        assert!(!DefaultShardClassifier::is_lookup_table("orders"));
    }
}

mod config_tests {
    use super::*;

    #[test]
    fn test_parse_yaml_config() {
        let yaml = r#"
tenant:
  column: company_id
  root_tables:
    - companies
    - users

tables:
  migrations:
    role: system
  permissions:
    role: lookup
    include: true
  role_user:
    role: junction
  comments:
    self_fk: parent_id
  activity_log:
    skip: true

include_global: lookups
"#;

        let config: ShardYamlConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.tenant.column, Some("company_id".to_string()));
        assert!(config.tenant.root_tables.contains(&"companies".to_string()));

        assert_eq!(
            config.get_classification("migrations"),
            Some(ShardTableClassification::System)
        );
        assert_eq!(
            config.get_classification("permissions"),
            Some(ShardTableClassification::Lookup)
        );
        assert_eq!(
            config.get_classification("role_user"),
            Some(ShardTableClassification::Junction)
        );

        assert!(config.should_skip("activity_log"));
        assert!(!config.should_skip("users"));
    }

    #[test]
    fn test_default_classifier() {
        assert!(DefaultShardClassifier::is_system_table("migrations"));
        assert!(DefaultShardClassifier::is_system_table("failed_jobs"));
        assert!(DefaultShardClassifier::is_system_table("telescope_entries"));
        assert!(!DefaultShardClassifier::is_system_table("users"));

        assert!(DefaultShardClassifier::is_lookup_table("countries"));
        assert!(DefaultShardClassifier::is_lookup_table("permissions"));
        assert!(!DefaultShardClassifier::is_lookup_table("orders"));

        assert!(DefaultShardClassifier::is_junction_table_by_name(
            "role_user_pivot"
        ));
        assert!(DefaultShardClassifier::is_junction_table_by_name(
            "user_has_role"
        ));
        assert!(!DefaultShardClassifier::is_junction_table_by_name("users"));
    }

    #[test]
    fn test_global_table_mode() {
        assert_eq!(
            "none".parse::<GlobalTableMode>().unwrap(),
            GlobalTableMode::None
        );
        assert_eq!(
            "lookups".parse::<GlobalTableMode>().unwrap(),
            GlobalTableMode::Lookups
        );
        assert_eq!(
            "all".parse::<GlobalTableMode>().unwrap(),
            GlobalTableMode::All
        );
    }
}

// =============================================================================
// Postgres Shard Tests
// =============================================================================

mod postgres_tests {
    use super::*;

    fn create_postgres_test_dump() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
CREATE TABLE "companies" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255)
);

INSERT INTO "companies" VALUES (1, 'Acme Corp'), (2, 'Widgets Inc'), (3, 'Tech Co');

CREATE TABLE "users" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(255),
    "company_id" INTEGER,
    FOREIGN KEY ("company_id") REFERENCES "companies" ("id")
);

INSERT INTO "users" VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2), (4, 'Dave', 3);

CREATE TABLE "orders" (
    "id" SERIAL PRIMARY KEY,
    "company_id" INTEGER,
    "user_id" INTEGER,
    "amount" DECIMAL(10,2),
    FOREIGN KEY ("company_id") REFERENCES "companies" ("id"),
    FOREIGN KEY ("user_id") REFERENCES "users" ("id")
);

INSERT INTO "orders" VALUES (1, 1, 1, 100.00), (2, 1, 1, 200.00), (3, 2, 2, 150.00), (4, 3, 3, 300.00);
"#
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_shard_postgres_single_tenant() {
        let dump = create_postgres_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::Postgres,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0, "Should process tables");
        assert!(stats.total_rows_selected > 0, "Should select some rows");
    }

    #[test]
    fn test_shard_postgres_dry_run() {
        let dump = create_postgres_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::Postgres,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0);
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }

    #[test]
    fn test_shard_postgres_tenant_detection() {
        let dump = create_postgres_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::Postgres,
            tenant_column: None, // Should auto-detect company_id
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }
}

// =============================================================================
// SQLite Shard Tests
// =============================================================================

mod sqlite_tests {
    use super::*;

    fn create_sqlite_test_dump() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
CREATE TABLE "companies" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT
);

INSERT INTO "companies" VALUES (1, 'Acme Corp'), (2, 'Widgets Inc'), (3, 'Tech Co');

CREATE TABLE "users" (
    "id" INTEGER PRIMARY KEY,
    "name" TEXT,
    "company_id" INTEGER,
    FOREIGN KEY ("company_id") REFERENCES "companies" ("id")
);

INSERT INTO "users" VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2), (4, 'Dave', 3);

CREATE TABLE "orders" (
    "id" INTEGER PRIMARY KEY,
    "company_id" INTEGER,
    "user_id" INTEGER,
    "amount" REAL,
    FOREIGN KEY ("company_id") REFERENCES "companies" ("id"),
    FOREIGN KEY ("user_id") REFERENCES "users" ("id")
);

INSERT INTO "orders" VALUES (1, 1, 1, 100.00), (2, 1, 1, 200.00), (3, 2, 2, 150.00), (4, 3, 3, 300.00);
"#
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_shard_sqlite_single_tenant() {
        let dump = create_sqlite_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::Sqlite,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0, "Should process tables");
        assert!(stats.total_rows_selected > 0, "Should select some rows");
    }

    #[test]
    fn test_shard_sqlite_dry_run() {
        let dump = create_sqlite_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::Sqlite,
            tenant_column: Some("company_id".to_string()),
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0);
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }

    #[test]
    fn test_shard_sqlite_tenant_detection() {
        let dump = create_sqlite_test_dump();

        let config = ShardConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::Sqlite,
            tenant_column: None, // Should auto-detect company_id
            tenant_value: "1".to_string(),
            dry_run: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();
        assert_eq!(stats.detected_tenant_column, Some("company_id".to_string()));
    }
}

// =============================================================================
// MSSQL Shard Tests (non-dry-run variant)
// =============================================================================

mod mssql_tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn mssql_multi_tenant_fixture() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/static/mssql/multi_tenant.sql")
    }

    #[test]
    fn test_shard_mssql_write_output() {
        let tmp = TempDir::new().unwrap();
        let out = tmp.path().join("shard.sql");

        let config = ShardConfig {
            input: mssql_multi_tenant_fixture(),
            output: Some(out.clone()),
            dialect: SqlDialect::Mssql,
            tenant_column: Some("tenant_id".to_string()),
            tenant_value: "1".to_string(),
            dry_run: false,
            include_schema: true,
            ..Default::default()
        };

        let stats = sql_splitter::shard::run(config).unwrap();

        assert!(stats.tables_processed > 0, "Should process tables");
        assert!(out.exists(), "Output file should be created");

        let content = fs::read_to_string(&out).unwrap();
        assert!(
            content.contains("INSERT INTO"),
            "Output should contain INSERT statements"
        );
    }
}
