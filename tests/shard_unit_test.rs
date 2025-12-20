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
