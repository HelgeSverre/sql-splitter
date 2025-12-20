//! Unit tests for the sample module, extracted from inline tests.

use rand::SeedableRng;
use rand::rngs::StdRng;
use sql_splitter::sample::{GlobalTableMode, SampleYamlConfig, TableClassification, DefaultClassifier, Reservoir};

mod sample_tests {
    use sql_splitter::sample::{SampleConfig, SampleMode, run};
    use sql_splitter::parser::SqlDialect;
    use std::io::Write;
    use tempfile::NamedTempFile;

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
"#
        )
        .unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_sample_basic() {
        let dump = create_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = SampleConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::MySql,
            mode: SampleMode::Percent(100),
            preserve_relations: false,
            seed: 42,
            ..Default::default()
        };

        let stats = run(config).unwrap();

        assert_eq!(stats.tables_sampled, 2);
        assert_eq!(stats.total_rows_seen, 7);
        assert_eq!(stats.total_rows_selected, 7);
    }

    #[test]
    fn test_sample_with_preserve_relations() {
        let dump = create_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = SampleConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::MySql,
            mode: SampleMode::Rows(2),
            preserve_relations: true,
            seed: 42,
            ..Default::default()
        };

        let stats = run(config).unwrap();

        assert_eq!(stats.tables_sampled, 2);
    }

    #[test]
    fn test_sample_dry_run() {
        let dump = create_test_dump();

        let config = SampleConfig {
            input: dump.path().to_path_buf(),
            output: None,
            dialect: SqlDialect::MySql,
            mode: SampleMode::Percent(50),
            preserve_relations: false,
            seed: 42,
            dry_run: true,
            ..Default::default()
        };

        let stats = run(config).unwrap();

        assert_eq!(stats.tables_sampled, 2);
    }

    #[test]
    fn test_sample_with_max_rows() {
        let dump = create_test_dump();
        let output = NamedTempFile::new().unwrap();

        let config = SampleConfig {
            input: dump.path().to_path_buf(),
            output: Some(output.path().to_path_buf()),
            dialect: SqlDialect::MySql,
            mode: SampleMode::Percent(100),
            preserve_relations: false,
            seed: 42,
            max_total_rows: Some(5),
            ..Default::default()
        };

        let stats = run(config).unwrap();

        // Should hit the limit
        assert!(stats.total_rows_selected <= 5 || !stats.warnings.is_empty());
    }

    #[test]
    fn test_global_table_mode() {
        use super::GlobalTableMode;
        
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

mod config_tests {
    use super::*;

    #[test]
    fn test_parse_yaml_config() {
        let yaml = r#"
default:
  percent: 10

classification:
  global:
    - permissions
  system:
    - migrations
    - cache
  lookup:
    - countries
    - currencies

tables:
  users:
    rows: 500
  posts:
    percent: 5
  sessions:
    skip: true
"#;

        let config: SampleYamlConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.default.percent, Some(10));
        assert!(config
            .classification
            .global
            .contains(&"permissions".to_string()));
        assert!(config
            .classification
            .system
            .contains(&"migrations".to_string()));

        let users = config.get_table_config("users").unwrap();
        assert_eq!(users.rows, Some(500));

        assert!(config.should_skip("sessions"));
        assert!(!config.should_skip("users"));

        assert_eq!(config.get_percent("posts"), Some(5));
        assert_eq!(config.get_percent("unknown"), Some(10)); // Falls back to default
    }

    #[test]
    fn test_classification() {
        let yaml = r#"
classification:
  system:
    - migrations
  lookup:
    - currencies
"#;

        let config: SampleYamlConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            config.get_classification("migrations"),
            TableClassification::System
        );
        assert_eq!(
            config.get_classification("currencies"),
            TableClassification::Lookup
        );
        assert_eq!(
            config.get_classification("users"),
            TableClassification::Normal
        );
    }

    #[test]
    fn test_default_classifier() {
        assert_eq!(
            DefaultClassifier::classify("migrations"),
            TableClassification::System
        );
        assert_eq!(
            DefaultClassifier::classify("failed_jobs"),
            TableClassification::System
        );
        assert_eq!(
            DefaultClassifier::classify("countries"),
            TableClassification::Lookup
        );
        assert_eq!(
            DefaultClassifier::classify("users"),
            TableClassification::Normal
        );
    }

    #[test]
    fn test_global_table_mode_parse() {
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

mod reservoir_tests {
    use super::*;

    #[test]
    fn test_reservoir_underfilled() {
        let rng = StdRng::seed_from_u64(42);
        let mut reservoir: Reservoir<i32> = Reservoir::new(10, rng);

        for i in 0..5 {
            reservoir.consider(i);
        }

        assert_eq!(reservoir.len(), 5);
        assert_eq!(reservoir.total_seen(), 5);

        let items = reservoir.into_items();
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_reservoir_overfilled() {
        let rng = StdRng::seed_from_u64(42);
        let mut reservoir: Reservoir<i32> = Reservoir::new(5, rng);

        for i in 0..100 {
            reservoir.consider(i);
        }

        assert_eq!(reservoir.len(), 5);
        assert_eq!(reservoir.total_seen(), 100);

        let items = reservoir.into_items();
        assert_eq!(items.len(), 5);
    }

    #[test]
    fn test_reservoir_deterministic_with_seed() {
        let rng1 = StdRng::seed_from_u64(42);
        let rng2 = StdRng::seed_from_u64(42);

        let mut reservoir1: Reservoir<i32> = Reservoir::new(5, rng1);
        let mut reservoir2: Reservoir<i32> = Reservoir::new(5, rng2);

        for i in 0..100 {
            reservoir1.consider(i);
            reservoir2.consider(i);
        }

        let items1 = reservoir1.into_items();
        let items2 = reservoir2.into_items();

        assert_eq!(items1, items2);
    }

    #[test]
    fn test_reservoir_uniform_distribution() {
        // Statistical test: with many runs, each item should appear roughly equally
        let trials = 10000;
        let capacity = 10;
        let stream_size = 100;
        let mut counts = vec![0usize; stream_size];

        for seed in 0..trials {
            let rng = StdRng::seed_from_u64(seed);
            let mut reservoir: Reservoir<usize> = Reservoir::new(capacity, rng);

            for i in 0..stream_size {
                reservoir.consider(i);
            }

            for item in reservoir.into_items() {
                counts[item] += 1;
            }
        }

        // Expected count per item: trials * capacity / stream_size = 1000
        let expected = (trials as usize * capacity) / stream_size;
        let tolerance = expected / 5; // 20% tolerance

        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count > expected.saturating_sub(tolerance) && count < expected + tolerance,
                "Item {} count {} is outside expected range {} ± {}",
                i,
                count,
                expected,
                tolerance
            );
        }
    }
}
