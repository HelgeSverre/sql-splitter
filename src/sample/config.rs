//! YAML configuration for the sample command.
//!
//! Supports per-table sampling strategies and table classification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// How to handle global/lookup tables
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GlobalTableMode {
    /// Exclude global tables
    None,
    /// Include lookup tables in full (default)
    #[default]
    Lookups,
    /// Include all global tables in full
    All,
}

impl std::str::FromStr for GlobalTableMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(GlobalTableMode::None),
            "lookups" => Ok(GlobalTableMode::Lookups),
            "all" => Ok(GlobalTableMode::All),
            _ => Err(format!(
                "Unknown global mode: {}. Valid options: none, lookups, all",
                s
            )),
        }
    }
}

impl std::fmt::Display for GlobalTableMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GlobalTableMode::None => write!(f, "none"),
            GlobalTableMode::Lookups => write!(f, "lookups"),
            GlobalTableMode::All => write!(f, "all"),
        }
    }
}

/// Table classification for sampling behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableClassification {
    /// Normal table, sample according to mode
    #[default]
    Normal,
    /// Root table (has no FK dependencies or explicitly specified)
    Root,
    /// Global/lookup table (include fully or skip based on --include-global)
    Lookup,
    /// System table (skip by default: migrations, jobs, cache)
    System,
    /// Junction/pivot table (many-to-many)
    Junction,
}

/// Per-table sampling configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableConfig {
    /// Sample percentage for this table (overrides default)
    pub percent: Option<u32>,
    /// Fixed row count for this table (overrides default)
    pub rows: Option<usize>,
    /// Skip this table entirely
    pub skip: bool,
    /// Override table classification
    pub classification: Option<TableClassification>,
}

/// Default sampling settings
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DefaultConfig {
    /// Default sample percentage
    pub percent: Option<u32>,
    /// Default row count
    pub rows: Option<usize>,
}

/// Table classification lists
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClassificationConfig {
    /// Tables to classify as global (include fully)
    #[serde(default)]
    pub global: Vec<String>,
    /// Tables to classify as system (skip by default)
    #[serde(default)]
    pub system: Vec<String>,
    /// Tables to classify as lookup (include based on --include-global)
    #[serde(default)]
    pub lookup: Vec<String>,
    /// Tables to classify as root (start sampling from these)
    #[serde(default)]
    pub root: Vec<String>,
}

/// Complete YAML configuration for sample command
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SampleYamlConfig {
    /// Default sampling settings
    pub default: DefaultConfig,
    /// Table classification lists
    pub classification: ClassificationConfig,
    /// Per-table settings
    #[serde(default)]
    pub tables: HashMap<String, TableConfig>,
}

impl SampleYamlConfig {
    /// Load configuration from a YAML file
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: SampleYamlConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Get configuration for a specific table
    pub fn get_table_config(&self, table_name: &str) -> Option<&TableConfig> {
        self.tables.get(table_name).or_else(|| {
            // Try case-insensitive match
            let lower = table_name.to_lowercase();
            self.tables
                .iter()
                .find(|(k, _)| k.to_lowercase() == lower)
                .map(|(_, v)| v)
        })
    }

    /// Get classification for a table
    pub fn get_classification(&self, table_name: &str) -> TableClassification {
        // Check per-table override first
        if let Some(config) = self.get_table_config(table_name) {
            if let Some(class) = config.classification {
                return class;
            }
        }

        let lower = table_name.to_lowercase();

        // Check classification lists
        if self
            .classification
            .global
            .iter()
            .any(|t| t.to_lowercase() == lower)
        {
            return TableClassification::Lookup;
        }
        if self
            .classification
            .system
            .iter()
            .any(|t| t.to_lowercase() == lower)
        {
            return TableClassification::System;
        }
        if self
            .classification
            .lookup
            .iter()
            .any(|t| t.to_lowercase() == lower)
        {
            return TableClassification::Lookup;
        }
        if self
            .classification
            .root
            .iter()
            .any(|t| t.to_lowercase() == lower)
        {
            return TableClassification::Root;
        }

        TableClassification::Normal
    }

    /// Check if a table should be skipped
    pub fn should_skip(&self, table_name: &str) -> bool {
        if let Some(config) = self.get_table_config(table_name) {
            return config.skip;
        }
        false
    }

    /// Get sample percent for a table (table-specific or default)
    pub fn get_percent(&self, table_name: &str) -> Option<u32> {
        if let Some(config) = self.get_table_config(table_name) {
            if config.percent.is_some() {
                return config.percent;
            }
        }
        self.default.percent
    }

    /// Get sample rows for a table (table-specific or default)
    pub fn get_rows(&self, table_name: &str) -> Option<usize> {
        if let Some(config) = self.get_table_config(table_name) {
            if config.rows.is_some() {
                return config.rows;
            }
        }
        self.default.rows
    }
}

/// Default patterns for table classification (used when no config file)
pub struct DefaultClassifier;

impl DefaultClassifier {
    /// Well-known system table patterns
    const SYSTEM_PATTERNS: &'static [&'static str] = &[
        "migrations",
        "failed_jobs",
        "job_batches",
        "jobs",
        "cache",
        "cache_locks",
        "sessions",
        "password_reset_tokens",
        "personal_access_tokens",
        "telescope_entries",
        "telescope_entries_tags",
        "telescope_monitoring",
        "pulse_",
        "horizon_",
    ];

    /// Well-known lookup/global table patterns
    const LOOKUP_PATTERNS: &'static [&'static str] = &[
        "countries",
        "states",
        "provinces",
        "cities",
        "currencies",
        "languages",
        "timezones",
        "permissions",
        "roles",
        "settings",
    ];

    /// Classify a table using default patterns
    pub fn classify(table_name: &str) -> TableClassification {
        let lower = table_name.to_lowercase();

        // Check system patterns
        for pattern in Self::SYSTEM_PATTERNS {
            if lower.starts_with(pattern) || lower == *pattern {
                return TableClassification::System;
            }
        }

        // Check lookup patterns
        for pattern in Self::LOOKUP_PATTERNS {
            if lower == *pattern {
                return TableClassification::Lookup;
            }
        }

        TableClassification::Normal
    }
}

#[cfg(test)]
mod tests {
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
