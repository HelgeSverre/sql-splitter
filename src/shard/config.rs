//! YAML configuration for the shard command.
//!
//! Supports tenant column specification, table classification overrides,
//! and system/lookup table patterns.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// How to handle global/lookup tables during sharding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GlobalTableMode {
    /// Exclude global tables from output
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

/// Table classification for sharding behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShardTableClassification {
    /// Table has the tenant column directly
    TenantRoot,
    /// Table is connected to tenant via FK chain
    TenantDependent,
    /// Junction/pivot table (many-to-many, include if any FK matches)
    Junction,
    /// Global/lookup table (include fully or skip based on config)
    Lookup,
    /// System table (skip by default: migrations, jobs, cache)
    System,
    /// Normal table that couldn't be classified
    #[default]
    Unknown,
}

impl std::fmt::Display for ShardTableClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardTableClassification::TenantRoot => write!(f, "tenant-root"),
            ShardTableClassification::TenantDependent => write!(f, "tenant-dependent"),
            ShardTableClassification::Junction => write!(f, "junction"),
            ShardTableClassification::Lookup => write!(f, "lookup"),
            ShardTableClassification::System => write!(f, "system"),
            ShardTableClassification::Unknown => write!(f, "unknown"),
        }
    }
}

/// Per-table configuration override
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TableOverride {
    /// Override classification
    pub role: Option<ShardTableClassification>,
    /// Include this lookup/global table
    pub include: Option<bool>,
    /// Self-referential FK column (e.g., parent_id for hierarchical tables)
    pub self_fk: Option<String>,
    /// Skip this table entirely
    pub skip: bool,
}

/// Tenant configuration section
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TenantConfig {
    /// Column name for tenant identification
    pub column: Option<String>,
    /// Explicit root tables (tables that have the tenant column)
    #[serde(default)]
    pub root_tables: Vec<String>,
}

/// Complete YAML configuration for shard command
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardYamlConfig {
    /// Tenant configuration
    pub tenant: TenantConfig,
    /// Per-table overrides
    #[serde(default)]
    pub tables: HashMap<String, TableOverride>,
    /// Global table handling
    pub include_global: Option<GlobalTableMode>,
}

impl ShardYamlConfig {
    /// Load configuration from a YAML file
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: ShardYamlConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Get override for a specific table
    pub fn get_table_override(&self, table_name: &str) -> Option<&TableOverride> {
        self.tables.get(table_name).or_else(|| {
            let lower = table_name.to_lowercase();
            self.tables
                .iter()
                .find(|(k, _)| k.to_lowercase() == lower)
                .map(|(_, v)| v)
        })
    }

    /// Get classification override for a table
    pub fn get_classification(&self, table_name: &str) -> Option<ShardTableClassification> {
        self.get_table_override(table_name)
            .and_then(|o| o.role)
    }

    /// Check if a table should be skipped
    pub fn should_skip(&self, table_name: &str) -> bool {
        self.get_table_override(table_name)
            .map(|o| o.skip)
            .unwrap_or(false)
    }

    /// Get self-FK column for hierarchical tables (for future self-referential closure)
    #[allow(dead_code)]
    pub fn get_self_fk(&self, table_name: &str) -> Option<&str> {
        self.get_table_override(table_name)
            .and_then(|o| o.self_fk.as_deref())
    }
}

/// Default patterns for table classification when no config file provided
pub struct DefaultShardClassifier;

impl DefaultShardClassifier {
    /// Well-known tenant column names (in priority order)
    pub const TENANT_COLUMNS: &'static [&'static str] = &[
        "company_id",
        "tenant_id",
        "organization_id",
        "org_id",
        "account_id",
        "team_id",
        "workspace_id",
    ];

    /// Well-known system table patterns
    pub const SYSTEM_PATTERNS: &'static [&'static str] = &[
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
    pub const LOOKUP_PATTERNS: &'static [&'static str] = &[
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

    /// Check if a table name matches system table patterns
    pub fn is_system_table(table_name: &str) -> bool {
        let lower = table_name.to_lowercase();
        for pattern in Self::SYSTEM_PATTERNS {
            if lower.starts_with(pattern) || lower == *pattern {
                return true;
            }
        }
        false
    }

    /// Check if a table name matches lookup table patterns
    pub fn is_lookup_table(table_name: &str) -> bool {
        let lower = table_name.to_lowercase();
        for pattern in Self::LOOKUP_PATTERNS {
            if lower == *pattern {
                return true;
            }
        }
        false
    }

    /// Detect junction table by name pattern
    pub fn is_junction_table_by_name(table_name: &str) -> bool {
        let lower = table_name.to_lowercase();
        lower.contains("_has_")
            || lower.ends_with("_pivot")
            || lower.ends_with("_link")
            || lower.ends_with("_map")
    }
}


