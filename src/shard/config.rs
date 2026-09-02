//! YAML configuration for the shard command.
//!
//! Supports tenant column specification, table classification overrides,
//! and system/lookup table patterns.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub use crate::transform_common::GlobalTableMode;

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
        let config: ShardYamlConfig = serde_yaml_ng::from_str(&content)?;
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
        self.get_table_override(table_name).and_then(|o| o.role)
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

    /// Check if a table name matches system table patterns
    pub fn is_system_table(table_name: &str) -> bool {
        crate::transform_common::is_system_table(table_name)
    }

    /// Check if a table name matches lookup table patterns
    pub fn is_lookup_table(table_name: &str) -> bool {
        crate::transform_common::is_lookup_table(table_name)
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
