//! Configuration types for the redactor.

use crate::parser::SqlDialect;
use crate::redactor::StrategyKind;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Runtime configuration for redaction
#[derive(Debug)]
pub struct RedactConfig {
    /// Input SQL file
    pub input: PathBuf,
    /// Output SQL file (None for stdout)
    pub output: Option<PathBuf>,
    /// SQL dialect
    pub dialect: SqlDialect,
    /// Redaction rules
    pub rules: Vec<Rule>,
    /// Default strategy for unmatched columns
    pub default_strategy: StrategyKind,
    /// Random seed for reproducibility
    pub seed: Option<u64>,
    /// Locale for fake data
    pub locale: String,
    /// Tables to include (None = all)
    pub tables_filter: Option<Vec<String>>,
    /// Tables to exclude
    pub exclude: Vec<String>,
    /// Fail on warnings
    pub strict: bool,
    /// Show progress
    pub progress: bool,
    /// Dry run mode
    pub dry_run: bool,
}

impl RedactConfig {
    /// Create a new builder
    pub fn builder() -> RedactConfigBuilder {
        RedactConfigBuilder::default()
    }

    /// Validate the configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        // Check that input file exists
        if !self.input.exists() {
            anyhow::bail!("Input file not found: {:?}", self.input);
        }

        // Validate locale
        if !is_valid_locale(&self.locale) && self.strict {
            anyhow::bail!(
                "Unsupported locale: {}. Use --locale with a supported value.",
                self.locale
            );
        }

        // Validate rules
        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }
}

/// Check if a locale is valid
fn is_valid_locale(locale: &str) -> bool {
    matches!(
        locale.to_lowercase().as_str(),
        "en" | "en_us" | "de_de" | "fr_fr" | "zh_cn" | "zh_tw" | "ja_jp" | "pt_br" | "ar_sa"
    )
}

/// Builder for RedactConfig
#[derive(Default)]
pub struct RedactConfigBuilder {
    input: Option<PathBuf>,
    output: Option<PathBuf>,
    dialect: Option<SqlDialect>,
    config_file: Option<PathBuf>,
    null_patterns: Vec<String>,
    hash_patterns: Vec<String>,
    fake_patterns: Vec<String>,
    mask_patterns: Vec<String>,
    constant_patterns: Vec<String>,
    seed: Option<u64>,
    locale: String,
    tables_filter: Option<Vec<String>>,
    exclude: Vec<String>,
    strict: bool,
    progress: bool,
    dry_run: bool,
}

impl RedactConfigBuilder {
    pub fn input(mut self, path: PathBuf) -> Self {
        self.input = Some(path);
        self
    }

    pub fn output(mut self, path: Option<PathBuf>) -> Self {
        self.output = path;
        self
    }

    pub fn dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = Some(dialect);
        self
    }

    pub fn config_file(mut self, path: Option<PathBuf>) -> Self {
        self.config_file = path;
        self
    }

    pub fn null_patterns(mut self, patterns: Vec<String>) -> Self {
        self.null_patterns = patterns;
        self
    }

    pub fn hash_patterns(mut self, patterns: Vec<String>) -> Self {
        self.hash_patterns = patterns;
        self
    }

    pub fn fake_patterns(mut self, patterns: Vec<String>) -> Self {
        self.fake_patterns = patterns;
        self
    }

    pub fn mask_patterns(mut self, patterns: Vec<String>) -> Self {
        self.mask_patterns = patterns;
        self
    }

    pub fn constant_patterns(mut self, patterns: Vec<String>) -> Self {
        self.constant_patterns = patterns;
        self
    }

    pub fn seed(mut self, seed: Option<u64>) -> Self {
        self.seed = seed;
        self
    }

    pub fn locale(mut self, locale: String) -> Self {
        self.locale = locale;
        self
    }

    pub fn tables_filter(mut self, tables: Option<Vec<String>>) -> Self {
        self.tables_filter = tables;
        self
    }

    pub fn exclude(mut self, exclude: Vec<String>) -> Self {
        self.exclude = exclude;
        self
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn progress(mut self, progress: bool) -> Self {
        self.progress = progress;
        self
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Build the RedactConfig
    pub fn build(self) -> anyhow::Result<RedactConfig> {
        let input = self
            .input
            .ok_or_else(|| anyhow::anyhow!("Input file is required"))?;
        let dialect = self.dialect.unwrap_or(SqlDialect::MySql);
        let locale = if self.locale.is_empty() {
            "en".to_string()
        } else {
            self.locale
        };

        // Load YAML config if specified
        let yaml_config = if let Some(ref path) = self.config_file {
            Some(RedactYamlConfig::load(path)?)
        } else {
            None
        };

        // Build rules from YAML + CLI patterns
        let mut rules = Vec::new();

        // Add rules from YAML config
        if let Some(ref yaml) = yaml_config {
            rules.extend(yaml.rules.clone());
        }

        // Add CLI patterns as rules (CLI takes precedence)
        for pattern in &self.null_patterns {
            rules.push(Rule {
                column: pattern.clone(),
                strategy: StrategyKind::Null,
            });
        }

        for pattern in &self.hash_patterns {
            rules.push(Rule {
                column: pattern.clone(),
                strategy: StrategyKind::Hash {
                    preserve_domain: false,
                },
            });
        }

        for pattern in &self.fake_patterns {
            rules.push(Rule {
                column: pattern.clone(),
                strategy: StrategyKind::Fake {
                    generator: "name".to_string(),
                },
            });
        }

        for pattern in &self.mask_patterns {
            // Parse "pattern=column" format
            if let Some((mask_pattern, column)) = pattern.split_once('=') {
                rules.push(Rule {
                    column: column.to_string(),
                    strategy: StrategyKind::Mask {
                        pattern: mask_pattern.to_string(),
                    },
                });
            }
        }

        for pattern in &self.constant_patterns {
            // Parse "column=value" format
            if let Some((column, value)) = pattern.split_once('=') {
                rules.push(Rule {
                    column: column.to_string(),
                    strategy: StrategyKind::Constant {
                        value: value.to_string(),
                    },
                });
            }
        }

        // Determine default strategy
        let default_strategy = yaml_config
            .as_ref()
            .and_then(|y| y.defaults.as_ref())
            .map(|d| d.strategy.clone())
            .unwrap_or(StrategyKind::Skip);

        // Merge seed (CLI overrides YAML)
        let seed = self
            .seed
            .or_else(|| yaml_config.as_ref().and_then(|y| y.seed));

        // Merge locale (CLI overrides YAML)
        let locale = if locale != "en" {
            locale
        } else {
            yaml_config
                .as_ref()
                .and_then(|y| y.locale.clone())
                .unwrap_or(locale)
        };

        // Merge skip_tables
        let mut exclude = self.exclude;
        if let Some(ref yaml) = yaml_config {
            if let Some(ref skip) = yaml.skip_tables {
                exclude.extend(skip.iter().cloned());
            }
        }

        Ok(RedactConfig {
            input,
            output: self.output,
            dialect,
            rules,
            default_strategy,
            seed,
            locale,
            tables_filter: self.tables_filter,
            exclude,
            strict: self.strict,
            progress: self.progress,
            dry_run: self.dry_run,
        })
    }
}

/// A redaction rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Column pattern (glob pattern like "*.email" or "users.ssn")
    pub column: String,
    /// Redaction strategy
    #[serde(flatten)]
    pub strategy: StrategyKind,
}

impl Rule {
    /// Validate the rule
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.column.is_empty() {
            anyhow::bail!("Rule column pattern cannot be empty");
        }
        self.strategy.validate()
    }
}

/// YAML configuration file structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactYamlConfig {
    /// Random seed for reproducibility
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,

    /// Locale for fake data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,

    /// Default settings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults: Option<Defaults>,

    /// Redaction rules
    #[serde(default)]
    pub rules: Vec<Rule>,

    /// Tables to skip entirely
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_tables: Option<Vec<String>>,
}

impl RedactYamlConfig {
    /// Load configuration from a YAML file
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to a YAML file
    pub fn save(&self, path: &PathBuf) -> anyhow::Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

/// Default settings in YAML config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Defaults {
    /// Default strategy for columns not matching any rule
    pub strategy: StrategyKind,
}
