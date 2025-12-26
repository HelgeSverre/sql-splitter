//! Column pattern matching for redaction rules.

use crate::redactor::config::{RedactConfig, Rule};
use crate::redactor::StrategyKind;
use crate::schema::TableSchema;
use glob::Pattern;

/// Compiled column matcher for efficient pattern matching
#[derive(Debug)]
pub struct ColumnMatcher {
    /// Compiled rules with glob patterns
    rules: Vec<CompiledRule>,
    /// Default strategy for unmatched columns
    default_strategy: StrategyKind,
}

/// A rule with pre-compiled glob patterns
#[derive(Debug)]
struct CompiledRule {
    /// Table pattern (None = match all tables)
    table_pattern: Option<Pattern>,
    /// Column pattern
    column_pattern: Pattern,
    /// The strategy to apply
    strategy: StrategyKind,
}

impl ColumnMatcher {
    /// Create a new matcher from configuration
    pub fn from_config(config: &RedactConfig) -> anyhow::Result<Self> {
        let mut rules = Vec::with_capacity(config.rules.len());

        for rule in &config.rules {
            let compiled = Self::compile_rule(rule)?;
            rules.push(compiled);
        }

        Ok(Self {
            rules,
            default_strategy: config.default_strategy.clone(),
        })
    }

    /// Compile a rule into table and column patterns
    fn compile_rule(rule: &Rule) -> anyhow::Result<CompiledRule> {
        let pattern = &rule.column;

        // Check if pattern contains a table qualifier (table.column)
        let (table_pattern, column_pattern) = if let Some(dot_pos) = pattern.find('.') {
            let table_part = &pattern[..dot_pos];
            let column_part = &pattern[dot_pos + 1..];

            // Compile table pattern (might be * for all tables)
            let table_pat = if table_part == "*" {
                None
            } else {
                Some(Pattern::new(table_part).map_err(|e| {
                    anyhow::anyhow!("Invalid table pattern '{}': {}", table_part, e)
                })?)
            };

            let col_pat = Pattern::new(column_part)
                .map_err(|e| anyhow::anyhow!("Invalid column pattern '{}': {}", column_part, e))?;

            (table_pat, col_pat)
        } else {
            // No table qualifier - match all tables
            let col_pat = Pattern::new(pattern)
                .map_err(|e| anyhow::anyhow!("Invalid column pattern '{}': {}", pattern, e))?;
            (None, col_pat)
        };

        Ok(CompiledRule {
            table_pattern,
            column_pattern,
            strategy: rule.strategy.clone(),
        })
    }

    /// Get the strategy for a specific column
    pub fn get_strategy(&self, table_name: &str, column_name: &str) -> StrategyKind {
        // Find first matching rule (rules are processed in order)
        for rule in &self.rules {
            if self.rule_matches(rule, table_name, column_name) {
                return rule.strategy.clone();
            }
        }

        // No match - return default
        self.default_strategy.clone()
    }

    /// Get strategies for all columns in a table
    pub fn get_strategies(&self, table_name: &str, table: &TableSchema) -> Vec<StrategyKind> {
        table
            .columns
            .iter()
            .map(|col| self.get_strategy(table_name, &col.name))
            .collect()
    }

    /// Count how many columns match any redaction rule
    pub fn count_matches(&self, table_name: &str, table: &TableSchema) -> usize {
        table
            .columns
            .iter()
            .filter(|col| {
                let strategy = self.get_strategy(table_name, &col.name);
                !matches!(strategy, StrategyKind::Skip)
            })
            .count()
    }

    /// Check if a rule matches a table/column pair
    fn rule_matches(&self, rule: &CompiledRule, table_name: &str, column_name: &str) -> bool {
        // Check table pattern (if specified)
        if let Some(ref table_pat) = rule.table_pattern {
            if !table_pat.matches(table_name) && !table_pat.matches(&table_name.to_lowercase()) {
                return false;
            }
        }

        // Check column pattern
        rule.column_pattern.matches(column_name)
            || rule.column_pattern.matches(&column_name.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_schema() -> TableSchema {
        use crate::schema::{Column, ColumnId, ColumnType, TableId};

        TableSchema {
            name: "users".to_string(),
            id: TableId(0),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    col_type: ColumnType::Int,
                    ordinal: ColumnId(0),
                    is_primary_key: true,
                    is_nullable: false,
                },
                Column {
                    name: "email".to_string(),
                    col_type: ColumnType::Text,
                    ordinal: ColumnId(1),
                    is_primary_key: false,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    col_type: ColumnType::Text,
                    ordinal: ColumnId(2),
                    is_primary_key: false,
                    is_nullable: true,
                },
                Column {
                    name: "ssn".to_string(),
                    col_type: ColumnType::Text,
                    ordinal: ColumnId(3),
                    is_primary_key: false,
                    is_nullable: true,
                },
            ],
            primary_key: vec![ColumnId(0)],
            foreign_keys: vec![],
            indexes: vec![],
            create_statement: None,
        }
    }

    #[test]
    fn test_wildcard_column_match() {
        let config = RedactConfig {
            input: std::path::PathBuf::new(),
            output: None,
            dialect: crate::parser::SqlDialect::MySql,
            rules: vec![Rule {
                column: "*.email".to_string(),
                strategy: StrategyKind::Hash {
                    preserve_domain: true,
                },
            }],
            default_strategy: StrategyKind::Skip,
            seed: None,
            locale: "en".to_string(),
            tables_filter: None,
            exclude: vec![],
            strict: false,
            progress: false,
            dry_run: false,
        };

        let matcher = ColumnMatcher::from_config(&config).unwrap();
        let schema = create_test_schema();

        let strategies = matcher.get_strategies("users", &schema);

        // id: skip, email: hash, name: skip, ssn: skip
        assert!(matches!(strategies[0], StrategyKind::Skip));
        assert!(matches!(strategies[1], StrategyKind::Hash { .. }));
        assert!(matches!(strategies[2], StrategyKind::Skip));
        assert!(matches!(strategies[3], StrategyKind::Skip));
    }

    #[test]
    fn test_exact_column_match() {
        let config = RedactConfig {
            input: std::path::PathBuf::new(),
            output: None,
            dialect: crate::parser::SqlDialect::MySql,
            rules: vec![Rule {
                column: "users.ssn".to_string(),
                strategy: StrategyKind::Null,
            }],
            default_strategy: StrategyKind::Skip,
            seed: None,
            locale: "en".to_string(),
            tables_filter: None,
            exclude: vec![],
            strict: false,
            progress: false,
            dry_run: false,
        };

        let matcher = ColumnMatcher::from_config(&config).unwrap();

        // Should match users.ssn
        let strategy = matcher.get_strategy("users", "ssn");
        assert!(matches!(strategy, StrategyKind::Null));

        // Should NOT match other_table.ssn
        let strategy = matcher.get_strategy("other_table", "ssn");
        assert!(matches!(strategy, StrategyKind::Skip));
    }

    #[test]
    fn test_rule_priority() {
        let config = RedactConfig {
            input: std::path::PathBuf::new(),
            output: None,
            dialect: crate::parser::SqlDialect::MySql,
            rules: vec![
                // More specific rule first
                Rule {
                    column: "admins.email".to_string(),
                    strategy: StrategyKind::Skip,
                },
                // General rule second
                Rule {
                    column: "*.email".to_string(),
                    strategy: StrategyKind::Hash {
                        preserve_domain: false,
                    },
                },
            ],
            default_strategy: StrategyKind::Skip,
            seed: None,
            locale: "en".to_string(),
            tables_filter: None,
            exclude: vec![],
            strict: false,
            progress: false,
            dry_run: false,
        };

        let matcher = ColumnMatcher::from_config(&config).unwrap();

        // admins.email should skip (first rule)
        let strategy = matcher.get_strategy("admins", "email");
        assert!(matches!(strategy, StrategyKind::Skip));

        // users.email should hash (second rule)
        let strategy = matcher.get_strategy("users", "email");
        assert!(matches!(strategy, StrategyKind::Hash { .. }));
    }

    #[test]
    fn test_count_matches() {
        let config = RedactConfig {
            input: std::path::PathBuf::new(),
            output: None,
            dialect: crate::parser::SqlDialect::MySql,
            rules: vec![
                Rule {
                    column: "*.email".to_string(),
                    strategy: StrategyKind::Hash {
                        preserve_domain: false,
                    },
                },
                Rule {
                    column: "*.ssn".to_string(),
                    strategy: StrategyKind::Null,
                },
            ],
            default_strategy: StrategyKind::Skip,
            seed: None,
            locale: "en".to_string(),
            tables_filter: None,
            exclude: vec![],
            strict: false,
            progress: false,
            dry_run: false,
        };

        let matcher = ColumnMatcher::from_config(&config).unwrap();
        let schema = create_test_schema();

        // Should match email and ssn (2 columns)
        assert_eq!(matcher.count_matches("users", &schema), 2);
    }
}
