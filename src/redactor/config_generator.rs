//! Config generator for auto-detecting PII columns.
//!
//! Analyzes the input file schema and suggests redaction strategies
//! based on column names and types.

use crate::parser::{Parser, SqlDialect, StatementType};
use crate::redactor::config::RedactConfig;
use crate::redactor::StrategyKind;
use crate::schema::SchemaBuilder;
use std::fs::File;
use std::io::Write;

/// Column analysis result for config generation
#[derive(Debug)]
pub struct ColumnAnalysis {
    pub table: String,
    pub column: String,
    pub column_type: String,
    pub suggested_strategy: Option<StrategyKind>,
    pub confidence: Confidence,
}

/// Confidence level for PII detection
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Confidence {
    High,
    Medium,
    Low,
    None,
}

impl Confidence {
    fn as_comment(&self) -> &'static str {
        match self {
            Confidence::High => "",
            Confidence::Medium => "  # Medium confidence",
            Confidence::Low => "  # Low confidence - review",
            Confidence::None => "",
        }
    }
}

/// Generate a YAML config file by analyzing the input
pub fn generate_config(config: &RedactConfig) -> anyhow::Result<()> {
    let analyses = analyze_for_config(&config.input, config.dialect)?;

    // Determine output path
    let output_path = config.output.clone().unwrap_or_else(|| {
        let mut path = config.input.clone();
        path.set_extension("redact.yaml");
        path
    });

    generate_config_yaml(&analyses, &output_path)?;

    eprintln!("Generated config: {:?}", output_path);
    eprintln!(
        "Found {} columns with potential PII",
        analyses
            .iter()
            .filter(|a| a.suggested_strategy.is_some())
            .count()
    );

    Ok(())
}

/// Analyze input file for PII columns
fn analyze_for_config(
    input: &std::path::Path,
    dialect: SqlDialect,
) -> anyhow::Result<Vec<ColumnAnalysis>> {
    let file = File::open(input)?;
    let mut parser = Parser::with_dialect(file, 64 * 1024, dialect);
    let mut builder = SchemaBuilder::new();

    // Build schema
    while let Some(stmt) = parser.read_statement()? {
        let (stmt_type, _table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

        if stmt_type == StatementType::CreateTable {
            let stmt_str = String::from_utf8_lossy(&stmt);
            builder.parse_create_table(&stmt_str);
        }
    }

    let schema = builder.build();

    // Analyze each column
    let mut analyses = Vec::new();
    for table in schema.iter() {
        for col in &table.columns {
            let (strategy, confidence) = detect_pii(&col.name, &format!("{:?}", col.col_type));
            analyses.push(ColumnAnalysis {
                table: table.name.clone(),
                column: col.name.clone(),
                column_type: format!("{:?}", col.col_type),
                suggested_strategy: strategy,
                confidence,
            });
        }
    }

    Ok(analyses)
}

/// Detect PII based on column name patterns
fn detect_pii(column_name: &str, _column_type: &str) -> (Option<StrategyKind>, Confidence) {
    let name = column_name.to_lowercase();

    // High confidence patterns
    if name.contains("email") {
        return (
            Some(StrategyKind::Hash {
                preserve_domain: true,
            }),
            Confidence::High,
        );
    }
    if name.contains("password") || name.contains("passwd") {
        return (
            Some(StrategyKind::Constant {
                value: "$2b$10$REDACTED".to_string(),
            }),
            Confidence::High,
        );
    }
    if name.contains("ssn") || name.contains("social_security") {
        return (Some(StrategyKind::Null), Confidence::High);
    }
    if name.contains("tax_id") || name == "tin" {
        return (Some(StrategyKind::Null), Confidence::High);
    }
    if name.contains("credit_card") || name.starts_with("cc_") {
        return (
            Some(StrategyKind::Mask {
                pattern: "****-****-****-XXXX".to_string(),
            }),
            Confidence::High,
        );
    }

    // Medium confidence patterns
    if name.contains("phone") || name.contains("mobile") || name.contains("cell") {
        return (
            Some(StrategyKind::Fake {
                generator: "phone".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name == "first_name" || name == "fname" {
        return (
            Some(StrategyKind::Fake {
                generator: "first_name".to_string(),
            }),
            Confidence::High,
        );
    }
    if name == "last_name" || name == "lname" || name.contains("surname") {
        return (
            Some(StrategyKind::Fake {
                generator: "last_name".to_string(),
            }),
            Confidence::High,
        );
    }
    if (name.contains("name") && !name.contains("username") && name != "name")
        || name == "full_name"
    {
        return (
            Some(StrategyKind::Fake {
                generator: "name".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name.contains("address") || name.contains("street") {
        return (
            Some(StrategyKind::Fake {
                generator: "address".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name == "city" {
        return (
            Some(StrategyKind::Fake {
                generator: "city".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name.contains("zip") || name.contains("postal") {
        return (
            Some(StrategyKind::Fake {
                generator: "zip".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name.contains("ip_address") || name == "ip_addr" || name == "ip" {
        return (
            Some(StrategyKind::Fake {
                generator: "ip".to_string(),
            }),
            Confidence::Medium,
        );
    }
    if name.contains("birth") || name == "dob" || name.contains("date_of_birth") {
        return (
            Some(StrategyKind::Fake {
                generator: "date".to_string(),
            }),
            Confidence::Medium,
        );
    }

    // Low confidence patterns
    if name.contains("company") || name.contains("organization") {
        return (
            Some(StrategyKind::Fake {
                generator: "company".to_string(),
            }),
            Confidence::Low,
        );
    }

    (None, Confidence::None)
}

/// Generate the YAML config file
fn generate_config_yaml(
    analyses: &[ColumnAnalysis],
    output: &std::path::Path,
) -> anyhow::Result<()> {
    let mut yaml = String::new();

    // Header
    yaml.push_str("# sql-splitter redact configuration\n");
    yaml.push_str("# Generated by: sql-splitter redact <input> --generate-config\n");
    yaml.push_str("#\n");
    yaml.push_str("# Review and modify this file before running redaction.\n");
    yaml.push_str("# See: https://github.com/helgesverre/sql-splitter#redact-config\n");
    yaml.push('\n');

    // Seed
    yaml.push_str("# Random seed for reproducible redaction (optional)\n");
    yaml.push_str("# seed: 12345\n\n");

    // Locale
    yaml.push_str("# Locale for fake data generation\n");
    yaml.push_str("# Supported: en, de_de, fr_fr, zh_cn, zh_tw, ja_jp, pt_br, ar_sa\n");
    yaml.push_str("locale: en\n\n");

    // Defaults
    yaml.push_str("# Default strategy for columns not matching any rule\n");
    yaml.push_str("defaults:\n");
    yaml.push_str("  strategy: skip\n\n");

    // Rules - grouped by table
    yaml.push_str("# Redaction rules (processed in order, first match wins)\n");
    yaml.push_str("rules:\n");

    // Group by table
    let mut by_table: std::collections::BTreeMap<&str, Vec<&ColumnAnalysis>> =
        std::collections::BTreeMap::new();
    for analysis in analyses {
        by_table.entry(&analysis.table).or_default().push(analysis);
    }

    for (table, columns) in by_table {
        yaml.push_str(&format!("\n  # --- Table: {} ---\n", table));

        for col in columns {
            if let Some(ref strategy) = col.suggested_strategy {
                let confidence_note = col.confidence.as_comment();

                yaml.push_str(&format!("  - column: \"{}.{}\"\n", table, col.column));

                match strategy {
                    StrategyKind::Null => {
                        yaml.push_str("    strategy: null\n");
                    }
                    StrategyKind::Constant { value } => {
                        yaml.push_str("    strategy: constant\n");
                        yaml.push_str(&format!("    value: \"{}\"\n", value));
                    }
                    StrategyKind::Hash { preserve_domain } => {
                        yaml.push_str("    strategy: hash\n");
                        if *preserve_domain {
                            yaml.push_str("    preserve_domain: true\n");
                        }
                    }
                    StrategyKind::Mask { pattern } => {
                        yaml.push_str("    strategy: mask\n");
                        yaml.push_str(&format!("    pattern: \"{}\"\n", pattern));
                    }
                    StrategyKind::Fake { generator } => {
                        yaml.push_str("    strategy: fake\n");
                        yaml.push_str(&format!("    generator: {}\n", generator));
                    }
                    StrategyKind::Shuffle => {
                        yaml.push_str("    strategy: shuffle\n");
                    }
                    StrategyKind::Skip => {
                        yaml.push_str("    strategy: skip\n");
                    }
                }

                if !confidence_note.is_empty() {
                    yaml.push_str(&format!("   {}\n", confidence_note.trim()));
                }
            } else {
                // Columns without suggestion - comment out
                yaml.push_str(&format!(
                    "  # - column: \"{}.{}\"  # No PII detected\n",
                    table, col.column
                ));
                yaml.push_str("  #   strategy: skip\n");
            }
        }
    }

    // Skip tables
    yaml.push_str("\n# Tables to skip entirely (no redaction applied)\n");
    yaml.push_str("skip_tables:\n");
    yaml.push_str("  # - schema_migrations\n");
    yaml.push_str("  # - ar_internal_metadata\n");

    // Write file
    let mut file = File::create(output)?;
    file.write_all(yaml.as_bytes())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_email() {
        let (strategy, confidence) = detect_pii("email", "Text");
        assert!(matches!(strategy, Some(StrategyKind::Hash { .. })));
        assert_eq!(confidence, Confidence::High);

        let (strategy, _) = detect_pii("user_email", "Text");
        assert!(matches!(strategy, Some(StrategyKind::Hash { .. })));
    }

    #[test]
    fn test_detect_password() {
        let (strategy, confidence) = detect_pii("password", "Text");
        assert!(matches!(strategy, Some(StrategyKind::Constant { .. })));
        assert_eq!(confidence, Confidence::High);
    }

    #[test]
    fn test_detect_ssn() {
        let (strategy, confidence) = detect_pii("ssn", "Text");
        assert!(matches!(strategy, Some(StrategyKind::Null)));
        assert_eq!(confidence, Confidence::High);
    }

    #[test]
    fn test_detect_phone() {
        let (strategy, confidence) = detect_pii("phone_number", "Text");
        assert!(matches!(strategy, Some(StrategyKind::Fake { .. })));
        assert_eq!(confidence, Confidence::Medium);
    }

    #[test]
    fn test_detect_no_pii() {
        let (strategy, confidence) = detect_pii("id", "Int");
        assert!(strategy.is_none());
        assert_eq!(confidence, Confidence::None);

        let (strategy, _) = detect_pii("created_at", "DateTime");
        assert!(strategy.is_none());
    }
}
