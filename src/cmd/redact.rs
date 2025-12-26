//! CLI handler for the redact command.

use crate::parser::SqlDialect;
use crate::redactor::{RedactConfig, RedactStats, Redactor};
use std::path::PathBuf;

/// Run the redact command with the given options
#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    dialect: Option<String>,
    config: Option<PathBuf>,
    generate_config: bool,
    null_patterns: Vec<String>,
    hash_patterns: Vec<String>,
    fake_patterns: Vec<String>,
    mask_patterns: Vec<String>,
    constant_patterns: Vec<String>,
    seed: Option<u64>,
    locale: String,
    tables: Vec<String>,
    exclude: Vec<String>,
    strict: bool,
    progress: bool,
    dry_run: bool,
    json: bool,
    validate_only: bool,
) -> anyhow::Result<()> {
    // Determine dialect
    let dialect = if let Some(d) = dialect {
        match d.to_lowercase().as_str() {
            "mysql" | "mariadb" => SqlDialect::MySql,
            "postgres" | "postgresql" => SqlDialect::Postgres,
            "sqlite" => SqlDialect::Sqlite,
            "mssql" | "sqlserver" | "sql_server" | "tsql" => SqlDialect::Mssql,
            _ => anyhow::bail!("Unknown dialect: {}. Use: mysql, postgres, sqlite, mssql", d),
        }
    } else {
        // Auto-detect from file extension or content
        SqlDialect::MySql // Default to MySQL
    };

    // Build config from YAML file and/or CLI options
    let redact_config = RedactConfig::builder()
        .input(file)
        .output(output)
        .dialect(dialect)
        .config_file(config)
        .null_patterns(null_patterns)
        .hash_patterns(hash_patterns)
        .fake_patterns(fake_patterns)
        .mask_patterns(mask_patterns)
        .constant_patterns(constant_patterns)
        .seed(seed)
        .locale(locale)
        .tables_filter(if tables.is_empty() {
            None
        } else {
            Some(tables)
        })
        .exclude(exclude)
        .strict(strict)
        .progress(progress)
        .dry_run(dry_run)
        .build()?;

    // Handle special modes
    if validate_only {
        redact_config.validate()?;
        if !json {
            eprintln!("Configuration is valid");
        } else {
            println!(r#"{{"valid": true}}"#);
        }
        return Ok(());
    }

    if generate_config {
        return crate::redactor::generate_config(&redact_config);
    }

    // Run redaction
    let mut redactor = Redactor::new(redact_config)?;
    let stats = redactor.run()?;

    // Output results
    output_stats(&stats, json);

    Ok(())
}

fn output_stats(stats: &RedactStats, json: bool) {
    if json {
        println!("{}", serde_json::to_string_pretty(stats).unwrap());
    } else {
        println!("\nRedaction complete:");
        println!("  Tables processed: {}", stats.tables_processed);
        println!("  Rows redacted: {}", stats.rows_redacted);
        println!("  Columns redacted: {}", stats.columns_redacted);

        if !stats.warnings.is_empty() {
            eprintln!("\nWarnings:");
            for warning in &stats.warnings {
                eprintln!("  - {}", warning);
            }
        }
    }
}
