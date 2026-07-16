//! CLI handler for the redact command.

use super::common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, MODE, OUTPUT_FORMAT};
use crate::redactor::{RedactConfig, RedactStats, Redactor};
use clap::{Args, ValueHint};
use std::path::PathBuf;

#[derive(Args)]
pub struct RedactArgs {
    /// Input SQL file (supports .gz, .bz2, .xz, .zst)
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// YAML config file for redaction rules
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    config: Option<PathBuf>,

    /// Generate annotated YAML config by analyzing input file
    #[arg(long, help_heading = MODE)]
    generate_config: bool,

    /// Columns to set to NULL (glob patterns, comma-separated)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    null: Vec<String>,

    /// Columns to hash with SHA256 (glob patterns)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    hash: Vec<String>,

    /// Columns to replace with fake data (glob patterns)
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    fake: Vec<String>,

    /// Columns to mask (format: pattern=column, e.g., "****-XXXX=*.credit_card")
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    mask: Vec<String>,

    /// Column=value pairs for constant replacement
    #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
    constant: Vec<String>,

    /// Random seed for reproducible redaction
    #[arg(long, help_heading = MODE)]
    seed: Option<u64>,

    /// Locale for fake data generation (default: en)
    #[arg(long, default_value = "en", help_heading = MODE)]
    locale: String,

    /// Only redact specific tables (comma-separated)
    #[arg(short, long, value_delimiter = ',', help_heading = FILTERING)]
    tables: Vec<String>,

    /// Exclude specific tables (comma-separated)
    #[arg(short = 'x', long, value_delimiter = ',', help_heading = FILTERING)]
    exclude: Vec<String>,

    /// Fail on warnings (e.g., unsupported locale)
    #[arg(long, help_heading = BEHAVIOR)]
    strict: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Preview without writing files
    #[arg(long, help_heading = BEHAVIOR)]
    dry_run: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,

    /// Validate config only, don't process
    #[arg(long, help_heading = BEHAVIOR)]
    validate: bool,
}

/// Run the redact command with the given options
pub fn run(args: RedactArgs) -> anyhow::Result<()> {
    let RedactArgs {
        file,
        output,
        dialect,
        config,
        generate_config,
        null: null_patterns,
        hash: hash_patterns,
        fake: fake_patterns,
        mask: mask_patterns,
        constant: constant_patterns,
        seed,
        locale,
        tables,
        exclude,
        strict,
        progress,
        dry_run,
        json,
        validate: validate_only,
    } = args;
    let output = super::common::dash_is_stdout(output);

    // Determine dialect. Unlike the old hand-rolled resolver, a detection
    // failure is now reported as an error instead of silently defaulting to
    // MySQL (which could corrupt Postgres/MSSQL dumps by redacting with the
    // wrong quoting rules).
    let dialect = super::common::resolve_dialect(&file, dialect.as_deref(), json)?;

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
