use super::common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, LIMITS, MODE, OUTPUT_FORMAT};
use crate::differ::{format_diff, DiffConfig, DiffOutputFormat, Differ};
use clap::{Args, ValueHint};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
pub struct DiffArgs {
    /// Original SQL dump file
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    old_file: PathBuf,

    /// Updated SQL dump file
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    new_file: PathBuf,

    /// Output file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Only compare these tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    tables: Option<String>,

    /// Exclude these tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    exclude: Option<String>,

    /// Ignore columns matching glob patterns (e.g., *.updated_at)
    #[arg(long, help_heading = FILTERING)]
    ignore_columns: Option<String>,

    /// Compare schema only, skip data
    #[arg(long, conflicts_with = "data_only", help_heading = MODE)]
    schema_only: bool,

    /// Compare data only, skip schema
    #[arg(long, conflicts_with = "schema_only", help_heading = MODE)]
    data_only: bool,

    /// Override primary key (format: table:col1+col2,table2:col)
    #[arg(long, help_heading = MODE)]
    primary_key: Option<String>,

    /// Compare tables without PK using all columns as key
    #[arg(long, help_heading = BEHAVIOR)]
    allow_no_pk: bool,

    /// Ignore column order differences in schema
    #[arg(long, help_heading = BEHAVIOR)]
    ignore_order: bool,

    /// Max PK entries per table (limits memory)
    #[arg(long, default_value = "10000000", help_heading = LIMITS)]
    max_pk_entries: usize,

    /// Output format: text, json, sql
    #[arg(short, long, default_value = "text", help_heading = OUTPUT_FORMAT)]
    format: Option<String>,

    /// Show sample PK values for changes
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    verbose: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,
}

fn parse_pk_overrides(s: &str) -> HashMap<String, Vec<String>> {
    s.split(',')
        .filter_map(|pair| {
            let (table, cols) = pair.split_once(':')?;
            let columns: Vec<String> = cols.split('+').map(|c| c.trim().to_string()).collect();
            Some((table.trim().to_lowercase(), columns))
        })
        .collect()
}

pub fn run(args: DiffArgs) -> anyhow::Result<()> {
    let DiffArgs {
        old_file,
        new_file,
        output,
        dialect,
        tables,
        exclude,
        ignore_columns,
        schema_only,
        data_only,
        primary_key,
        allow_no_pk,
        ignore_order,
        max_pk_entries,
        format,
        verbose,
        progress,
    } = args;

    // Validate files exist
    if !old_file.exists() {
        anyhow::bail!("Old file does not exist: {}", old_file.display());
    }
    if !new_file.exists() {
        anyhow::bail!("New file does not exist: {}", new_file.display());
    }

    // Parse format
    let output_format: DiffOutputFormat = format
        .as_deref()
        .map(|f| f.parse().map_err(|e: String| anyhow::anyhow!(e)))
        .transpose()?
        .unwrap_or(DiffOutputFormat::Text);

    let is_json = matches!(output_format, DiffOutputFormat::Json);

    // Resolve dialect
    let resolved_dialect = super::common::resolve_dialect(&old_file, dialect.as_deref(), false)?;

    // Parse table filters
    let tables_filter: Vec<String> = tables
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let exclude_filter: Vec<String> = exclude
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Calculate file sizes for display and progress
    let old_size = std::fs::metadata(&old_file)?.len();
    let new_size = std::fs::metadata(&new_file)?.len();
    let total_bytes = if schema_only || data_only {
        old_size + new_size
    } else {
        (old_size + new_size) * 2
    };

    if !is_json {
        eprintln!("Comparing: {} → {}", old_file.display(), new_file.display());
        eprintln!(
            "  Old: {:.2} MB, New: {:.2} MB",
            old_size as f64 / (1024.0 * 1024.0),
            new_size as f64 / (1024.0 * 1024.0)
        );
        eprintln!("  Dialect: {}", resolved_dialect);
        eprintln!();
    }

    let start_time = Instant::now();

    // Set up progress bar
    let pb = if progress && !is_json {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Comparing...");
        Some(pb)
    } else {
        None
    };

    // Parse PK overrides
    let pk_overrides = primary_key
        .as_deref()
        .map(parse_pk_overrides)
        .unwrap_or_default();

    // Parse ignore columns
    let ignore_columns_vec: Vec<String> = ignore_columns
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_default();

    // Build config
    let config = DiffConfig {
        old_path: old_file,
        new_path: new_file,
        dialect: Some(resolved_dialect),
        schema_only,
        data_only,
        tables: tables_filter,
        exclude: exclude_filter,
        format: output_format,
        verbose,
        progress,
        max_pk_entries,
        allow_no_pk,
        ignore_column_order: ignore_order,
        pk_overrides,
        ignore_columns: ignore_columns_vec.clone(),
    };

    // Run diff
    let mut differ = Differ::new(config.clone());
    if let Some(ref pb) = pb {
        let pb_clone = pb.clone();
        differ = differ.with_progress(move |current, total| {
            pb_clone.set_length(total);
            pb_clone.set_position(current);
        });
    }

    let result = differ.diff()?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }

    let elapsed = start_time.elapsed();

    // Format output
    let output_text = format_diff(&result, output_format, resolved_dialect);

    // Write output
    match output {
        Some(path) => {
            let mut file = std::fs::File::create(&path)?;
            file.write_all(output_text.as_bytes())?;
            if !is_json {
                eprintln!("Output written to: {}", path.display());
            }
        }
        None => {
            print!("{}", output_text);
        }
    }

    if !is_json {
        eprintln!();
        eprintln!("Diff completed in {:.3?}", elapsed);
    }

    Ok(())
}
