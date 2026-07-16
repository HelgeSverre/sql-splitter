//! Sample command CLI handler.

use super::common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, LIMITS, MODE, OUTPUT_FORMAT};
use crate::parser::SqlDialect;
use crate::sample::{
    self, GlobalTableMode, SampleConfig, SampleMode, SampleStats, TableClassification,
};
use clap::{Args, ValueHint};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Args)]
pub struct SampleArgs {
    /// Input SQL file (supports .gz, .bz2, .xz, .zst)
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output SQL file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// YAML config file for per-table settings
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    config: Option<PathBuf>,

    /// Sample percentage of rows (1-100)
    #[arg(long, conflicts_with = "rows", help_heading = MODE)]
    percent: Option<u32>,

    /// Sample fixed number of rows per table
    #[arg(long, conflicts_with = "percent", help_heading = MODE)]
    rows: Option<usize>,

    /// Random seed for reproducible sampling
    #[arg(long, help_heading = MODE)]
    seed: Option<u64>,

    /// Only sample specific tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    tables: Option<String>,

    /// Exclude specific tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    exclude: Option<String>,

    /// Tables to start sampling from (comma-separated)
    #[arg(long, help_heading = FILTERING)]
    root_tables: Option<String>,

    /// Handle lookup tables: none, lookups, all
    #[arg(long, default_value = "lookups", help_heading = FILTERING)]
    include_global: Option<String>,

    /// Maintain FK integrity by including referenced rows
    #[arg(long, help_heading = BEHAVIOR)]
    preserve_relations: bool,

    /// Fail on FK integrity violations
    #[arg(long, help_heading = BEHAVIOR)]
    strict_fk: bool,

    /// Exclude CREATE TABLE statements from output
    #[arg(long, help_heading = BEHAVIOR)]
    no_schema: bool,

    /// Max total rows to sample (0 = unlimited)
    #[arg(long, help_heading = LIMITS)]
    max_total_rows: Option<usize>,

    /// Disable row limit
    #[arg(long, help_heading = LIMITS)]
    no_limit: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,

    /// Preview without writing files
    #[arg(long, help_heading = BEHAVIOR)]
    dry_run: bool,
}

/// JSON output for sample command
#[derive(Serialize, JsonSchema)]
pub(crate) struct SampleJsonOutput {
    input_file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    dialect: String,
    dry_run: bool,
    mode: SampleModeJson,
    statistics: SampleStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    tables: Vec<TableSampleJson>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct SampleModeJson {
    #[serde(rename = "type")]
    mode_type: String,
    value: u64,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct SampleStatistics {
    tables_sampled: usize,
    tables_skipped: usize,
    rows_selected: u64,
    rows_total: u64,
    sample_rate_percent: f64,
    fk_orphans_rejected: u64,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct TableSampleJson {
    name: String,
    classification: String,
    rows_selected: u64,
    rows_total: u64,
    sample_rate_percent: f64,
}

pub fn run(args: SampleArgs) -> anyhow::Result<()> {
    let SampleArgs {
        file,
        output,
        dialect,
        config,
        percent,
        rows,
        seed,
        tables,
        exclude,
        root_tables,
        include_global,
        preserve_relations,
        strict_fk,
        no_schema,
        max_total_rows,
        no_limit,
        progress,
        json,
        dry_run,
    } = args;
    let output = super::common::dash_is_stdout(output);
    let max_total_rows = if no_limit || max_total_rows == Some(0) {
        None
    } else {
        max_total_rows
    };

    // Validate sampling mode - exactly one must be specified (or from config)
    let mode = match (percent, rows) {
        (Some(p), None) => {
            if p == 0 || p > 100 {
                anyhow::bail!("--percent must be between 1 and 100");
            }
            SampleMode::Percent(p)
        }
        (None, Some(r)) => {
            if r == 0 {
                anyhow::bail!("--rows must be greater than 0");
            }
            SampleMode::Rows(r)
        }
        (Some(_), Some(_)) => {
            anyhow::bail!("Cannot specify both --percent and --rows");
        }
        (None, None) => {
            // Will be determined from config or default
            if config.is_none() {
                anyhow::bail!("Must specify either --percent, --rows, or --config");
            }
            // Placeholder - will be overridden by config
            SampleMode::Percent(10)
        }
    };

    // Parse dialect
    let dialect_resolved = super::common::resolve_dialect(&file, dialect.as_deref(), json)?;

    // Parse table filters
    let tables_filter = tables.map(|t| t.split(',').map(|s| s.trim().to_string()).collect());

    let exclude_list: Vec<String> = exclude
        .map(|e| e.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    let root_tables_list: Vec<String> = root_tables
        .map(|r| r.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Parse include_global mode
    let include_global_mode = if let Some(ref mode) = include_global {
        mode.parse::<GlobalTableMode>()
            .map_err(|e| anyhow::anyhow!("{}", e))?
    } else {
        GlobalTableMode::Lookups
    };

    // Generate random seed if not provided
    let seed = seed.unwrap_or_else(rand::random);

    let sample_config = SampleConfig {
        input: file.clone(),
        output: output.clone(),
        dialect: dialect_resolved,
        mode,
        preserve_relations,
        tables_filter,
        exclude: exclude_list,
        root_tables: root_tables_list,
        include_global: include_global_mode,
        seed,
        dry_run,
        progress: progress && !json,
        config_file: config,
        max_total_rows,
        strict_fk,
        include_schema: !no_schema,
    };

    // Run sampling
    let stats = sample::run(sample_config)?;

    if json {
        print_json(&file, &output, dialect_resolved, mode, dry_run, &stats)?;
    } else {
        print_stats(&stats, dry_run, progress);
    }

    Ok(())
}

fn print_json(
    file: &std::path::Path,
    output: &Option<PathBuf>,
    dialect: SqlDialect,
    mode: SampleMode,
    dry_run: bool,
    stats: &SampleStats,
) -> anyhow::Result<()> {
    let mode_json = match mode {
        SampleMode::Percent(p) => SampleModeJson {
            mode_type: "percent".to_string(),
            value: p as u64,
        },
        SampleMode::Rows(r) => SampleModeJson {
            mode_type: "rows".to_string(),
            value: r as u64,
        },
    };

    let sample_rate = if stats.total_rows_seen > 0 {
        (stats.total_rows_selected as f64 / stats.total_rows_seen as f64) * 100.0
    } else {
        0.0
    };

    let tables: Vec<TableSampleJson> = stats
        .table_stats
        .iter()
        .map(|t| {
            let pct = if t.rows_seen > 0 {
                (t.rows_selected as f64 / t.rows_seen as f64) * 100.0
            } else {
                0.0
            };
            TableSampleJson {
                name: t.name.clone(),
                classification: format!("{:?}", t.classification).to_lowercase(),
                rows_selected: t.rows_selected,
                rows_total: t.rows_seen,
                sample_rate_percent: pct,
            }
        })
        .collect();

    let output_json = SampleJsonOutput {
        input_file: file.display().to_string(),
        output_file: output.as_ref().map(|p| p.display().to_string()),
        dialect: dialect.to_string(),
        dry_run,
        mode: mode_json,
        statistics: SampleStatistics {
            tables_sampled: stats.tables_sampled,
            tables_skipped: stats.tables_skipped,
            rows_selected: stats.total_rows_selected,
            rows_total: stats.total_rows_seen,
            sample_rate_percent: sample_rate,
            fk_orphans_rejected: stats.fk_orphans_rejected,
        },
        warnings: stats.warnings.clone(),
        tables,
    };

    println!("{}", serde_json::to_string_pretty(&output_json)?);
    Ok(())
}

fn print_stats(stats: &SampleStats, dry_run: bool, progress: bool) {
    if !progress && !dry_run {
        return;
    }

    eprintln!();
    eprintln!("Sample Statistics:");
    eprintln!("  Tables sampled: {}", stats.tables_sampled);
    eprintln!("  Tables skipped: {}", stats.tables_skipped);

    let percent = if stats.total_rows_seen > 0 {
        (stats.total_rows_selected as f64 / stats.total_rows_seen as f64) * 100.0
    } else {
        0.0
    };
    eprintln!(
        "  Total rows: {} / {} ({:.1}%)",
        stats.total_rows_selected, stats.total_rows_seen, percent
    );

    if stats.fk_orphans_rejected > 0 {
        eprintln!("  FK orphans rejected: {}", stats.fk_orphans_rejected);
    }

    if !stats.warnings.is_empty() {
        eprintln!();
        for warning in &stats.warnings {
            eprintln!("  Warning: {}", warning);
        }
    }

    if dry_run {
        eprintln!();
        eprintln!("Per-table breakdown:");
        for table_stat in &stats.table_stats {
            let pct = if table_stat.rows_seen > 0 {
                (table_stat.rows_selected as f64 / table_stat.rows_seen as f64) * 100.0
            } else {
                0.0
            };
            let class_str = match table_stat.classification {
                TableClassification::Root => " [root]",
                TableClassification::Lookup => " [lookup]",
                TableClassification::System => " [system]",
                TableClassification::Junction => " [junction]",
                TableClassification::Normal => "",
            };
            eprintln!(
                "  {}{}: {} / {} rows ({:.1}%)",
                table_stat.name, class_str, table_stat.rows_selected, table_stat.rows_seen, pct
            );
        }
    }
}
