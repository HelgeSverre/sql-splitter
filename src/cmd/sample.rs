//! Sample command CLI handler.

use crate::parser::SqlDialect;
use crate::sample::{self, GlobalTableMode, SampleConfig, SampleMode, TableClassification};
use std::path::PathBuf;

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    dialect: Option<String>,
    percent: Option<u32>,
    rows: Option<usize>,
    preserve_relations: bool,
    tables: Option<String>,
    exclude: Option<String>,
    root_tables: Option<String>,
    include_global: Option<String>,
    seed: Option<u64>,
    config: Option<PathBuf>,
    max_total_rows: Option<usize>,
    strict_fk: bool,
    no_schema: bool,
    progress: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
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
    let dialect = if let Some(d) = dialect {
        d.parse::<SqlDialect>()
            .map_err(|e| anyhow::anyhow!("{}", e))?
    } else {
        // Auto-detect from file
        let result = crate::parser::detect_dialect_from_file(&file)?;
        if progress {
            eprintln!(
                "Auto-detected dialect: {} (confidence: {:?})",
                result.dialect, result.confidence
            );
        }
        result.dialect
    };

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
        input: file,
        output,
        dialect,
        mode,
        preserve_relations,
        tables_filter,
        exclude: exclude_list,
        root_tables: root_tables_list,
        include_global: include_global_mode,
        seed,
        dry_run,
        progress,
        config_file: config,
        max_total_rows,
        strict_fk,
        include_schema: !no_schema,
    };

    // Run sampling
    let stats = sample::run(sample_config)?;

    // Print results
    if progress || dry_run {
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

    Ok(())
}
