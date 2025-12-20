//! Shard command CLI handler.

use crate::parser::SqlDialect;
use crate::shard::{self, GlobalTableMode, ShardConfig, ShardTableClassification};
use std::path::PathBuf;

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    dialect: Option<String>,
    tenant_column: Option<String>,
    tenant_value: Option<String>,
    tenant_values: Option<String>,
    root_tables: Option<String>,
    include_global: Option<String>,
    config: Option<PathBuf>,
    max_selected_rows: Option<usize>,
    strict_fk: bool,
    no_schema: bool,
    progress: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    // Validate that at least one tenant value is provided
    if tenant_value.is_none() && tenant_values.is_none() {
        anyhow::bail!("Must specify either --tenant-value or --tenant-values");
    }

    // Parse dialect
    let dialect = if let Some(d) = dialect {
        d.parse::<SqlDialect>()
            .map_err(|e| anyhow::anyhow!("{}", e))?
    } else {
        let result = crate::parser::detect_dialect_from_file(&file)?;
        if progress {
            eprintln!(
                "Auto-detected dialect: {} (confidence: {:?})",
                result.dialect, result.confidence
            );
        }
        result.dialect
    };

    // Parse root tables
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

    // Handle multi-tenant mode
    if let Some(values) = tenant_values {
        let tenant_list: Vec<String> = values
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if tenant_list.is_empty() {
            anyhow::bail!("--tenant-values must contain at least one value");
        }

        // Output must be a directory for multi-tenant mode
        let output_dir = output.unwrap_or_else(|| PathBuf::from("shards"));
        std::fs::create_dir_all(&output_dir)?;

        if progress {
            eprintln!(
                "Extracting {} tenants to {}/",
                tenant_list.len(),
                output_dir.display()
            );
        }

        let mut total_stats = shard::ShardStats::default();

        for tenant_val in &tenant_list {
            let output_file = output_dir.join(format!("tenant_{}.sql", tenant_val));

            if progress {
                eprintln!("\nProcessing tenant: {}", tenant_val);
            }

            let shard_config = ShardConfig {
                input: file.clone(),
                output: Some(output_file.clone()),
                dialect,
                tenant_column: tenant_column.clone(),
                tenant_value: tenant_val.clone(),
                root_tables: root_tables_list.clone(),
                include_global: include_global_mode,
                dry_run,
                progress: false, // Suppress per-tenant progress for cleaner output
                config_file: config.clone(),
                max_selected_rows,
                strict_fk,
                include_schema: !no_schema,
            };

            let stats = shard::run(shard_config)?;

            if progress || dry_run {
                eprintln!(
                    "  Tenant {}: {} rows selected",
                    tenant_val, stats.total_rows_selected
                );
            }

            // Aggregate stats
            total_stats.tables_processed = stats.tables_processed;
            total_stats.tables_skipped = stats.tables_skipped;
            total_stats.total_rows_seen = stats.total_rows_seen;
            total_stats.total_rows_selected += stats.total_rows_selected;
            total_stats.fk_orphans_skipped += stats.fk_orphans_skipped;
            total_stats.detected_tenant_column = stats.detected_tenant_column;
        }

        if progress || dry_run {
            eprintln!();
            eprintln!("Multi-Tenant Summary:");
            eprintln!("  Tenants extracted: {}", tenant_list.len());
            eprintln!("  Total rows selected: {}", total_stats.total_rows_selected);
            eprintln!("  Output directory: {}", output_dir.display());
        }

        return Ok(());
    }

    // Single tenant mode
    let tenant_val = tenant_value.unwrap();

    let shard_config = ShardConfig {
        input: file,
        output,
        dialect,
        tenant_column,
        tenant_value: tenant_val,
        root_tables: root_tables_list,
        include_global: include_global_mode,
        dry_run,
        progress,
        config_file: config,
        max_selected_rows,
        strict_fk,
        include_schema: !no_schema,
    };

    // Run sharding
    let stats = shard::run(shard_config)?;

    // Print results
    print_stats(&stats, dry_run, progress);

    Ok(())
}

fn print_stats(stats: &shard::ShardStats, dry_run: bool, progress: bool) {
    if !progress && !dry_run {
        return;
    }

    eprintln!();
    eprintln!("Shard Statistics:");
    if let Some(ref col) = stats.detected_tenant_column {
        eprintln!("  Tenant column: {}", col);
    }
    eprintln!("  Tables processed: {}", stats.tables_processed);
    eprintln!("  Tables with data: {}", stats.tables_with_data);
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

    if stats.fk_orphans_skipped > 0 {
        eprintln!("  FK orphans skipped: {}", stats.fk_orphans_skipped);
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
                ShardTableClassification::TenantRoot => " [tenant-root]",
                ShardTableClassification::TenantDependent => " [dependent]",
                ShardTableClassification::Junction => " [junction]",
                ShardTableClassification::Lookup => " [lookup]",
                ShardTableClassification::System => " [system]",
                ShardTableClassification::Unknown => " [unknown]",
            };
            eprintln!(
                "  {}{}: {} / {} rows ({:.1}%)",
                table_stat.name, class_str, table_stat.rows_selected, table_stat.rows_seen, pct
            );
        }
    }
}
