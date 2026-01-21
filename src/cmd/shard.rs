//! Shard command CLI handler.

use crate::parser::SqlDialect;
use crate::shard::{self, GlobalTableMode, ShardConfig, ShardStats, ShardTableClassification};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;

/// JSON output for single-tenant shard command
#[derive(Serialize, JsonSchema)]
pub(crate) struct ShardJsonOutput {
    input_file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    dialect: String,
    dry_run: bool,
    tenant: TenantInfo,
    statistics: ShardStatistics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    tables: Vec<TableShardJson>,
}

/// JSON output for multi-tenant shard command
#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiShardJsonOutput {
    input_file: String,
    output_dir: String,
    dialect: String,
    tenant: MultiTenantInfo,
    statistics: MultiShardStatistics,
    shards: Vec<ShardResult>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct TenantInfo {
    column: String,
    value: String,
    auto_detected: bool,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiTenantInfo {
    column: String,
    values: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct ShardStatistics {
    tables_processed: usize,
    tables_with_data: usize,
    tables_skipped: usize,
    rows_selected: u64,
    rows_total: u64,
    reduction_percent: f64,
    fk_orphans_skipped: u64,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiShardStatistics {
    tenants_extracted: usize,
    total_rows_selected: u64,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct ShardResult {
    tenant_value: String,
    output_file: String,
    rows_selected: u64,
    tables_with_data: usize,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct TableShardJson {
    name: String,
    classification: String,
    rows_selected: u64,
    rows_total: u64,
}

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
    json: bool,
) -> anyhow::Result<()> {
    // Validate that at least one tenant value is provided
    if tenant_value.is_none() && tenant_values.is_none() {
        anyhow::bail!("Must specify either --tenant-value or --tenant-values");
    }

    // Parse dialect
    let dialect_resolved = if let Some(d) = dialect {
        d.parse::<SqlDialect>()
            .map_err(|e| anyhow::anyhow!("{}", e))?
    } else {
        let result = crate::parser::detect_dialect_from_file(&file)?;
        if progress && !json {
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

        if !json && progress {
            eprintln!(
                "Extracting {} tenants to {}/",
                tenant_list.len(),
                output_dir.display()
            );
        }

        let mut total_rows_selected = 0u64;
        let mut shard_results: Vec<ShardResult> = Vec::new();
        let mut detected_column: Option<String> = None;

        for tenant_val in &tenant_list {
            let output_file = output_dir.join(format!("tenant_{}.sql", tenant_val));

            if !json && progress {
                eprintln!("\nProcessing tenant: {}", tenant_val);
            }

            let shard_config = ShardConfig {
                input: file.clone(),
                output: Some(output_file.clone()),
                dialect: dialect_resolved,
                tenant_column: tenant_column.clone(),
                tenant_value: tenant_val.clone(),
                root_tables: root_tables_list.clone(),
                include_global: include_global_mode,
                dry_run,
                progress: false,
                config_file: config.clone(),
                max_selected_rows,
                strict_fk,
                include_schema: !no_schema,
            };

            let stats = shard::run(shard_config)?;

            if !json && (progress || dry_run) {
                eprintln!(
                    "  Tenant {}: {} rows selected",
                    tenant_val, stats.total_rows_selected
                );
            }

            if detected_column.is_none() {
                detected_column = stats.detected_tenant_column.clone();
            }

            total_rows_selected += stats.total_rows_selected;
            shard_results.push(ShardResult {
                tenant_value: tenant_val.clone(),
                output_file: output_file.display().to_string(),
                rows_selected: stats.total_rows_selected,
                tables_with_data: stats.tables_with_data,
            });
        }

        if json {
            let output_json = MultiShardJsonOutput {
                input_file: file.display().to_string(),
                output_dir: output_dir.display().to_string(),
                dialect: dialect_resolved.to_string(),
                tenant: MultiTenantInfo {
                    column: detected_column
                        .unwrap_or_else(|| tenant_column.clone().unwrap_or_default()),
                    values: tenant_list,
                },
                statistics: MultiShardStatistics {
                    tenants_extracted: shard_results.len(),
                    total_rows_selected,
                },
                shards: shard_results,
            };
            println!("{}", serde_json::to_string_pretty(&output_json)?);
        } else if progress || dry_run {
            eprintln!();
            eprintln!("Multi-Tenant Summary:");
            eprintln!("  Tenants extracted: {}", shard_results.len());
            eprintln!("  Total rows selected: {}", total_rows_selected);
            eprintln!("  Output directory: {}", output_dir.display());
        }

        return Ok(());
    }

    // Single tenant mode
    let tenant_val = tenant_value.unwrap();

    let shard_config = ShardConfig {
        input: file.clone(),
        output: output.clone(),
        dialect: dialect_resolved,
        tenant_column: tenant_column.clone(),
        tenant_value: tenant_val.clone(),
        root_tables: root_tables_list,
        include_global: include_global_mode,
        dry_run,
        progress: progress && !json,
        config_file: config,
        max_selected_rows,
        strict_fk,
        include_schema: !no_schema,
    };

    // Run sharding
    let stats = shard::run(shard_config)?;

    if json {
        print_json(
            &file,
            &output,
            dialect_resolved,
            &tenant_val,
            &tenant_column,
            dry_run,
            &stats,
        )?;
    } else {
        print_stats(&stats, dry_run, progress);
    }

    Ok(())
}

fn print_json(
    file: &std::path::Path,
    output: &Option<PathBuf>,
    dialect: SqlDialect,
    tenant_value: &str,
    tenant_column: &Option<String>,
    dry_run: bool,
    stats: &ShardStats,
) -> anyhow::Result<()> {
    let reduction = if stats.total_rows_seen > 0 {
        100.0 - (stats.total_rows_selected as f64 / stats.total_rows_seen as f64) * 100.0
    } else {
        0.0
    };

    let tables: Vec<TableShardJson> = stats
        .table_stats
        .iter()
        .map(|t| TableShardJson {
            name: t.name.clone(),
            classification: format!("{:?}", t.classification).to_lowercase(),
            rows_selected: t.rows_selected,
            rows_total: t.rows_seen,
        })
        .collect();

    let detected_column = stats.detected_tenant_column.clone();
    let column = detected_column.unwrap_or_else(|| tenant_column.clone().unwrap_or_default());

    let output_json = ShardJsonOutput {
        input_file: file.display().to_string(),
        output_file: output.as_ref().map(|p| p.display().to_string()),
        dialect: dialect.to_string(),
        dry_run,
        tenant: TenantInfo {
            column: column.clone(),
            value: tenant_value.to_string(),
            auto_detected: tenant_column.is_none(),
        },
        statistics: ShardStatistics {
            tables_processed: stats.tables_processed,
            tables_with_data: stats.tables_with_data,
            tables_skipped: stats.tables_skipped,
            rows_selected: stats.total_rows_selected,
            rows_total: stats.total_rows_seen,
            reduction_percent: reduction,
            fk_orphans_skipped: stats.fk_orphans_skipped,
        },
        warnings: stats.warnings.clone(),
        tables,
    };

    println!("{}", serde_json::to_string_pretty(&output_json)?);
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
