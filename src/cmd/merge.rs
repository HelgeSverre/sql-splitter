use super::common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, OUTPUT_FORMAT};
use crate::merger::Merger;
use crate::parser::SqlDialect;
use clap::{Args, ValueHint};
use schemars::JsonSchema;
use serde::Serialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Args)]
pub struct MergeArgs {
    /// Directory containing split SQL files
    #[arg(value_hint = ValueHint::DirPath, help_heading = INPUT_OUTPUT)]
    input_dir: PathBuf,

    /// Output SQL file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect for output formatting
    #[arg(short, long, default_value = "mysql", help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Only merge specific tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    tables: Option<String>,

    /// Exclude specific tables (comma-separated)
    #[arg(short, long, help_heading = FILTERING)]
    exclude: Option<String>,

    /// Wrap output in BEGIN/COMMIT transaction
    #[arg(long, help_heading = BEHAVIOR)]
    transaction: bool,

    /// Omit header comments
    #[arg(long, help_heading = BEHAVIOR)]
    no_header: bool,

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

/// JSON output for merge command
#[derive(Serialize, JsonSchema)]
pub(crate) struct MergeJsonOutput {
    input_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    dialect: String,
    dry_run: bool,
    statistics: MergeStatistics,
    tables: Vec<String>,
    options: MergeOptions,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct MergeStatistics {
    tables_merged: usize,
    bytes_written: u64,
    elapsed_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    throughput_kb_per_sec: Option<f64>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct MergeOptions {
    transaction: bool,
    header: bool,
}

pub fn run(args: MergeArgs) -> anyhow::Result<()> {
    let MergeArgs {
        input_dir,
        output,
        dialect,
        tables,
        exclude,
        transaction,
        no_header,
        progress,
        json,
        dry_run,
    } = args;
    let output = super::common::dash_is_stdout(output);

    // Validate input directory
    if !input_dir.exists() {
        anyhow::bail!("input directory does not exist: {}", input_dir.display());
    }
    if !input_dir.is_dir() {
        anyhow::bail!("input path is not a directory: {}", input_dir.display());
    }

    // Parse dialect
    let dialect: SqlDialect = dialect
        .map(|d| d.parse())
        .transpose()
        .map_err(|e: String| anyhow::anyhow!(e))?
        .unwrap_or_default();

    // Parse table filters
    let tables_filter: Option<HashSet<String>> =
        tables.map(|t| t.split(',').map(|s| s.trim().to_lowercase()).collect());

    let exclude_set: HashSet<String> = exclude
        .map(|e| e.split(',').map(|s| s.trim().to_lowercase()).collect())
        .unwrap_or_default();

    let mut merger = Merger::new(input_dir.clone(), output.clone())
        .with_dialect(dialect)
        .with_exclude(exclude_set)
        .with_transaction(transaction)
        .with_header(!no_header);
    if let Some(tables) = tables_filter {
        merger = merger.with_tables(tables);
    }
    let sorted_files = merger.plan()?;

    if !json {
        if let Some(ref out) = output {
            println!(
                "Merging {} tables from: {}",
                sorted_files.len(),
                input_dir.display()
            );
            println!("Output: {}", out.display());
            println!();
        } else {
            // SQL goes to stdout — keep status lines out of the piped output
            eprintln!(
                "Merging {} tables from: {}",
                sorted_files.len(),
                input_dir.display()
            );
            eprintln!("Output: stdout");
            eprintln!();
        }
    }

    if dry_run {
        if json {
            let output_json = MergeJsonOutput {
                input_dir: input_dir.display().to_string(),
                output_file: output.as_ref().map(|p| p.display().to_string()),
                dialect: dialect.to_string(),
                dry_run: true,
                statistics: MergeStatistics {
                    tables_merged: sorted_files.len(),
                    bytes_written: 0,
                    elapsed_secs: 0.0,
                    throughput_kb_per_sec: None,
                },
                tables: sorted_files.iter().map(|(name, _)| name.clone()).collect(),
                options: MergeOptions {
                    transaction,
                    header: !no_header,
                },
            };
            println!("{}", serde_json::to_string_pretty(&output_json)?);
        } else {
            println!("Tables to merge:");
            for (name, _) in &sorted_files {
                println!("  - {}", name);
            }
            println!("\nDry run complete. No files written.");
        }
        return Ok(());
    }

    let start_time = Instant::now();

    if progress && !json {
        let pb = super::common::byte_progress_bar(0);
        merger = merger.with_progress(Box::new(move |done, total| {
            pb.set_length(total);
            pb.set_position(done);
            if done >= total {
                pb.finish_with_message("done");
            }
        }));
    }

    let stats = merger.merge()?;

    let elapsed = start_time.elapsed();

    if json {
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            Some(stats.bytes_written as f64 / 1024.0 / elapsed.as_secs_f64())
        } else {
            None
        };

        let output_json = MergeJsonOutput {
            input_dir: input_dir.display().to_string(),
            output_file: output.as_ref().map(|p| p.display().to_string()),
            dialect: dialect.to_string(),
            dry_run: false,
            statistics: MergeStatistics {
                tables_merged: stats.tables_merged,
                bytes_written: stats.bytes_written,
                elapsed_secs: elapsed.as_secs_f64(),
                throughput_kb_per_sec: throughput,
            },
            tables: stats.table_names.clone(),
            options: MergeOptions {
                transaction,
                header: !no_header,
            },
        };
        // Only print JSON to stdout if we're not also writing merge output to stdout
        if output.is_some() {
            println!("{}", serde_json::to_string_pretty(&output_json)?);
        } else {
            // Can't output JSON when merge output goes to stdout
            // The JSON would be mixed with SQL output
            eprintln!("{}", serde_json::to_string_pretty(&output_json)?);
        }
    } else if output.is_some() {
        println!("\n✓ Merge completed successfully!");
        println!("\nStatistics:");
        println!("  Tables merged: {}", stats.tables_merged);
        println!(
            "  Bytes written: {:.2} KB",
            stats.bytes_written as f64 / 1024.0
        );
        println!("  Elapsed time: {:.3?}", elapsed);

        if elapsed.as_secs_f64() > 0.0 {
            let throughput = stats.bytes_written as f64 / 1024.0 / elapsed.as_secs_f64();
            println!("  Throughput: {:.2} KB/s", throughput);
        }
    }

    Ok(())
}
