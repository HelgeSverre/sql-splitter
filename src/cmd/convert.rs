//! Convert command CLI handler.

use crate::convert::{self, ConvertConfig, ConvertStats};
use crate::parser::SqlDialect;
use clap::{Args, ValueHint};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;
use std::process::ExitCode;

use super::common::{BEHAVIOR, INPUT_OUTPUT, MODE, OUTPUT_FORMAT};
use super::glob_util::{drive_multi_file, expand_file_pattern, FileOutcome};

#[derive(Args)]
pub struct ConvertArgs {
    /// Input SQL file or glob pattern
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output SQL file or directory (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// Source dialect (auto-detected if omitted)
    #[arg(long, help_heading = MODE)]
    from: Option<String>,

    /// Target dialect (required)
    #[arg(long, help_heading = MODE)]
    to: String,

    /// Fail on unsupported features instead of warning
    #[arg(long, help_heading = BEHAVIOR)]
    strict: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,

    /// Preview without writing files
    #[arg(long, help_heading = BEHAVIOR)]
    dry_run: bool,

    /// Stop on first error (for glob patterns)
    #[arg(long, help_heading = BEHAVIOR)]
    fail_fast: bool,
}

/// JSON output for single file convert
#[derive(Serialize, JsonSchema)]
pub(crate) struct ConvertJsonOutput {
    input_file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    conversion: ConversionInfo,
    dry_run: bool,
    statistics: ConvertStatistics,
    warnings: Vec<crate::convert::ConvertWarning>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct ConversionInfo {
    from: String,
    to: String,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct ConvertStatistics {
    statements_processed: u64,
    statements_converted: u64,
    statements_unchanged: u64,
    statements_skipped: u64,
}

/// JSON output for multi-file convert
#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiConvertJsonOutput {
    total_files: usize,
    succeeded: usize,
    failed: usize,
    conversion: ConversionInfo,
    aggregate_stats: AggregateConvertStats,
    results: Vec<ConvertFileResult>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct AggregateConvertStats {
    statements_processed: u64,
    statements_converted: u64,
    statements_unchanged: u64,
    statements_skipped: u64,
    total_warnings: usize,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct ConvertFileResult {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statements_converted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statements_unchanged: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    warnings_count: Option<usize>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub fn run(args: ConvertArgs) -> anyhow::Result<ExitCode> {
    let ConvertArgs {
        file,
        output,
        from: from_dialect,
        to: to_dialect,
        strict,
        progress,
        json,
        dry_run,
        fail_fast,
    } = args;
    let output = super::common::dash_is_stdout(output);
    let expanded = expand_file_pattern(&file)?;

    if expanded.files.len() == 1 {
        run_single(
            expanded.files.into_iter().next().unwrap(),
            output,
            from_dialect,
            to_dialect,
            strict,
            progress,
            dry_run,
            json,
        )
    } else {
        let output_dir = match output {
            Some(dir) => dir,
            None => {
                anyhow::bail!(
                    "Output directory required when using glob patterns. Use --output <dir>"
                );
            }
        };

        run_multi(
            expanded.files,
            output_dir,
            from_dialect,
            to_dialect,
            strict,
            progress,
            dry_run,
            fail_fast,
            json,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn run_single(
    file: PathBuf,
    output: Option<PathBuf>,
    from_dialect: Option<String>,
    to_dialect: String,
    strict: bool,
    progress: bool,
    dry_run: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let from = if let Some(d) = from_dialect.clone() {
        Some(
            d.parse::<SqlDialect>()
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        )
    } else {
        None
    };

    let to = to_dialect
        .parse::<SqlDialect>()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let config = ConvertConfig {
        input: file.clone(),
        output: output.clone(),
        from_dialect: from,
        to_dialect: to,
        dry_run,
        progress: progress && !json,
        strict,
    };

    let stats = convert::run(config)?;

    if json {
        let from_str = from
            .map(|d| d.to_string())
            .unwrap_or_else(|| "auto".to_string());

        let output_json = ConvertJsonOutput {
            input_file: file.display().to_string(),
            output_file: output.as_ref().map(|p| p.display().to_string()),
            conversion: ConversionInfo {
                from: from_str,
                to: to.to_string(),
            },
            dry_run,
            statistics: ConvertStatistics {
                statements_processed: stats.statements_processed,
                statements_converted: stats.statements_converted,
                statements_unchanged: stats.statements_unchanged,
                statements_skipped: stats.statements_skipped,
            },
            warnings: stats.warnings.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&output_json)?);
    } else {
        print_stats(&stats, dry_run, progress);
    }

    if strict && !stats.warnings.is_empty() {
        anyhow::bail!("Strict mode: {} warnings generated", stats.warnings.len());
    }

    Ok(ExitCode::SUCCESS)
}

#[allow(clippy::too_many_arguments)]
fn run_multi(
    files: Vec<PathBuf>,
    output_dir: PathBuf,
    from_dialect: Option<String>,
    to_dialect: String,
    strict: bool,
    _progress: bool,
    dry_run: bool,
    fail_fast: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let total = files.len();

    let to = to_dialect
        .parse::<SqlDialect>()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let from = if let Some(ref d) = from_dialect {
        Some(
            d.parse::<SqlDialect>()
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        )
    } else {
        None
    };

    if !dry_run {
        std::fs::create_dir_all(&output_dir)?;
    }

    if !json {
        eprintln!("Converting {} files to {}...\n", total, to);
    }

    let mut aggregate = AggregateConvertStats {
        statements_processed: 0,
        statements_converted: 0,
        statements_unchanged: 0,
        statements_skipped: 0,
        total_warnings: 0,
    };

    let failed_entry =
        |file: &std::path::Path, size_mb: Option<f64>, error: &anyhow::Error| ConvertFileResult {
            file: file.display().to_string(),
            output_file: None,
            size_mb,
            statements_converted: None,
            statements_unchanged: None,
            warnings_count: None,
            status: "failed".to_string(),
            error: Some(error.to_string()),
        };

    let run = drive_multi_file(
        &files,
        fail_fast,
        |idx, file| {
            if !json {
                eprintln!("[{}/{}] Converting: {}", idx + 1, total, file.display());
            }

            let file_size = match std::fs::metadata(file) {
                Ok(m) => m.len(),
                Err(e) => {
                    let e = anyhow::Error::from(e);
                    if !json {
                        eprintln!("  Error: {}\n", e);
                    }
                    return FileOutcome::Failure {
                        payload: Some(failed_entry(file, None, &e)),
                        error: e.to_string(),
                    };
                }
            };
            let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

            let output_file = if dry_run {
                None
            } else {
                let file_name = file
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| format!("output_{}.sql", idx));
                Some(output_dir.join(file_name))
            };

            let config = ConvertConfig {
                input: file.to_path_buf(),
                output: output_file.clone(),
                from_dialect: from,
                to_dialect: to,
                dry_run,
                progress: false,
                strict,
            };

            match convert::run(config) {
                Ok(stats) => {
                    if !json {
                        let warning_str = if stats.warnings.is_empty() {
                            String::new()
                        } else {
                            format!(" ({} warnings)", stats.warnings.len())
                        };

                        eprintln!(
                            "  {:.2} MB → {} converted, {} unchanged{}",
                            file_size_mb,
                            stats.statements_converted,
                            stats.statements_unchanged,
                            warning_str
                        );

                        if let Some(ref out) = output_file {
                            eprintln!("  → {}", out.display());
                        }
                        eprintln!();
                    }

                    aggregate.statements_processed += stats.statements_processed;
                    aggregate.statements_converted += stats.statements_converted;
                    aggregate.statements_unchanged += stats.statements_unchanged;
                    aggregate.statements_skipped += stats.statements_skipped;
                    aggregate.total_warnings += stats.warnings.len();

                    let payload = ConvertFileResult {
                        file: file.display().to_string(),
                        output_file: output_file.as_ref().map(|p| p.display().to_string()),
                        size_mb: Some(file_size_mb),
                        statements_converted: Some(stats.statements_converted),
                        statements_unchanged: Some(stats.statements_unchanged),
                        warnings_count: Some(stats.warnings.len()),
                        status: "success".to_string(),
                        error: None,
                    };

                    if strict && !stats.warnings.is_empty() {
                        FileOutcome::Failure {
                            payload: Some(payload),
                            error: format!("{} warnings in strict mode", stats.warnings.len()),
                        }
                    } else {
                        FileOutcome::Success(payload)
                    }
                }
                Err(e) => {
                    if !json {
                        eprintln!("  Error: {}\n", e);
                    }
                    FileOutcome::Failure {
                        payload: Some(failed_entry(file, Some(file_size_mb), &e)),
                        error: e.to_string(),
                    }
                }
            }
        },
        |_| None,
    );

    let has_failures = run.has_failures();
    if json {
        let from_str = from_dialect
            .map(|d| d.to_string())
            .unwrap_or_else(|| "auto".to_string());

        let output_json = MultiConvertJsonOutput {
            total_files: run.total,
            succeeded: run.succeeded,
            failed: run.failed,
            conversion: ConversionInfo {
                from: from_str,
                to: to.to_string(),
            },
            aggregate_stats: aggregate,
            results: run.payloads,
        };
        println!("{}", serde_json::to_string_pretty(&output_json)?);
    } else {
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("Conversion Summary:");
        eprintln!("  Total files: {}", run.total);
        eprintln!("  Succeeded: {}", run.succeeded);
        eprintln!("  Failed: {}", run.failed);

        if has_failures {
            eprintln!();
            eprintln!("Failed files:");
            for (path, error) in &run.errors {
                eprintln!("  - {}: {}", path.display(), error);
            }
        }
    }

    // A batch with failures exits non-zero in both JSON and text mode so
    // scripted pipelines don't have to parse the output to detect it.
    if has_failures {
        Ok(ExitCode::FAILURE)
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

fn print_stats(stats: &ConvertStats, dry_run: bool, progress: bool) {
    if !progress && !dry_run {
        return;
    }

    eprintln!();
    eprintln!("Conversion Statistics:");
    eprintln!("  Statements processed: {}", stats.statements_processed);
    eprintln!("  Statements converted: {}", stats.statements_converted);
    eprintln!("  Statements unchanged: {}", stats.statements_unchanged);
    eprintln!("  Statements skipped: {}", stats.statements_skipped);

    if !stats.warnings.is_empty() {
        crate::convert::print_warnings_summary(&stats.warnings, 100);
    }

    if dry_run {
        eprintln!();
        eprintln!("(Dry run - no output written)");
    }
}
