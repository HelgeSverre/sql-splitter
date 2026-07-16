use super::common::{BEHAVIOR, INPUT_OUTPUT, OUTPUT_FORMAT};
use crate::analyzer::Analyzer;
use crate::splitter::Compression;
use clap::{Args, ValueHint};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

use super::glob_util::{drive_multi_file, expand_file_pattern, FileOutcome};

/// JSON output for single file analyze
#[derive(Serialize, JsonSchema)]
pub(crate) struct AnalyzeJsonOutput {
    input_file: String,
    dialect: String,
    size_mb: f64,
    elapsed_secs: f64,
    summary: AnalyzeSummary,
    tables: Vec<TableAnalysis>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct AnalyzeSummary {
    total_tables: usize,
    total_inserts: u64,
    total_bytes: u64,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct TableAnalysis {
    name: String,
    inserts: u64,
    creates: u64,
    statements: u64,
    bytes: u64,
    size_mb: f64,
}

/// JSON output for multi-file analyze
#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiAnalyzeJsonOutput {
    total_files: usize,
    succeeded: usize,
    failed: usize,
    elapsed_secs: f64,
    results: Vec<AnalyzeFileResult>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct AnalyzeFileResult {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialect: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_inserts: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables: Option<Vec<TableAnalysis>>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Args)]
pub struct AnalyzeArgs {
    /// Input SQL file or glob pattern
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,

    /// Stop on first error (for glob patterns)
    #[arg(long, help_heading = BEHAVIOR)]
    fail_fast: bool,
}

pub fn run(args: AnalyzeArgs) -> anyhow::Result<ExitCode> {
    let AnalyzeArgs {
        file,
        dialect,
        progress,
        json,
        fail_fast,
    } = args;
    let expanded = expand_file_pattern(&file)?;

    if expanded.files.len() == 1 {
        run_single(
            expanded.files.into_iter().next().unwrap(),
            dialect,
            progress,
            json,
        )
    } else {
        run_multi(expanded.files, dialect, progress, fail_fast, json)
    }
}

fn run_single(
    file: PathBuf,
    dialect: Option<String>,
    progress: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    let compression = Compression::from_path(&file);
    if !json && compression != Compression::None {
        println!("Detected compression: {}", compression);
    }

    let dialect = super::common::resolve_dialect(&file, dialect.as_deref(), json)?;

    if !json {
        println!(
            "Analyzing SQL file: {} ({:.2} MB) [dialect: {}]",
            file.display(),
            file_size_mb,
            dialect
        );
        println!();
    }

    let start_time = Instant::now();

    let stats = if progress && !json {
        let pb = super::common::byte_progress_bar(file_size);

        let pb_clone = pb.clone();
        let analyzer = Analyzer::new(file.clone()).with_dialect(dialect);
        let stats = analyzer.analyze_with_progress(move |bytes| {
            pb_clone.set_position(bytes);
        })?;

        pb.finish_with_message("done");
        stats
    } else {
        let analyzer = Analyzer::new(file.clone()).with_dialect(dialect);
        analyzer.analyze()?
    };

    let elapsed = start_time.elapsed();

    if json {
        let total_inserts: u64 = stats.iter().map(|s| s.insert_count).sum();
        let total_bytes: u64 = stats.iter().map(|s| s.total_bytes).sum();

        let tables: Vec<TableAnalysis> = stats
            .iter()
            .map(|s| TableAnalysis {
                name: s.table_name.clone(),
                inserts: s.insert_count,
                creates: s.create_count,
                statements: s.statement_count,
                bytes: s.total_bytes,
                size_mb: s.total_bytes as f64 / (1024.0 * 1024.0),
            })
            .collect();

        let output = AnalyzeJsonOutput {
            input_file: file.display().to_string(),
            dialect: dialect.to_string(),
            size_mb: file_size_mb,
            elapsed_secs: elapsed.as_secs_f64(),
            summary: AnalyzeSummary {
                total_tables: stats.len(),
                total_inserts,
                total_bytes,
            },
            tables,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("\n✓ Analysis completed in {:.3?}\n", elapsed);

        if stats.is_empty() {
            println!("No tables found in SQL file.");
            return Ok(ExitCode::SUCCESS);
        }

        print_stats(&stats);
    }

    Ok(ExitCode::SUCCESS)
}

fn run_multi(
    files: Vec<PathBuf>,
    dialect: Option<String>,
    progress: bool,
    fail_fast: bool,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let total = files.len();

    if !json {
        println!("Analyzing {} files...\n", total);
    }

    let failed_entry = |file: &std::path::Path,
                        size_mb: Option<f64>,
                        dialect: Option<String>,
                        error: &anyhow::Error| AnalyzeFileResult {
        file: file.display().to_string(),
        size_mb,
        dialect,
        tables_count: None,
        total_inserts: None,
        total_size_mb: None,
        tables: None,
        status: "failed".to_string(),
        error: Some(error.to_string()),
    };

    let run = drive_multi_file(
        &files,
        fail_fast,
        |idx, file| {
            if !json {
                println!(
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n[{}/{}] {}",
                    idx + 1,
                    total,
                    file.display()
                );
            }

            let file_size = match std::fs::metadata(file) {
                Ok(m) => m.len(),
                Err(e) => {
                    let e = anyhow::Error::from(e);
                    if !json {
                        println!("  Error: {}\n", e);
                    }
                    return FileOutcome::Failure {
                        payload: Some(failed_entry(file, None, None, &e)),
                        error: e.to_string(),
                    };
                }
            };
            let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

            let resolved_dialect =
                match super::common::resolve_dialect(file, dialect.as_deref(), json) {
                    Ok(d) => d,
                    Err(e) => {
                        if !json {
                            println!("  Error: {}\n", e);
                        }
                        return FileOutcome::Failure {
                            payload: Some(failed_entry(file, Some(file_size_mb), None, &e)),
                            error: e.to_string(),
                        };
                    }
                };

            if !json {
                println!(
                    "  Size: {:.2} MB | Dialect: {}",
                    file_size_mb, resolved_dialect
                );
            }

            let analyzer = Analyzer::new(file.to_path_buf()).with_dialect(resolved_dialect);
            let stats = if progress && !json {
                let pb = super::common::compact_progress_bar(file_size);

                let pb_clone = pb.clone();
                let result = analyzer.analyze_with_progress(move |bytes| {
                    pb_clone.set_position(bytes);
                });

                pb.finish_and_clear();
                result
            } else {
                analyzer.analyze()
            };

            match stats {
                Ok(stats) => {
                    let total_inserts: u64 = stats.iter().map(|s| s.insert_count).sum();
                    let total_bytes: u64 = stats.iter().map(|s| s.total_bytes).sum();

                    if !json {
                        println!(
                            "  Tables: {} | INSERTs: {} | Data: {:.2} MB\n",
                            stats.len(),
                            total_inserts,
                            total_bytes as f64 / (1024.0 * 1024.0)
                        );
                    }

                    let tables: Vec<TableAnalysis> = stats
                        .iter()
                        .map(|s| TableAnalysis {
                            name: s.table_name.clone(),
                            inserts: s.insert_count,
                            creates: s.create_count,
                            statements: s.statement_count,
                            bytes: s.total_bytes,
                            size_mb: s.total_bytes as f64 / (1024.0 * 1024.0),
                        })
                        .collect();

                    FileOutcome::Success(AnalyzeFileResult {
                        file: file.display().to_string(),
                        size_mb: Some(file_size_mb),
                        dialect: Some(resolved_dialect.to_string()),
                        tables_count: Some(stats.len()),
                        total_inserts: Some(total_inserts),
                        total_size_mb: Some(total_bytes as f64 / (1024.0 * 1024.0)),
                        tables: Some(tables),
                        status: "success".to_string(),
                        error: None,
                    })
                }
                Err(e) => {
                    if !json {
                        println!("  Error: {}\n", e);
                    }
                    FileOutcome::Failure {
                        payload: Some(failed_entry(
                            file,
                            Some(file_size_mb),
                            Some(resolved_dialect.to_string()),
                            &e,
                        )),
                        error: e.to_string(),
                    }
                }
            }
        },
        |_| None,
    );

    let has_failures = run.has_failures();
    if json {
        let output = MultiAnalyzeJsonOutput {
            total_files: run.total,
            succeeded: run.succeeded,
            failed: run.failed,
            elapsed_secs: run.elapsed.as_secs_f64(),
            results: run.payloads,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Analysis Summary:");
        println!("  Total files: {}", run.total);
        println!("  Succeeded: {}", run.succeeded);
        println!("  Failed: {}", run.failed);
        println!("  Time: {:.3?}", run.elapsed);

        if has_failures {
            println!();
            println!("Failed files:");
            for (path, error) in &run.errors {
                println!("  - {}: {}", path.display(), error);
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

fn print_stats(stats: &[crate::analyzer::TableStats]) {
    println!("Found {} tables:\n", stats.len());
    println!(
        "{:<40} {:>12} {:>12} {:>12}",
        "Table Name", "INSERTs", "Total Stmts", "Size (MB)"
    );
    println!("{}", "─".repeat(80));

    let mut total_inserts: u64 = 0;
    let mut total_bytes: u64 = 0;

    for stat in stats {
        let name = truncate_string(&stat.table_name, 40);
        println!(
            "{:<40} {:>12} {:>12} {:>12.2}",
            name,
            stat.insert_count,
            stat.statement_count,
            stat.total_bytes as f64 / (1024.0 * 1024.0)
        );

        total_inserts += stat.insert_count;
        total_bytes += stat.total_bytes;
    }

    println!("{}", "─".repeat(80));
    println!(
        "{:<40} {:>12} {:>12} {:>12.2}",
        "TOTAL",
        total_inserts,
        "-",
        total_bytes as f64 / (1024.0 * 1024.0)
    );
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
