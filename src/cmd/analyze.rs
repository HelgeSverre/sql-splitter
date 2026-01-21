use crate::analyzer::Analyzer;
use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, SqlDialect};
use crate::splitter::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::glob_util::{expand_file_pattern, MultiFileResult};

/// JSON output for single file analyze
#[derive(Serialize)]
struct AnalyzeJsonOutput {
    input_file: String,
    dialect: String,
    size_mb: f64,
    elapsed_secs: f64,
    summary: AnalyzeSummary,
    tables: Vec<TableAnalysis>,
}

#[derive(Serialize)]
struct AnalyzeSummary {
    total_tables: usize,
    total_inserts: u64,
    total_bytes: u64,
}

#[derive(Serialize)]
struct TableAnalysis {
    name: String,
    inserts: u64,
    creates: u64,
    statements: u64,
    bytes: u64,
    size_mb: f64,
}

/// JSON output for multi-file analyze
#[derive(Serialize)]
struct MultiAnalyzeJsonOutput {
    total_files: usize,
    succeeded: usize,
    failed: usize,
    elapsed_secs: f64,
    results: Vec<AnalyzeFileResult>,
}

#[derive(Serialize)]
struct AnalyzeFileResult {
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

pub fn run(
    file: PathBuf,
    dialect: Option<String>,
    progress: bool,
    fail_fast: bool,
    json: bool,
) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    let compression = Compression::from_path(&file);
    if !json && compression != Compression::None {
        println!("Detected compression: {}", compression);
    }

    let dialect = resolve_dialect(&file, dialect, compression, json)?;

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
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let bytes_read = Arc::new(AtomicU64::new(0));
        let bytes_read_clone = bytes_read.clone();
        let pb_clone = pb.clone();

        let analyzer = Analyzer::new(file.clone()).with_dialect(dialect);
        let stats = analyzer.analyze_with_progress(move |bytes| {
            bytes_read_clone.store(bytes, Ordering::Relaxed);
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
            return Ok(());
        }

        print_stats(&stats);
    }

    Ok(())
}

fn run_multi(
    files: Vec<PathBuf>,
    dialect: Option<String>,
    progress: bool,
    fail_fast: bool,
    json: bool,
) -> anyhow::Result<()> {
    let total = files.len();
    let mut result = MultiFileResult::new();
    result.total_files = total;

    if !json {
        println!("Analyzing {} files...\n", total);
    }

    let start_time = Instant::now();
    let mut json_results: Vec<AnalyzeFileResult> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
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
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(AnalyzeFileResult {
                    file: file.display().to_string(),
                    size_mb: None,
                    dialect: None,
                    tables_count: None,
                    total_inserts: None,
                    total_size_mb: None,
                    tables: None,
                    status: "failed".to_string(),
                    error: Some(e.to_string()),
                });
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
                continue;
            }
        };
        let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

        let compression = Compression::from_path(file);
        let resolved_dialect = match resolve_dialect(file, dialect.clone(), compression, json) {
            Ok(d) => d,
            Err(e) => {
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(AnalyzeFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: None,
                    tables_count: None,
                    total_inserts: None,
                    total_size_mb: None,
                    tables: None,
                    status: "failed".to_string(),
                    error: Some(e.to_string()),
                });
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
                continue;
            }
        };

        if !json {
            println!(
                "  Size: {:.2} MB | Dialect: {}",
                file_size_mb, resolved_dialect
            );
        }

        let analyzer = Analyzer::new(file.clone()).with_dialect(resolved_dialect);
        let stats = if progress && !json {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::with_template(
                    "  {spinner:.green} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
                )
                .unwrap()
                .progress_chars("█▓▒░  "),
            );
            pb.enable_steady_tick(std::time::Duration::from_millis(100));

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

                json_results.push(AnalyzeFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: Some(resolved_dialect.to_string()),
                    tables_count: Some(stats.len()),
                    total_inserts: Some(total_inserts),
                    total_size_mb: Some(total_bytes as f64 / (1024.0 * 1024.0)),
                    tables: Some(tables),
                    status: "success".to_string(),
                    error: None,
                });
                result.record_success();
            }
            Err(e) => {
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(AnalyzeFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: Some(resolved_dialect.to_string()),
                    tables_count: None,
                    total_inserts: None,
                    total_size_mb: None,
                    tables: None,
                    status: "failed".to_string(),
                    error: Some(e.to_string()),
                });
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
            }
        }
    }

    let elapsed = start_time.elapsed();

    if json {
        let output = MultiAnalyzeJsonOutput {
            total_files: total,
            succeeded: result.succeeded,
            failed: result.failed,
            elapsed_secs: elapsed.as_secs_f64(),
            results: json_results,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Analysis Summary:");
        println!("  Total files: {}", total);
        println!("  Succeeded: {}", result.succeeded);
        println!("  Failed: {}", result.failed);
        println!("  Time: {:.3?}", elapsed);

        if result.has_failures() {
            println!();
            println!("Failed files:");
            for (path, error) in &result.errors {
                println!("  - {}: {}", path.display(), error);
            }
            std::process::exit(1);
        }
    }

    Ok(())
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

fn resolve_dialect(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
    json: bool,
) -> anyhow::Result<SqlDialect> {
    use std::io::Read;

    match dialect {
        Some(d) => d.parse().map_err(|e: String| anyhow::anyhow!(e)),
        None => {
            let result = if compression != Compression::None {
                let file_handle = std::fs::File::open(file)?;
                let mut reader = compression.wrap_reader(Box::new(file_handle))?;
                let mut header = vec![0u8; 8192];
                let bytes_read = reader.read(&mut header)?;
                header.truncate(bytes_read);
                detect_dialect(&header)
            } else {
                detect_dialect_from_file(file)?
            };

            if !json {
                let confidence_str = match result.confidence {
                    DialectConfidence::High => "high confidence",
                    DialectConfidence::Medium => "medium confidence",
                    DialectConfidence::Low => "low confidence",
                };
                println!(
                    "Auto-detected dialect: {} ({})",
                    result.dialect, confidence_str
                );
            }
            Ok(result.dialect)
        }
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
