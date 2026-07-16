use crate::parser::{detect_dialect_from_file, ContentFilter, DialectConfidence, SqlDialect};
use crate::splitter::{Compression, Splitter};
use crate::writer::IoProfile;
#[allow(unused_imports)]
use anyhow::Context;
use indicatif::{ProgressBar, ProgressStyle};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::glob_util::{expand_file_pattern, MultiFileResult};

/// JSON output for single file split
#[derive(Serialize, JsonSchema)]
pub(crate) struct SplitJsonOutput {
    input_file: String,
    output_dir: String,
    dialect: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialect_confidence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<String>,
    dry_run: bool,
    statistics: SplitStatistics,
    tables: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct SplitStatistics {
    tables_found: usize,
    statements_processed: u64,
    bytes_processed: u64,
    elapsed_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    throughput_mb_per_sec: Option<f64>,
}

/// JSON output for multi-file split
#[derive(Serialize, JsonSchema)]
pub(crate) struct MultiSplitJsonOutput {
    total_files: usize,
    succeeded: usize,
    failed: usize,
    elapsed_secs: f64,
    results: Vec<SplitFileResult>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct SplitFileResult {
    file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_mb: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dialect: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables_found: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statements_processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tables: Option<Vec<String>>,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: PathBuf,
    dialect: Option<String>,
    verbose: bool,
    dry_run: bool,
    progress: bool,
    tables: Option<String>,
    schema_only: bool,
    data_only: bool,
    fail_fast: bool,
    json: bool,
    compress: String,
    io_profile: String,
) -> anyhow::Result<()> {
    let out_compression = Compression::parse_output(&compress).map_err(|e| anyhow::anyhow!(e))?;
    let io_profile = IoProfile::parse(&io_profile).map_err(|e| anyhow::anyhow!(e))?;
    let expanded = expand_file_pattern(&file)?;

    #[cfg(feature = "archive")]
    if expanded.files.len() > 1
        && crate::archive::ArchiveFormat::from_output_path(&output).is_some()
    {
        anyhow::bail!(
            "archive output (e.g. -o out.tar.gz) requires a single input file; the pattern matched {} files",
            expanded.files.len()
        );
    }

    if expanded.files.len() == 1 {
        run_single(
            expanded.files.into_iter().next().unwrap(),
            output,
            dialect,
            verbose,
            dry_run,
            progress,
            tables,
            schema_only,
            data_only,
            json,
            out_compression,
            io_profile,
        )
    } else {
        run_multi(
            expanded.files,
            output,
            dialect,
            verbose,
            dry_run,
            progress,
            tables,
            schema_only,
            data_only,
            fail_fast,
            json,
            out_compression,
            io_profile,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn run_single(
    file: PathBuf,
    output: PathBuf,
    dialect: Option<String>,
    verbose: bool,
    dry_run: bool,
    progress: bool,
    tables: Option<String>,
    schema_only: bool,
    data_only: bool,
    json: bool,
    out_compression: Compression,
    io_profile: IoProfile,
) -> anyhow::Result<()> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    let compression = Compression::from_path(&file);
    let compression_str = if compression != Compression::None {
        if !json {
            println!("Detected compression: {}", compression);
        }
        Some(compression.to_string())
    } else {
        None
    };

    let (dialect_resolved, dialect_confidence) =
        resolve_dialect_with_confidence(&file, dialect.clone(), compression)?;

    if !json && dialect.is_none() {
        let confidence_str = match dialect_confidence {
            DialectConfidence::High => "high confidence",
            DialectConfidence::Medium => "medium confidence",
            DialectConfidence::Low => "low confidence",
        };
        println!(
            "Auto-detected dialect: {} ({})",
            dialect_resolved, confidence_str
        );
    }

    let content_filter = if schema_only {
        ContentFilter::SchemaOnly
    } else if data_only {
        ContentFilter::DataOnly
    } else {
        ContentFilter::All
    };

    // Archive output? Inferred from the -o extension (e.g. `-o dump.tar.gz`).
    #[cfg(feature = "archive")]
    let archive_format = crate::archive::ArchiveFormat::from_output_path(&output);
    let archive_label: Option<&'static str> = {
        #[cfg(feature = "archive")]
        {
            archive_format.map(|f| f.label())
        }
        #[cfg(not(feature = "archive"))]
        {
            None
        }
    };
    if archive_label.is_some() && out_compression != Compression::None {
        anyhow::bail!(
            "`--compress` applies to directory output; for an archive the format comes from the -o extension (e.g. -o out.tar.gz)"
        );
    }

    // Archive mode spools the per-table files into a temp dir on the same
    // filesystem as the target, then packs them; directory mode writes directly.
    let spool = if archive_label.is_some() && !dry_run {
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        std::fs::create_dir_all(&parent).ok();
        Some(
            tempfile::Builder::new()
                .prefix(".sqlsplit-spool-")
                .tempdir_in(&parent)
                .context("creating spool directory for archive output")?,
        )
    } else {
        None
    };
    let split_out_dir = spool
        .as_ref()
        .map(|d| d.path().to_path_buf())
        .unwrap_or_else(|| output.clone());

    if !json {
        if dry_run {
            println!(
                "Dry run: analyzing SQL file: {} ({:.2} MB)",
                file.display(),
                file_size_mb
            );
        } else {
            println!(
                "Splitting SQL file: {} ({:.2} MB)",
                file.display(),
                file_size_mb
            );
            if let Some(fmt) = archive_label {
                println!("Output archive: {} ({})", output.display(), fmt);
            } else {
                println!("Output directory: {}", output.display());
                if out_compression != Compression::None {
                    println!(
                        "Compressing output: {} (*.sql{})",
                        out_compression,
                        out_compression.output_extension()
                    );
                }
            }
        }

        match content_filter {
            ContentFilter::SchemaOnly => println!("Mode: schema-only (DDL statements)"),
            ContentFilter::DataOnly => println!("Mode: data-only (INSERT/COPY statements)"),
            ContentFilter::All => {}
        }
        println!();
    }

    let table_filter: Vec<String> = tables
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    if !json && !table_filter.is_empty() {
        println!("Filtering to tables: {}\n", table_filter.join(", "));
    }

    let mut splitter = Splitter::new(file.clone(), split_out_dir.clone())
        .with_dialect(dialect_resolved)
        .with_dry_run(dry_run)
        .with_content_filter(content_filter)
        .with_output_compression(out_compression)
        .with_io_profile(io_profile);

    if !table_filter.is_empty() {
        splitter = splitter.with_table_filter(table_filter);
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

        splitter = splitter.with_progress(move |bytes| {
            bytes_read_clone.store(bytes, Ordering::Relaxed);
            pb_clone.set_position(bytes);
        });

        let stats = splitter.split()?;
        pb.finish_with_message("done");
        stats
    } else {
        splitter.split()?
    };

    // Archive mode: pack the spooled per-table files into the single archive,
    // then the spool tempdir is removed when `spool` drops.
    #[cfg(feature = "archive")]
    if !dry_run {
        if let (Some(fmt), Some(dir)) = (archive_format, spool.as_ref()) {
            crate::archive::pack_directory(dir.path(), &output, fmt)
                .with_context(|| format!("packing archive {}", output.display()))?;
        }
    }

    let elapsed = start_time.elapsed();

    if json {
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            Some(stats.bytes_processed as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64())
        } else {
            None
        };

        let output_json = SplitJsonOutput {
            input_file: file.display().to_string(),
            output_dir: output.display().to_string(),
            dialect: dialect_resolved.to_string(),
            dialect_confidence: if dialect.is_none() {
                Some(format!("{:?}", dialect_confidence).to_lowercase())
            } else {
                None
            },
            compression: compression_str,
            dry_run,
            statistics: SplitStatistics {
                tables_found: stats.tables_found,
                statements_processed: stats.statements_processed,
                bytes_processed: stats.bytes_processed,
                elapsed_secs: elapsed.as_secs_f64(),
                throughput_mb_per_sec: throughput,
            },
            tables: stats.table_names,
        };
        println!("{}", serde_json::to_string_pretty(&output_json)?);
    } else {
        if dry_run {
            println!("\n✓ Dry run completed!");
            println!("\nWould create {} table files:", stats.tables_found);
            for name in &stats.table_names {
                println!("  - {}.sql", name);
            }
        } else {
            println!("\n✓ Split completed successfully!");
        }

        println!("\nStatistics:");
        println!("  Tables found: {}", stats.tables_found);
        println!("  Statements processed: {}", stats.statements_processed);
        println!(
            "  Bytes processed: {:.2} MB",
            stats.bytes_processed as f64 / (1024.0 * 1024.0)
        );
        println!("  Elapsed time: {:.3?}", elapsed);

        if elapsed.as_secs_f64() > 0.0 {
            let throughput =
                stats.bytes_processed as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
            println!("  Throughput: {:.2} MB/s", throughput);
        }

        if verbose && !dry_run {
            println!("\nOutput files created in: {}", output.display());
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_multi(
    files: Vec<PathBuf>,
    base_output: PathBuf,
    dialect: Option<String>,
    verbose: bool,
    dry_run: bool,
    progress: bool,
    tables: Option<String>,
    schema_only: bool,
    data_only: bool,
    fail_fast: bool,
    json: bool,
    out_compression: Compression,
    io_profile: IoProfile,
) -> anyhow::Result<()> {
    let total = files.len();
    let mut result = MultiFileResult::new();
    result.total_files = total;

    if !json {
        println!("Splitting {} files...\n", total);
    }

    let start_time = Instant::now();
    let mut json_results: Vec<SplitFileResult> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
        if !json {
            println!(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n[{}/{}] {}",
                idx + 1,
                total,
                file.display()
            );
        }

        let file_stem = file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("file_{}", idx));
        let output_dir = base_output.join(&file_stem);

        let file_size = match std::fs::metadata(file) {
            Ok(m) => m.len(),
            Err(e) => {
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(SplitFileResult {
                    file: file.display().to_string(),
                    size_mb: None,
                    dialect: None,
                    output_dir: None,
                    tables_found: None,
                    statements_processed: None,
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
        let resolved_dialect = match resolve_dialect(file, dialect.clone(), compression) {
            Ok(d) => d,
            Err(e) => {
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(SplitFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: None,
                    output_dir: None,
                    tables_found: None,
                    statements_processed: None,
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
            if !dry_run {
                println!("  Output: {}", output_dir.display());
            }
        }

        let content_filter = if schema_only {
            ContentFilter::SchemaOnly
        } else if data_only {
            ContentFilter::DataOnly
        } else {
            ContentFilter::All
        };

        let table_filter: Vec<String> = tables
            .clone()
            .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();

        let mut splitter = Splitter::new(file.clone(), output_dir.clone())
            .with_dialect(resolved_dialect)
            .with_dry_run(dry_run)
            .with_content_filter(content_filter)
            .with_output_compression(out_compression)
            .with_io_profile(io_profile);

        if !table_filter.is_empty() {
            splitter = splitter.with_table_filter(table_filter);
        }

        let split_result = if progress && !json {
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
            splitter = splitter.with_progress(move |bytes| {
                pb_clone.set_position(bytes);
            });

            let result = splitter.split();
            pb.finish_and_clear();
            result
        } else {
            splitter.split()
        };

        match split_result {
            Ok(stats) => {
                if !json {
                    println!(
                        "  Tables: {} | Statements: {} | {}\n",
                        stats.tables_found,
                        stats.statements_processed,
                        if dry_run { "(dry run)" } else { "✓" }
                    );
                    if verbose {
                        for name in &stats.table_names {
                            println!("    - {}.sql", name);
                        }
                        println!();
                    }
                }
                json_results.push(SplitFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: Some(resolved_dialect.to_string()),
                    output_dir: Some(output_dir.display().to_string()),
                    tables_found: Some(stats.tables_found),
                    statements_processed: Some(stats.statements_processed),
                    tables: Some(stats.table_names),
                    status: "success".to_string(),
                    error: None,
                });
                result.record_success();
            }
            Err(e) => {
                if !json {
                    println!("  Error: {}\n", e);
                }
                json_results.push(SplitFileResult {
                    file: file.display().to_string(),
                    size_mb: Some(file_size_mb),
                    dialect: Some(resolved_dialect.to_string()),
                    output_dir: None,
                    tables_found: None,
                    statements_processed: None,
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
        let output_json = MultiSplitJsonOutput {
            total_files: total,
            succeeded: result.succeeded,
            failed: result.failed,
            elapsed_secs: elapsed.as_secs_f64(),
            results: json_results,
        };
        println!("{}", serde_json::to_string_pretty(&output_json)?);
    } else {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Split Summary:");
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

fn resolve_dialect(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
) -> anyhow::Result<SqlDialect> {
    let (dialect, _) = resolve_dialect_with_confidence(file, dialect, compression)?;
    Ok(dialect)
}

fn resolve_dialect_with_confidence(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
) -> anyhow::Result<(SqlDialect, DialectConfidence)> {
    use crate::parser::detect_dialect;
    use std::io::Read;

    match dialect {
        Some(d) => {
            let parsed: SqlDialect = d.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            Ok((parsed, DialectConfidence::High))
        }
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

            Ok((result.dialect, result.confidence))
        }
    }
}
