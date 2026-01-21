use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, SqlDialect};
use crate::splitter::Compression;
use crate::validate::{ValidateOptions, Validator};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Instant;

use super::glob_util::{expand_file_pattern, MultiFileResult};

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    dialect: Option<String>,
    progress: bool,
    strict: bool,
    json: bool,
    max_rows_per_table: usize,
    no_fk_checks: bool,
    fail_fast: bool,
) -> anyhow::Result<()> {
    let expanded = expand_file_pattern(&file)?;

    if expanded.files.len() == 1 {
        run_single(
            expanded.files.into_iter().next().unwrap(),
            dialect,
            progress,
            strict,
            json,
            max_rows_per_table,
            no_fk_checks,
        )
    } else {
        run_multi(
            expanded.files,
            dialect,
            progress,
            strict,
            json,
            max_rows_per_table,
            no_fk_checks,
            fail_fast,
        )
    }
}

fn run_single(
    file: PathBuf,
    dialect: Option<String>,
    progress: bool,
    strict: bool,
    json: bool,
    max_rows_per_table: usize,
    no_fk_checks: bool,
) -> anyhow::Result<()> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    let compression = Compression::from_path(&file);
    let dialect = resolve_dialect(&file, dialect, compression)?;

    if !json {
        if compression != Compression::None {
            eprintln!("Detected compression: {}", compression);
        }
        eprintln!(
            "Validating SQL file: {} ({:.2} MB) [dialect: {}]",
            file.display(),
            file_size_mb,
            dialect
        );
        eprintln!();
    }

    let start_time = Instant::now();

    let pb = if progress && !json {
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
        pb.set_message("Validating...");
        Some(pb)
    } else {
        None
    };

    let options = ValidateOptions {
        path: file,
        dialect: Some(dialect),
        progress,
        strict,
        json,
        max_rows_per_table,
        fk_checks_enabled: !no_fk_checks,
        max_pk_fk_keys: None,
    };

    let mut validator = Validator::new(options);
    if let Some(ref pb) = pb {
        let pb_clone = pb.clone();
        validator = validator.with_progress(move |bytes| {
            pb_clone.set_position(bytes);
        });
    }
    let summary = validator.validate()?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }

    let elapsed = start_time.elapsed();

    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        for issue in &summary.issues {
            eprintln!("{}", issue);
        }

        if !summary.issues.is_empty() {
            eprintln!();
        }

        eprintln!("Validation summary:");
        eprintln!("  Dialect: {} (auto-detected)", summary.dialect);
        eprintln!(
            "  Statements scanned: {}",
            summary.summary.statements_scanned
        );
        eprintln!("  Tables found: {}", summary.summary.tables_scanned);
        eprintln!("  Time: {:.3?}", elapsed);
        eprintln!();
        eprintln!("  Checks:");
        eprintln!("    - SQL syntax:        {}", summary.checks.syntax);
        eprintln!("    - Encoding:          {}", summary.checks.encoding);
        eprintln!(
            "    - DDL/DML consistency: {}",
            summary.checks.ddl_dml_consistency
        );
        eprintln!("    - PK duplicates:     {}", summary.checks.pk_duplicates);
        eprintln!("    - FK integrity:      {}", summary.checks.fk_integrity);
        eprintln!();
        eprintln!(
            "  Total: {} errors, {} warnings",
            summary.summary.errors, summary.summary.warnings
        );
        eprintln!();

        if summary.has_errors() {
            eprintln!("Result: FAILED");
        } else if summary.has_warnings() && strict {
            eprintln!("Result: FAILED (--strict mode, warnings treated as errors)");
        } else if summary.has_warnings() {
            eprintln!("Result: PASSED (with warnings)");
        } else {
            eprintln!("Result: PASSED");
        }
    }

    if summary.has_errors() || (strict && summary.has_warnings()) {
        std::process::exit(1);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_multi(
    files: Vec<PathBuf>,
    dialect: Option<String>,
    _progress: bool,
    strict: bool,
    json: bool,
    max_rows_per_table: usize,
    no_fk_checks: bool,
    fail_fast: bool,
) -> anyhow::Result<()> {
    let total = files.len();
    let mut result = MultiFileResult::new();
    result.total_files = total;

    if !json {
        eprintln!("Validating {} files...\n", total);
    }

    let start_time = Instant::now();
    let mut all_passed = true;
    let mut json_results: Vec<serde_json::Value> = Vec::new();

    for (idx, file) in files.iter().enumerate() {
        if !json {
            eprintln!("[{}/{}] Validating: {}", idx + 1, total, file.display());
        }

        let file_size = match std::fs::metadata(file) {
            Ok(m) => m.len(),
            Err(e) => {
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
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
                continue;
            }
        };

        let options = ValidateOptions {
            path: file.clone(),
            dialect: Some(resolved_dialect),
            progress: false,
            strict,
            json,
            max_rows_per_table,
            fk_checks_enabled: !no_fk_checks,
            max_pk_fk_keys: None,
        };

        let validator = Validator::new(options);
        let summary = match validator.validate() {
            Ok(s) => s,
            Err(e) => {
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
                continue;
            }
        };

        let file_passed = !(summary.has_errors() || strict && summary.has_warnings());

        if json {
            json_results.push(serde_json::json!({
                "file": file.display().to_string(),
                "size_mb": file_size_mb,
                "passed": file_passed,
                "summary": summary
            }));
        } else {
            let status = if summary.has_errors() {
                "FAILED"
            } else if summary.has_warnings() && strict {
                "FAILED (strict)"
            } else if summary.has_warnings() {
                "PASSED (warnings)"
            } else {
                "PASSED"
            };

            eprintln!(
                "  {} ({:.2} MB): {} errors, {} warnings - {}",
                file.file_name().unwrap_or_default().to_string_lossy(),
                file_size_mb,
                summary.summary.errors,
                summary.summary.warnings,
                status
            );
        }

        if file_passed {
            result.record_success();
        } else {
            all_passed = false;
            result.record_failure(
                file.clone(),
                format!(
                    "{} errors, {} warnings",
                    summary.summary.errors, summary.summary.warnings
                ),
            );
            if fail_fast {
                break;
            }
        }
    }

    let elapsed = start_time.elapsed();

    if json {
        let aggregate = serde_json::json!({
            "total_files": total,
            "passed": result.succeeded,
            "failed": result.failed,
            "elapsed_secs": elapsed.as_secs_f64(),
            "results": json_results
        });
        println!("{}", serde_json::to_string_pretty(&aggregate)?);
    } else {
        eprintln!();
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("Validation Summary:");
        eprintln!("  Total files: {}", total);
        eprintln!("  Passed: {}", result.succeeded);
        eprintln!("  Failed: {}", result.failed);
        eprintln!("  Time: {:.3?}", elapsed);

        if !result.errors.is_empty() {
            eprintln!();
            eprintln!("Failed files:");
            for (path, error) in &result.errors {
                eprintln!("  - {}: {}", path.display(), error);
            }
        }

        eprintln!();
        if all_passed {
            eprintln!("Result: ALL PASSED");
        } else {
            eprintln!("Result: SOME FAILED");
        }
    }

    if result.has_failures() {
        std::process::exit(1);
    }

    Ok(())
}

fn resolve_dialect(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
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

            let confidence_str = match result.confidence {
                DialectConfidence::High => "high confidence",
                DialectConfidence::Medium => "medium confidence",
                DialectConfidence::Low => "low confidence",
            };
            eprintln!(
                "Auto-detected dialect: {} ({})",
                result.dialect, confidence_str
            );
            Ok(result.dialect)
        }
    }
}
