use crate::splitter::Compression;
use crate::validate::{ValidateOptions, Validator};
use clap::{Args, ValueHint};
use std::path::PathBuf;
use std::time::Instant;

use super::common::{BEHAVIOR, INPUT_OUTPUT, LIMITS, OUTPUT_FORMAT};
use super::glob_util::{expand_file_pattern, MultiFileResult};

#[derive(Args)]
pub struct ValidateArgs {
    /// Input SQL file or glob pattern
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Treat warnings as errors (exit code 1)
    #[arg(long, help_heading = BEHAVIOR)]
    strict: bool,

    /// Skip PK/FK data integrity checks
    #[arg(long, help_heading = BEHAVIOR)]
    no_fk_checks: bool,

    /// Stop on first error (for glob patterns)
    #[arg(long, help_heading = BEHAVIOR)]
    fail_fast: bool,

    /// Max rows per table for PK/FK checks (0 = unlimited)
    #[arg(long, default_value = "1000000", help_heading = LIMITS)]
    max_rows_per_table: usize,

    /// Disable row limit for PK/FK checks
    #[arg(long, help_heading = LIMITS)]
    no_limit: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,
}

pub fn run(args: ValidateArgs) -> anyhow::Result<()> {
    let ValidateArgs {
        file,
        dialect,
        strict,
        no_fk_checks,
        fail_fast,
        max_rows_per_table,
        no_limit,
        progress,
        json,
    } = args;
    let max_rows_per_table = if no_limit || max_rows_per_table == 0 {
        usize::MAX
    } else {
        max_rows_per_table
    };
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
    let dialect = super::common::resolve_dialect(&file, dialect.as_deref(), false)?;

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
        let pb = super::common::byte_progress_bar(file_size);
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

        let resolved_dialect = match super::common::resolve_dialect(file, dialect.as_deref(), false)
        {
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
