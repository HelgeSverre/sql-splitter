//! Convert command CLI handler.

use crate::convert::{self, ConvertConfig};
use crate::parser::SqlDialect;
use std::path::PathBuf;

use super::glob_util::{expand_file_pattern, MultiFileResult};

#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    from_dialect: Option<String>,
    to_dialect: String,
    strict: bool,
    progress: bool,
    dry_run: bool,
    fail_fast: bool,
) -> anyhow::Result<()> {
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
        )
    }
}

fn run_single(
    file: PathBuf,
    output: Option<PathBuf>,
    from_dialect: Option<String>,
    to_dialect: String,
    strict: bool,
    progress: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    let from = if let Some(d) = from_dialect {
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
        input: file,
        output,
        from_dialect: from,
        to_dialect: to,
        dry_run,
        progress,
        strict,
    };

    let stats = convert::run(config)?;

    print_stats(&stats, dry_run, progress);

    if strict && !stats.warnings.is_empty() {
        anyhow::bail!("Strict mode: {} warnings generated", stats.warnings.len());
    }

    Ok(())
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
) -> anyhow::Result<()> {
    let total = files.len();
    let mut result = MultiFileResult::new();
    result.total_files = total;

    let to = to_dialect
        .parse::<SqlDialect>()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    if !dry_run {
        std::fs::create_dir_all(&output_dir)?;
    }

    eprintln!("Converting {} files to {}...\n", total, to);

    for (idx, file) in files.iter().enumerate() {
        eprintln!(
            "[{}/{}] Converting: {}",
            idx + 1,
            total,
            file.display()
        );

        let file_size = match std::fs::metadata(file) {
            Ok(m) => m.len(),
            Err(e) => {
                eprintln!("  Error: {}\n", e);
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
                continue;
            }
        };
        let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

        let from = if let Some(ref d) = from_dialect {
            Some(
                d.parse::<SqlDialect>()
                    .map_err(|e| anyhow::anyhow!("{}", e))?,
            )
        } else {
            None
        };

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
            input: file.clone(),
            output: output_file.clone(),
            from_dialect: from,
            to_dialect: to,
            dry_run,
            progress: false,
            strict,
        };

        match convert::run(config) {
            Ok(stats) => {
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

                if let Some(out) = output_file {
                    eprintln!("  → {}", out.display());
                }
                eprintln!();

                if strict && !stats.warnings.is_empty() {
                    result.record_failure(
                        file.clone(),
                        format!("{} warnings in strict mode", stats.warnings.len()),
                    );
                    if fail_fast {
                        break;
                    }
                } else {
                    result.record_success();
                }
            }
            Err(e) => {
                eprintln!("  Error: {}\n", e);
                result.record_failure(file.clone(), e.to_string());
                if fail_fast {
                    break;
                }
            }
        }
    }

    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Conversion Summary:");
    eprintln!("  Total files: {}", total);
    eprintln!("  Succeeded: {}", result.succeeded);
    eprintln!("  Failed: {}", result.failed);

    if result.has_failures() {
        eprintln!();
        eprintln!("Failed files:");
        for (path, error) in &result.errors {
            eprintln!("  - {}: {}", path.display(), error);
        }
        std::process::exit(1);
    }

    Ok(())
}

fn print_stats(stats: &convert::ConvertStats, dry_run: bool, progress: bool) {
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
        eprintln!();
        eprintln!("Warnings ({}):", stats.warnings.len());
        for warning in &stats.warnings {
            eprintln!("  ⚠ {}", warning);
        }
    }

    if dry_run {
        eprintln!();
        eprintln!("(Dry run - no output written)");
    }
}
