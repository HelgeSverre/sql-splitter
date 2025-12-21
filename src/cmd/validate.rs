use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, SqlDialect};
use crate::splitter::Compression;
use crate::validate::{ValidateOptions, Validator};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::time::Instant;

pub fn run(
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
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap()
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
    };

    let validator = Validator::new(options);
    let summary = validator.validate()?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }

    let elapsed = start_time.elapsed();

    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        // Print issues
        for issue in &summary.issues {
            eprintln!("{}", issue);
        }

        if !summary.issues.is_empty() {
            eprintln!();
        }

        // Print summary
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

    // Exit code
    if summary.has_errors() || (strict && summary.has_warnings()) {
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
                let mut reader = compression.wrap_reader(Box::new(file_handle));
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
