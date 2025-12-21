use crate::differ::{format_diff, DiffConfig, DiffOutputFormat, Differ};
use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, SqlDialect};
use crate::splitter::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

fn parse_pk_overrides(s: &str) -> HashMap<String, Vec<String>> {
    s.split(',')
        .filter_map(|pair| {
            let (table, cols) = pair.split_once(':')?;
            let columns: Vec<String> = cols.split('+').map(|c| c.trim().to_string()).collect();
            Some((table.trim().to_lowercase(), columns))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    old_file: PathBuf,
    new_file: PathBuf,
    output: Option<PathBuf>,
    tables: Option<String>,
    exclude: Option<String>,
    schema_only: bool,
    data_only: bool,
    format: Option<String>,
    dialect: Option<String>,
    verbose: bool,
    progress: bool,
    max_pk_entries: usize,
    allow_no_pk: bool,
    ignore_order: bool,
    primary_key: Option<String>,
    ignore_columns: Option<String>,
) -> anyhow::Result<()> {
    // Validate files exist
    if !old_file.exists() {
        anyhow::bail!("Old file does not exist: {}", old_file.display());
    }
    if !new_file.exists() {
        anyhow::bail!("New file does not exist: {}", new_file.display());
    }

    // Parse format
    let output_format: DiffOutputFormat = format
        .as_deref()
        .map(|f| f.parse().map_err(|e: String| anyhow::anyhow!(e)))
        .transpose()?
        .unwrap_or(DiffOutputFormat::Text);

    let is_json = matches!(output_format, DiffOutputFormat::Json);

    // Resolve dialect
    let resolved_dialect = resolve_dialect(&old_file, dialect.clone())?;

    // Parse table filters
    let tables_filter: Vec<String> = tables
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let exclude_filter: Vec<String> = exclude
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Calculate file sizes for display and progress
    let old_size = std::fs::metadata(&old_file)?.len();
    let new_size = std::fs::metadata(&new_file)?.len();
    let total_bytes = if schema_only || data_only {
        old_size + new_size
    } else {
        (old_size + new_size) * 2
    };

    if !is_json {
        eprintln!("Comparing: {} → {}", old_file.display(), new_file.display());
        eprintln!(
            "  Old: {:.2} MB, New: {:.2} MB",
            old_size as f64 / (1024.0 * 1024.0),
            new_size as f64 / (1024.0 * 1024.0)
        );
        eprintln!("  Dialect: {}", resolved_dialect);
        eprintln!();
    }

    let start_time = Instant::now();

    // Set up progress bar
    let pb = if progress && !is_json {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Comparing...");
        Some(pb)
    } else {
        None
    };

    // Parse PK overrides
    let pk_overrides = primary_key
        .as_deref()
        .map(parse_pk_overrides)
        .unwrap_or_default();

    // Parse ignore columns
    let ignore_columns_vec: Vec<String> = ignore_columns
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_default();

    // Build config
    let config = DiffConfig {
        old_path: old_file,
        new_path: new_file,
        dialect: Some(resolved_dialect),
        schema_only,
        data_only,
        tables: tables_filter,
        exclude: exclude_filter,
        format: output_format,
        verbose,
        progress,
        max_pk_entries,
        allow_no_pk,
        ignore_column_order: ignore_order,
        pk_overrides,
        ignore_columns: ignore_columns_vec.clone(),
    };

    // Run diff
    let mut differ = Differ::new(config.clone());
    if let Some(ref pb) = pb {
        let pb_clone = pb.clone();
        differ = differ.with_progress(move |current, total| {
            pb_clone.set_length(total);
            pb_clone.set_position(current);
        });
    }

    let result = differ.diff()?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }

    let elapsed = start_time.elapsed();

    // Format output
    let output_text = format_diff(&result, output_format, resolved_dialect);

    // Write output
    match output {
        Some(path) => {
            let mut file = std::fs::File::create(&path)?;
            file.write_all(output_text.as_bytes())?;
            if !is_json {
                eprintln!("Output written to: {}", path.display());
            }
        }
        None => {
            print!("{}", output_text);
        }
    }

    if !is_json {
        eprintln!();
        eprintln!("Diff completed in {:.3?}", elapsed);
    }

    Ok(())
}

fn resolve_dialect(file: &std::path::Path, dialect: Option<String>) -> anyhow::Result<SqlDialect> {
    use std::io::Read;

    match dialect {
        Some(d) => d.parse().map_err(|e: String| anyhow::anyhow!(e)),
        None => {
            let compression = Compression::from_path(file);
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
