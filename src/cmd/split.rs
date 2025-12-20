use crate::parser::{detect_dialect_from_file, ContentFilter, DialectConfidence, SqlDialect};
use crate::splitter::{Compression, Splitter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;

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
) -> anyhow::Result<()> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    // Detect compression
    let compression = Compression::from_path(&file);
    if compression != Compression::None {
        println!("Detected compression: {}", compression);
    }

    let dialect = resolve_dialect(&file, dialect, compression)?;

    // Determine content filter
    let content_filter = if schema_only {
        ContentFilter::SchemaOnly
    } else if data_only {
        ContentFilter::DataOnly
    } else {
        ContentFilter::All
    };

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
        println!("Output directory: {}", output.display());
    }

    // Show content filter mode
    match content_filter {
        ContentFilter::SchemaOnly => println!("Mode: schema-only (DDL statements)"),
        ContentFilter::DataOnly => println!("Mode: data-only (INSERT/COPY statements)"),
        ContentFilter::All => {}
    }
    println!();

    let table_filter: Vec<String> = tables
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    if !table_filter.is_empty() {
        println!("Filtering to tables: {}\n", table_filter.join(", "));
    }

    let mut splitter = Splitter::new(file, output.clone())
        .with_dialect(dialect)
        .with_dry_run(dry_run)
        .with_content_filter(content_filter);

    if !table_filter.is_empty() {
        splitter = splitter.with_table_filter(table_filter);
    }

    let start_time = Instant::now();

    let stats = if progress {
        let last_progress = AtomicI32::new(0);
        splitter = splitter.with_progress(move |bytes_read| {
            let pct = (bytes_read as f64 / file_size as f64 * 100.0) as i32;
            let last = last_progress.load(Ordering::Relaxed);
            if pct > last && pct % 5 == 0 {
                last_progress.store(pct, Ordering::Relaxed);
                eprint!("\rProgress: {}%", pct);
            }
        });
        let stats = splitter.split()?;
        eprintln!();
        stats
    } else {
        splitter.split()?
    };

    let elapsed = start_time.elapsed();

    if dry_run {
        println!("✓ Dry run completed!");
        println!("\nWould create {} table files:", stats.tables_found);
        for name in &stats.table_names {
            println!("  - {}.sql", name);
        }
    } else {
        println!("✓ Split completed successfully!");
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
        let throughput = stats.bytes_processed as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
        println!("  Throughput: {:.2} MB/s", throughput);
    }

    if verbose && !dry_run {
        println!("\nOutput files created in: {}", output.display());
    }

    Ok(())
}

fn resolve_dialect(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
) -> anyhow::Result<SqlDialect> {
    use crate::parser::detect_dialect;
    use std::io::Read;

    match dialect {
        Some(d) => d.parse().map_err(|e: String| anyhow::anyhow!(e)),
        None => {
            // For compressed files, we need to decompress a sample first
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
            println!(
                "Auto-detected dialect: {} ({})",
                result.dialect, confidence_str
            );
            Ok(result.dialect)
        }
    }
}
