use crate::analyzer::Analyzer;
use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, SqlDialect};
use crate::splitter::Compression;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;

pub fn run(file: PathBuf, dialect: Option<String>, progress: bool) -> anyhow::Result<()> {
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

    println!(
        "Analyzing SQL file: {} ({:.2} MB) [dialect: {}]",
        file.display(),
        file_size_mb,
        dialect
    );
    println!();

    let start_time = Instant::now();

    let stats = if progress {
        let last_progress = AtomicI32::new(0);
        let analyzer = Analyzer::new(file).with_dialect(dialect);
        let stats = analyzer.analyze_with_progress(move |bytes_read| {
            let pct = (bytes_read as f64 / file_size as f64 * 100.0) as i32;
            let last = last_progress.load(Ordering::Relaxed);
            if pct > last && pct % 5 == 0 {
                last_progress.store(pct, Ordering::Relaxed);
                eprint!("\rProgress: {}%", pct);
            }
        })?;
        eprintln!();
        stats
    } else {
        let analyzer = Analyzer::new(file).with_dialect(dialect);
        analyzer.analyze()?
    };

    let elapsed = start_time.elapsed();

    println!("✓ Analysis completed in {:.3?}\n", elapsed);

    if stats.is_empty() {
        println!("No tables found in SQL file.");
        return Ok(());
    }

    println!("Found {} tables:\n", stats.len());
    println!(
        "{:<40} {:>12} {:>12} {:>12}",
        "Table Name", "INSERTs", "Total Stmts", "Size (MB)"
    );
    println!("{}", "─".repeat(80));

    let mut total_inserts: u64 = 0;
    let mut total_bytes: u64 = 0;

    for stat in &stats {
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
            // For compressed files, decompress a sample first
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

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
