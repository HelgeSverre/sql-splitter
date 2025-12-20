use crate::analyzer::Analyzer;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Instant;

pub fn run(file: PathBuf, progress: bool) -> anyhow::Result<()> {
    if !file.exists() {
        anyhow::bail!("input file does not exist: {}", file.display());
    }

    let file_size = std::fs::metadata(&file)?.len();
    let file_size_mb = file_size as f64 / (1024.0 * 1024.0);

    println!(
        "Analyzing SQL file: {} ({:.2} MB)",
        file.display(),
        file_size_mb
    );
    println!();

    let start_time = Instant::now();

    let stats = if progress {
        let last_progress = AtomicI32::new(0);
        let analyzer = Analyzer::new(file);
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
        let analyzer = Analyzer::new(file);
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

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
