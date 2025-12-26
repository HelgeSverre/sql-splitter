use crate::parser::SqlDialect;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

/// Statistics from merge operation
#[derive(Debug, Default, Serialize)]
pub struct MergeStats {
    pub tables_merged: usize,
    pub bytes_written: u64,
    pub table_names: Vec<String>,
}

/// Merge configuration (reserved for future use)
#[allow(dead_code)]
pub struct MergeConfig {
    pub input_dir: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: SqlDialect,
    pub tables: Option<HashSet<String>>,
    pub exclude: HashSet<String>,
    pub add_transaction: bool,
    pub add_header: bool,
    pub progress: bool,
}

/// JSON output for merge command
#[derive(Serialize)]
struct MergeJsonOutput {
    input_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_file: Option<String>,
    dialect: String,
    dry_run: bool,
    statistics: MergeStatistics,
    tables: Vec<String>,
    options: MergeOptions,
}

#[derive(Serialize)]
struct MergeStatistics {
    tables_merged: usize,
    bytes_written: u64,
    elapsed_secs: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    throughput_kb_per_sec: Option<f64>,
}

#[derive(Serialize)]
struct MergeOptions {
    transaction: bool,
    header: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    input_dir: PathBuf,
    output: Option<PathBuf>,
    dialect: Option<String>,
    tables: Option<String>,
    exclude: Option<String>,
    transaction: bool,
    no_header: bool,
    progress: bool,
    dry_run: bool,
    json: bool,
) -> anyhow::Result<()> {
    // Validate input directory
    if !input_dir.exists() {
        anyhow::bail!("input directory does not exist: {}", input_dir.display());
    }
    if !input_dir.is_dir() {
        anyhow::bail!("input path is not a directory: {}", input_dir.display());
    }

    // Parse dialect
    let dialect: SqlDialect = dialect
        .map(|d| d.parse())
        .transpose()
        .map_err(|e: String| anyhow::anyhow!(e))?
        .unwrap_or_default();

    // Parse table filters
    let tables_filter: Option<HashSet<String>> =
        tables.map(|t| t.split(',').map(|s| s.trim().to_lowercase()).collect());

    let exclude_set: HashSet<String> = exclude
        .map(|e| e.split(',').map(|s| s.trim().to_lowercase()).collect())
        .unwrap_or_default();

    // Discover SQL files
    let sql_files = discover_sql_files(&input_dir)?;
    if sql_files.is_empty() {
        anyhow::bail!("no .sql files found in directory: {}", input_dir.display());
    }

    // Filter files
    let filtered_files: Vec<(String, PathBuf)> = sql_files
        .into_iter()
        .filter(|(name, _)| {
            let name_lower = name.to_lowercase();
            // Check include filter
            if let Some(ref include) = tables_filter {
                if !include.contains(&name_lower) {
                    return false;
                }
            }
            // Check exclude filter
            !exclude_set.contains(&name_lower)
        })
        .collect();

    if filtered_files.is_empty() {
        anyhow::bail!("no tables remaining after filtering");
    }

    // Sort alphabetically
    let mut sorted_files = filtered_files;
    sorted_files.sort_by(|a, b| a.0.cmp(&b.0));

    if !json {
        println!(
            "Merging {} tables from: {}",
            sorted_files.len(),
            input_dir.display()
        );
        if let Some(ref out) = output {
            println!("Output: {}", out.display());
        } else {
            println!("Output: stdout");
        }
        println!();
    }

    if dry_run {
        if json {
            let output_json = MergeJsonOutput {
                input_dir: input_dir.display().to_string(),
                output_file: output.as_ref().map(|p| p.display().to_string()),
                dialect: dialect.to_string(),
                dry_run: true,
                statistics: MergeStatistics {
                    tables_merged: sorted_files.len(),
                    bytes_written: 0,
                    elapsed_secs: 0.0,
                    throughput_kb_per_sec: None,
                },
                tables: sorted_files.iter().map(|(name, _)| name.clone()).collect(),
                options: MergeOptions {
                    transaction,
                    header: !no_header,
                },
            };
            println!("{}", serde_json::to_string_pretty(&output_json)?);
        } else {
            println!("Tables to merge:");
            for (name, _) in &sorted_files {
                println!("  - {}", name);
            }
            println!("\nDry run complete. No files written.");
        }
        return Ok(());
    }

    let start_time = Instant::now();

    // Set up output writer
    let stats = if let Some(ref out_path) = output {
        // Ensure parent directory exists
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(out_path)?;
        let writer = BufWriter::with_capacity(256 * 1024, file);
        merge_files(
            sorted_files,
            writer,
            dialect,
            transaction,
            !no_header,
            progress && !json,
        )?
    } else {
        let stdout = io::stdout();
        let writer = BufWriter::new(stdout.lock());
        merge_files(
            sorted_files,
            writer,
            dialect,
            transaction,
            !no_header,
            progress && !json,
        )?
    };

    let elapsed = start_time.elapsed();

    if json {
        let throughput = if elapsed.as_secs_f64() > 0.0 {
            Some(stats.bytes_written as f64 / 1024.0 / elapsed.as_secs_f64())
        } else {
            None
        };

        let output_json = MergeJsonOutput {
            input_dir: input_dir.display().to_string(),
            output_file: output.as_ref().map(|p| p.display().to_string()),
            dialect: dialect.to_string(),
            dry_run: false,
            statistics: MergeStatistics {
                tables_merged: stats.tables_merged,
                bytes_written: stats.bytes_written,
                elapsed_secs: elapsed.as_secs_f64(),
                throughput_kb_per_sec: throughput,
            },
            tables: stats.table_names.clone(),
            options: MergeOptions {
                transaction,
                header: !no_header,
            },
        };
        // Only print JSON to stdout if we're not also writing merge output to stdout
        if output.is_some() {
            println!("{}", serde_json::to_string_pretty(&output_json)?);
        } else {
            // Can't output JSON when merge output goes to stdout
            // The JSON would be mixed with SQL output
            eprintln!("{}", serde_json::to_string_pretty(&output_json)?);
        }
    } else if output.is_some() {
        println!("\n✓ Merge completed successfully!");
        println!("\nStatistics:");
        println!("  Tables merged: {}", stats.tables_merged);
        println!(
            "  Bytes written: {:.2} KB",
            stats.bytes_written as f64 / 1024.0
        );
        println!("  Elapsed time: {:.3?}", elapsed);

        if elapsed.as_secs_f64() > 0.0 {
            let throughput = stats.bytes_written as f64 / 1024.0 / elapsed.as_secs_f64();
            println!("  Throughput: {:.2} KB/s", throughput);
        }
    }

    Ok(())
}

/// Discover all .sql files in a directory, returning (table_name, path) pairs
fn discover_sql_files(dir: &PathBuf) -> anyhow::Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext.eq_ignore_ascii_case("sql") {
                    if let Some(stem) = path.file_stem() {
                        let table_name = stem.to_string_lossy().to_string();
                        files.push((table_name, path));
                    }
                }
            }
        }
    }

    Ok(files)
}

/// Merge multiple SQL files into a single output
fn merge_files<W: Write>(
    files: Vec<(String, PathBuf)>,
    mut writer: W,
    dialect: SqlDialect,
    add_transaction: bool,
    add_header: bool,
    show_progress: bool,
) -> anyhow::Result<MergeStats> {
    let mut stats = MergeStats::default();

    // Calculate total size for progress
    let total_size: u64 = files
        .iter()
        .map(|(_, p)| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    let pb = if show_progress {
        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("█▓▒░  "),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    // Write header
    if add_header {
        write_header(&mut writer, dialect, files.len())?;
        stats.bytes_written += count_header_bytes(dialect, files.len()) as u64;
    }

    // Write transaction start
    if add_transaction {
        let tx_start = transaction_start(dialect);
        writer.write_all(tx_start.as_bytes())?;
        stats.bytes_written += tx_start.len() as u64;
    }

    let mut bytes_processed: u64 = 0;

    // Merge each file
    for (table_name, path) in &files {
        // Write table separator comment
        let separator = format!(
            "\n-- ============================================================\n-- Table: {}\n-- ============================================================\n\n",
            table_name
        );
        writer.write_all(separator.as_bytes())?;
        stats.bytes_written += separator.len() as u64;

        // Stream file content
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let reader = BufReader::with_capacity(64 * 1024, file);

        for line in reader.lines() {
            let line = line?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            stats.bytes_written += line.len() as u64 + 1;
        }

        bytes_processed += file_size;
        if let Some(ref pb) = pb {
            pb.set_position(bytes_processed);
        }

        stats.table_names.push(table_name.clone());
        stats.tables_merged += 1;
    }

    // Write transaction end
    if add_transaction {
        let tx_end = transaction_end(dialect);
        writer.write_all(tx_end.as_bytes())?;
        stats.bytes_written += tx_end.len() as u64;
    }

    // Write footer
    if add_header {
        write_footer(&mut writer, dialect)?;
    }

    writer.flush()?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }

    Ok(stats)
}

fn write_header<W: Write>(w: &mut W, dialect: SqlDialect, table_count: usize) -> io::Result<()> {
    writeln!(w, "-- SQL Merge Output")?;
    writeln!(w, "-- Generated by sql-splitter")?;
    writeln!(w, "-- Tables: {}", table_count)?;
    writeln!(w, "-- Dialect: {}", dialect)?;
    writeln!(w)?;

    match dialect {
        SqlDialect::MySql => {
            writeln!(w, "SET NAMES utf8mb4;")?;
            writeln!(w, "SET FOREIGN_KEY_CHECKS = 0;")?;
        }
        SqlDialect::Postgres => {
            writeln!(w, "SET client_encoding = 'UTF8';")?;
        }
        SqlDialect::Sqlite => {
            writeln!(w, "PRAGMA foreign_keys = OFF;")?;
        }
        SqlDialect::Mssql => {
            writeln!(w, "SET ANSI_NULLS ON;")?;
            writeln!(w, "SET QUOTED_IDENTIFIER ON;")?;
            writeln!(w, "SET NOCOUNT ON;")?;
        }
    }
    writeln!(w)?;

    Ok(())
}

fn count_header_bytes(dialect: SqlDialect, table_count: usize) -> usize {
    let base = format!(
        "-- SQL Merge Output\n-- Generated by sql-splitter\n-- Tables: {}\n-- Dialect: {}\n\n",
        table_count, dialect
    );
    let dialect_specific = match dialect {
        SqlDialect::MySql => "SET NAMES utf8mb4;\nSET FOREIGN_KEY_CHECKS = 0;\n\n",
        SqlDialect::Postgres => "SET client_encoding = 'UTF8';\n\n",
        SqlDialect::Sqlite => "PRAGMA foreign_keys = OFF;\n\n",
        SqlDialect::Mssql => "SET ANSI_NULLS ON;\nSET QUOTED_IDENTIFIER ON;\nSET NOCOUNT ON;\n\n",
    };
    base.len() + dialect_specific.len()
}

fn write_footer<W: Write>(w: &mut W, dialect: SqlDialect) -> io::Result<()> {
    writeln!(w)?;
    match dialect {
        SqlDialect::MySql => {
            writeln!(w, "SET FOREIGN_KEY_CHECKS = 1;")?;
        }
        SqlDialect::Postgres => {
            // No footer needed
        }
        SqlDialect::Sqlite => {
            writeln!(w, "PRAGMA foreign_keys = ON;")?;
        }
        SqlDialect::Mssql => {
            // No footer needed
        }
    }
    Ok(())
}

fn transaction_start(dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::MySql => "START TRANSACTION;\n\n".to_string(),
        SqlDialect::Postgres => "BEGIN;\n\n".to_string(),
        SqlDialect::Sqlite => "BEGIN TRANSACTION;\n\n".to_string(),
        SqlDialect::Mssql => "BEGIN TRANSACTION;\n\n".to_string(),
    }
}

fn transaction_end(dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::MySql | SqlDialect::Postgres | SqlDialect::Sqlite | SqlDialect::Mssql => {
            "\nCOMMIT;\n".to_string()
        }
    }
}
