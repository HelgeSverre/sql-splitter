//! Query command for running SQL queries on dump files using DuckDB.

use crate::duckdb::{
    should_use_disk_mode, CacheManager, OutputFormat, QueryConfig, QueryEngine,
    QueryResultFormatter,
};
use crate::parser::SqlDialect;
use anyhow::{Context, Result};
use clap::Args;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Query SQL dump files using DuckDB's analytical engine
#[derive(Args, Debug)]
#[command(after_help = "Examples:
  sql-splitter query dump.sql \"SELECT COUNT(*) FROM users\"
  sql-splitter query dump.sql \"SELECT * FROM orders WHERE total > 100\" -f json
  sql-splitter query dump.sql \"SELECT * FROM users LIMIT 10\" -o results.csv -f csv
  sql-splitter query dump.sql --interactive
  sql-splitter query huge.sql \"SELECT ...\" --disk
  sql-splitter query dump.sql \"SELECT ...\" --cache")]
pub struct QueryArgs {
    /// SQL dump file to query
    #[arg(value_name = "INPUT", required_unless_present_any = ["clear_cache", "list_cache"])]
    pub input: Option<PathBuf>,

    /// SQL query to execute (omit for --interactive mode)
    #[arg(value_name = "QUERY")]
    pub query: Option<String>,

    /// Output format: table, json, jsonl, csv, tsv
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// Write output to file instead of stdout
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Source SQL dialect (auto-detected if omitted)
    #[arg(short, long, value_name = "DIALECT")]
    pub dialect: Option<String>,

    /// Use disk-based temp storage (for large dumps >2GB)
    #[arg(long)]
    pub disk: bool,

    /// Cache imported data for repeated queries
    #[arg(long)]
    pub cache: bool,

    /// Start interactive query session (REPL)
    #[arg(short, long)]
    pub interactive: bool,

    /// Only import specific tables (comma-separated)
    #[arg(long, value_name = "TABLES", value_delimiter = ',')]
    pub tables: Option<Vec<String>>,

    /// Memory limit for DuckDB (e.g., "4GB")
    #[arg(long, value_name = "LIMIT")]
    pub memory_limit: Option<String>,

    /// Show import progress
    #[arg(long)]
    pub progress: bool,

    /// Show query execution time
    #[arg(long)]
    pub timing: bool,

    /// Clear all cached databases
    #[arg(long)]
    pub clear_cache: bool,

    /// List cached databases
    #[arg(long)]
    pub list_cache: bool,
}

/// Run the query command
pub fn run(args: QueryArgs) -> Result<()> {
    // Handle cache management commands
    if args.clear_cache {
        return clear_cache();
    }

    if args.list_cache {
        return list_cache();
    }

    // Get input path (required at this point)
    let input = args
        .input
        .ok_or_else(|| anyhow::anyhow!("Input file is required"))?;

    // Validate arguments
    if !args.interactive && args.query.is_none() {
        anyhow::bail!("Query is required (or use --interactive mode)");
    }

    // Parse output format
    let output_format: OutputFormat = args
        .format
        .parse()
        .map_err(|e: String| anyhow::anyhow!(e))?;

    // Parse dialect
    let dialect: Option<SqlDialect> = if let Some(ref d) = args.dialect {
        Some(d.parse().map_err(|e: String| anyhow::anyhow!(e))?)
    } else {
        None
    };

    // Check file size for auto disk mode
    let file_size = std::fs::metadata(&input)
        .with_context(|| format!("Cannot access file: {}", input.display()))?
        .len();

    let disk_mode = args.disk || should_use_disk_mode(file_size);

    // Build configuration
    let config = QueryConfig {
        dialect,
        disk_mode,
        cache_enabled: args.cache,
        tables: args.tables,
        memory_limit: args.memory_limit,
        progress: args.progress || args.interactive,
    };

    // Try to use cache if enabled
    let mut engine = if args.cache {
        try_load_from_cache(&input, &config)?
    } else {
        None
    };

    // Import dump if not loaded from cache
    if engine.is_none() {
        let mut new_engine = QueryEngine::new(&config)?;

        eprintln!("Importing {}...", input.display());
        let stats = new_engine.import_dump(&input)?;

        let tables_created = stats.tables_created;
        let rows_inserted = stats.rows_inserted;
        let duration_secs = stats.duration_secs;
        let warnings: Vec<String> = stats.warnings.clone();

        eprintln!(
            "Imported {} tables, {} rows in {:.2}s",
            tables_created, rows_inserted, duration_secs
        );

        // Print warnings
        for warning in &warnings {
            eprintln!("Warning: {}", warning);
        }

        // Save to cache if enabled
        if args.cache {
            save_to_cache(&new_engine, &input, tables_created, rows_inserted)?;
        }

        engine = Some(new_engine);
    }

    let engine = engine.unwrap();

    // Run query or start REPL
    if args.interactive {
        run_repl(&engine, output_format, args.timing)
    } else {
        let query = args.query.unwrap();
        let result = engine.query(&query)?;

        // Output result
        let formatted = QueryResultFormatter::format(&result, output_format);

        if let Some(output_path) = args.output {
            let file = File::create(&output_path)
                .with_context(|| format!("Cannot create output file: {}", output_path.display()))?;
            let mut writer = BufWriter::new(file);
            writer.write_all(formatted.as_bytes())?;
            eprintln!(
                "Wrote {} rows to {}",
                result.row_count(),
                output_path.display()
            );
        } else {
            print!("{}", formatted);
        }

        if args.timing {
            eprintln!("Query executed in {:.3}s", result.execution_time_secs);
        }

        Ok(())
    }
}

/// Try to load a query engine from cache
fn try_load_from_cache(dump_path: &Path, config: &QueryConfig) -> Result<Option<QueryEngine>> {
    let cache_manager = CacheManager::new()?;

    if let Some(cache_path) = cache_manager.get_cache(dump_path)? {
        eprintln!("Using cached database: {}", cache_path.display());
        let engine = QueryEngine::from_cache(&cache_path, config)?;
        return Ok(Some(engine));
    }

    Ok(None)
}

/// Save the current database to cache
fn save_to_cache(
    engine: &QueryEngine,
    dump_path: &Path,
    table_count: usize,
    row_count: u64,
) -> Result<()> {
    let cache_manager = CacheManager::new()?;
    let cache_path = cache_manager.create_cache(dump_path, table_count, row_count)?;

    // Copy database to cache location
    // DuckDB supports EXPORT DATABASE but for caching we use ATTACH and copy
    engine
        .connection()
        .execute("CHECKPOINT", [])
        .context("Failed to checkpoint database")?;

    // For in-memory databases, we need to export
    // Try to attach and copy to the cache database
    engine
        .connection()
        .execute(
            &format!("ATTACH '{}' AS cache_db", cache_path.display()),
            [],
        )
        .context("Failed to attach cache database")?;

    // Copy all tables
    let tables = engine.list_tables()?;
    for table in tables {
        engine
            .connection()
            .execute(
                &format!(
                    "CREATE TABLE cache_db.\"{}\" AS SELECT * FROM main.\"{}\"",
                    table, table
                ),
                [],
            )
            .with_context(|| format!("Failed to copy table {} to cache", table))?;
    }
    engine
        .connection()
        .execute("DETACH cache_db", [])
        .context("Failed to detach cache database")?;

    eprintln!("Cached database to: {}", cache_path.display());
    Ok(())
}

/// Clear all cached databases
fn clear_cache() -> Result<()> {
    let cache_manager = CacheManager::new()?;
    let count = cache_manager.clear_all()?;
    eprintln!("Cleared {} cached database(s)", count);
    Ok(())
}

/// List all cached databases
fn list_cache() -> Result<()> {
    let cache_manager = CacheManager::new()?;
    let entries = cache_manager.list_entries()?;

    if entries.is_empty() {
        println!("No cached databases found.");
        println!("Cache directory: {}", cache_manager.cache_dir().display());
        return Ok(());
    }

    println!("Cached databases:");
    println!();

    for entry in &entries {
        let cache_size_mb = entry.cache_size as f64 / (1024.0 * 1024.0);
        let dump_size_mb = entry.dump_size as f64 / (1024.0 * 1024.0);

        println!("  {}", entry.dump_path);
        println!(
            "    Tables: {}, Rows: {}, Cache: {:.1} MB (Dump: {:.1} MB)",
            entry.table_count, entry.row_count, cache_size_mb, dump_size_mb
        );
        println!("    Key: {}", entry.cache_key);
        println!();
    }

    let total_mb = cache_manager.total_size()? as f64 / (1024.0 * 1024.0);
    println!("Total cache size: {:.1} MB", total_mb);
    println!("Cache directory: {}", cache_manager.cache_dir().display());

    Ok(())
}

/// Run the interactive REPL
fn run_repl(engine: &QueryEngine, default_format: OutputFormat, timing: bool) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    // Load history
    let history_path = dirs::cache_dir()
        .map(|d| d.join("sql-splitter").join("query_history"))
        .unwrap_or_else(|| PathBuf::from(".sql_splitter_history"));

    let _ = rl.load_history(&history_path);

    println!("sql-splitter query REPL");
    println!("Type .help for available commands, .exit to quit");
    println!();

    let mut current_format = default_format;

    loop {
        let readline = rl.readline("sql> ");

        match readline {
            Ok(line) => {
                let trimmed = line.trim();

                if trimmed.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(trimmed);

                // Handle meta-commands
                if trimmed.starts_with('.') {
                    if handle_meta_command(trimmed, engine, &mut current_format)? {
                        break;
                    }
                    continue;
                }

                // Execute query
                match engine.query(trimmed) {
                    Ok(result) => {
                        let formatted = QueryResultFormatter::format(&result, current_format);
                        print!("{}", formatted);

                        if timing {
                            eprintln!("({:.3}s)", result.execution_time_secs);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    if let Some(parent) = history_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = rl.save_history(&history_path);

    Ok(())
}

/// Handle REPL meta-commands. Returns true if REPL should exit.
fn handle_meta_command(
    command: &str,
    engine: &QueryEngine,
    format: &mut OutputFormat,
) -> Result<bool> {
    let parts: Vec<&str> = command.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("");

    match cmd {
        ".exit" | ".quit" | ".q" => {
            println!("Goodbye!");
            return Ok(true);
        }
        ".help" | ".h" | ".?" => {
            println!("Available commands:");
            println!("  .tables              List all tables");
            println!("  .schema [table]      Show schema (all tables or specific table)");
            println!("  .describe <table>    Describe a specific table");
            println!("  .format <fmt>        Set output format (table, json, csv, tsv)");
            println!("  .count <table>       Count rows in a table");
            println!("  .sample <table> [n]  Show sample rows from a table (default: 10)");
            println!("  .export <file> <query>  Export query results to file");
            println!("  .exit, .quit, .q     Exit the REPL");
            println!();
        }
        ".tables" => {
            let tables = engine.list_tables()?;
            for table in tables {
                println!("  {}", table);
            }
        }
        ".schema" => {
            if parts.len() > 1 {
                let table = parts[1];
                let result = engine.describe_table(table)?;
                let formatted = QueryResultFormatter::format(&result, OutputFormat::Table);
                print!("{}", formatted);
            } else {
                // Show all tables with their columns
                let tables = engine.list_tables()?;
                for table in tables {
                    println!("{}:", table);
                    if let Ok(result) = engine.describe_table(&table) {
                        for row in &result.rows {
                            if row.len() >= 2 {
                                println!("  {} {}", row[0], row[1]);
                            }
                        }
                    }
                    println!();
                }
            }
        }
        ".describe" | ".desc" => {
            if parts.len() < 2 {
                eprintln!("Usage: .describe <table>");
            } else {
                let table = parts[1];
                let result = engine.describe_table(table)?;
                let formatted = QueryResultFormatter::format(&result, OutputFormat::Table);
                print!("{}", formatted);
            }
        }
        ".format" => {
            if parts.len() < 2 {
                println!("Current format: {}", format);
                println!("Usage: .format <table|json|jsonl|csv|tsv>");
            } else {
                match parts[1].parse::<OutputFormat>() {
                    Ok(new_format) => {
                        *format = new_format;
                        println!("Output format set to: {}", format);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
        }
        ".count" => {
            if parts.len() < 2 {
                eprintln!("Usage: .count <table>");
            } else {
                let table = parts[1];
                let result =
                    engine.query(&format!("SELECT COUNT(*) as count FROM \"{}\"", table))?;
                if let Some(row) = result.rows.first() {
                    println!("{}", row[0]);
                }
            }
        }
        ".sample" => {
            if parts.len() < 2 {
                eprintln!("Usage: .sample <table> [n]");
            } else {
                let table = parts[1];
                let limit: usize = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
                let result =
                    engine.query(&format!("SELECT * FROM \"{}\" LIMIT {}", table, limit))?;
                let formatted = QueryResultFormatter::format(&result, *format);
                print!("{}", formatted);
            }
        }
        ".export" => {
            if parts.len() < 3 {
                eprintln!("Usage: .export <file> <query>");
            } else {
                let file_path = parts[1];
                let query = parts[2..].join(" ");

                match engine.query(&query) {
                    Ok(result) => {
                        // Detect format from extension
                        let export_format = if file_path.ends_with(".json") {
                            OutputFormat::Json
                        } else if file_path.ends_with(".csv") {
                            OutputFormat::Csv
                        } else if file_path.ends_with(".tsv") {
                            OutputFormat::Tsv
                        } else {
                            *format
                        };

                        let formatted = QueryResultFormatter::format(&result, export_format);
                        std::fs::write(file_path, formatted)?;
                        println!("Exported {} rows to {}", result.row_count(), file_path);
                    }
                    Err(e) => {
                        eprintln!("Query error: {}", e);
                    }
                }
            }
        }
        _ => {
            eprintln!(
                "Unknown command: {}. Type .help for available commands.",
                cmd
            );
        }
    }

    Ok(false)
}
