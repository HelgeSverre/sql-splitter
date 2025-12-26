//! Order command - output SQL dump with tables in topological order.

use crate::parser::{detect_dialect, detect_dialect_from_file, DialectConfidence, Parser, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph};
use crate::splitter::Compression;
use ahash::AHashMap;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

/// Order command options
pub struct OrderOptions {
    pub file: PathBuf,
    pub output: Option<PathBuf>,
    pub dialect: Option<String>,
    pub check: bool,
    pub dry_run: bool,
    pub reverse: bool,
}

/// Run the order command
#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    dialect: Option<String>,
    check: bool,
    dry_run: bool,
    reverse: bool,
) -> Result<()> {
    if !file.exists() {
        bail!("input file does not exist: {}", file.display());
    }

    let compression = Compression::from_path(&file);
    let sql_dialect = resolve_dialect(&file, dialect, compression)?;

    eprintln!("Analyzing schema for topological order...");

    // First pass: build schema graph
    let (graph, table_statements, other_statements) =
        collect_statements(&file, sql_dialect, compression)?;

    if graph.is_empty() {
        eprintln!("No tables found in the file.");
        return Ok(());
    }

    // Get topological order
    let topo_result = graph.topo_sort();

    if !topo_result.cyclic_tables.is_empty() {
        eprintln!("\nWarning: Circular dependencies detected!");
        eprintln!("The following tables are part of cycles:");
        for table_id in &topo_result.cyclic_tables {
            if let Some(name) = graph.table_name(*table_id) {
                eprintln!("  - {}", name);
            }
        }
        eprintln!();

        if check {
            eprintln!("Check FAILED: Cannot determine valid ordering due to cycles.");
            eprintln!("Use 'sql-splitter graph --cycles-only' to analyze cycles.");
            std::process::exit(1);
        }
    }

    // Build ordered table list
    let mut ordered_tables: Vec<String> = topo_result
        .order
        .iter()
        .filter_map(|id| graph.table_name(*id).map(|s| s.to_string()))
        .collect();

    // Add cyclic tables at the end
    for table_id in &topo_result.cyclic_tables {
        if let Some(name) = graph.table_name(*table_id) {
            ordered_tables.push(name.to_string());
        }
    }

    if reverse {
        ordered_tables.reverse();
    }

    // Check mode: just verify and report
    if check {
        eprintln!("Check PASSED: Tables can be ordered topologically.");
        eprintln!("\nSuggested order ({} tables):", ordered_tables.len());
        for (i, table) in ordered_tables.iter().enumerate() {
            eprintln!("  {}. {}", i + 1, table);
        }
        return Ok(());
    }

    // Dry run: just show the order
    if dry_run {
        eprintln!("\nTopological order ({} tables):", ordered_tables.len());
        for (i, table) in ordered_tables.iter().enumerate() {
            eprintln!("  {}. {}", i + 1, table);
        }
        return Ok(());
    }

    // Write output
    let writer: Box<dyn Write> = if let Some(ref out_path) = output {
        Box::new(BufWriter::new(File::create(out_path)?))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };

    write_ordered_output(
        writer,
        &ordered_tables,
        &table_statements,
        &other_statements,
    )?;

    if let Some(ref out_path) = output {
        eprintln!("Ordered dump written to: {}", out_path.display());
    }

    eprintln!(
        "\nProcessed {} tables in topological order.",
        ordered_tables.len()
    );

    Ok(())
}

/// Collected statements from the SQL file
struct CollectedStatements {
    /// CREATE TABLE statements per table
    create_statements: AHashMap<String, String>,
    /// INSERT statements per table (in order)
    insert_statements: AHashMap<String, Vec<String>>,
    /// Other statements per table (ALTER, CREATE INDEX, etc.)
    other_table_statements: AHashMap<String, Vec<String>>,
}

/// Collect and categorize statements from the file
fn collect_statements(
    path: &PathBuf,
    dialect: crate::parser::SqlDialect,
    compression: Compression,
) -> Result<(SchemaGraph, CollectedStatements, Vec<String>)> {
    let file = File::open(path)?;
    let reader: Box<dyn Read> = if compression != Compression::None {
        compression.wrap_reader(Box::new(file))
    } else {
        Box::new(file)
    };

    let mut parser = Parser::with_dialect(reader, 64 * 1024, dialect);
    let mut builder = SchemaBuilder::new();

    let mut collected = CollectedStatements {
        create_statements: AHashMap::new(),
        insert_statements: AHashMap::new(),
        other_table_statements: AHashMap::new(),
    };
    let mut other_statements: Vec<String> = Vec::new();

    while let Some(stmt) = parser.read_statement()? {
        let stmt_str = String::from_utf8_lossy(&stmt).to_string();
        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

        match stmt_type {
            StatementType::CreateTable => {
                builder.parse_create_table(&stmt_str);
                if !table_name.is_empty() {
                    collected
                        .create_statements
                        .insert(table_name, stmt_str);
                }
            }
            StatementType::AlterTable => {
                builder.parse_alter_table(&stmt_str);
                if !table_name.is_empty() {
                    collected
                        .other_table_statements
                        .entry(table_name)
                        .or_default()
                        .push(stmt_str);
                }
            }
            StatementType::CreateIndex => {
                builder.parse_create_index(&stmt_str);
                // Try to extract table name from CREATE INDEX
                if !table_name.is_empty() {
                    collected
                        .other_table_statements
                        .entry(table_name)
                        .or_default()
                        .push(stmt_str);
                } else {
                    other_statements.push(stmt_str);
                }
            }
            StatementType::Insert | StatementType::Copy => {
                if !table_name.is_empty() {
                    collected
                        .insert_statements
                        .entry(table_name)
                        .or_default()
                        .push(stmt_str);
                }
            }
            _ => {
                // Header comments, SET statements, etc.
                other_statements.push(stmt_str);
            }
        }
    }

    let graph = SchemaGraph::from_schema(builder.build());
    Ok((graph, collected, other_statements))
}

/// Write statements in topological order
fn write_ordered_output(
    mut writer: Box<dyn Write>,
    ordered_tables: &[String],
    table_statements: &CollectedStatements,
    other_statements: &[String],
) -> Result<()> {
    // Write header/other statements first
    for stmt in other_statements {
        writeln!(writer, "{}", stmt)?;
    }

    if !other_statements.is_empty() {
        writeln!(writer)?;
    }

    // Write table statements in topological order
    for table in ordered_tables {
        // CREATE TABLE
        if let Some(create) = table_statements.create_statements.get(table) {
            writeln!(writer, "{}", create)?;
            writeln!(writer)?;
        }

        // ALTER TABLE, CREATE INDEX, etc.
        if let Some(others) = table_statements.other_table_statements.get(table) {
            for stmt in others {
                writeln!(writer, "{}", stmt)?;
            }
            if !others.is_empty() {
                writeln!(writer)?;
            }
        }

        // INSERT statements
        if let Some(inserts) = table_statements.insert_statements.get(table) {
            for stmt in inserts {
                writeln!(writer, "{}", stmt)?;
            }
            if !inserts.is_empty() {
                writeln!(writer)?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

/// Resolve SQL dialect
fn resolve_dialect(
    file: &std::path::Path,
    dialect: Option<String>,
    compression: Compression,
) -> Result<crate::parser::SqlDialect> {
    match dialect {
        Some(d) => d.parse().map_err(|e: String| anyhow::anyhow!(e)),
        None => {
            let result = if compression != Compression::None {
                let file_handle = File::open(file)?;
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
