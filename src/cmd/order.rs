//! Order command - output SQL dump with tables in topological order.

use super::common::{BEHAVIOR, INPUT_OUTPUT};
use crate::parser::{Parser, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph};
use ahash::AHashMap;
use anyhow::{bail, Result};
use clap::{Args, ValueHint};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Args)]
pub struct OrderArgs {
    /// Input SQL file (supports .gz, .bz2, .xz, .zst, .zip)
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output file (default: stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Verify ordering without writing output
    #[arg(long, help_heading = BEHAVIOR)]
    check: bool,

    /// Show order without rewriting the file
    #[arg(long, help_heading = BEHAVIOR)]
    dry_run: bool,

    /// Order children before parents (for DROP operations)
    #[arg(long, help_heading = BEHAVIOR)]
    reverse: bool,
}

/// Run the order command
pub fn run(args: OrderArgs) -> Result<ExitCode> {
    let OrderArgs {
        file,
        output,
        dialect,
        check,
        dry_run,
        reverse,
    } = args;
    let output = super::common::dash_is_stdout(output);

    if !file.exists() {
        bail!("input file does not exist: {}", file.display());
    }

    let sql_dialect = super::common::resolve_dialect(&file, dialect.as_deref(), false)?;

    eprintln!("Analyzing schema for topological order...");

    // First pass: build schema graph
    let (graph, table_statements, other_statements) = collect_statements(&file, sql_dialect)?;

    if graph.is_empty() {
        eprintln!("No tables found in the file.");
        return Ok(ExitCode::SUCCESS);
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
            return Ok(ExitCode::FAILURE);
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
        return Ok(ExitCode::SUCCESS);
    }

    // Dry run: just show the order
    if dry_run {
        eprintln!("\nTopological order ({} tables):", ordered_tables.len());
        for (i, table) in ordered_tables.iter().enumerate() {
            eprintln!("  {}. {}", i + 1, table);
        }
        return Ok(ExitCode::SUCCESS);
    }

    // Write output
    let writer: Box<dyn Write> = if let Some(ref out_path) = output {
        Box::new(BufWriter::new(File::create(out_path)?))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };

    write_ordered_output(
        writer,
        sql_dialect,
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

    Ok(ExitCode::SUCCESS)
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
    path: &std::path::Path,
    dialect: crate::parser::SqlDialect,
) -> Result<(SchemaGraph, CollectedStatements, Vec<String>)> {
    let reader: Box<dyn Read> = crate::splitter::open_input(path)?;

    let mut parser = Parser::with_dialect(reader, 64 * 1024, dialect);
    let mut builder = SchemaBuilder::new();

    let mut collected = CollectedStatements {
        create_statements: AHashMap::new(),
        insert_statements: AHashMap::new(),
        other_table_statements: AHashMap::new(),
    };
    let mut other_statements: Vec<String> = Vec::new();
    // `read_statement` emits PostgreSQL COPY headers and their data blocks as
    // separate statements. Keep the block with its header so reordering never
    // puts COPY rows before the command that consumes them.
    let mut pending_copy_table: Option<String> = None;

    while let Some(stmt) = parser.read_statement()? {
        let stmt_str = String::from_utf8_lossy(&stmt).to_string();
        let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

        if crate::copy_data::is_copy_data_block(&stmt) {
            if let Some(table_name) = pending_copy_table.take() {
                if let Some(copy_statement) = collected
                    .insert_statements
                    .get_mut(&table_name)
                    .and_then(|statements| statements.last_mut())
                {
                    copy_statement.push_str(&stmt_str);
                    continue;
                }
            }
        }
        pending_copy_table = None;

        // Feed DDL into the schema builder (no-op for other statement types)
        builder.ingest(stmt_type, &stmt_str);

        match stmt_type {
            StatementType::CreateTable => {
                if !table_name.is_empty() {
                    collected.create_statements.insert(table_name, stmt_str);
                }
            }
            StatementType::AlterTable => {
                if !table_name.is_empty() {
                    collected
                        .other_table_statements
                        .entry(table_name)
                        .or_default()
                        .push(stmt_str);
                }
            }
            StatementType::CreateIndex => {
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
                    let copy_table = (stmt_type == StatementType::Copy).then(|| table_name.clone());
                    collected
                        .insert_statements
                        .entry(table_name)
                        .or_default()
                        .push(stmt_str);
                    pending_copy_table = copy_table;
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
    mut writer: impl Write,
    dialect: crate::parser::SqlDialect,
    ordered_tables: &[String],
    table_statements: &CollectedStatements,
    other_statements: &[String],
) -> Result<()> {
    // Write header/other statements first
    for stmt in other_statements {
        write_statement(&mut writer, stmt, dialect)?;
    }

    if !other_statements.is_empty() {
        writeln!(writer)?;
    }

    // Write table statements in topological order
    for table in ordered_tables {
        // CREATE TABLE
        if let Some(create) = table_statements.create_statements.get(table) {
            write_statement(&mut writer, create, dialect)?;
            writeln!(writer)?;
        }

        // ALTER TABLE, CREATE INDEX, etc.
        if let Some(others) = table_statements.other_table_statements.get(table) {
            for stmt in others {
                write_statement(&mut writer, stmt, dialect)?;
            }
            if !others.is_empty() {
                writeln!(writer)?;
            }
        }

        // INSERT statements
        if let Some(inserts) = table_statements.insert_statements.get(table) {
            for stmt in inserts {
                write_statement(&mut writer, stmt, dialect)?;
            }
            if !inserts.is_empty() {
                writeln!(writer)?;
            }
        }
    }

    writer.flush()?;
    Ok(())
}

/// Write one parsed statement, restoring SQL Server batch separators that the
/// parser consumes while reading the source dump.
fn write_statement(
    writer: &mut dyn Write,
    statement: &str,
    dialect: crate::parser::SqlDialect,
) -> Result<()> {
    writeln!(writer, "{statement}")?;
    if dialect == crate::parser::SqlDialect::Mssql {
        writeln!(writer, "GO")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mssql_output_restores_go_batch_separators() -> Result<()> {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/generate/production_shape_mssql.sql");
        let (graph, statements, other_statements) =
            collect_statements(&fixture, crate::parser::SqlDialect::Mssql)?;
        let table_order = graph
            .topo_sort()
            .order
            .iter()
            .filter_map(|id| graph.table_name(*id).map(str::to_owned))
            .collect::<Vec<_>>();
        let mut output = Vec::new();

        write_ordered_output(
            &mut output,
            crate::parser::SqlDialect::Mssql,
            &table_order,
            &statements,
            &other_statements,
        )?;

        let mut parser = Parser::with_dialect(
            output.as_slice(),
            64 * 1024,
            crate::parser::SqlDialect::Mssql,
        );
        let mut table_statements = Vec::new();
        while let Some(statement) = parser.read_statement()? {
            let (statement_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(
                &statement,
                crate::parser::SqlDialect::Mssql,
            );
            if matches!(
                statement_type,
                StatementType::CreateTable | StatementType::Insert
            ) {
                table_statements.push((statement_type, table_name));
            }
        }

        assert_eq!(
            table_statements,
            vec![
                (StatementType::CreateTable, "dbo.users".to_string()),
                (StatementType::Insert, "dbo.users".to_string()),
                (StatementType::CreateTable, "dbo.orders".to_string()),
                (StatementType::Insert, "dbo.orders".to_string()),
            ]
        );
        Ok(())
    }
}
