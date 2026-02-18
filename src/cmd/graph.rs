//! Graph command implementation for ERD generation.

use crate::graph::{
    cyclic_tables, find_cycles, to_dot, to_html, to_json, to_mermaid, Cycle, GraphView, Layout,
    OutputFormat,
};
use crate::parser::{detect_dialect_from_file, Parser, StatementType};
use crate::schema::{SchemaBuilder, SchemaGraph};
use crate::splitter::Compression;
use anyhow::{bail, Result};
use glob::Pattern;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Run the graph command
#[allow(clippy::too_many_arguments)]
pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    format: Option<String>,
    dialect: Option<String>,
    layout: Option<String>,
    cycles_only: bool,
    table: Option<String>,
    transitive: bool,
    reverse: bool,
    tables: Option<String>,
    exclude: Option<String>,
    max_depth: Option<usize>,
    render: bool,
    _progress: bool,
    json: bool,
) -> Result<()> {
    // Parse format
    let format = if json {
        OutputFormat::Json
    } else if let Some(ref f) = format {
        f.parse().map_err(|e| anyhow::anyhow!("{}", e))?
    } else if let Some(ref out) = output {
        // Detect from output extension
        out.extension()
            .and_then(|e| e.to_str())
            .and_then(OutputFormat::from_extension)
            .unwrap_or(OutputFormat::Html)
    } else {
        OutputFormat::Html // Default to HTML for best experience
    };

    // Parse layout
    let layout = layout
        .map(|l| l.parse())
        .transpose()
        .map_err(|e| anyhow::anyhow!("{}", e))?
        .unwrap_or(Layout::LR);

    // Parse table filters
    let tables_filter: Option<Vec<String>> =
        tables.map(|t| t.split(',').map(|s| s.trim().to_string()).collect());
    let exclude_filter: Option<Vec<String>> =
        exclude.map(|e| e.split(',').map(|s| s.trim().to_string()).collect());

    if !file.exists() {
        bail!("input file does not exist: {}", file.display());
    }

    // Detect dialect
    let compression = Compression::from_path(&file);
    let dialect = resolve_dialect(&file, dialect, compression)?;

    if !matches!(format, OutputFormat::Json) {
        eprintln!("Generating ERD: {} [dialect: {}]", file.display(), dialect);
    }

    // Build schema graph from file
    let graph = build_schema_graph_from_file(&file, dialect, compression)?;

    if graph.is_empty() {
        if !matches!(format, OutputFormat::Json) {
            eprintln!("No tables found in the file.");
        }
        return Ok(());
    }

    // Create graph view
    let mut view = GraphView::from_schema_graph(&graph);

    // Apply filters
    if let Some(ref tables) = tables_filter {
        let patterns: Vec<Pattern> = tables.iter().filter_map(|t| Pattern::new(t).ok()).collect();
        view.filter_tables(&patterns);
    }

    if let Some(ref exclude) = exclude_filter {
        let patterns: Vec<Pattern> = exclude
            .iter()
            .filter_map(|e| Pattern::new(e).ok())
            .collect();
        view.exclude_tables(&patterns);
    }

    // Focus on specific table if requested
    if let Some(ref table) = table {
        view.focus_table(table, transitive, reverse, max_depth);
    }

    // Filter to cyclic tables only if requested
    let cycles = if cycles_only {
        let cycles = find_cycles(&view);
        let cyclic = cyclic_tables(&view);
        view.filter_to_cyclic_tables(&cyclic);
        Some(cycles)
    } else {
        None
    };

    // Generate output
    let title = file
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| format!("ERD - {}", n))
        .unwrap_or_else(|| "Entity Relationship Diagram".to_string());

    let output_content = match format {
        OutputFormat::Dot => to_dot(&view, layout),
        OutputFormat::Mermaid => to_mermaid(&view),
        OutputFormat::Json => to_json(&view),
        OutputFormat::Html => to_html(&view, &title),
    };

    // Handle rendering to PNG/SVG if requested
    let should_render = render
        || output
            .as_ref()
            .and_then(|p| p.extension())
            .and_then(|e| e.to_str())
            .map(|e| matches!(e.to_lowercase().as_str(), "png" | "svg" | "pdf"))
            .unwrap_or(false);

    if should_render && format == OutputFormat::Dot {
        if let Some(ref out_path) = output {
            return render_with_graphviz(&output_content, out_path);
        }
    }

    // Write output
    if let Some(ref out_path) = output {
        let mut file = File::create(out_path)?;
        file.write_all(output_content.as_bytes())?;
        eprintln!("ERD written to: {}", out_path.display());
    } else {
        println!("{}", output_content);
    }

    // Print cycle info if cycles-only mode
    if let Some(cycles) = cycles {
        if !cycles.is_empty() {
            eprintln!("\nCycles detected ({}):", cycles.len());
            for (i, cycle) in cycles.iter().enumerate() {
                eprintln!("  {}. {}", i + 1, cycle.display());
            }
        } else {
            eprintln!("\nNo cycles detected.");
        }
    }

    // Print summary if not JSON
    if !matches!(format, OutputFormat::Json) {
        let total_columns: usize = view.sorted_tables().iter().map(|t| t.columns.len()).sum();
        eprintln!(
            "\nERD: {} tables, {} columns, {} relationships",
            view.table_count(),
            total_columns,
            view.edge_count()
        );
    }

    Ok(())
}

/// Build schema graph from a SQL file
fn build_schema_graph_from_file(
    path: &Path,
    dialect: crate::parser::SqlDialect,
    compression: Compression,
) -> Result<SchemaGraph> {
    let file = File::open(path)?;
    let reader: Box<dyn Read> = if compression != Compression::None {
        compression.wrap_reader(Box::new(file))?
    } else {
        Box::new(file)
    };

    let mut parser = Parser::with_dialect(reader, 64 * 1024, dialect);
    let mut builder = SchemaBuilder::new();

    while let Some(stmt) = parser.read_statement()? {
        let stmt_str = String::from_utf8_lossy(&stmt);
        let (stmt_type, _) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

        match stmt_type {
            StatementType::CreateTable => {
                builder.parse_create_table(&stmt_str);
            }
            StatementType::AlterTable => {
                builder.parse_alter_table(&stmt_str);
            }
            StatementType::CreateIndex => {
                builder.parse_create_index(&stmt_str);
            }
            _ => {}
        }
    }

    Ok(SchemaGraph::from_schema(builder.build()))
}

/// Render DOT to PNG/SVG/PDF using Graphviz
fn render_with_graphviz(dot_source: &str, output_path: &Path) -> Result<()> {
    let ext = output_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("png");

    let format_arg = format!("-T{}", ext);

    let mut child = Command::new("dot")
        .arg(&format_arg)
        .arg("-o")
        .arg(output_path)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "Graphviz 'dot' command not found. Install Graphviz or use --format html instead."
                )
            } else {
                anyhow::anyhow!("Failed to run dot: {}", e)
            }
        })?;

    if let Some(ref mut stdin) = child.stdin {
        stdin.write_all(dot_source.as_bytes())?;
    }

    let status = child.wait()?;
    if !status.success() {
        bail!("Graphviz dot command failed with status: {}", status);
    }

    eprintln!("Rendered to: {}", output_path.display());
    Ok(())
}

/// Resolve SQL dialect
fn resolve_dialect(
    file: &Path,
    dialect: Option<String>,
    compression: Compression,
) -> Result<crate::parser::SqlDialect> {
    use crate::parser::{detect_dialect, DialectConfidence};

    match dialect {
        Some(d) => d.parse().map_err(|e: String| anyhow::anyhow!(e)),
        None => {
            let result = if compression != Compression::None {
                let file_handle = File::open(file)?;
                let mut reader = compression.wrap_reader(Box::new(file_handle))?;
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

/// Display cycles in a user-friendly format
#[allow(dead_code)]
pub fn display_cycles(cycles: &[Cycle]) {
    if cycles.is_empty() {
        eprintln!("No cycles detected in the schema.");
        return;
    }

    eprintln!("Cycles detected ({}):", cycles.len());
    for (i, cycle) in cycles.iter().enumerate() {
        eprintln!("  {}. {}", i + 1, cycle.display());
    }
}
