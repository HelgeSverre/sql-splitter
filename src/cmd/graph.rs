//! Graph command implementation for ERD generation.

use super::common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, OUTPUT_FORMAT};
use crate::graph::{
    cyclic_tables, find_cycles, to_dot, to_html, to_json, to_mermaid, GraphView, Layout,
    OutputFormat,
};
use crate::schema::{Schema, SchemaGraph};
use anyhow::{bail, Result};
use clap::{Args, ValueHint};
use glob::Pattern;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Args)]
pub struct GraphArgs {
    /// Input SQL file (supports .gz, .bz2, .xz, .zst)
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    file: PathBuf,

    /// Output file (.html, .dot, .mmd, .json, .png, .svg)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// Output format: html, dot, mermaid, json
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    format: Option<String>,

    /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
    #[arg(short, long, help_heading = INPUT_OUTPUT)]
    dialect: Option<String>,

    /// Layout direction: lr (left-right), tb (top-bottom)
    #[arg(long, default_value = "lr", help_heading = OUTPUT_FORMAT)]
    layout: Option<String>,

    /// Show only tables involved in circular dependencies
    #[arg(long, help_heading = FILTERING)]
    cycles_only: bool,

    /// Focus on a specific table
    #[arg(long, help_heading = FILTERING)]
    table: Option<String>,

    /// Show transitive dependencies (parents of parents)
    #[arg(long, help_heading = FILTERING)]
    transitive: bool,

    /// Show reverse dependencies (who references this table)
    #[arg(long, help_heading = FILTERING)]
    reverse: bool,

    /// Only include these tables (comma-separated, supports globs)
    #[arg(short, long, help_heading = FILTERING)]
    tables: Option<String>,

    /// Exclude these tables (comma-separated, supports globs)
    #[arg(short, long, help_heading = FILTERING)]
    exclude: Option<String>,

    /// Maximum depth for transitive dependencies
    #[arg(long, help_heading = FILTERING)]
    max_depth: Option<usize>,

    /// Render DOT to PNG/SVG using Graphviz
    #[arg(long, help_heading = BEHAVIOR)]
    render: bool,

    /// Show progress bar
    #[arg(short, long, help_heading = OUTPUT_FORMAT)]
    progress: bool,

    /// Output results as JSON
    #[arg(long, help_heading = OUTPUT_FORMAT)]
    json: bool,
}

/// Run the graph command
pub fn run(args: GraphArgs) -> Result<()> {
    let GraphArgs {
        file,
        output,
        format,
        dialect,
        layout,
        cycles_only,
        table,
        transitive,
        reverse,
        tables,
        exclude,
        max_depth,
        render,
        progress: _progress,
        json,
    } = args;

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
    let dialect = super::common::resolve_dialect(&file, dialect.as_deref(), false)?;

    if !matches!(format, OutputFormat::Json) {
        eprintln!("Generating ERD: {} [dialect: {}]", file.display(), dialect);
    }

    // Build schema graph from file
    let graph = build_schema_graph_from_file(&file, dialect)?;

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
) -> Result<SchemaGraph> {
    Ok(SchemaGraph::from_schema(Schema::from_sql_file(
        path, dialect, None,
    )?))
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
