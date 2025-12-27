//! CLI for generating test fixtures.
//!
//! Usage:
//!   # Multi-tenant schema (existing behavior)
//!   gen-fixtures --dialect mysql --scale small --seed 42 > fixtures/mysql/small.sql
//!
//!   # Simple uniform tables (streaming, low memory)
//!   gen-fixtures --dialect mysql --rows 125000 --tables 10 > large.sql

use clap::Parser;
use std::fs::File;
use std::io::{self};
use test_data_gen::{
    Dialect, MultiTenantConfig, MultiTenantGenerator, Scale, StreamingConfig, StreamingGenerator,
};

#[derive(Parser, Debug)]
#[command(name = "gen-fixtures")]
#[command(about = "Generate SQL test fixtures for sql-splitter", long_about = None)]
struct Args {
    /// SQL dialect: mysql, postgres, sqlite, mssql
    #[arg(short, long, default_value = "mysql")]
    dialect: String,

    /// Scale preset: small, medium, large, xlarge (multi-tenant schema)
    /// Ignored if --rows is specified
    #[arg(short, long, default_value = "small")]
    scale: String,

    /// Random seed for reproducibility
    #[arg(long, default_value = "12345")]
    seed: u64,

    /// Output file (default: stdout)
    #[arg(short, long)]
    output: Option<String>,

    /// Use INSERT instead of COPY for PostgreSQL
    #[arg(long)]
    no_copy: bool,

    /// Skip CREATE TABLE statements
    #[arg(long)]
    data_only: bool,

    /// Skip data, output schema only
    #[arg(long)]
    schema_only: bool,

    /// Rows per table (enables simple streaming mode, ignores --scale)
    #[arg(long)]
    rows: Option<usize>,

    /// Number of tables (only with --rows, default: 10)
    #[arg(long, default_value = "10")]
    tables: usize,

    /// Batch size for INSERT statements (default: 100)
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Skip foreign key constraints in simple mode
    #[arg(long)]
    no_fk: bool,

    /// Use GO batch separators for MSSQL (production-style output)
    #[arg(long)]
    go_separator: bool,

    /// Use [dbo]. schema prefix for MSSQL (production-style output)
    #[arg(long)]
    schema_prefix: bool,

    /// Use named CONSTRAINT syntax for MSSQL (e.g., CONSTRAINT [PK_table])
    #[arg(long)]
    named_constraints: bool,

    /// Enable all MSSQL production-style options (--go-separator --schema-prefix --named-constraints)
    #[arg(long)]
    mssql_production: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dialect: Dialect = args
        .dialect
        .parse()
        .map_err(|e: String| anyhow::anyhow!(e))?;

    // If --rows is specified, use simple streaming mode
    if let Some(rows) = args.rows {
        return run_streaming_mode(&args, dialect, rows);
    }

    // Otherwise, use the multi-tenant schema generator
    run_schema_mode(&args, dialect)
}

/// Simple streaming mode: uniform tables with specified row count
fn run_streaming_mode(args: &Args, dialect: Dialect, rows: usize) -> anyhow::Result<()> {
    let config = StreamingConfig {
        dialect,
        rows_per_table: rows,
        num_tables: args.tables,
        seed: args.seed,
        batch_size: args.batch_size,
        include_schema: !args.data_only,
        include_fk: !args.no_fk,
        use_go_separator: args.go_separator || args.mssql_production,
        use_schema_prefix: args.schema_prefix || args.mssql_production,
        use_named_constraints: args.named_constraints || args.mssql_production,
    };

    let mut gen = StreamingGenerator::new(config);

    if let Some(ref path) = args.output {
        let file = File::create(path)?;
        gen.generate(file)?;
        eprintln!(
            "Generated {} tables × {} rows to {}",
            args.tables, rows, path
        );
    } else {
        let stdout = io::stdout();
        gen.generate(stdout.lock())?;
    }

    Ok(())
}

/// Multi-tenant schema mode (streaming for low memory)
fn run_schema_mode(args: &Args, dialect: Dialect) -> anyhow::Result<()> {
    let scale: Scale = args.scale.parse().map_err(|e: String| anyhow::anyhow!(e))?;

    let config = MultiTenantConfig {
        dialect,
        scale,
        seed: args.seed,
        batch_size: args.batch_size,
        include_schema: !args.data_only,
        use_go_separator: args.go_separator || args.mssql_production,
        use_schema_prefix: args.schema_prefix || args.mssql_production,
        use_named_constraints: args.named_constraints || args.mssql_production,
    };

    let mut gen = MultiTenantGenerator::new(config);

    if let Some(ref path) = args.output {
        let file = File::create(path)?;
        gen.generate(file)?;
        eprintln!("Generated {} to {}", scale_description(scale), path);
    } else {
        let stdout = io::stdout();
        gen.generate(stdout.lock())?;
    }

    Ok(())
}

fn scale_description(scale: Scale) -> &'static str {
    match scale {
        Scale::Small => "small fixture (~500 rows)",
        Scale::Medium => "medium fixture (~10K rows)",
        Scale::Large => "large fixture (~200K rows)",
        Scale::XLarge => "xlarge fixture (~1M rows)",
    }
}
