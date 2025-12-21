//! CLI for generating test fixtures.
//!
//! Usage:
//!   gen-fixtures --dialect mysql --scale small --seed 42 > fixtures/mysql/small.sql

use clap::Parser;
use std::io::{self, Write};
use test_data_gen::{Dialect, Generator, RenderConfig, Renderer, Scale};

#[derive(Parser, Debug)]
#[command(name = "gen-fixtures")]
#[command(about = "Generate SQL test fixtures for sql-splitter", long_about = None)]
struct Args {
    /// SQL dialect: mysql, postgres, sqlite
    #[arg(short, long, default_value = "mysql")]
    dialect: String,

    /// Scale: small, medium, large
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
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dialect: Dialect = args
        .dialect
        .parse()
        .map_err(|e: String| anyhow::anyhow!(e))?;
    let scale: Scale = args.scale.parse().map_err(|e: String| anyhow::anyhow!(e))?;

    // Generate data
    let mut gen = Generator::new(args.seed, scale);
    let data = gen.generate();

    // Configure renderer
    let config = match dialect {
        Dialect::MySql => RenderConfig::mysql(),
        Dialect::Postgres => {
            if args.no_copy {
                RenderConfig::postgres_inserts()
            } else {
                RenderConfig::postgres()
            }
        }
        Dialect::Sqlite => RenderConfig::sqlite(),
    };

    let config = RenderConfig {
        include_schema: !args.data_only,
        ..config
    };

    let renderer = Renderer::new(config);

    // Render to output
    if let Some(path) = args.output {
        let file = std::fs::File::create(&path)?;
        let mut writer = io::BufWriter::new(file);

        if args.schema_only {
            // For schema-only, we'd need to modify the renderer
            // For now, just render with empty data
            let schema_only_data = test_data_gen::GeneratedData {
                tables: data
                    .tables
                    .iter()
                    .map(|t| test_data_gen::TableData {
                        table_name: t.table_name.clone(),
                        columns: t.columns.clone(),
                        rows: vec![],
                    })
                    .collect(),
            };
            renderer.render(&schema_only_data, &mut writer)?;
        } else {
            renderer.render(&data, &mut writer)?;
        }

        writer.flush()?;
        eprintln!("Generated {} to {}", scale_description(scale), path);
    } else {
        let stdout = io::stdout();
        let mut writer = stdout.lock();

        if args.schema_only {
            let schema_only_data = test_data_gen::GeneratedData {
                tables: data
                    .tables
                    .iter()
                    .map(|t| test_data_gen::TableData {
                        table_name: t.table_name.clone(),
                        columns: t.columns.clone(),
                        rows: vec![],
                    })
                    .collect(),
            };
            renderer.render(&schema_only_data, &mut writer)?;
        } else {
            renderer.render(&data, &mut writer)?;
        }
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
