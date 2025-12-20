mod analyze;
mod split;

use crate::parser::SqlDialect;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sql-splitter")]
#[command(author = "Helge Sverre <helge.sverre@gmail.com>")]
#[command(version)]
#[command(about = "Split large SQL dump files into individual table files", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Split a SQL file into individual table files
    Split {
        /// Input SQL file
        file: PathBuf,

        /// Output directory for split files
        #[arg(short, long, default_value = "output")]
        output: PathBuf,

        /// SQL dialect: mysql, postgres, or sqlite
        #[arg(short, long, default_value = "mysql")]
        dialect: String,

        /// Verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Preview without writing files (dry run)
        #[arg(long)]
        dry_run: bool,

        /// Show progress during processing
        #[arg(short, long)]
        progress: bool,

        /// Only split specific tables (comma-separated)
        #[arg(short, long)]
        tables: Option<String>,
    },

    /// Analyze a SQL file and display statistics
    Analyze {
        /// Input SQL file
        file: PathBuf,

        /// SQL dialect: mysql, postgres, or sqlite
        #[arg(short, long, default_value = "mysql")]
        dialect: String,

        /// Show progress during analysis
        #[arg(short, long)]
        progress: bool,
    },
}

pub fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Split {
            file,
            output,
            dialect,
            verbose,
            dry_run,
            progress,
            tables,
        } => {
            let dialect: SqlDialect = dialect.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            split::run(file, output, dialect, verbose, dry_run, progress, tables)
        }
        Commands::Analyze {
            file,
            dialect,
            progress,
        } => {
            let dialect: SqlDialect = dialect.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            analyze::run(file, dialect, progress)
        }
    }
}
