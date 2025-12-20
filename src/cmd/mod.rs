mod analyze;
mod split;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use std::io;
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
        /// Input SQL file (supports .gz, .bz2, .xz, .zst compression)
        file: PathBuf,

        /// Output directory for split files
        #[arg(short, long, default_value = "output")]
        output: PathBuf,

        /// SQL dialect: mysql, postgres, or sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

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

        /// Only include schema statements (CREATE TABLE, CREATE INDEX, ALTER TABLE, DROP TABLE)
        #[arg(long, conflicts_with = "data_only")]
        schema_only: bool,

        /// Only include data statements (INSERT, COPY)
        #[arg(long, conflicts_with = "schema_only")]
        data_only: bool,
    },

    /// Analyze a SQL file and display statistics
    Analyze {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst compression)
        file: PathBuf,

        /// SQL dialect: mysql, postgres, or sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

        /// Show progress during analysis
        #[arg(short, long)]
        progress: bool,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
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
            schema_only,
            data_only,
        } => split::run(
            file,
            output,
            dialect,
            verbose,
            dry_run,
            progress,
            tables,
            schema_only,
            data_only,
        ),
        Commands::Analyze {
            file,
            dialect,
            progress,
        } => analyze::run(file, dialect, progress),
        Commands::Completions { shell } => {
            generate(
                shell,
                &mut Cli::command(),
                "sql-splitter",
                &mut io::stdout(),
            );
            Ok(())
        }
    }
}
