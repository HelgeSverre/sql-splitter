mod analyze;
mod convert;
mod glob_util;
mod merge;
mod sample;
mod shard;
mod split;
mod validate;

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
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        /// Supports .gz, .bz2, .xz, .zst compression
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

        /// Stop on first file that fails (for glob patterns)
        #[arg(long)]
        fail_fast: bool,
    },

    /// Analyze a SQL file and display statistics
    Analyze {
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        /// Supports .gz, .bz2, .xz, .zst compression
        file: PathBuf,

        /// SQL dialect: mysql, postgres, or sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

        /// Show progress during analysis
        #[arg(short, long)]
        progress: bool,

        /// Stop on first file that fails (for glob patterns)
        #[arg(long)]
        fail_fast: bool,
    },

    /// Merge split SQL files back into a single file
    Merge {
        /// Directory containing split SQL files
        input_dir: PathBuf,

        /// Output SQL file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, or sqlite
        #[arg(short, long, default_value = "mysql")]
        dialect: Option<String>,

        /// Only merge specific tables (comma-separated)
        #[arg(short, long)]
        tables: Option<String>,

        /// Exclude specific tables (comma-separated)
        #[arg(short, long)]
        exclude: Option<String>,

        /// Wrap output in a transaction
        #[arg(long)]
        transaction: bool,

        /// Skip header comments
        #[arg(long)]
        no_header: bool,

        /// Show progress during merging
        #[arg(short, long)]
        progress: bool,

        /// Preview without writing files (dry run)
        #[arg(long)]
        dry_run: bool,
    },

    /// Sample a subset of rows from a SQL dump while preserving FK integrity
    Sample {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst compression)
        file: PathBuf,

        /// Output SQL file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

        /// Sample percentage (1-100) - mutually exclusive with --rows
        #[arg(long, conflicts_with = "rows")]
        percent: Option<u32>,

        /// Sample fixed number of rows per table - mutually exclusive with --percent
        #[arg(long, conflicts_with = "percent")]
        rows: Option<usize>,

        /// Preserve foreign key relationships (filter rows that reference missing parents)
        #[arg(long)]
        preserve_relations: bool,

        /// Only sample specific tables (comma-separated)
        #[arg(short, long)]
        tables: Option<String>,

        /// Exclude specific tables (comma-separated)
        #[arg(short, long)]
        exclude: Option<String>,

        /// Explicit root tables for sampling (comma-separated)
        #[arg(long)]
        root_tables: Option<String>,

        /// How to handle global/lookup tables: none, lookups, all
        #[arg(long, default_value = "lookups")]
        include_global: Option<String>,

        /// Random seed for reproducibility
        #[arg(long)]
        seed: Option<u64>,

        /// YAML config file for per-table settings
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Maximum total rows to sample (explosion guard). Use 0 or --no-limit to disable.
        #[arg(long)]
        max_total_rows: Option<usize>,

        /// Disable row limit (equivalent to --max-total-rows=0)
        #[arg(long)]
        no_limit: bool,

        /// Fail if any FK integrity issues detected
        #[arg(long)]
        strict_fk: bool,

        /// Exclude CREATE TABLE statements from output
        #[arg(long)]
        no_schema: bool,

        /// Show progress during sampling
        #[arg(short, long)]
        progress: bool,

        /// Preview without writing files (dry run)
        #[arg(long)]
        dry_run: bool,
    },

    /// Extract tenant-specific data from a multi-tenant SQL dump
    Shard {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst compression)
        file: PathBuf,

        /// Output SQL file (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

        /// Column name for tenant identification (auto-detected if not specified)
        #[arg(long)]
        tenant_column: Option<String>,

        /// Tenant value to extract (use this OR --tenant-values)
        #[arg(long, conflicts_with = "tenant_values")]
        tenant_value: Option<String>,

        /// Multiple tenant values to extract (comma-separated, outputs to directory)
        #[arg(long, conflicts_with = "tenant_value")]
        tenant_values: Option<String>,

        /// Explicit root tables that have the tenant column (comma-separated)
        #[arg(long)]
        root_tables: Option<String>,

        /// How to handle global/lookup tables: none, lookups, all
        #[arg(long, default_value = "lookups")]
        include_global: Option<String>,

        /// YAML config file for table classification overrides
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Maximum rows to select (memory guard). Use 0 or --no-limit to disable.
        #[arg(long)]
        max_selected_rows: Option<usize>,

        /// Disable row limit (equivalent to --max-selected-rows=0)
        #[arg(long)]
        no_limit: bool,

        /// Fail if any FK integrity issues detected
        #[arg(long)]
        strict_fk: bool,

        /// Exclude CREATE TABLE statements from output
        #[arg(long)]
        no_schema: bool,

        /// Show progress during sharding
        #[arg(short, long)]
        progress: bool,

        /// Preview without writing files (dry run)
        #[arg(long)]
        dry_run: bool,
    },

    /// Convert a SQL dump between dialects (MySQL, PostgreSQL, SQLite)
    Convert {
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        /// Supports .gz, .bz2, .xz, .zst compression
        file: PathBuf,

        /// Output SQL file or directory (default: stdout for single file, required for glob)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Source dialect: mysql, postgres, sqlite (auto-detected if not specified)
        #[arg(long)]
        from: Option<String>,

        /// Target dialect: mysql, postgres, sqlite (required)
        #[arg(long)]
        to: String,

        /// Strict mode: fail on any unsupported feature
        #[arg(long)]
        strict: bool,

        /// Show progress during conversion
        #[arg(short, long)]
        progress: bool,

        /// Preview without writing files (dry run)
        #[arg(long)]
        dry_run: bool,

        /// Stop on first file that fails (for glob patterns)
        #[arg(long)]
        fail_fast: bool,
    },

    /// Validate a SQL dump for structural and data integrity issues
    Validate {
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        /// Supports .gz, .bz2, .xz, .zst compression
        file: PathBuf,

        /// SQL dialect: mysql, postgres, sqlite (auto-detected if not specified)
        #[arg(short, long)]
        dialect: Option<String>,

        /// Show progress during validation
        #[arg(short, long)]
        progress: bool,

        /// Treat warnings as errors (non-zero exit on any warning)
        #[arg(long)]
        strict: bool,

        /// Output results as JSON instead of human-readable text
        #[arg(long)]
        json: bool,

        /// Maximum rows per table for heavy checks (PK/FK). Use 0 or --no-limit to disable.
        #[arg(long, default_value = "1000000")]
        max_rows_per_table: usize,

        /// Disable row limit for PK/FK checks (equivalent to --max-rows-per-table=0)
        #[arg(long)]
        no_limit: bool,

        /// Disable PK/FK data integrity checks
        #[arg(long)]
        no_fk_checks: bool,

        /// Stop on first file that fails validation (for glob patterns)
        #[arg(long)]
        fail_fast: bool,
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
            fail_fast,
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
            fail_fast,
        ),
        Commands::Analyze {
            file,
            dialect,
            progress,
            fail_fast,
        } => analyze::run(file, dialect, progress, fail_fast),
        Commands::Merge {
            input_dir,
            output,
            dialect,
            tables,
            exclude,
            transaction,
            no_header,
            progress,
            dry_run,
        } => merge::run(
            input_dir,
            output,
            dialect,
            tables,
            exclude,
            transaction,
            no_header,
            progress,
            dry_run,
        ),
        Commands::Sample {
            file,
            output,
            dialect,
            percent,
            rows,
            preserve_relations,
            tables,
            exclude,
            root_tables,
            include_global,
            seed,
            config,
            max_total_rows,
            no_limit,
            strict_fk,
            no_schema,
            progress,
            dry_run,
        } => {
            let effective_limit = if no_limit || max_total_rows == Some(0) {
                None
            } else {
                max_total_rows
            };
            sample::run(
                file,
                output,
                dialect,
                percent,
                rows,
                preserve_relations,
                tables,
                exclude,
                root_tables,
                include_global,
                seed,
                config,
                effective_limit,
                strict_fk,
                no_schema,
                progress,
                dry_run,
            )
        }
        Commands::Shard {
            file,
            output,
            dialect,
            tenant_column,
            tenant_value,
            tenant_values,
            root_tables,
            include_global,
            config,
            max_selected_rows,
            no_limit,
            strict_fk,
            no_schema,
            progress,
            dry_run,
        } => {
            let effective_limit = if no_limit || max_selected_rows == Some(0) {
                None
            } else {
                max_selected_rows
            };
            shard::run(
                file,
                output,
                dialect,
                tenant_column,
                tenant_value,
                tenant_values,
                root_tables,
                include_global,
                config,
                effective_limit,
                strict_fk,
                no_schema,
                progress,
                dry_run,
            )
        }
        Commands::Convert {
            file,
            output,
            from,
            to,
            strict,
            progress,
            dry_run,
            fail_fast,
        } => convert::run(file, output, from, to, strict, progress, dry_run, fail_fast),
        Commands::Validate {
            file,
            dialect,
            progress,
            strict,
            json,
            max_rows_per_table,
            no_limit,
            no_fk_checks,
            fail_fast,
        } => {
            let effective_limit = if no_limit || max_rows_per_table == 0 {
                usize::MAX
            } else {
                max_rows_per_table
            };
            validate::run(
                file,
                dialect,
                progress,
                strict,
                json,
                effective_limit,
                no_fk_checks,
                fail_fast,
            )
        }
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
