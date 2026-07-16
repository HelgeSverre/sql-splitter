pub(crate) mod analyze;
mod common;
pub(crate) mod convert;
mod diff;
mod glob_util;
pub(crate) mod graph;
pub(crate) mod merge;
mod order;
#[cfg(feature = "duckdb-query")]
mod query;
pub(crate) mod redact;
pub(crate) mod sample;
pub(crate) mod shard;
pub(crate) mod split;
pub(crate) mod validate;

use clap::{CommandFactory, Parser, Subcommand, ValueHint};
use clap_complete::{generate, Shell};
use std::io;
use std::path::PathBuf;

const AFTER_HELP: &str = "\x1b[1mCommon workflows:\x1b[0m
  Split a dump into per-table files:
    sql-splitter split dump.sql -o tables/

  Create a 10% sample for development:
    sql-splitter sample dump.sql -o dev.sql --percent 10 --preserve-relations

  Convert MySQL to PostgreSQL:
    sql-splitter convert mysql.sql --to postgres -o pg.sql

  Compare two dumps for changes:
    sql-splitter diff old.sql new.sql --format sql -o migration.sql

\x1b[1mMore info:\x1b[0m
  Run 'sql-splitter <command> --help' for command-specific options.
  Documentation: https://github.com/helgesverre/sql-splitter
  Enable completions: sql-splitter completions <shell>";

#[derive(Parser)]
#[command(name = "sql-splitter")]
#[command(author = "Helge Sverre <helge.sverre@gmail.com>")]
#[command(version)]
#[command(
    about = "High-performance CLI for splitting, merging, converting, and analyzing SQL dump files"
)]
#[command(after_help = AFTER_HELP)]
#[command(arg_required_else_help = true)]
#[command(max_term_width = 100)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

// Help heading constants for consistency
use common::{BEHAVIOR, FILTERING, INPUT_OUTPUT, LIMITS, MODE, OUTPUT_FORMAT};

#[derive(Subcommand)]
pub enum Commands {
    /// Split a SQL dump into individual table files
    #[command(visible_alias = "sp")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter split dump.sql -o tables/
  sql-splitter split dump.sql.gz -o tables/ --tables users,orders
  sql-splitter split dump.zip -o tables/
  sql-splitter split dump.sql -o schema/ --schema-only
  sql-splitter split dump.sql -o tables/ --compress zstd
  sql-splitter split dump.sql -o dump.tar.gz
  sql-splitter split dump.sql -o /mnt/usb/tables/ --io-strategy hdd
  sql-splitter split \"backups/*.sql\" -o out/ --fail-fast")]
    Split {
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output directory, or archive path (.tar.gz/.tgz/.tar.zst/.tar.bz2/.tar.xz/.tar/.zip)
        #[arg(short, long, default_value = "output", value_hint = ValueHint::DirPath, help_heading = INPUT_OUTPUT)]
        output: PathBuf,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// Only split specific tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        tables: Option<String>,

        /// Only include schema statements (CREATE, ALTER, DROP)
        #[arg(long, conflicts_with = "data_only", help_heading = FILTERING)]
        schema_only: bool,

        /// Only include data statements (INSERT, COPY)
        #[arg(long, conflicts_with = "schema_only", help_heading = FILTERING)]
        data_only: bool,

        /// Show verbose output
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        verbose: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,

        /// Compress each output file: none, gzip, zstd, bzip2, xz
        #[arg(long, default_value = "none", value_name = "FORMAT", help_heading = INPUT_OUTPUT)]
        compress: String,

        /// Output device I/O strategy: auto, ssd, hdd, cheap
        #[arg(long, default_value = "auto", value_name = "STRATEGY", help_heading = BEHAVIOR)]
        io_strategy: String,

        /// Preview without writing files
        #[arg(long, help_heading = BEHAVIOR)]
        dry_run: bool,

        /// Stop on first error (for glob patterns)
        #[arg(long, help_heading = BEHAVIOR)]
        fail_fast: bool,
    },

    /// Analyze a SQL dump and display table statistics
    #[command(visible_alias = "an")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter analyze dump.sql
  sql-splitter analyze dump.sql.gz --progress
  sql-splitter analyze \"dumps/*.sql\" --json")]
    Analyze {
        /// Input SQL file or glob pattern
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,

        /// Stop on first error (for glob patterns)
        #[arg(long, help_heading = BEHAVIOR)]
        fail_fast: bool,
    },

    /// Merge split SQL files back into a single dump
    #[command(visible_alias = "mg")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter merge tables/ -o restored.sql
  sql-splitter merge tables/ -o restored.sql --transaction
  sql-splitter merge tables/ -o partial.sql --tables users,orders
  sql-splitter merge tables/ -o clean.sql --exclude logs,cache")]
    Merge {
        /// Directory containing split SQL files
        #[arg(value_hint = ValueHint::DirPath, help_heading = INPUT_OUTPUT)]
        input_dir: PathBuf,

        /// Output SQL file (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// SQL dialect for output formatting
        #[arg(short, long, default_value = "mysql", help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// Only merge specific tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        tables: Option<String>,

        /// Exclude specific tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        exclude: Option<String>,

        /// Wrap output in BEGIN/COMMIT transaction
        #[arg(long, help_heading = BEHAVIOR)]
        transaction: bool,

        /// Omit header comments
        #[arg(long, help_heading = BEHAVIOR)]
        no_header: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,

        /// Preview without writing files
        #[arg(long, help_heading = BEHAVIOR)]
        dry_run: bool,
    },

    /// Create a reduced dataset preserving FK relationships
    #[command(visible_alias = "sa")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter sample dump.sql -o dev.sql --percent 10
  sql-splitter sample dump.sql -o dev.sql --rows 1000 --preserve-relations
  sql-splitter sample dump.sql -o dev.sql --percent 5 --seed 42
  sql-splitter sample dump.sql -o dev.sql --tables users,orders --percent 20")]
    Sample(sample::SampleArgs),

    /// Extract tenant-specific data from a multi-tenant dump
    #[command(visible_alias = "sh")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter shard dump.sql -o tenant.sql --tenant-value 123
  sql-splitter shard dump.sql -o tenant.sql --tenant-column company_id --tenant-value 42
  sql-splitter shard dump.sql -o shards/ --tenant-values \"1,2,3\"")]
    Shard(shard::ShardArgs),

    /// Convert a SQL dump between MySQL, PostgreSQL, SQLite, and MSSQL
    #[command(visible_alias = "cv")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter convert mysql.sql --to postgres -o pg.sql
  sql-splitter convert pg_dump.sql --to mysql -o mysql.sql
  sql-splitter convert dump.sql --from mysql --to sqlite -o sqlite.sql
  sql-splitter convert mysql.sql --to postgres | psql mydb")]
    Convert {
        /// Input SQL file or glob pattern
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output SQL file or directory (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// Source dialect (auto-detected if omitted)
        #[arg(long, help_heading = MODE)]
        from: Option<String>,

        /// Target dialect (required)
        #[arg(long, help_heading = MODE)]
        to: String,

        /// Fail on unsupported features instead of warning
        #[arg(long, help_heading = BEHAVIOR)]
        strict: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,

        /// Preview without writing files
        #[arg(long, help_heading = BEHAVIOR)]
        dry_run: bool,

        /// Stop on first error (for glob patterns)
        #[arg(long, help_heading = BEHAVIOR)]
        fail_fast: bool,
    },

    /// Validate SQL dump syntax, encoding, and data integrity
    #[command(visible_alias = "val")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter validate dump.sql
  sql-splitter validate dump.sql --strict
  sql-splitter validate \"dumps/*.sql\" --json --fail-fast
  sql-splitter validate dump.sql --no-fk-checks")]
    Validate {
        /// Input SQL file or glob pattern
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// Treat warnings as errors (exit code 1)
        #[arg(long, help_heading = BEHAVIOR)]
        strict: bool,

        /// Skip PK/FK data integrity checks
        #[arg(long, help_heading = BEHAVIOR)]
        no_fk_checks: bool,

        /// Stop on first error (for glob patterns)
        #[arg(long, help_heading = BEHAVIOR)]
        fail_fast: bool,

        /// Max rows per table for PK/FK checks (0 = unlimited)
        #[arg(long, default_value = "1000000", help_heading = LIMITS)]
        max_rows_per_table: usize,

        /// Disable row limit for PK/FK checks
        #[arg(long, help_heading = LIMITS)]
        no_limit: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,
    },

    /// Compare two SQL dumps and report schema + data differences
    #[command(visible_alias = "df")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter diff old.sql new.sql
  sql-splitter diff old.sql new.sql --schema-only
  sql-splitter diff old.sql new.sql --format sql -o migration.sql
  sql-splitter diff old.sql new.sql --verbose --ignore-columns \"*.updated_at\"
  sql-splitter diff old.sql new.sql --primary-key logs:timestamp+message")]
    Diff(diff::DiffArgs),

    /// Redact sensitive data (PII) from SQL dumps
    #[command(visible_alias = "rd")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml
  sql-splitter redact dump.sql -o safe.sql --null \"*.ssn\" --hash \"*.email\"
  sql-splitter redact dump.sql --generate-config -o redact.yaml
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml --seed 42")]
    Redact(redact::RedactArgs),

    /// Generate Entity Relationship Diagram (ERD) from SQL dump
    #[command(visible_alias = "gr")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter graph dump.sql -o schema.html
  sql-splitter graph dump.sql -o schema.mmd --format mermaid
  sql-splitter graph dump.sql -o schema.png --render
  sql-splitter graph dump.sql --cycles-only
  sql-splitter graph dump.sql --table users --transitive")]
    Graph(graph::GraphArgs),

    /// Output SQL dump with tables in topological FK order
    #[command(visible_alias = "ord")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter order dump.sql -o ordered.sql
  sql-splitter order dump.sql --check
  sql-splitter order dump.sql --dry-run
  sql-splitter order dump.sql --reverse")]
    Order {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst)
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
    },

    /// Query SQL dumps using DuckDB's analytical engine
    #[cfg(feature = "duckdb-query")]
    #[command(visible_alias = "qy")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter query dump.sql \"SELECT COUNT(*) FROM users\"
  sql-splitter query dump.sql \"SELECT * FROM orders WHERE total > 100\" -f json
  sql-splitter query dump.sql \"SELECT * FROM users LIMIT 10\" -o results.csv -f csv
  sql-splitter query dump.sql --interactive
  sql-splitter query huge.sql \"SELECT ...\" --disk
  sql-splitter query dump.sql \"SELECT ...\" --cache
  sql-splitter query --list-cache")]
    Query(query::QueryArgs),

    /// Generate JSON schemas for --json output types (developer tool)
    #[command(hide = true)]
    Schema {
        /// Output directory for schema files
        #[arg(short, long, default_value = "schemas", value_hint = ValueHint::DirPath)]
        output: PathBuf,

        /// Generate schema for a specific command only
        #[arg(short, long)]
        command: Option<String>,

        /// Print schemas to stdout instead of writing files
        #[arg(long)]
        stdout: bool,

        /// List available schema names
        #[arg(long)]
        list: bool,
    },

    /// Generate shell completion scripts
    #[command(after_help = "\x1b[1mInstallation:\x1b[0m
  Bash:
    sql-splitter completions bash > /etc/bash_completion.d/sql-splitter
    # or: sql-splitter completions bash >> ~/.bashrc

  Zsh:
    sql-splitter completions zsh > \"${fpath[1]}/_sql-splitter\"
    # or for oh-my-zsh: sql-splitter completions zsh > ~/.oh-my-zsh/completions/_sql-splitter

  Fish:
    sql-splitter completions fish > ~/.config/fish/completions/sql-splitter.fish

  PowerShell:
    sql-splitter completions powershell >> $PROFILE")]
    Completions {
        /// Target shell
        #[arg(value_enum)]
        shell: Shell,
    },
}

use common::dash_is_stdout;

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
            json,
            compress,
            io_strategy,
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
            json,
            compress,
            io_strategy,
        ),
        Commands::Analyze {
            file,
            dialect,
            progress,
            fail_fast,
            json,
        } => analyze::run(file, dialect, progress, fail_fast, json),
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
            json,
        } => merge::run(
            input_dir,
            dash_is_stdout(output),
            dialect,
            tables,
            exclude,
            transaction,
            no_header,
            progress,
            dry_run,
            json,
        ),
        Commands::Sample(args) => sample::run(args),
        Commands::Shard(args) => shard::run(args),
        Commands::Convert {
            file,
            output,
            from,
            to,
            strict,
            progress,
            dry_run,
            fail_fast,
            json,
        } => convert::run(
            file,
            dash_is_stdout(output),
            from,
            to,
            strict,
            progress,
            dry_run,
            fail_fast,
            json,
        ),
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
        Commands::Diff(args) => diff::run(args),
        Commands::Redact(args) => redact::run(args),
        Commands::Graph(args) => graph::run(args),
        Commands::Order {
            file,
            output,
            dialect,
            check,
            dry_run,
            reverse,
        } => order::run(
            file,
            dash_is_stdout(output),
            dialect,
            check,
            dry_run,
            reverse,
        ),
        #[cfg(feature = "duckdb-query")]
        Commands::Query(args) => query::run(args),
        Commands::Schema {
            output,
            command,
            stdout,
            list,
        } => run_schema(output, command, stdout, list),
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

fn run_schema(
    output_dir: PathBuf,
    command: Option<String>,
    to_stdout: bool,
    list: bool,
) -> anyhow::Result<()> {
    use crate::json_schema;
    use std::fs;

    if list {
        let schemas = json_schema::all_schemas();
        for name in schemas.keys() {
            println!("{}", name);
        }
        return Ok(());
    }

    if let Some(cmd) = command {
        let schema = json_schema::get_schema(&cmd).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown command: {}. Use --list to see available schemas.",
                cmd
            )
        })?;

        let json = serde_json::to_string_pretty(&schema)?;

        if to_stdout {
            println!("{}", json);
        } else {
            fs::create_dir_all(&output_dir)?;
            let path = output_dir.join(format!("{}.schema.json", cmd));
            fs::write(&path, json)?;
            eprintln!("Wrote: {}", path.display());
        }
    } else if to_stdout {
        let schemas = json_schema::all_schemas();
        for (name, schema) in &schemas {
            let json = serde_json::to_string_pretty(schema)?;
            println!("// {}.schema.json\n{}\n", name, json);
        }
    } else {
        let schemas = json_schema::all_schemas();
        fs::create_dir_all(&output_dir)?;
        for (name, schema) in &schemas {
            let json = serde_json::to_string_pretty(schema)?;
            let path = output_dir.join(format!("{}.schema.json", name));
            fs::write(&path, json)?;
            eprintln!("Wrote: {}", path.display());
        }
    }

    Ok(())
}
