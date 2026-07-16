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
use std::process::ExitCode;

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
    Split(split::SplitArgs),

    /// Analyze a SQL dump and display table statistics
    #[command(visible_alias = "an")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter analyze dump.sql
  sql-splitter analyze dump.sql.gz --progress
  sql-splitter analyze \"dumps/*.sql\" --json")]
    Analyze(analyze::AnalyzeArgs),

    /// Merge split SQL files back into a single dump
    #[command(visible_alias = "mg")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter merge tables/ -o restored.sql
  sql-splitter merge tables/ -o restored.sql --transaction
  sql-splitter merge tables/ -o partial.sql --tables users,orders
  sql-splitter merge tables/ -o clean.sql --exclude logs,cache")]
    Merge(merge::MergeArgs),

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
    Convert(convert::ConvertArgs),

    /// Validate SQL dump syntax, encoding, and data integrity
    #[command(visible_alias = "val")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter validate dump.sql
  sql-splitter validate dump.sql --strict
  sql-splitter validate \"dumps/*.sql\" --json --fail-fast
  sql-splitter validate dump.sql --no-fk-checks")]
    Validate(validate::ValidateArgs),

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
    Order(order::OrderArgs),

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

/// Dispatch to the selected subcommand, returning the process exit code.
///
/// Commands that can partially fail (multi-file batches, validation, order
/// --check) report failure through the returned [`ExitCode`] instead of
/// calling `std::process::exit`, so destructors (tempdir cleanup, buffered
/// writers) always run and the cmd layer stays usable as a library.
pub fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    match cli.command {
        Commands::Split(args) => split::run(args),
        Commands::Analyze(args) => analyze::run(args),
        Commands::Merge(args) => merge::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Sample(args) => sample::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Shard(args) => shard::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Convert(args) => convert::run(args),
        Commands::Validate(args) => validate::run(args),
        Commands::Diff(args) => diff::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Redact(args) => redact::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Graph(args) => graph::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Order(args) => order::run(args),
        #[cfg(feature = "duckdb-query")]
        Commands::Query(args) => query::run(args).map(|()| ExitCode::SUCCESS),
        Commands::Schema {
            output,
            command,
            stdout,
            list,
        } => run_schema(output, command, stdout, list).map(|()| ExitCode::SUCCESS),
        Commands::Completions { shell } => {
            generate(
                shell,
                &mut Cli::command(),
                "sql-splitter",
                &mut io::stdout(),
            );
            Ok(ExitCode::SUCCESS)
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
