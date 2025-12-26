mod analyze;
mod convert;
mod diff;
mod glob_util;
mod graph;
mod merge;
mod order;
mod query;
mod redact;
mod sample;
mod shard;
mod split;
mod validate;

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
const INPUT_OUTPUT: &str = "Input/Output";
const FILTERING: &str = "Filtering";
const MODE: &str = "Mode";
const BEHAVIOR: &str = "Behavior";
const LIMITS: &str = "Limits";
const OUTPUT_FORMAT: &str = "Output";

#[derive(Subcommand)]
pub enum Commands {
    /// Split a SQL dump into individual table files
    #[command(visible_alias = "sp")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter split dump.sql -o tables/
  sql-splitter split dump.sql.gz -o tables/ --tables users,orders
  sql-splitter split dump.sql -o schema/ --schema-only
  sql-splitter split \"backups/*.sql\" -o out/ --fail-fast")]
    Split {
        /// Input SQL file or glob pattern (e.g., *.sql, dumps/**/*.sql)
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output directory for split files
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
    Sample {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst)
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output SQL file (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// YAML config file for per-table settings
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        config: Option<PathBuf>,

        /// Sample percentage of rows (1-100)
        #[arg(long, conflicts_with = "rows", help_heading = MODE)]
        percent: Option<u32>,

        /// Sample fixed number of rows per table
        #[arg(long, conflicts_with = "percent", help_heading = MODE)]
        rows: Option<usize>,

        /// Random seed for reproducible sampling
        #[arg(long, help_heading = MODE)]
        seed: Option<u64>,

        /// Only sample specific tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        tables: Option<String>,

        /// Exclude specific tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        exclude: Option<String>,

        /// Tables to start sampling from (comma-separated)
        #[arg(long, help_heading = FILTERING)]
        root_tables: Option<String>,

        /// Handle lookup tables: none, lookups, all
        #[arg(long, default_value = "lookups", help_heading = FILTERING)]
        include_global: Option<String>,

        /// Maintain FK integrity by including referenced rows
        #[arg(long, help_heading = BEHAVIOR)]
        preserve_relations: bool,

        /// Fail on FK integrity violations
        #[arg(long, help_heading = BEHAVIOR)]
        strict_fk: bool,

        /// Exclude CREATE TABLE statements from output
        #[arg(long, help_heading = BEHAVIOR)]
        no_schema: bool,

        /// Max total rows to sample (0 = unlimited)
        #[arg(long, help_heading = LIMITS)]
        max_total_rows: Option<usize>,

        /// Disable row limit
        #[arg(long, help_heading = LIMITS)]
        no_limit: bool,

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

    /// Extract tenant-specific data from a multi-tenant dump
    #[command(visible_alias = "sh")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter shard dump.sql -o tenant.sql --tenant-value 123
  sql-splitter shard dump.sql -o tenant.sql --tenant-column company_id --tenant-value 42
  sql-splitter shard dump.sql -o shards/ --tenant-values \"1,2,3\"")]
    Shard {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst)
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output SQL file or directory (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// YAML config file for table classification
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        config: Option<PathBuf>,

        /// Column containing tenant ID (auto-detected if omitted)
        #[arg(long, help_heading = MODE)]
        tenant_column: Option<String>,

        /// Single tenant value to extract
        #[arg(long, conflicts_with = "tenant_values", help_heading = MODE)]
        tenant_value: Option<String>,

        /// Multiple tenant values (comma-separated, outputs to directory)
        #[arg(long, conflicts_with = "tenant_value", help_heading = MODE)]
        tenant_values: Option<String>,

        /// Tables containing tenant column (comma-separated)
        #[arg(long, help_heading = FILTERING)]
        root_tables: Option<String>,

        /// Handle lookup tables: none, lookups, all
        #[arg(long, default_value = "lookups", help_heading = FILTERING)]
        include_global: Option<String>,

        /// Fail on FK integrity violations
        #[arg(long, help_heading = BEHAVIOR)]
        strict_fk: bool,

        /// Exclude CREATE TABLE statements from output
        #[arg(long, help_heading = BEHAVIOR)]
        no_schema: bool,

        /// Max rows to select (0 = unlimited)
        #[arg(long, help_heading = LIMITS)]
        max_selected_rows: Option<usize>,

        /// Disable row limit
        #[arg(long, help_heading = LIMITS)]
        no_limit: bool,

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

    /// Convert a SQL dump between MySQL, PostgreSQL, and SQLite
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
    Diff {
        /// Original SQL dump file
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        old_file: PathBuf,

        /// Updated SQL dump file
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        new_file: PathBuf,

        /// Output file (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// Only compare these tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        tables: Option<String>,

        /// Exclude these tables (comma-separated)
        #[arg(short, long, help_heading = FILTERING)]
        exclude: Option<String>,

        /// Ignore columns matching glob patterns (e.g., *.updated_at)
        #[arg(long, help_heading = FILTERING)]
        ignore_columns: Option<String>,

        /// Compare schema only, skip data
        #[arg(long, conflicts_with = "data_only", help_heading = MODE)]
        schema_only: bool,

        /// Compare data only, skip schema
        #[arg(long, conflicts_with = "schema_only", help_heading = MODE)]
        data_only: bool,

        /// Override primary key (format: table:col1+col2,table2:col)
        #[arg(long, help_heading = MODE)]
        primary_key: Option<String>,

        /// Compare tables without PK using all columns as key
        #[arg(long, help_heading = BEHAVIOR)]
        allow_no_pk: bool,

        /// Ignore column order differences in schema
        #[arg(long, help_heading = BEHAVIOR)]
        ignore_order: bool,

        /// Max PK entries per table (limits memory)
        #[arg(long, default_value = "10000000", help_heading = LIMITS)]
        max_pk_entries: usize,

        /// Output format: text, json, sql
        #[arg(short, long, default_value = "text", help_heading = OUTPUT_FORMAT)]
        format: Option<String>,

        /// Show sample PK values for changes
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        verbose: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,
    },

    /// Redact sensitive data (PII) from SQL dumps
    #[command(visible_alias = "rd")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml
  sql-splitter redact dump.sql -o safe.sql --null \"*.ssn\" --hash \"*.email\"
  sql-splitter redact dump.sql --generate-config -o redact.yaml
  sql-splitter redact dump.sql -o safe.sql --config redact.yaml --seed 42")]
    Redact {
        /// Input SQL file (supports .gz, .bz2, .xz, .zst)
        #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        file: PathBuf,

        /// Output file (default: stdout)
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        output: Option<PathBuf>,

        /// SQL dialect: mysql, postgres, sqlite, mssql (auto-detected if omitted)
        #[arg(short, long, help_heading = INPUT_OUTPUT)]
        dialect: Option<String>,

        /// YAML config file for redaction rules
        #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
        config: Option<PathBuf>,

        /// Generate annotated YAML config by analyzing input file
        #[arg(long, help_heading = MODE)]
        generate_config: bool,

        /// Columns to set to NULL (glob patterns, comma-separated)
        #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
        null: Vec<String>,

        /// Columns to hash with SHA256 (glob patterns)
        #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
        hash: Vec<String>,

        /// Columns to replace with fake data (glob patterns)
        #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
        fake: Vec<String>,

        /// Columns to mask (format: pattern=column, e.g., "****-XXXX=*.credit_card")
        #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
        mask: Vec<String>,

        /// Column=value pairs for constant replacement
        #[arg(long, value_delimiter = ',', help_heading = "Inline Strategies")]
        constant: Vec<String>,

        /// Random seed for reproducible redaction
        #[arg(long, help_heading = MODE)]
        seed: Option<u64>,

        /// Locale for fake data generation (default: en)
        #[arg(long, default_value = "en", help_heading = MODE)]
        locale: String,

        /// Only redact specific tables (comma-separated)
        #[arg(short, long, value_delimiter = ',', help_heading = FILTERING)]
        tables: Vec<String>,

        /// Exclude specific tables (comma-separated)
        #[arg(short = 'x', long, value_delimiter = ',', help_heading = FILTERING)]
        exclude: Vec<String>,

        /// Fail on warnings (e.g., unsupported locale)
        #[arg(long, help_heading = BEHAVIOR)]
        strict: bool,

        /// Show progress bar
        #[arg(short, long, help_heading = OUTPUT_FORMAT)]
        progress: bool,

        /// Preview without writing files
        #[arg(long, help_heading = BEHAVIOR)]
        dry_run: bool,

        /// Output results as JSON
        #[arg(long, help_heading = OUTPUT_FORMAT)]
        json: bool,

        /// Validate config only, don't process
        #[arg(long, help_heading = BEHAVIOR)]
        validate: bool,
    },

    /// Generate Entity Relationship Diagram (ERD) from SQL dump
    #[command(visible_alias = "gr")]
    #[command(after_help = "\x1b[1mExamples:\x1b[0m
  sql-splitter graph dump.sql -o schema.html
  sql-splitter graph dump.sql -o schema.mmd --format mermaid
  sql-splitter graph dump.sql -o schema.png --render
  sql-splitter graph dump.sql --cycles-only
  sql-splitter graph dump.sql --table users --transitive")]
    Graph {
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
    },

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
            output,
            dialect,
            tables,
            exclude,
            transaction,
            no_header,
            progress,
            dry_run,
            json,
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
            json,
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
                json,
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
            json,
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
                json,
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
            json,
        } => convert::run(
            file, output, from, to, strict, progress, dry_run, fail_fast, json,
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
        Commands::Diff {
            old_file,
            new_file,
            output,
            tables,
            exclude,
            schema_only,
            data_only,
            format,
            dialect,
            verbose,
            progress,
            max_pk_entries,
            allow_no_pk,
            ignore_order,
            primary_key,
            ignore_columns,
        } => diff::run(
            old_file,
            new_file,
            output,
            tables,
            exclude,
            schema_only,
            data_only,
            format,
            dialect,
            verbose,
            progress,
            max_pk_entries,
            allow_no_pk,
            ignore_order,
            primary_key,
            ignore_columns,
        ),
        Commands::Redact {
            file,
            output,
            dialect,
            config,
            generate_config,
            null,
            hash,
            fake,
            mask,
            constant,
            seed,
            locale,
            tables,
            exclude,
            strict,
            progress,
            dry_run,
            json,
            validate,
        } => redact::run(
            file,
            output,
            dialect,
            config,
            generate_config,
            null,
            hash,
            fake,
            mask,
            constant,
            seed,
            locale,
            tables,
            exclude,
            strict,
            progress,
            dry_run,
            json,
            validate,
        ),
        Commands::Graph {
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
            progress,
            json,
        } => graph::run(
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
            progress,
            json,
        ),
        Commands::Order {
            file,
            output,
            dialect,
            check,
            dry_run,
            reverse,
        } => order::run(file, output, dialect, check, dry_run, reverse),
        Commands::Query(args) => query::run(args),
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
