//! CLI handler for the `generate` command: model-driven synthetic data
//! generation.
//!
//! The command wires the complete-model path — `--config model.yaml` compiled
//! and generated, checked, or dry-run — plus JSON/quiet reporting and
//! clap-level usage validation (see [`GenerateArgs::try_into_request`]). Dump
//! profiling (`[INPUT]`, `--profile-depth`, `--profile-sample`), config
//! emission (`--emit-config`), post-generation verification (`--verify`), and
//! the `--explain` inference report are all wired end to end, as are
//! `--mssql-production-style`/`--mssql-go`. The one accepted-but-inert flag is
//! `--compress`: it fails with a clear "not available yet" error rather than
//! silently doing nothing.

use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, ValueHint};
use schemars::JsonSchema;
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::diagnostic::{DiagnosticBag, Severity};
use crate::generate::{
    CompileOptions, ExplainColumn, Generate, GenerateError, GenerateReport, GenerateRequest,
    OutputTarget, RenderOptions, RunMode, SourceOptions, TableCountOverride,
};
use crate::parser::SqlDialect;
use crate::profile::ProfileDepth;
use crate::synthetic::OutputMode;

use super::common::{dash_is_stdout, FILTERING};

const INPUT_OUTPUT: &str = "Input/Model";
const VOLUME: &str = "Volume";
const RANDOMNESS: &str = "Randomness";
const RENDERING: &str = "Rendering";
const PREFLIGHT: &str = "Preflight/Reporting";

/// Profiling depth for `[INPUT]` (`--profile-depth`).
///
/// Schema-only profiling remains a library/internal mode — the CLI only
/// exposes `basic`/`full`.
#[derive(clap::ValueEnum, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProfileDepthArg {
    #[default]
    Basic,
    Full,
}

impl ProfileDepthArg {
    fn to_depth(self) -> ProfileDepth {
        match self {
            ProfileDepthArg::Basic => ProfileDepth::Basic,
            ProfileDepthArg::Full => ProfileDepth::Full,
        }
    }
}

/// Output compression format for `--compress`. Accepted for forward
/// compatibility; rendering never wraps its output writer in a compressor
/// yet, so any value here fails with a clear "not available" error.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressFormat {
    Gzip,
    Bzip2,
    Xz,
    Zstd,
}

/// CLI surface for `sql-splitter generate`. See the module docs for supported
/// workflows and [`GenerateArgs::try_into_request`] for request validation.
#[derive(Args)]
pub struct GenerateArgs {
    /// Source SQL dump to profile into a base model
    #[arg(value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    input: Option<PathBuf>,

    /// `kind: model` (or `kind: overrides`) YAML document to generate from
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    config: Option<PathBuf>,

    /// Write the resolved model as YAML instead of generating
    #[arg(long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    emit_config: Option<PathBuf>,

    /// Output file for generated SQL (default: stdout; `-` also means stdout)
    #[arg(short, long, value_hint = ValueHint::FilePath, help_heading = INPUT_OUTPUT)]
    output: Option<PathBuf>,

    /// Depth of profiling to run against `[INPUT]`
    #[arg(long, value_enum, default_value_t = ProfileDepthArg::Basic, help_heading = INPUT_OUTPUT)]
    profile_depth: ProfileDepthArg,

    /// Row sample size used while profiling `[INPUT]`
    #[arg(long, help_heading = INPUT_OUTPUT)]
    profile_sample: Option<usize>,

    /// Dialect `[INPUT]` is written in, if profiling (auto-detected otherwise)
    #[arg(long, help_heading = INPUT_OUTPUT)]
    input_dialect: Option<SqlDialect>,

    /// SQL dialect to render output for (default: the model's output.dialect or
    /// source dialect; falls back to mysql when neither is known)
    #[arg(long, help_heading = RENDERING)]
    dialect: Option<SqlDialect>,

    /// Global multiplicative row-count scale
    #[arg(long, conflicts_with = "rows", help_heading = VOLUME)]
    scale: Option<f64>,

    /// Global absolute root row count
    #[arg(long, conflicts_with = "scale", help_heading = VOLUME)]
    rows: Option<u64>,

    /// Per-table absolute row-count override (`table=count`, repeatable)
    #[arg(long = "table-rows", help_heading = VOLUME)]
    table_rows: Vec<String>,

    /// Per-table row-count scale override (`table=factor`, repeatable)
    #[arg(long = "table-scale", help_heading = VOLUME)]
    table_scale: Vec<String>,

    /// Upper bound applied to every table's row count, last
    #[arg(long, help_heading = VOLUME)]
    max_rows: Option<u64>,

    /// Only generate these tables (comma-separated globs)
    #[arg(long, value_delimiter = ',', help_heading = FILTERING)]
    tables: Vec<String>,

    /// Exclude these tables (comma-separated globs)
    #[arg(long, value_delimiter = ',', help_heading = FILTERING)]
    exclude: Vec<String>,

    /// Run root seed, overriding the model's own `seed:`
    #[arg(long, conflicts_with = "randomize", help_heading = RANDOMNESS)]
    seed: Option<u64>,

    /// Use a fresh random seed instead of the model's (or a fixed) seed
    #[arg(long, conflicts_with = "seed", help_heading = RANDOMNESS)]
    randomize: bool,

    /// Render only `CREATE TABLE`/DDL, no row data
    #[arg(long, conflicts_with = "data_only", help_heading = RENDERING)]
    schema_only: bool,

    /// Render only row data, no DDL
    #[arg(long, conflicts_with = "schema_only", help_heading = RENDERING)]
    data_only: bool,

    /// Rows per `INSERT`/`COPY` batch
    #[arg(long, default_value_t = 1_000, help_heading = RENDERING)]
    batch_size: usize,

    /// Force multi-row `INSERT` for PostgreSQL instead of `COPY`
    #[arg(long, help_heading = RENDERING)]
    no_copy: bool,

    /// Compress rendered output (not yet implemented)
    #[arg(long, value_enum, help_heading = RENDERING)]
    compress: Option<CompressFormat>,

    /// Render MSSQL output in production style: [dbo]. schema-qualified
    /// names, a named clustered PRIMARY KEY constraint, an ON [PRIMARY]
    /// filegroup clause, and a SET ANSI_NULLS/QUOTED_IDENTIFIER session
    /// header. Requires `--dialect mssql`.
    #[arg(long, help_heading = RENDERING)]
    mssql_production_style: bool,

    /// Emit a `GO` batch separator every N INSERT batches instead of after
    /// every batch. Requires `--dialect mssql`.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..), help_heading = RENDERING)]
    mssql_go: Option<u64>,

    /// Validate the model and exit; writes no SQL
    #[arg(long, conflicts_with_all = ["dry_run", "verify"], help_heading = PREFLIGHT)]
    check: bool,

    /// Compile the model and report resolved row counts; writes no SQL
    #[arg(long, conflicts_with_all = ["check", "verify"], help_heading = PREFLIGHT)]
    dry_run: bool,

    /// Verify generated rows against the model's constraints
    #[arg(long, conflicts_with_all = ["check", "dry_run"], help_heading = PREFLIGHT)]
    verify: bool,

    /// Explain each column's inference decision (winning rule and rejected
    /// alternatives); never prints observed values
    #[arg(long, help_heading = PREFLIGHT)]
    explain: bool,

    /// Treat model warnings as errors
    #[arg(long, help_heading = PREFLIGHT)]
    strict: bool,

    /// Show a progress bar (not yet wired to generation; accepted and ignored)
    #[arg(long, conflicts_with = "quiet", help_heading = PREFLIGHT)]
    progress: bool,

    /// Output the report as JSON (owns stdout)
    #[arg(long, help_heading = PREFLIGHT)]
    json: bool,

    /// Suppress the non-JSON summary report
    #[arg(long, conflicts_with = "progress", help_heading = PREFLIGHT)]
    quiet: bool,
}

/// A [`GenerateRequest`] plus the CLI-only knobs [`run`] needs once
/// [`Generate::run`] returns: how to report the outcome and (when generated SQL
/// has nowhere else to go) the
/// temporary file it was rendered to so it can be streamed to stdout.
struct PreparedRequest {
    request: GenerateRequest,
    mode: RunMode,
    /// Whether the human report should print the `--explain` inference detail.
    explain: bool,
    json: bool,
    quiet: bool,
    /// Present only when SQL renders to stdout: [`GenerateRequest`] has no
    /// stdout [`OutputTarget`], so this stdout case renders to a temp file
    /// first and [`run`] streams it to stdout after a successful run.
    stdout_temp: Option<NamedTempFile>,
    /// Present only when `--emit-config -` writes the model to stdout: the
    /// resolved model is spooled to this temp file, then streamed to stdout.
    emit_stdout_temp: Option<NamedTempFile>,
}

/// A problem with the CLI invocation itself, distinct from a model/runtime
/// failure. [`RequestError::Usage`] is a shape problem clap can't express
/// (e.g. a value-conditional conflict) — [`run`] maps it to clap's own usage
/// exit code, `2`. [`RequestError::Unavailable`] is a well-formed request for
/// an accepted request for a capability that is not implemented — [`run`] maps
/// it to the ordinary failure exit code, `1`, like any other runtime error.
#[derive(Debug)]
enum RequestError {
    Usage(String),
    Unavailable(String),
}

impl RequestError {
    fn usage(message: impl Into<String>) -> Self {
        RequestError::Usage(message.into())
    }

    fn unavailable(message: impl Into<String>) -> Self {
        RequestError::Unavailable(message.into())
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Usage(message) | RequestError::Unavailable(message) => {
                write!(f, "{message}")
            }
        }
    }
}

impl std::error::Error for RequestError {}

impl GenerateArgs {
    /// Validate CLI-shape rules clap's declarative `conflicts_with` can't
    /// express (they depend on argument *values*, e.g. `--output -`, not
    /// just presence), reject not-yet-implemented flags with a clear error,
    /// and assemble a [`PreparedRequest`].
    ///
    /// Model-level problems (an invalid config or a compile error) intentionally
    /// are not checked here — those surface from [`Generate::run`] itself, as
    /// [`GenerateError`]. Check mode's required config *presence* is a CLI-shape
    /// rule and is rejected here with the usage exit code.
    fn try_into_request(self) -> Result<PreparedRequest, RequestError> {
        if self.check && self.input.is_some() {
            return Err(RequestError::usage(
                "--check requires a complete `--config` model and cannot profile an input dump; \
                 omit `[INPUT]`",
            ));
        }
        if self.check && self.config.is_none() {
            return Err(RequestError::usage(
                "--check requires a complete model supplied with `--config <PATH>`",
            ));
        }

        // `--emit-config` with no SQL destination and no other preflight mode
        // selects EmitModel: the resolved model is the sole output. With `-o`
        // (or under check/dry-run) it stays an orthogonal side output.
        let mode = if self.check {
            RunMode::Check
        } else if self.dry_run {
            RunMode::DryRun
        } else if self.emit_config.is_some() && self.output.is_none() {
            RunMode::EmitModel
        } else {
            RunMode::Generate
        };

        let output_explicit_dash = is_dash(&self.output);
        let emit_config_dash = is_dash(&self.emit_config);
        let sql_wants_stdout = mode == RunMode::Generate
            && !self.json
            && (self.output.is_none() || output_explicit_dash);

        if self.json && output_explicit_dash {
            return Err(RequestError::usage(
                "--json and `--output -` both claim stdout; choose one",
            ));
        }
        if self.json && emit_config_dash {
            return Err(RequestError::usage(
                "--json and `--emit-config -` both claim stdout; choose one",
            ));
        }
        if mode == RunMode::Generate && self.json && self.output.is_none() {
            // `--json` takes stdout for the report, so generated SQL would have
            // nowhere to go and be silently discarded. Require a real output
            // file, or use --check/--dry-run for a report-only run.
            return Err(RequestError::usage(
                "--json in generate mode writes only the report to stdout; add `-o <path>` for the generated SQL, or use --check/--dry-run for a report-only run",
            ));
        }
        if sql_wants_stdout && emit_config_dash {
            return Err(RequestError::usage(
                "generated SQL and `--emit-config -` both claim stdout; choose one",
            ));
        }

        if self.verify {
            let has_file_output = self
                .output
                .as_ref()
                .is_some_and(|path| path.as_os_str() != "-");
            if !has_file_output {
                return Err(RequestError::usage(
                    "--verify requires a real output file (`-o <path>`), not stdout",
                ));
            }
        }

        if self.batch_size == 0 {
            return Err(RequestError::usage("--batch-size must be at least 1"));
        }

        if self.compress.is_some() && (self.output.is_none() || output_explicit_dash) {
            return Err(RequestError::usage(
                "--compress requires a real output file, not stdout",
            ));
        }

        let mut table_rows = Vec::with_capacity(self.table_rows.len());
        let mut rows_tables = HashSet::new();
        for raw in &self.table_rows {
            let (table, value) = split_table_override(raw).ok_or_else(|| {
                RequestError::usage(format!("--table-rows `{raw}` must be `table=count`"))
            })?;
            let count: u64 = value.parse().map_err(|_| {
                RequestError::usage(format!("--table-rows `{raw}` has a non-numeric count"))
            })?;
            rows_tables.insert(table.to_string());
            table_rows.push(TableCountOverride::rows(table, count));
        }

        let mut scale_tables = HashSet::new();
        for raw in &self.table_scale {
            let (table, value) = split_table_override(raw).ok_or_else(|| {
                RequestError::usage(format!("--table-scale `{raw}` must be `table=factor`"))
            })?;
            let factor: f64 = value.parse().map_err(|_| {
                RequestError::usage(format!("--table-scale `{raw}` has a non-numeric factor"))
            })?;
            if !factor.is_finite() || factor < 0.0 {
                return Err(RequestError::usage(format!(
                    "--table-scale `{raw}` must be a finite, non-negative number"
                )));
            }
            scale_tables.insert(table.to_string());
            table_rows.push(TableCountOverride::scale(table, factor));
        }

        if let Some(table) = rows_tables.intersection(&scale_tables).next() {
            return Err(RequestError::usage(format!(
                "table `{table}` is targeted by both --table-rows and --table-scale"
            )));
        }

        let effective_dialect = self.dialect.unwrap_or_default();
        if (self.mssql_production_style || self.mssql_go.is_some())
            && effective_dialect != SqlDialect::Mssql
        {
            return Err(RequestError::usage(
                "--mssql-production-style/--mssql-go require an explicit `--dialect mssql`",
            ));
        }
        if self.compress.is_some() {
            return Err(RequestError::unavailable(
                "--compress is not available yet: generated output cannot be compressed",
            ));
        }

        let seed = if self.randomize {
            Some(rand::random::<u64>())
        } else {
            self.seed
        };

        let render_mode = match (self.schema_only, self.data_only) {
            (true, false) => OutputMode::SchemaOnly,
            (false, true) => OutputMode::DataOnly,
            _ => OutputMode::SchemaAndData,
        };
        let render = RenderOptions {
            dialect: effective_dialect,
            source_dialect: None,
            mode: render_mode,
            no_copy: self.no_copy,
            batch_size: self.batch_size,
            mssql_production_style: self.mssql_production_style,
            mssql_go: self.mssql_go,
        };

        let compile = CompileOptions {
            seed,
            scale: self.scale,
            rows: self.rows,
            max_rows: self.max_rows,
            table_rows,
            tables: self.tables,
            exclude: self.exclude,
            family_budget_bytes: None,
        };

        let (output, stdout_temp) = match mode {
            // `Generate::run` never reads `output` under Check/DryRun/EmitModel
            // (they never render SQL), so any `-o` given alongside them is
            // inert; discard rather than modeling a meaningless path.
            RunMode::Check | RunMode::DryRun | RunMode::EmitModel => (OutputTarget::Discard, None),
            RunMode::Generate => match dash_is_stdout(self.output) {
                Some(path) => (OutputTarget::Path(path), None),
                None if self.json => (OutputTarget::Discard, None),
                None => {
                    let temp = NamedTempFile::new().map_err(|error| {
                        RequestError::unavailable(format!(
                            "failed to create a temporary file for stdout output: {error}"
                        ))
                    })?;
                    let path = temp.path().to_path_buf();
                    (OutputTarget::Path(path), Some(temp))
                }
            },
        };

        // `--emit-config` writes to a real path; `--emit-config -` spools the
        // model through a temp file that `run` streams to stdout afterward.
        let (emit, emit_stdout_temp) = match self.emit_config {
            None => (None, None),
            Some(path) if path.as_os_str() == "-" => {
                let temp = NamedTempFile::new().map_err(|error| {
                    RequestError::unavailable(format!(
                        "failed to create a temporary file for stdout emit-config: {error}"
                    ))
                })?;
                let emit_path = temp.path().to_path_buf();
                (Some(OutputTarget::Path(emit_path)), Some(temp))
            }
            Some(path) => (Some(OutputTarget::Path(path)), None),
        };

        let source = SourceOptions {
            dialect: self.input_dialect,
            depth: Some(self.profile_depth.to_depth()),
            sample: self.profile_sample,
        };

        let request = GenerateRequest {
            input: self.input,
            config: self.config,
            output,
            emit,
            compile,
            render,
            output_dialect: self.dialect,
            mode,
            explain: self.explain,
            verify: self.verify,
            strict: self.strict,
            source,
        };

        Ok(PreparedRequest {
            request,
            mode,
            explain: self.explain,
            emit_stdout_temp,
            json: self.json,
            quiet: self.quiet,
            stdout_temp,
        })
    }
}

/// Whether `path` is the literal `-` (the CLI's stdout convention).
fn is_dash(path: &Option<PathBuf>) -> bool {
    path.as_deref().is_some_and(|path| path.as_os_str() == "-")
}

/// Split a `table=value` CLI pattern (`--table-rows`/`--table-scale`) into
/// its table and value halves. `None` if `raw` has no `=`, or either half is
/// empty.
fn split_table_override(raw: &str) -> Option<(&str, &str)> {
    let (table, value) = raw.split_once('=')?;
    let table = table.trim();
    let value = value.trim();
    if table.is_empty() || value.is_empty() {
        return None;
    }
    Some((table, value))
}

/// Run the `generate` command: assemble a [`GenerateRequest`] from `args`,
/// run it, and report the outcome.
///
/// Exit codes: `2` for a CLI usage problem (a clap-level conflict, or a
/// post-clap one [`GenerateArgs::try_into_request`] catches); `1` for a
/// model/compile failure ([`GenerateError::Diagnostics`]), a `--strict` run
/// with warnings, or any other runtime error; `0` on success.
pub fn run(args: GenerateArgs) -> anyhow::Result<ExitCode> {
    let prepared = match args.try_into_request() {
        Ok(prepared) => prepared,
        Err(RequestError::Usage(message)) => {
            eprintln!("error: {message}");
            return Ok(ExitCode::from(2));
        }
        Err(RequestError::Unavailable(message)) => return Err(anyhow::anyhow!(message)),
    };

    let PreparedRequest {
        request,
        mode,
        explain,
        json,
        quiet,
        stdout_temp,
        emit_stdout_temp,
    } = prepared;

    match Generate::run(request) {
        Ok(report) => {
            if let Some(temp) = &stdout_temp {
                stream_to_stdout(temp.path())?;
            }
            if let Some(temp) = &emit_stdout_temp {
                stream_to_stdout(temp.path())?;
            }

            write_report(&report, mode, explain, json, quiet, stdout_temp.is_some())?;
            Ok(ExitCode::SUCCESS)
        }
        Err(GenerateError::Diagnostics(bag)) => {
            write_diagnostics(&bag, json)?;
            Ok(ExitCode::FAILURE)
        }
        Err(GenerateError::Diagnostic(diagnostic)) => {
            write_diagnostics(
                &DiagnosticBag {
                    diagnostics: vec![*diagnostic],
                },
                json,
            )?;
            Ok(ExitCode::FAILURE)
        }
        Err(error) => Err(error.into()),
    }
}

/// Stream the SQL rendered to a temporary file (the stdout-output case; see
/// [`PreparedRequest::stdout_temp`]) to the process's stdout.
fn stream_to_stdout(path: &Path) -> anyhow::Result<()> {
    let mut file = File::open(path)?;
    io::copy(&mut file, &mut io::stdout())?;
    Ok(())
}

/// The label a report's `mode` field carries, matching the CLI flag that
/// selected it (`generate` is the default; `check`/`dry_run` opt in).
fn mode_label(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Generate => "generate",
        RunMode::Check => "check",
        RunMode::DryRun => "dry_run",
        RunMode::EmitModel => "emit_model",
    }
}

/// The human-readable one-line summary for a successful run.
fn summary_line(mode: RunMode, rows_written: u64) -> String {
    match mode {
        RunMode::Generate => format!("Generated {rows_written} row(s)."),
        RunMode::Check => "Check passed: the model compiles.".to_string(),
        RunMode::DryRun => format!("Dry run: the model would generate {rows_written} row(s)."),
        RunMode::EmitModel => "Wrote resolved model.".to_string(),
    }
}

/// Report a successful [`GenerateReport`], respecting `--json`/`--quiet`.
///
/// `sql_on_stdout` is set when generated SQL was just streamed to stdout
/// ([`stream_to_stdout`]); the non-JSON summary then goes to stderr instead,
/// so it never lands inside the SQL stream.
fn write_report(
    report: &GenerateReport,
    mode: RunMode,
    explain: bool,
    json: bool,
    quiet: bool,
    sql_on_stdout: bool,
) -> anyhow::Result<()> {
    if json {
        let payload = GenerateJsonOutput {
            mode: mode_label(mode).to_string(),
            rows_written: report.rows_written,
            effective_seed: report.effective_seed,
            diagnostics: diagnostic_entries(&report.diagnostics),
            explain: if explain {
                explain_entries(&report.explain)
            } else {
                Vec::new()
            },
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    if quiet {
        for diagnostic in report
            .diagnostics
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == Severity::Advisory)
        {
            eprintln!("{diagnostic}");
        }
        return Ok(());
    }

    let line = summary_line(mode, report.rows_written);
    if sql_on_stdout {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
    for diagnostic in &report.diagnostics.diagnostics {
        eprintln!("{diagnostic}");
    }
    if explain {
        print_explain(&report.explain);
    }
    Ok(())
}

/// Print the `--explain` inference detail to stderr (never values).
fn print_explain(columns: &[ExplainColumn]) {
    for column in columns {
        eprintln!(
            "explain[{}] {} via {} (confidence {}{})",
            column.column,
            column.reason,
            column.generator_kind,
            column.confidence,
            if column.source_derived {
                ", source-derived"
            } else {
                ""
            },
        );
        for rejected in &column.rejected {
            eprintln!(
                "    rejected {} ({}, precedence {}, confidence {})",
                rejected.generator_kind, rejected.reason, rejected.precedence, rejected.confidence,
            );
        }
    }
}

/// Report a failed run's diagnostics ([`GenerateError::Diagnostics`], or a
/// `--strict` run that only had warnings), respecting `--json`.
fn write_diagnostics(bag: &DiagnosticBag, json: bool) -> anyhow::Result<()> {
    if json {
        let payload = serde_json::json!({ "diagnostics": diagnostic_entries(bag) });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        for diagnostic in &bag.diagnostics {
            eprintln!("{diagnostic}");
        }
    }
    Ok(())
}

/// JSON report for `generate --json`.
#[derive(Serialize, JsonSchema)]
pub(crate) struct GenerateJsonOutput {
    mode: String,
    rows_written: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_seed: Option<u64>,
    diagnostics: Vec<DiagnosticEntry>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    explain: Vec<ExplainEntry>,
}

/// One column's inference decision, reshaped for JSON `--explain` reporting.
#[derive(Serialize, JsonSchema)]
pub(crate) struct ExplainEntry {
    column: String,
    reason: String,
    confidence: String,
    generator_kind: String,
    source_derived: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rejected: Vec<ExplainRejectedEntry>,
}

/// One rejected inference alternative, reshaped for JSON reporting.
#[derive(Serialize, JsonSchema)]
pub(crate) struct ExplainRejectedEntry {
    generator_kind: String,
    reason: String,
    precedence: String,
    confidence: String,
}

fn explain_entries(columns: &[ExplainColumn]) -> Vec<ExplainEntry> {
    columns
        .iter()
        .map(|column| ExplainEntry {
            column: column.column.clone(),
            reason: column.reason.clone(),
            confidence: column.confidence.clone(),
            generator_kind: column.generator_kind.clone(),
            source_derived: column.source_derived,
            rejected: column
                .rejected
                .iter()
                .map(|rejected| ExplainRejectedEntry {
                    generator_kind: rejected.generator_kind.clone(),
                    reason: rejected.reason.clone(),
                    precedence: rejected.precedence.clone(),
                    confidence: rejected.confidence.clone(),
                })
                .collect(),
        })
        .collect()
}

/// One [`crate::diagnostic::Diagnostic`], reshaped for JSON reporting
/// (`DiagnosticBag`/`Diagnostic` themselves don't derive [`JsonSchema`]).
#[derive(Serialize, JsonSchema)]
pub(crate) struct DiagnosticEntry {
    code: String,
    severity: String,
    path: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    documentation_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    related: Vec<RelatedLocationEntry>,
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct RelatedLocationEntry {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

fn diagnostic_entries(bag: &DiagnosticBag) -> Vec<DiagnosticEntry> {
    bag.diagnostics
        .iter()
        .map(|diagnostic| DiagnosticEntry {
            code: diagnostic.code.clone(),
            severity: match diagnostic.severity {
                Severity::Info => "info".to_string(),
                Severity::Advisory => "advisory".to_string(),
                Severity::Warning => "warning".to_string(),
                Severity::Error => "error".to_string(),
            },
            path: diagnostic.path.clone(),
            message: diagnostic.message.clone(),
            documentation_url: diagnostic.documentation_url(),
            help: diagnostic.help.clone(),
            related: diagnostic
                .related
                .iter()
                .map(|location| RelatedLocationEntry {
                    path: location.path.clone(),
                    description: location.description.clone(),
                })
                .collect(),
        })
        .collect()
}
