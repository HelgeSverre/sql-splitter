use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "sql-splitter-migration-spike")]
#[command(about = "EXPERIMENTAL SPIKE — NOT FOR PRODUCTION")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the implemented contract boundary.
    Status,
    /// Run the Phase 1–5 contract path against in-memory fixtures.
    Demo {
        /// Directory for new protected plan and state artifacts.
        #[arg(long)]
        artifact_dir: PathBuf,
    },
    /// Inspect two live PostgreSQL catalogs and write a deterministic plan.
    PlanPostgres {
        /// Source endpoint TOML. Credentials are referenced through an environment variable.
        #[arg(long)]
        source_config: PathBuf,
        /// Target endpoint TOML. Plan-only opens a read-only catalog transaction.
        #[arg(long)]
        target_config: PathBuf,
        /// New protected plan artifact. Existing files are not replaced.
        #[arg(long)]
        plan_output: PathBuf,
    },
    /// Execute one reviewed same-dialect PostgreSQL plan into an empty target.
    ExecutePostgres {
        /// Exact protected reviewed plan artifact.
        #[arg(long)]
        plan_input: PathBuf,
        /// Source endpoint TOML with a server-enforced read-only role.
        #[arg(long)]
        source_config: PathBuf,
        /// Empty migration-owned target endpoint TOML.
        #[arg(long)]
        target_config: PathBuf,
        /// External approval or change-record reference bound into state.
        #[arg(long)]
        approval_ref: String,
        /// Explicit write authorization gate.
        #[arg(long, required = true)]
        execute: bool,
        /// Required exact schema and canonical row verification gate.
        #[arg(long, required = true)]
        strict_verification: bool,
        /// New protected durable migration state artifact.
        #[arg(long)]
        state_output: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    eprintln!("{}", sql_splitter::migration::SPIKE_WARNING);
    match cli.command {
        Command::Status => {
            println!("fixture-backed contract runner and read-only PostgreSQL plan adapter");
        }
        Command::Demo { artifact_dir } => {
            let result = sql_splitter::migration::runner::run_fixture_spike(artifact_dir)?;
            println!("copied and strictly verified {} rows", result.copied_rows);
            println!("plan: {}", result.plan.display());
            println!("state: {}", result.state.display());
            println!("plan hash: {}", result.plan_hash);
        }
        Command::PlanPostgres {
            source_config,
            target_config,
            plan_output,
        } => {
            let plan = sql_splitter::migration::postgres::write_live_plan(
                source_config,
                target_config,
                &plan_output,
            )?;
            println!("plan: {}", plan_output.display());
            println!("plan hash: {}", plan.plan_hash);
            println!(
                "unsupported objects: {} (execution-blocking: {})",
                plan.plan.unsupported_objects.objects.len(),
                plan.plan.unsupported_objects.blocks_execution()
            );
        }
        Command::ExecutePostgres {
            plan_input,
            source_config,
            target_config,
            approval_ref,
            execute,
            strict_verification,
            state_output,
        } => {
            if !execute || !strict_verification {
                anyhow::bail!("--execute and --strict-verification are required");
            }
            let report = sql_splitter::migration::runner::execute_postgres_plan(
                plan_input,
                source_config,
                target_config,
                &approval_ref,
                &state_output,
            )?;
            println!("state: {}", report.state.display());
            println!("copied rows: {}", report.copied_rows);
            println!("committed chunks: {}", report.committed_chunks);
        }
    }
    Ok(())
}
