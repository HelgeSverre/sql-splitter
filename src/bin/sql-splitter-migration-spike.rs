use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ConsistencyMode {
    ConsistentSnapshot,
    WriteFence,
}

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
    /// Inspect one PostgreSQL source and write a read-only assessment and report.
    AssessPostgres {
        /// Source endpoint TOML with a server-enforced read-only role.
        #[arg(long)]
        source_config: PathBuf,
        /// New protected machine-readable assessment artifact.
        #[arg(long)]
        assessment_output: PathBuf,
        /// New protected deterministic Markdown report.
        #[arg(long)]
        report_output: PathBuf,
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
        /// Execution consistency contract recorded for review.
        #[arg(long, value_enum)]
        consistency: ConsistencyMode,
    },
    /// Install and durably record the source fence required by a write-fence plan.
    FenceInstallPostgres {
        #[arg(long)]
        plan_input: PathBuf,
        #[arg(long)]
        fence_admin_config: PathBuf,
        #[arg(long)]
        fence_artifact: PathBuf,
        #[arg(long, required = true)]
        execute: bool,
    },
    /// Attest an installed source fence without changing it.
    FenceAttestPostgres {
        #[arg(long)]
        fence_admin_config: PathBuf,
        #[arg(long)]
        fence_artifact: PathBuf,
    },
    /// Release an exact installed source fence after explicit authorization.
    FenceReleasePostgres {
        #[arg(long)]
        fence_admin_config: PathBuf,
        #[arg(long)]
        fence_artifact: PathBuf,
        #[arg(long)]
        approval_ref: String,
        #[arg(long, required = true)]
        execute: bool,
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
        /// Privileged source fence administrator configuration for write-fence plans.
        #[arg(long, requires = "fence_artifact")]
        fence_admin_config: Option<PathBuf>,
        /// Protected installed-fence artifact for write-fence plans.
        #[arg(long, requires = "fence_admin_config")]
        fence_artifact: Option<PathBuf>,
    },
    /// Resume only the intent embedded in an existing write-fenced state artifact.
    ResumePostgres {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        source_config: PathBuf,
        #[arg(long)]
        target_config: PathBuf,
        #[arg(long)]
        fence_admin_config: PathBuf,
        #[arg(long)]
        fence_artifact: PathBuf,
        #[arg(long, required = true)]
        execute: bool,
        #[arg(long, required = true)]
        strict_verification: bool,
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
        Command::AssessPostgres {
            source_config,
            assessment_output,
            report_output,
        } => {
            let assessment = sql_splitter::migration::postgres::write_live_assessment(
                source_config,
                &assessment_output,
                &report_output,
            )?;
            println!("assessment: {}", assessment_output.display());
            println!("report: {}", report_output.display());
            println!("plan hash: {}", assessment.reviewed_plan.plan_hash);
            println!(
                "unsupported objects: {} (execution-blocking: {})",
                assessment
                    .reviewed_plan
                    .plan
                    .unsupported_objects
                    .objects
                    .len(),
                assessment
                    .reviewed_plan
                    .plan
                    .unsupported_objects
                    .blocks_execution()
            );
        }
        Command::PlanPostgres {
            source_config,
            target_config,
            plan_output,
            consistency,
        } => {
            let consistency = match consistency {
                ConsistencyMode::ConsistentSnapshot => {
                    sql_splitter::migration::postgres::PostgresConsistencyMode::ConsistentSnapshot
                }
                ConsistencyMode::WriteFence => {
                    sql_splitter::migration::postgres::PostgresConsistencyMode::WriteFence
                }
            };
            let plan = sql_splitter::migration::postgres::write_live_plan_with_consistency(
                source_config,
                target_config,
                &plan_output,
                consistency,
            )?;
            println!("plan: {}", plan_output.display());
            println!("plan hash: {}", plan.plan_hash);
            println!(
                "unsupported objects: {} (execution-blocking: {})",
                plan.plan.unsupported_objects.objects.len(),
                plan.plan.unsupported_objects.blocks_execution()
            );
        }
        Command::FenceInstallPostgres {
            plan_input,
            fence_admin_config,
            fence_artifact,
            execute,
        } => {
            if !execute {
                anyhow::bail!("--execute is required");
            }
            let reviewed: sql_splitter::migration::plan::ReviewedPlan =
                sql_splitter::migration::artifact::read_json(plan_input)?;
            let admin = sql_splitter::migration::postgres::PostgresEndpointConfig::read(
                fence_admin_config,
            )?;
            let installed = sql_splitter::migration::postgres_fence::install_postgres_write_fence(
                &admin,
                &reviewed,
                &fence_artifact,
            )?;
            println!("fence artifact: {}", fence_artifact.display());
            println!("fence evidence: {:?}", installed.evidence);
        }
        Command::FenceAttestPostgres {
            fence_admin_config,
            fence_artifact,
        } => {
            let admin = sql_splitter::migration::postgres::PostgresEndpointConfig::read(
                fence_admin_config,
            )?;
            let installed: sql_splitter::migration::postgres_fence::InstalledPostgresFence =
                sql_splitter::migration::artifact::read_json(fence_artifact)?;
            let inventory = sql_splitter::migration::postgres_fence::attest_postgres_write_fence(
                &admin,
                &installed.evidence,
            )?;
            println!("attested tables: {}", inventory.tables.len());
        }
        Command::FenceReleasePostgres {
            fence_admin_config,
            fence_artifact,
            approval_ref,
            execute,
        } => {
            if !execute || approval_ref.trim().is_empty() {
                anyhow::bail!("--execute and a non-empty --approval-ref are required");
            }
            let admin = sql_splitter::migration::postgres::PostgresEndpointConfig::read(
                fence_admin_config,
            )?;
            let installed: sql_splitter::migration::postgres_fence::InstalledPostgresFence =
                sql_splitter::migration::artifact::read_json(fence_artifact)?;
            let generation = match &installed.evidence {
                sql_splitter::migration::journal::ConsistencyEvidence::WriteFence {
                    generation,
                    ..
                } => generation,
                _ => anyhow::bail!("fence artifact does not contain write-fence evidence"),
            };
            sql_splitter::migration::postgres_fence::release_postgres_write_fence(
                &admin,
                generation,
                &installed.token,
            )?;
            println!("released fence generation: {generation}");
        }
        Command::ExecutePostgres {
            plan_input,
            source_config,
            target_config,
            approval_ref,
            execute,
            strict_verification,
            state_output,
            fence_admin_config,
            fence_artifact,
        } => {
            if !execute || !strict_verification {
                anyhow::bail!("--execute and --strict-verification are required");
            }
            let report = match (fence_admin_config, fence_artifact) {
                (Some(admin), Some(fence)) => {
                    sql_splitter::migration::runner::execute_postgres_fenced_plan(
                        plan_input,
                        source_config,
                        target_config,
                        admin,
                        fence,
                        &approval_ref,
                        &state_output,
                    )?
                }
                (None, None) => sql_splitter::migration::runner::execute_postgres_plan(
                    plan_input,
                    source_config,
                    target_config,
                    &approval_ref,
                    &state_output,
                )?,
                _ => unreachable!("clap requires both fence arguments"),
            };
            println!("state: {}", report.state.display());
            println!("copied rows: {}", report.copied_rows);
            println!("committed chunks: {}", report.committed_chunks);
        }
        Command::ResumePostgres {
            state,
            source_config,
            target_config,
            fence_admin_config,
            fence_artifact,
            execute,
            strict_verification,
        } => {
            if !execute || !strict_verification {
                anyhow::bail!("--execute and --strict-verification are required");
            }
            let report = sql_splitter::migration::runner::resume_postgres_fenced_plan(
                &state,
                source_config,
                target_config,
                fence_admin_config,
                fence_artifact,
            )?;
            println!("state: {}", report.state.display());
            println!("copied rows: {}", report.copied_rows);
            println!("committed chunks: {}", report.committed_chunks);
        }
    }
    Ok(())
}
