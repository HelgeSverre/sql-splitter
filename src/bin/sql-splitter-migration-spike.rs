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
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    eprintln!("{}", sql_splitter::migration::SPIKE_WARNING);
    match cli.command {
        Command::Status => {
            println!("fixture-backed Phase 1-5 contract spike; no live database adapter");
        }
        Command::Demo { artifact_dir } => {
            let result = sql_splitter::migration::runner::run_fixture_spike(artifact_dir)?;
            println!("copied and strictly verified {} rows", result.copied_rows);
            println!("plan: {}", result.plan.display());
            println!("state: {}", result.state.display());
            println!("plan hash: {}", result.plan_hash);
        }
    }
    Ok(())
}
