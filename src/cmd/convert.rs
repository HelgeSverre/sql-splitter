//! Convert command CLI handler.

use crate::convert::{self, ConvertConfig};
use crate::parser::SqlDialect;
use std::path::PathBuf;

pub fn run(
    file: PathBuf,
    output: Option<PathBuf>,
    from_dialect: Option<String>,
    to_dialect: String,
    strict: bool,
    progress: bool,
    dry_run: bool,
) -> anyhow::Result<()> {
    // Parse dialects
    let from = if let Some(d) = from_dialect {
        Some(
            d.parse::<SqlDialect>()
                .map_err(|e| anyhow::anyhow!("{}", e))?,
        )
    } else {
        None
    };

    let to = to_dialect
        .parse::<SqlDialect>()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let config = ConvertConfig {
        input: file,
        output,
        from_dialect: from,
        to_dialect: to,
        dry_run,
        progress,
        strict,
    };

    // Run conversion
    let stats = convert::run(config)?;

    // Print results
    print_stats(&stats, dry_run, progress);

    // Exit with error if strict mode had warnings
    if strict && !stats.warnings.is_empty() {
        anyhow::bail!(
            "Strict mode: {} warnings generated",
            stats.warnings.len()
        );
    }

    Ok(())
}

fn print_stats(stats: &convert::ConvertStats, dry_run: bool, progress: bool) {
    if !progress && !dry_run {
        return;
    }

    eprintln!();
    eprintln!("Conversion Statistics:");
    eprintln!("  Statements processed: {}", stats.statements_processed);
    eprintln!("  Statements converted: {}", stats.statements_converted);
    eprintln!("  Statements unchanged: {}", stats.statements_unchanged);
    eprintln!("  Statements skipped: {}", stats.statements_skipped);

    if !stats.warnings.is_empty() {
        eprintln!();
        eprintln!("Warnings ({}):", stats.warnings.len());
        for warning in &stats.warnings {
            eprintln!("  ⚠ {}", warning);
        }
    }

    if dry_run {
        eprintln!();
        eprintln!("(Dry run - no output written)");
    }
}
