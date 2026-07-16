//! Shared helpers for the CLI command layer.

use crate::parser::{detect_dialect_from_file, DialectConfidence, SqlDialect};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::time::Duration;

// Help heading constants shared by the per-command clap Args structs.
pub(crate) const INPUT_OUTPUT: &str = "Input/Output";
pub(crate) const FILTERING: &str = "Filtering";
pub(crate) const MODE: &str = "Mode";
pub(crate) const BEHAVIOR: &str = "Behavior";
pub(crate) const LIMITS: &str = "Limits";
pub(crate) const OUTPUT_FORMAT: &str = "Output";

/// Treat an output path of `-` as "write to stdout" (Unix convention).
pub(crate) fn dash_is_stdout(output: Option<PathBuf>) -> Option<PathBuf> {
    output.filter(|p| p.as_os_str() != "-")
}

/// Full-width byte progress bar used by single-file command runs.
pub(crate) fn byte_progress_bar(len: u64) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
        )
        .expect("progress template is static and valid")
        .progress_chars("█▓▒░  ")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Compact indented byte progress bar used per file by multi-file (glob) runs.
pub(crate) fn compact_progress_bar(len: u64) -> ProgressBar {
    let pb = ProgressBar::new(len);
    pb.set_style(
        ProgressStyle::with_template(
            "  {spinner:.green} [{bar:30.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
        )
        .expect("progress template is static and valid")
        .progress_chars("█▓▒░  "),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Resolve the SQL dialect for `file`, returning the detection confidence.
///
/// If `explicit` is given it is parsed via [`SqlDialect::from_str`] and
/// reported as [`DialectConfidence::High`]. Otherwise the dialect is
/// auto-detected from the file contents (`detect_dialect_from_file` opens
/// through `open_input`, so compressed/zipped input is handled transparently).
///
/// Unless `quiet` is set, auto-detection prints a
/// `Auto-detected dialect: <dialect> (<confidence> confidence)` banner to
/// stderr so it never pollutes piped stdout (JSON or SQL output).
pub(crate) fn resolve_dialect_with_confidence(
    file: &Path,
    explicit: Option<&str>,
    quiet: bool,
) -> anyhow::Result<(SqlDialect, DialectConfidence)> {
    match explicit {
        Some(d) => {
            let parsed: SqlDialect = d.parse().map_err(|e: String| anyhow::anyhow!(e))?;
            Ok((parsed, DialectConfidence::High))
        }
        None => {
            let result = detect_dialect_from_file(file)?;
            if !quiet {
                eprintln!(
                    "Auto-detected dialect: {} ({})",
                    result.dialect,
                    confidence_label(result.confidence)
                );
            }
            Ok((result.dialect, result.confidence))
        }
    }
}

/// [`resolve_dialect_with_confidence`] for callers that only need the dialect.
pub(crate) fn resolve_dialect(
    file: &Path,
    explicit: Option<&str>,
    quiet: bool,
) -> anyhow::Result<SqlDialect> {
    resolve_dialect_with_confidence(file, explicit, quiet).map(|(dialect, _)| dialect)
}

/// Human-readable label for a detection confidence level.
pub(crate) fn confidence_label(confidence: DialectConfidence) -> &'static str {
    match confidence {
        DialectConfidence::High => "high confidence",
        DialectConfidence::Medium => "medium confidence",
        DialectConfidence::Low => "low confidence",
    }
}
