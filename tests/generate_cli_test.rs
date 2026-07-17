//! CLI tests for `sql-splitter generate`.
//!
//! Two layers, matching the two places a conflict can be caught:
//!
//! - Pure clap-level conflicts (`conflicts_with`/`conflicts_with_all`) fail
//!   during [`Cli::try_parse_from`] itself, before any subcommand code runs.
//! - Value-conditional conflicts (e.g. `--json` claiming stdout at the same
//!   time as an explicit `--output -`) can't be expressed as a clap
//!   attribute, since `conflicts_with` only sees argument *presence*, not
//!   argument *values*. Those are rejected by
//!   `cmd::generate::GenerateArgs::try_into_request` instead, which is
//!   private to the `generate` module — so this layer is exercised end to
//!   end, as a subprocess, asserting on the process exit code (`2`, clap's
//!   own usage exit code, reused here so every usage rejection looks the
//!   same to a caller regardless of which layer caught it).

use std::fs;
use std::process::Command;

use clap::Parser;
use sql_splitter::cmd::Cli;

const SIMPLE_MODEL: &str = "tests/fixtures/generate/simple.yaml";

fn sql_splitter_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

// ===========================================================================
// Clap-level conflicts
// ===========================================================================

#[test]
fn generate_cli_rejects_conflicting_modes_and_stdout_owners() {
    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--scale",
        "0.1",
        "--rows",
        "10"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--check",
        "--dry-run"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--seed",
        "1",
        "--randomize"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--schema-only",
        "--data-only"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--quiet",
        "--progress"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--check",
        "--verify"
    ])
    .is_err());

    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--dry-run",
        "--verify"
    ])
    .is_err());
}

#[test]
fn generate_cli_parses_value_conditional_conflicts_clap_cannot_express() {
    // `--json --output -`: both a `--json` report and an explicit `-o -`
    // claim stdout. Clap's `conflicts_with` only tracks argument presence,
    // so it can't reject this by itself; it parses fine here and is instead
    // rejected by `try_into_request` (see the subprocess test below).
    assert!(Cli::try_parse_from([
        "sql-splitter",
        "generate",
        "model.yaml",
        "--json",
        "--output",
        "-"
    ])
    .is_ok());
}

// ===========================================================================
// End-to-end: usage conflicts caught post-clap (subprocess, exit code 2)
// ===========================================================================

#[test]
fn json_and_explicit_stdout_output_exit_with_usage_code() {
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--json",
            "--output",
            "-",
        ])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn json_and_explicit_emit_config_stdout_exit_with_usage_code() {
    // `--json` (the report) and `--emit-config -` both claim stdout; this
    // usage rejection must fire before `--emit-config`'s "not available
    // yet" error (exit `1`), so the exit code here is `2`, not `1`.
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--json",
            "--emit-config",
            "-",
        ])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn default_stdout_sql_and_emit_config_stdout_exit_with_usage_code() {
    // No `--json`, no `-o`: generated SQL defaults to stdout. `--emit-config
    // -` also claims stdout, so this is a collision too — again, this usage
    // rejection must fire before `--emit-config`'s "not available yet"
    // error, so the exit code is `2`, not `1`.
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--emit-config", "-"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn check_with_an_input_dump_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args(["generate", "dump.sql", "--check"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn verify_without_a_real_output_file_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--verify"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn zero_batch_size_exits_with_usage_code() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.sql");
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--batch-size",
            "0",
            "-o",
        ])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn malformed_table_rows_pattern_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--table-rows",
            "not-a-pattern",
            "--check",
        ])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn table_targeted_by_both_table_rows_and_table_scale_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--table-rows",
            "customers=10",
            "--table-scale",
            "customers=2.0",
            "--check",
        ])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn compress_with_stdout_output_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--compress", "gzip"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

// ===========================================================================
// End-to-end: not-yet-implemented flags fail clearly (exit code 1)
// ===========================================================================

#[test]
fn emit_config_is_not_yet_available() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("model.yaml");
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--emit-config"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not available"), "stderr: {stderr}");
}

// ===========================================================================
// End-to-end: complete-model generate/check/dry-run (Phase 1 happy paths)
// ===========================================================================

#[test]
fn generate_writes_sql_to_the_given_output_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("synthetic.sql");

    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--seed", "42", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sql = fs::read_to_string(&out).unwrap();
    assert!(sql.contains("INSERT INTO"));
}

#[test]
fn generate_defaults_to_stdout_without_json_or_output() {
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--seed", "42"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("INSERT INTO"), "stdout: {stdout}");
}

#[test]
fn check_mode_exits_zero_and_writes_no_sql() {
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--check"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("INSERT INTO"));
}

#[test]
fn dry_run_mode_exits_zero_and_writes_no_sql() {
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--dry-run"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("INSERT INTO"));
}

#[test]
fn json_report_to_a_file_output_is_valid_json_with_rows_written() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("synthetic.sql");

    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--json", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("failed to parse JSON: {stdout}"));
    assert_eq!(json["mode"], "generate");
    assert!(json["rows_written"].as_u64().unwrap() > 0);
    assert!(fs::read_to_string(&out).unwrap().contains("INSERT INTO"));
}

#[test]
fn same_seed_reproduces_identical_output() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.sql");
    let second = dir.path().join("second.sql");

    for out in [&first, &second] {
        let output = sql_splitter_bin()
            .args(["generate", "--config", SIMPLE_MODEL, "--seed", "7", "-o"])
            .arg(out)
            .output()
            .expect("failed to run sql-splitter");
        assert_eq!(output.status.code(), Some(0));
    }

    assert_eq!(
        fs::read_to_string(first).unwrap(),
        fs::read_to_string(second).unwrap()
    );
}

#[test]
fn different_seeds_produce_different_output() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first.sql");
    let second = dir.path().join("second.sql");

    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--seed", "7", "-o"])
        .arg(&first)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(output.status.code(), Some(0));

    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--seed", "8", "-o"])
        .arg(&second)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(output.status.code(), Some(0));

    assert_ne!(
        fs::read_to_string(first).unwrap(),
        fs::read_to_string(second).unwrap()
    );
}

#[test]
fn an_invalid_model_exits_with_failure_code_not_usage_code() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist.yaml");

    let output = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&missing)
        .arg("--check")
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(1));
}
