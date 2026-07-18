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
fn explicit_stdout_sql_and_emit_config_stdout_exit_with_usage_code() {
    // Both generated SQL (`-o -`) and the emitted model (`--emit-config -`)
    // claim stdout at once — a collision `try_into_request` rejects with the
    // usage exit code `2`.
    let output = sql_splitter_bin()
        .args([
            "generate",
            "--config",
            SIMPLE_MODEL,
            "--emit-config",
            "-",
            "-o",
            "-",
        ])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn emit_config_stdout_without_sql_output_emits_the_model() {
    // `--emit-config -` with no `-o` writes the resolved model to stdout and
    // generates no SQL — a valid EmitModel run, not a stdout collision.
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--emit-config", "-"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("kind: model"), "stdout: {stdout}");
    assert!(!stdout.contains("INSERT INTO"), "stdout: {stdout}");
}

#[test]
fn check_with_an_input_dump_exits_with_usage_code() {
    let output = sql_splitter_bin()
        .args(["generate", "dump.sql", "--check"])
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).expect("stderr is valid UTF-8");
    let normalized = stderr.to_ascii_lowercase();
    assert!(!normalized.contains("phase"), "stderr: {stderr}");
    assert!(!normalized.contains("task"), "stderr: {stderr}");
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
// End-to-end: --verify generates, audits, and atomically publishes
// ===========================================================================

#[test]
fn verify_generates_audits_and_publishes_to_a_real_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("synthetic.sql");
    let output = sql_splitter_bin()
        .args(["generate", "--config", SIMPLE_MODEL, "--verify", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out.exists(), "verified output should be published");
    let published = std::fs::read_to_string(&out).unwrap();
    assert!(published.contains("INSERT INTO"), "{published}");
}

// ===========================================================================
// Dump-to-model and dump-to-SQL workflows
// ===========================================================================

const PRODUCTION_DUMP: &str = "tests/fixtures/generate/production_shape.sql";

/// Workflow 1: `generate production.sql -o synthetic.sql` profiles the dump,
/// infers a model, and generates SQL — no config required.
#[test]
fn dump_workflow_infers_and_generates() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("synthetic.sql");

    let output = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--seed", "42", "-o"])
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
    assert!(sql.contains("INSERT INTO"), "sql: {sql}");
    // Tables from the dump are present.
    assert!(sql.contains("users"), "sql: {sql}");
}

/// Workflow 2: `generate production.sql --emit-config model.yaml --dry-run`
/// writes ONLY a complete model — no SQL anywhere.
#[test]
fn dump_workflow_emit_config_dry_run_writes_only_a_model() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");

    let output = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--emit-config"])
        .arg(&model)
        .arg("--dry-run")
        .output()
        .expect("failed to run sql-splitter");

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let yaml = fs::read_to_string(&model).unwrap();
    assert!(yaml.contains("kind: model"), "yaml: {yaml}");
    // Self-contained: inference disabled, observed counts retained, no raw
    // sample values leaked into the document.
    assert!(yaml.contains("inference: disabled"), "yaml: {yaml}");
    assert!(yaml.contains("kind: observed"), "yaml: {yaml}");

    // No SQL was produced on stdout.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("INSERT INTO"), "stdout: {stdout}");
}

/// Workflow 3: `generate --config model.yaml -o synthetic.sql` needs no source
/// dump. We emit a model from the dump first, then generate purely from it.
#[test]
fn dump_workflow_config_only_needs_no_source() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");
    let out = dir.path().join("synthetic.sql");

    let emit = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--seed", "42", "--emit-config"])
        .arg(&model)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(emit.status.code(), Some(0));

    // No input dump here — only the emitted model.
    let gen = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&model)
        .arg("-o")
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(
        gen.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&gen.stderr)
    );
    assert!(fs::read_to_string(&out).unwrap().contains("INSERT INTO"));
}

/// Workflow 4: `generate production.sql --emit-config resolved.yaml -o out.sql`
/// writes AND executes the SAME resolved decisions. Reloading the emitted model
/// (seeded) reproduces byte-identical SQL, and removing the optional `profiles`
/// and `source` blocks from it does not change seeded output.
#[test]
fn dump_workflow_emit_and_execute_are_consistent_and_trimmable() {
    let dir = tempfile::tempdir().unwrap();
    let resolved = dir.path().join("resolved.yaml");
    let out1 = dir.path().join("out1.sql");

    // Emit + execute in one seeded run.
    let run = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--seed", "42", "--emit-config"])
        .arg(&resolved)
        .arg("-o")
        .arg(&out1)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(
        run.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let sql1 = fs::read_to_string(&out1).unwrap();
    assert!(sql1.contains("INSERT INTO"), "sql1: {sql1}");

    // Reload the emitted model (which carries seed 42) and regenerate: the
    // resolved decisions execute identically.
    let out2 = dir.path().join("out2.sql");
    let reload = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&resolved)
        .arg("-o")
        .arg(&out2)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(reload.status.code(), Some(0));
    assert_eq!(sql1, fs::read_to_string(&out2).unwrap());

    // Removing either optional metadata block — `profiles` or `source` — does
    // not change seeded output. (The compiler keeps one provenance marker as a
    // guard against a bare `observed` count, so each is dropped independently.)
    for drop_key in ["profiles", "source"] {
        let mut doc: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&fs::read_to_string(&resolved).unwrap()).unwrap();
        if let serde_yaml_ng::Value::Mapping(map) = &mut doc {
            map.remove(serde_yaml_ng::Value::from(drop_key));
        }
        let trimmed = dir.path().join(format!("trimmed_{drop_key}.yaml"));
        fs::write(&trimmed, serde_yaml_ng::to_string(&doc).unwrap()).unwrap();

        let out3 = dir.path().join(format!("out3_{drop_key}.sql"));
        let trimmed_run = sql_splitter_bin()
            .args(["generate", "--config"])
            .arg(&trimmed)
            .arg("-o")
            .arg(&out3)
            .output()
            .expect("failed to run sql-splitter");
        assert_eq!(
            trimmed_run.status.code(),
            Some(0),
            "dropping `{drop_key}` stderr: {}",
            String::from_utf8_lossy(&trimmed_run.stderr)
        );
        assert_eq!(
            sql1,
            fs::read_to_string(&out3).unwrap(),
            "dropping `{drop_key}` changed seeded output"
        );
    }
}

/// The conservative source-derived safety notice fires (the dump's categorical
/// `status` column replays observed literals), survives `--quiet`, and the JSON
/// report lists paths and rule kinds but never the values.
#[test]
fn dump_workflow_source_values_notice_survives_quiet_and_json_hides_values() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("synthetic.sql");

    // --quiet: the GEN-SOURCE-VALUES notice must still reach stderr.
    let quiet = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--seed", "42", "--quiet", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(quiet.status.code(), Some(0));
    let stderr = String::from_utf8_lossy(&quiet.stderr);
    assert!(stderr.contains("GEN-SOURCE-VALUES"), "stderr: {stderr}");
    // The notice never leaks a value (e.g. the observed status `paid`).
    assert!(!stderr.contains("paid"), "stderr leaked a value: {stderr}");

    // --json: structured source_values carry paths + rule kinds, not values.
    let out_json = dir.path().join("synthetic_json.sql");
    let json = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--seed", "42", "--json", "-o"])
        .arg(&out_json)
        .output()
        .expect("failed to run sql-splitter");
    assert_eq!(json.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&json.stdout);
    let report: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("failed to parse JSON: {stdout}"));
    let source_values = report["source_values"]
        .as_array()
        .expect("source_values array");
    assert!(
        !source_values.is_empty(),
        "expected source_values: {stdout}"
    );
    for entry in source_values {
        assert!(entry.get("path").is_some(), "entry missing path: {entry}");
        assert!(
            entry.get("rule_kind").is_some(),
            "entry missing rule_kind: {entry}"
        );
    }
    // No observed value string appears anywhere in the JSON report.
    assert!(!stdout.contains("paid"), "json leaked a value: {stdout}");
}

/// A seeded emit records the seed for byte-equivalent reload; an unseeded run
/// records the effective seed in its report while emitting no seed.
#[test]
fn dump_workflow_unseeded_run_records_effective_seed() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");
    let out = dir.path().join("synthetic.sql");

    let output = sql_splitter_bin()
        .args(["generate", PRODUCTION_DUMP, "--json", "--emit-config"])
        .arg(&model)
        .arg("-o")
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
    let report: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("failed to parse JSON: {stdout}"));
    assert!(
        report["effective_seed"].as_u64().is_some(),
        "expected effective_seed: {stdout}"
    );

    // The emitted model records no seed (unseeded runs stay fresh on reload).
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&fs::read_to_string(&model).unwrap()).unwrap();
    let seed = doc.get("seed");
    assert!(
        seed.is_none() || seed == Some(&serde_yaml_ng::Value::Null),
        "emitted model should record no seed, got {seed:?}"
    );
}

// ===========================================================================
// End-to-end: complete-model generate/check/dry-run happy paths
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

// ===========================================================================
// Lossy cross-dialect warnings are surfaced and strict-promoted
// ===========================================================================

/// A model whose MySQL `ENUM` column narrows to a plain string on any other
/// dialect — a genuinely lossy cross-dialect conversion.
const LOSSY_ENUM_MODEL: &str = r#"
version: 1
kind: model
source: { dialect: mysql }
defaults: { inference: disabled }
seed: 7
tables:
  widgets:
    rows: { kind: fixed, count: 3 }
    schema:
      name: widgets
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: kind, type: "enum('a','b')", nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      kind: { generator: { kind: choice, values: [a, b] } }
"#;

#[test]
fn lossy_cross_dialect_conversion_warns_on_a_normal_run() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");
    let out = dir.path().join("out.sql");
    fs::write(&model, LOSSY_ENUM_MODEL).unwrap();

    // Render the MySQL-source model to PostgreSQL: ENUM -> VARCHAR(255) is lossy.
    let run = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&model)
        .args(["--dialect", "postgres", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    let stderr = String::from_utf8_lossy(&run.stderr);
    assert_eq!(run.status.code(), Some(0), "stderr: {stderr}");
    // The lossy conversion is no longer silent: it reaches the report diagnostics
    // and is printed (previously `renderer.warnings()` was dropped entirely).
    assert!(
        stderr.contains("GEN-LOSSY-TYPE"),
        "lossy warning not surfaced; stderr: {stderr}"
    );
    // The narrowed type is what actually landed in the DDL.
    assert!(fs::read_to_string(&out).unwrap().contains("VARCHAR(255)"));
}

#[test]
fn lossy_cross_dialect_conversion_fails_under_strict() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");
    let out = dir.path().join("out.sql");
    fs::write(&model, LOSSY_ENUM_MODEL).unwrap();

    // The SAME run under --strict promotes the lossy warning to a failure.
    let run = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&model)
        .args(["--dialect", "postgres", "--strict", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    let stderr = String::from_utf8_lossy(&run.stderr);
    assert_eq!(
        run.status.code(),
        Some(1),
        "expected strict failure; stderr: {stderr}"
    );
    assert!(stderr.contains("GEN-LOSSY-TYPE"), "stderr: {stderr}");
}

#[test]
fn same_dialect_run_has_no_lossy_warning() {
    let dir = tempfile::tempdir().unwrap();
    let model = dir.path().join("model.yaml");
    let out = dir.path().join("out.sql");
    fs::write(&model, LOSSY_ENUM_MODEL).unwrap();

    // Rendering to the model's own dialect maps nothing, so --strict still passes.
    let run = sql_splitter_bin()
        .args(["generate", "--config"])
        .arg(&model)
        .args(["--dialect", "mysql", "--strict", "-o"])
        .arg(&out)
        .output()
        .expect("failed to run sql-splitter");

    let stderr = String::from_utf8_lossy(&run.stderr);
    assert_eq!(run.status.code(), Some(0), "stderr: {stderr}");
    assert!(!stderr.contains("GEN-LOSSY-TYPE"), "stderr: {stderr}");
}
