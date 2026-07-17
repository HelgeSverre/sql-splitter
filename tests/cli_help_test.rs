//! Documentation-accuracy gate for `sql-splitter generate`.
//!
//! `docs/generate/`, `website/src/content/docs/commands/generate.mdx`,
//! `README.md`, and `skills/sql-splitter/SKILL.md` all describe `generate`'s
//! CLI surface from the canonical design spec's CLI-contract tables. This
//! test is the trip wire against that surface silently drifting: it asserts
//! the `generate` subcommand is discoverable from top-level `--help`, and
//! that every flag `docs/generate/README.md`/`generate.mdx` document
//! actually appears in `generate --help`'s own output. If a flag is
//! renamed, removed, or a new one is added to `src/cmd/generate.rs` without
//! a matching doc update, this test fails before a reader hits a stale page.

use std::process::Command;

fn sql_splitter_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

fn run_help(args: &[&str]) -> String {
    let output = sql_splitter_bin()
        .args(args)
        .output()
        .expect("sql-splitter binary runs");
    assert!(
        output.status.success(),
        "{:?} exited with {:?}\nstderr: {}",
        args,
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("help output is valid UTF-8")
}

#[test]
fn top_level_help_lists_the_generate_subcommand() {
    let help = run_help(&["--help"]);
    assert!(
        help.contains("generate"),
        "top-level --help does not mention the `generate` subcommand:\n{help}"
    );
}

#[test]
fn generate_help_documents_every_cli_contract_flag() {
    let help = run_help(&["generate", "--help"]);

    // Every flag from the canonical spec's CLI-contract tables
    // (docs/superpowers/specs/2026-07-16-synthetic-data-generation-design.md,
    // "CLI contract"), cross-checked against `GenerateArgs`
    // (src/cmd/generate.rs) — including `--mssql-production-style`/
    // `--mssql-go`, which Task 31 wired end to end after the spec was written.
    let expected_flags = [
        // Input and model options
        "--config",
        "--emit-config",
        "--profile-depth",
        "--profile-sample",
        "--input-dialect",
        // Volume options
        "--scale",
        "--rows",
        "--table-rows",
        "--table-scale",
        "--max-rows",
        "--tables",
        "--exclude",
        // Randomness options
        "--seed",
        "--randomize",
        // Rendering options
        "--output",
        "--dialect",
        "--schema-only",
        "--data-only",
        "--batch-size",
        "--no-copy",
        "--compress",
        "--mssql-production-style",
        "--mssql-go",
        // Preflight and reporting options
        "--check",
        "--dry-run",
        "--verify",
        "--explain",
        "--strict",
        "--progress",
        "--json",
        "--quiet",
    ];

    let missing: Vec<&str> = expected_flags
        .iter()
        .copied()
        .filter(|flag| !help.contains(flag))
        .collect();

    assert!(
        missing.is_empty(),
        "generate --help is missing documented flag(s) {missing:?}\n\nfull help output:\n{help}"
    );
}

#[test]
fn generate_help_documents_short_flags() {
    let help = run_help(&["generate", "--help"]);
    for short in ["-c", "-o"] {
        assert!(
            help.contains(short),
            "generate --help is missing short flag `{short}`:\n{help}"
        );
    }
}

#[test]
fn generate_help_mentions_the_gen_alias() {
    let help = run_help(&["--help"]);
    assert!(
        help.contains("gen"),
        "top-level --help does not mention the `gen` alias for `generate`:\n{help}"
    );
}
