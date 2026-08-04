//! `--help` gains a pointer to the LLM doc set when an AI coding agent is
//! driving the CLI, and only then. Detection reads process environment, so
//! these drive the real binary with an explicit environment rather than
//! mutating the (process-global) test environment.

use std::process::Command;

fn help_with_env(vars: &[(&str, &str)], unset: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sql-splitter"));
    for name in unset {
        cmd.env_remove(name);
    }
    for (name, value) in vars {
        cmd.env(name, value);
    }
    let output = cmd
        .arg("--help")
        .output()
        .expect("sql-splitter binary runs");
    assert!(output.status.success(), "--help exited non-zero");
    String::from_utf8(output.stdout).expect("help output is valid UTF-8")
}

const HINT_MARKER: &str = "Notes for AI agents:";

#[test]
fn help_stays_human_when_no_agent_is_detected() {
    // The override also covers the case where the test runner is itself an agent.
    let help = help_with_env(&[("SQL_SPLITTER_AGENT", "0")], &[]);
    assert!(
        !help.contains(HINT_MARKER),
        "human --help leaked the agent note:\n{help}"
    );
    assert!(help.contains("Common workflows:"), "human footer missing");
}

#[test]
fn help_points_agents_at_the_llm_docs() {
    let help = help_with_env(&[("CLAUDECODE", "1")], &["SQL_SPLITTER_AGENT"]);
    assert!(
        help.contains(HINT_MARKER) && help.contains("https://sql-splitter.dev/llms.txt"),
        "agent --help is missing the LLM doc pointer:\n{help}"
    );
    assert!(
        help.contains("Common workflows:"),
        "agent --help dropped the human footer:\n{help}"
    );
}

#[test]
fn detection_can_be_forced_on_without_a_vendor_variable() {
    let help = help_with_env(&[("SQL_SPLITTER_AGENT", "1")], &[]);
    assert!(
        help.contains(HINT_MARKER),
        "forced-on hint missing:\n{help}"
    );
}
