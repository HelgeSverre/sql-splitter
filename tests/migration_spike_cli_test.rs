#![cfg(feature = "enterprise-migration-spike")]

use std::process::Command;

fn spike() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter-migration-spike"))
}

#[test]
fn help_is_explicitly_experimental_and_modes_are_isolated() {
    let output = spike().arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("EXPERIMENTAL SPIKE"));
    assert!(help.contains("plan-postgres"));
    assert!(help.contains("execute-postgres"));
    assert!(help.contains("fence-install-postgres"));
    assert!(help.contains("fence-attest-postgres"));
    assert!(help.contains("fence-release-postgres"));
    assert!(help.contains("resume-postgres"));
    assert!(!help.contains("--password"));
    assert!(!help.contains("--database-url"));

    let output = spike().args(["plan-postgres", "--help"]).output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--consistency <CONSISTENCY>"));
    assert!(help.contains("consistent-snapshot"));
    assert!(help.contains("write-fence"));
    assert!(!help.contains("--execute"));
    assert!(!help.contains("--approval-ref"));
}

#[test]
fn resume_consumes_state_and_endpoint_evidence_only() {
    let output = spike()
        .args(["resume-postgres", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--state",
        "--source-config",
        "--target-config",
        "--fence-admin-config",
        "--fence-artifact",
        "--execute",
        "--strict-verification",
    ] {
        assert!(help.contains(required));
    }
    assert!(!help.contains("--plan-input"));
    assert!(!help.contains("--approval-ref"));
}

#[test]
fn mutating_fence_commands_require_explicit_authorization() {
    let install = spike()
        .args([
            "fence-install-postgres",
            "--plan-input",
            "plan.json",
            "--fence-admin-config",
            "admin.toml",
            "--fence-artifact",
            "fence.json",
        ])
        .output()
        .unwrap();
    assert!(!install.status.success());
    assert!(String::from_utf8(install.stderr)
        .unwrap()
        .contains("--execute"));

    let release = spike()
        .args([
            "fence-release-postgres",
            "--fence-admin-config",
            "admin.toml",
            "--fence-artifact",
            "fence.json",
            "--approval-ref",
            "change-1",
        ])
        .output()
        .unwrap();
    assert!(!release.status.success());
    assert!(String::from_utf8(release.stderr)
        .unwrap()
        .contains("--execute"));
}

#[test]
fn plan_requires_an_explicit_consistency_contract() {
    let output = spike()
        .args([
            "plan-postgres",
            "--source-config",
            "source.toml",
            "--target-config",
            "target.toml",
            "--plan-output",
            "plan.json",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--consistency"));
}

#[test]
fn execute_requires_both_write_and_verification_gates() {
    let base = [
        "execute-postgres",
        "--plan-input",
        "plan.json",
        "--source-config",
        "source.toml",
        "--target-config",
        "target.toml",
        "--approval-ref",
        "change-1",
        "--state-output",
        "state.json",
    ];
    for missing_gate in ["--execute", "--strict-verification"] {
        let mut arguments = base.to_vec();
        arguments.push(if missing_gate == "--execute" {
            "--strict-verification"
        } else {
            "--execute"
        });
        let output = spike().args(arguments).output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains(missing_gate));
    }
}
