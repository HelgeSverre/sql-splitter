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
    assert!(help.contains("assess-postgres"));
    assert!(help.contains("probe-postgres-source-profile"));
    assert!(help.contains("plan-postgres"));
    assert!(help.contains("plan-mysql"));
    assert!(help.contains("plan-postgres-to-mysql"));
    assert!(help.contains("plan-mysql-to-postgres"));
    assert!(help.contains("execute-postgres-to-mysql"));
    assert!(help.contains("resume-postgres-to-mysql"));
    assert!(help.contains("execute-mysql-to-postgres"));
    assert!(help.contains("resume-mysql-to-postgres"));
    assert!(help.contains("execute-mysql"));
    assert!(help.contains("resume-mysql"));
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
    assert!(help.contains("--assessment-input <ASSESSMENT_INPUT>"));
    assert!(help.contains("--max-outage-seconds <MAX_OUTAGE_SECONDS>"));
    assert!(help.contains("--source-profile <SOURCE_PROFILE>"));
    assert!(help.contains("--source-profile-evidence <SOURCE_PROFILE_EVIDENCE>"));
    assert!(help.contains("--verified-external-quiesce-rescan"));
    assert!(help.contains("consistent-snapshot"));
    assert!(help.contains("write-fence"));
    assert!(!help.contains("--execute"));
    assert!(!help.contains("--approval-ref"));
}

#[test]
fn cross_dialect_modes_expose_exact_inputs_and_execution_gates() {
    for (command, expected) in [
        (
            "plan-postgres-to-mysql",
            &[
                "--source-config",
                "--target-config",
                "--target-metadata-admin-config",
                "--mapping-input",
                "--plan-output",
                "--consistency",
            ][..],
        ),
        (
            "plan-mysql-to-postgres",
            &[
                "--source-config",
                "--source-metadata-admin-config",
                "--freeze-admin-config",
                "--target-config",
                "--mapping-input",
                "--plan-output",
                "--consistency",
            ][..],
        ),
        (
            "execute-postgres-to-mysql",
            &[
                "--plan-input",
                "--source-config",
                "--fence-admin-config",
                "--fence-artifact",
                "--target-config",
                "--target-metadata-admin-config",
                "--approval-ref",
                "--state-output",
                "--execute",
                "--strict-verification",
            ][..],
        ),
        (
            "resume-postgres-to-mysql",
            &[
                "--state",
                "--source-config",
                "--fence-admin-config",
                "--fence-artifact",
                "--target-config",
                "--target-metadata-admin-config",
                "--execute",
                "--strict-verification",
            ][..],
        ),
        (
            "execute-mysql-to-postgres",
            &[
                "--plan-input",
                "--source-config",
                "--source-metadata-admin-config",
                "--freeze-admin-config",
                "--target-config",
                "--external-freeze-assertion",
                "--approval-ref",
                "--state-output",
                "--execute",
                "--strict-verification",
            ][..],
        ),
        (
            "resume-mysql-to-postgres",
            &[
                "--state",
                "--source-config",
                "--source-metadata-admin-config",
                "--freeze-admin-config",
                "--target-config",
                "--external-freeze-assertion",
                "--execute",
                "--strict-verification",
            ][..],
        ),
    ] {
        let output = spike().args([command, "--help"]).output().unwrap();
        assert!(output.status.success(), "{command} help failed");
        let help = String::from_utf8(output.stdout).unwrap();
        for flag in expected {
            assert!(help.contains(flag), "{command} omits {flag}");
        }
        if command.starts_with("resume-") {
            assert!(!help.contains("--plan-input"));
            assert!(!help.contains("--approval-ref"));
            assert!(!help.contains("--state-output"));
        }
        if command.starts_with("plan-") {
            assert!(!help.contains("--execute"));
            assert!(!help.contains("--approval-ref"));
        }
    }
}

#[test]
fn cross_dialect_plans_reject_the_wrong_consistency_mode_before_io() {
    let postgres_to_mysql = spike()
        .args([
            "plan-postgres-to-mysql",
            "--source-config",
            "source.toml",
            "--target-config",
            "target.toml",
            "--target-metadata-admin-config",
            "metadata.toml",
            "--mapping-input",
            "mapping.json",
            "--plan-output",
            "plan.json",
            "--consistency",
            "consistent-snapshot",
        ])
        .output()
        .unwrap();
    assert!(!postgres_to_mysql.status.success());
    assert!(String::from_utf8(postgres_to_mysql.stderr)
        .unwrap()
        .contains("requires --consistency write-fence"));

    let mysql_to_postgres = spike()
        .args([
            "plan-mysql-to-postgres",
            "--source-config",
            "source.toml",
            "--source-metadata-admin-config",
            "metadata.toml",
            "--freeze-admin-config",
            "freeze.toml",
            "--target-config",
            "target.toml",
            "--mapping-input",
            "mapping.json",
            "--plan-output",
            "plan.json",
            "--consistency",
            "write-fence",
        ])
        .output()
        .unwrap();
    assert!(!mysql_to_postgres.status.success());
    assert!(String::from_utf8(mysql_to_postgres.stderr)
        .unwrap()
        .contains("requires --consistency consistent-snapshot"));
}

#[test]
fn mysql_execute_and_resume_require_mutation_and_verification_gates() {
    let output = spike().args(["execute-mysql", "--help"]).output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--plan-input",
        "--source-config",
        "--source-metadata-admin-config",
        "--freeze-admin-config",
        "--target-config",
        "--target-metadata-admin-config",
        "--external-freeze-assertion",
        "--approval-ref",
        "--state-output",
        "--execute",
        "--strict-verification",
    ] {
        assert!(help.contains(required));
    }

    let output = spike().args(["resume-mysql", "--help"]).output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--state",
        "--source-config",
        "--source-metadata-admin-config",
        "--freeze-admin-config",
        "--target-config",
        "--target-metadata-admin-config",
        "--external-freeze-assertion",
        "--execute",
        "--strict-verification",
    ] {
        assert!(help.contains(required));
    }
    assert!(!help.contains("--plan-input"));
    assert!(!help.contains("--approval-ref"));
    assert!(!help.contains("--state-output"));
}

#[test]
fn mysql_plan_is_read_only_and_requires_an_explicit_consistency_contract() {
    let output = spike().args(["plan-mysql", "--help"]).output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--source-config",
        "--source-metadata-admin-config",
        "--freeze-admin-config",
        "--target-config",
        "--target-metadata-admin-config",
        "--authorization-mapping",
        "--plan-output",
        "--consistency <CONSISTENCY>",
    ] {
        assert!(help.contains(required));
    }
    for forbidden in ["--execute", "--approval-ref", "--fence-artifact"] {
        assert!(!help.contains(forbidden));
    }

    let output = spike()
        .args([
            "plan-mysql",
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
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("--consistency"));
}

#[test]
fn source_profile_probe_is_separate_and_requires_explicit_authorization() {
    let output = spike()
        .args(["probe-postgres-source-profile", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--source-config",
        "--admin-config",
        "--profile",
        "--probe-output",
        "--execute",
    ] {
        assert!(help.contains(required));
    }
    for forbidden in ["--target-config", "--plan-output", "--approval-ref"] {
        assert!(!help.contains(forbidden));
    }

    let output = spike()
        .args([
            "probe-postgres-source-profile",
            "--source-config",
            "source.toml",
            "--admin-config",
            "admin.toml",
            "--profile",
            "managed-administrator",
            "--probe-output",
            "probe.json",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("--execute"));
}

#[test]
fn assessment_is_source_only_and_has_no_mutation_flags() {
    let output = spike()
        .args(["assess-postgres", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--source-config",
        "--assessment-output",
        "--report-output",
        "--throughput-profile",
    ] {
        assert!(help.contains(required));
    }
    for forbidden in [
        "--target-config",
        "--consistency",
        "--approval-ref",
        "--execute",
        "--fence",
    ] {
        assert!(!help.contains(forbidden));
    }
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
fn plan_accepts_no_outage_policy_or_the_complete_pair() {
    let base = [
        "plan-postgres",
        "--source-config",
        "source.toml",
        "--target-config",
        "target.toml",
        "--plan-output",
        "plan.json",
        "--consistency",
        "write-fence",
    ];
    let output = spike().args(base).output().unwrap();
    assert!(!String::from_utf8(output.stderr)
        .unwrap()
        .contains("--assessment-input"));

    for (present_flag, present_value, missing_flag) in [
        (
            "--assessment-input",
            "assessment.json",
            "--max-outage-seconds",
        ),
        ("--max-outage-seconds", "60", "--assessment-input"),
    ] {
        let mut arguments = base.to_vec();
        arguments.extend([present_flag, present_value]);
        let output = spike().args(arguments).output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains(missing_flag));
    }
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
