#![cfg(feature = "enterprise-migration-spike")]

#[cfg(feature = "migration-fault-injection")]
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "migration-fault-injection")]
use std::{thread, time::Instant};

#[cfg(feature = "migration-fault-injection")]
#[path = "support/postgres_commit_proxy.rs"]
mod mysql_commit_proxy;
#[cfg(feature = "migration-fault-injection")]
use mysql_commit_proxy::{CommitFaultMode, PostgresCommitProxy};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, SslOpts};

use sql_splitter::migration::append_journal::{
    AppendJournal, Genesis, OperationPhase, OperationSpec,
};
use sql_splitter::migration::artifact::{read_json, write_json_new};
use sql_splitter::migration::canonical::{
    canonicalize_json, digest_rows, CanonicalRow, CANONICAL_ENCODING_VERSION,
};
use sql_splitter::migration::connection::{
    CancellationToken, KeysetPage, ReadSession, SourceConnectionFactory, TargetConnectionFactory,
};
use sql_splitter::migration::journal::{
    ConsistencyEvidence, MigrationStatus, OperationState, ResumeBinding,
};
use sql_splitter::migration::model::{
    ColumnMeta, DbValue, Identifier, KeyTuple, QualifiedTable, RowBatch,
};
use sql_splitter::migration::mysql::{
    attest_mysql_external_freeze, build_plan, build_plan_with_visibility,
    collect_mysql_metadata_visibility, inspect_live_endpoint, mysql_auto_increment_states,
    mysql_catalog_fingerprint, mysql_foreign_keys, mysql_table_definitions, mysql_tls_binding,
    validate_mysql_external_freeze_continuity, write_live_plan, write_live_plan_with_visibility,
    MySqlEndpointConfig, MySqlSourceFactory, MySqlTableState, MySqlTargetFactory,
    MYSQL_CONSISTENCY_SNAPSHOT,
};
use sql_splitter::migration::mysql_execution::{
    execute_live_mysql_frozen_plan, reconcile_mysql_pre_data_schema, resume_live_mysql_frozen_plan,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::mysql_execution::{
    execute_live_mysql_frozen_plan_interrupted, resume_live_mysql_frozen_plan_with_cancellation,
    MySqlCancellationResume, MySqlExecutionInterruption, MySqlInterruptedExecution,
};
use sql_splitter::migration::mysql_profile::{
    MySqlDdlFreezeMechanism, MySqlDmlFreezeMechanism, MySqlExternalFreezeAssertion,
    MySqlExternalFreezeAttestation, MySqlFreezeAttestationStatus, MySqlFreezeProfileKind,
    MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION, MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
};
use sql_splitter::migration::mysql_visibility::{
    MySqlGrantRecord, MySqlOperationalAccountPurpose, MySqlProxyTarget,
};
use sql_splitter::migration::plan::{OperationKind, ReviewedPlan, UnsupportedObjectCode};

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

#[test]
#[ignore = "requires the disposable MySQL 8.0/8.4 TLS matrix"]
fn live_mysql_metadata_visibility_contract() -> anyhow::Result<()> {
    let source_config =
        MySqlEndpointConfig::read(&required_path("SQL_SPLITTER_MYSQL_TEST_SOURCE_CONFIG")?)?;
    let source_metadata_config = MySqlEndpointConfig::read(&required_path(
        "SQL_SPLITTER_MYSQL_TEST_SOURCE_METADATA_CONFIG",
    )?)?;
    let freeze_config =
        MySqlEndpointConfig::read(&required_path("SQL_SPLITTER_MYSQL_TEST_ADMIN_CONFIG")?)?;
    let target_config =
        MySqlEndpointConfig::read(&required_path("SQL_SPLITTER_MYSQL_TEST_TARGET_CONFIG")?)?;
    let target_metadata_config = MySqlEndpointConfig::read(&required_path(
        "SQL_SPLITTER_MYSQL_TEST_TARGET_METADATA_CONFIG",
    )?)?;

    let source = inspect_live_endpoint(source_config.clone())?;
    let source_visibility = collect_mysql_metadata_visibility(
        &source,
        &source_config,
        &source_metadata_config,
        Some(&freeze_config),
    )?;
    let target = inspect_live_endpoint(target_config.clone())?;
    let target_visibility =
        collect_mysql_metadata_visibility(&target, &target_config, &target_metadata_config, None)?;
    assert!(source_visibility
        .evidence
        .grant_inventory
        .unknown_privilege_classes
        .is_empty());
    assert!(target_visibility
        .evidence
        .grant_inventory
        .unknown_privilege_classes
        .is_empty());
    assert!(source_visibility
        .evidence
        .operational_exclusions
        .iter()
        .any(|exclusion| exclusion.purpose == MySqlOperationalAccountPurpose::FreezeAdministrator));
    assert!(source_visibility
        .evidence
        .active_administrator_roles
        .iter()
        .any(|role| role.user == "source_metadata_role" && role.host == "%"));
    assert!(source_visibility
        .evidence
        .effective_administrator_privileges
        .iter()
        .any(|privilege| privilege == "SHOW_ROUTINE"));
    assert!(source_visibility
        .evidence
        .grant_inventory
        .records
        .iter()
        .any(|record| matches!(
            record,
            MySqlGrantRecord::PartialRevoke {
                account,
                database,
                privileges,
            } if account.user == "partially_restricted_reader"
                && database == "migration_source"
                && privileges == &["SELECT"]
        )));
    assert!(source_visibility
        .evidence
        .grant_inventory
        .records
        .iter()
        .any(|record| matches!(
            record,
            MySqlGrantRecord::Proxy {
                account,
                target: MySqlProxyTarget::AnyAccount,
                grantable: true,
            } if account.user == "migration_admin" && account.host == "%"
        )));
    let reviewed =
        build_plan_with_visibility(&source, &target, &source_visibility, &target_visibility)?;
    assert_eq!(
        reviewed.plan.mysql_metadata_visibility,
        Some(source_visibility.evidence)
    );
    assert_eq!(
        reviewed.plan.mysql_target_metadata_visibility,
        Some(target_visibility.evidence)
    );
    assert!(!reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| matches!(
            object.object_kind.as_str(),
            "catalog_visibility" | "target_catalog_visibility"
        )));
    assert!(reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| object.object_kind == "privilege"));
    Ok(())
}

#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_two_container_execute_and_resume() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?;
    let assertion_path = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?;
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?;

    let reviewed = write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    assert!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "unexpected blockers: {:#?}\nnon-operational grants: {:#?}",
        reviewed.plan.unsupported_objects.objects,
        reviewed
            .plan
            .mysql_metadata_visibility
            .as_ref()
            .map(|visibility| visibility.non_operational_records())
    );
    write_live_freeze_assertion(&assertion_path, "two-container-execution-v1")?;

    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let mut target = connect(&target_config)?;
    target.query_drop(
        "CREATE TABLE migration_execution_target.unreviewed_target_object (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
    )?;
    let error = execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-two-container-approval",
        &journal_path,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("differs from the reviewed empty target"));
    assert!(!journal_path.exists());
    target.query_drop("DROP TABLE migration_execution_target.unreviewed_target_object")?;

    let report = execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-two-container-approval",
        &journal_path,
    )?;
    assert_eq!(report.copied_rows, 3);
    assert_eq!(report.committed_chunks, 2);
    let rows: Vec<(i64, String)> = target
        .query("SELECT id, payload FROM migration_execution_target.copy_items ORDER BY id")?;
    assert_eq!(
        rows,
        vec![(1, "one".into()), (2, "two".into()), (3, "three".into()),]
    );
    let next_value: Option<u64> = target.query_first(
        "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'migration_execution_target' AND TABLE_NAME = 'copy_items'",
    )?;
    assert_eq!(next_value, Some(10));

    let resumed = resume_live_mysql_frozen_plan(
        &journal_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
    )?;
    assert_eq!(resumed.copied_rows, 3);
    assert_eq!(resumed.committed_chunks, 2);
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_foreign_key_integrity_and_recovery_matrix() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_FK_SOURCE_CONFIG")?;
    let source_metadata_path = required_path("SQL_SPLITTER_MYSQL_TEST_FK_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_FK_FREEZE_CONFIG")?;
    let artifact_dir = required_path("SQL_SPLITTER_MYSQL_TEST_FK_ARTIFACT_DIR")?;
    let journal_dir = required_path("SQL_SPLITTER_MYSQL_TEST_FK_JOURNAL_DIR")?;

    for (name, target_env, target_metadata_env, interruption, durable_state) in [
        (
            "prepared",
            "SQL_SPLITTER_MYSQL_TEST_FK_PREPARED_TARGET_CONFIG",
            "SQL_SPLITTER_MYSQL_TEST_FK_PREPARED_TARGET_METADATA_CONFIG",
            MySqlExecutionInterruption::ForeignKeyPrepared,
            OperationState::Prepared,
        ),
        (
            "committed",
            "SQL_SPLITTER_MYSQL_TEST_FK_COMMITTED_TARGET_CONFIG",
            "SQL_SPLITTER_MYSQL_TEST_FK_COMMITTED_TARGET_METADATA_CONFIG",
            MySqlExecutionInterruption::ForeignKeyCommitted,
            OperationState::Committed,
        ),
    ] {
        let target_path = required_path(target_env)?;
        let target_metadata_path = required_path(target_metadata_env)?;
        let plan_path = artifact_dir.join(format!("fk-{name}-plan.json"));
        let assertion_path = artifact_dir.join(format!("fk-{name}-assertion.json"));
        let journal_path = journal_dir.join(format!("fk-{name}.journal"));
        let reviewed = write_live_plan_with_visibility(
            &source_path,
            &source_metadata_path,
            &freeze_path,
            &target_path,
            &target_metadata_path,
            &plan_path,
        )?;
        assert!(
            !reviewed.plan.unsupported_objects.blocks_execution(),
            "unexpected MySQL FK blockers: {:#?}",
            reviewed.plan.unsupported_objects.objects
        );
        assert_eq!(
            mysql_foreign_keys(reviewed.plan.source_catalog.as_ref().unwrap())?.len(),
            4
        );
        assert_eq!(
            reviewed
                .plan
                .operations
                .iter()
                .filter(|operation| operation.kind == OperationKind::CheckForeignKey)
                .count(),
            4
        );
        assert_eq!(
            reviewed
                .plan
                .operations
                .iter()
                .filter(|operation| operation.kind == OperationKind::AddForeignKey)
                .count(),
            4
        );
        write_live_freeze_assertion(&assertion_path, &format!("mysql-fk-{name}"))?;
        assert!(
            execute_live_mysql_frozen_plan_interrupted(MySqlInterruptedExecution {
                plan_path: &plan_path,
                source_config_path: &source_path,
                source_metadata_config_path: &source_metadata_path,
                freeze_config_path: &freeze_path,
                target_config_path: &target_path,
                target_metadata_config_path: &target_metadata_path,
                freeze_assertion_path: &assertion_path,
                approval_reference: "mysql-fk-live-approval",
                state_path: &journal_path,
                interruption,
            })
            .is_err()
        );
        let journal = AppendJournal::open_resume(&journal_path)?;
        assert!(journal
            .reviewed_plan()
            .plan
            .operations
            .iter()
            .filter(|operation| operation.kind == OperationKind::AddForeignKey)
            .any(|operation| {
                journal.projection().operations.get(operation.id.as_str()) == Some(&durable_state)
            }));
        drop(journal);

        let report = resume_live_mysql_frozen_plan(
            &journal_path,
            &source_path,
            &source_metadata_path,
            &freeze_path,
            &target_path,
            &target_metadata_path,
            &assertion_path,
        )?;
        assert_eq!(report.copied_rows, 10);
        let target_config = MySqlEndpointConfig::read(&target_path)?;
        let mut target = connect(&target_config)?;
        let foreign_key_count: u64 = target
            .query_first(
                "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_TYPE = 'FOREIGN KEY'",
            )?
            .unwrap();
        assert_eq!(foreign_key_count, 4);
        let child_rows: Vec<(i64, Option<i64>, Option<i64>)> = target
            .query("SELECT child_id, tenant_id, parent_id FROM fk_child ORDER BY child_id")?;
        assert_eq!(
            child_rows,
            vec![
                (1, Some(1), Some(10)),
                (2, None, Some(999)),
                (3, Some(2), None),
                (4, None, None),
            ]
        );
        let cycle: Vec<(i64, Option<i64>, i64, Option<i64>)> = target.query(
            "SELECT a.id, a.b_id, b.id, b.a_id FROM fk_cycle_a a JOIN fk_cycle_b b ON b.id = a.b_id",
        )?;
        assert_eq!(cycle, vec![(1, Some(1), 1, Some(1))]);
        assert!(target
            .query_drop("INSERT INTO fk_child VALUES (99, 9, 9)")
            .is_err());
        let journal = AppendJournal::open_resume(&journal_path)?;
        for operation in journal
            .reviewed_plan()
            .plan
            .operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation.kind,
                    OperationKind::CheckForeignKey | OperationKind::AddForeignKey
                )
            })
        {
            assert_eq!(
                journal.projection().operations.get(operation.id.as_str()),
                Some(&OperationState::Verified)
            );
        }
        assert_eq!(journal.projection().status, MigrationStatus::Completed);
    }

    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_FK_VIOLATION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_FK_VIOLATION_TARGET_METADATA_CONFIG")?;
    let plan_path = artifact_dir.join("fk-violation-plan.json");
    let assertion_path = artifact_dir.join("fk-violation-assertion.json");
    let journal_path = journal_dir.join("fk-violation.journal");
    write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    write_live_freeze_assertion(&assertion_path, "mysql-fk-violation")?;
    assert!(
        execute_live_mysql_frozen_plan_interrupted(MySqlInterruptedExecution {
            plan_path: &plan_path,
            source_config_path: &source_path,
            source_metadata_config_path: &source_metadata_path,
            freeze_config_path: &freeze_path,
            target_config_path: &target_path,
            target_metadata_config_path: &target_metadata_path,
            freeze_assertion_path: &assertion_path,
            approval_reference: "mysql-fk-live-approval",
            state_path: &journal_path,
            interruption: MySqlExecutionInterruption::BeforeForeignKeyChecks,
        })
        .is_err()
    );
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    connect(&target_config)?.query_drop("INSERT INTO fk_child VALUES (99, 9, 9)")?;
    assert!(resume_live_mysql_frozen_plan(
        &journal_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
    )
    .is_err());
    let journal = AppendJournal::open_resume(&journal_path)?;
    assert_eq!(
        journal.projection().status,
        MigrationStatus::ManualReconciliationRequired
    );
    Ok(())
}

#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_canonical_value_matrix() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_METADATA_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_PLAN_OUTPUT")?;
    let assertion_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_ASSERTION_OUTPUT")?;
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_JOURNAL_OUTPUT")?;

    let reviewed = write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    assert!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "unexpected value-matrix blockers: {:#?}",
        reviewed.plan.unsupported_objects.objects
    );
    assert_eq!(
        reviewed.plan.canonical_encoding_version,
        CANONICAL_ENCODING_VERSION
    );
    write_live_freeze_assertion(&assertion_path, "mysql-canonical-values-v2")?;

    let report = execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-canonical-value-approval",
        &journal_path,
    )?;
    assert_eq!(report.copied_rows, 3);
    assert_eq!(report.committed_chunks, 2);

    let source_config = MySqlEndpointConfig::read(&source_path)?;
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let projection = mysql_value_projection()?;
    let (source_columns, source_rows) = read_mysql_value_rows(&source_config, &projection)?;
    let (target_columns, target_rows) = read_mysql_value_rows(&target_config, &projection)?;
    assert_eq!(
        source_columns
            .iter()
            .map(|column| &column.name)
            .collect::<Vec<_>>(),
        projection.iter().collect::<Vec<_>>()
    );
    assert_eq!(target_columns, source_columns);
    assert_eq!(target_rows, source_rows);
    assert_eq!(source_rows.len(), 3);
    assert_eq!(
        source_columns[14].timezone_semantics.as_deref(),
        Some("local_without_offset")
    );
    assert_eq!(
        source_columns[16].timezone_semantics.as_deref(),
        Some("mysql_session_time_zone")
    );
    assert_eq!(source_columns[14].scale, Some(6));
    assert_eq!(source_columns[15].scale, Some(0));
    assert_eq!(source_columns[16].scale, Some(6));
    assert_eq!(source_columns[17].scale, Some(0));
    assert_eq!(source_columns[18].scale, Some(6));

    assert_eq!(source_rows[0][1], DbValue::Null);
    assert_eq!(source_rows[0][2], DbValue::Text("Unicode: åß水🧪".into()));
    assert_eq!(source_rows[0][3], DbValue::Signed(-128));
    assert_eq!(source_rows[0][4], DbValue::Signed(-32_768));
    assert_eq!(source_rows[0][5], DbValue::Signed(-8_388_608));
    assert_eq!(source_rows[0][6], DbValue::Signed(-2_147_483_648));
    assert_eq!(source_rows[0][7], DbValue::Signed(i64::MIN.into()));
    assert_eq!(source_rows[1][3], DbValue::Signed(127));
    assert_eq!(source_rows[1][4], DbValue::Signed(32_767));
    assert_eq!(source_rows[1][5], DbValue::Signed(8_388_607));
    assert_eq!(source_rows[1][6], DbValue::Signed(2_147_483_647));
    assert_eq!(source_rows[1][7], DbValue::Signed(i64::MAX.into()));
    assert_eq!(source_rows[1][8], DbValue::Unsigned(u64::MAX.into()));
    assert_eq!(source_rows[0][12], DbValue::Bytes(vec![0, 1]));
    assert_eq!(source_rows[1][12], DbValue::Bytes(vec![1, 255]));
    assert_eq!(
        source_rows[0][13],
        DbValue::Date {
            year: 1000,
            month: 1,
            day: 1,
        }
    );
    assert_eq!(
        source_rows[0][14],
        DbValue::Timestamp {
            local: "2000-02-29 10:11:12.123456".into(),
            offset_minutes: None,
            precision: 6,
        }
    );
    assert_eq!(
        source_rows[0][15],
        DbValue::Timestamp {
            local: "2000-02-29 10:11:12.000000".into(),
            offset_minutes: None,
            precision: 0,
        }
    );
    assert_eq!(
        source_rows[0][16],
        DbValue::Timestamp {
            local: "2000-02-29 10:11:12.123456".into(),
            offset_minutes: None,
            precision: 6,
        }
    );
    assert_eq!(
        source_rows[0][18],
        DbValue::Time {
            nanos: -(838_i128 * 3_600 + 59 * 60 + 59) * 1_000_000_000,
        }
    );
    assert_eq!(source_rows[0][19], DbValue::Signed(1901));
    assert_eq!(
        source_rows[2][9],
        DbValue::Decimal {
            coefficient: b"100000000".to_vec(),
            scale: 10,
        }
    );
    // MySQL normalizes negative zero when it stores FLOAT and DOUBLE. The
    // canonical encoder still distinguishes the bit patterns for adapters
    // whose server exposes them; this matrix binds MySQL's observed value.
    assert_eq!(source_rows[0][10], DbValue::Float32(0.0f32.to_bits()));
    assert_eq!(source_rows[0][11], DbValue::Float64(0.0f64.to_bits()));
    assert_eq!(source_rows[0][20], DbValue::Bytes(vec![0, 255, 16]));
    assert_mysql_json_eq(
        &source_rows[0][22],
        br#"{"a":1,"ten":12,"wide":9007199254740993,"z":1}"#,
    )?;
    assert_mysql_json_eq(
        &source_rows[1][22],
        br#"{"array":[1,1,1,null,true,false],"nested":{"a":1,"z":0}}"#,
    )?;
    assert_mysql_json_eq(&source_rows[2][22], br#"{"duplicate":2,"number":1}"#)?;
    let mut source_observer = connect(&source_config)?;
    let mut target_observer = connect(&target_config)?;
    let source_number_types: Option<(String, String, String)> = source_observer.query_first(
        "SELECT JSON_TYPE(JSON_EXTRACT(json_value, '$.ten')), JSON_TYPE(JSON_EXTRACT(json_value, '$.wide')), JSON_UNQUOTE(JSON_EXTRACT(json_value, '$.wide')) FROM migration_values_source.value_matrix WHERE id = 1",
    )?;
    let target_number_types: Option<(String, String, String)> = target_observer.query_first(
        "SELECT JSON_TYPE(JSON_EXTRACT(json_value, '$.ten')), JSON_TYPE(JSON_EXTRACT(json_value, '$.wide')), JSON_UNQUOTE(JSON_EXTRACT(json_value, '$.wide')) FROM migration_values_target.value_matrix WHERE id = 1",
    )?;
    assert_eq!(
        source_number_types,
        Some((
            "INTEGER".into(),
            "UNSIGNED INTEGER".into(),
            "9007199254740993".into()
        ))
    );
    assert_eq!(target_number_types, source_number_types);

    let journal = AppendJournal::open_resume(&journal_path)?;
    assert_eq!(
        journal.genesis().binding.canonical_encoding_version,
        CANONICAL_ENCODING_VERSION
    );
    let committed = journal
        .all_committed_chunks()?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].final_key, vec![DbValue::Signed(2)]);
    assert_eq!(committed[1].final_key, vec![DbValue::Signed(3)]);
    let column_names = source_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let mut first = 0usize;
    for chunk in &committed {
        let count = usize::try_from(chunk.row_count)?;
        let last = first + count;
        assert_eq!(
            chunk.canonical_digest,
            mysql_value_rows_digest(
                "migration_values_source.value_matrix",
                &column_names,
                &source_rows[first..last],
            )?
        );
        first = last;
    }
    assert_eq!(first, source_rows.len());
    drop(journal);

    let resumed = resume_live_mysql_frozen_plan(
        &journal_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
    )?;
    assert_eq!(resumed.copied_rows, 3);
    assert_eq!(resumed.committed_chunks, 2);
    Ok(())
}

#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_drift_rejection_matrix() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let wrong_source_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_CONFIG")?;
    let wrong_source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_SOURCE_METADATA_CONFIG")?;
    let wrong_freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_FREEZE_CONFIG")?;
    let wrong_target_path = required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_CONFIG")?;
    let wrong_target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_VALUES_TARGET_METADATA_CONFIG")?;
    let base_plan = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?;
    let base_assertion = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?;
    let base_journal = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?;
    let plan_path = base_plan.with_extension("drift.plan.json");
    let assertion_path = base_assertion.with_extension("drift.assertion.json");
    let journal_path = base_journal.with_extension("drift.journal");
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let mut target = connect(&target_config)?;
    target.query_drop("DROP TABLE IF EXISTS migration_execution_target.copy_items")?;

    let reviewed = write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    write_live_freeze_assertion(&assertion_path, "mysql-drift-matrix")?;

    let mut stale_tool_plan = reviewed.plan.clone();
    stale_tool_plan.tool_version = "different-tool-version".into();
    let stale_tool = ReviewedPlan::new(stale_tool_plan)?;
    let stale_tool_path = base_plan.with_extension("drift.stale-tool.plan.json");
    let stale_tool_journal = base_journal.with_extension("drift.stale-tool.journal");
    write_json_new(&stale_tool_path, &stale_tool)?;
    let error = execute_live_mysql_frozen_plan(
        &stale_tool_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &stale_tool_journal,
    )
    .unwrap_err();
    assert!(error.to_string().contains("running tool contract"));
    assert!(!stale_tool_journal.exists());

    let mut wrong_policy = reviewed.plan.clone();
    wrong_policy.conversion_policy = "different-policy".into();
    assert!(ReviewedPlan::new(wrong_policy).is_err());

    let mut operation_tamper = reviewed.clone();
    operation_tamper
        .plan
        .operations
        .iter_mut()
        .find(|operation| operation.kind == OperationKind::CopyTable)
        .expect("copy operation")
        .parameters
        .insert("unreviewed_parameter".into(), serde_json::json!(true));
    let operation_tamper_path = base_plan.with_extension("drift.operation.plan.json");
    let operation_tamper_journal = base_journal.with_extension("drift.operation.journal");
    write_json_new(&operation_tamper_path, &operation_tamper)?;
    assert!(execute_live_mysql_frozen_plan(
        &operation_tamper_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &operation_tamper_journal,
    )
    .is_err());
    assert!(!operation_tamper_journal.exists());

    let wrong_source_journal = base_journal.with_extension("drift.wrong-source.journal");
    assert!(execute_live_mysql_frozen_plan(
        &plan_path,
        &wrong_source_path,
        &wrong_source_metadata_path,
        &wrong_freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &wrong_source_journal,
    )
    .is_err());
    assert!(!wrong_source_journal.exists());

    let wrong_target_journal = base_journal.with_extension("drift.wrong-target.journal");
    assert!(execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &wrong_target_path,
        &wrong_target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &wrong_target_journal,
    )
    .is_err());
    assert!(!wrong_target_journal.exists());

    let report = execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &journal_path,
    )?;
    assert_eq!(report.copied_rows, 3);
    assert_eq!(report.committed_chunks, 2);
    let completed_journal = std::fs::read(&journal_path)?;
    let assert_resume_rejects = || -> anyhow::Result<()> {
        assert!(resume_live_mysql_frozen_plan(
            &journal_path,
            &source_path,
            &source_metadata_path,
            &freeze_path,
            &target_path,
            &target_metadata_path,
            &assertion_path,
        )
        .is_err());
        assert_eq!(std::fs::read(&journal_path)?, completed_journal);
        Ok(())
    };

    target.query_drop(
        "UPDATE migration_execution_target.copy_items SET payload = 'different' WHERE id = 2",
    )?;
    assert_resume_rejects()?;
    target.query_drop(
        "UPDATE migration_execution_target.copy_items SET payload = 'two' WHERE id = 2",
    )?;

    target.query_drop(
        "INSERT INTO migration_execution_target.copy_items(id, payload) VALUES (0, 'before')",
    )?;
    assert_resume_rejects()?;
    target.query_drop("DELETE FROM migration_execution_target.copy_items WHERE id = 0")?;

    target.query_drop(
        "INSERT INTO migration_execution_target.copy_items(id, payload) VALUES (4, 'after')",
    )?;
    assert_resume_rejects()?;
    target.query_drop("DELETE FROM migration_execution_target.copy_items WHERE id = 4")?;

    target.query_drop(
        "CREATE TABLE migration_execution_target.extra_table (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB",
    )?;
    assert_resume_rejects()?;
    target.query_drop("DROP TABLE migration_execution_target.extra_table")?;

    target.query_drop(
        "ALTER TABLE migration_execution_target.copy_items ADD COLUMN extra_value BIGINT NULL",
    )?;
    assert_resume_rejects()?;
    target
        .query_drop("ALTER TABLE migration_execution_target.copy_items DROP COLUMN extra_value")?;

    target.query_drop("ALTER TABLE migration_execution_target.copy_items AUTO_INCREMENT = 20")?;
    assert_resume_rejects()?;
    Ok(())
}

#[test]
#[ignore = "requires the disposable MySQL 8.0/8.4 TLS matrix after lock loss"]
fn live_mysql_freeze_loss_stops_before_journal() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?
        .with_extension("drift.plan.json");
    let assertion_path = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?
        .with_extension("drift.assertion.json");
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?
        .with_extension("drift.lost-freeze.journal");
    let error = execute_live_mysql_frozen_plan(
        &plan_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
        "live-drift-approval",
        &journal_path,
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("backup-lock owner is absent"));
    assert!(!journal_path.exists());
    Ok(())
}

struct CollectedMySqlPages {
    rows: Vec<Vec<DbValue>>,
    cursors: Vec<Vec<DbValue>>,
}

fn read_all_mysql_pages(
    reader: &mut dyn ReadSession,
    table: &str,
    projection: &[&str],
    key: &[&str],
    limit: u32,
) -> anyhow::Result<CollectedMySqlPages> {
    let projection = projection
        .iter()
        .map(|column| Identifier::new(*column))
        .collect::<Result<Vec<_>, _>>()?;
    let key = key
        .iter()
        .map(|column| Identifier::new(*column))
        .collect::<Result<Vec<_>, _>>()?;
    let key_positions = key
        .iter()
        .map(|column| {
            projection
                .iter()
                .position(|projected| projected == column)
                .ok_or_else(|| anyhow::anyhow!("key column {column} is not projected"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let table = QualifiedTable {
        namespace: identifier("migration_source"),
        name: Identifier::new(table)?,
    };
    let mut rows = Vec::new();
    let mut cursors = Vec::new();
    let mut after = None;
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.clone(),
            key: key.clone(),
            after,
            limit,
        })?;
        if batch.is_empty() {
            break;
        }
        let final_row = batch
            .rows()
            .last()
            .ok_or_else(|| anyhow::anyhow!("nonempty page has no final row"))?;
        let cursor = key_positions
            .iter()
            .map(|position| final_row[*position].clone())
            .collect::<Vec<_>>();
        cursors.push(cursor.clone());
        rows.extend(batch.rows().iter().cloned());
        after = Some(KeyTuple::new(cursor));
    }
    Ok(CollectedMySqlPages { rows, cursors })
}

fn mysql_value_projection() -> anyhow::Result<Vec<Identifier>> {
    Ok([
        "id",
        "nullable_value",
        "unicode_value",
        "tiny_value",
        "small_value",
        "medium_value",
        "int_value",
        "big_value",
        "unsigned_big_value",
        "decimal_value",
        "float_value",
        "double_value",
        "bit_value",
        "date_value",
        "datetime6_value",
        "datetime0_value",
        "timestamp6_value",
        "timestamp0_value",
        "time6_value",
        "year_value",
        "binary_value",
        "blob_value",
        "json_value",
    ]
    .into_iter()
    .map(Identifier::new)
    .collect::<Result<Vec<_>, _>>()?)
}

fn assert_mysql_json_eq(actual: &DbValue, expected: &[u8]) -> anyhow::Result<()> {
    let DbValue::Json(actual) = actual else {
        anyhow::bail!("MySQL JSON column did not decode as JSON");
    };
    assert_eq!(canonicalize_json(actual)?, canonicalize_json(expected)?);
    Ok(())
}

fn read_mysql_value_rows(
    config: &MySqlEndpointConfig,
    projection: &[Identifier],
) -> anyhow::Result<(Vec<ColumnMeta>, Vec<Vec<DbValue>>)> {
    let factory = MySqlSourceFactory::new(config.clone());
    let snapshot = factory.capture_snapshot()?;
    let mut reader = factory.open_reader(&snapshot, CancellationToken::default())?;
    let table = QualifiedTable {
        namespace: Identifier::new(config.database.clone())?,
        name: identifier("value_matrix"),
    };
    let mut after = None;
    let mut columns = None;
    let mut rows = Vec::new();
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.to_vec(),
            key: vec![identifier("id")],
            after,
            limit: u32::MAX,
        })?;
        if batch.is_empty() {
            break;
        }
        if let Some(existing) = &columns {
            assert_eq!(existing, batch.columns());
        } else {
            columns = Some(batch.columns().to_vec());
        }
        let final_key = batch
            .rows()
            .last()
            .and_then(|row| row.first())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("MySQL value page has no complete final key"))?;
        after = Some(KeyTuple::new(vec![final_key]));
        rows.extend(batch.rows().iter().cloned());
    }
    Ok((
        columns.ok_or_else(|| anyhow::anyhow!("MySQL value matrix is unexpectedly empty"))?,
        rows,
    ))
}

fn mysql_value_rows_digest(
    table: &str,
    columns: &[&str],
    rows: &[Vec<DbValue>],
) -> anyhow::Result<String> {
    let keys = rows
        .iter()
        .map(|row| vec![row[0].clone()])
        .collect::<Vec<_>>();
    let canonical = rows
        .iter()
        .zip(&keys)
        .map(|(row, key)| CanonicalRow {
            table,
            columns,
            key,
            values: row,
        })
        .collect::<Vec<_>>();
    Ok(hex::encode(digest_rows(canonical.iter())?))
}

#[cfg(feature = "migration-fault-injection")]
#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_recovery_boundary_matrix() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let base_plan = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?;
    let base_assertion = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?;
    let base_journal = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?;
    let cases = [
        ("ddl-prepared", MySqlExecutionInterruption::DdlPrepared),
        ("ddl-committed", MySqlExecutionInterruption::DdlCommitted),
        ("chunk-prepared", MySqlExecutionInterruption::ChunkPrepared),
        (
            "chunk-applied",
            MySqlExecutionInterruption::ChunkCommitBeforeJournal,
        ),
        (
            "chunk-committed",
            MySqlExecutionInterruption::CommittedChunks(1),
        ),
        (
            "auto-increment-prepared",
            MySqlExecutionInterruption::AutoIncrementPrepared,
        ),
        (
            "auto-increment-committed",
            MySqlExecutionInterruption::AutoIncrementCommitted,
        ),
    ];
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let mut target = connect(&target_config)?;

    for (name, interruption) in cases {
        target.query_drop("DROP TABLE IF EXISTS migration_execution_target.copy_items")?;
        let plan_path = base_plan.with_extension(format!("{name}.plan.json"));
        let assertion_path = base_assertion.with_extension(format!("{name}.assertion.json"));
        let journal_path = base_journal.with_extension(format!("{name}.journal"));
        let reviewed = write_live_plan_with_visibility(
            &source_path,
            &source_metadata_path,
            &freeze_path,
            &target_path,
            &target_metadata_path,
            &plan_path,
        )?;
        write_live_freeze_assertion(&assertion_path, &format!("mysql-recovery-{name}"))?;

        let error = execute_live_mysql_frozen_plan_interrupted(MySqlInterruptedExecution {
            plan_path: &plan_path,
            source_config_path: &source_path,
            source_metadata_config_path: &source_metadata_path,
            freeze_config_path: &freeze_path,
            target_config_path: &target_path,
            target_metadata_config_path: &target_metadata_path,
            freeze_assertion_path: &assertion_path,
            approval_reference: "live-recovery-boundary-approval",
            state_path: &journal_path,
            interruption,
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("injected MySQL execution interruption"));

        let interrupted = AppendJournal::open_resume(&journal_path)?;
        assert_ne!(
            interrupted.projection().status,
            sql_splitter::migration::journal::MigrationStatus::Completed
        );
        let create_id = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CreateTable)
            .unwrap()
            .id
            .to_string();
        let restore_id = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| {
                matches!(
                    &operation.kind,
                    OperationKind::Vendor(name) if name == "restore_mysql_auto_increment"
                )
            })
            .unwrap()
            .id
            .to_string();
        let target_table_exists: Option<u8> = target.query_first(
            "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'migration_execution_target' AND TABLE_NAME = 'copy_items'",
        )?;
        let target_rows = if target_table_exists.is_some() {
            target
                .query_first::<u64, _>(
                    "SELECT COUNT(*) FROM migration_execution_target.copy_items",
                )?
                .unwrap()
        } else {
            0
        };
        match interruption {
            MySqlExecutionInterruption::DdlPrepared => {
                assert_eq!(
                    interrupted.projection().operations.get(&create_id),
                    Some(&OperationState::Prepared)
                );
                assert!(target_table_exists.is_none());
            }
            MySqlExecutionInterruption::DdlCommitted => {
                assert_eq!(
                    interrupted.projection().operations.get(&create_id),
                    Some(&OperationState::Prepared)
                );
                assert!(target_table_exists.is_some());
                assert_eq!(target_rows, 0);
            }
            MySqlExecutionInterruption::ChunkPrepared => {
                assert_eq!(
                    interrupted
                        .projection()
                        .prepared_chunk
                        .as_ref()
                        .map(|chunk| chunk.chunk_id),
                    Some(1)
                );
                assert_eq!(target_rows, 0);
            }
            MySqlExecutionInterruption::ChunkCommitBeforeJournal => {
                assert_eq!(
                    interrupted
                        .projection()
                        .prepared_chunk
                        .as_ref()
                        .map(|chunk| chunk.chunk_id),
                    Some(1)
                );
                assert_eq!(target_rows, 2);
            }
            MySqlExecutionInterruption::CommittedChunks(1) => {
                assert!(interrupted.projection().prepared_chunk.is_none());
                assert_eq!(interrupted.projection().last_chunk_id, 1);
                assert_eq!(target_rows, 2);
            }
            MySqlExecutionInterruption::AutoIncrementPrepared => {
                assert_eq!(
                    interrupted.projection().operations.get(&restore_id),
                    Some(&OperationState::Prepared)
                );
                assert_eq!(target_rows, 3);
            }
            MySqlExecutionInterruption::AutoIncrementCommitted => {
                assert_eq!(
                    interrupted.projection().operations.get(&restore_id),
                    Some(&OperationState::Committed)
                );
                assert_eq!(target_rows, 3);
            }
            MySqlExecutionInterruption::CommittedChunks(_) => {
                unreachable!("the matrix uses one exact committed-chunk boundary")
            }
            MySqlExecutionInterruption::NetworkCommitFault(_) => {
                unreachable!("network commit faults have a separate causal proxy matrix")
            }
            MySqlExecutionInterruption::BeforeForeignKeyChecks
            | MySqlExecutionInterruption::ForeignKeyPrepared
            | MySqlExecutionInterruption::ForeignKeyCommitted => {
                unreachable!("foreign-key faults have a separate integrity matrix")
            }
        }
        drop(interrupted);

        let resumed = resume_live_mysql_frozen_plan(
            &journal_path,
            &source_path,
            &source_metadata_path,
            &freeze_path,
            &target_path,
            &target_metadata_path,
            &assertion_path,
        )?;
        assert_eq!(resumed.copied_rows, 3, "case {name}");
        assert_eq!(resumed.committed_chunks, 2, "case {name}");
        let rows: Vec<(i64, String)> = target
            .query("SELECT id, payload FROM migration_execution_target.copy_items ORDER BY id")?;
        assert_eq!(
            rows,
            vec![(1, "one".into()), (2, "two".into()), (3, "three".into())],
            "case {name}"
        );
        let next_value: Option<u64> = target.query_first(
            "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'migration_execution_target' AND TABLE_NAME = 'copy_items'",
        )?;
        assert_eq!(next_value, Some(10), "case {name}");
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_cancellation_rolls_back_and_resumes() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?
        .with_extension("cancellation.plan.json");
    let assertion_path = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?
        .with_extension("cancellation.assertion.json");
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?
        .with_extension("cancellation.journal");
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let mut blocker = connect(&target_config)?;
    blocker.query_drop("DROP TABLE IF EXISTS migration_execution_target.copy_items")?;
    write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    write_live_freeze_assertion(&assertion_path, "mysql-cancellation")?;
    let error = execute_live_mysql_frozen_plan_interrupted(MySqlInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        source_metadata_config_path: &source_metadata_path,
        freeze_config_path: &freeze_path,
        target_config_path: &target_path,
        target_metadata_config_path: &target_metadata_path,
        freeze_assertion_path: &assertion_path,
        approval_reference: "live-cancellation-approval",
        state_path: &journal_path,
        interruption: MySqlExecutionInterruption::DdlCommitted,
    })
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("injected MySQL execution interruption"));

    blocker.query_drop("START TRANSACTION")?;
    blocker.query_drop(
        "SELECT id FROM migration_execution_target.copy_items WHERE id = 1 FOR UPDATE",
    )?;
    let mut observer = connect(&target_config)?;
    let cancellation = CancellationToken::default();
    let runner_token = cancellation.clone();
    let cancelled = thread::scope(|scope| -> anyhow::Result<anyhow::Error> {
        let handle = scope.spawn(|| {
            resume_live_mysql_frozen_plan_with_cancellation(
                MySqlCancellationResume {
                    state_path: &journal_path,
                    source_config_path: &source_path,
                    source_metadata_config_path: &source_metadata_path,
                    freeze_config_path: &freeze_path,
                    target_config_path: &target_path,
                    target_metadata_config_path: &target_metadata_path,
                    freeze_assertion_path: &assertion_path,
                },
                runner_token,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let waiting: Option<u64> = observer.exec_first(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB = ? AND INFO LIKE 'INSERT INTO%copy_items%'",
                (&target_config.database,),
            )?;
            if waiting.unwrap_or(0) > 0 {
                break;
            }
            if Instant::now() >= deadline {
                anyhow::bail!("MySQL INSERT did not block before cancellation timeout");
            }
            thread::sleep(Duration::from_millis(5));
        }
        cancellation.cancel();
        let result = handle
            .join()
            .map_err(|_| anyhow::anyhow!("MySQL cancellation runner thread panicked"))?;
        Ok(result.expect_err("MySQL cancellation must stop resume"))
    })?;
    assert!(format!("{cancelled:#}")
        .to_ascii_lowercase()
        .contains("cancel"));
    blocker.query_drop("ROLLBACK")?;

    let interrupted = AppendJournal::open_resume(&journal_path)?;
    assert_eq!(
        interrupted
            .projection()
            .prepared_chunk
            .as_ref()
            .map(|chunk| chunk.chunk_id),
        Some(1)
    );
    drop(interrupted);
    let target_rows: u64 = observer
        .query_first("SELECT COUNT(*) FROM migration_execution_target.copy_items")?
        .unwrap();
    assert_eq!(target_rows, 0, "cancelled chunk must roll back completely");

    let resumed = resume_live_mysql_frozen_plan(
        &journal_path,
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &target_path,
        &target_metadata_path,
        &assertion_path,
    )?;
    assert_eq!(resumed.copied_rows, 3);
    assert_eq!(resumed.committed_chunks, 2);
    let rows: Vec<(i64, String)> = observer
        .query("SELECT id, payload FROM migration_execution_target.copy_items ORDER BY id")?;
    assert_eq!(
        rows,
        vec![(1, "one".into()), (2, "two".into()), (3, "three".into())]
    );
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
#[test]
#[ignore = "requires two disposable MySQL 8.0/8.4 TLS containers"]
fn live_mysql_network_commit_response_loss_matrix() -> anyhow::Result<()> {
    for (suffix, mode) in [
        ("not-forwarded", CommitFaultMode::NotForwarded),
        ("ack-lost", CommitFaultMode::AppliedAckLost),
    ] {
        run_live_mysql_network_commit_response_loss_case(suffix, mode)?;
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_mysql_network_commit_response_loss_case(
    suffix: &str,
    mode: CommitFaultMode,
) -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_CONFIG")?;
    let source_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_SOURCE_METADATA_CONFIG")?;
    let freeze_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_FREEZE_CONFIG")?;
    let direct_target_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_CONFIG")?;
    let target_metadata_path =
        required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_TARGET_METADATA_CONFIG")?;
    let direct_target = MySqlEndpointConfig::read(&direct_target_path)?;
    let upstream = (direct_target.host.as_str(), direct_target.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("MySQL target endpoint did not resolve"))?;
    let proxy = PostgresCommitProxy::start(upstream, mode)?;
    let mut proxied_target = direct_target.clone();
    proxied_target.port = proxy.data_port();
    let proxied_target_path = direct_target_path.with_extension(format!("{suffix}.proxy.toml"));
    std::fs::write(&proxied_target_path, toml::to_string(&proxied_target)?)?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_PLAN_OUTPUT")?
        .with_extension(format!("{suffix}.network.plan.json"));
    let assertion_path = required_path("SQL_SPLITTER_MYSQL_TEST_FREEZE_ASSERTION_OUTPUT")?
        .with_extension(format!("{suffix}.network.assertion.json"));
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_EXECUTION_JOURNAL_OUTPUT")?
        .with_extension(format!("{suffix}.network.journal"));
    let mut observer = connect(&direct_target)?;
    observer.query_drop("DROP TABLE IF EXISTS migration_execution_target.copy_items")?;
    write_live_plan_with_visibility(
        &source_path,
        &source_metadata_path,
        &freeze_path,
        &proxied_target_path,
        &target_metadata_path,
        &plan_path,
    )?;
    write_live_freeze_assertion(&assertion_path, &format!("mysql-network-{suffix}"))?;
    let execution = MySqlInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        source_metadata_config_path: &source_metadata_path,
        freeze_config_path: &freeze_path,
        target_config_path: &proxied_target_path,
        target_metadata_config_path: &target_metadata_path,
        freeze_assertion_path: &assertion_path,
        approval_reference: "live-network-commit-loss",
        state_path: &journal_path,
        interruption: MySqlExecutionInterruption::NetworkCommitFault(proxy.control_port()),
    };

    match mode {
        CommitFaultMode::NotForwarded => {
            let error = execute_live_mysql_frozen_plan_interrupted(execution).unwrap_err();
            assert!(error.to_string().contains("commit outcome is unknown"));
            proxy.wait_for("discarded MySQL COMMIT bytes", |telemetry| {
                telemetry.dropped_client_bytes_after_arm > 0
            })?;
            let telemetry = proxy.telemetry()?;
            assert_eq!(telemetry.forwarded_client_bytes_after_arm, 0);
            let target_rows: u64 = observer
                .query_first("SELECT COUNT(*) FROM migration_execution_target.copy_items")?
                .unwrap();
            assert_eq!(target_rows, 0);
            let interrupted = AppendJournal::open_resume(&journal_path)?;
            assert_eq!(interrupted.projection().last_chunk_id, 0);
            assert_eq!(
                interrupted
                    .projection()
                    .prepared_chunk
                    .as_ref()
                    .map(|chunk| chunk.chunk_id),
                Some(1)
            );
            drop(interrupted);
            resume_live_mysql_frozen_plan(
                &journal_path,
                &source_path,
                &source_metadata_path,
                &freeze_path,
                &proxied_target_path,
                &target_metadata_path,
                &assertion_path,
            )?;
        }
        CommitFaultMode::AppliedAckLost => {
            let result = thread::scope(|scope| -> anyhow::Result<_> {
                let handle = scope.spawn(|| execute_live_mysql_frozen_plan_interrupted(execution));
                let deadline = Instant::now() + Duration::from_secs(20);
                loop {
                    let table_exists: Option<u8> = observer.query_first(
                        "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'migration_execution_target' AND TABLE_NAME = 'copy_items'",
                    )?;
                    let rows: Vec<(i64, String)> = if table_exists.is_some() {
                        observer.query(
                            "SELECT id, payload FROM migration_execution_target.copy_items ORDER BY id",
                        )?
                    } else {
                        Vec::new()
                    };
                    if rows == vec![(1, "one".into()), (2, "two".into())] {
                        break;
                    }
                    if Instant::now() >= deadline {
                        anyhow::bail!("MySQL committed chunk was not visible before CUT timeout");
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                proxy.wait_for("withheld MySQL COMMIT response", |telemetry| {
                    telemetry.forwarded_client_bytes_after_arm > 0
                        && telemetry.dropped_server_bytes_after_arm > 0
                })?;
                proxy.cut()?;
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("MySQL network-fault runner thread panicked"))
            })?;
            let report = result?;
            assert_eq!(report.copied_rows, 3);
            assert_eq!(report.committed_chunks, 2);
        }
    }

    let telemetry = proxy.telemetry()?;
    match mode {
        CommitFaultMode::NotForwarded => {
            assert!(telemetry.dropped_client_bytes_after_arm > 0);
            assert_eq!(telemetry.forwarded_client_bytes_after_arm, 0);
        }
        CommitFaultMode::AppliedAckLost => {
            assert!(telemetry.forwarded_client_bytes_after_arm > 0);
            assert_eq!(telemetry.forwarded_server_bytes_after_arm, 0);
            assert!(telemetry.dropped_server_bytes_after_arm > 0);
        }
    }
    let completed = AppendJournal::open_resume(&journal_path)?;
    assert_eq!(
        completed.projection().status,
        sql_splitter::migration::journal::MigrationStatus::Completed
    );
    let committed = completed
        .all_committed_chunks()?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(committed.len(), 2);
    let rows: Vec<(i64, String)> = observer
        .query("SELECT id, payload FROM migration_execution_target.copy_items ORDER BY id")?;
    assert_eq!(
        rows,
        vec![(1, "one".into()), (2, "two".into()), (3, "three".into())]
    );
    Ok(())
}

fn write_live_freeze_assertion(path: &Path, generation: &str) -> anyhow::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    write_json_new(
        path,
        &MySqlExternalFreezeAssertion {
            schema_version: MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION,
            profile_generation: generation.into(),
            provider_reference: "disposable-mysql-matrix".into(),
            activated_at_unix_seconds: now.saturating_sub(30),
            expires_at_unix_seconds: now.saturating_add(600),
            continuity_token_hash: "c".repeat(64),
            backup_lock_connection_id: std::env::var(
                "SQL_SPLITTER_MYSQL_BACKUP_LOCK_CONNECTION_ID",
            )?
            .parse()?,
            backup_lock_owner_user: std::env::var("SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_USER")?,
            backup_lock_owner_host: std::env::var("SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_HOST")?,
        },
    )?;
    Ok(())
}

fn identifier(value: &str) -> Identifier {
    Identifier::new(value).unwrap()
}

fn connect(config: &MySqlEndpointConfig) -> anyhow::Result<Conn> {
    let password = std::env::var(&config.credential_env)?;
    let mut ssl = SslOpts::default()
        .with_root_cert_path(config.tls.ca_certificate.clone())
        .with_danger_skip_domain_validation(config.tls.insecure)
        .with_danger_accept_invalid_certs(config.tls.insecure);
    if let (Some(path), Some(reference)) = (
        &config.tls.client_identity_pkcs12,
        &config.tls.client_identity_password_env,
    ) {
        ssl = ssl.with_client_identity(Some(
            mysql::ClientIdentity::new(path.clone()).with_password(std::env::var(reference)?),
        ));
    }
    Ok(Conn::new(
        OptsBuilder::new()
            .ip_or_hostname(Some(config.host.clone()))
            .tcp_port(config.port)
            .user(Some(config.user.clone()))
            .pass(Some(password))
            .db_name(Some(config.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(Duration::from_secs(config.connect_timeout_seconds)))
            .ssl_opts(Some(ssl)),
    )?)
}

#[test]
#[ignore = "requires the disposable MySQL 8.0/8.4 TLS matrix"]
fn live_mysql_snapshot_catalog_and_blocked_plan() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_MYSQL_TEST_SOURCE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_MYSQL_TEST_TARGET_CONFIG")?;
    let admin_path = required_path("SQL_SPLITTER_MYSQL_TEST_ADMIN_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_PLAN_OUTPUT")?;
    let journal_path = required_path("SQL_SPLITTER_MYSQL_TEST_JOURNAL_OUTPUT")?;
    let source_config = MySqlEndpointConfig::read(&source_path)?;
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let admin_config = MySqlEndpointConfig::read(&admin_path)?;

    let source = inspect_live_endpoint(source_config.clone())?;
    assert_eq!(source.catalog.dialect, "mysql");
    assert!(source.server_version.starts_with("8.0.") || source.server_version.starts_with("8.4."));
    assert!(source.snapshot_evidence.transaction_read_only);
    assert!(!source.snapshot_evidence.catalog_snapshot_protected);
    assert_eq!(source.snapshot_evidence.information_schema_stats_expiry, 0);
    assert_eq!(source.snapshot_evidence.lower_case_table_names, 0);
    assert_eq!(
        source.snapshot_evidence.session_sql_mode,
        sql_splitter::migration::plan::MYSQL_STRICT_SQL_MODE
    );
    assert_eq!(
        source
            .catalog
            .vendor_metadata
            .get("information_schema_stats_expiry"),
        Some(&"0".to_string())
    );
    assert!(source
        .blockers
        .iter()
        .any(|blocker| { blocker.object_kind == "table" && blocker.reason.contains("MyISAM") }));
    assert!(source
        .blockers
        .iter()
        .any(|blocker| blocker.object_kind == "resumable_key"));
    assert!(source
        .blockers
        .iter()
        .any(|blocker| blocker.object_kind == "index"));
    assert!(source
        .blockers
        .iter()
        .any(|blocker| blocker.object_kind == "view"));
    assert!(source.blockers.iter().any(|blocker| {
        blocker.object_kind == "catalog_visibility" && blocker.reason.contains("account-dependent")
    }));
    for table_name in ["key_nullable", "key_nonunique"] {
        let table_id = source.catalog.namespaces[0]
            .objects
            .iter()
            .find(|object| {
                object.kind == sql_splitter::migration::model::CatalogObjectKind::Table
                    && object.name.as_str() == table_name
            })
            .ok_or_else(|| anyhow::anyhow!("source catalog has no {table_name} table"))?
            .id
            .clone();
        assert!(source.blockers.iter().any(|blocker| {
            blocker.object_kind == "resumable_key" && blocker.object_id == table_id
        }));
    }
    assert!(!source.catalog.namespaces[0].objects.iter().any(|object| {
        object
            .attributes
            .get("grantee")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|grantee| grantee.contains("business_reader"))
    }));
    assert!(!source.catalog.namespaces[0]
        .objects
        .iter()
        .any(|object| object.name.as_str() == "case_collision"));
    let auto_increment = mysql_auto_increment_states(&source.catalog)?;
    assert_eq!(auto_increment.len(), 1);
    assert_eq!(auto_increment[0].table.name, identifier("auto_items"));
    assert_eq!(auto_increment[0].column, identifier("id"));
    assert_eq!(auto_increment[0].next_value, Some(2));
    assert_eq!(auto_increment[0].stats_expiry, 0);

    let factory = MySqlSourceFactory::new(source_config.clone());
    let token = factory.capture_snapshot()?;
    assert_eq!(token.consistency_mode, MYSQL_CONSISTENCY_SNAPSHOT);
    let mut reader = factory.open_reader(&token, CancellationToken::default())?;
    let CollectedMySqlPages {
        rows: scalar_rows,
        cursors: scalar_cursors,
    } = read_all_mysql_pages(
        reader.as_mut(),
        "key_scalar",
        &["id", "payload"],
        &["id"],
        2,
    )?;
    assert_eq!(
        scalar_rows,
        vec![
            vec![
                DbValue::Signed(i64::MIN.into()),
                DbValue::Text("minimum".into())
            ],
            vec![DbValue::Signed(0), DbValue::Text("zero".into())],
            vec![
                DbValue::Signed(i64::MAX.into()),
                DbValue::Text("maximum".into())
            ],
        ]
    );
    assert_eq!(
        scalar_cursors,
        vec![
            vec![DbValue::Signed(0)],
            vec![DbValue::Signed(i64::MAX.into())]
        ]
    );

    let CollectedMySqlPages {
        rows: composite_rows,
        cursors: composite_cursors,
    } = read_all_mysql_pages(
        reader.as_mut(),
        "key_composite",
        &["tenant_id", "id", "payload"],
        &["tenant_id", "id"],
        2,
    )?;
    assert_eq!(
        composite_rows,
        vec![
            vec![
                DbValue::Signed(-1),
                DbValue::Signed(i64::MIN.into()),
                DbValue::Text("negative".into()),
            ],
            vec![
                DbValue::Signed(0),
                DbValue::Signed(0),
                DbValue::Text("zero".into())
            ],
            vec![
                DbValue::Signed(0),
                DbValue::Signed(1),
                DbValue::Text("repeated".into())
            ],
            vec![
                DbValue::Signed(1),
                DbValue::Signed(i64::MAX.into()),
                DbValue::Text("maximum".into()),
            ],
        ]
    );
    assert_eq!(
        composite_cursors,
        vec![
            vec![DbValue::Signed(0), DbValue::Signed(0)],
            vec![DbValue::Signed(1), DbValue::Signed(i64::MAX.into())],
        ]
    );

    let text_rows = read_all_mysql_pages(
        reader.as_mut(),
        "key_text",
        &["key_value", "payload"],
        &["key_value"],
        2,
    )?
    .rows;
    assert_eq!(
        text_rows,
        vec![
            vec![DbValue::Text(String::new()), DbValue::Signed(0)],
            vec![DbValue::Text("a".into()), DbValue::Signed(1)],
            vec![DbValue::Text("å".into()), DbValue::Signed(2)],
            vec![DbValue::Text("水".into()), DbValue::Signed(3)],
        ]
    );
    let binary_rows = read_all_mysql_pages(
        reader.as_mut(),
        "key_binary",
        &["key_value", "payload"],
        &["key_value"],
        2,
    )?
    .rows;
    assert_eq!(
        binary_rows,
        vec![
            vec![DbValue::Bytes(Vec::new()), DbValue::Signed(0)],
            vec![DbValue::Bytes(vec![0]), DbValue::Signed(1)],
            vec![DbValue::Bytes(vec![0, 255]), DbValue::Signed(2)],
            vec![DbValue::Bytes(vec![255]), DbValue::Signed(3)],
        ]
    );
    let exact_n = read_all_mysql_pages(reader.as_mut(), "key_exact_n", &["id"], &["id"], 2)?;
    assert_eq!(
        exact_n.rows,
        vec![vec![DbValue::Signed(1)], vec![DbValue::Signed(2)]]
    );
    assert_eq!(exact_n.cursors, vec![vec![DbValue::Signed(2)]]);

    let n_plus_one = read_all_mysql_pages(reader.as_mut(), "key_n_plus_one", &["id"], &["id"], 2)?;
    assert_eq!(
        n_plus_one.rows,
        vec![
            vec![DbValue::Signed(1)],
            vec![DbValue::Signed(2)],
            vec![DbValue::Signed(3)],
        ]
    );
    assert_eq!(
        n_plus_one.cursors,
        vec![vec![DbValue::Signed(2)], vec![DbValue::Signed(3)]]
    );

    let one = read_all_mysql_pages(reader.as_mut(), "key_one", &["id"], &["id"], 2)?;
    assert_eq!(one.rows, vec![vec![DbValue::Signed(1)]]);
    assert_eq!(one.cursors, vec![vec![DbValue::Signed(1)]]);

    let empty = read_all_mysql_pages(reader.as_mut(), "key_empty", &["id"], &["id"], 2)?;
    assert!(empty.rows.is_empty());
    assert!(empty.cursors.is_empty());

    let sample = vec![DbValue::Signed(1), DbValue::Text("x".repeat(80))];
    let mut byte_bounded_config = source_config.clone();
    byte_bounded_config.max_batch_rows = 10;
    byte_bounded_config.max_batch_bytes = serde_json::to_vec(&sample)?.len() + 1;
    let byte_bounded_factory = MySqlSourceFactory::new(byte_bounded_config);
    let byte_bounded_token = byte_bounded_factory.capture_snapshot()?;
    let mut byte_bounded_reader =
        byte_bounded_factory.open_reader(&byte_bounded_token, CancellationToken::default())?;
    let CollectedMySqlPages {
        rows: byte_bounded_rows,
        cursors: byte_bounded_cursors,
    } = read_all_mysql_pages(
        byte_bounded_reader.as_mut(),
        "key_byte_bound",
        &["id", "payload"],
        &["id"],
        10,
    )?;
    assert_eq!(byte_bounded_rows.len(), 3);
    assert_eq!(
        byte_bounded_cursors,
        vec![
            vec![DbValue::Signed(1)],
            vec![DbValue::Signed(2)],
            vec![DbValue::Signed(3)],
        ]
    );
    drop(byte_bounded_reader);

    let mut admin = connect(&admin_config)?;
    admin.query_drop("UPDATE migration_source.items SET id=4 WHERE id=2")?;
    admin.query_drop("DELETE FROM migration_source.items WHERE id=1")?;
    admin.query_drop("INSERT INTO migration_source.items(id, name) VALUES (3, 'three')")?;
    let table = QualifiedTable {
        namespace: identifier("migration_source"),
        name: identifier("items"),
    };
    let page = reader.select_page(&KeysetPage {
        table,
        projection: vec![identifier("id"), identifier("name")],
        key: vec![identifier("id")],
        after: None,
        limit: 10,
    })?;
    assert_eq!(
        page.rows(),
        &[
            vec![DbValue::Signed(1), DbValue::Text("one".into())],
            vec![DbValue::Signed(2), DbValue::Text("two".into())],
        ]
    );
    let empty = reader.select_page(&KeysetPage {
        table: QualifiedTable {
            namespace: identifier("migration_source"),
            name: identifier("items"),
        },
        projection: vec![identifier("id"), identifier("name")],
        key: vec![identifier("id")],
        after: Some(KeyTuple::new(vec![DbValue::Signed(2)])),
        limit: 10,
    })?;
    assert!(empty.is_empty());
    drop(reader);

    let reviewed = write_live_plan(&source_path, &target_path, &plan_path)?;
    assert!(plan_path.is_file());
    assert!(reviewed.plan.mysql_source_profile.is_some());
    assert!(!reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| object.code == UnsupportedObjectCode::MySqlFreezeEvidence));
    assert!(reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| object.code == UnsupportedObjectCode::MySqlStorageEngine));
    assert!(reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| object.code == UnsupportedObjectCode::ResumableKey));
    assert!(reviewed
        .plan
        .unsupported_objects
        .objects
        .iter()
        .any(|object| object.code == UnsupportedObjectCode::SameEndpoint));
    assert!(reviewed.plan.unsupported_objects.blocks_execution());

    let mut copy_catalog = source.catalog.clone();
    let namespace = &mut copy_catalog.namespaces[0];
    namespace.objects.retain(|object| {
        (object.kind == sql_splitter::migration::model::CatalogObjectKind::Table
            && object.name.as_str() == "copy_items")
            || object
                .attributes
                .get("table_name")
                .and_then(serde_json::Value::as_str)
                == Some("copy_items")
    });
    let retained_ids = namespace
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    copy_catalog.dependencies.retain(|dependency| {
        retained_ids.contains(&dependency.from_object_id)
            && retained_ids.contains(&dependency.to_object_id)
    });
    let mut copy_source = source.clone();
    copy_source.catalog = copy_catalog.clone();
    copy_source.blockers.retain(|blocker| {
        blocker.object_kind == "catalog_visibility" || retained_ids.contains(&blocker.object_id)
    });
    copy_source.snapshot_evidence.catalog_fingerprint = mysql_catalog_fingerprint(&copy_catalog)?;
    let target_before_copy = inspect_live_endpoint(target_config.clone())?;
    let copy_plan = build_plan(&copy_source, &target_before_copy)?;
    let definitions = mysql_table_definitions(&copy_catalog)?;
    assert_eq!(definitions.len(), 1);
    let target_factory = MySqlTargetFactory::new(
        target_config.clone(),
        copy_catalog,
        target_before_copy.snapshot_evidence.clone(),
    )?;
    let target_table = QualifiedTable {
        namespace: identifier("migration_target"),
        name: identifier("copy_items"),
    };
    let expected = target_factory.reviewed_table(&target_table)?;
    target_factory.assert_empty()?;
    assert_eq!(
        target_factory.inspect_table(expected)?,
        MySqlTableState::Absent
    );
    let freeze = MySqlExternalFreezeAttestation {
        schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
        profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
        status: MySqlFreezeAttestationStatus::Active,
        source_endpoint_identity: copy_plan.plan.source_endpoint_identity.clone(),
        source_database_identity: copy_source.snapshot_evidence.database_identity.clone(),
        source_catalog_fingerprint: copy_plan.plan.source_catalog_fingerprint.clone(),
        server_uuid: copy_source.snapshot_evidence.server_uuid.clone(),
        server_start_lower_bound_unix_seconds: 99,
        server_start_upper_bound_unix_seconds: 101,
        administrator_tls_binding: mysql_tls_binding(&admin_config)?,
        profile_generation: "target-adapter-test-only".into(),
        provider_reference: "target-adapter-test-does-not-admit-execution".into(),
        activated_at_unix_seconds: 10,
        expires_at_unix_seconds: 30,
        continuity_token_hash: "c".repeat(64),
        backup_lock_connection_id: 42,
        backup_lock_owner_thread_id: 23,
        backup_lock_owner_user: "test-only".into(),
        backup_lock_owner_host: "localhost".into(),
        read_only: true,
        super_read_only: true,
        super_read_only_persisted: true,
        active_replication_channels: Vec::new(),
        dml_mechanism: MySqlDmlFreezeMechanism::PersistedSuperReadOnly,
        ddl_mechanism: MySqlDdlFreezeMechanism::ExternalBackupLock,
        freeze_enforced_by_tool: false,
        attested_at_unix_seconds: 20,
    };
    let binding = ResumeBinding {
        migration_id: copy_plan.plan.migration_id.clone(),
        plan_hash: copy_plan.plan_hash.to_string(),
        approval_reference: "live-mysql-target-adapter-test".into(),
        tool_version: copy_plan.plan.tool_version.clone(),
        source_endpoint: copy_plan.plan.source_endpoint_identity.clone(),
        target_endpoint: copy_plan
            .plan
            .target_endpoint_identity
            .as_assessed()
            .expect("execution plan target")
            .clone(),
        consistency_evidence: ConsistencyEvidence::MySqlExternalFreeze {
            endpoint_identity: copy_source.snapshot_evidence.endpoint_identity.clone(),
            database_identity: copy_source.snapshot_evidence.database_identity.clone(),
            server_uuid: freeze.server_uuid.clone(),
            source_catalog_fingerprint: copy_plan.plan.source_catalog_fingerprint.clone(),
            profile_generation: freeze.profile_generation.clone(),
            continuity_token_hash: freeze.continuity_token_hash.clone(),
            backup_lock_connection_id: freeze.backup_lock_connection_id,
        },
        source_schema_fingerprint: copy_plan.plan.source_catalog_fingerprint.clone(),
        target_schema_fingerprint: copy_plan
            .plan
            .target_catalog_fingerprint
            .as_assessed()
            .expect("execution plan target fingerprint")
            .clone(),
        outage_projection_digest: None,
        external_quiesce_attestation_digest: None,
        mysql_freeze_attestation_digest: Some(freeze.canonical_hash()?),
        conversion_policy: copy_plan.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    };
    let operations = copy_plan
        .plan
        .operations
        .iter()
        .map(|operation| OperationSpec {
            operation_id: operation.id.to_string(),
            dependencies: operation
                .dependencies
                .iter()
                .map(ToString::to_string)
                .collect(),
            is_copy: operation.kind == OperationKind::CopyTable,
            phase: if matches!(
                operation.kind,
                OperationKind::VerifyTable | OperationKind::VerifySchema
            ) {
                OperationPhase::Verification
            } else {
                OperationPhase::Execution
            },
        })
        .collect();
    let create_operation_id = copy_plan
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::CreateTable)
        .expect("create-table operation")
        .id
        .to_string();
    let mut journal = AppendJournal::create_new(
        &journal_path,
        Genesis {
            binding,
            reviewed_plan: copy_plan.clone(),
            accepted_outage_projection: None,
            accepted_external_quiesce: None,
            accepted_mysql_freeze: Some(freeze),
            operations,
        },
    )?;
    reconcile_mysql_pre_data_schema(
        &copy_plan,
        &target_factory,
        &mut journal,
        &CancellationToken::default(),
    )?;
    assert_eq!(
        journal.projection().operations.get(&create_operation_id),
        Some(&OperationState::Verified)
    );
    drop(journal);
    let mut journal = AppendJournal::open_resume(&journal_path)?;
    reconcile_mysql_pre_data_schema(
        &copy_plan,
        &target_factory,
        &mut journal,
        &CancellationToken::default(),
    )?;
    assert_eq!(
        target_factory.inspect_table(expected)?,
        MySqlTableState::Exact
    );
    let mut batch = RowBatch::new(
        vec![
            ColumnMeta {
                name: identifier("id"),
                ordinal: 1,
                vendor_type: "bigint".into(),
                nullable: false,
                collation: None,
                precision: Some(64),
                scale: Some(0),
                timezone_semantics: None,
            },
            ColumnMeta {
                name: identifier("payload"),
                ordinal: 2,
                vendor_type: "varchar".into(),
                nullable: false,
                collation: Some("utf8mb4_0900_bin".into()),
                precision: None,
                scale: None,
                timezone_semantics: None,
            },
        ],
        10,
        1024 * 1024,
    );
    batch.try_push(vec![DbValue::Signed(1), DbValue::Text("one".into())], 32)?;
    batch.try_push(vec![DbValue::Signed(2), DbValue::Text("two".into())], 32)?;
    let mut writer = target_factory.open_writer(CancellationToken::default())?;
    writer.begin()?;
    writer.insert(&expected.table, &batch)?;
    writer.commit()?;
    drop(writer);
    let mut verifier = target_factory.open_verifier(CancellationToken::default())?;
    let target_rows = verifier.select_page(&KeysetPage {
        table: expected.table.clone(),
        projection: vec![identifier("id"), identifier("payload")],
        key: vec![identifier("id")],
        after: None,
        limit: 10,
    })?;
    assert_eq!(target_rows.rows(), batch.rows());
    drop(verifier);

    let mut oversized = RowBatch::new(batch.columns().to_vec(), 1, 1024 * 1024);
    oversized.try_push(vec![DbValue::Signed(3), DbValue::Text("x".repeat(65))], 96)?;
    let mut writer = target_factory.open_writer(CancellationToken::default())?;
    writer.begin()?;
    assert!(writer.insert(&expected.table, &oversized).is_err());
    writer.rollback()?;
    drop(writer);
    let mut verifier = target_factory.open_verifier(CancellationToken::default())?;
    let target_rows = verifier.select_page(&KeysetPage {
        table: expected.table.clone(),
        projection: vec![identifier("id"), identifier("payload")],
        key: vec![identifier("id")],
        after: None,
        limit: 10,
    })?;
    assert_eq!(target_rows.rows(), batch.rows());

    let target = inspect_live_endpoint(target_config)?;
    assert!(target.catalog.namespaces[0].objects.iter().any(|object| {
        object.kind == sql_splitter::migration::model::CatalogObjectKind::Table
            && object.name == identifier("copy_items")
    }));
    Ok(())
}

#[test]
#[ignore = "requires an externally owned MySQL backup lock and persisted super_read_only"]
fn live_mysql_external_freeze_attestation() -> anyhow::Result<()> {
    let admin_path = required_path("SQL_SPLITTER_MYSQL_TEST_ADMIN_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_MYSQL_TEST_PLAN_OUTPUT")?;
    let admin_config = MySqlEndpointConfig::read(&admin_path)?;
    let reviewed: ReviewedPlan = read_json(&plan_path)?;
    let backup_lock_connection_id: u32 =
        std::env::var("SQL_SPLITTER_MYSQL_BACKUP_LOCK_CONNECTION_ID")?.parse()?;
    let backup_lock_owner_user = std::env::var("SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_USER")?;
    let backup_lock_owner_host = std::env::var("SQL_SPLITTER_MYSQL_BACKUP_LOCK_OWNER_HOST")?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let assertion = MySqlExternalFreezeAssertion {
        schema_version: MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION,
        profile_generation: "local-live-generation-1".into(),
        provider_reference: "local-docker-external-lock-owner".into(),
        activated_at_unix_seconds: now.saturating_sub(1),
        expires_at_unix_seconds: now + 300,
        continuity_token_hash: "d".repeat(64),
        backup_lock_connection_id,
        backup_lock_owner_user,
        backup_lock_owner_host,
    };
    let accepted = attest_mysql_external_freeze(&admin_config, &reviewed, &assertion)?;
    assert!(accepted.read_only);
    assert!(accepted.super_read_only);
    assert!(accepted.super_read_only_persisted);
    assert!(accepted.active_replication_channels.is_empty());
    assert_eq!(
        accepted.backup_lock_connection_id,
        backup_lock_connection_id
    );
    assert!(accepted.backup_lock_owner_thread_id > 0);
    assert!(
        accepted.server_start_lower_bound_unix_seconds
            <= accepted.server_start_upper_bound_unix_seconds
    );

    let current = attest_mysql_external_freeze(&admin_config, &reviewed, &assertion)?;
    validate_mysql_external_freeze_continuity(&accepted, &current)?;
    let mut replaced_owner = current;
    replaced_owner.backup_lock_connection_id += 1;
    assert!(validate_mysql_external_freeze_continuity(&accepted, &replaced_owner).is_err());
    let mut restarted = accepted.clone();
    restarted.server_start_lower_bound_unix_seconds =
        accepted.server_start_upper_bound_unix_seconds + 10;
    restarted.server_start_upper_bound_unix_seconds =
        restarted.server_start_lower_bound_unix_seconds + 2;
    assert!(validate_mysql_external_freeze_continuity(&accepted, &restarted).is_err());
    Ok(())
}
