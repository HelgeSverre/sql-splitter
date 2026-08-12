#![cfg(feature = "enterprise-migration-spike")]

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, SslOpts};

use sql_splitter::migration::append_journal::{
    AppendJournal, Genesis, OperationPhase, OperationSpec,
};
use sql_splitter::migration::artifact::{read_json, write_json_new};
use sql_splitter::migration::canonical::CANONICAL_ENCODING_VERSION;
use sql_splitter::migration::connection::{
    CancellationToken, KeysetPage, SourceConnectionFactory, TargetConnectionFactory,
};
use sql_splitter::migration::journal::{ConsistencyEvidence, OperationState, ResumeBinding};
use sql_splitter::migration::model::{
    ColumnMeta, DbValue, Identifier, KeyTuple, QualifiedTable, RowBatch,
};
use sql_splitter::migration::mysql::{
    attest_mysql_external_freeze, build_plan, build_plan_with_visibility,
    collect_mysql_metadata_visibility, inspect_live_endpoint, mysql_auto_increment_states,
    mysql_catalog_fingerprint, mysql_table_definitions, mysql_tls_binding,
    validate_mysql_external_freeze_continuity, write_live_plan, write_live_plan_with_visibility,
    MySqlEndpointConfig, MySqlSourceFactory, MySqlTableState, MySqlTargetFactory,
    MYSQL_CONSISTENCY_SNAPSHOT,
};
use sql_splitter::migration::mysql_execution::{
    execute_live_mysql_frozen_plan, reconcile_mysql_pre_data_schema, resume_live_mysql_frozen_plan,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::mysql_execution::{
    execute_live_mysql_frozen_plan_interrupted, MySqlExecutionInterruption,
    MySqlInterruptedExecution,
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
    let mut admin = connect(&admin_config)?;
    admin.query_drop("UPDATE migration_source.items SET name='changed' WHERE id=2")?;
    admin.query_drop("INSERT INTO migration_source.items(id, name) VALUES (3, 'three')")?;
    let mut reader = factory.open_reader(&token, CancellationToken::default())?;
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
