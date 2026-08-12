#![cfg(feature = "enterprise-migration-spike")]

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use sql_splitter::migration::append_journal::AppendJournal;
use sql_splitter::migration::artifact::write_json_new;
use sql_splitter::migration::cross_dialect::{
    write_live_mysql_to_postgres_plan, write_live_postgres_to_mysql_plan,
};
use sql_splitter::migration::cross_dialect_execution::{
    execute_mysql_to_postgres_plan, execute_postgres_to_mysql_plan,
};
use sql_splitter::migration::journal::MigrationStatus;
use sql_splitter::migration::mysql_profile::{
    MySqlExternalFreezeAssertion, MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION,
};
use sql_splitter::migration::postgres::PostgresEndpointConfig;
use sql_splitter::migration::postgres_fence::install_postgres_write_fence;

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} is required"))
}

fn private_tempdir() -> anyhow::Result<tempfile::TempDir> {
    Ok(tempfile::Builder::new()
        .prefix("cross-dialect-live-")
        .tempdir_in(".")?)
}

fn write_freeze_assertion(path: &Path) -> anyhow::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    write_json_new(
        path,
        &MySqlExternalFreezeAssertion {
            schema_version: MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION,
            profile_generation: "cross-dialect-live-generation".into(),
            provider_reference: "disposable-cross-dialect-matrix".into(),
            activated_at_unix_seconds: now.saturating_sub(30),
            expires_at_unix_seconds: now.saturating_add(600),
            continuity_token_hash: "d".repeat(64),
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

#[test]
#[ignore = "requires the disposable PostgreSQL and MySQL TLS matrix"]
fn live_postgres_to_mysql_executes_bounded_verified_conversion() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_CROSS_PG_SOURCE_CONFIG")?;
    let fence_admin = required_path("SQL_SPLITTER_CROSS_PG_FENCE_ADMIN_CONFIG")?;
    let target = required_path("SQL_SPLITTER_CROSS_MYSQL_TARGET_CONFIG")?;
    let target_metadata = required_path("SQL_SPLITTER_CROSS_MYSQL_TARGET_METADATA_CONFIG")?;
    let mapping = required_path("SQL_SPLITTER_CROSS_PG_TO_MYSQL_MAPPING")?;
    let directory = private_tempdir()?;
    let plan_path = directory.path().join("postgres-to-mysql-plan.json");
    let fence_path = directory.path().join("postgres-fence.json");
    let state_path = directory.path().join("postgres-to-mysql.state");

    let reviewed = write_live_postgres_to_mysql_plan(
        &source,
        &target,
        &target_metadata,
        &mapping,
        &plan_path,
    )?;
    let admin = PostgresEndpointConfig::read(&fence_admin)?;
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let report = execute_postgres_to_mysql_plan(
        &plan_path,
        &source,
        &fence_admin,
        &fence_path,
        &target,
        &target_metadata,
        "cross-dialect-live-approval",
        &state_path,
    )?;

    assert!(report.copied_rows > 1);
    assert!(report.committed_chunks > 1);
    let journal = AppendJournal::open_resume(&state_path)?;
    assert_eq!(
        journal.projection().status,
        MigrationStatus::CompletedWithApprovedTransformations
    );
    assert!(journal.projection().schema_verified);
    Ok(())
}

#[test]
#[ignore = "requires the disposable MySQL and PostgreSQL TLS matrix"]
fn live_mysql_to_postgres_executes_bounded_verified_conversion() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_CROSS_MYSQL_SOURCE_CONFIG")?;
    let source_metadata = required_path("SQL_SPLITTER_CROSS_MYSQL_SOURCE_METADATA_CONFIG")?;
    let freeze_admin = required_path("SQL_SPLITTER_CROSS_MYSQL_FREEZE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_CROSS_PG_TARGET_CONFIG")?;
    let mapping = required_path("SQL_SPLITTER_CROSS_MYSQL_TO_PG_MAPPING")?;
    let directory = private_tempdir()?;
    let plan_path = directory.path().join("mysql-to-postgres-plan.json");
    let assertion_path = directory.path().join("mysql-freeze-assertion.json");
    let state_path = directory.path().join("mysql-to-postgres.state");

    write_freeze_assertion(&assertion_path)?;
    write_live_mysql_to_postgres_plan(
        &source,
        &source_metadata,
        &freeze_admin,
        &target,
        &mapping,
        &plan_path,
    )?;
    let report = execute_mysql_to_postgres_plan(
        &plan_path,
        &source,
        &source_metadata,
        &freeze_admin,
        &target,
        &assertion_path,
        "cross-dialect-live-approval",
        &state_path,
    )?;

    assert!(report.copied_rows > 1);
    assert!(report.committed_chunks > 1);
    let journal = AppendJournal::open_resume(&state_path)?;
    assert_eq!(
        journal.projection().status,
        MigrationStatus::CompletedWithApprovedTransformations
    );
    assert!(journal.projection().schema_verified);
    Ok(())
}
