#![cfg(feature = "enterprise-migration-spike")]

#[cfg(feature = "migration-fault-injection")]
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
#[cfg(feature = "migration-fault-injection")]
use std::process::Command;
#[cfg(feature = "migration-fault-injection")]
use std::thread;
#[cfg(feature = "migration-fault-injection")]
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "migration-fault-injection")]
use mysql::prelude::Queryable;
#[cfg(feature = "migration-fault-injection")]
use mysql::{Conn, OptsBuilder, SslOpts};
#[cfg(feature = "migration-fault-injection")]
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
#[cfg(feature = "migration-fault-injection")]
use postgres::config::SslMode;
#[cfg(feature = "migration-fault-injection")]
use postgres::{Client, Config};
#[cfg(feature = "migration-fault-injection")]
use postgres_openssl::MakeTlsConnector;

use sql_splitter::migration::append_journal::AppendJournal;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::artifact::read_json;
use sql_splitter::migration::artifact::write_json_new;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::connection::{CancellationToken, TargetConnectionFactory};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::conversion::{MigrationConversionMode, TableConversionPolicy};
use sql_splitter::migration::cross_dialect::{
    write_live_mysql_to_postgres_plan, write_live_postgres_to_mysql_plan,
};
use sql_splitter::migration::cross_dialect_execution::{
    execute_mysql_to_postgres_plan, execute_postgres_to_mysql_plan,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::cross_dialect_execution::{
    execute_mysql_to_postgres_plan_interrupted, execute_postgres_to_mysql_plan_interrupted,
    resume_mysql_to_postgres_plan, resume_postgres_to_mysql_plan,
    CrossDialectExecutionInterruption, InterruptedMySqlToPostgresExecution,
    InterruptedPostgresToMySqlExecution,
};
use sql_splitter::migration::journal::MigrationStatus;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::model::{DbValue, RowBatch};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::mysql::{MySqlEndpointConfig, MySqlTargetFactory};
use sql_splitter::migration::mysql_profile::{
    MySqlExternalFreezeAssertion, MYSQL_FREEZE_ASSERTION_SCHEMA_VERSION,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::plan::ReviewedPlan;
use sql_splitter::migration::postgres::PostgresEndpointConfig;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::postgres::PostgresTargetFactory;
use sql_splitter::migration::postgres_fence::install_postgres_write_fence;

#[cfg(feature = "migration-fault-injection")]
#[path = "support/postgres_commit_proxy.rs"]
mod cross_commit_proxy;
#[cfg(feature = "migration-fault-injection")]
use cross_commit_proxy::{CommitFaultMode, PostgresCommitProxy};

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

#[cfg(feature = "migration-fault-injection")]
fn selected_interruption() -> anyhow::Result<CrossDialectExecutionInterruption> {
    match std::env::var("SQL_SPLITTER_CROSS_INTERRUPTION")?.as_str() {
        "table-prepared" => Ok(CrossDialectExecutionInterruption::TablePrepared),
        "table-effect-applied" => Ok(CrossDialectExecutionInterruption::TableEffectApplied),
        "chunk-prepared" => Ok(CrossDialectExecutionInterruption::ChunkPrepared),
        "chunk-effect-applied" => Ok(CrossDialectExecutionInterruption::ChunkEffectApplied),
        "identity-prepared" => Ok(CrossDialectExecutionInterruption::IdentityPrepared),
        "identity-effect-applied" => Ok(CrossDialectExecutionInterruption::IdentityEffectApplied),
        "identity-committed" => Ok(CrossDialectExecutionInterruption::IdentityCommitted),
        "cancel-after-insert" => Ok(CrossDialectExecutionInterruption::CancelAfterInsert),
        "after-verification" => Ok(CrossDialectExecutionInterruption::AfterVerification),
        "after-postgres-fence-release" => {
            Ok(CrossDialectExecutionInterruption::AfterPostgresFenceRelease)
        }
        value => Err(anyhow::anyhow!(
            "unsupported SQL_SPLITTER_CROSS_INTERRUPTION {value}"
        )),
    }
}

#[cfg(feature = "migration-fault-injection")]
fn selected_network_mode() -> Option<CommitFaultMode> {
    match std::env::var("SQL_SPLITTER_CROSS_INTERRUPTION").as_deref() {
        Ok("network-not-forwarded") => Some(CommitFaultMode::NotForwarded),
        Ok("network-ack-lost") => Some(CommitFaultMode::AppliedAckLost),
        _ => None,
    }
}

fn assert_completed(state_path: &Path) -> anyhow::Result<()> {
    let journal = AppendJournal::open_resume(state_path)?;
    assert_eq!(
        journal.projection().status,
        MigrationStatus::CompletedWithApprovedTransformations
    );
    assert!(journal.projection().schema_verified);
    assert!(journal.projection().prepared_chunk.is_none());
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn reviewed_policies(reviewed: &ReviewedPlan) -> anyhow::Result<Vec<TableConversionPolicy>> {
    let MigrationConversionMode::CrossDialect { tables, .. } =
        &reviewed.plan.conversion_policy.mode
    else {
        return Err(anyhow::anyhow!("reviewed plan is not cross-dialect"));
    };
    Ok(tables.clone())
}

#[cfg(feature = "migration-fault-injection")]
fn extra_items_batch(policy: &TableConversionPolicy) -> anyhow::Result<RowBatch> {
    let columns = policy
        .row_policy
        .columns
        .iter()
        .map(|column| column.target.clone())
        .collect::<Vec<_>>();
    if columns.len() != 1 || policy.source_table.name.as_str() != "items" {
        return Err(anyhow::anyhow!(
            "items fault policy has an unexpected shape"
        ));
    }
    let mut batch = RowBatch::new(columns, 1, 1024);
    batch.try_push(vec![DbValue::Signed(99)], 8)?;
    Ok(batch)
}

#[cfg(feature = "migration-fault-injection")]
fn inject_postgres_target_extra_row(
    reviewed: &ReviewedPlan,
    target_path: &Path,
) -> anyhow::Result<()> {
    let policies = reviewed_policies(reviewed)?;
    let policy = policies
        .iter()
        .find(|policy| policy.source_table.name.as_str() == "items")
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no items policy"))?;
    let target_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no target catalog"))?;
    let target = PostgresTargetFactory::new_cross_dialect_with_cancellation(
        PostgresEndpointConfig::read(target_path)?,
        target_catalog,
        policies.clone(),
        CancellationToken::default(),
    )?;
    let mut writer = target.open_writer(CancellationToken::default())?;
    writer.begin()?;
    writer.insert(&policy.target_table, &extra_items_batch(policy)?)?;
    writer.commit()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn inject_mysql_target_extra_row(
    reviewed: &ReviewedPlan,
    target_path: &Path,
) -> anyhow::Result<()> {
    let policies = reviewed_policies(reviewed)?;
    let policy = policies
        .iter()
        .find(|policy| policy.source_table.name.as_str() == "items")
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no items policy"))?;
    let target_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no target catalog"))?
        .clone();
    let target_evidence = reviewed
        .plan
        .mysql_target_snapshot_evidence
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no MySQL target evidence"))?
        .clone();
    let target = MySqlTargetFactory::new_cross_dialect_with_cancellation(
        MySqlEndpointConfig::read(target_path)?,
        target_catalog,
        policies.clone(),
        target_evidence,
        CancellationToken::default(),
    )?;
    let mut writer = target.open_writer(CancellationToken::default())?;
    writer.begin()?;
    writer.insert(&policy.target_table, &extra_items_batch(policy)?)?;
    writer.commit()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_docker(args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new("docker").args(args).output()?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "docker fault command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn withdraw_mysql_source_freeze() -> anyhow::Result<()> {
    let container = std::env::var("SQL_SPLITTER_CROSS_MYSQL_CONTAINER")?;
    run_docker(&[
        "exec",
        &container,
        "mysql",
        "-uroot",
        "-prootpass",
        "-Nse",
        "SET PERSIST super_read_only=OFF",
    ])
}

#[cfg(feature = "migration-fault-injection")]
fn disable_postgres_source_fence() -> anyhow::Result<()> {
    let container = std::env::var("SQL_SPLITTER_CROSS_PG_CONTAINER")?;
    run_docker(&[
        "exec",
        &container,
        "psql",
        "-U",
        "postgres",
        "-d",
        "cross_pg_source",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        "ALTER EVENT TRIGGER sql_splitter_migration_fence_ddl DISABLE",
    ])
}

#[cfg(feature = "migration-fault-injection")]
fn proxied_postgres_target(
    direct_path: &Path,
    directory: &Path,
    mode: CommitFaultMode,
) -> anyhow::Result<(PathBuf, PostgresCommitProxy)> {
    let mut config = PostgresEndpointConfig::read(direct_path)?;
    let upstream = (config.host.as_str(), config.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL target endpoint did not resolve"))?;
    let proxy = PostgresCommitProxy::start(upstream, mode)?;
    config.port = proxy.data_port();
    let path = directory.join("postgres-target-proxy.toml");
    std::fs::write(&path, toml::to_string(&config)?)?;
    Ok((path, proxy))
}

#[cfg(feature = "migration-fault-injection")]
fn proxied_mysql_target(
    direct_path: &Path,
    directory: &Path,
    mode: CommitFaultMode,
) -> anyhow::Result<(PathBuf, PostgresCommitProxy)> {
    let mut config = MySqlEndpointConfig::read(direct_path)?;
    let upstream = (config.host.as_str(), config.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("MySQL target endpoint did not resolve"))?;
    let proxy = PostgresCommitProxy::start(upstream, mode)?;
    config.port = proxy.data_port();
    let path = directory.join("mysql-target-proxy.toml");
    std::fs::write(&path, toml::to_string(&config)?)?;
    Ok((path, proxy))
}

#[cfg(feature = "migration-fault-injection")]
fn connect_postgres(config: &PostgresEndpointConfig) -> anyhow::Result<Client> {
    let password = std::env::var(&config.credential_env)?;
    let mut pg = Config::new();
    pg.host(&config.host)
        .port(config.port)
        .dbname(&config.database)
        .user(&config.user)
        .password(password)
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
    let mut tls = SslConnector::builder(SslMethod::tls())?;
    if let Some(path) = &config.tls.ca_certificate {
        tls.set_ca_file(path)?;
    }
    if let (Some(certificate_path), Some(key_path)) = (
        &config.tls.client_certificate,
        &config.tls.client_private_key,
    ) {
        tls.set_certificate_chain_file(certificate_path)?;
        tls.set_private_key_file(key_path, SslFiletype::PEM)?;
        tls.check_private_key()?;
    }
    if config.tls.insecure {
        tls.set_verify(SslVerifyMode::NONE);
    }
    let mut connector = MakeTlsConnector::new(tls.build());
    if config.tls.insecure {
        connector.set_callback(|configuration, _| {
            configuration.set_verify_hostname(false);
            Ok(())
        });
    }
    Ok(pg.connect(connector)?)
}

#[cfg(feature = "migration-fault-injection")]
fn assert_postgres_identity_next_value(config_path: &Path) -> anyhow::Result<()> {
    let mut client = connect_postgres(&PostgresEndpointConfig::read(config_path)?)?;
    let identity: String = client
        .query_one(
            "SELECT a.attidentity::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='public' AND c.relname='items_from_mysql' AND a.attname='id'",
            &[],
        )?
        .get(0);
    assert_eq!(identity, "d");
    let inserted: i64 = client
        .query_one(
            "INSERT INTO public.items_from_mysql DEFAULT VALUES RETURNING id",
            &[],
        )?
        .get(0);
    assert_eq!(inserted, 42);
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn connect_mysql(config: &MySqlEndpointConfig) -> anyhow::Result<Conn> {
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
    assert_completed(&state_path)?;
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
    assert_completed(&state_path)?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
#[test]
#[ignore = "requires the disposable PostgreSQL and MySQL TLS matrix"]
fn live_mysql_to_postgres_resumes_selected_recovery_boundary() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_CROSS_MYSQL_SOURCE_CONFIG")?;
    let source_metadata = required_path("SQL_SPLITTER_CROSS_MYSQL_SOURCE_METADATA_CONFIG")?;
    let freeze_admin = required_path("SQL_SPLITTER_CROSS_MYSQL_FREEZE_CONFIG")?;
    let direct_target = required_path("SQL_SPLITTER_CROSS_PG_TARGET_CONFIG")?;
    let mapping = required_path("SQL_SPLITTER_CROSS_MYSQL_TO_PG_MAPPING")?;
    let directory = private_tempdir()?;
    let network_mode = selected_network_mode();
    let (target, commit_proxy) = if let Some(mode) = network_mode {
        let (target, proxy) = proxied_postgres_target(&direct_target, directory.path(), mode)?;
        (target, Some(proxy))
    } else {
        (direct_target.clone(), None)
    };
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
    let interruption = commit_proxy
        .as_ref()
        .map_or_else(selected_interruption, |proxy| {
            Ok(CrossDialectExecutionInterruption::NetworkCommitFault(
                proxy.control_port(),
            ))
        })?;
    if interruption == CrossDialectExecutionInterruption::AfterPostgresFenceRelease {
        return Err(anyhow::anyhow!(
            "PostgreSQL fence release is not a MySQL-source recovery boundary"
        ));
    }
    let execution = InterruptedMySqlToPostgresExecution {
        plan_path: &plan_path,
        source_config_path: &source,
        source_metadata_config_path: &source_metadata,
        freeze_config_path: &freeze_admin,
        target_config_path: &target,
        freeze_assertion_path: &assertion_path,
        approval_reference: "cross-dialect-live-approval",
        state_path: &state_path,
        interruption,
    };
    let error = if network_mode == Some(CommitFaultMode::AppliedAckLost) {
        let proxy = commit_proxy.as_ref().expect("proxy is present");
        let mut observer = connect_postgres(&PostgresEndpointConfig::read(&direct_target)?)?;
        let result = thread::scope(|scope| -> anyhow::Result<_> {
            let handle = scope.spawn(|| execute_mysql_to_postgres_plan_interrupted(execution));
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let exists: Option<String> = observer
                    .query_one(
                        "SELECT to_regclass('public.bool_values_from_mysql')::text",
                        &[],
                    )?
                    .get(0);
                let rows = if exists.is_some() {
                    observer
                        .query_one("SELECT count(*) FROM public.bool_values_from_mysql", &[])?
                        .get::<_, i64>(0)
                } else {
                    0
                };
                if rows == 2 {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "PostgreSQL committed cross-dialect chunk was not visible before CUT"
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
            proxy.wait_for("withheld PostgreSQL COMMIT response", |telemetry| {
                telemetry.forwarded_client_bytes_after_arm > 0
                    && telemetry.dropped_server_bytes_after_arm > 0
            })?;
            proxy.cut()?;
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("cross-dialect PostgreSQL runner panicked"))
        })?;
        result.unwrap_err()
    } else {
        execute_mysql_to_postgres_plan_interrupted(execution).unwrap_err()
    };
    if let Some(proxy) = &commit_proxy {
        assert!(error.to_string().contains("commit outcome is unknown"));
        match network_mode.expect("network mode is present") {
            CommitFaultMode::NotForwarded => {
                proxy.wait_for(
                    "discarded cross-dialect PostgreSQL COMMIT bytes",
                    |telemetry| telemetry.dropped_client_bytes_after_arm > 0,
                )?;
                assert_eq!(proxy.telemetry()?.forwarded_client_bytes_after_arm, 0);
            }
            CommitFaultMode::AppliedAckLost => {
                let telemetry = proxy.telemetry()?;
                assert!(telemetry.forwarded_client_bytes_after_arm > 0);
                assert_eq!(telemetry.forwarded_server_bytes_after_arm, 0);
                assert!(telemetry.dropped_server_bytes_after_arm > 0);
            }
        }
    } else if interruption == CrossDialectExecutionInterruption::CancelAfterInsert {
        assert!(error.to_string().contains("Cancelled"));
    } else {
        assert!(error
            .to_string()
            .contains("injected cross-dialect execution interruption"));
    }
    let interrupted = AppendJournal::open_resume(&state_path)?;
    assert_ne!(
        interrupted.projection().status,
        MigrationStatus::CompletedWithApprovedTransformations
    );
    drop(interrupted);

    if std::env::var_os("SQL_SPLITTER_CROSS_EXPECT_SOURCE_LOSS").is_some() {
        withdraw_mysql_source_freeze()?;
        let error = resume_mysql_to_postgres_plan(
            &state_path,
            &source,
            &source_metadata,
            &freeze_admin,
            &target,
            &assertion_path,
        )
        .unwrap_err();
        assert!(error.to_string().contains("freeze"));
        let journal = AppendJournal::open_resume(&state_path)?;
        assert_ne!(
            journal.projection().status,
            MigrationStatus::CompletedWithApprovedTransformations
        );
        return Ok(());
    }

    if std::env::var_os("SQL_SPLITTER_CROSS_EXPECT_TARGET_DRIFT").is_some() {
        let reviewed: ReviewedPlan = read_json(&plan_path)?;
        inject_postgres_target_extra_row(&reviewed, &target)?;
        let error = resume_mysql_to_postgres_plan(
            &state_path,
            &source,
            &source_metadata,
            &freeze_admin,
            &target,
            &assertion_path,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside committed intervals"));
        let journal = AppendJournal::open_resume(&state_path)?;
        assert_ne!(
            journal.projection().status,
            MigrationStatus::CompletedWithApprovedTransformations
        );
        return Ok(());
    }

    let report = resume_mysql_to_postgres_plan(
        &state_path,
        &source,
        &source_metadata,
        &freeze_admin,
        &target,
        &assertion_path,
    )?;
    assert!(report.copied_rows > 1);
    assert_completed(&state_path)?;
    assert_postgres_identity_next_value(&direct_target)
}

#[cfg(feature = "migration-fault-injection")]
#[test]
#[ignore = "requires the disposable PostgreSQL and MySQL TLS matrix"]
fn live_postgres_to_mysql_resumes_selected_recovery_boundary() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_CROSS_PG_SOURCE_CONFIG")?;
    let fence_admin = required_path("SQL_SPLITTER_CROSS_PG_FENCE_ADMIN_CONFIG")?;
    let direct_target = required_path("SQL_SPLITTER_CROSS_MYSQL_TARGET_CONFIG")?;
    let target_metadata = required_path("SQL_SPLITTER_CROSS_MYSQL_TARGET_METADATA_CONFIG")?;
    let mapping = required_path("SQL_SPLITTER_CROSS_PG_TO_MYSQL_MAPPING")?;
    let directory = private_tempdir()?;
    let network_mode = selected_network_mode();
    let (target, commit_proxy) = if let Some(mode) = network_mode {
        let (target, proxy) = proxied_mysql_target(&direct_target, directory.path(), mode)?;
        (target, Some(proxy))
    } else {
        (direct_target.clone(), None)
    };
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
    let interruption = commit_proxy
        .as_ref()
        .map_or_else(selected_interruption, |proxy| {
            Ok(CrossDialectExecutionInterruption::NetworkCommitFault(
                proxy.control_port(),
            ))
        })?;
    let execution = InterruptedPostgresToMySqlExecution {
        plan_path: &plan_path,
        source_config_path: &source,
        fence_admin_config_path: &fence_admin,
        fence_artifact_path: &fence_path,
        target_config_path: &target,
        target_metadata_config_path: &target_metadata,
        approval_reference: "cross-dialect-live-approval",
        state_path: &state_path,
        interruption,
    };
    let error = if network_mode == Some(CommitFaultMode::AppliedAckLost) {
        let proxy = commit_proxy.as_ref().expect("proxy is present");
        let mut observer = connect_mysql(&MySqlEndpointConfig::read(&direct_target)?)?;
        let result = thread::scope(|scope| -> anyhow::Result<_> {
            let handle = scope.spawn(|| execute_postgres_to_mysql_plan_interrupted(execution));
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let exists: Option<u8> = observer.exec_first(
                    "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    ("cross_mysql_target", "bool_values_from_pg"),
                )?;
                let rows: u64 = if exists.is_some() {
                    observer
                        .query_first("SELECT COUNT(*) FROM cross_mysql_target.bool_values_from_pg")?
                        .unwrap_or(0)
                } else {
                    0
                };
                if rows == 2 {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "MySQL committed cross-dialect chunk was not visible before CUT"
                    ));
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
                .map_err(|_| anyhow::anyhow!("cross-dialect MySQL runner panicked"))
        })?;
        result.unwrap_err()
    } else {
        execute_postgres_to_mysql_plan_interrupted(execution).unwrap_err()
    };
    if let Some(proxy) = &commit_proxy {
        assert!(error.to_string().contains("commit outcome is unknown"));
        match network_mode.expect("network mode is present") {
            CommitFaultMode::NotForwarded => {
                proxy.wait_for("discarded cross-dialect MySQL COMMIT bytes", |telemetry| {
                    telemetry.dropped_client_bytes_after_arm > 0
                })?;
                assert_eq!(proxy.telemetry()?.forwarded_client_bytes_after_arm, 0);
            }
            CommitFaultMode::AppliedAckLost => {
                let telemetry = proxy.telemetry()?;
                assert!(telemetry.forwarded_client_bytes_after_arm > 0);
                assert_eq!(telemetry.forwarded_server_bytes_after_arm, 0);
                assert!(telemetry.dropped_server_bytes_after_arm > 0);
            }
        }
    } else if interruption == CrossDialectExecutionInterruption::CancelAfterInsert {
        assert!(error.to_string().contains("Cancelled"));
    } else {
        assert!(error
            .to_string()
            .contains("injected cross-dialect execution interruption"));
    }
    let interrupted = AppendJournal::open_resume(&state_path)?;
    assert_ne!(
        interrupted.projection().status,
        MigrationStatus::CompletedWithApprovedTransformations
    );
    drop(interrupted);

    if std::env::var_os("SQL_SPLITTER_CROSS_EXPECT_SOURCE_LOSS").is_some() {
        disable_postgres_source_fence()?;
        let error = resume_postgres_to_mysql_plan(
            &state_path,
            &source,
            &fence_admin,
            &fence_path,
            &target,
            &target_metadata,
        )
        .unwrap_err();
        assert!(error.to_string().contains("fence"));
        let journal = AppendJournal::open_resume(&state_path)?;
        assert_ne!(
            journal.projection().status,
            MigrationStatus::CompletedWithApprovedTransformations
        );
        return Ok(());
    }

    if std::env::var_os("SQL_SPLITTER_CROSS_EXPECT_TARGET_DRIFT").is_some() {
        let reviewed: ReviewedPlan = read_json(&plan_path)?;
        inject_mysql_target_extra_row(&reviewed, &target)?;
        let error = resume_postgres_to_mysql_plan(
            &state_path,
            &source,
            &fence_admin,
            &fence_path,
            &target,
            &target_metadata,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside committed intervals"));
        let journal = AppendJournal::open_resume(&state_path)?;
        assert_ne!(
            journal.projection().status,
            MigrationStatus::CompletedWithApprovedTransformations
        );
        return Ok(());
    }

    let report = resume_postgres_to_mysql_plan(
        &state_path,
        &source,
        &fence_admin,
        &fence_path,
        &target,
        &target_metadata,
    )?;
    assert!(report.copied_rows > 1);
    assert_completed(&state_path)
}
