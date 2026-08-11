#![cfg(feature = "enterprise-migration-spike")]

use std::path::PathBuf;
use std::time::Duration;

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, SslOpts};

use sql_splitter::migration::connection::{CancellationToken, KeysetPage, SourceConnectionFactory};
use sql_splitter::migration::model::{DbValue, Identifier, KeyTuple, QualifiedTable};
use sql_splitter::migration::mysql::{
    inspect_live_endpoint, mysql_auto_increment_states, write_live_plan, MySqlEndpointConfig,
    MySqlSourceFactory, MYSQL_CONSISTENCY_SNAPSHOT,
};
use sql_splitter::migration::plan::UnsupportedObjectCode;

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} is required"))
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
    let source_config = MySqlEndpointConfig::read(&source_path)?;
    let target_config = MySqlEndpointConfig::read(&target_path)?;
    let admin_config = MySqlEndpointConfig::read(&admin_path)?;

    let source = inspect_live_endpoint(source_config.clone())?;
    assert_eq!(source.catalog.dialect, "mysql");
    assert!(source.server_version.starts_with("8.0.") || source.server_version.starts_with("8.4."));
    assert!(source.snapshot_evidence.transaction_read_only);
    assert!(!source.snapshot_evidence.catalog_snapshot_protected);
    assert_eq!(source.snapshot_evidence.information_schema_stats_expiry, 0);
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
    assert!(source.blockers.iter().any(|blocker| {
        blocker.object_kind == "catalog_visibility" && blocker.reason.contains("account-dependent")
    }));
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
    assert!(reviewed
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
    assert!(reviewed.plan.unsupported_objects.blocks_execution());

    let target = inspect_live_endpoint(target_config)?;
    assert!(target.catalog.namespaces[0]
        .objects
        .iter()
        .all(|object| matches!(
            &object.kind,
            sql_splitter::migration::model::CatalogObjectKind::Vendor(kind)
                if kind == "mysql_privilege"
        )));
    Ok(())
}
