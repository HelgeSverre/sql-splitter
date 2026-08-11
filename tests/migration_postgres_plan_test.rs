#![cfg(feature = "enterprise-migration-spike")]

use std::path::PathBuf;
use std::time::Duration;

use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sql_splitter::migration::artifact::read_json;
use sql_splitter::migration::connection::{
    CancellationToken, ConnectionError, KeysetPage, SourceConnectionFactory,
};
use sql_splitter::migration::model::{DbValue, Identifier, KeyTuple, QualifiedTable};
use sql_splitter::migration::plan::ReviewedPlan;
use sql_splitter::migration::postgres::{
    write_live_plan, PostgresEndpointConfig, PostgresSourceFactory,
};

#[test]
#[ignore = "requires TLS-enabled PostgreSQL source and empty target databases"]
fn live_plan_is_read_only_self_contained_and_deterministic() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?;
    let directory = tempfile::tempdir()?;
    let first_path = directory.path().join("first.json");
    let second_path = directory.path().join("second.json");

    let first = write_live_plan(&source, &target, &first_path)?;
    let second = write_live_plan(&source, &target, &second_path)?;
    assert_eq!(first, second);
    assert_eq!(std::fs::read(&first_path)?, std::fs::read(&second_path)?);
    assert!(first.plan.source_catalog.is_some());
    assert!(first.plan.target_catalog.is_some());
    assert!(first
        .plan
        .target_catalog
        .as_ref()
        .is_some_and(|catalog| catalog
            .namespaces
            .iter()
            .all(|namespace| namespace.objects.is_empty())));
    assert!(!first.plan.operations.is_empty());
    assert_eq!(read_json::<ReviewedPlan>(first_path)?, first);
    Ok(())
}

#[test]
#[ignore = "requires TLS-enabled PostgreSQL read-only and mutator roles"]
fn live_snapshot_paging_is_stable_during_concurrent_writes() -> anyhow::Result<()> {
    let source_path = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let mutator_path = required_path("SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG")?;
    let source_config = PostgresEndpointConfig::read(source_path)?;
    let mut mutator = connect(&PostgresEndpointConfig::read(&mutator_path)?)?;
    mutator.batch_execute(
        "TRUNCATE TABLE public.live_snapshot_rows; INSERT INTO public.live_snapshot_rows (id, payload) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )?;

    let factory = PostgresSourceFactory::new(source_config);
    let snapshot = factory.capture_snapshot()?;
    let mut reader = factory.open_reader(&snapshot, CancellationToken::default())?;
    let table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("live_snapshot_rows")?,
    };
    let projection = vec![Identifier::new("id")?, Identifier::new("payload")?];
    let key = vec![Identifier::new("id")?];

    let first = reader.select_page(&KeysetPage {
        table: table.clone(),
        projection: projection.clone(),
        key: key.clone(),
        after: None,
        limit: 2,
    })?;
    assert_eq!(first.len(), 2);
    mutator.batch_execute(
        "UPDATE public.live_snapshot_rows SET payload = 'changed' WHERE id = 2; DELETE FROM public.live_snapshot_rows WHERE id = 3; INSERT INTO public.live_snapshot_rows (id, payload) VALUES (4, 'four')",
    )?;

    let second = reader.select_page(&KeysetPage {
        table,
        projection,
        key,
        after: Some(KeyTuple::new(vec![DbValue::Signed(2)])),
        limit: 2,
    })?;
    assert_eq!(
        second.rows(),
        &[vec![DbValue::Signed(3), DbValue::Text("three".into())]]
    );
    assert_eq!(reader.snapshot(), &snapshot);
    drop(reader);

    let mutator_factory = PostgresSourceFactory::new(PostgresEndpointConfig::read(mutator_path)?);
    assert!(matches!(
        mutator_factory.capture_snapshot(),
        Err(ConnectionError::InvalidRequest(message))
            if message.contains("write capability")
    ));
    Ok(())
}

fn connect(config: &PostgresEndpointConfig) -> anyhow::Result<Client> {
    let password = std::env::var(&config.credential_env)?;
    let mut pg = Config::new();
    pg.host(&config.host)
        .port(config.port)
        .dbname(&config.database)
        .user(&config.user)
        .password(password)
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
    let mut tls = TlsConnector::builder();
    if let Some(path) = &config.tls.ca_certificate {
        tls.add_root_certificate(Certificate::from_pem(&std::fs::read(path)?)?);
    }
    if config.tls.insecure {
        tls.danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }
    Ok(pg.connect(MakeTlsConnector::new(tls.build()?))?)
}

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} must name a PostgreSQL endpoint config"))
}
