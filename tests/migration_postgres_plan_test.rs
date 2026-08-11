#![cfg(feature = "enterprise-migration-spike")]

use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sql_splitter::migration::artifact::read_json;
use sql_splitter::migration::connection::{
    CancellationToken, ConnectionError, KeysetPage, SourceConnectionFactory,
    TargetConnectionFactory,
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

#[test]
#[ignore = "requires a TLS-enabled PostgreSQL slow_rows test view"]
fn live_control_session_cancels_the_active_query() -> anyhow::Result<()> {
    let source_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?)?;
    let factory = PostgresSourceFactory::new(source_config);
    let snapshot = factory.capture_snapshot()?;
    let mut control = factory.open_control()?;
    let mut reader = factory.open_reader(&snapshot, CancellationToken::default())?;
    let cancel_thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        control.cancel_active_statement()
    });
    let result = reader.select_page(&KeysetPage {
        table: QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("slow_rows")?,
        },
        projection: vec![Identifier::new("id")?, Identifier::new("payload")?],
        key: vec![Identifier::new("id")?],
        after: None,
        limit: 100,
    });
    cancel_thread
        .join()
        .map_err(|_| anyhow::anyhow!("control thread panicked"))??;
    assert_eq!(result, Err(ConnectionError::Cancelled));
    Ok(())
}

#[test]
#[ignore = "requires TLS-enabled PostgreSQL read-only and target roles"]
fn live_target_writer_round_trips_binary_protocol_values() -> anyhow::Result<()> {
    let source_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?)?;
    let target_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG")?)?;
    let mut administrator = connect(&target_config)?;
    administrator.batch_execute(
        "DROP TABLE IF EXISTS public.target_values; DROP TABLE IF EXISTS public.source_values; CREATE TABLE public.source_values (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, text_value text NOT NULL, binary_value bytea NOT NULL, float_value double precision NOT NULL, json_value jsonb NOT NULL, numeric_value numeric(20,5) NOT NULL, timestamp_value timestamptz NOT NULL); CREATE TABLE public.target_values (LIKE public.source_values INCLUDING ALL); GRANT SELECT ON public.source_values TO migration_reader; INSERT INTO public.source_values (text_value, binary_value, float_value, json_value, numeric_value, timestamp_value) VALUES ('exact text', decode('00ff10', 'hex'), '-0'::double precision, '{\"b\":2,\"a\":1}'::jsonb, 1234567890123.45000, '2026-08-11 10:11:12.123456+02')",
    )?;

    let source = PostgresSourceFactory::new(source_config);
    let snapshot = source.capture_snapshot()?;
    let mut reader = source.open_reader(&snapshot, CancellationToken::default())?;
    let source_table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("source_values")?,
    };
    let projection = [
        "id",
        "text_value",
        "binary_value",
        "float_value",
        "json_value",
        "numeric_value",
        "timestamp_value",
    ]
    .into_iter()
    .map(Identifier::new)
    .collect::<Result<Vec<_>, _>>()?;
    let key = vec![Identifier::new("id")?];
    let batch = reader.select_page(&KeysetPage {
        table: source_table,
        projection: projection.clone(),
        key: key.clone(),
        after: None,
        limit: 10,
    })?;
    assert_eq!(batch.len(), 1);

    let target = sql_splitter::migration::postgres::PostgresTargetFactory::new(target_config);
    let cancellation = CancellationToken::default();
    let mut writer = target.open_writer(cancellation.clone())?;
    writer.begin()?;
    let target_table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("target_values")?,
    };
    writer.insert(&target_table, &batch)?;
    writer.commit()?;

    let mut verifier = target.open_verifier(cancellation)?;
    let observed = verifier.select_page(&KeysetPage {
        table: target_table,
        projection,
        key,
        after: None,
        limit: 10,
    })?;
    assert_eq!(observed.rows(), batch.rows());
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
