#![cfg(feature = "enterprise-migration-spike")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::Context;
use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sql_splitter::migration::artifact::read_json;
use sql_splitter::migration::connection::{
    CancellationToken, ConnectionError, KeysetPage, SourceConnectionFactory,
    TargetConnectionFactory,
};
use sql_splitter::migration::journal::{MigrationState, MigrationStatus};
use sql_splitter::migration::model::{
    CatalogNamespace, CatalogObject, CatalogObjectKind, DbValue, Identifier, KeyTuple,
    QualifiedTable, VendorCatalog,
};
use sql_splitter::migration::plan::ReviewedPlan;
use sql_splitter::migration::postgres::{
    write_live_plan, write_live_plan_with_consistency, PostgresConsistencyMode,
    PostgresEndpointConfig, PostgresSourceFactory,
};
use sql_splitter::migration::postgres_fence::{
    attest_postgres_write_fence, install_postgres_write_fence, InstalledPostgresFence,
};
use sql_splitter::migration::runner::{
    execute_postgres_fenced_plan_with_interruption, execute_postgres_plan,
    resume_postgres_fenced_plan,
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
    let source_admin_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG")?)?;
    let target_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?)?;
    let mut source_administrator = connect(&source_admin_config)?;
    source_administrator.batch_execute(
        "DROP TABLE IF EXISTS public.source_values; CREATE TABLE public.source_values (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, text_value text NOT NULL, binary_value bytea NOT NULL, float_value double precision NOT NULL, json_value jsonb NOT NULL, numeric_value numeric(20,5) NOT NULL, timestamp_value timestamptz NOT NULL); GRANT SELECT ON public.source_values TO migration_reader; INSERT INTO public.source_values (text_value, binary_value, float_value, json_value, numeric_value, timestamp_value) VALUES ('exact text', decode('00ff10', 'hex'), '-0'::double precision, '{\"b\":2,\"a\":1}'::jsonb, 1234567890123.45000, '2026-08-11 10:11:12.123456+02')",
    )?;
    let mut target_administrator = connect(&target_config)?;
    target_administrator.batch_execute(
        "DROP TABLE IF EXISTS public.target_values; CREATE TABLE public.target_values (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, text_value text NOT NULL, binary_value bytea NOT NULL, float_value double precision NOT NULL, json_value jsonb NOT NULL, numeric_value numeric(20,5) NOT NULL, timestamp_value timestamptz NOT NULL)",
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

#[test]
#[ignore = "requires an empty TLS-enabled PostgreSQL target owned by the target role"]
fn live_pre_data_ddl_is_create_only_and_rechecks_emptiness() -> anyhow::Result<()> {
    let target_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?)?;
    let target = sql_splitter::migration::postgres::PostgresTargetFactory::new(target_config);
    target.assert_empty_and_owned()?;
    target.create_pre_data_schema(&ddl_catalog()?)?;
    assert!(matches!(
        target.create_pre_data_schema(&ddl_catalog()?),
        Err(ConnectionError::InvalidRequest(message)) if message.contains("not empty")
    ));
    Ok(())
}

#[test]
#[ignore = "requires dedicated TLS-enabled PostgreSQL execution databases"]
fn live_reviewed_plan_executes_and_strictly_finalizes() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_RUN_TARGET_CONFIG")?;
    let directory = tempfile::tempdir()?;
    let plan_path = directory.path().join("reviewed-plan.json");
    let state_path = directory.path().join("migration-state.json");
    let reviewed = write_live_plan(&source, &target, &plan_path)?;
    assert!(!reviewed.plan.unsupported_objects.blocks_execution());

    let report = execute_postgres_plan(
        &plan_path,
        &source,
        &target,
        "LIVE-TEST-APPROVAL",
        &state_path,
    )?;
    assert_eq!(report.copied_rows, 3);
    assert!(report.committed_chunks >= 1);
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state
        .operations
        .iter()
        .all(|operation| operation.state
            == sql_splitter::migration::journal::OperationState::Verified));
    assert!(state
        .chunks
        .iter()
        .all(|chunk| chunk.state == sql_splitter::migration::journal::ChunkState::Committed));

    let mut target_client = connect(&PostgresEndpointConfig::read(target)?)?;
    let rows = target_client.query("SELECT id, name FROM public.accounts ORDER BY id", &[])?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[2].get::<_, String>(1), "three");
    Ok(())
}

#[test]
#[ignore = "requires dedicated TLS-enabled PostgreSQL source and superuser fence admin"]
fn live_write_fence_install_is_durable() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?;
    let admin = required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_PG_FENCE_PLAN")?;
    let reviewed = write_live_plan_with_consistency(
        &source,
        &target,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    let admin_config = PostgresEndpointConfig::read(&admin)?;
    install_postgres_write_fence(
        &admin_config,
        &reviewed,
        required_path("SQL_SPLITTER_PG_FENCE_ARTIFACT")?,
    )?;
    Ok(())
}

#[test]
#[ignore = "requires a previously installed PostgreSQL fence and protected artifact"]
fn live_write_fence_attests_after_restart_blocks_writes_and_releases() -> anyhow::Result<()> {
    let admin = required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?;
    let source = required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?;
    let plan = required_path("SQL_SPLITTER_PG_FENCE_PLAN")?;
    let artifact = required_path("SQL_SPLITTER_PG_FENCE_ARTIFACT")?;
    let state_path = required_path("SQL_SPLITTER_PG_FENCE_STATE")?;
    let installed: InstalledPostgresFence = read_json(&artifact)?;
    let admin_config = PostgresEndpointConfig::read(&admin)?;
    attest_postgres_write_fence(&admin_config, &installed.evidence)
        .context("attest durable fence after PostgreSQL restart")?;

    let mut client = connect(&admin_config)?;
    for statement in [
        "INSERT INTO public.accounts VALUES (10, 'blocked')",
        "UPDATE public.accounts SET name = 'blocked' WHERE id = 1",
        "DELETE FROM public.accounts WHERE id = 1",
        "TRUNCATE public.accounts",
        "CREATE TABLE public.blocked_ddl (id integer)",
        "ALTER TABLE public.accounts ADD COLUMN blocked integer",
        "GRANT SELECT ON public.accounts TO PUBLIC",
    ] {
        assert!(client.batch_execute(statement).is_err(), "{statement}");
    }
    let row_count: i64 = client
        .query_one("SELECT count(*) FROM public.accounts", &[])?
        .get(0);
    assert_eq!(row_count, 3);

    drop(client);
    let interrupted = execute_postgres_fenced_plan_with_interruption(
        &plan,
        &source,
        &target,
        &admin,
        &artifact,
        "docker-write-fence-approval",
        &state_path,
        1,
    );
    assert!(interrupted
        .unwrap_err()
        .to_string()
        .contains("injected interruption"));
    let interrupted_state: MigrationState = read_json(&state_path)?;
    assert_eq!(interrupted_state.status, MigrationStatus::Running);

    let report = resume_postgres_fenced_plan(&state_path, &source, &target, &admin, &artifact)
        .context("resume the reviewed plan under the durable write fence")?;
    assert_eq!(report.copied_rows, 3);
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);

    let mut target_client = connect(&PostgresEndpointConfig::read(target)?)?;
    let rows = target_client.query("SELECT id, name FROM public.accounts ORDER BY id", &[])?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[2].get::<_, String>(1), "three");

    let mut client = connect(&admin_config)?;
    client.batch_execute("INSERT INTO public.accounts VALUES (10, 'released')")?;
    Ok(())
}

fn ddl_catalog() -> anyhow::Result<VendorCatalog> {
    let table_id = "table-accounts";
    let table = CatalogObject {
        id: table_id.into(),
        kind: CatalogObjectKind::Table,
        name: Identifier::new("accounts")?,
        definition: Vec::new(),
        attributes: BTreeMap::from([
            ("relkind".into(), serde_json::Value::String("r".into())),
            ("persistence".into(), serde_json::Value::String("p".into())),
        ]),
    };
    let column = |id: &str, name: &str, ordinal: i64, ty: &str, identity: &str| {
        Ok::<_, anyhow::Error>(CatalogObject {
            id: id.into(),
            kind: CatalogObjectKind::Column,
            name: Identifier::new(name)?,
            definition: ty.as_bytes().to_vec(),
            attributes: BTreeMap::from([
                (
                    "table_oid".into(),
                    serde_json::Value::String(table_id.into()),
                ),
                ("ordinal".into(), serde_json::Value::Number(ordinal.into())),
                ("nullable".into(), serde_json::Value::Bool(false)),
                (
                    "identity".into(),
                    serde_json::Value::String(identity.into()),
                ),
                ("generated".into(), serde_json::Value::String(String::new())),
                (
                    "type_schema".into(),
                    serde_json::Value::String("pg_catalog".into()),
                ),
            ]),
        })
    };
    let primary_key = CatalogObject {
        id: "pk-accounts".into(),
        kind: CatalogObjectKind::PrimaryKey,
        name: Identifier::new("accounts_pkey")?,
        definition: b"PRIMARY KEY (id)".to_vec(),
        attributes: BTreeMap::from([(
            "table_oid".into(),
            serde_json::Value::String(table_id.into()),
        )]),
    };
    Ok(VendorCatalog {
        format_version: 1,
        dialect: "postgresql".into(),
        server_version: "17".into(),
        database: Identifier::new("source")?,
        namespaces: vec![CatalogNamespace {
            id: "namespace-public".into(),
            name: Identifier::new("public")?,
            owner: None,
            charset: Some("UTF8".into()),
            collation: None,
            objects: vec![
                table,
                column("column-id", "id", 1, "bigint", "a")?,
                column("column-name", "name", 2, "text", "")?,
                primary_key,
            ],
        }],
        dependencies: Vec::new(),
        vendor_metadata: BTreeMap::new(),
    })
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
