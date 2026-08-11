#![cfg(feature = "enterprise-migration-spike")]

use std::collections::BTreeMap;
#[cfg(feature = "migration-fault-injection")]
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(feature = "migration-fault-injection")]
#[path = "support/postgres_commit_proxy.rs"]
mod postgres_commit_proxy;
#[cfg(feature = "migration-fault-injection")]
use postgres_commit_proxy::{CommitFaultMode, PostgresCommitProxy};

use anyhow::Context;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use sql_splitter::migration::artifact::read_json;
use sql_splitter::migration::connection::{
    CancellationToken, ConnectionError, KeysetPage, ReadSession, SourceConnectionFactory,
    TargetConnectionFactory,
};
use sql_splitter::migration::journal::{MigrationState, MigrationStatus};
use sql_splitter::migration::model::{
    CatalogNamespace, CatalogObject, CatalogObjectKind, DbValue, Identifier, KeyTuple,
    QualifiedTable, VendorCatalog,
};
use sql_splitter::migration::plan::ReviewedPlan;
use sql_splitter::migration::postgres::{
    build_plan, inspect_endpoint, write_live_plan, write_live_plan_with_consistency,
    PostgresConsistencyMode, PostgresEndpointConfig, PostgresSourceFactory,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::postgres::{postgres_foreign_keys, PostgresTargetFactory};
use sql_splitter::migration::postgres_fence::{
    attest_postgres_write_fence, install_postgres_write_fence, postgres_write_fence_is_released,
    release_postgres_write_fence, InstalledPostgresFence,
};
use sql_splitter::migration::runner::execute_postgres_plan;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::runner::{
    execute_postgres_fenced_plan_with_cancellation, execute_postgres_fenced_plan_with_interruption,
    execute_postgres_interrupted, resume_postgres_fenced_plan, PostgresCancellationExecution,
    PostgresExecutionInterruption, PostgresInterruptedExecution,
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
#[ignore = "requires PostgreSQL configured to require a trusted client certificate"]
fn live_mutual_tls_requires_valid_client_identity() -> anyhow::Result<()> {
    let config_path = required_path("SQL_SPLITTER_PG_MTLS_CONFIG")?;
    let config = PostgresEndpointConfig::read(config_path)?;
    let snapshot = inspect_endpoint(&config)?;
    assert!(snapshot.tls_binding.starts_with("hostname_verified+mtls;"));

    let mut missing_identity = config.clone();
    missing_identity.tls.client_certificate = None;
    missing_identity.tls.client_private_key = None;
    assert!(inspect_endpoint(&missing_identity).is_err());

    let mut wrong_ca = config;
    wrong_ca.tls.ca_certificate = Some(
        required_path("SQL_SPLITTER_PG_ROGUE_CA")?
            .to_string_lossy()
            .into_owned(),
    );
    assert!(inspect_endpoint(&wrong_ca).is_err());
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
        "UPDATE public.live_snapshot_rows SET payload = 'changed' WHERE id = 2;
         UPDATE public.live_snapshot_rows SET id = 30 WHERE id = 3;
         INSERT INTO public.live_snapshot_rows (id, payload) VALUES (4, 'four')",
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
#[ignore = "requires TLS-enabled PostgreSQL read-only and mutator roles"]
fn live_key_pagination_matrix_is_exact_and_bounded() -> anyhow::Result<()> {
    let mut source_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?)?;
    source_config.max_batch_rows = 2;
    let mut mutator = connect(&PostgresEndpointConfig::read(required_path(
        "SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG",
    )?)?)?;
    mutator.batch_execute(
        "DROP TABLE IF EXISTS public.migration_key_i16,
                                    public.migration_key_i32,
                                    public.migration_key_i64,
                                    public.migration_key_composite,
                                    public.migration_key_text,
                                    public.migration_key_bytes,
                                    public.migration_key_empty,
                                    public.migration_key_one,
                                    public.migration_key_nullable,
                                    public.migration_key_nonunique,
                                    public.migration_key_bytes_bounded;
         CREATE TABLE public.migration_key_i16 (id smallint PRIMARY KEY);
         INSERT INTO public.migration_key_i16 VALUES (-32768), (0), (32767);
         CREATE TABLE public.migration_key_i32 (id integer PRIMARY KEY);
         INSERT INTO public.migration_key_i32 VALUES (-2147483648), (0), (2147483647);
         CREATE TABLE public.migration_key_i64 (id bigint PRIMARY KEY);
         INSERT INTO public.migration_key_i64 VALUES (-9223372036854775808), (0), (9223372036854775807);
         CREATE TABLE public.migration_key_composite (a integer NOT NULL, b bigint NOT NULL, PRIMARY KEY (a,b));
         INSERT INTO public.migration_key_composite VALUES (-1,9), (0,-9223372036854775808), (0,0), (0,9223372036854775807), (1,-9);
         CREATE TABLE public.migration_key_text (id text COLLATE \"C\" PRIMARY KEY);
         INSERT INTO public.migration_key_text VALUES (''), ('A'), ('a'), ('é'), ('😀');
         CREATE TABLE public.migration_key_bytes (id bytea PRIMARY KEY);
         INSERT INTO public.migration_key_bytes VALUES ('\\x00'), ('\\x0000'), ('\\x00ff'), ('\\xff');
         CREATE TABLE public.migration_key_empty (id bigint PRIMARY KEY);
         CREATE TABLE public.migration_key_one (id bigint PRIMARY KEY);
         INSERT INTO public.migration_key_one VALUES (7);
         CREATE TABLE public.migration_key_nullable (id bigint UNIQUE);
         INSERT INTO public.migration_key_nullable VALUES (NULL), (1);
         CREATE TABLE public.migration_key_nonunique (id bigint NOT NULL);
         INSERT INTO public.migration_key_nonunique VALUES (1), (1);
         CREATE TABLE public.migration_key_bytes_bounded (id bigint PRIMARY KEY, payload text NOT NULL);
         INSERT INTO public.migration_key_bytes_bounded VALUES (1, repeat('a',30)), (2, repeat('b',30)), (3, repeat('c',30));
         GRANT SELECT ON public.migration_key_i16,
                         public.migration_key_i32,
                         public.migration_key_i64,
                         public.migration_key_composite,
                         public.migration_key_text,
                         public.migration_key_bytes,
                         public.migration_key_empty,
                         public.migration_key_one,
                         public.migration_key_nullable,
                         public.migration_key_nonunique,
                         public.migration_key_bytes_bounded TO migration_reader",
    )?;

    let factory = PostgresSourceFactory::new(source_config.clone());
    let snapshot = factory.capture_snapshot()?;
    let mut reader = factory.open_reader(&snapshot, CancellationToken::default())?;
    assert_eq!(
        page_all(&mut *reader, "migration_key_i16", &["id"], &["id"])?,
        vec![
            vec![DbValue::Signed(i16::MIN.into())],
            vec![DbValue::Signed(0)],
            vec![DbValue::Signed(i16::MAX.into())],
        ]
    );
    assert_eq!(
        page_all(&mut *reader, "migration_key_i32", &["id"], &["id"])?,
        vec![
            vec![DbValue::Signed(i32::MIN.into())],
            vec![DbValue::Signed(0)],
            vec![DbValue::Signed(i32::MAX.into())],
        ]
    );
    assert_eq!(
        page_all(&mut *reader, "migration_key_i64", &["id"], &["id"])?,
        vec![
            vec![DbValue::Signed(i64::MIN.into())],
            vec![DbValue::Signed(0)],
            vec![DbValue::Signed(i64::MAX.into())],
        ]
    );
    assert_eq!(
        page_all(
            &mut *reader,
            "migration_key_composite",
            &["a", "b"],
            &["a", "b"],
        )?,
        vec![
            vec![DbValue::Signed(-1), DbValue::Signed(9)],
            vec![DbValue::Signed(0), DbValue::Signed(i64::MIN.into())],
            vec![DbValue::Signed(0), DbValue::Signed(0)],
            vec![DbValue::Signed(0), DbValue::Signed(i64::MAX.into())],
            vec![DbValue::Signed(1), DbValue::Signed(-9)],
        ]
    );
    assert_eq!(
        page_all(&mut *reader, "migration_key_text", &["id"], &["id"])?,
        ["", "A", "a", "é", "😀"]
            .into_iter()
            .map(|value| vec![DbValue::Text(value.into())])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        page_all(&mut *reader, "migration_key_bytes", &["id"], &["id"])?,
        vec![
            vec![DbValue::Bytes(vec![0])],
            vec![DbValue::Bytes(vec![0, 0])],
            vec![DbValue::Bytes(vec![0, 255])],
            vec![DbValue::Bytes(vec![255])],
        ]
    );
    assert!(page_all(&mut *reader, "migration_key_empty", &["id"], &["id"])?.is_empty());
    assert_eq!(
        page_all(&mut *reader, "migration_key_one", &["id"], &["id"])?,
        vec![vec![DbValue::Signed(7)]]
    );
    for table in ["migration_key_nullable", "migration_key_nonunique"] {
        let error = reader
            .select_page(&key_page(table, &["id"], &["id"], None, 2)?)
            .unwrap_err();
        assert!(
            matches!(error, ConnectionError::InvalidRequest(_)),
            "{error:?}"
        );
    }
    drop(reader);

    source_config.max_batch_rows = 10;
    source_config.max_batch_bytes = 70;
    let plan_source_config = source_config.clone();
    let bounded_factory = PostgresSourceFactory::new(source_config);
    let bounded_snapshot = bounded_factory.capture_snapshot()?;
    let mut bounded_reader =
        bounded_factory.open_reader(&bounded_snapshot, CancellationToken::default())?;
    assert_eq!(
        page_all(
            &mut *bounded_reader,
            "migration_key_bytes_bounded",
            &["id", "payload"],
            &["id"],
        )?
        .len(),
        3
    );
    let target_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?)?;
    let reviewed = build_plan(
        &inspect_endpoint(&plan_source_config)?,
        &inspect_endpoint(&target_config)?,
    )?;
    let source_catalog = reviewed
        .plan
        .source_catalog
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no source catalog"))?;
    let text_key = source_catalog
        .namespaces
        .iter()
        .flat_map(|namespace| &namespace.objects)
        .find(|object| {
            object.kind == CatalogObjectKind::Column
                && object.name.as_str() == "id"
                && object
                    .attributes
                    .get("table")
                    .and_then(serde_json::Value::as_str)
                    == Some("migration_key_text")
        })
        .ok_or_else(|| anyhow::anyhow!("text key column is absent from catalog"))?;
    assert_eq!(
        text_key
            .attributes
            .get("collation")
            .and_then(serde_json::Value::as_str),
        Some("C")
    );
    assert_eq!(
        text_key
            .attributes
            .get("collation_schema")
            .and_then(serde_json::Value::as_str),
        Some("pg_catalog")
    );
    assert!(text_key.attributes.contains_key("collation_provider"));
    assert!(!source_catalog.server_version.is_empty());
    for table in ["migration_key_nullable", "migration_key_nonunique"] {
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.object_kind == "resumable_key" && object.object_id.ends_with(table)
            }));
    }
    assert!(reviewed.plan.validate_for_execution().is_err());
    Ok(())
}

fn page_all(
    reader: &mut dyn ReadSession,
    table: &str,
    projection: &[&str],
    key: &[&str],
) -> anyhow::Result<Vec<Vec<DbValue>>> {
    let mut after = None;
    let mut rows = Vec::new();
    loop {
        let page = reader.select_page(&key_page(table, projection, key, after.clone(), 2)?)?;
        if page.is_empty() {
            return Ok(rows);
        }
        let key_indexes = key
            .iter()
            .map(|key_name| {
                projection
                    .iter()
                    .position(|column| column == key_name)
                    .ok_or_else(|| anyhow::anyhow!("key is absent from projection"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        after = Some(KeyTuple::new(
            key_indexes
                .iter()
                .map(|index| page.rows().last().expect("nonempty page")[*index].clone())
                .collect(),
        ));
        rows.extend_from_slice(page.rows());
    }
}

fn key_page(
    table: &str,
    projection: &[&str],
    key: &[&str],
    after: Option<KeyTuple>,
    limit: u32,
) -> anyhow::Result<KeysetPage> {
    Ok(KeysetPage {
        table: QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new(table)?,
        },
        projection: projection
            .iter()
            .map(|name| Identifier::new(*name))
            .collect::<Result<_, _>>()?,
        key: key
            .iter()
            .map(|name| Identifier::new(*name))
            .collect::<Result<_, _>>()?,
        after,
        limit,
    })
}

#[test]
#[ignore = "requires a TLS-enabled PostgreSQL slow_rows test table"]
fn live_control_session_cancels_the_active_query() -> anyhow::Result<()> {
    let mut source_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?)?;
    source_config.max_batch_rows = 1_000_000;
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
        limit: 1_000_000,
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
    assert!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "{:#?}",
        reviewed.plan.unsupported_objects
    );

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
#[cfg(feature = "migration-fault-injection")]
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

#[test]
#[ignore = "requires a released durable PostgreSQL fence and its reviewed plan"]
fn live_write_fence_rearms_without_erasing_prior_history() -> anyhow::Result<()> {
    let admin_path = required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?;
    let plan_path = required_path("SQL_SPLITTER_PG_FENCE_PLAN")?;
    let old_artifact_path = required_path("SQL_SPLITTER_PG_FENCE_ARTIFACT")?;
    let admin = PostgresEndpointConfig::read(&admin_path)?;
    let reviewed: ReviewedPlan = read_json(plan_path)?;
    let old: InstalledPostgresFence = read_json(old_artifact_path)?;
    assert!(postgres_write_fence_is_released(&admin, &old.evidence)?);

    let directory = tempfile::tempdir()?;
    let new_artifact_path = directory.path().join("rearmed-fence.json");
    let new = install_postgres_write_fence(&admin, &reviewed, &new_artifact_path)?;
    assert_ne!(old.evidence, new.evidence);
    assert!(attest_postgres_write_fence(&admin, &old.evidence).is_err());
    assert!(release_postgres_write_fence(
        &admin,
        match &old.evidence {
            sql_splitter::migration::journal::ConsistencyEvidence::WriteFence {
                generation,
                ..
            } => generation,
            _ => anyhow::bail!("old artifact is not write-fence evidence"),
        },
        &old.token,
    )
    .is_err());
    attest_postgres_write_fence(&admin, &new.evidence)?;

    let new_generation = match &new.evidence {
        sql_splitter::migration::journal::ConsistencyEvidence::WriteFence {
            generation, ..
        } => generation,
        _ => anyhow::bail!("new artifact is not write-fence evidence"),
    };
    let mut client = connect(&admin)?;
    let histories: i64 = client
        .query_one(
            "SELECT count(DISTINCT generation) FROM sql_splitter_migration_fence.history",
            &[],
        )?
        .get(0);
    assert!(histories >= 2);
    release_postgres_write_fence(&admin, new_generation, &new.token)?;
    assert!(postgres_write_fence_is_released(&admin, &new.evidence)?);
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_write_fence_recovery_boundary_matrix() -> anyhow::Result<()> {
    let base_source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let base_target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let base_admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let directory = tempfile::tempdir()?;
    let interruptions = [
        PostgresExecutionInterruption::AfterDdlPrepared,
        PostgresExecutionInterruption::AfterDdlCommitted,
        PostgresExecutionInterruption::AfterChunkPrepared,
        PostgresExecutionInterruption::CommitUnknownAfterApply,
        PostgresExecutionInterruption::AfterAllVerified,
        PostgresExecutionInterruption::AfterFenceReleased,
    ];

    let mut control = connect(&base_admin)?;
    for (index, interruption) in interruptions.into_iter().enumerate() {
        let source_database = format!("migration_recovery_source_{index}");
        let target_database = format!("migration_recovery_target_{index}");
        control.batch_execute(&format!(
            "CREATE DATABASE {source_database} OWNER migration_mutator"
        ))?;
        control.batch_execute(&format!(
            "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
        ))?;
        let cleanup = RecoveryDatabaseCleanup::new(
            base_admin.clone(),
            source_database.clone(),
            target_database.clone(),
        );

        let mut source = base_source.clone();
        source.database.clone_from(&source_database);
        let mut target = base_target.clone();
        target.database.clone_from(&target_database);
        let mut admin = base_admin.clone();
        admin.database.clone_from(&source_database);
        let mut setup = connect(&admin)?;
        setup.batch_execute(&format!(
            "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT CONNECT ON DATABASE {source_database} TO migration_reader; GRANT USAGE ON SCHEMA public TO migration_reader; CREATE TABLE public.accounts (id bigint PRIMARY KEY, name text NOT NULL); INSERT INTO public.accounts VALUES (1, 'one'), (2, 'two'), (3, 'three'); GRANT SELECT ON public.accounts TO migration_reader"
        ))?;
        drop(setup);

        let case = directory.path().join(index.to_string());
        std::fs::create_dir(&case)?;
        let source_path = case.join("source.toml");
        let target_path = case.join("target.toml");
        let admin_path = case.join("admin.toml");
        std::fs::write(&source_path, toml::to_string(&source)?)?;
        std::fs::write(&target_path, toml::to_string(&target)?)?;
        std::fs::write(&admin_path, toml::to_string(&admin)?)?;
        let plan_path = case.join("plan.json");
        let fence_path = case.join("fence.json");
        let state_path = case.join("state.json");
        let reviewed = write_live_plan_with_consistency(
            &source_path,
            &target_path,
            &plan_path,
            PostgresConsistencyMode::WriteFence,
        )?;
        install_postgres_write_fence(&admin, &reviewed, &fence_path)?;

        let error = execute_postgres_interrupted(PostgresInterruptedExecution {
            plan_path: &plan_path,
            source_config_path: &source_path,
            target_config_path: &target_path,
            fence_admin_config_path: &admin_path,
            fence_artifact_path: &fence_path,
            approval_reference: "docker-recovery-matrix",
            state_path: &state_path,
            interruption,
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected interruption"));

        let report = resume_postgres_fenced_plan(
            &state_path,
            &source_path,
            &target_path,
            &admin_path,
            &fence_path,
        )?;
        assert_eq!(report.copied_rows, 3, "{interruption:?}");
        let state: MigrationState = read_json(&state_path)?;
        assert_eq!(state.status, MigrationStatus::Completed, "{interruption:?}");
        assert_eq!(
            state
                .chunks
                .iter()
                .map(|chunk| chunk.row_count)
                .sum::<u64>(),
            3,
            "{interruption:?}"
        );
        assert!(state.chunks.iter().all(|chunk| {
            chunk.state == sql_splitter::migration::journal::ChunkState::Committed
        }));
        assert!(state.operations.iter().all(|operation| {
            operation.state == sql_splitter::migration::journal::OperationState::Verified
        }));
        let mut target_client = connect(&target)?;
        let rows = target_client.query("SELECT id, name FROM public.accounts ORDER BY id", &[])?;
        let actual = rows
            .iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (1, "one".to_owned()),
                (2, "two".to_owned()),
                (3, "three".to_owned()),
            ],
            "{interruption:?}"
        );
        let mut source_client = connect(&admin)?;
        source_client.batch_execute("INSERT INTO public.accounts VALUES (10, 'released')")?;
        drop(source_client);
        drop(target_client);

        cleanup.run()?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_runner_cancellation_rolls_back_and_resumes_exactly() -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = "migration_cancellation_source".to_owned();
    let target_database = "migration_cancellation_target".to_owned();
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    source.max_batch_rows = 50_000;
    source.max_batch_bytes = 16 * 1024 * 1024;
    target.database.clone_from(&target_database);
    target.max_batch_rows = 50_001;
    target.max_batch_bytes = 16 * 1024 * 1024;
    admin.database.clone_from(&source_database);

    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.accounts (id bigint PRIMARY KEY, name text NOT NULL);
         INSERT INTO public.accounts SELECT id, 'row-' || id::text FROM generate_series(1,50000) AS id;
         GRANT SELECT ON public.accounts TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;

    let cancellation = CancellationToken::default();
    let runner_token = cancellation.clone();
    let mut observer = connect(&PostgresEndpointConfig::read(required_path(
        "SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG",
    )?)?)?;
    let cancelled = thread::scope(|scope| -> anyhow::Result<anyhow::Error> {
        let handle = scope.spawn(|| {
            execute_postgres_fenced_plan_with_cancellation(
                PostgresCancellationExecution {
                    plan_path: &plan_path,
                    source_config_path: &source_path,
                    target_config_path: &target_path,
                    fence_admin_config_path: &admin_path,
                    fence_artifact_path: &fence_path,
                    approval_reference: "docker-cancellation",
                    state_path: &state_path,
                },
                runner_token,
            )
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if state_path.exists() {
                let state: MigrationState = read_json(&state_path)?;
                let transaction_is_active: bool = observer
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND usename='migration_fence_target_owner' AND xact_start IS NOT NULL AND query LIKE 'INSERT INTO%')",
                        &[&target_database],
                    )?
                    .get(0);
                if state.prepared_chunk()?.is_some() && transaction_is_active {
                    break;
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("target INSERT transaction did not become observable before timeout");
            }
            thread::sleep(Duration::from_millis(5));
        }
        cancellation.cancel();
        let result = handle
            .join()
            .map_err(|_| anyhow::anyhow!("cancellation runner thread panicked"))?;
        Ok(result.expect_err("controlled cancellation must stop execution"))
    })?;
    assert!(format!("{cancelled:#}").contains("Cancelled"));

    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.chunks.len(), 1);
    assert_eq!(
        state.chunks[0].state,
        sql_splitter::migration::journal::ChunkState::Prepared
    );
    let mut target_client = connect(&target)?;
    let row_count: i64 = target_client
        .query_one("SELECT count(*) FROM public.accounts", &[])?
        .get(0);
    assert_eq!(row_count, 0, "cancelled target transaction must roll back");
    drop(target_client);

    let report = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    )?;
    assert_eq!(report.copied_rows, 50_000);
    let completed: MigrationState = read_json(&state_path)?;
    assert_eq!(completed.status, MigrationStatus::Completed);
    cleanup.run()?;
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_network_commit_response_loss_matrix() -> anyhow::Result<()> {
    for (suffix, mode) in [
        ("not_forwarded", CommitFaultMode::NotForwarded),
        ("ack_lost", CommitFaultMode::AppliedAckLost),
    ] {
        run_live_network_commit_response_loss_case(suffix, mode)?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_standalone_unique_index_is_created_before_copy_and_resumed() -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = "migration_unique_index_source".to_owned();
    let target_database = "migration_unique_index_target".to_owned();
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.accounts (tenant_id bigint NOT NULL, id bigint NOT NULL, name text NOT NULL);
         CREATE UNIQUE INDEX accounts_tenant_id_id_uidx ON public.accounts USING btree (tenant_id,id);
         INSERT INTO public.accounts VALUES (1,1,'one'), (1,2,'two'), (2,1,'three');
         GRANT SELECT ON public.accounts TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    assert!(!reviewed.plan.unsupported_objects.blocks_execution());
    let copy = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == sql_splitter::migration::plan::OperationKind::CopyTable)
        .ok_or_else(|| anyhow::anyhow!("copy operation is absent"))?;
    assert_eq!(
        copy.parameters["resumable_key"]["kind"].as_str(),
        Some("standalone_unique_index")
    );
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-unique-index",
        state_path: &state_path,
        interruption: PostgresExecutionInterruption::AfterDdlCommitted,
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("injected interruption"),
        "{error:#}"
    );
    resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    )?;
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state.operations.iter().all(|operation| {
        operation.state == sql_splitter::migration::journal::OperationState::Verified
    }));
    let mut target_client = connect(&target)?;
    let index = target_client.query_one(
        "SELECT i.indisunique, i.indisvalid, i.indisready,
                ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,pos) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.pos)
         FROM pg_index i
         JOIN pg_class c ON c.oid=i.indexrelid
         JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='public' AND c.relname='accounts_tenant_id_id_uidx'",
        &[],
    )?;
    assert!(index.get::<_, bool>(0));
    assert!(index.get::<_, bool>(1));
    assert!(index.get::<_, bool>(2));
    assert_eq!(
        index.get::<_, Vec<String>>(3),
        vec!["tenant_id".to_owned(), "id".to_owned()]
    );
    let rows = target_client.query(
        "SELECT tenant_id,id,name FROM public.accounts ORDER BY tenant_id,id",
        &[],
    )?;
    assert_eq!(rows.len(), 3);
    drop(target_client);
    cleanup.run()?;
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_ordinary_indexes_are_created_after_copy_and_reconciled() -> anyhow::Result<()> {
    for (suffix, interruption, exists_before_resume, inject_conflict, cancel_ddl) in [
        (
            "prepared",
            PostgresExecutionInterruption::AfterIndexPrepared,
            false,
            false,
            false,
        ),
        (
            "committed",
            PostgresExecutionInterruption::AfterIndexCommitted,
            true,
            false,
            false,
        ),
        (
            "conflict",
            PostgresExecutionInterruption::AfterIndexPrepared,
            false,
            true,
            false,
        ),
        (
            "cancelled_ddl",
            PostgresExecutionInterruption::AfterIndexPrepared,
            false,
            false,
            true,
        ),
    ] {
        run_live_ordinary_index_recovery_case(
            suffix,
            interruption,
            exists_before_resume,
            inject_conflict,
            cancel_ddl,
        )
        .with_context(|| format!("ordinary-index recovery case {suffix}"))?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_sequences_are_fenced_restored_and_reconciled() -> anyhow::Result<()> {
    for (suffix, interruption, applied, conflict) in [
        (
            "prepared",
            PostgresExecutionInterruption::AfterSequencePrepared,
            false,
            false,
        ),
        (
            "committed",
            PostgresExecutionInterruption::AfterSequenceCommitted,
            true,
            false,
        ),
        (
            "conflict",
            PostgresExecutionInterruption::AfterSequencePrepared,
            false,
            true,
        ),
    ] {
        run_live_sequence_recovery_case(suffix, interruption, applied, conflict)
            .with_context(|| format!("sequence recovery case {suffix}"))?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_generated_columns_are_recomputed_and_reconciled() -> anyhow::Result<()> {
    for (suffix, interruption, applied, conflict) in [
        (
            "prepared",
            PostgresExecutionInterruption::AfterChunkPrepared,
            false,
            false,
        ),
        (
            "committed",
            PostgresExecutionInterruption::CommitUnknownAfterApply,
            true,
            false,
        ),
        (
            "conflict",
            PostgresExecutionInterruption::CommitUnknownAfterApply,
            true,
            true,
        ),
    ] {
        run_live_generated_column_case(suffix, interruption, applied, conflict)
            .with_context(|| format!("generated-column recovery case {suffix}"))?;
    }
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_partition_topologies_route_and_resume_exactly() -> anyhow::Result<()> {
    for (strategy, interruption) in [
        ("range", PostgresExecutionInterruption::AfterDdlPrepared),
        ("list", PostgresExecutionInterruption::AfterDdlCommitted),
        (
            "hash",
            PostgresExecutionInterruption::AfterCommittedChunks(1),
        ),
    ] {
        run_live_partition_case(strategy, interruption)
            .with_context(|| format!("partition topology case {strategy}"))?;
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_partition_case(
    strategy: &str,
    interruption: PostgresExecutionInterruption,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    source.max_batch_rows = 2;
    target.max_batch_rows = 2;
    let control_config = admin.clone();
    let source_database = format!("migration_partition_{strategy}_source");
    let target_database = format!("migration_partition_{strategy}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let partition_ddl = match strategy {
        "range" => "
            CREATE TABLE public.accounts (id bigint PRIMARY KEY, payload text NOT NULL) PARTITION BY RANGE (id);
            CREATE TABLE public.accounts_low PARTITION OF public.accounts FOR VALUES FROM (MINVALUE) TO (0);
            CREATE TABLE public.accounts_mid PARTITION OF public.accounts FOR VALUES FROM (0) TO (10);
            CREATE TABLE public.accounts_default PARTITION OF public.accounts DEFAULT;",
        "list" => "
            CREATE TABLE public.accounts (id bigint PRIMARY KEY, payload text NOT NULL) PARTITION BY LIST (id);
            CREATE TABLE public.accounts_low PARTITION OF public.accounts FOR VALUES IN (-2,0);
            CREATE TABLE public.accounts_mid PARTITION OF public.accounts FOR VALUES IN (9);
            CREATE TABLE public.accounts_default PARTITION OF public.accounts DEFAULT;",
        "hash" => "
            CREATE TABLE public.accounts (id bigint PRIMARY KEY, payload text NOT NULL) PARTITION BY HASH (id);
            CREATE TABLE public.accounts_h0 PARTITION OF public.accounts FOR VALUES WITH (modulus 4, remainder 0);
            CREATE TABLE public.accounts_h1 PARTITION OF public.accounts FOR VALUES WITH (modulus 4, remainder 1);
            CREATE TABLE public.accounts_h2 PARTITION OF public.accounts FOR VALUES WITH (modulus 4, remainder 2);
            CREATE TABLE public.accounts_h3 PARTITION OF public.accounts FOR VALUES WITH (modulus 4, remainder 3);",
        _ => unreachable!("fixed strategy matrix"),
    };
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         SET ROLE migration_mutator;
         {partition_ddl}
         INSERT INTO public.accounts VALUES (-2,'negative'),(0,'zero'),(9,'nine'),(10,'ten'),(17,'seventeen');
         RESET ROLE;
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_reader"
    ))?;
    drop(setup);
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    if strategy == "range" {
        let mut source_admin = connect(&admin)?;
        source_admin.batch_execute(
            "SET ROLE migration_mutator;
             ALTER INDEX public.accounts_low_pkey RENAME TO accounts_low_renamed_pkey;
             RESET ROLE",
        )?;
        drop(source_admin);
        let renamed_path = directory.path().join("renamed-index-plan.json");
        let blocked = write_live_plan_with_consistency(
            &source_path,
            &target_path,
            &renamed_path,
            PostgresConsistencyMode::WriteFence,
        )?;
        assert!(blocked
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.required_semantics
                    && object.object_id.starts_with("partition-child-index-name:")
            }));
        let mut source_admin = connect(&admin)?;
        source_admin.batch_execute(
            "SET ROLE migration_mutator;
             ALTER INDEX public.accounts_low_renamed_pkey RENAME TO accounts_low_pkey;
             CREATE INDEX accounts_low_payload_local_idx ON public.accounts_low(payload);
             RESET ROLE",
        )?;
        drop(source_admin);
        let blocked_path = directory.path().join("blocked-plan.json");
        let blocked = write_live_plan_with_consistency(
            &source_path,
            &target_path,
            &blocked_path,
            PostgresConsistencyMode::WriteFence,
        )?;
        assert!(blocked
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.required_semantics && object.object_id.starts_with("partition-local-index:")
            }));
        let mut source_admin = connect(&admin)?;
        source_admin.batch_execute(
            "SET ROLE migration_mutator;
             DROP INDEX public.accounts_low_payload_local_idx;
             RESET ROLE",
        )?;
    }
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    assert!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "{:#?}",
        reviewed.plan.unsupported_objects
    );
    assert_eq!(
        reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| operation.kind
                == sql_splitter::migration::plan::OperationKind::CopyTable)
            .count(),
        1
    );
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let mut mutator = source.clone();
    mutator.user = "migration_mutator".into();
    mutator.credential_env = "SQL_SPLITTER_PG_MUTATOR_PASSWORD".into();
    let mut blocked = connect(&mutator)?;
    assert!(blocked
        .batch_execute("INSERT INTO public.accounts VALUES (99,'blocked-root')")
        .is_err());
    let leaf = if strategy == "hash" {
        "accounts_h0"
    } else {
        "accounts_low"
    };
    assert!(blocked
        .batch_execute(&format!("TRUNCATE TABLE public.{leaf}"))
        .is_err());
    drop(blocked);
    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-partition",
        state_path: &state_path,
        interruption,
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("injected interruption"),
        "{error:#}"
    );
    resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    )?;
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state
        .operations
        .iter()
        .all(|operation| operation.state
            == sql_splitter::migration::journal::OperationState::Verified));
    let mut source_client = connect(&source)?;
    let mut target_client = connect(&target)?;
    let query = "SELECT tableoid::regclass::text,id,payload FROM public.accounts ORDER BY id";
    let physical = |client: &mut Client| -> anyhow::Result<Vec<(String, i64, String)>> {
        Ok(client
            .query(query, &[])?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    };
    assert_eq!(physical(&mut target_client)?, physical(&mut source_client)?);
    cleanup.run()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_generated_column_case(
    suffix: &str,
    interruption: PostgresExecutionInterruption,
    applied: bool,
    inject_conflict: bool,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = format!("migration_generated_{suffix}_source");
    let target_database = format!("migration_generated_{suffix}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         SET ROLE migration_mutator;
         CREATE TABLE public.accounts (
           id bigint PRIMARY KEY,
           doubled bigint GENERATED ALWAYS AS (amount * 2) STORED,
           amount bigint
         );
         INSERT INTO public.accounts (id,amount) VALUES (1,21),(2,-5),(3,NULLIF(0,0));
         RESET ROLE;
         GRANT SELECT ON public.accounts TO migration_reader"
    ))?;
    drop(setup);
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    assert!(!reviewed.plan.unsupported_objects.blocks_execution());
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-generated-column",
        state_path: &state_path,
        interruption,
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("injected interruption"),
        "{error:#}"
    );
    let mut target_client = connect(&target)?;
    let rows = target_client.query(
        "SELECT id,doubled,amount FROM public.accounts ORDER BY id",
        &[],
    )?;
    assert_eq!(rows.len(), if applied { 3 } else { 0 });
    if inject_conflict {
        target_client.batch_execute("UPDATE public.accounts SET amount=22 WHERE id=1")?;
    }
    drop(target_client);
    let resumed = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    );
    if inject_conflict {
        let error = resumed.unwrap_err();
        assert!(error.to_string().contains("durable intent"), "{error:#}");
        let state: MigrationState = read_json(&state_path)?;
        assert_eq!(state.status, MigrationStatus::ManualReconciliationRequired);
        cleanup.run()?;
        return Ok(());
    }
    resumed?;
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    let mut target_client = connect(&target)?;
    let rows = target_client.query(
        "SELECT id,doubled,amount FROM public.accounts ORDER BY id",
        &[],
    )?;
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, Option<i64>>(1),
                row.get::<_, Option<i64>>(2)
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, Some(42), Some(21)),
            (2, Some(-10), Some(-5)),
            (3, None, None)
        ]
    );
    cleanup.run()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_sequence_recovery_case(
    suffix: &str,
    interruption: PostgresExecutionInterruption,
    applied: bool,
    inject_conflict: bool,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = format!("migration_sequence_{suffix}_source");
    let target_database = format!("migration_sequence_{suffix}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);

    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         SET ROLE migration_mutator;
         CREATE SEQUENCE public.descending_seq AS bigint INCREMENT BY -3 MINVALUE -20 MAXVALUE 10 START WITH 10 CACHE 4 CYCLE;
         SELECT pg_catalog.setval('public.descending_seq'::regclass, 4, true);
         CREATE SEQUENCE public.never_called_seq AS integer START WITH 7;
         CREATE TABLE public.accounts (
           id bigint GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME public.accounts_id_seq START WITH 100 INCREMENT BY 5 MINVALUE 100 MAXVALUE 1000 CACHE 3),
           serial_id bigserial NOT NULL,
           name text NOT NULL,
           PRIMARY KEY (id)
         );
         INSERT INTO public.accounts (id,serial_id,name) OVERRIDING SYSTEM VALUE VALUES (105,20,'one'),(110,21,'two');
         SELECT pg_catalog.setval('public.accounts_id_seq'::regclass, 115, true);
         SELECT pg_catalog.setval(pg_get_serial_sequence('public.accounts','serial_id')::regclass, 30, true);
         RESET ROLE;
         GRANT SELECT ON public.accounts TO migration_reader;
         GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    assert!(!reviewed.plan.unsupported_objects.blocks_execution());
    let restore_count = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| matches!(&operation.kind, sql_splitter::migration::plan::OperationKind::Vendor(name) if name == "restore_postgres_sequence"))
        .count();
    assert_eq!(restore_count, 4);
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;

    let mut mutator = source.clone();
    mutator.user = "migration_mutator".into();
    mutator.credential_env = "SQL_SPLITTER_PG_MUTATOR_PASSWORD".into();
    let denied = connect(&mutator)?.query_one("SELECT nextval('public.descending_seq')", &[]);
    assert!(denied.is_err(), "active fence allowed nextval");

    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-sequence",
        state_path: &state_path,
        interruption,
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("injected interruption"),
        "{error:?}"
    );

    let mut target_client = connect(&target)?;
    let state: (i64, bool) = target_client
        .query_one(
            "SELECT last_value,is_called FROM public.accounts_id_seq",
            &[],
        )
        .map(|row| (row.get(0), row.get(1)))?;
    assert_eq!(state, if applied { (115, true) } else { (100, false) });
    if inject_conflict {
        target_client.batch_execute(
            "SELECT pg_catalog.setval('public.accounts_id_seq'::regclass, 105, true)",
        )?;
    }
    drop(target_client);

    let resumed = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    );
    if inject_conflict {
        let error = resumed.unwrap_err();
        assert!(
            error.to_string().contains("manual intervention"),
            "{error:#}"
        );
        let state: MigrationState = read_json(&state_path)?;
        assert_eq!(state.status, MigrationStatus::ManualReconciliationRequired);
        cleanup.run()?;
        return Ok(());
    }
    resumed?;
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state
        .operations
        .iter()
        .all(|operation| operation.state
            == sql_splitter::migration::journal::OperationState::Verified));
    let mut target_client = connect(&target)?;
    let rows = target_client.query(
        "SELECT id,serial_id,name FROM public.accounts ORDER BY id",
        &[],
    )?;
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, String>(2)
            ))
            .collect::<Vec<_>>(),
        vec![(105, 20, "one".into()), (110, 21, "two".into())]
    );
    assert_eq!(
        target_client
            .query_one("SELECT nextval('public.descending_seq')", &[])?
            .get::<_, i64>(0),
        1
    );
    assert_eq!(
        target_client
            .query_one("SELECT nextval('public.never_called_seq')", &[])?
            .get::<_, i64>(0),
        7
    );
    assert_eq!(
        target_client
            .query_one("SELECT nextval('public.accounts_id_seq')", &[])?
            .get::<_, i64>(0),
        120
    );
    drop(target_client);
    assert_eq!(
        connect(&mutator)?
            .query_one("SELECT nextval('public.descending_seq')", &[])?
            .get::<_, i64>(0),
        1
    );
    cleanup.run()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_ordinary_index_recovery_case(
    suffix: &str,
    interruption: PostgresExecutionInterruption,
    exists_before_resume: bool,
    inject_conflict: bool,
    cancel_ddl: bool,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let observer_config = control_config.clone();
    let source_database = format!("migration_ordinary_index_{suffix}_source");
    let target_database = format!("migration_ordinary_index_{suffix}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.accounts (id bigint PRIMARY KEY, tenant_id bigint NOT NULL, name text NOT NULL);
         CREATE INDEX accounts_tenant_name_idx ON public.accounts USING btree (tenant_id,name);
         INSERT INTO public.accounts VALUES (1,2,'two'), (2,1,'one'), (3,2,'three');
         GRANT SELECT ON public.accounts TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    assert!(!reviewed.plan.unsupported_objects.blocks_execution());
    let index_operation = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.parameters.contains_key("postgres_index"))
        .ok_or_else(|| anyhow::anyhow!("ordinary index operation is absent"))?;
    assert_eq!(index_operation.dependencies.len(), 1);
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-ordinary-index",
        state_path: &state_path,
        interruption,
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("injected interruption"),
        "{error:#}"
    );
    let mut target_client = connect(&target)?;
    let exists: bool = target_client
        .query_one(
            "SELECT to_regclass('public.accounts_tenant_name_idx') IS NOT NULL",
            &[],
        )?
        .get(0);
    assert_eq!(exists, exists_before_resume);
    if inject_conflict {
        target_client.batch_execute("CREATE SEQUENCE public.accounts_tenant_name_idx")?;
    }
    if cancel_ddl {
        target_client
            .batch_execute("BEGIN; LOCK TABLE public.accounts IN SHARE ROW EXCLUSIVE MODE")?;
        let cancellation = CancellationToken::default();
        let runner_token = cancellation.clone();
        let mut observer = connect(&observer_config)?;
        let cancelled = thread::scope(|scope| -> anyhow::Result<anyhow::Error> {
            let handle = scope.spawn(|| {
                sql_splitter::migration::runner::resume_postgres_fenced_plan_with_cancellation(
                    sql_splitter::migration::runner::PostgresCancellationResume {
                        state_path: &state_path,
                        source_config_path: &source_path,
                        target_config_path: &target_path,
                        fence_admin_config_path: &admin_path,
                        fence_artifact_path: &fence_path,
                    },
                    runner_token,
                )
            });
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let create_index_is_waiting: bool = observer
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND usename='migration_fence_target_owner' AND state='active' AND query LIKE 'CREATE INDEX%')",
                        &[&target_database],
                    )?
                    .get(0);
                if create_index_is_waiting {
                    break;
                }
                if Instant::now() >= deadline {
                    anyhow::bail!("CREATE INDEX did not block before cancellation timeout");
                }
                thread::sleep(Duration::from_millis(5));
            }
            cancellation.cancel();
            let result = handle
                .join()
                .map_err(|_| anyhow::anyhow!("DDL cancellation runner thread panicked"))?;
            Ok(result.expect_err("DDL cancellation must stop resume"))
        })?;
        assert!(format!("{cancelled:#}").contains("Cancelled"));
        target_client.batch_execute("ROLLBACK")?;
        let state: MigrationState = read_json(&state_path)?;
        let index_operation = state
            .operations
            .iter()
            .find(|operation| operation.operation_id == index_operation.id.as_str())
            .ok_or_else(|| anyhow::anyhow!("ordinary index state is absent"))?;
        assert_eq!(
            index_operation.state,
            sql_splitter::migration::journal::OperationState::Prepared
        );
        let exists: bool = target_client
            .query_one(
                "SELECT to_regclass('public.accounts_tenant_name_idx') IS NOT NULL",
                &[],
            )?
            .get(0);
        assert!(!exists, "cancelled CREATE INDEX must roll back");
    }
    drop(target_client);

    let resume = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    );
    if inject_conflict {
        let error = resume.unwrap_err();
        assert!(
            error.to_string().contains("manual intervention"),
            "{error:#}"
        );
        let state: MigrationState = read_json(&state_path)?;
        assert_eq!(state.status, MigrationStatus::ManualReconciliationRequired);
        cleanup.run()?;
        return Ok(());
    }
    resume?;
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state.operations.iter().all(|operation| {
        operation.state == sql_splitter::migration::journal::OperationState::Verified
    }));
    let mut target_client = connect(&target)?;
    let index = target_client.query_one(
        "SELECT i.indisunique, i.indisvalid, i.indisready,
                ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,pos) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.pos)
         FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='public' AND c.relname='accounts_tenant_name_idx'",
        &[],
    )?;
    assert!(!index.get::<_, bool>(0));
    assert!(index.get::<_, bool>(1));
    assert!(index.get::<_, bool>(2));
    assert_eq!(
        index.get::<_, Vec<String>>(3),
        vec!["tenant_id".to_owned(), "name".to_owned()]
    );
    let rows = target_client.query(
        "SELECT id,tenant_id,name FROM public.accounts ORDER BY id",
        &[],
    )?;
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.get::<_, i64>(0),
                row.get::<_, i64>(1),
                row.get::<_, String>(2),
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, 2, "two".to_owned()),
            (2, 1, "one".to_owned()),
            (3, 2, "three".to_owned()),
        ]
    );
    drop(target_client);
    cleanup.run()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_network_commit_response_loss_case(
    suffix: &str,
    mode: CommitFaultMode,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let direct_target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = format!("migration_network_{suffix}_source");
    let target_database = format!("migration_network_{suffix}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    admin.database.clone_from(&source_database);
    let mut target = direct_target.clone();
    target.database.clone_from(&target_database);
    let upstream = (target.host.as_str(), target.port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("target endpoint did not resolve"))?;
    let proxy = PostgresCommitProxy::start(upstream, mode)?;
    let mut proxied_target = target.clone();
    proxied_target.port = proxy.data_port();

    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.accounts (id bigint PRIMARY KEY, name text NOT NULL);
         INSERT INTO public.accounts VALUES (1, 'one'), (2, 'two'), (3, 'three');
         GRANT SELECT ON public.accounts TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&proxied_target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;

    let execution = PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-network-commit-loss",
        state_path: &state_path,
        interruption: PostgresExecutionInterruption::NetworkCommitFault(proxy.control_port()),
    };

    match mode {
        CommitFaultMode::NotForwarded => {
            let _error = execute_postgres_interrupted(execution).unwrap_err();
            proxy.wait_for("discarded commit bytes", |telemetry| {
                telemetry.dropped_client_bytes_after_arm > 0
            })?;
            let telemetry = proxy.telemetry()?;
            assert_eq!(telemetry.forwarded_client_bytes_after_arm, 0);
            assert_eq!(exact_account_rows(&target)?.len(), 0);
            assert_one_prepared_chunk(&state_path)?;
            resume_postgres_fenced_plan(
                &state_path,
                &source_path,
                &target_path,
                &admin_path,
                &fence_path,
            )?;
        }
        CommitFaultMode::AppliedAckLost => {
            let execution_thread = thread::scope(|scope| -> anyhow::Result<_> {
                let handle = scope.spawn(|| execute_postgres_interrupted(execution));
                wait_for_exact_account_rows(&target)?;
                proxy.wait_for("withheld commit response", |telemetry| {
                    telemetry.forwarded_client_bytes_after_arm > 0
                        && telemetry.dropped_server_bytes_after_arm > 0
                })?;
                proxy.cut()?;
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("execution thread panicked"))
            })?;
            execution_thread?;
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
    assert_completed_exactly_once(&state_path, &target)?;
    cleanup.run()?;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn exact_account_rows(target: &PostgresEndpointConfig) -> anyhow::Result<Vec<(i64, String)>> {
    let mut client = connect(target)?;
    let rows = match client.query("SELECT id, name FROM public.accounts ORDER BY id", &[]) {
        Ok(rows) => rows,
        Err(error) if error.code() == Some(&postgres::error::SqlState::UNDEFINED_TABLE) => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error.into()),
    };
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect())
}

#[cfg(feature = "migration-fault-injection")]
fn wait_for_exact_account_rows(target: &PostgresEndpointConfig) -> anyhow::Result<()> {
    let expected = vec![
        (1, "one".to_owned()),
        (2, "two".to_owned()),
        (3, "three".to_owned()),
    ];
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if exact_account_rows(target)? == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for committed target rows");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(feature = "migration-fault-injection")]
fn assert_one_prepared_chunk(state_path: &std::path::Path) -> anyhow::Result<()> {
    let state: MigrationState = read_json(state_path)?;
    assert_eq!(state.chunks.len(), 1);
    assert_eq!(
        state.chunks[0].state,
        sql_splitter::migration::journal::ChunkState::Prepared
    );
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn assert_completed_exactly_once(
    state_path: &std::path::Path,
    target: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let state: MigrationState = read_json(state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert_eq!(state.chunks.len(), 1);
    assert_eq!(
        state.chunks[0].state,
        sql_splitter::migration::journal::ChunkState::Committed
    );
    assert!(state.operations.iter().all(|operation| {
        operation.state == sql_splitter::migration::journal::OperationState::Verified
    }));
    assert_eq!(
        exact_account_rows(target)?,
        vec![
            (1, "one".to_owned()),
            (2, "two".to_owned()),
            (3, "three".to_owned()),
        ]
    );
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_foreign_keys_are_checked_added_and_database_validated() -> anyhow::Result<()> {
    for (suffix, interruption) in [
        (
            "prepared",
            PostgresExecutionInterruption::AfterForeignKeyPrepared,
        ),
        (
            "committed",
            PostgresExecutionInterruption::AfterForeignKeyCommitted,
        ),
    ] {
        run_live_foreign_key_recovery_case(suffix, interruption)?;
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn run_live_foreign_key_recovery_case(
    suffix: &str,
    interruption: PostgresExecutionInterruption,
) -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = format!("migration_fk_{suffix}_source");
    let target_database = format!("migration_fk_{suffix}_target");
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.parents (a bigint NOT NULL, b bigint NOT NULL, PRIMARY KEY (a,b));
         CREATE TABLE public.children (
           id bigint PRIMARY KEY,
           pa bigint,
           pb bigint,
           parent_id bigint,
           CONSTRAINT children_parent_fk FOREIGN KEY (pa,pb) REFERENCES public.parents(a,b) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL,
           CONSTRAINT children_self_fk FOREIGN KEY (parent_id) REFERENCES public.children(id) DEFERRABLE INITIALLY DEFERRED
         );
         CREATE TABLE public.full_children (
           id bigint PRIMARY KEY,
           pa bigint,
           pb bigint,
           CONSTRAINT full_parent_fk FOREIGN KEY (pa,pb) REFERENCES public.parents(a,b) MATCH FULL
         );
         CREATE TABLE public.cycle_a (id bigint PRIMARY KEY, b_id bigint);
         CREATE TABLE public.cycle_b (id bigint PRIMARY KEY, a_id bigint);
         ALTER TABLE public.cycle_a ADD CONSTRAINT cycle_a_b_fk FOREIGN KEY (b_id) REFERENCES public.cycle_b(id) DEFERRABLE INITIALLY DEFERRED;
         ALTER TABLE public.cycle_b ADD CONSTRAINT cycle_b_a_fk FOREIGN KEY (a_id) REFERENCES public.cycle_a(id) DEFERRABLE INITIALLY DEFERRED;
         INSERT INTO public.parents VALUES (1,10), (2,20);
         INSERT INTO public.children VALUES (1,1,10,NULL), (2,2,20,1), (3,NULL,99,2);
         INSERT INTO public.full_children VALUES (1,1,10), (2,NULL,NULL);
         BEGIN;
         INSERT INTO public.cycle_a VALUES (1,1);
         INSERT INTO public.cycle_b VALUES (1,1);
         COMMIT;
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let interrupted = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-foreign-key-approval",
        state_path: &state_path,
        interruption,
    });
    assert!(interrupted
        .unwrap_err()
        .to_string()
        .contains("injected interruption"));
    let report = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    )?;
    assert_eq!(report.copied_rows, 9);
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::Completed);
    assert!(state.operations.iter().all(|operation| {
        operation.state == sql_splitter::migration::journal::OperationState::Verified
    }));
    let mut target_client = connect(&target)?;
    let constraint_counts = target_client.query_one(
        "SELECT count(*), count(*) FILTER (WHERE convalidated) FROM pg_constraint WHERE contype='f'",
        &[],
    )?;
    assert_eq!(constraint_counts.get::<_, i64>(0), 5);
    assert_eq!(constraint_counts.get::<_, i64>(1), 5);
    let constraints = target_client.query(
        "SELECT con.conname, array_agg(ca.attname ORDER BY ck.ordinality)::text[], array_agg(ra.attname ORDER BY rk.ordinality)::text[], con.confmatchtype::text, con.confupdtype::text, con.confdeltype::text, con.condeferrable, con.condeferred, (to_jsonb(con)->'confdelsetcols')::text, (SELECT count(*) FROM pg_trigger t WHERE t.tgconstraint=con.oid), (SELECT bool_and(t.tgisinternal AND t.tgenabled='O') FROM pg_trigger t WHERE t.tgconstraint=con.oid) FROM pg_constraint con JOIN pg_class child ON child.oid=con.conrelid JOIN pg_namespace n ON n.oid=child.relnamespace JOIN unnest(con.conkey) WITH ORDINALITY ck(attnum, ordinality) ON true JOIN pg_attribute ca ON ca.attrelid=child.oid AND ca.attnum=ck.attnum JOIN unnest(con.confkey) WITH ORDINALITY rk(attnum, ordinality) ON rk.ordinality=ck.ordinality JOIN pg_attribute ra ON ra.attrelid=con.confrelid AND ra.attnum=rk.attnum WHERE con.contype='f' AND n.nspname='public' GROUP BY con.oid, con.conname ORDER BY con.conname",
        &[],
    )?;
    let actual_constraints = constraints
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, Vec<String>>(1),
                row.get::<_, Vec<String>>(2),
                row.get::<_, String>(3),
                row.get::<_, String>(4),
                row.get::<_, String>(5),
                row.get::<_, bool>(6),
                row.get::<_, bool>(7),
                row.get::<_, Option<String>>(8),
                row.get::<_, i64>(9),
                row.get::<_, bool>(10),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual_constraints,
        vec![
            (
                "children_parent_fk".into(),
                vec!["pa".into(), "pb".into()],
                vec!["a".into(), "b".into()],
                "s".into(),
                "c".into(),
                "n".into(),
                false,
                false,
                Some("null".into()),
                4,
                true
            ),
            (
                "children_self_fk".into(),
                vec!["parent_id".into()],
                vec!["id".into()],
                "s".into(),
                "a".into(),
                "a".into(),
                true,
                true,
                Some("null".into()),
                4,
                true
            ),
            (
                "cycle_a_b_fk".into(),
                vec!["b_id".into()],
                vec!["id".into()],
                "s".into(),
                "a".into(),
                "a".into(),
                true,
                true,
                Some("null".into()),
                4,
                true
            ),
            (
                "cycle_b_a_fk".into(),
                vec!["a_id".into()],
                vec!["id".into()],
                "s".into(),
                "a".into(),
                "a".into(),
                true,
                true,
                Some("null".into()),
                4,
                true
            ),
            (
                "full_parent_fk".into(),
                vec!["pa".into(), "pb".into()],
                vec!["a".into(), "b".into()],
                "f".into(),
                "a".into(),
                "a".into(),
                false,
                false,
                Some("null".into()),
                4,
                true
            ),
        ]
    );
    let violations: bool = target_client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM public.children c WHERE c.pa IS NOT NULL AND c.pb IS NOT NULL AND NOT EXISTS (SELECT 1 FROM public.parents p WHERE (p.a,p.b)=(c.pa,c.pb)))",
            &[],
        )?
        .get(0);
    assert!(!violations);
    drop(target_client);
    cleanup.run()?;
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and fence-admin roles"]
fn live_foreign_key_conflict_requires_manual_reconciliation() -> anyhow::Result<()> {
    let mut source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let mut target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let mut admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let control_config = admin.clone();
    let source_database = "migration_fk_violation_source".to_owned();
    let target_database = "migration_fk_violation_target".to_owned();
    let mut control = connect(&control_config)?;
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database} OWNER migration_mutator"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database} OWNER migration_fence_target_owner"
    ))?;
    let cleanup = RecoveryDatabaseCleanup::new(
        control_config,
        source_database.clone(),
        target_database.clone(),
    );
    source.database.clone_from(&source_database);
    target.database.clone_from(&target_database);
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC;
         REVOKE CREATE ON SCHEMA public FROM PUBLIC;
         GRANT CONNECT ON DATABASE {source_database} TO migration_reader;
         GRANT USAGE ON SCHEMA public TO migration_reader;
         CREATE TABLE public.parents (id bigint PRIMARY KEY);
         CREATE TABLE public.children (
           id bigint PRIMARY KEY,
           parent_id bigint,
           CONSTRAINT children_parent_fk FOREIGN KEY (parent_id) REFERENCES public.parents(id)
         );
         INSERT INTO public.parents VALUES (1);
         INSERT INTO public.children VALUES (1,1);
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_reader"
    ))?;
    drop(setup);

    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.json");
    let reviewed = write_live_plan_with_consistency(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
    )?;
    install_postgres_write_fence(&admin, &reviewed, &fence_path)?;
    let error = execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: &plan_path,
        source_config_path: &source_path,
        target_config_path: &target_path,
        fence_admin_config_path: &admin_path,
        fence_artifact_path: &fence_path,
        approval_reference: "docker-foreign-key-violation",
        state_path: &state_path,
        interruption: PostgresExecutionInterruption::BeforeForeignKeyChecks,
    })
    .unwrap_err();
    assert!(error.to_string().contains("injected interruption"));

    let mut target_client = connect(&target)?;
    target_client.batch_execute("INSERT INTO public.children VALUES (2,999)")?;
    let source_catalog = reviewed
        .plan
        .source_catalog
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("reviewed plan has no source catalog"))?;
    let foreign_key = postgres_foreign_keys(source_catalog)?
        .into_iter()
        .find(|foreign_key| foreign_key.name.as_str() == "children_parent_fk")
        .ok_or_else(|| anyhow::anyhow!("reviewed foreign key is absent"))?;
    assert!(
        PostgresTargetFactory::new(target.clone())
            .check_foreign_key(&foreign_key)?
            .has_violation
    );
    target_client.batch_execute(
        "DELETE FROM public.children WHERE id=2;
         ALTER TABLE public.children ADD CONSTRAINT children_parent_fk FOREIGN KEY (parent_id) REFERENCES public.parents(id) ON DELETE CASCADE",
    )?;
    drop(target_client);
    let error = resume_postgres_fenced_plan(
        &state_path,
        &source_path,
        &target_path,
        &admin_path,
        &fence_path,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("foreign-key reconciliation requires manual intervention"),
        "{error:#}"
    );
    let state: MigrationState = read_json(&state_path)?;
    assert_eq!(state.status, MigrationStatus::ManualReconciliationRequired);
    let mut source_admin = connect(&admin)?;
    assert!(source_admin
        .batch_execute("INSERT INTO public.parents VALUES (2)")
        .is_err());
    drop(source_admin);
    cleanup.run()?;
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
        attributes: BTreeMap::from([
            (
                "table_oid".into(),
                serde_json::Value::String(table_id.into()),
            ),
            ("validated".into(), serde_json::Value::Bool(true)),
            ("columns".into(), serde_json::json!(["id"])),
        ]),
    };
    Ok(VendorCatalog {
        format_version: 2,
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
    if let (Some(certificate_path), Some(key_path)) = (
        &config.tls.client_certificate,
        &config.tls.client_private_key,
    ) {
        tls.identity(Identity::from_pkcs8(
            &std::fs::read(certificate_path)?,
            &std::fs::read(key_path)?,
        )?);
    }
    if config.tls.insecure {
        tls.danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }
    Ok(pg.connect(MakeTlsConnector::new(tls.build()?))?)
}

struct RecoveryDatabaseCleanup {
    control: PostgresEndpointConfig,
    source_database: String,
    target_database: String,
    armed: bool,
}

impl RecoveryDatabaseCleanup {
    fn new(
        control: PostgresEndpointConfig,
        source_database: String,
        target_database: String,
    ) -> Self {
        Self {
            control,
            source_database,
            target_database,
            armed: true,
        }
    }

    fn run(mut self) -> anyhow::Result<()> {
        self.cleanup()?;
        self.armed = false;
        Ok(())
    }

    fn cleanup(&self) -> anyhow::Result<()> {
        let mut client = connect(&self.control)?;
        client.batch_execute(&format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.source_database
        ))?;
        client.batch_execute(&format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.target_database
        ))?;
        Ok(())
    }
}

impl Drop for RecoveryDatabaseCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} must name a PostgreSQL endpoint config"))
}
