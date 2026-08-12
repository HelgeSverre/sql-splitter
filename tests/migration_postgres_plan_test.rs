#![cfg(feature = "enterprise-migration-spike")]

use std::collections::BTreeMap;
#[cfg(feature = "migration-fault-injection")]
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "migration-fault-injection")]
#[path = "support/postgres_commit_proxy.rs"]
mod postgres_commit_proxy;
#[cfg(feature = "migration-fault-injection")]
use postgres_commit_proxy::{CommitFaultMode, PostgresCommitProxy};

use anyhow::Context;
use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres::config::SslMode;
use postgres::{Client, Config};
use postgres_openssl::MakeTlsConnector;
use sha2::{Digest, Sha256};
use sql_splitter::migration::append_journal::{AppendJournal, PreparedChunk};
use sql_splitter::migration::artifact::{read_json, write_json_new};
use sql_splitter::migration::assessment::{
    render_markdown, stable_report_body, AssessmentArtifact, ProjectedWindow, ThroughputProfile,
    THROUGHPUT_PROFILE_SCHEMA_VERSION,
};
use sql_splitter::migration::canonical::{digest_rows, CanonicalRow};
use sql_splitter::migration::connection::{
    CancellationToken, ConnectionError, KeysetPage, ReadSession, SourceConnectionFactory,
    TargetConnectionFactory,
};
use sql_splitter::migration::journal::{MigrationStatus, OperationState};
use sql_splitter::migration::model::{
    CatalogNamespace, CatalogObject, CatalogObjectKind, ColumnMeta, DbValue, Identifier, KeyTuple,
    QualifiedTable, RowBatch, VendorCatalog,
};
use sql_splitter::migration::plan::{
    AssessmentStatus, PlanPurpose, ReviewedPlan, UnsupportedObjectCode,
};
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::postgres::postgres_foreign_keys;
use sql_splitter::migration::postgres::{
    build_plan, collect_live_assessment, collect_live_assessment_with_profile, inspect_endpoint,
    postgres_sequences, probe_live_postgres_source_profile, write_live_assessment, write_live_plan,
    write_live_plan_with_consistency as write_unbudgeted_live_plan_with_consistency,
    write_live_plan_with_outage_policy, write_live_plan_with_policies,
    write_live_plan_with_profile_tier, PostgresConsistencyMode, PostgresEndpointConfig,
    PostgresPlanError, PostgresSequenceOwnership, PostgresSequenceOwnershipKind,
    PostgresSourceFactory, PostgresTargetFactory, PostgresWritePolicy,
};
use sql_splitter::migration::postgres_fence::{
    attest_postgres_write_fence, install_postgres_write_fence, postgres_write_fence_is_released,
    release_postgres_write_fence, InstalledPostgresFence,
};
use sql_splitter::migration::postgres_profile::{
    PostgresExternalQuiesceAttestation, PostgresExternalQuiesceStatus, PostgresSourceProbeArtifact,
    PostgresSourceProbeStatus, PostgresSourceProfileContract, PostgresSourceProfileKind,
    POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
};
use sql_splitter::migration::runner::execute_postgres_plan;
#[cfg(feature = "migration-fault-injection")]
use sql_splitter::migration::runner::{
    execute_postgres_external_quiesce_plan_with_interruption,
    execute_postgres_fenced_plan_with_cancellation, execute_postgres_fenced_plan_with_interruption,
    execute_postgres_interrupted, resume_postgres_fenced_plan,
    resume_postgres_plan_with_external_quiesce, PostgresCancellationExecution,
    PostgresExecutionInterruption, PostgresInterruptedExecution,
};

#[test]
#[ignore = "requires TLS-enabled PostgreSQL source and empty target databases"]
fn live_plan_is_read_only_self_contained_and_deterministic() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?;
    let directory = private_tempdir()?;
    let first_path = directory.path().join("first.json");
    let second_path = directory.path().join("second.json");

    let first = write_live_plan(&source, &target, &first_path)?;
    let second = write_live_plan(&source, &target, &second_path)?;
    assert_eq!(first, second);
    assert_eq!(std::fs::read(&first_path)?, std::fs::read(&second_path)?);
    assert!(first.plan.source_catalog.is_some());
    assert!(first.plan.target_catalog.is_assessed());
    assert!(first
        .plan
        .target_catalog
        .as_assessed()
        .is_some_and(|catalog| catalog
            .namespaces
            .iter()
            .all(|namespace| namespace.objects.is_empty())));
    assert!(!first.plan.operations.is_empty());
    assert_eq!(read_json::<ReviewedPlan>(first_path)?, first);
    Ok(())
}

#[test]
#[ignore = "requires a TLS-enabled PostgreSQL source with a read-only assessment role"]
fn live_source_only_assessment_is_read_only_and_deterministic() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let source_config = PostgresEndpointConfig::read(&source)?;
    let log_admin_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_LOG_ADMIN_CONFIG")?)?;
    let mut log_admin = connect(&log_admin_config)?;
    let before_rows: i64 = log_admin
        .query_one("SELECT count(*) FROM public.live_snapshot_rows", &[])?
        .get(0);
    let before_objects: i64 = log_admin
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_'",
            &[],
        )?
        .get(0);
    let before_sequence_row = log_admin.query_one(
        "SELECT last_value,is_called FROM public.assessment_sequence",
        &[],
    )?;
    let before_sequence = (
        before_sequence_row.get::<_, i64>(0),
        before_sequence_row.get::<_, bool>(1),
    );
    let directory = private_tempdir()?;
    let first_artifact_path = directory.path().join("assessment-first.json");
    let first_report_path = directory.path().join("assessment-first.md");
    let second_artifact_path = directory.path().join("assessment-second.json");
    let second_report_path = directory.path().join("assessment-second.md");
    let cli_artifact_path = directory.path().join("assessment-cli.json");
    let cli_report_path = directory.path().join("assessment-cli.md");

    let first = write_live_assessment(&source, &first_artifact_path, &first_report_path)?;
    let second = write_live_assessment(&source, &second_artifact_path, &second_report_path)?;
    let cli_output = Command::new(env!("CARGO_BIN_EXE_sql-splitter-migration-spike"))
        .args([
            "assess-postgres",
            "--source-config",
            source.to_str().context("source config path is not UTF-8")?,
            "--assessment-output",
            cli_artifact_path
                .to_str()
                .context("CLI assessment path is not UTF-8")?,
            "--report-output",
            cli_report_path
                .to_str()
                .context("CLI report path is not UTF-8")?,
        ])
        .env_remove("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")
        .env_remove("SQL_SPLITTER_PG_TARGET_PASSWORD")
        .env_remove("SQL_SPLITTER_PG_RUN_TARGET_CONFIG")
        .env_remove("SQL_SPLITTER_PG_RUN_TARGET_PASSWORD")
        .output()?;
    assert!(
        cli_output.status.success(),
        "source-only assessment CLI failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    let cli_assessment = read_json::<AssessmentArtifact>(&cli_artifact_path)?;
    assert_eq!(cli_assessment.reviewed_plan, first.reviewed_plan);
    assert_eq!(
        stable_report_body(&std::fs::read_to_string(cli_report_path)?),
        stable_report_body(&std::fs::read_to_string(&first_report_path)?)
    );
    assert_eq!(first.reviewed_plan.plan.purpose, PlanPurpose::Assessment);
    assert_eq!(
        first.reviewed_plan.plan.target_endpoint_identity,
        AssessmentStatus::NotAssessed
    );
    assert_eq!(
        first.reviewed_plan.plan.target_catalog,
        AssessmentStatus::NotAssessed
    );
    assert!(first.source_evidence.transaction_read_only);
    assert!(first.source_evidence.direct_write_privileges_absent);
    assert!(first.reviewed_plan.plan.validate_for_execution().is_err());
    assert_eq!(
        read_json::<AssessmentArtifact>(&first_artifact_path)?,
        first
    );

    let first_report = std::fs::read_to_string(first_report_path)?;
    let second_report = std::fs::read_to_string(second_report_path)?;
    assert_eq!(
        stable_report_body(&first_report),
        stable_report_body(&second_report)
    );
    assert!(first_report.contains("Target endpoint: **not assessed**"));
    assert!(first_report.contains("Direct database-object write privileges: `absent`"));
    assert!(first_report.contains("Executable routine side effects: `not proven`"));
    assert!(first_report.contains("`acl.report_only`: `approval_required`"));
    assert_eq!(first.reviewed_plan, second.reviewed_plan);
    assert_eq!(
        log_admin
            .query_one("SELECT count(*) FROM public.live_snapshot_rows", &[])?
            .get::<_, i64>(0),
        before_rows
    );
    assert_eq!(
        log_admin
            .query_one(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_'",
                &[],
            )?
            .get::<_, i64>(0),
        before_objects
    );
    let after_sequence = log_admin.query_one(
        "SELECT last_value,is_called FROM public.assessment_sequence",
        &[],
    )?;
    assert_eq!(
        (
            after_sequence.get::<_, i64>(0),
            after_sequence.get::<_, bool>(1)
        ),
        before_sequence
    );
    assert_assessment_statement_log(&mut log_admin, 3)?;
    let assessment_locks: i64 = log_admin
        .query_one(
            "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='sql-splitter-migration-assessment'",
            &[],
        )?
        .get(0);
    assert_eq!(assessment_locks, 0);

    let mut reader = connect(&source_config)?;
    let large_object_oid: u32 = reader.query_one("SELECT lo_create(0)", &[])?.get(0);
    let removed: i32 = reader
        .query_one("SELECT lo_unlink($1)", &[&large_object_oid])?
        .get(0);
    assert_eq!(removed, 1, "large-object fixture cleanup failed");
    // PostgreSQL versions do not uniformly reject every state-changing
    // function from a READ ONLY transaction. The assessment therefore relies
    // on its audited statement allowlist as well as transaction restrictions;
    // it does not treat READ ONLY as a routine-side-effect sandbox.
    Ok(())
}

#[test]
#[ignore = "requires the PG15-17 Docker blocking-code fixture role"]
fn live_postgres_blocking_code_registry_matrix() -> anyhow::Result<()> {
    const LIVE_ASSESSMENT_REQUIRED: &[UnsupportedObjectCode] = &[
        UnsupportedObjectCode::SequencePersistence,
        UnsupportedObjectCode::ViewSecurity,
        UnsupportedObjectCode::ViewColumnAcl,
        UnsupportedObjectCode::ViewAst,
        UnsupportedObjectCode::MaterializedView,
        UnsupportedObjectCode::RowSecurity,
        UnsupportedObjectCode::PartitionTopology,
        UnsupportedObjectCode::PartitionLocalIndex,
        UnsupportedObjectCode::PartitionChildIndexStorage,
        UnsupportedObjectCode::PartitionChildIndexName,
        UnsupportedObjectCode::PartitionLocalConstraint,
        UnsupportedObjectCode::PartitionStorage,
        UnsupportedObjectCode::PartitionTrigger,
        UnsupportedObjectCode::TraditionalInheritance,
        UnsupportedObjectCode::SequenceOwnership,
        UnsupportedObjectCode::UserTypeDdl,
        UnsupportedObjectCode::Extension,
        UnsupportedObjectCode::GeneratedDependency,
        UnsupportedObjectCode::UserDefinedColumnType,
        UnsupportedObjectCode::CollationVersion,
        UnsupportedObjectCode::StandaloneIndex,
        UnsupportedObjectCode::Trigger,
        UnsupportedObjectCode::Routine,
        UnsupportedObjectCode::RowSecurityPolicy,
        UnsupportedObjectCode::EventTrigger,
        UnsupportedObjectCode::RewriteRule,
        UnsupportedObjectCode::Publication,
        UnsupportedObjectCode::ForeignServer,
        UnsupportedObjectCode::ForeignTable,
        UnsupportedObjectCode::ExtendedStatistics,
        UnsupportedObjectCode::UserCollation,
        UnsupportedObjectCode::ResumableKey,
    ];
    const LIVE_EXECUTION_ONLY: &[(UnsupportedObjectCode, &str)] = &[
        (
            UnsupportedObjectCode::SequenceConsistency,
            "source-only assessment records the write-fence requirement instead of selecting an execution consistency mode",
        ),
        (
            UnsupportedObjectCode::TargetNotEmpty,
            "source-only assessment has no target endpoint",
        ),
        (
            UnsupportedObjectCode::SameEndpoint,
            "source-only assessment has no target endpoint",
        ),
    ];
    const ASSESSMENT_UNREACHABLE: &[(UnsupportedObjectCode, &str)] = &[
        (
            UnsupportedObjectCode::GeneratedMode,
            "virtual generated columns require PostgreSQL 18 and are outside the PostgreSQL 15-17 contract",
        ),
        (
            UnsupportedObjectCode::GeneratedCrossMajor,
            "cross-major compatibility requires both source and target endpoints",
        ),
        (
            UnsupportedObjectCode::MySqlStorageEngine,
            "MySQL-specific catalog evidence is outside the PostgreSQL matrix",
        ),
        (
            UnsupportedObjectCode::MySqlCatalogSemantics,
            "MySQL-specific catalog evidence is outside the PostgreSQL matrix",
        ),
        (
            UnsupportedObjectCode::MySqlFreezeEvidence,
            "MySQL-specific freeze evidence is outside the PostgreSQL matrix",
        ),
        (
            UnsupportedObjectCode::MySqlAutoIncrementConsistency,
            "MySQL-specific AUTO_INCREMENT evidence is outside the PostgreSQL matrix",
        ),
    ];
    const NONBLOCKING_REPORT_REQUIRED: &[UnsupportedObjectCode] = &[
        UnsupportedObjectCode::NamespaceAcl,
        UnsupportedObjectCode::RelationAcl,
        UnsupportedObjectCode::RoutineAcl,
        UnsupportedObjectCode::DefaultPrivileges,
    ];
    let classified = LIVE_ASSESSMENT_REQUIRED
        .iter()
        .copied()
        .chain(LIVE_EXECUTION_ONLY.iter().map(|(code, _)| *code))
        .chain(ASSESSMENT_UNREACHABLE.iter().map(|(code, _)| *code))
        .chain(NONBLOCKING_REPORT_REQUIRED.iter().copied())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        classified.len(),
        LIVE_ASSESSMENT_REQUIRED.len()
            + LIVE_EXECUTION_ONLY.len()
            + ASSESSMENT_UNREACHABLE.len()
            + NONBLOCKING_REPORT_REQUIRED.len(),
        "blocking-code registry categories overlap"
    );
    assert_eq!(
        classified,
        UnsupportedObjectCode::ALL.iter().copied().collect(),
        "every unsupported-object code must have an explicit live-test classification"
    );

    let source_path = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let target_path = required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?;
    let admin = PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_ADMIN_CONFIG")?)?;
    let source_config = PostgresEndpointConfig::read(&source_path)?;
    let mut admin_client = connect(&admin)?;
    let mut observed = std::collections::BTreeSet::new();

    admin_client.batch_execute(
        "CREATE SCHEMA blocking_key;
         CREATE TABLE blocking_key.no_key(id bigint,payload text);
         GRANT USAGE ON SCHEMA blocking_key TO migration_reader;
         GRANT SELECT ON blocking_key.no_key TO migration_reader",
    )?;
    let key_snapshot = inspect_endpoint(&source_config)?;
    let empty_target = inspect_endpoint(&PostgresEndpointConfig::read(&target_path)?)?;
    let assessment = build_plan(&key_snapshot, &empty_target)?;
    observed.extend(
        assessment
            .plan
            .unsupported_objects
            .objects
            .iter()
            .filter(|finding| finding.required_semantics)
            .map(|finding| finding.code),
    );
    admin_client.batch_execute("DROP SCHEMA blocking_key CASCADE")?;

    admin_client.batch_execute(
        "CREATE SCHEMA blocking_sequence;
         CREATE SEQUENCE blocking_sequence.needs_fence CACHE 2;
         GRANT USAGE ON SCHEMA blocking_sequence TO migration_reader;
         GRANT SELECT ON SEQUENCE blocking_sequence.needs_fence TO migration_reader",
    )?;
    let sequence_source = inspect_endpoint(&source_config)?;
    let sequence_plan = build_plan(&sequence_source, &empty_target)?;
    observed.extend(
        sequence_plan
            .plan
            .unsupported_objects
            .objects
            .iter()
            .filter(|finding| finding.required_semantics)
            .map(|finding| finding.code),
    );
    let same_endpoint = build_plan(&sequence_source, &sequence_source)?;
    observed.extend(
        same_endpoint
            .plan
            .unsupported_objects
            .objects
            .iter()
            .filter(|finding| finding.required_semantics)
            .map(|finding| finding.code),
    );
    admin_client.batch_execute("DROP SCHEMA blocking_sequence CASCADE")?;

    admin_client.batch_execute(
        "CREATE SCHEMA blocking_matrix;
         GRANT USAGE ON SCHEMA blocking_matrix TO migration_reader;
         CREATE TABLE blocking_matrix.base(id integer,payload text,extra text);
         ALTER TABLE blocking_matrix.base ENABLE ROW LEVEL SECURITY;
         CREATE POLICY base_policy ON blocking_matrix.base USING (true);
         CREATE UNLOGGED SEQUENCE blocking_matrix.unlogged_seq;
         CREATE SEQUENCE blocking_matrix.bad_owner OWNED BY blocking_matrix.base.id;
         GRANT SELECT ON ALL SEQUENCES IN SCHEMA blocking_matrix TO migration_reader;
         CREATE VIEW blocking_matrix.secure_view WITH (security_barrier=true) AS SELECT id FROM blocking_matrix.base;
         CREATE VIEW blocking_matrix.column_acl_view AS SELECT id FROM blocking_matrix.base;
         GRANT SELECT(id) ON blocking_matrix.column_acl_view TO migration_reader;
         CREATE COLLATION blocking_matrix.custom_collation (provider=icu,locale='en-US',version='wrong');
         CREATE TABLE blocking_matrix.collated(id integer PRIMARY KEY,payload text COLLATE blocking_matrix.custom_collation);
         CREATE VIEW blocking_matrix.ast_view AS SELECT payload COLLATE blocking_matrix.custom_collation AS payload FROM blocking_matrix.base;
         CREATE MATERIALIZED VIEW blocking_matrix.materialized AS SELECT id FROM blocking_matrix.base;
         CREATE TABLE blocking_matrix.bad_partition(id text) PARTITION BY RANGE(id);
         CREATE TABLE blocking_matrix.bad_partition_leaf PARTITION OF blocking_matrix.bad_partition FOR VALUES FROM ('a') TO ('z');
         CREATE TABLE blocking_matrix.part_root(id integer,payload text) PARTITION BY RANGE(id);
         CREATE TABLE blocking_matrix.part_leaf PARTITION OF blocking_matrix.part_root FOR VALUES FROM (0) TO (100);
         CREATE INDEX part_root_payload_idx ON blocking_matrix.part_root(payload);
         ALTER INDEX blocking_matrix.part_leaf_payload_idx RENAME TO unexpected_child_idx;
         ALTER INDEX blocking_matrix.unexpected_child_idx SET (fillfactor=70);
         CREATE INDEX part_leaf_local_idx ON blocking_matrix.part_leaf(id);
         ALTER TABLE blocking_matrix.part_leaf ADD CONSTRAINT leaf_local_check CHECK(id>=0);
         ALTER TABLE blocking_matrix.part_leaf SET (fillfactor=70);
         CREATE TABLE blocking_matrix.inheritance_parent(id integer);
         CREATE TABLE blocking_matrix.inheritance_child() INHERITS(blocking_matrix.inheritance_parent);
         CREATE TYPE blocking_matrix.mood AS ENUM('ok','bad');
         CREATE DOMAIN blocking_matrix.account_id AS bigint;
         CREATE TABLE blocking_matrix.typed(id blocking_matrix.account_id);
         CREATE FUNCTION blocking_matrix.bump(value integer) RETURNS integer LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE RETURN value+1;
         REVOKE ALL ON FUNCTION blocking_matrix.bump(integer) FROM PUBLIC;
         GRANT EXECUTE ON FUNCTION blocking_matrix.bump(integer) TO migration_reader;
         ALTER DEFAULT PRIVILEGES IN SCHEMA blocking_matrix GRANT SELECT ON TABLES TO migration_reader;
         CREATE TABLE blocking_matrix.generated(id integer,derived integer GENERATED ALWAYS AS (blocking_matrix.bump(id)) STORED);
         CREATE INDEX base_payload_hash ON blocking_matrix.base USING hash(payload);
         CREATE FUNCTION blocking_matrix.trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
         CREATE TRIGGER base_trigger BEFORE INSERT ON blocking_matrix.base FOR EACH ROW EXECUTE FUNCTION blocking_matrix.trigger_fn();
         CREATE TRIGGER leaf_trigger BEFORE INSERT ON blocking_matrix.part_leaf FOR EACH ROW EXECUTE FUNCTION blocking_matrix.trigger_fn();
         CREATE FUNCTION blocking_matrix.event_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;
         CREATE EVENT TRIGGER blocking_matrix_event ON ddl_command_end EXECUTE FUNCTION blocking_matrix.event_fn();
         CREATE RULE base_insert_rule AS ON INSERT TO blocking_matrix.base DO INSTEAD NOTHING;
         CREATE PUBLICATION blocking_matrix_publication FOR TABLE blocking_matrix.base;
         CREATE EXTENSION hstore;
         CREATE EXTENSION postgres_fdw;
         CREATE SERVER blocking_matrix_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host '127.0.0.1',dbname 'postgres');
         CREATE FOREIGN TABLE blocking_matrix.foreign_rows(id integer) SERVER blocking_matrix_server OPTIONS(table_name 'base');
         CREATE STATISTICS blocking_matrix.base_stats ON payload,extra FROM blocking_matrix.base;
         GRANT SELECT ON ALL TABLES IN SCHEMA blocking_matrix TO migration_reader",
    )?;
    let snapshot = inspect_endpoint(&source_config)?;
    observed.extend(
        snapshot
            .unsupported
            .objects
            .iter()
            .filter(|finding| finding.required_semantics)
            .map(|finding| finding.code),
    );
    let assessment = collect_live_assessment(&source_config)?;
    let assessment_codes = assessment
        .reviewed_plan
        .plan
        .unsupported_objects
        .objects
        .iter()
        .map(|finding| finding.code)
        .collect::<std::collections::BTreeSet<_>>();
    let report = render_markdown(&assessment)?;
    for code in LIVE_ASSESSMENT_REQUIRED {
        assert!(
            assessment_codes.contains(code),
            "source assessment omitted live-required code {code:?}"
        );
        assert!(
            report.contains(code.as_str()),
            "assessment Markdown omitted live-required code {code:?}"
        );
    }
    for code in NONBLOCKING_REPORT_REQUIRED {
        assert!(
            assessment_codes.contains(code),
            "source assessment omitted report-only code {code:?}"
        );
        assert!(
            report.contains(code.as_str()),
            "assessment Markdown omitted report-only code {code:?}"
        );
    }
    admin_client.batch_execute(
        "DROP EVENT TRIGGER blocking_matrix_event;
         DROP PUBLICATION blocking_matrix_publication;
         DROP SERVER blocking_matrix_server CASCADE;
         DROP EXTENSION postgres_fdw CASCADE;
         DROP EXTENSION hstore CASCADE;
         DROP SCHEMA blocking_matrix CASCADE",
    )?;

    let required = LIVE_ASSESSMENT_REQUIRED
        .iter()
        .copied()
        .chain(LIVE_EXECUTION_ONLY.iter().map(|(code, _)| *code))
        .collect::<std::collections::BTreeSet<_>>();
    let missing = required.difference(&observed).copied().collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "live blocking-code cases are missing: {missing:?}"
    );
    Ok(())
}

#[test]
#[ignore = "requires a TLS-enabled PostgreSQL source with a read-only assessment role"]
fn live_assessment_rejects_direct_write_privileges() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_TEST_SOURCE_CONFIG")?;
    let source_config = PostgresEndpointConfig::read(&source)?;
    let mutator_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_MUTATOR_CONFIG")?)?;
    let directory = private_tempdir()?;
    let mut mutator = connect(&mutator_config)?;
    let mut reader = connect(&source_config)?;

    mutator.batch_execute(
        "CREATE OR REPLACE FUNCTION public.assessment_write_escape() RETURNS void LANGUAGE sql SECURITY DEFINER AS $$ INSERT INTO public.live_snapshot_rows(id,payload) VALUES (999999,'escape') ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload $$; GRANT EXECUTE ON FUNCTION public.assessment_write_escape() TO PUBLIC",
    )?;
    reader.batch_execute("SELECT public.assessment_write_escape()")?;
    let escaped: bool = mutator
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM public.live_snapshot_rows WHERE id=999999 AND payload='escape')",
            &[],
        )?
        .get(0);
    assert!(
        escaped,
        "fixture did not prove the SECURITY DEFINER write path"
    );
    let rejected = write_live_assessment(
        &source,
        directory.path().join("unsafe-assessment.json"),
        directory.path().join("unsafe-assessment.md"),
    );
    mutator.batch_execute(
        "DELETE FROM public.live_snapshot_rows WHERE id=999999; DROP FUNCTION public.assessment_write_escape()",
    )?;
    assert!(matches!(
        rejected,
        Err(PostgresPlanError::SourceRoleHoldsDirectWritePrivilege)
    ));

    mutator.batch_execute(
        "CREATE VIEW public.assessment_write_view AS SELECT id,payload FROM public.live_snapshot_rows; GRANT INSERT,UPDATE,DELETE ON public.assessment_write_view TO migration_reader",
    )?;
    reader.batch_execute(
        "INSERT INTO public.assessment_write_view(id,payload) VALUES (999998,'view-escape')",
    )?;
    let view_escaped: bool = mutator
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM public.live_snapshot_rows WHERE id=999998 AND payload='view-escape')",
            &[],
        )?
        .get(0);
    assert!(view_escaped, "fixture did not prove the writable-view path");
    let view_rejected = write_live_assessment(
        &source,
        directory.path().join("writable-view-assessment.json"),
        directory.path().join("writable-view-assessment.md"),
    );
    mutator.batch_execute(
        "DELETE FROM public.live_snapshot_rows WHERE id=999998; DROP VIEW public.assessment_write_view",
    )?;
    assert!(matches!(
        view_rejected,
        Err(PostgresPlanError::SourceRoleHoldsDirectWritePrivilege)
    ));
    Ok(())
}

#[test]
#[ignore = "requires PostgreSQL configured to require a trusted client certificate"]
fn live_mutual_tls_requires_valid_client_identity() -> anyhow::Result<()> {
    let config_path = required_path("SQL_SPLITTER_PG_MTLS_CONFIG")?;
    let config = PostgresEndpointConfig::read(config_path)?;
    let snapshot = inspect_endpoint(&config)?;
    assert!(snapshot.tls_binding.starts_with("hostname_verified+mtls;"));

    let mut wrong_hostname = config.clone();
    wrong_hostname.host = "127.1".into();
    assert!(inspect_endpoint(&wrong_hostname).is_err());
    wrong_hostname.tls.insecure = true;
    assert!(inspect_endpoint(&wrong_hostname)?
        .tls_binding
        .starts_with("insecure_explicit+mtls;"));

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
    let mut peer = factory.open_imported_snapshot_peer(&snapshot, CancellationToken::default())?;
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
    let peer_first = peer.select_page(&KeysetPage {
        table: table.clone(),
        projection: projection.clone(),
        key: key.clone(),
        after: None,
        limit: 2,
    })?;
    assert_eq!(peer_first.rows(), first.rows());
    mutator.batch_execute(
        "UPDATE public.live_snapshot_rows SET payload = 'changed' WHERE id = 2;
         UPDATE public.live_snapshot_rows SET id = 30 WHERE id = 3;
         INSERT INTO public.live_snapshot_rows (id, payload) VALUES (4, 'four')",
    )?;

    let second_request = KeysetPage {
        table,
        projection,
        key,
        after: Some(KeyTuple::new(vec![DbValue::Signed(2)])),
        limit: 2,
    };
    let second = reader.select_page(&second_request)?;
    let peer_second = peer.select_page(&second_request)?;
    assert_eq!(
        second.rows(),
        &[vec![DbValue::Signed(3), DbValue::Text("three".into())]]
    );
    assert_eq!(peer_second.rows(), second.rows());
    assert_eq!(peer.snapshot(), &snapshot);
    assert_eq!(reader.snapshot(), &snapshot);
    drop(reader);

    let mutator_factory = PostgresSourceFactory::new(PostgresEndpointConfig::read(mutator_path)?);
    let mutator_error = mutator_factory
        .capture_snapshot()
        .expect_err("write-capable source role must be rejected");
    assert!(
        matches!(
            &mutator_error,
            ConnectionError::InvalidRequest(message)
                if message.contains("write capability")
        ),
        "unexpected mutator rejection: {mutator_error:?}"
    );
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
        let table_id = source_catalog
            .namespaces
            .iter()
            .flat_map(|namespace| &namespace.objects)
            .find(|object| object.kind == CatalogObjectKind::Table && object.name.as_str() == table)
            .map(|object| object.id.as_str())
            .ok_or_else(|| anyhow::anyhow!("table {table} is absent from source catalog"))?;
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.code == UnsupportedObjectCode::ResumableKey && object.object_id == table_id
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
    let mut source_administrator =
        connect(&source_admin_config).context("connect source administrator")?;
    source_administrator
        .batch_execute(
            "DROP TABLE IF EXISTS public.source_values;
         CREATE TABLE public.source_values (
           id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
           nullable_value text,
           bool_value boolean,
           int2_value smallint,
           int4_value integer,
           int8_value bigint,
           oid_value oid,
           float4_value real,
           float8_value double precision,
           text_value text,
           varchar_value varchar(12),
           bpchar_value char(6),
           name_value name,
           binary_value bytea,
           json_value json,
           jsonb_value jsonb,
           numeric_value numeric(20,5),
           date_value date,
           time_value time(6),
           time0_value time(0),
           timetz_value time(6) with time zone,
           timestamp_value timestamp(6),
           timestamp0_value timestamp(0),
           timestamptz_value timestamptz(6),
           timestamptz0_value timestamptz(0),
           uuid_value uuid
         );
         GRANT SELECT ON public.source_values TO migration_reader;
         GRANT SELECT ON SEQUENCE public.source_values_id_seq TO migration_reader;
         INSERT INTO public.source_values (
           nullable_value,bool_value,int2_value,int4_value,int8_value,oid_value,
           float4_value,float8_value,text_value,varchar_value,bpchar_value,name_value,
           binary_value,json_value,jsonb_value,numeric_value,date_value,time_value,time0_value,
           timetz_value,timestamp_value,timestamp0_value,timestamptz_value,timestamptz0_value,
           uuid_value
         ) VALUES (
           'present',true,-32768,-2147483648,-9223372036854775808,42,
           '-0'::real,'-0'::double precision,'exact text','varchar','xy','mixed.name',
           decode('00ff10','hex'),'{ \"b\": 2, \"a\": 1 }'::json,
           '{\"b\":2,\"a\":1}'::jsonb,1234567890123.45000,'2026-08-11',
           '10:11:12.123456','10:11:13','10:11:12.123456+02',
           '2026-08-11 10:11:12.123456','2026-08-11 10:11:13',
           '2026-08-11 10:11:12.123456+02','2026-08-11 10:11:13+02',
           '123e4567-e89b-12d3-a456-426614174000'
         );
         INSERT INTO public.source_values DEFAULT VALUES;
         INSERT INTO public.source_values (
           nullable_value,bool_value,int2_value,int4_value,int8_value,oid_value,
           float4_value,float8_value,text_value,varchar_value,bpchar_value,name_value,
           binary_value,json_value,jsonb_value,numeric_value,date_value,time_value,time0_value,
           timetz_value,timestamp_value,timestamp0_value,timestamptz_value,timestamptz0_value,
           uuid_value
         ) VALUES (
           NULL,false,32767,2147483647,9223372036854775807,4294967295,
           'Infinity'::real,'-Infinity'::double precision,'Unicode: åß水🧪','edge','z','Upper.Name',
           decode('00ff','hex'),'{\"duplicate\":1,\"duplicate\":2,\"number\":12,\"wide\":9007199254740993}'::json,
           '{\"number\":12,\"wide\":9007199254740993,\"nested\":{\"z\":0,\"a\":1}}'::jsonb,
           999999999999999.00001,'2000-02-29','23:59:59.999999','23:59:59',
           '00:00:00.000001-07:30','1999-12-31 23:59:59.999999','1999-12-31 23:59:59',
           '2000-01-01 00:00:00.000001-07:30','2000-01-01 00:00:00-07:30',
           'ffffffff-ffff-ffff-ffff-ffffffffffff'
         );
         INSERT INTO public.source_values (
           nullable_value,bool_value,int2_value,int4_value,int8_value,oid_value,
           float4_value,float8_value,text_value,varchar_value,bpchar_value,name_value,
           binary_value,json_value,jsonb_value,numeric_value,date_value,time_value,time0_value,
           timetz_value,timestamp_value,timestamp0_value,timestamptz_value,timestamptz0_value,
           uuid_value
         ) VALUES (
           '',true,0,0,0,0,'NaN'::real,'NaN'::double precision,
           U&'combining: e\\0301','', '', 'n',decode('00ff00','hex'),
           '[1,1.0,1e0,null,true,false]'::json,
           '[1,1.0,1e0,null,true,false]'::jsonb,-0.0100,'1970-01-01',
           '00:00:00','00:00:00','00:00:00+00','1970-01-01 00:00:00.1',
           '1970-01-01 00:00:00','1970-01-01 00:00:00.1+00',
           '1970-01-01 00:00:00+00','00000000-0000-0000-0000-000000000000'
         )",
        )
        .context("prepare source value matrix")?;
    let float4_nan_a = f32::from_bits(0x7fc0_0001);
    let float8_nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let float4_nan_b = f32::from_bits(0xffc0_1234);
    let float8_nan_b = f64::from_bits(0xfff8_0000_0000_1234);
    source_administrator
        .execute(
            "INSERT INTO public.source_values (float4_value,float8_value) VALUES ($1,$2),($3,$4)",
            &[&float4_nan_a, &float8_nan_a, &float4_nan_b, &float8_nan_b],
        )
        .context("prepare distinct floating-point NaN payloads")?;
    let mut target_administrator =
        connect(&target_config).context("connect target administrator")?;
    target_administrator
        .batch_execute(
            "DROP TABLE IF EXISTS public.target_values_insert, public.target_values_copy;
         CREATE TABLE public.target_values_insert (
           id bigint PRIMARY KEY, nullable_value text, bool_value boolean,
           int2_value smallint, int4_value integer, int8_value bigint, oid_value oid,
           float4_value real, float8_value double precision, text_value text,
           varchar_value varchar(12), bpchar_value char(6), name_value name,
           binary_value bytea, json_value json, jsonb_value jsonb,
           numeric_value numeric(20,5), date_value date, time_value time(6), time0_value time(0),
           timetz_value time(6) with time zone, timestamp_value timestamp(6),
           timestamp0_value timestamp(0), timestamptz_value timestamptz(6),
           timestamptz0_value timestamptz(0), uuid_value uuid
         );
         CREATE TABLE public.target_values_copy
           (LIKE public.target_values_insert INCLUDING ALL)",
        )
        .context("prepare target value matrices")?;

    let source = PostgresSourceFactory::new(source_config);
    let snapshot = source
        .capture_snapshot()
        .context("capture source snapshot")?;
    let mut reader = source
        .open_reader(&snapshot, CancellationToken::default())
        .context("open source reader")?;
    let source_table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("source_values")?,
    };
    let projection = [
        "id",
        "nullable_value",
        "bool_value",
        "int2_value",
        "int4_value",
        "int8_value",
        "oid_value",
        "float4_value",
        "float8_value",
        "text_value",
        "varchar_value",
        "bpchar_value",
        "name_value",
        "binary_value",
        "json_value",
        "jsonb_value",
        "numeric_value",
        "date_value",
        "time_value",
        "time0_value",
        "timetz_value",
        "timestamp_value",
        "timestamp0_value",
        "timestamptz_value",
        "timestamptz0_value",
        "uuid_value",
    ]
    .into_iter()
    .map(Identifier::new)
    .collect::<Result<Vec<_>, _>>()?;
    let key = vec![Identifier::new("id")?];
    let batch = reader
        .select_page(&KeysetPage {
            table: source_table,
            projection: projection.clone(),
            key: key.clone(),
            after: None,
            limit: 10,
        })
        .context("read source value matrix")?;
    assert_eq!(batch.len(), 6);

    let target = sql_splitter::migration::postgres::PostgresTargetFactory::new(target_config);
    let cancellation = CancellationToken::default();
    let insert_table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("target_values_insert")?,
    };
    let copy_table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("target_values_copy")?,
    };
    let mut insert_writer = target
        .open_writer(cancellation.clone())
        .context("open INSERT writer")?;
    insert_writer.begin().context("begin INSERT transaction")?;
    insert_writer
        .insert(&insert_table, &batch)
        .context("plain INSERT value matrix")?;
    insert_writer.commit().context("plain INSERT commit")?;
    let mut copy_writer = target
        .open_writer(cancellation.clone())
        .context("open COPY writer")?;
    copy_writer.begin().context("begin COPY transaction")?;
    copy_writer
        .bulk_write(&copy_table, &batch)
        .context("binary COPY value matrix")?;
    copy_writer.commit().context("binary COPY commit")?;

    let mut insert_verifier = target.open_verifier(cancellation.clone())?;
    let inserted = insert_verifier.select_page(&KeysetPage {
        table: insert_table,
        projection: projection.clone(),
        key: key.clone(),
        after: None,
        limit: 10,
    })?;
    let mut copy_verifier = target.open_verifier(cancellation)?;
    let copied = copy_verifier.select_page(&KeysetPage {
        table: copy_table,
        projection,
        key,
        after: None,
        limit: 10,
    })?;
    assert_eq!(inserted.rows(), batch.rows());
    assert_eq!(copied.rows(), batch.rows());
    assert_eq!(copied.rows(), inserted.rows());
    let expected_digest = canonical_value_matrix_digest(&batch)?;
    assert_eq!(canonical_value_matrix_digest(&inserted)?, expected_digest);
    assert_eq!(canonical_value_matrix_digest(&copied)?, expected_digest);
    Ok(())
}

fn canonical_value_matrix_digest(batch: &RowBatch) -> anyhow::Result<String> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let keys = batch
        .rows()
        .iter()
        .map(|row| {
            row.first()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("value-matrix row has no key"))
                .map(|key| vec![key])
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let rows = batch
        .rows()
        .iter()
        .zip(&keys)
        .map(|(values, key)| CanonicalRow {
            table: "public.value_matrix",
            columns: &columns,
            key,
            values,
        })
        .collect::<Vec<_>>();
    Ok(hex::encode(digest_rows(rows.iter())?))
}

#[test]
#[ignore = "requires an empty TLS-enabled PostgreSQL target for reproducible throughput measurements"]
fn live_insert_and_copy_throughput_matrix() -> anyhow::Result<()> {
    const ROWS: usize = 10_000;
    const WIDE_BYTES: usize = 2_048;

    let target_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_TEST_TARGET_CONFIG")?)?;
    let mut administrator = connect(&target_config)?;
    administrator.batch_execute(
        "DROP TABLE IF EXISTS
            public.throughput_narrow_insert,
            public.throughput_narrow_copy,
            public.throughput_wide_insert,
            public.throughput_wide_copy,
            public.throughput_bytea_insert,
            public.throughput_bytea_copy;
         CREATE TABLE public.throughput_narrow_insert (id bigint PRIMARY KEY, payload bigint NOT NULL);
         CREATE TABLE public.throughput_narrow_copy (LIKE public.throughput_narrow_insert INCLUDING ALL);
         CREATE TABLE public.throughput_wide_insert (id bigint PRIMARY KEY, payload text NOT NULL);
         CREATE TABLE public.throughput_wide_copy (LIKE public.throughput_wide_insert INCLUDING ALL);
         CREATE TABLE public.throughput_bytea_insert (id bigint PRIMARY KEY, payload bytea NOT NULL);
         CREATE TABLE public.throughput_bytea_copy (LIKE public.throughput_bytea_insert INCLUDING ALL)",
    )?;
    let server_version: String = administrator
        .query_one("SELECT current_setting('server_version')", &[])?
        .get(0);

    let column = |name: &str, ordinal, vendor_type: &str| -> anyhow::Result<ColumnMeta> {
        Ok(ColumnMeta {
            name: Identifier::new(name)?,
            ordinal,
            vendor_type: vendor_type.into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        })
    };
    let mut narrow = RowBatch::new(
        vec![column("id", 0, "bigint")?, column("payload", 1, "bigint")?],
        ROWS,
        usize::MAX,
    );
    let mut wide = RowBatch::new(
        vec![column("id", 0, "bigint")?, column("payload", 1, "text")?],
        ROWS,
        usize::MAX,
    );
    let mut bytea = RowBatch::new(
        vec![column("id", 0, "bigint")?, column("payload", 1, "bytea")?],
        ROWS,
        usize::MAX,
    );
    let wide_value = "x".repeat(WIDE_BYTES);
    let bytea_value = (0..WIDE_BYTES)
        .map(|index| u8::try_from(index % 251))
        .collect::<Result<Vec<_>, _>>()?;
    for id in 1..=ROWS {
        let id = i128::try_from(id)?;
        narrow.try_push(vec![DbValue::Signed(id), DbValue::Signed(-id)], 16)?;
        wide.try_push(
            vec![DbValue::Signed(id), DbValue::Text(wide_value.clone())],
            8 + WIDE_BYTES,
        )?;
        bytea.try_push(
            vec![DbValue::Signed(id), DbValue::Bytes(bytea_value.clone())],
            8 + WIDE_BYTES,
        )?;
    }

    let target = PostgresTargetFactory::new(target_config.clone());
    let build_profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    for (shape, batch) in [("narrow", &narrow), ("wide", &wide), ("bytea", &bytea)] {
        let insert_table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new(format!("throughput_{shape}_insert"))?,
        };
        let copy_table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new(format!("throughput_{shape}_copy"))?,
        };
        let insert_elapsed = measure_target_write(&target, &insert_table, batch, false)?;
        let copy_elapsed = measure_target_write(&target, &copy_table, batch, true)?;
        for (method, elapsed) in [("insert", insert_elapsed), ("copy", copy_elapsed)] {
            let seconds = elapsed.as_secs_f64();
            eprintln!(
                "MIGRATION_THROUGHPUT postgres={} profile={} shape={} method={} rows={} encoded_bytes={} elapsed_seconds={:.6} rows_per_second={:.2} bytes_per_second={:.2}",
                server_version,
                build_profile,
                shape,
                method,
                batch.len(),
                batch.encoded_bytes(),
                seconds,
                batch.len() as f64 / seconds,
                batch.encoded_bytes() as f64 / seconds,
            );
        }
        let verify_elapsed = measure_target_verification(
            &target,
            &insert_table,
            &copy_table,
            batch,
            u32::try_from(ROWS.min(1_000))?,
        )?;
        let verify_seconds = verify_elapsed.as_secs_f64();
        eprintln!(
            "MIGRATION_THROUGHPUT postgres={} profile={} shape={} method=verify rows={} encoded_bytes={} elapsed_seconds={:.6} rows_per_second={:.2} bytes_per_second={:.2}",
            server_version,
            build_profile,
            shape,
            batch.len(),
            batch.encoded_bytes(),
            verify_seconds,
            batch.len() as f64 / verify_seconds,
            batch.encoded_bytes() as f64 / verify_seconds,
        );
        let insert_count: i64 = administrator
            .query_one(
                &format!("SELECT count(*) FROM public.throughput_{shape}_insert"),
                &[],
            )?
            .get(0);
        let copy_count: i64 = administrator
            .query_one(
                &format!("SELECT count(*) FROM public.throughput_{shape}_copy"),
                &[],
            )?
            .get(0);
        assert_eq!(insert_count, i64::try_from(ROWS)?);
        assert_eq!(copy_count, insert_count);
    }
    Ok(())
}

fn measure_target_write(
    target: &PostgresTargetFactory,
    table: &QualifiedTable,
    batch: &RowBatch,
    bulk: bool,
) -> anyhow::Result<Duration> {
    let mut writer = target.open_writer(CancellationToken::default())?;
    writer.begin()?;
    let started = Instant::now();
    if bulk {
        writer.bulk_write(table, batch)?;
    } else {
        writer.insert(table, batch)?;
    }
    writer.commit()?;
    Ok(started.elapsed())
}

fn measure_target_verification(
    target: &PostgresTargetFactory,
    insert_table: &QualifiedTable,
    copy_table: &QualifiedTable,
    expected: &RowBatch,
    page_limit: u32,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let cancellation = CancellationToken::default();
    let mut insert_verifier = target.open_verifier(cancellation.clone())?;
    let mut copy_verifier = target.open_verifier(cancellation)?;
    let projection = expected
        .columns()
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let key = vec![Identifier::new("id")?];
    let mut after = None;
    let mut verified_rows = 0usize;
    loop {
        let inserted = insert_verifier.select_page(&KeysetPage {
            table: insert_table.clone(),
            projection: projection.clone(),
            key: key.clone(),
            after: after.clone(),
            limit: page_limit,
        })?;
        let copied = copy_verifier.select_page(&KeysetPage {
            table: copy_table.clone(),
            projection: projection.clone(),
            key: key.clone(),
            after: after.clone(),
            limit: page_limit,
        })?;
        assert_eq!(copied.rows(), inserted.rows());
        assert_eq!(
            canonical_value_matrix_digest(&copied)?,
            canonical_value_matrix_digest(&inserted)?
        );
        if inserted.is_empty() {
            break;
        }
        let end = verified_rows
            .checked_add(inserted.len())
            .ok_or_else(|| anyhow::anyhow!("verification row count overflow"))?;
        assert_eq!(
            inserted.rows(),
            expected
                .rows()
                .get(verified_rows..end)
                .ok_or_else(|| anyhow::anyhow!("verification returned excess rows"))?
        );
        let final_key = inserted
            .rows()
            .last()
            .and_then(|row| row.first())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("verification page has no final key"))?;
        after = Some(KeyTuple::new(vec![final_key]));
        verified_rows = end;
    }
    assert_eq!(verified_rows, expected.len());
    Ok(started.elapsed())
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
    let source_template = required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_RUN_TARGET_CONFIG")?;
    let directory = private_tempdir()?;
    let source = directory.path().join("source.toml");
    let mut source_config = PostgresEndpointConfig::read(source_template)?;
    source_config.max_batch_rows = 1;
    std::fs::write(&source, toml::to_string(&source_config)?)?;
    let plan_path = directory.path().join("reviewed-plan.json");
    let state_path = directory.path().join("migration-state.journal");
    let budget_plan_path = directory.path().join("budget-plan.json");
    let budget_assessment_path = directory.path().join("budget-assessment.json");
    let blocked_state_path = directory.path().join("blocked-state.journal");
    let reviewed = write_live_execution_plan(
        &source,
        &target,
        &plan_path,
        PostgresConsistencyMode::ConsistentSnapshot,
    )?;
    assert!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "{:#?}",
        reviewed.plan.unsupported_objects
    );

    let source_endpoint = PostgresEndpointConfig::read(&source)?;
    let measured_at_unix_seconds = u64::try_from(
        connect(&source_endpoint)?
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint",
                &[],
            )?
            .get::<_, i64>(0),
    )?;
    let budget_profile = ThroughputProfile {
        schema_version: THROUGHPUT_PROFILE_SCHEMA_VERSION,
        measurement_reference: "live-preflight-budget".into(),
        environment_reference: format!("postgres-{}-docker", source_endpoint.database),
        postgres_major_version: u16::try_from(
            inspect_endpoint(&source_endpoint)?.server_version_num / 10_000,
        )?,
        measured_at_unix_seconds,
        valid_for_seconds: 3_600,
        copy_bytes_per_second: 1,
        verification_bytes_per_second: 1,
    };
    let budget_assessment =
        collect_live_assessment_with_profile(&source_endpoint, Some(&budget_profile))?;
    let reviewed_seconds = match &budget_assessment.projected_window {
        ProjectedWindow::Estimated { seconds, .. } => *seconds,
        ProjectedWindow::NotAssessed { reason } => {
            anyhow::bail!("live budget assessment was not projected: {reason}")
        }
    };
    write_json_new(&budget_assessment_path, &budget_assessment)?;
    write_live_plan_with_outage_policy(
        &source,
        &target,
        &budget_plan_path,
        PostgresConsistencyMode::ConsistentSnapshot,
        Some(&budget_assessment_path),
        Some(reviewed_seconds),
    )?;

    let report = execute_postgres_plan(
        &plan_path,
        &source,
        &target,
        "LIVE-TEST-APPROVAL",
        &state_path,
    )?;
    assert_eq!(report.copied_rows, 3);
    assert_eq!(report.committed_chunks, 3);
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
    assert_eq!(committed_chunks(&state)?.count(), 3);
    let accepted = state
        .genesis()
        .accepted_outage_projection
        .as_ref()
        .context("policy-bound execution did not record its accepted projection")?;
    let policy = reviewed
        .plan
        .outage_policy
        .as_ref()
        .context("policy-bound plan did not retain its outage policy")?;
    let accepted_digest = accepted.canonical_hash(policy)?;
    assert_eq!(
        state.genesis().binding.outage_projection_digest.as_deref(),
        Some(accepted_digest.as_str())
    );
    assert!(accepted.projected_seconds <= policy.maximum_approved_seconds);

    let mut target_client = connect(&PostgresEndpointConfig::read(&target)?)?;
    let rows = target_client.query("SELECT id, name FROM public.accounts ORDER BY id", &[])?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[2].get::<_, String>(1), "three");
    let target_objects_before: i64 = target_client
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'",
            &[],
        )?
        .get(0);

    let admin_config =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    let mut admin = connect(&admin_config)?;
    admin.execute(
        "UPDATE public.accounts SET name=(SELECT string_agg(md5(value::text),'') FROM generate_series(1,40000) value) WHERE id=3",
        &[],
    )?;
    let blocked = execute_postgres_plan(
        &budget_plan_path,
        &source,
        &target,
        "LIVE-BUDGET-BLOCK",
        &blocked_state_path,
    );
    let restore = admin.execute("UPDATE public.accounts SET name='three' WHERE id=3", &[]);
    restore?;
    let error = match blocked {
        Ok(report) => {
            anyhow::bail!("expanded source unexpectedly passed outage preflight: {report:?}")
        }
        Err(error) => error,
    };
    anyhow::ensure!(
        error
            .to_string()
            .contains("projected outage exceeds the reviewed maximum"),
        "unexpected preflight error: {error:#}"
    );
    anyhow::ensure!(
        !blocked_state_path.exists(),
        "blocked outage preflight created a migration journal"
    );
    let target_rows_after =
        target_client.query("SELECT id, name FROM public.accounts ORDER BY id", &[])?;
    anyhow::ensure!(
        target_rows_after.len() == 3
            && target_rows_after[0].get::<_, i64>(0) == 1
            && target_rows_after[2].get::<_, String>(1) == "three",
        "blocked outage preflight changed target rows"
    );
    let target_objects_after: i64 = target_client
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'",
            &[],
        )?
        .get(0);
    anyhow::ensure!(
        target_objects_after == target_objects_before,
        "blocked outage preflight changed target schema objects"
    );
    Ok(())
}

#[test]
#[ignore = "requires TLS-enabled PostgreSQL source, distinct reader, and administrator roles"]
fn live_source_profile_probe_is_bound_and_rolled_back() -> anyhow::Result<()> {
    let source = required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?;
    let target = required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?;
    let admin_path = required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?;
    let directory = private_tempdir()?;
    let probe_path = directory.path().join("source-profile-probe.json");
    let plan_path = directory.path().join("source-profile-plan.json");

    let artifact = probe_live_postgres_source_profile(
        &source,
        &admin_path,
        PostgresSourceProfileKind::SelfManagedAdministrator,
        &probe_path,
    )?;
    artifact.require_all_proven()?;
    let source_snapshot = inspect_endpoint(&PostgresEndpointConfig::read(&source)?)?;
    assert_eq!(
        artifact.source_catalog_fingerprint,
        sql_splitter::migration::postgres::catalog_fingerprint(&source_snapshot.catalog)?
    );

    let mut admin = connect(&PostgresEndpointConfig::read(&admin_path)?)?;
    let residual: i64 = admin
        .query_one(
            "SELECT (SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'sql_splitter_profile_probe_%') + (SELECT count(*) FROM pg_event_trigger WHERE evtname LIKE 'sql_splitter_profile_probe_%')",
            &[],
        )?
        .get(0);
    assert_eq!(residual, 0, "transactional probes left catalog objects");

    let reviewed = write_live_plan_with_policies(
        &source,
        &target,
        &plan_path,
        PostgresConsistencyMode::WriteFence,
        None,
        None,
        Some(PostgresSourceProfileKind::SelfManagedAdministrator),
        Some(&probe_path),
    )?;
    match reviewed.plan.postgres_source_profile.as_ref() {
        Some(PostgresSourceProfileContract::SelfManagedAdministrator {
            probe_artifact,
            probe_artifact_digest,
        }) => {
            assert_eq!(probe_artifact, &artifact);
            assert_eq!(probe_artifact_digest, &artifact.canonical_hash()?);
        }
        profile => anyhow::bail!("unexpected reviewed source profile: {profile:?}"),
    }

    admin
        .batch_execute("CREATE TABLE public.profile_probe_catalog_drift (id bigint PRIMARY KEY)")?;
    let drifted = write_live_plan_with_policies(
        &source,
        &target,
        directory.path().join("drifted-plan.json"),
        PostgresConsistencyMode::WriteFence,
        None,
        None,
        Some(PostgresSourceProfileKind::SelfManagedAdministrator),
        Some(&probe_path),
    );
    admin.batch_execute("DROP TABLE public.profile_probe_catalog_drift")?;
    let drifted = drifted.expect_err("catalog drift reused stale probe evidence");
    assert!(matches!(
        drifted,
        PostgresPlanError::SourceProfile(
            sql_splitter::migration::postgres_profile::PostgresSourceProfileError::ProfileEvidenceMismatch
        )
    ));
    Ok(())
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires TLS-enabled PostgreSQL plus reader, target-owner, and administrator roles"]
fn live_external_quiesce_sequence_equality_executes_and_binds() -> anyhow::Result<()> {
    let base_source =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_RUN_SOURCE_CONFIG")?)?;
    let base_target =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_TARGET_CONFIG")?)?;
    let base_admin =
        PostgresEndpointConfig::read(required_path("SQL_SPLITTER_PG_FENCE_ADMIN_CONFIG")?)?;
    run_external_quiesce_sequence_equality(
        base_source,
        base_target,
        base_admin,
        "migration_mutator",
        "migration_reader",
        "migration_fence_target_owner",
        None,
    )?;
    Ok(())
}

fn run_external_quiesce_sequence_equality(
    base_source: PostgresEndpointConfig,
    base_target: PostgresEndpointConfig,
    base_admin: PostgresEndpointConfig,
    source_database_owner: &str,
    source_reader_role: &str,
    target_database_owner: &str,
    probe_profile: Option<PostgresSourceProfileKind>,
) -> anyhow::Result<Option<PostgresSourceProbeArtifact>> {
    let directory = private_tempdir()?;
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let source_database = format!("migration_external_source_{suffix}");
    let target_database = format!("migration_external_target_{suffix}");

    let mut control = connect(&base_admin)?;
    let source_database_sql = postgres_identifier(&source_database)?;
    let target_database_sql = postgres_identifier(&target_database)?;
    let source_database_owner_sql = postgres_identifier(source_database_owner)?;
    let source_reader_role_sql = postgres_identifier(source_reader_role)?;
    let target_database_owner_sql = postgres_identifier(target_database_owner)?;
    let cleanup = RecoveryDatabaseCleanup::new(
        base_admin.clone(),
        source_database.clone(),
        target_database.clone(),
    );
    control.batch_execute(&format!(
        "CREATE DATABASE {source_database_sql} OWNER {source_database_owner_sql}"
    ))?;
    control.batch_execute(&format!(
        "CREATE DATABASE {target_database_sql} OWNER {target_database_owner_sql}"
    ))?;
    let mut source = base_source.clone();
    source.database.clone_from(&source_database);
    let mut target = base_target.clone();
    target.database.clone_from(&target_database);
    let mut admin = base_admin.clone();
    admin.database.clone_from(&source_database);
    let mut setup = connect(&admin)?;
    setup.batch_execute(&format!(
        "REVOKE CREATE,TEMP ON DATABASE {source_database_sql} FROM PUBLIC; \
         REVOKE CREATE ON SCHEMA public FROM PUBLIC; \
         GRANT CONNECT ON DATABASE {source_database_sql} TO {source_reader_role_sql}; \
         GRANT USAGE ON SCHEMA public TO {source_reader_role_sql}; \
         CREATE TABLE public.external_rows (id bigint PRIMARY KEY, payload text NOT NULL); \
         INSERT INTO public.external_rows VALUES (1, 'one'), (2, 'two'), (3, 'three'); \
         CREATE SEQUENCE public.external_sequence AS bigint START WITH 7 CACHE 1; \
         SELECT pg_catalog.setval('public.external_sequence'::regclass, 41, true); \
         GRANT SELECT ON public.external_rows TO {source_reader_role_sql}; \
         GRANT SELECT ON SEQUENCE public.external_sequence TO {source_reader_role_sql}"
    ))?;
    drop(setup);

    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    let plan_path = directory.path().join("plan.json");
    let attestation_path = directory.path().join("external-quiesce.json");
    let state_path = directory.path().join("state.journal");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;

    let probe = probe_profile
        .map(|profile| {
            probe_live_postgres_source_profile(
                &source_path,
                &admin_path,
                profile,
                directory.path().join("source-profile-probe.json"),
            )
        })
        .transpose()?;

    let reviewed = write_live_plan_with_profile_tier(
        &source_path,
        &target_path,
        &plan_path,
        PostgresConsistencyMode::ConsistentSnapshot,
        None,
        None,
        Some(PostgresSourceProfileKind::AttestedExternalQuiesce),
        None,
        true,
    )?;
    anyhow::ensure!(
        !reviewed.plan.unsupported_objects.blocks_execution(),
        "external-quiesce plan unexpectedly blocks: {:#?}",
        reviewed.plan.unsupported_objects
    );
    assert_eq!(
        reviewed.plan.postgres_source_profile,
        Some(PostgresSourceProfileContract::AttestedExternalQuiesce {
            verified_rescan: true,
            freeze_enforced_by_tool: false,
        })
    );

    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let attestation = PostgresExternalQuiesceAttestation {
        schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
        source_endpoint_identity: reviewed.plan.source_endpoint_identity.clone(),
        source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        attestation_reference: format!("live-external-quiesce-{suffix}"),
        issued_at_unix_seconds: now.saturating_sub(1),
        expires_at_unix_seconds: now
            .checked_add(5)
            .context("external-quiesce expiry overflowed")?,
        status: PostgresExternalQuiesceStatus::Active,
    };
    attestation.require_active_at(now)?;
    write_json_new(&attestation_path, &attestation)?;

    let interrupted = execute_postgres_external_quiesce_plan_with_interruption(
        &plan_path,
        &source_path,
        &target_path,
        &attestation_path,
        "LIVE-EXTERNAL-QUIESCE",
        &state_path,
        PostgresExecutionInterruption::AfterCommittedChunks(1),
    )
    .expect_err("external execution did not interrupt");
    assert!(interrupted
        .to_string()
        .contains("injected interruption after a durable committed chunk"));

    let interrupted_journal = AppendJournal::open_resume(&state_path)?;
    assert_eq!(
        interrupted_journal.projection().status,
        MigrationStatus::Running
    );
    assert_eq!(
        interrupted_journal
            .genesis()
            .accepted_external_quiesce
            .as_ref(),
        Some(&attestation)
    );
    drop(interrupted_journal);

    let withdrawn_path = directory.path().join("external-quiesce-withdrawn.json");
    let mut withdrawn = attestation.clone();
    withdrawn.status = PostgresExternalQuiesceStatus::Withdrawn;
    write_json_new(&withdrawn_path, &withdrawn)?;
    let withdrawal_error = resume_postgres_plan_with_external_quiesce(
        &state_path,
        &source_path,
        &target_path,
        &withdrawn_path,
    )
    .expect_err("withdrawn external-quiesce evidence resumed execution");
    assert!(withdrawal_error.to_string().contains("withdrawn"));

    let mut source_admin = connect(&admin)?;
    source_admin.query_one(
        "SELECT pg_catalog.setval('public.external_sequence'::regclass, 42, true)",
        &[],
    )?;
    let drift_error = resume_postgres_plan_with_external_quiesce(
        &state_path,
        &source_path,
        &target_path,
        &attestation_path,
    )
    .expect_err("source sequence drift resumed execution");
    let reviewed_sequence_id = postgres_sequences(
        reviewed
            .plan
            .source_catalog
            .as_ref()
            .context("reviewed plan has no source catalog")?,
    )?[0]
        .catalog_object_id
        .clone();
    assert!(drift_error
        .to_string()
        .contains("resumed source sequence contracts changed"));
    assert!(drift_error.to_string().contains(&reviewed_sequence_id));
    source_admin.query_one(
        "SELECT pg_catalog.setval('public.external_sequence'::regclass, 41, true)",
        &[],
    )?;
    drop(source_admin);

    let sleep_seconds = attestation
        .expires_at_unix_seconds
        .saturating_sub(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
        .saturating_add(1);
    thread::sleep(Duration::from_secs(sleep_seconds));
    let report = resume_postgres_plan_with_external_quiesce(
        &state_path,
        &source_path,
        &target_path,
        &attestation_path,
    )?;
    assert_eq!(report.state, state_path);
    assert_eq!(report.copied_rows, 3);

    let journal = AppendJournal::open_resume(&state_path)?;
    assert_eq!(journal.projection().status, MigrationStatus::Completed);
    assert_eq!(
        journal.genesis().accepted_external_quiesce.as_ref(),
        Some(&attestation)
    );
    let attestation_digest = attestation.canonical_hash()?;
    assert_eq!(
        journal
            .genesis()
            .binding
            .external_quiesce_attestation_digest
            .as_deref(),
        Some(attestation_digest.as_str())
    );
    let equality = journal
        .projection()
        .sequence_equality
        .as_ref()
        .context("completed sequence migration has no equality evidence")?;
    equality.validate()?;
    assert_eq!(equality.initial, equality.final_observation);
    assert_eq!(equality.initial.len(), 1);
    assert_eq!(equality.initial[0].cache_size, 1);
    assert_eq!(equality.initial[0].last_value, 41);
    assert!(equality.initial[0].is_called);
    let rescan = journal
        .projection()
        .external_quiesce_rescan
        .as_ref()
        .context("verified external-quiesce plan has no fresh re-scan evidence")?;
    rescan.validate()?;
    assert_eq!(rescan.tables.len(), 1);
    assert_eq!(rescan.tables[0].row_count, 3);
    assert_eq!(
        rescan.tables[0].fresh_source_hash,
        rescan.tables[0].target_hash
    );

    let target_snapshot = inspect_endpoint(&target)?;
    let target_sequences = postgres_sequences(&target_snapshot.catalog)?;
    assert_eq!(target_sequences.len(), 1);
    assert_eq!(target_sequences[0].name.as_str(), "external_sequence");
    assert_eq!(target_sequences[0].cache_size, 1);
    assert_eq!(target_sequences[0].last_value, 41);
    assert!(target_sequences[0].is_called);
    let mut target_client = connect(&target)?;
    let rows = target_client.query(
        "SELECT id,payload FROM public.external_rows ORDER BY id",
        &[],
    )?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[2].get::<_, String>(1), "three");
    drop(target_client);

    cleanup.run()?;
    Ok(probe)
}

#[test]
#[cfg(feature = "migration-fault-injection")]
#[ignore = "requires the dedicated Amazon RDS PostgreSQL Phase 5b evidence endpoint"]
fn live_managed_provider_phase5b_matrix() -> anyhow::Result<()> {
    let admin_path = required_path("SQL_SPLITTER_PG_PROVIDER_ADMIN_CONFIG")?;
    let base_admin = PostgresEndpointConfig::read(admin_path)?;
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let reader_role = format!("sqlspl_provider_reader_{suffix}");
    let target_role = format!("sqlspl_provider_target_{suffix}");
    let reader_password = hex::encode(rand::random::<[u8; 32]>());
    let target_password = hex::encode(rand::random::<[u8; 32]>());
    let reader_password_env = format!("SQL_SPLITTER_PROVIDER_READER_{}", suffix.to_uppercase());
    let target_password_env = format!("SQL_SPLITTER_PROVIDER_TARGET_{}", suffix.to_uppercase());
    let reader_environment = EnvironmentVariableGuard::set(&reader_password_env, &reader_password);
    let target_environment = EnvironmentVariableGuard::set(&target_password_env, &target_password);

    let mut control = connect(&base_admin)?;
    let provider: bool = control
        .query_one(
            "SELECT NOT rolsuper AND pg_has_role(current_user,'rds_superuser','MEMBER') FROM pg_roles WHERE rolname=current_user",
            &[],
        )?
        .get(0);
    anyhow::ensure!(provider, "administrator is not an RDS administrator role");
    let server_version_num: i32 = control
        .query_one("SELECT current_setting('server_version_num')::integer", &[])?
        .get(0);
    anyhow::ensure!(
        (160_000..170_000).contains(&server_version_num),
        "provider matrix requires PostgreSQL 16"
    );
    let server_version: String = control
        .query_one("SELECT current_setting('server_version')", &[])?
        .get(0);

    let cleanup = ProviderRoleCleanup::new(
        base_admin.clone(),
        vec![reader_role.clone(), target_role.clone()],
    );
    control.batch_execute(&format!(
        "CREATE ROLE {} LOGIN PASSWORD '{}'; CREATE ROLE {} LOGIN PASSWORD '{}'; GRANT {} TO {}",
        postgres_identifier(&reader_role)?,
        reader_password,
        postgres_identifier(&target_role)?,
        target_password,
        postgres_identifier(&target_role)?,
        postgres_identifier(&base_admin.user)?,
    ))?;
    drop(control);

    let mut source = base_admin.clone();
    source.user.clone_from(&reader_role);
    source.credential_env.clone_from(&reader_password_env);
    let mut target = base_admin.clone();
    target.user.clone_from(&target_role);
    target.credential_env.clone_from(&target_password_env);
    let probe = run_external_quiesce_sequence_equality(
        source,
        target,
        base_admin.clone(),
        &base_admin.user,
        &reader_role,
        &target_role,
        Some(PostgresSourceProfileKind::ManagedAdministrator),
    )?
    .context("managed-provider matrix did not produce probe evidence")?;
    probe.validate()?;
    probe.require_all_proven()?;

    println!(
        "provider=amazon-rds engine=postgresql server_version={server_version} admin_role=rds_superuser-member"
    );
    for result in &probe.results {
        let status = match &result.status {
            PostgresSourceProbeStatus::Proven => "proven",
            PostgresSourceProbeStatus::Unavailable { .. } => "unavailable",
        };
        println!("probe={:?} status={status}", result.requirement);
    }

    cleanup.run()?;
    drop(target_environment);
    drop(reader_environment);
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
    let interrupted_error = interrupted.expect_err("execution must stop at the injected boundary");
    assert!(
        interrupted_error
            .to_string()
            .contains("injected interruption"),
        "unexpected interrupted execution error: {interrupted_error:#}"
    );
    let interrupted_state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(
        interrupted_state.projection().status,
        MigrationStatus::Running
    );
    drop(interrupted_state);

    let report = resume_postgres_fenced_plan(&state_path, &source, &target, &admin, &artifact)
        .context("resume the reviewed plan under the durable write fence")?;
    assert_eq!(report.copied_rows, 3);
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);

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

    let directory = private_tempdir()?;
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
    let directory = private_tempdir()?;
    let interruptions = [
        PostgresExecutionInterruption::AfterDdlPrepared,
        PostgresExecutionInterruption::AfterDdlCommitted,
        PostgresExecutionInterruption::AfterChunkPrepared,
        PostgresExecutionInterruption::CommitUnknownAfterApply,
        PostgresExecutionInterruption::TornChunkCommittedEnospc,
        PostgresExecutionInterruption::ChunkCommittedSyncAckLost,
        PostgresExecutionInterruption::AfterPipelinedEvidence,
        PostgresExecutionInterruption::AfterAllVerified,
        PostgresExecutionInterruption::AfterFenceReleased,
    ];

    let mut control = connect(&base_admin)?;
    for (index, interruption) in interruptions.into_iter().enumerate() {
        eprintln!("running recovery interruption {interruption:?}");
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
            "REVOKE CREATE,TEMP ON DATABASE {source_database} FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT CONNECT ON DATABASE {source_database} TO migration_reader; GRANT USAGE ON SCHEMA public TO migration_reader; CREATE TABLE public.accounts (id bigint PRIMARY KEY, name text NOT NULL); INSERT INTO public.accounts VALUES (1, 'one'), (2, 'two'), (3, 'three'); CREATE FUNCTION public.double_id(value bigint) RETURNS bigint LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE RETURN value * 2; CREATE VIEW public.account_values AS SELECT id, public.double_id(id) AS doubled FROM public.accounts; GRANT SELECT ON public.accounts TO migration_reader"
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
        let state_path = case.join("state.journal");
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
        match interruption {
            PostgresExecutionInterruption::TornChunkCommittedEnospc => assert!(
                format!("{error:#}").contains("No space left on device"),
                "{interruption:?}: {error:#}"
            ),
            PostgresExecutionInterruption::ChunkCommittedSyncAckLost => assert!(
                format!("{error:#}").contains("Input/output error"),
                "{interruption:?}: {error:#}"
            ),
            _ => assert!(
                error.to_string().contains("injected interruption"),
                "{interruption:?}: {error:#}"
            ),
        }
        if interruption == PostgresExecutionInterruption::TornChunkCommittedEnospc {
            let prepared = AppendJournal::open_resume(&state_path)?;
            assert!(prepared.projection().prepared_chunk.is_some());
            assert_eq!(prepared.projection().last_chunk_id, 0);
            assert!(prepared.projection().copy_cursors.is_empty());
            drop(prepared);
        }
        if interruption == PostgresExecutionInterruption::ChunkCommittedSyncAckLost {
            let committed = AppendJournal::open_resume(&state_path)?;
            assert!(committed.projection().prepared_chunk.is_none());
            assert_eq!(committed.projection().last_chunk_id, 1);
            assert_eq!(
                committed
                    .projection()
                    .copy_cursors
                    .values()
                    .map(|cursor| cursor.rows)
                    .sum::<u64>(),
                3
            );
            drop(committed);
        }
        if interruption == PostgresExecutionInterruption::AfterPipelinedEvidence {
            let pipelined = AppendJournal::open_resume(&state_path)?;
            assert!(pipelined.projection().prepared_chunk.is_none());
            assert!(pipelined.projection().last_chunk_id > 0);
            assert!(!pipelined.projection().copy_cursors.is_empty());
            assert!(pipelined.projection().table_verifications.is_empty());
            assert!(!pipelined.projection().schema_verified);
            drop(pipelined);
        }

        let report = resume_postgres_fenced_plan(
            &state_path,
            &source_path,
            &target_path,
            &admin_path,
            &fence_path,
        )?;
        assert_eq!(report.copied_rows, 3, "{interruption:?}");
        let state = AppendJournal::open_resume(&state_path)?;
        assert_eq!(
            state.projection().status,
            MigrationStatus::Completed,
            "{interruption:?}"
        );
        let copied_rows = committed_chunks(&state)?
            .try_fold(0_u64, |sum, chunk| chunk.map(|chunk| sum + chunk.row_count))?;
        assert_eq!(copied_rows, 3, "{interruption:?}");
        assert!(state
            .projection()
            .operations
            .values()
            .all(|state| *state == OperationState::Verified));
        if interruption == PostgresExecutionInterruption::AfterPipelinedEvidence {
            assert!(!state.projection().table_verifications.is_empty());
            assert!(state.projection().schema_verified);
        }
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
        let view_rows = target_client.query(
            "SELECT id,doubled FROM public.account_values ORDER BY id",
            &[],
        )?;
        assert_eq!(
            view_rows
                .iter()
                .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))
                .collect::<Vec<_>>(),
            vec![(1, 2), (2, 4), (3, 6)]
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
                let state = AppendJournal::read_snapshot(&state_path)?;
                let transaction_is_active: bool = observer
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=$1 AND usename='migration_fence_target_owner' AND xact_start IS NOT NULL AND (query LIKE 'COPY %' OR query LIKE 'INSERT INTO%'))",
                        &[&target_database],
                    )?
                    .get(0);
                if state.is_some_and(|snapshot| snapshot.projection.prepared_chunk.is_some())
                    && transaction_is_active
                {
                    break;
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "target COPY or INSERT transaction did not become observable before timeout"
                );
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

    let state = AppendJournal::open_resume(&state_path)?;
    assert!(state.projection().prepared_chunk.is_some());
    assert_eq!(state.projection().last_chunk_id, 0);
    assert!(state.projection().copy_cursors.is_empty());
    drop(state);
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
    let completed = AppendJournal::open_resume(&state_path)?;
    assert_eq!(completed.projection().status, MigrationStatus::Completed);
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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
    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
    if strategy == "range" {
        let mut source_admin = connect(&admin)?;
        source_admin.batch_execute(
            "SET ROLE migration_mutator;
             ALTER INDEX public.accounts_low_pkey RENAME TO accounts_low_renamed_pkey;
             RESET ROLE",
        )?;
        drop(source_admin);
        let renamed_path = directory.path().join("renamed-index-plan.json");
        let blocked = write_unbudgeted_live_plan_with_consistency(
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
                    && object.code == UnsupportedObjectCode::PartitionChildIndexName
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
        let blocked = write_unbudgeted_live_plan_with_consistency(
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
                object.required_semantics
                    && object.code == UnsupportedObjectCode::PartitionLocalIndex
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
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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
    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
        let state = AppendJournal::open_resume(&state_path)?;
        assert_eq!(
            state.projection().status,
            MigrationStatus::ManualReconciliationRequired
        );
        cleanup.run()?;
        return Ok(());
    }
    resumed?;
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
        .find(|operation| {
            operation.kind == sql_splitter::migration::plan::OperationKind::CopyTable
                && operation
                    .table
                    .as_ref()
                    .is_some_and(|table| table.name.as_str() == "accounts")
        })
        .ok_or_else(|| anyhow::anyhow!("accounts CopyTable operation is absent"))?;
    assert_eq!(
        serde_json::from_value::<PostgresWritePolicy>(
            copy.parameters["postgres_write_policy"].clone()
        )?,
        PostgresWritePolicy::PlainInsertIdentityAlwaysV1
    );
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
    assert_target_table_used_insert_not_copy(&mut control, &target_database, "accounts")?;
    if inject_conflict {
        let error = resumed.unwrap_err();
        assert!(
            error.to_string().contains("manual intervention"),
            "{error:#}"
        );
        let state = AppendJournal::open_resume(&state_path)?;
        assert_eq!(
            state.projection().status,
            MigrationStatus::ManualReconciliationRequired
        );
        cleanup.run()?;
        return Ok(());
    }
    resumed?;
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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
            .query_one(
                "SELECT last_value,is_called FROM public.accounts_id_seq",
                &[],
            )
            .map(|row| (row.get::<_, i64>(0), row.get::<_, bool>(1)))?,
        (115, true)
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
        let state = AppendJournal::open_resume(&state_path)?;
        let index_operation = state
            .projection()
            .operations
            .get(index_operation.id.as_str())
            .ok_or_else(|| anyhow::anyhow!("ordinary index state is absent"))?;
        assert_eq!(*index_operation, OperationState::Prepared);
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
        let state = AppendJournal::open_resume(&state_path)?;
        assert_eq!(
            state.projection().status,
            MigrationStatus::ManualReconciliationRequired
        );
        cleanup.run()?;
        return Ok(());
    }
    resume?;
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&proxied_target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
    let state = AppendJournal::open_resume(state_path)?;
    assert!(state.projection().prepared_chunk.is_some());
    assert_eq!(state.projection().last_chunk_id, 0);
    assert!(state.projection().copy_cursors.is_empty());
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn assert_completed_exactly_once(
    state_path: &std::path::Path,
    target: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let state = AppendJournal::open_resume(state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert_eq!(committed_chunks(&state)?.count(), 1);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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

fn committed_chunks(
    journal: &AppendJournal,
) -> anyhow::Result<
    impl Iterator<
        Item = Result<PreparedChunk, sql_splitter::migration::append_journal::AppendJournalError>,
    >,
> {
    Ok(journal.all_committed_chunks()?)
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(state.projection().status, MigrationStatus::Completed);
    assert!(state
        .projection()
        .operations
        .values()
        .all(|state| *state == OperationState::Verified));
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

    let directory = private_tempdir()?;
    let source_path = directory.path().join("source.toml");
    let target_path = directory.path().join("target.toml");
    let admin_path = directory.path().join("admin.toml");
    std::fs::write(&source_path, toml::to_string(&source)?)?;
    std::fs::write(&target_path, toml::to_string(&target)?)?;
    std::fs::write(&admin_path, toml::to_string(&admin)?)?;
    let plan_path = directory.path().join("plan.json");
    let fence_path = directory.path().join("fence.json");
    let state_path = directory.path().join("state.journal");
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
    let state = AppendJournal::open_resume(&state_path)?;
    assert_eq!(
        state.projection().status,
        MigrationStatus::ManualReconciliationRequired
    );
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
    let sequence_id = "sequence-accounts-id";
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
    let identity_sequence = CatalogObject {
        id: sequence_id.into(),
        kind: CatalogObjectKind::Sequence,
        name: Identifier::new("accounts_id_seq")?,
        definition: Vec::new(),
        attributes: BTreeMap::from([
            ("relkind".into(), serde_json::json!("S")),
            ("persistence".into(), serde_json::json!("p")),
            ("type".into(), serde_json::json!("bigint")),
            ("start".into(), serde_json::json!("1")),
            ("increment".into(), serde_json::json!("1")),
            ("minimum".into(), serde_json::json!("1")),
            ("maximum".into(), serde_json::json!(i64::MAX.to_string())),
            ("cache".into(), serde_json::json!("1")),
            ("cycle".into(), serde_json::json!(false)),
            ("last_value".into(), serde_json::json!("1")),
            ("is_called".into(), serde_json::json!(false)),
            ("ownership_count".into(), serde_json::json!(1)),
            (
                "ownership".into(),
                serde_json::to_value(PostgresSequenceOwnership {
                    table: QualifiedTable {
                        namespace: Identifier::new("public")?,
                        name: Identifier::new("accounts")?,
                    },
                    column: Identifier::new("id")?,
                    kind: PostgresSequenceOwnershipKind::IdentityAlways,
                })?,
            ),
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
                identity_sequence,
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

#[cfg(feature = "migration-fault-injection")]
fn assert_target_table_used_insert_not_copy(
    admin: &mut Client,
    database: &str,
    table: &str,
) -> anyhow::Result<()> {
    let insert_marker = format!("INSERT INTO \"public\".\"{table}\"");
    let copy_marker = format!("COPY \"public\".\"{table}\"");
    for _ in 0..40 {
        let path: String = admin
            .query_one("SELECT pg_current_logfile('jsonlog')", &[])?
            .get(0);
        let log: String = admin.query_one("SELECT pg_read_file($1)", &[&path])?.get(0);
        let statements = log
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| {
                entry.get("dbname").and_then(serde_json::Value::as_str) == Some(database)
            })
            .filter_map(|entry| {
                entry
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        assert!(
            statements
                .iter()
                .all(|statement| !statement.contains(&copy_marker)),
            "identity-ALWAYS table was written through COPY: {statements:?}"
        );
        if statements
            .iter()
            .any(|statement| statement.contains(&insert_marker))
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    anyhow::bail!("no INSERT statement for identity-ALWAYS table reached the PostgreSQL log")
}

fn assert_assessment_statement_log(
    admin: &mut Client,
    expected_sessions: usize,
) -> anyhow::Result<()> {
    let mut last_session_count = 0;
    for _ in 0..40 {
        let path: String = admin
            .query_one("SELECT pg_current_logfile('jsonlog')", &[])?
            .get(0);
        let log: String = admin.query_one("SELECT pg_read_file($1)", &[&path])?.get(0);
        let mut sessions = BTreeMap::<String, Vec<String>>::new();
        let mut errors = Vec::new();
        for line in log.lines().filter(|line| !line.trim().is_empty()) {
            let entry: serde_json::Value = serde_json::from_str(line)?;
            if entry
                .get("application_name")
                .and_then(|value| value.as_str())
                != Some("sql-splitter-migration-assessment")
            {
                continue;
            }
            if entry
                .get("error_severity")
                .and_then(|value| value.as_str())
                .is_some_and(|severity| severity == "ERROR" || severity == "FATAL")
            {
                errors.push(entry.to_string());
            }
            let Some(message) = entry.get("message").and_then(|value| value.as_str()) else {
                continue;
            };
            let statement = message.strip_prefix("statement: ").or_else(|| {
                message
                    .strip_prefix("execute ")
                    .and_then(|message| message.split_once(": ").map(|(_, sql)| sql))
            });
            let Some(statement) = statement else {
                continue;
            };
            let session_id = entry
                .get("session_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| anyhow::anyhow!("assessment log entry has no session_id"))?;
            sessions
                .entry(session_id.to_owned())
                .or_default()
                .push(statement.trim().to_owned());
        }
        last_session_count = sessions.len();
        if sessions.len() < expected_sessions {
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        assert_eq!(sessions.len(), expected_sessions);
        assert!(errors.is_empty(), "assessment logged errors: {errors:?}");
        for statements in sessions.values() {
            assert_assessment_statement_set(statements)?;
            assert_eq!(
                statements
                    .iter()
                    .filter(|statement| {
                        statement.starts_with("BEGIN") || statement.starts_with("START TRANSACTION")
                    })
                    .count(),
                1,
                "assessment transaction did not have one BEGIN: {statements:?}"
            );
            assert_eq!(
                statements
                    .iter()
                    .filter(|statement| statement.as_str() == "COMMIT")
                    .count(),
                1,
                "assessment transaction did not have one COMMIT: {statements:?}"
            );
        }
        return Ok(());
    }
    anyhow::bail!(
        "only {last_session_count} assessment sessions reached the PostgreSQL JSON statement log"
    )
}

fn assert_assessment_statement_set(statements: &[String]) -> anyhow::Result<()> {
    const EXPECTED_COUNT: usize = 23;
    const EXPECTED_SHA256: &str =
        "4cc4c6a80b1bcfe97fbda2fad4870abfc2feba72558d49a7f73bafaf166566eb";

    let mut digest = Sha256::new();
    for statement in statements {
        digest.update((statement.len() as u64).to_be_bytes());
        digest.update(statement.as_bytes());
    }
    let actual = hex::encode(digest.finalize());
    if statements.len() != EXPECTED_COUNT || actual != EXPECTED_SHA256 {
        anyhow::bail!(
            "assessment statement set changed: count={} sha256={actual}",
            statements.len()
        );
    }
    Ok(())
}

#[test]
fn assessment_statement_audit_rejects_mutating_select() {
    let statements = vec!["SELECT lo_create(0)".to_owned()];
    assert!(assert_assessment_statement_set(&statements).is_err());
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
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            postgres_identifier(&self.source_database)?
        ))?;
        client.batch_execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            postgres_identifier(&self.target_database)?
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

struct ProviderRoleCleanup {
    control: PostgresEndpointConfig,
    roles: Vec<String>,
    armed: bool,
}

impl ProviderRoleCleanup {
    fn new(control: PostgresEndpointConfig, roles: Vec<String>) -> Self {
        Self {
            control,
            roles,
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
        for role in self.roles.iter().rev() {
            client.batch_execute(&format!(
                "DROP ROLE IF EXISTS {}",
                postgres_identifier(role)?
            ))?;
        }
        Ok(())
    }
}

impl Drop for ProviderRoleCleanup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.cleanup();
        }
    }
}

struct EnvironmentVariableGuard {
    name: String,
    previous: Option<std::ffi::OsString>,
}

impl EnvironmentVariableGuard {
    fn set(name: &str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self {
            name: name.into(),
            previous,
        }
    }
}

impl Drop for EnvironmentVariableGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            std::env::set_var(&self.name, previous);
        } else {
            std::env::remove_var(&self.name);
        }
    }
}

fn postgres_identifier(value: &str) -> anyhow::Result<String> {
    Identifier::new(value)?;
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn required_path(name: &str) -> anyhow::Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("{name} must name a PostgreSQL endpoint config"))
}

fn write_live_execution_plan(
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency: PostgresConsistencyMode,
) -> anyhow::Result<ReviewedPlan> {
    let source_config_path = source_config_path.as_ref();
    let target_config_path = target_config_path.as_ref();
    let output = output.as_ref();
    let source_config = PostgresEndpointConfig::read(source_config_path)?;
    let source = inspect_endpoint(&source_config)?;
    let measured_at_unix_seconds = u64::try_from(
        connect(&source_config)?
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint",
                &[],
            )?
            .get::<_, i64>(0),
    )?;
    let profile = ThroughputProfile {
        schema_version: THROUGHPUT_PROFILE_SCHEMA_VERSION,
        measurement_reference: "live-test-profile".into(),
        environment_reference: "disposable-postgres-live-test".into(),
        postgres_major_version: u16::try_from(source.server_version_num / 10_000)?,
        measured_at_unix_seconds,
        valid_for_seconds: 3_600,
        copy_bytes_per_second: 1,
        verification_bytes_per_second: 1,
    };
    let assessment = collect_live_assessment_with_profile(&source_config, Some(&profile))?;
    let assessment_path = output.with_extension("assessment.json");
    write_json_new(&assessment_path, &assessment)?;
    Ok(write_live_plan_with_outage_policy(
        source_config_path,
        target_config_path,
        output,
        consistency,
        Some(&assessment_path),
        Some(u64::MAX),
    )?)
}

fn write_live_plan_with_consistency(
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency: PostgresConsistencyMode,
) -> anyhow::Result<ReviewedPlan> {
    write_live_execution_plan(source_config_path, target_config_path, output, consistency)
}

fn private_tempdir() -> anyhow::Result<tempfile::TempDir> {
    Ok(tempfile::Builder::new()
        .prefix("migration-postgres-")
        .tempdir_in(std::env::current_dir()?)?)
}
