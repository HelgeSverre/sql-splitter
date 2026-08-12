//! Shared journaled execution for reviewed PostgreSQL↔MySQL conversions.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Context};
use sha2::{Digest, Sha256};

use super::append_journal::{
    AppendJournal, CommittedChunkIter, Genesis, OperationPhase, OperationSpec, PreparedChunk,
};
use super::artifact::read_json;
use super::canonical::{digest_rows, CanonicalRow, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, ConnectionError, KeyTuple, KeysetPage, ReadSession, SourceConnectionFactory,
    TargetConnectionFactory,
};
use super::conversion::{ConversionDialect, MigrationConversionMode, TableConversionPolicy};
use super::journal::{ConsistencyEvidence, MigrationStatus, OperationState, ResumeBinding};
use super::model::{DbValue, Identifier, QualifiedTable, RowBatch, VendorCatalog};
use super::mysql::{
    attest_mysql_external_freeze, collect_mysql_metadata_visibility, mysql_catalog_fingerprint,
    mysql_catalog_visibility_is_complete, mysql_tls_binding,
    validate_mysql_external_freeze_continuity, MySqlCatalogSnapshot, MySqlEndpointConfig,
    MySqlSourceFactory, MySqlTableState, MySqlTargetFactory,
};
use super::mysql_execution::{
    attest_current_mysql_source_visibility, mysql_execution_snapshot_binding_is_exact,
};
use super::mysql_profile::{MySqlExternalFreezeAssertion, MySqlExternalFreezeAttestation};
use super::plan::{OperationKind, PlanOperation, ReviewedPlan};
use super::postgres::{
    catalog_fingerprint, postgres_tls_binding, PostgresConversionTableState,
    PostgresEndpointConfig, PostgresTargetFactory,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrossTableState {
    Absent,
    Exact,
    Different,
}

trait CrossDialectTarget: TargetConnectionFactory {
    fn inspect_cross_table(
        &self,
        policy: &TableConversionPolicy,
    ) -> Result<CrossTableState, ConnectionError>;

    fn create_cross_table(&self, policy: &TableConversionPolicy) -> Result<(), ConnectionError>;

    fn verify_cross_schema(
        &self,
        policies: &[TableConversionPolicy],
    ) -> Result<String, ConnectionError>;
}

impl CrossDialectTarget for MySqlTargetFactory {
    fn inspect_cross_table(
        &self,
        policy: &TableConversionPolicy,
    ) -> Result<CrossTableState, ConnectionError> {
        Ok(match self.inspect_conversion_table(policy)? {
            MySqlTableState::Absent => CrossTableState::Absent,
            MySqlTableState::Exact => CrossTableState::Exact,
            MySqlTableState::Different => CrossTableState::Different,
        })
    }

    fn create_cross_table(&self, policy: &TableConversionPolicy) -> Result<(), ConnectionError> {
        self.create_conversion_table(policy)
    }

    fn verify_cross_schema(
        &self,
        policies: &[TableConversionPolicy],
    ) -> Result<String, ConnectionError> {
        self.verify_conversion_schema(policies)
    }
}

impl CrossDialectTarget for PostgresTargetFactory {
    fn inspect_cross_table(
        &self,
        policy: &TableConversionPolicy,
    ) -> Result<CrossTableState, ConnectionError> {
        Ok(match self.inspect_conversion_table(policy)? {
            PostgresConversionTableState::Absent => CrossTableState::Absent,
            PostgresConversionTableState::Exact => CrossTableState::Exact,
            PostgresConversionTableState::Different => CrossTableState::Different,
        })
    }

    fn create_cross_table(&self, policy: &TableConversionPolicy) -> Result<(), ConnectionError> {
        self.create_conversion_table(policy)
    }

    fn verify_cross_schema(
        &self,
        policies: &[TableConversionPolicy],
    ) -> Result<String, ConnectionError> {
        self.verify_conversion_schema(policies)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossDialectExecutionReport {
    pub state: PathBuf,
    pub copied_rows: u64,
    pub committed_chunks: u64,
}

struct MySqlPostgresCancellationMonitor {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl MySqlPostgresCancellationMonitor {
    fn start(
        source: Arc<MySqlSourceFactory>,
        target: Arc<PostgresTargetFactory>,
        cancellation: CancellationToken,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) && !cancellation.is_cancelled() {
                thread::sleep(Duration::from_millis(10));
            }
            if worker_stop.load(Ordering::Acquire) {
                return;
            }
            let mut errors = Vec::new();
            for (label, control) in [
                ("MySQL source", source.open_control()),
                ("PostgreSQL target", target.open_control()),
            ] {
                match control {
                    Ok(mut control) => {
                        if let Err(error) = control.cancel_active_statement() {
                            if !matches!(error, ConnectionError::InvalidRequest(_)) {
                                errors.push(format!("{label}: {error}"));
                            }
                        }
                    }
                    Err(error) if !matches!(error, ConnectionError::InvalidRequest(_)) => {
                        errors.push(format!("{label}: {error}"));
                    }
                    Err(_) => {}
                }
            }
            if !errors.is_empty() {
                cancellation.record_control_error(errors.join("; "));
            }
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for MySqlPostgresCancellationMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn execute_mysql_to_postgres_plan(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    source_metadata_config_path: impl AsRef<Path>,
    freeze_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    freeze_assertion_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
) -> anyhow::Result<CrossDialectExecutionReport> {
    if approval_reference.trim().is_empty() {
        return Err(anyhow!("approval reference must not be empty"));
    }
    let reviewed: ReviewedPlan = read_json(plan_path)?;
    validate_mysql_postgres_plan(&reviewed)?;
    let source_config = MySqlEndpointConfig::read(source_config_path.as_ref())?;
    let source_metadata_config = MySqlEndpointConfig::read(source_metadata_config_path.as_ref())?;
    let freeze_config = MySqlEndpointConfig::read(freeze_config_path.as_ref())?;
    let target_config = PostgresEndpointConfig::read(target_config_path.as_ref())?;
    validate_mysql_postgres_credentials(
        &source_config,
        &source_metadata_config,
        &freeze_config,
        &target_config,
    )?;
    validate_cross_tls(&reviewed, &source_config, &target_config)?;
    let assertion: MySqlExternalFreezeAssertion = read_json(freeze_assertion_path)?;
    let accepted = attest_mysql_external_freeze(&freeze_config, &reviewed, &assertion)?;
    validate_cross_mysql_freeze(&reviewed, &accepted)?;
    let cancellation = CancellationToken::default();
    cancellation.observe_process_sigint()?;
    let source = Arc::new(MySqlSourceFactory::new_with_cancellation(
        source_config.clone(),
        cancellation.clone(),
    ));
    let policies = cross_policies(&reviewed)?;
    let target_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow!("cross-dialect plan has no target catalog"))?
        .clone();
    let target = Arc::new(PostgresTargetFactory::new_cross_dialect_with_cancellation(
        target_config.clone(),
        &target_catalog,
        policies,
        cancellation.clone(),
    )?);
    let _monitor = MySqlPostgresCancellationMonitor::start(
        Arc::clone(&source),
        Arc::clone(&target),
        cancellation.clone(),
    );
    source.capabilities().require(&[
        "consistent_snapshot",
        "server_read_only",
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
    ])?;
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
        "plain_insert",
    ])?;
    let (mut reader, _) = capture_cross_mysql_source(
        &reviewed,
        source.as_ref(),
        &source_metadata_config,
        &freeze_config,
        &cancellation,
    )?;
    attest_initial_postgres_target(&reviewed, target.as_ref())?;
    target.assert_empty_and_owned()?;
    let binding = cross_mysql_resume_binding(&reviewed, &accepted, approval_reference)?;
    let accepted_for_checks = accepted.clone();
    let mut journal = AppendJournal::create_new(
        state_path.as_ref(),
        cross_journal_genesis(binding, reviewed.clone(), accepted),
    )?;
    let mut require_source_consistency = || {
        require_current_cross_mysql_source_consistency(
            &reviewed,
            &accepted_for_checks,
            &source_config,
            &source_metadata_config,
            &freeze_config,
            &assertion,
            &cancellation,
        )
    };
    execute_cross_dialect_journal(
        &reviewed,
        reader.as_mut(),
        target.as_ref(),
        &mut journal,
        &cancellation,
        &mut require_source_consistency,
    )?;
    require_source_consistency()?;
    journal.transition_status(MigrationStatus::CompletedWithApprovedTransformations)?;
    cross_report(state_path.as_ref(), &journal)
}

#[allow(clippy::too_many_arguments)]
pub fn resume_mysql_to_postgres_plan(
    state_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    source_metadata_config_path: impl AsRef<Path>,
    freeze_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    freeze_assertion_path: impl AsRef<Path>,
) -> anyhow::Result<CrossDialectExecutionReport> {
    let mut journal = AppendJournal::open_resume(state_path.as_ref())?;
    let reviewed = journal.reviewed_plan().clone();
    validate_mysql_postgres_plan(&reviewed)?;
    validate_cross_resume_binding(&journal.genesis().binding, &reviewed)?;
    if matches!(
        journal.projection().status,
        MigrationStatus::Completed | MigrationStatus::CompletedWithApprovedTransformations
    ) {
        return Err(anyhow!(
            "completed cross-dialect migration cannot be resumed"
        ));
    }
    if journal.projection().status == MigrationStatus::ManualReconciliationRequired {
        return Err(anyhow!(
            "cross-dialect migration requires manual reconciliation"
        ));
    }
    let source_config = MySqlEndpointConfig::read(source_config_path.as_ref())?;
    let source_metadata_config = MySqlEndpointConfig::read(source_metadata_config_path.as_ref())?;
    let freeze_config = MySqlEndpointConfig::read(freeze_config_path.as_ref())?;
    let target_config = PostgresEndpointConfig::read(target_config_path.as_ref())?;
    validate_mysql_postgres_credentials(
        &source_config,
        &source_metadata_config,
        &freeze_config,
        &target_config,
    )?;
    validate_cross_tls(&reviewed, &source_config, &target_config)?;
    let assertion: MySqlExternalFreezeAssertion = read_json(freeze_assertion_path)?;
    let accepted = attest_mysql_external_freeze(&freeze_config, &reviewed, &assertion)?;
    validate_mysql_external_freeze_continuity(
        journal
            .accepted_mysql_freeze()
            .ok_or_else(|| anyhow!("cross-dialect journal has no accepted MySQL freeze"))?,
        &accepted,
    )
    .context("validate durable MySQL freeze continuity before resume")?;
    let cancellation = CancellationToken::default();
    cancellation.observe_process_sigint()?;
    let source = Arc::new(MySqlSourceFactory::new_with_cancellation(
        source_config.clone(),
        cancellation.clone(),
    ));
    let policies = cross_policies(&reviewed)?;
    let target_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow!("cross-dialect plan has no target catalog"))?
        .clone();
    let target = Arc::new(PostgresTargetFactory::new_cross_dialect_with_cancellation(
        target_config,
        &target_catalog,
        policies,
        cancellation.clone(),
    )?);
    let _monitor = MySqlPostgresCancellationMonitor::start(
        Arc::clone(&source),
        Arc::clone(&target),
        cancellation.clone(),
    );
    let (mut reader, _) = capture_cross_mysql_source(
        &reviewed,
        source.as_ref(),
        &source_metadata_config,
        &freeze_config,
        &cancellation,
    )?;
    let target_snapshot = target.inspect_endpoint()?;
    if target_snapshot.endpoint_identity != reviewed.plan.execution_target_endpoint_identity()?
        || target_snapshot.server_version
            != reviewed
                .plan
                .target_catalog
                .as_assessed()
                .ok_or_else(|| anyhow!("cross-dialect plan has no target catalog"))?
                .server_version
        || target_snapshot.tls_binding
            != *reviewed
                .plan
                .target_tls_binding
                .as_assessed()
                .ok_or_else(|| anyhow!("cross-dialect plan has no target TLS binding"))?
    {
        return Err(anyhow!("PostgreSQL target endpoint changed before resume"));
    }
    let accepted_for_checks = journal
        .accepted_mysql_freeze()
        .cloned()
        .ok_or_else(|| anyhow!("cross-dialect journal has no accepted MySQL freeze"))?;
    let mut require_source_consistency = || {
        require_current_cross_mysql_source_consistency(
            &reviewed,
            &accepted_for_checks,
            &source_config,
            &source_metadata_config,
            &freeze_config,
            &assertion,
            &cancellation,
        )
    };
    execute_cross_dialect_journal(
        &reviewed,
        reader.as_mut(),
        target.as_ref(),
        &mut journal,
        &cancellation,
        &mut require_source_consistency,
    )?;
    require_source_consistency()?;
    journal.transition_status(MigrationStatus::CompletedWithApprovedTransformations)?;
    cross_report(state_path.as_ref(), &journal)
}

fn validate_mysql_postgres_plan(reviewed: &ReviewedPlan) -> anyhow::Result<()> {
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    if reviewed.plan.tool_version != env!("CARGO_PKG_VERSION")
        || reviewed.plan.consistency_mode != super::mysql::MYSQL_CONSISTENCY_SNAPSHOT
        || reviewed.plan.conversion_policy.source_dialect() != ConversionDialect::MySql
        || reviewed.plan.conversion_policy.target_dialect() != Some(ConversionDialect::PostgreSql)
        || reviewed.plan.mysql_source_profile.is_none()
        || reviewed.plan.mysql_snapshot_evidence.is_none()
        || reviewed.plan.mysql_metadata_visibility.is_none()
        || reviewed.plan.mysql_target_snapshot_evidence.is_some()
        || reviewed.plan.mysql_target_metadata_visibility.is_some()
    {
        return Err(anyhow!(
            "reviewed plan is not an executable MySQL-to-PostgreSQL contract"
        ));
    }
    cross_dialect_contracts(reviewed)?;
    Ok(())
}

fn cross_policies(reviewed: &ReviewedPlan) -> anyhow::Result<Vec<TableConversionPolicy>> {
    let MigrationConversionMode::CrossDialect { tables, .. } =
        &reviewed.plan.conversion_policy.mode
    else {
        return Err(anyhow!("reviewed plan is not cross-dialect"));
    };
    Ok(tables.clone())
}

fn validate_mysql_postgres_credentials(
    source: &MySqlEndpointConfig,
    metadata: &MySqlEndpointConfig,
    freeze: &MySqlEndpointConfig,
    target: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let credentials = [
        source.credential_env.as_str(),
        metadata.credential_env.as_str(),
        freeze.credential_env.as_str(),
        target.credential_env.as_str(),
    ];
    if credentials
        .iter()
        .enumerate()
        .any(|(index, credential)| credentials[..index].contains(credential))
    {
        return Err(anyhow!(
            "MySQL source, metadata, freeze, and PostgreSQL target require separate credential references"
        ));
    }
    Ok(())
}

fn validate_cross_tls(
    reviewed: &ReviewedPlan,
    source: &MySqlEndpointConfig,
    target: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let target_binding = reviewed
        .plan
        .target_tls_binding
        .as_assessed()
        .ok_or_else(|| anyhow!("cross-dialect plan has no target TLS binding"))?;
    if reviewed.plan.source_tls_binding != mysql_tls_binding(source)?
        || target_binding != &postgres_tls_binding(target)?
        || reviewed.plan.capabilities.get("source_tls") != Some(&reviewed.plan.source_tls_binding)
        || reviewed.plan.capabilities.get("target_tls") != Some(target_binding)
    {
        return Err(anyhow!(
            "cross-dialect endpoint TLS policy differs from the reviewed plan"
        ));
    }
    Ok(())
}

fn validate_cross_mysql_freeze(
    reviewed: &ReviewedPlan,
    accepted: &MySqlExternalFreezeAttestation,
) -> anyhow::Result<()> {
    accepted.validate()?;
    let source = reviewed
        .plan
        .mysql_snapshot_evidence
        .as_ref()
        .ok_or_else(|| anyhow!("cross-dialect plan has no MySQL snapshot evidence"))?;
    if accepted.source_endpoint_identity != reviewed.plan.source_endpoint_identity
        || accepted.source_database_identity != source.database_identity
        || accepted.server_uuid != source.server_uuid
        || accepted.source_catalog_fingerprint != reviewed.plan.source_catalog_fingerprint
    {
        return Err(anyhow!(
            "accepted MySQL freeze differs from the reviewed cross-dialect source"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn require_current_cross_mysql_source_consistency(
    reviewed: &ReviewedPlan,
    accepted: &MySqlExternalFreezeAttestation,
    source_config: &MySqlEndpointConfig,
    metadata_config: &MySqlEndpointConfig,
    freeze_config: &MySqlEndpointConfig,
    assertion: &MySqlExternalFreezeAssertion,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    cancellation.check()?;
    let current = attest_mysql_external_freeze(freeze_config, reviewed, assertion)
        .context("re-attest external MySQL freeze")?;
    validate_mysql_external_freeze_continuity(accepted, &current)
        .context("validate continuous MySQL freeze evidence")?;
    attest_current_mysql_source_visibility(reviewed, source_config, metadata_config, freeze_config)
        .context("re-attest MySQL source catalog and metadata visibility")
}

fn capture_cross_mysql_source(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    metadata_config: &MySqlEndpointConfig,
    freeze_config: &MySqlEndpointConfig,
    cancellation: &CancellationToken,
) -> anyhow::Result<(Box<dyn ReadSession>, VendorCatalog)> {
    cancellation.check()?;
    source.validate_reviewed_binding(reviewed)?;
    let snapshot = source.capture_snapshot()?;
    let current_evidence = source.snapshot_evidence(&snapshot)?;
    let reviewed_evidence = reviewed
        .plan
        .mysql_snapshot_evidence
        .as_ref()
        .ok_or_else(|| anyhow!("cross-dialect plan has no MySQL snapshot evidence"))?;
    let (reader_catalog, reader_blockers, reader_fingerprint) =
        source.captured_catalog(&snapshot)?;
    let source_snapshot = MySqlCatalogSnapshot {
        endpoint_identity: snapshot.endpoint_identity.clone(),
        database_identity: snapshot.database_identity.clone(),
        server_version: snapshot.server_version.clone(),
        tls_binding: reviewed.plan.source_tls_binding.clone(),
        snapshot_evidence: current_evidence.clone(),
        catalog: reader_catalog,
        blockers: reader_blockers,
    };
    let visibility = collect_mysql_metadata_visibility(
        &source_snapshot,
        source.endpoint_config(),
        metadata_config,
        Some(freeze_config),
    )?;
    let reviewed_visibility = reviewed
        .plan
        .mysql_metadata_visibility
        .as_ref()
        .ok_or_else(|| anyhow!("cross-dialect plan has no MySQL metadata visibility"))?;
    let fingerprint = mysql_catalog_fingerprint(&visibility.authoritative_catalog)?;
    if !visibility.authoritative_blockers.is_empty()
        || visibility.evidence != *reviewed_visibility
        || !mysql_catalog_visibility_is_complete(&source_snapshot, &visibility)?
        || reader_fingerprint != reviewed_visibility.catalog_reader_fingerprint
        || fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&visibility.authoritative_catalog)
        || !mysql_execution_snapshot_binding_is_exact(&current_evidence, reviewed_evidence)
    {
        return Err(anyhow!(
            "fresh MySQL source differs from the reviewed cross-dialect catalog"
        ));
    }
    let reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced || reader.snapshot() != &snapshot {
        return Err(anyhow!(
            "MySQL source reader does not retain the reviewed read-only snapshot"
        ));
    }
    Ok((reader, visibility.authoritative_catalog))
}

fn attest_initial_postgres_target(
    reviewed: &ReviewedPlan,
    target: &PostgresTargetFactory,
) -> anyhow::Result<()> {
    let current = target.inspect_endpoint()?;
    let expected_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow!("cross-dialect plan has no target catalog"))?;
    if current.endpoint_identity != reviewed.plan.execution_target_endpoint_identity()?
        || &current.catalog != expected_catalog
        || catalog_fingerprint(&current.catalog)?
            != reviewed.plan.execution_target_catalog_fingerprint()?
        || current.tls_binding
            != *reviewed
                .plan
                .target_tls_binding
                .as_assessed()
                .ok_or_else(|| anyhow!("cross-dialect plan has no target TLS binding"))?
    {
        return Err(anyhow!(
            "PostgreSQL target differs from the reviewed empty endpoint"
        ));
    }
    Ok(())
}

fn cross_mysql_resume_binding(
    reviewed: &ReviewedPlan,
    accepted: &MySqlExternalFreezeAttestation,
    approval_reference: &str,
) -> anyhow::Result<ResumeBinding> {
    Ok(ResumeBinding {
        migration_id: reviewed.plan.migration_id.clone(),
        plan_hash: reviewed.plan_hash.to_string(),
        approval_reference: approval_reference.into(),
        tool_version: reviewed.plan.tool_version.clone(),
        source_endpoint: reviewed.plan.source_endpoint_identity.clone(),
        target_endpoint: reviewed
            .plan
            .execution_target_endpoint_identity()?
            .to_owned(),
        consistency_evidence: ConsistencyEvidence::MySqlExternalFreeze {
            endpoint_identity: accepted.source_endpoint_identity.clone(),
            database_identity: accepted.source_database_identity.clone(),
            server_uuid: accepted.server_uuid.clone(),
            source_catalog_fingerprint: accepted.source_catalog_fingerprint.clone(),
            profile_generation: accepted.profile_generation.clone(),
            continuity_token_hash: accepted.continuity_token_hash.clone(),
            backup_lock_connection_id: accepted.backup_lock_connection_id,
        },
        source_schema_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        target_schema_fingerprint: reviewed
            .plan
            .execution_target_catalog_fingerprint()?
            .to_owned(),
        outage_projection_digest: None,
        external_quiesce_attestation_digest: None,
        mysql_freeze_attestation_digest: Some(accepted.canonical_hash()?),
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    })
}

fn validate_cross_resume_binding(
    binding: &ResumeBinding,
    reviewed: &ReviewedPlan,
) -> anyhow::Result<()> {
    if binding.migration_id != reviewed.plan.migration_id
        || binding.plan_hash != reviewed.plan_hash.to_string()
        || binding.tool_version != env!("CARGO_PKG_VERSION")
        || binding.tool_version != reviewed.plan.tool_version
        || binding.source_endpoint != reviewed.plan.source_endpoint_identity
        || binding.target_endpoint != reviewed.plan.execution_target_endpoint_identity()?
        || binding.source_schema_fingerprint != reviewed.plan.source_catalog_fingerprint
        || binding.target_schema_fingerprint
            != reviewed.plan.execution_target_catalog_fingerprint()?
        || binding.outage_projection_digest.is_some()
        || binding.external_quiesce_attestation_digest.is_some()
        || binding.mysql_freeze_attestation_digest.is_none()
        || binding.conversion_policy != reviewed.plan.conversion_policy
        || binding.canonical_encoding_version != CANONICAL_ENCODING_VERSION
        || binding.approval_reference.trim().is_empty()
    {
        return Err(anyhow!(
            "cross-dialect journal binding differs from the reviewed plan"
        ));
    }
    Ok(())
}

fn cross_journal_genesis(
    binding: ResumeBinding,
    reviewed: ReviewedPlan,
    accepted: MySqlExternalFreezeAttestation,
) -> Genesis {
    let operations = reviewed
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
    Genesis {
        binding,
        reviewed_plan: reviewed,
        accepted_outage_projection: None,
        accepted_external_quiesce: None,
        accepted_mysql_freeze: Some(accepted),
        operations,
    }
}

fn cross_report(
    state_path: &Path,
    journal: &AppendJournal,
) -> anyhow::Result<CrossDialectExecutionReport> {
    let copied_rows = journal
        .projection()
        .copy_cursors
        .values()
        .try_fold(0_u64, |total, cursor| total.checked_add(cursor.rows))
        .ok_or_else(|| anyhow!("cross-dialect copied row count overflow"))?;
    Ok(CrossDialectExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: journal.projection().last_chunk_id,
    })
}

#[derive(Debug, Clone)]
struct CrossCopyContract {
    create_operation_id: String,
    copy_operation_id: String,
    verify_operation_id: String,
    policy: TableConversionPolicy,
    source_projection: Vec<Identifier>,
    target_projection: Vec<Identifier>,
    source_key_indexes: Vec<usize>,
    target_key_indexes: Vec<usize>,
}

impl CrossCopyContract {
    fn new(
        policy: TableConversionPolicy,
        create_operation_id: String,
        copy_operation_id: String,
        verify_operation_id: String,
    ) -> anyhow::Result<Self> {
        policy.validate()?;
        let source_projection = policy
            .row_policy
            .columns
            .iter()
            .map(|column| column.source.name.clone())
            .collect::<Vec<_>>();
        let target_projection = policy
            .row_policy
            .columns
            .iter()
            .map(|column| column.target.name.clone())
            .collect::<Vec<_>>();
        let source_key_indexes = key_indexes(
            &source_projection,
            &policy.resumable_key.source_columns,
            "source",
        )?;
        let target_key_indexes = key_indexes(
            &target_projection,
            &policy.resumable_key.target_columns,
            "target",
        )?;
        Ok(Self {
            create_operation_id,
            copy_operation_id,
            verify_operation_id,
            policy,
            source_projection,
            target_projection,
            source_key_indexes,
            target_key_indexes,
        })
    }
}

fn key_indexes(
    projection: &[Identifier],
    key: &[Identifier],
    side: &str,
) -> anyhow::Result<Vec<usize>> {
    key.iter()
        .map(|column| {
            projection
                .iter()
                .position(|candidate| candidate == column)
                .ok_or_else(|| anyhow!("cross-dialect {side} key column is not projected"))
        })
        .collect()
}

fn cross_dialect_contracts(reviewed: &ReviewedPlan) -> anyhow::Result<Vec<CrossCopyContract>> {
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    let MigrationConversionMode::CrossDialect { tables, .. } =
        &reviewed.plan.conversion_policy.mode
    else {
        return Err(anyhow!("reviewed plan is not cross-dialect"));
    };
    let mut contracts = Vec::with_capacity(tables.len());
    for policy in tables {
        let create = exact_policy_operation(
            &reviewed.plan.operations,
            OperationKind::CreateTable,
            "cross_dialect_target_table_policy",
            policy,
            &policy.target_table,
        )?;
        let copy = exact_policy_operation(
            &reviewed.plan.operations,
            OperationKind::CopyTable,
            "table_conversion_policy",
            policy,
            &policy.source_table,
        )?;
        let verify = exact_policy_operation(
            &reviewed.plan.operations,
            OperationKind::VerifyTable,
            "cross_dialect_verification_policy",
            policy,
            &policy.source_table,
        )?;
        contracts.push(CrossCopyContract::new(
            policy.clone(),
            create.id.to_string(),
            copy.id.to_string(),
            verify.id.to_string(),
        )?);
    }
    contracts.sort_by(|left, right| left.policy.source_table.cmp(&right.policy.source_table));
    Ok(contracts)
}

fn exact_policy_operation<'a>(
    operations: &'a [PlanOperation],
    kind: OperationKind,
    parameter: &str,
    policy: &TableConversionPolicy,
    table: &QualifiedTable,
) -> anyhow::Result<&'a PlanOperation> {
    let matches = operations
        .iter()
        .filter(|operation| operation.kind == kind && operation.table.as_ref() == Some(table))
        .collect::<Vec<_>>();
    let [operation] = matches.as_slice() else {
        return Err(anyhow!(
            "reviewed cross-dialect table has missing or duplicate {kind:?} operations"
        ));
    };
    let embedded: TableConversionPolicy = serde_json::from_value(
        operation
            .parameters
            .get(parameter)
            .cloned()
            .ok_or_else(|| anyhow!("cross-dialect operation has no {parameter}"))?,
    )?;
    if &embedded != policy || operation.parameters.len() != 1 {
        return Err(anyhow!(
            "cross-dialect operation policy differs from the reviewed conversion policy"
        ));
    }
    Ok(operation)
}

/// Execute or resume the database-effect portion of a cross-dialect plan.
/// The caller owns fresh endpoint, TLS, source-consistency, and target-empty
/// admission and supplies the retained exact source snapshot.
fn execute_cross_dialect_journal(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    require_source_consistency: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let contracts = cross_dialect_contracts(reviewed)?;
    reconcile_cross_tables(target, journal, &contracts, require_source_consistency)?;
    copy_cross_tables(
        reviewed,
        reader,
        target,
        journal,
        cancellation,
        &contracts,
        require_source_consistency,
    )?;
    require_source_consistency()?;
    if journal.projection().status == MigrationStatus::Running {
        journal.transition_status(MigrationStatus::Verifying)?;
    }
    if journal.projection().status != MigrationStatus::Verifying {
        return Err(anyhow!("cross-dialect journal is not in a resumable phase"));
    }
    verify_cross_tables(reader, target, journal, cancellation, &contracts)?;
    require_source_consistency()?;
    let policies = contracts
        .iter()
        .map(|contract| contract.policy.clone())
        .collect::<Vec<_>>();
    let schema_fingerprint = target.verify_cross_schema(&policies)?;
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("cross-dialect plan has no VerifySchema operation"))?;
    complete_operation(journal, verify_schema.id.as_str())?;
    if !journal.projection().schema_verified {
        journal.verify_schema(schema_fingerprint)?;
    }
    Ok(())
}

fn reconcile_cross_tables(
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    contracts: &[CrossCopyContract],
    require_source_consistency: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    for contract in contracts {
        require_source_consistency()?;
        let operation_id = contract.create_operation_id.as_str();
        let mut state = operation_state(journal, operation_id)?;
        if state == OperationState::Pending {
            journal.prepare_operations_atomic([operation_id])?;
            state = OperationState::Prepared;
        }
        if state == OperationState::Running {
            journal.transition_operation(operation_id, OperationState::Prepared)?;
            state = OperationState::Prepared;
        }
        if state == OperationState::Prepared {
            match target.inspect_cross_table(&contract.policy)? {
                CrossTableState::Absent => target.create_cross_table(&contract.policy)?,
                CrossTableState::Exact => {}
                CrossTableState::Different => {
                    return require_manual(
                        journal,
                        "prepared cross-dialect target table has different semantics".into(),
                    );
                }
            }
            if target.inspect_cross_table(&contract.policy)? != CrossTableState::Exact {
                return require_manual(
                    journal,
                    "cross-dialect target table is not exact after create".into(),
                );
            }
            journal.transition_operation(operation_id, OperationState::Committed)?;
            state = OperationState::Committed;
        }
        if state == OperationState::Committed {
            if target.inspect_cross_table(&contract.policy)? != CrossTableState::Exact {
                return require_manual(
                    journal,
                    "committed cross-dialect target table drifted".into(),
                );
            }
            journal.transition_operation(operation_id, OperationState::Verified)?;
            state = OperationState::Verified;
        }
        if state != OperationState::Verified
            || target.inspect_cross_table(&contract.policy)? != CrossTableState::Exact
        {
            return require_manual(
                journal,
                "verified cross-dialect target table drifted".into(),
            );
        }
    }
    Ok(())
}

fn copy_cross_tables(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[CrossCopyContract],
    require_source_consistency: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    if let Some(prepared) = journal.projection().prepared_chunk.clone() {
        let contract = contracts
            .iter()
            .find(|contract| contract.copy_operation_id == prepared.operation_id)
            .ok_or_else(|| anyhow!("prepared chunk has no cross-dialect contract"))?;
        reconcile_prepared_cross_chunk(
            reviewed,
            reader,
            target,
            journal,
            cancellation,
            contract,
            require_source_consistency,
        )?;
    }
    for contract in contracts {
        let operation_id = contract.copy_operation_id.as_str();
        let mut state = operation_state(journal, operation_id)?;
        if state == OperationState::Pending {
            journal.transition_operation(operation_id, OperationState::Running)?;
            state = OperationState::Running;
        }
        if state == OperationState::Running {
            let mut after = journal
                .projection()
                .copy_cursors
                .get(operation_id)
                .map(|cursor| KeyTuple::new(cursor.final_key.clone()));
            loop {
                cancellation.check()?;
                let source_batch =
                    reader.select_page(&source_page(contract, after.clone(), u32::MAX))?;
                if source_batch.is_empty() {
                    break;
                }
                let target_batch = contract.policy.convert_batch(&source_batch)?;
                let final_key = batch_key(&source_batch, &contract.source_key_indexes)?;
                let chunk_id = journal
                    .projection()
                    .last_chunk_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("cross-dialect chunk identifier overflow"))?;
                journal.prepare_chunk(PreparedChunk {
                    chunk_id,
                    operation_id: operation_id.into(),
                    start_key: after.as_ref().map(|key| key.0.clone()),
                    final_key: final_key.0.clone(),
                    row_count: u64::try_from(target_batch.len())?,
                    canonical_digest: converted_batch_digest(contract, &target_batch)?,
                    target_transaction_intent: format!(
                        "{}:{}:{chunk_id}",
                        reviewed.plan_hash, operation_id
                    ),
                })?;
                write_cross_chunk(
                    target,
                    journal,
                    cancellation,
                    contract,
                    &target_batch,
                    require_source_consistency,
                )?;
                after = Some(final_key);
            }
        }
        complete_operation(journal, operation_id)?;
    }
    Ok(())
}

fn reconcile_prepared_cross_chunk(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contract: &CrossCopyContract,
    require_source_consistency: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    require_source_consistency()?;
    let prepared = journal
        .projection()
        .prepared_chunk
        .clone()
        .ok_or_else(|| anyhow!("cross-dialect journal has no prepared chunk"))?;
    let limit = u32::try_from(prepared.row_count)
        .context("prepared cross-dialect chunk exceeds the page limit")?;
    let source_batch = reader.select_page(&source_page(
        contract,
        prepared.start_key.clone().map(KeyTuple::new),
        limit,
    ))?;
    let target_batch = contract.policy.convert_batch(&source_batch)?;
    if target_batch.len() as u64 != prepared.row_count
        || batch_key(&source_batch, &contract.source_key_indexes)?.0 != prepared.final_key
        || converted_batch_digest(contract, &target_batch)? != prepared.canonical_digest
        || prepared.target_transaction_intent
            != format!(
                "{}:{}:{}",
                reviewed.plan_hash, contract.copy_operation_id, prepared.chunk_id
            )
    {
        return require_manual(
            journal,
            "prepared cross-dialect chunk differs from the frozen source".into(),
        );
    }
    match inspect_target_interval(target, cancellation, contract, &prepared, &target_batch)? {
        CrossTableState::Exact => journal.commit_chunk_after_ack().map_err(Into::into),
        CrossTableState::Absent => write_cross_chunk(
            target,
            journal,
            cancellation,
            contract,
            &target_batch,
            require_source_consistency,
        ),
        CrossTableState::Different => require_manual(
            journal,
            "prepared cross-dialect target interval differs from durable intent".into(),
        ),
    }
}

fn write_cross_chunk(
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contract: &CrossCopyContract,
    target_batch: &RowBatch,
    require_source_consistency: &mut dyn FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    require_source_consistency()?;
    let mut writer = target.open_writer(cancellation.clone())?;
    writer.begin()?;
    if let Err(error) = writer.insert(&contract.policy.target_table, target_batch) {
        let rollback = writer.rollback();
        return match rollback {
            Ok(()) => Err(error.into()),
            Err(rollback) => Err(anyhow!(
                "cross-dialect write failed ({error}) and rollback failed ({rollback})"
            )),
        };
    }
    if let Err(cancelled) = cancellation.check() {
        writer
            .rollback()
            .context("roll back cancelled cross-dialect target transaction")?;
        return Err(cancelled.into());
    }
    match writer.commit() {
        Ok(()) => journal.commit_chunk_after_ack().map_err(Into::into),
        Err(ConnectionError::CommitOutcomeUnknown(reason)) => Err(anyhow!(
            "cross-dialect target commit outcome is unknown: {reason}; resume must reconcile durable intent"
        )),
        Err(error) => Err(error.into()),
    }
}

fn inspect_target_interval(
    target: &dyn CrossDialectTarget,
    cancellation: &CancellationToken,
    contract: &CrossCopyContract,
    prepared: &PreparedChunk,
    expected: &RowBatch,
) -> anyhow::Result<CrossTableState> {
    let limit = prepared
        .row_count
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| anyhow!("prepared cross-dialect chunk is too large to inspect"))?;
    let after = prepared
        .start_key
        .as_deref()
        .map(|key| contract.policy.convert_source_key(key))
        .transpose()?
        .map(KeyTuple::new);
    let mut verifier = target.open_verifier(cancellation.clone())?;
    let observed = verifier.select_page(&target_page(contract, after, limit))?;
    if observed.is_empty() {
        return Ok(CrossTableState::Absent);
    }
    let expected_final_key = contract.policy.convert_source_key(&prepared.final_key)?;
    let exact = observed.rows() == expected.rows()
        && observed.len() as u64 == prepared.row_count
        && batch_key(&observed, &contract.target_key_indexes)?.0 == expected_final_key
        && converted_batch_digest(contract, &observed)? == prepared.canonical_digest;
    Ok(if exact {
        CrossTableState::Exact
    } else {
        CrossTableState::Different
    })
}

fn verify_cross_tables(
    reader: &mut dyn ReadSession,
    target: &dyn CrossDialectTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[CrossCopyContract],
) -> anyhow::Result<()> {
    let mut chunks = journal.all_committed_chunks()?.peekable();
    for contract in contracts {
        let mut verifier = target.open_verifier(cancellation.clone())?;
        let (manifest, source_hash, target_hash) =
            verify_cross_table(reader, verifier.as_mut(), &mut chunks, contract)?;
        if let Some(durable) = journal.table_verification_evidence(&contract.copy_operation_id) {
            if durable.0 != manifest || durable.1 != source_hash || durable.2 != target_hash {
                return Err(anyhow!(
                    "fresh cross-dialect table verification differs from durable evidence"
                ));
            }
        } else {
            journal.verify_table(
                &contract.copy_operation_id,
                manifest,
                source_hash,
                target_hash,
            )?;
        }
        complete_operation(journal, &contract.verify_operation_id)?;
    }
    if let Some(extra) = chunks.next() {
        let extra = extra?;
        return Err(anyhow!(
            "journal contains an unexpected cross-dialect chunk for {}",
            extra.operation_id
        ));
    }
    Ok(())
}

fn verify_cross_table(
    reader: &mut dyn ReadSession,
    verifier: &mut dyn super::connection::VerificationSession,
    chunks: &mut std::iter::Peekable<CommittedChunkIter>,
    contract: &CrossCopyContract,
) -> anyhow::Result<(String, String, String)> {
    let mut source_after = None;
    let mut target_after = None;
    let mut manifest = Sha256::new();
    let mut source_hash = Sha256::new();
    let mut target_hash = Sha256::new();
    while let Some(next) = chunks.peek() {
        let next = next.as_ref().map_err(|error| anyhow!(error.to_string()))?;
        if next.operation_id != contract.copy_operation_id {
            break;
        }
        let chunk = chunks
            .next()
            .ok_or_else(|| anyhow!("cross-dialect chunk stream ended unexpectedly"))??;
        if chunk.start_key != source_after.as_ref().map(|key: &KeyTuple| key.0.clone()) {
            return Err(anyhow!("cross-dialect chunk manifest has a keyspace gap"));
        }
        let limit = u32::try_from(chunk.row_count)?;
        let source_batch =
            reader.select_page(&source_page(contract, source_after.clone(), limit))?;
        let expected = contract.policy.convert_batch(&source_batch)?;
        let target_batch =
            verifier.select_page(&target_page(contract, target_after.clone(), limit))?;
        let expected_target_final = contract.policy.convert_source_key(&chunk.final_key)?;
        if expected.len() as u64 != chunk.row_count
            || target_batch.len() as u64 != chunk.row_count
            || expected.rows() != target_batch.rows()
            || batch_key(&source_batch, &contract.source_key_indexes)?.0 != chunk.final_key
            || batch_key(&target_batch, &contract.target_key_indexes)?.0 != expected_target_final
        {
            return Err(anyhow!("cross-dialect committed row verification failed"));
        }
        let expected_digest = converted_batch_digest(contract, &expected)?;
        let target_digest = converted_batch_digest(contract, &target_batch)?;
        if expected_digest != chunk.canonical_digest || target_digest != chunk.canonical_digest {
            return Err(anyhow!(
                "cross-dialect committed digest verification failed"
            ));
        }
        let encoded = serde_json::to_vec(&chunk)?;
        manifest.update(u64::try_from(encoded.len())?.to_be_bytes());
        manifest.update(encoded);
        source_hash.update(expected_digest.as_bytes());
        target_hash.update(target_digest.as_bytes());
        source_after = Some(KeyTuple::new(chunk.final_key));
        target_after = Some(KeyTuple::new(expected_target_final));
    }
    let source_tail = reader.select_page(&source_page(contract, source_after, 1))?;
    let target_tail = verifier.select_page(&target_page(contract, target_after, 1))?;
    if !source_tail.is_empty() || !target_tail.is_empty() {
        return Err(anyhow!(
            "cross-dialect source or target contains rows outside committed intervals"
        ));
    }
    Ok((
        hex::encode(manifest.finalize()),
        hex::encode(source_hash.finalize()),
        hex::encode(target_hash.finalize()),
    ))
}

fn source_page(contract: &CrossCopyContract, after: Option<KeyTuple>, limit: u32) -> KeysetPage {
    KeysetPage {
        table: contract.policy.source_table.clone(),
        projection: contract.source_projection.clone(),
        key: contract.policy.resumable_key.source_columns.clone(),
        after,
        limit,
    }
}

fn target_page(contract: &CrossCopyContract, after: Option<KeyTuple>, limit: u32) -> KeysetPage {
    KeysetPage {
        table: contract.policy.target_table.clone(),
        projection: contract.target_projection.clone(),
        key: contract.policy.resumable_key.target_columns.clone(),
        after,
        limit,
    }
}

fn batch_key(batch: &RowBatch, indexes: &[usize]) -> anyhow::Result<KeyTuple> {
    let row = batch
        .rows()
        .last()
        .ok_or_else(|| anyhow!("cannot derive a key from an empty cross-dialect batch"))?;
    indexes
        .iter()
        .map(|index| {
            let value = row
                .get(*index)
                .ok_or_else(|| anyhow!("cross-dialect key index exceeds row width"))?;
            if *value == DbValue::Null {
                return Err(anyhow!("cross-dialect resumable key contains NULL"));
            }
            Ok(value.clone())
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(KeyTuple::new)
}

fn converted_batch_digest(
    contract: &CrossCopyContract,
    batch: &RowBatch,
) -> anyhow::Result<String> {
    let table = format!(
        "{}.{}",
        contract.policy.target_table.namespace, contract.policy.target_table.name
    );
    let columns = contract
        .target_projection
        .iter()
        .map(Identifier::as_str)
        .collect::<Vec<_>>();
    let keys = batch
        .rows()
        .iter()
        .map(|row| {
            contract
                .target_key_indexes
                .iter()
                .map(|index| {
                    row.get(*index)
                        .cloned()
                        .ok_or_else(|| anyhow!("cross-dialect target key exceeds row width"))
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let rows = batch
        .rows()
        .iter()
        .zip(&keys)
        .map(|(values, key)| CanonicalRow {
            table: &table,
            columns: &columns,
            key,
            values,
        })
        .collect::<Vec<_>>();
    Ok(hex::encode(digest_rows(rows.iter())?))
}

fn operation_state(journal: &AppendJournal, operation_id: &str) -> anyhow::Result<OperationState> {
    journal
        .projection()
        .operations
        .get(operation_id)
        .copied()
        .ok_or_else(|| anyhow!("cross-dialect journal has no operation {operation_id}"))
}

fn complete_operation(journal: &mut AppendJournal, operation_id: &str) -> anyhow::Result<()> {
    let mut state = operation_state(journal, operation_id)?;
    if state == OperationState::Pending {
        journal.transition_operation(operation_id, OperationState::Running)?;
        state = OperationState::Running;
    }
    if state == OperationState::Running {
        journal.transition_operation(operation_id, OperationState::Prepared)?;
        state = OperationState::Prepared;
    }
    if state == OperationState::Prepared {
        journal.transition_operation(operation_id, OperationState::Committed)?;
        state = OperationState::Committed;
    }
    if state == OperationState::Committed {
        journal.transition_operation(operation_id, OperationState::Verified)?;
        state = OperationState::Verified;
    }
    if state != OperationState::Verified {
        return Err(anyhow!(
            "cross-dialect operation {operation_id} did not reach Verified"
        ));
    }
    Ok(())
}

fn require_manual<T>(journal: &mut AppendJournal, reason: String) -> anyhow::Result<T> {
    journal.require_manual_reconciliation()?;
    Err(anyhow!(reason))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::migration::connection::{
        CapabilitySet, ControlSession, ReadOnlyEvidence, SnapshotToken, VerificationSession,
        WriteSession,
    };
    use crate::migration::cross_dialect::tests::mysql_to_postgres_reviewed_plan;
    use crate::migration::mysql_profile::{
        MySqlDdlFreezeMechanism, MySqlDmlFreezeMechanism, MySqlFreezeAttestationStatus,
        MySqlFreezeProfileKind, MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
    };

    struct TestReader {
        snapshot: SnapshotToken,
        evidence: ReadOnlyEvidence,
        rows: RowBatch,
    }

    impl ReadSession for TestReader {
        fn read_only_evidence(&self) -> &ReadOnlyEvidence {
            &self.evidence
        }

        fn snapshot(&self) -> &SnapshotToken {
            &self.snapshot
        }

        fn select_page(&mut self, request: &KeysetPage) -> Result<RowBatch, ConnectionError> {
            page(&self.rows, request)
        }
    }

    struct TestVerifier {
        rows: RowBatch,
    }

    impl VerificationSession for TestVerifier {
        fn select_page(&mut self, request: &KeysetPage) -> Result<RowBatch, ConnectionError> {
            page(&self.rows, request)
        }
    }

    #[derive(Clone)]
    struct TestTarget {
        expected_policy: TableConversionPolicy,
        state: Arc<Mutex<TestTargetState>>,
    }

    struct TestTargetState {
        created: bool,
        different_schema: bool,
        commit_unknown_after_apply: bool,
        rows: RowBatch,
    }

    struct TestWriter {
        state: Arc<Mutex<TestTargetState>>,
        pending: Option<RowBatch>,
    }

    impl WriteSession for TestWriter {
        fn begin(&mut self) -> Result<(), ConnectionError> {
            Ok(())
        }

        fn insert(
            &mut self,
            _table: &QualifiedTable,
            batch: &RowBatch,
        ) -> Result<(), ConnectionError> {
            self.pending = Some(batch.clone());
            Ok(())
        }

        fn commit(&mut self) -> Result<(), ConnectionError> {
            let pending = self
                .pending
                .take()
                .ok_or(ConnectionError::TransactionRequired)?;
            let mut state = self.state.lock().map_err(|_| {
                ConnectionError::Database("test target state lock is poisoned".into())
            })?;
            for row in pending.rows() {
                state
                    .rows
                    .try_push(row.clone(), 0)
                    .map_err(|error| ConnectionError::BatchLimit(error.to_string()))?;
            }
            if state.commit_unknown_after_apply {
                Err(ConnectionError::CommitOutcomeUnknown(
                    "injected acknowledgement loss".into(),
                ))
            } else {
                Ok(())
            }
        }

        fn rollback(&mut self) -> Result<(), ConnectionError> {
            self.pending = None;
            Ok(())
        }
    }

    struct TestControl;

    impl ControlSession for TestControl {
        fn cancel_active_statement(&mut self) -> Result<(), ConnectionError> {
            Ok(())
        }
    }

    impl TargetConnectionFactory for TestTarget {
        fn capabilities(&self) -> CapabilitySet {
            CapabilitySet::default()
        }

        fn open_writer(
            &self,
            _cancellation: CancellationToken,
        ) -> Result<Box<dyn WriteSession>, ConnectionError> {
            Ok(Box::new(TestWriter {
                state: Arc::clone(&self.state),
                pending: None,
            }))
        }

        fn open_verifier(
            &self,
            _cancellation: CancellationToken,
        ) -> Result<Box<dyn VerificationSession>, ConnectionError> {
            let state = self.state.lock().map_err(|_| {
                ConnectionError::Database("test target state lock is poisoned".into())
            })?;
            Ok(Box::new(TestVerifier {
                rows: state.rows.clone(),
            }))
        }

        fn open_control(&self) -> Result<Box<dyn ControlSession>, ConnectionError> {
            Ok(Box::new(TestControl))
        }
    }

    impl CrossDialectTarget for TestTarget {
        fn inspect_cross_table(
            &self,
            policy: &TableConversionPolicy,
        ) -> Result<CrossTableState, ConnectionError> {
            let state = self.state.lock().map_err(|_| {
                ConnectionError::Database("test target state lock is poisoned".into())
            })?;
            Ok(if !state.created {
                CrossTableState::Absent
            } else if state.different_schema || policy != &self.expected_policy {
                CrossTableState::Different
            } else {
                CrossTableState::Exact
            })
        }

        fn create_cross_table(
            &self,
            policy: &TableConversionPolicy,
        ) -> Result<(), ConnectionError> {
            if policy != &self.expected_policy {
                return Err(ConnectionError::InvalidRequest(
                    "test target received a substituted policy".into(),
                ));
            }
            self.state
                .lock()
                .map_err(|_| {
                    ConnectionError::Database("test target state lock is poisoned".into())
                })?
                .created = true;
            Ok(())
        }

        fn verify_cross_schema(
            &self,
            policies: &[TableConversionPolicy],
        ) -> Result<String, ConnectionError> {
            if policies != [self.expected_policy.clone()]
                || self.inspect_cross_table(&self.expected_policy)? != CrossTableState::Exact
            {
                return Err(ConnectionError::InvalidRequest(
                    "test target schema differs".into(),
                ));
            }
            serde_json::to_vec(policies)
                .map(|bytes| hex::encode(Sha256::digest(bytes)))
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))
        }
    }

    fn page(rows: &RowBatch, request: &KeysetPage) -> Result<RowBatch, ConnectionError> {
        let indexes = request
            .projection
            .iter()
            .map(|name| {
                rows.columns()
                    .iter()
                    .position(|column| column.name == *name)
                    .ok_or_else(|| {
                        ConnectionError::InvalidRequest("test projection differs".into())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key_index = rows
            .columns()
            .iter()
            .position(|column| request.key.first() == Some(&column.name))
            .ok_or_else(|| ConnectionError::InvalidRequest("test key differs".into()))?;
        let after = request
            .after
            .as_ref()
            .and_then(|key| key.0.first())
            .and_then(signed_value);
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        let mut result = RowBatch::new(
            indexes
                .iter()
                .map(|index| rows.columns()[*index].clone())
                .collect(),
            limit.max(1),
            usize::MAX,
        );
        for row in rows.rows().iter().filter(|row| {
            after.is_none_or(|after| signed_value(&row[key_index]).is_some_and(|key| key > after))
        }) {
            if result.len() >= limit {
                break;
            }
            result
                .try_push(indexes.iter().map(|index| row[*index].clone()).collect(), 0)
                .map_err(|error| ConnectionError::BatchLimit(error.to_string()))?;
        }
        Ok(result)
    }

    fn signed_value(value: &DbValue) -> Option<i128> {
        match value {
            DbValue::Signed(value) => Some(*value),
            _ => None,
        }
    }

    fn accepted_freeze(reviewed: &ReviewedPlan) -> MySqlExternalFreezeAttestation {
        let evidence = reviewed.plan.mysql_snapshot_evidence.as_ref().unwrap();
        MySqlExternalFreezeAttestation {
            schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
            profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
            status: MySqlFreezeAttestationStatus::Active,
            source_endpoint_identity: reviewed.plan.source_endpoint_identity.clone(),
            source_database_identity: evidence.database_identity.clone(),
            source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
            server_uuid: evidence.server_uuid.clone(),
            server_start_lower_bound_unix_seconds: 99,
            server_start_upper_bound_unix_seconds: 101,
            administrator_tls_binding: "admin-tls".into(),
            profile_generation: "generation-1".into(),
            provider_reference: "change-1".into(),
            activated_at_unix_seconds: 10,
            expires_at_unix_seconds: 1_000,
            continuity_token_hash: "b".repeat(64),
            backup_lock_connection_id: 7,
            backup_lock_owner_thread_id: 8,
            backup_lock_owner_user: "freeze".into(),
            backup_lock_owner_host: "localhost".into(),
            read_only: true,
            super_read_only: true,
            super_read_only_persisted: true,
            active_replication_channels: Vec::new(),
            dml_mechanism: MySqlDmlFreezeMechanism::PersistedSuperReadOnly,
            ddl_mechanism: MySqlDdlFreezeMechanism::ExternalBackupLock,
            freeze_enforced_by_tool: false,
            attested_at_unix_seconds: 100,
        }
    }

    fn source_rows(policy: &TableConversionPolicy) -> RowBatch {
        let mut rows = RowBatch::new(
            policy
                .row_policy
                .columns
                .iter()
                .map(|column| column.source.clone())
                .collect(),
            2,
            1024,
        );
        rows.try_push(vec![DbValue::Signed(1)], 8).unwrap();
        rows.try_push(vec![DbValue::Signed(2)], 8).unwrap();
        rows
    }

    fn reader(rows: RowBatch) -> TestReader {
        TestReader {
            snapshot: SnapshotToken {
                endpoint_identity: "mysql://source/app".into(),
                database_identity: "app".into(),
                snapshot_id: "snapshot".into(),
                consistency_mode: "mysql-consistent-snapshot".into(),
                server_version: "8.4.0".into(),
                lifecycle_id: "life".into(),
            },
            evidence: ReadOnlyEvidence {
                server_enforced: true,
                description: "test freeze".into(),
            },
            rows,
        }
    }

    fn target(policy: TableConversionPolicy, commit_unknown_after_apply: bool) -> TestTarget {
        let rows = RowBatch::new(
            policy
                .row_policy
                .columns
                .iter()
                .map(|column| column.target.clone())
                .collect(),
            16,
            usize::MAX,
        );
        TestTarget {
            expected_policy: policy,
            state: Arc::new(Mutex::new(TestTargetState {
                created: false,
                different_schema: false,
                commit_unknown_after_apply,
                rows,
            })),
        }
    }

    fn journal(reviewed: &ReviewedPlan, directory: &Path) -> AppendJournal {
        let accepted = accepted_freeze(reviewed);
        let binding = cross_mysql_resume_binding(reviewed, &accepted, "approval-1").unwrap();
        AppendJournal::create_new(
            directory.join("migration.state"),
            cross_journal_genesis(binding, reviewed.clone(), accepted),
        )
        .unwrap()
    }

    fn private_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("cross-dialect-journal-")
            .tempdir_in(".")
            .unwrap()
    }

    #[test]
    fn unknown_commit_after_apply_reconciles_the_same_prepared_chunk() {
        let reviewed = mysql_to_postgres_reviewed_plan();
        let policy = cross_policies(&reviewed).unwrap().remove(0);
        let source = source_rows(&policy);
        let target = target(policy.clone(), true);
        let directory = private_tempdir();
        let mut journal = journal(&reviewed, directory.path());
        let cancellation = CancellationToken::default();
        let mut consistency_checks = 0_u64;
        let mut require_consistency = || {
            consistency_checks += 1;
            Ok(())
        };

        let error = execute_cross_dialect_journal(
            &reviewed,
            &mut reader(source.clone()),
            &target,
            &mut journal,
            &cancellation,
            &mut require_consistency,
        )
        .unwrap_err();
        assert!(error.to_string().contains("outcome is unknown"));
        assert!(journal.projection().prepared_chunk.is_some());
        assert_eq!(journal.projection().last_chunk_id, 0);
        assert_eq!(target.state.lock().unwrap().rows.len(), 2);

        target.state.lock().unwrap().commit_unknown_after_apply = false;
        execute_cross_dialect_journal(
            &reviewed,
            &mut reader(source),
            &target,
            &mut journal,
            &cancellation,
            &mut require_consistency,
        )
        .unwrap();
        assert!(journal.projection().prepared_chunk.is_none());
        assert_eq!(journal.projection().last_chunk_id, 1);
        assert_eq!(journal.projection().status, MigrationStatus::Verifying);
        assert!(journal.projection().schema_verified);
        assert!(consistency_checks >= 5);
        journal
            .transition_status(MigrationStatus::CompletedWithApprovedTransformations)
            .unwrap();
    }

    #[test]
    fn resume_rechecks_rows_even_when_table_evidence_is_already_durable() {
        let reviewed = mysql_to_postgres_reviewed_plan();
        let policy = cross_policies(&reviewed).unwrap().remove(0);
        let source = source_rows(&policy);
        let target = target(policy, false);
        let directory = private_tempdir();
        let mut journal = journal(&reviewed, directory.path());
        let cancellation = CancellationToken::default();
        let mut require_consistency = || Ok(());
        execute_cross_dialect_journal(
            &reviewed,
            &mut reader(source.clone()),
            &target,
            &mut journal,
            &cancellation,
            &mut require_consistency,
        )
        .unwrap();
        assert!(!journal.projection().table_verifications.is_empty());
        let columns = target.state.lock().unwrap().rows.columns().to_vec();
        let mut drifted = RowBatch::new(columns, 2, 1024);
        drifted.try_push(vec![DbValue::Signed(9)], 8).unwrap();
        drifted.try_push(vec![DbValue::Signed(2)], 8).unwrap();
        target.state.lock().unwrap().rows = drifted;

        let error = execute_cross_dialect_journal(
            &reviewed,
            &mut reader(source),
            &target,
            &mut journal,
            &cancellation,
            &mut require_consistency,
        )
        .unwrap_err();
        assert!(error.to_string().contains("row verification failed"));
    }

    #[test]
    fn source_consistency_failure_stops_before_the_chunk_write() {
        let reviewed = mysql_to_postgres_reviewed_plan();
        let policy = cross_policies(&reviewed).unwrap().remove(0);
        let target = target(policy.clone(), false);
        let directory = private_tempdir();
        let mut journal = journal(&reviewed, directory.path());
        let cancellation = CancellationToken::default();
        let mut checks = 0_u8;
        let mut require_consistency = || {
            checks += 1;
            if checks == 2 {
                Err(anyhow!("freeze lost"))
            } else {
                Ok(())
            }
        };

        let error = execute_cross_dialect_journal(
            &reviewed,
            &mut reader(source_rows(&policy)),
            &target,
            &mut journal,
            &cancellation,
            &mut require_consistency,
        )
        .unwrap_err();
        assert!(error.to_string().contains("freeze lost"));
        assert!(journal.projection().prepared_chunk.is_some());
        assert!(target.state.lock().unwrap().rows.is_empty());
    }
}
