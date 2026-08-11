//! Fixture-backed orchestration for the migration contract spike.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};

use super::artifact::{read_json, replace_json, write_json_new};
use super::canonical::{digest_rows, encode_row, CanonicalRow, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, ConnectionError, KeyTuple, KeysetPage, ReadSession, SourceConnectionFactory,
    TargetConnectionFactory,
};
use super::fixture::{InMemorySource, InMemoryTarget};
use super::journal::{
    ChunkRecord, ChunkState, ConsistencyEvidence, MigrationState, MigrationStatus, OperationState,
    PreparedChunkEvidence, PreparedResolution, ResumeBinding,
};
use super::model::{
    CatalogObject, CatalogObjectKind, ColumnMeta, DbValue, Identifier, QualifiedTable, RowBatch,
    VendorCatalog,
};
use super::plan::{
    MigrationPlan, OperationKind, PlanOperation, ReviewedPlan, UnsupportedObjectReport,
    PLAN_SCHEMA_VERSION,
};
use super::verify::{verify_keyed_rows, KeyedRow};

#[cfg(feature = "enterprise-migration-spike")]
use super::postgres::{
    catalog_fingerprint, inspect_endpoint, postgres_foreign_keys, PostgresConsistencyMode,
    PostgresEndpointConfig, PostgresForeignKey, PostgresForeignKeyState, PostgresSourceFactory,
    PostgresTargetFactory,
};
#[cfg(feature = "enterprise-migration-spike")]
use super::postgres_fence::{
    attest_postgres_write_fence, postgres_write_fence_is_released, release_postgres_write_fence,
    remove_attested_fence_objects, InstalledPostgresFence,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpikeArtifacts {
    pub plan: PathBuf,
    pub state: PathBuf,
    pub plan_hash: String,
    pub copied_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresExecutionReport {
    pub state: PathBuf,
    pub copied_rows: u64,
    pub committed_chunks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Some boundaries are constructed only by the opt-in fault feature.
enum ExecutionInterruption {
    AfterDdlPrepared,
    AfterDdlCommitted,
    AfterChunkPrepared,
    CommitUnknownAfterApply,
    AfterCommittedChunks(u64),
    AfterAllVerified,
    BeforeForeignKeyChecks,
    AfterForeignKeyPrepared,
    AfterForeignKeyCommitted,
    AfterFenceReleased,
}

/// Deterministic failure boundaries used by the opt-in real-engine matrix.
#[cfg(feature = "migration-fault-injection")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresExecutionInterruption {
    AfterDdlPrepared,
    AfterDdlCommitted,
    AfterChunkPrepared,
    CommitUnknownAfterApply,
    AfterCommittedChunks(u64),
    AfterAllVerified,
    BeforeForeignKeyChecks,
    AfterForeignKeyPrepared,
    AfterForeignKeyCommitted,
    AfterFenceReleased,
}

#[cfg(feature = "migration-fault-injection")]
impl From<PostgresExecutionInterruption> for ExecutionInterruption {
    fn from(value: PostgresExecutionInterruption) -> Self {
        match value {
            PostgresExecutionInterruption::AfterDdlPrepared => Self::AfterDdlPrepared,
            PostgresExecutionInterruption::AfterDdlCommitted => Self::AfterDdlCommitted,
            PostgresExecutionInterruption::AfterChunkPrepared => Self::AfterChunkPrepared,
            PostgresExecutionInterruption::CommitUnknownAfterApply => Self::CommitUnknownAfterApply,
            PostgresExecutionInterruption::AfterCommittedChunks(count) => {
                Self::AfterCommittedChunks(count)
            }
            PostgresExecutionInterruption::AfterAllVerified => Self::AfterAllVerified,
            PostgresExecutionInterruption::BeforeForeignKeyChecks => Self::BeforeForeignKeyChecks,
            PostgresExecutionInterruption::AfterForeignKeyPrepared => Self::AfterForeignKeyPrepared,
            PostgresExecutionInterruption::AfterForeignKeyCommitted => {
                Self::AfterForeignKeyCommitted
            }
            PostgresExecutionInterruption::AfterFenceReleased => Self::AfterFenceReleased,
        }
    }
}

/// Inputs for one fault-injected PostgreSQL execution.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub struct PostgresInterruptedExecution<'a> {
    pub plan_path: &'a Path,
    pub source_config_path: &'a Path,
    pub target_config_path: &'a Path,
    pub fence_admin_config_path: &'a Path,
    pub fence_artifact_path: &'a Path,
    pub approval_reference: &'a str,
    pub state_path: &'a Path,
    pub interruption: PostgresExecutionInterruption,
}

#[cfg(feature = "enterprise-migration-spike")]
pub fn execute_postgres_plan(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_plan_internal(
        plan_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        approval_reference,
        state_path.as_ref(),
        None,
        None,
    )
}

#[cfg(feature = "enterprise-migration-spike")]
pub fn execute_postgres_fenced_plan(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    fence_admin_config_path: impl AsRef<Path>,
    fence_artifact_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_plan_internal(
        plan_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        approval_reference,
        state_path.as_ref(),
        Some((
            fence_admin_config_path.as_ref(),
            fence_artifact_path.as_ref(),
        )),
        None,
    )
}

/// Execute with a deterministic interruption after a durable chunk boundary.
///
/// This exists only to drive the real-engine crash matrix for the spike.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
#[allow(clippy::too_many_arguments)] // Mirrors the operator API plus one deterministic test boundary.
pub fn execute_postgres_fenced_plan_with_interruption(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    fence_admin_config_path: impl AsRef<Path>,
    fence_artifact_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
    after_committed_chunks: u64,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_interrupted(PostgresInterruptedExecution {
        plan_path: plan_path.as_ref(),
        source_config_path: source_config_path.as_ref(),
        target_config_path: target_config_path.as_ref(),
        fence_admin_config_path: fence_admin_config_path.as_ref(),
        fence_artifact_path: fence_artifact_path.as_ref(),
        approval_reference,
        state_path: state_path.as_ref(),
        interruption: PostgresExecutionInterruption::AfterCommittedChunks(after_committed_chunks),
    })
}

/// Execute until one exact recovery boundary and return an injected error.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub fn execute_postgres_interrupted(
    request: PostgresInterruptedExecution<'_>,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_plan_internal(
        request.plan_path,
        request.source_config_path,
        request.target_config_path,
        request.approval_reference,
        request.state_path,
        Some((request.fence_admin_config_path, request.fence_artifact_path)),
        Some(request.interruption.into()),
    )
}

/// Resume a PostgreSQL write-fenced migration from its protected state.
///
/// The reviewed plan is read only from the state artifact. Resume never
/// rebuilds intent from either live database.
#[cfg(feature = "enterprise-migration-spike")]
pub fn resume_postgres_fenced_plan(
    state_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    fence_admin_config_path: impl AsRef<Path>,
    fence_artifact_path: impl AsRef<Path>,
) -> anyhow::Result<PostgresExecutionReport> {
    resume_postgres_fenced_plan_internal(
        state_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        fence_admin_config_path.as_ref(),
        fence_artifact_path.as_ref(),
    )
}

#[cfg(feature = "enterprise-migration-spike")]
fn resume_postgres_fenced_plan_internal(
    state_path: &Path,
    source_config_path: &Path,
    target_config_path: &Path,
    fence_admin_config_path: &Path,
    fence_artifact_path: &Path,
) -> anyhow::Result<PostgresExecutionReport> {
    let mut state: MigrationState = read_json(state_path)?;
    let reviewed = state
        .reviewed_plan
        .clone()
        .ok_or_else(|| anyhow!("migration state has no embedded reviewed plan"))?;
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    validate_postgres_execution_operations(&reviewed)?;
    validate_resume_plan_binding(&state, &reviewed)?;
    state.validate_for_operations(
        reviewed
            .plan
            .operations
            .iter()
            .map(|operation| operation.id.as_str()),
    )?;
    if state.status == MigrationStatus::Completed {
        return Err(anyhow!("completed migration state cannot be resumed"));
    }
    if state.status == MigrationStatus::ManualReconciliationRequired {
        return Err(anyhow!(
            "migration requires manual reconciliation and cannot resume automatically"
        ));
    }

    let installed: InstalledPostgresFence = read_json(fence_artifact_path)?;
    if installed.evidence != state.binding.consistency_evidence {
        return Err(anyhow!(
            "write-fence artifact differs from the state consistency evidence"
        ));
    }
    if !matches!(installed.evidence, ConsistencyEvidence::WriteFence { .. }) {
        return Err(anyhow!("resume requires write-fence consistency evidence"));
    }
    let source_config = PostgresEndpointConfig::read(source_config_path)?;
    let target_config = PostgresEndpointConfig::read(target_config_path)?;
    let admin_config = PostgresEndpointConfig::read(fence_admin_config_path)?;
    validate_separate_postgres_credentials(&source_config, &target_config, &admin_config)?;
    validate_reviewed_tls(&reviewed, "source_tls", &source_config)?;
    validate_reviewed_tls(&reviewed, "target_tls", &target_config)?;
    if admin_config.tls.insecure {
        return Err(anyhow!(
            "fence administration cannot resume with insecure TLS"
        ));
    }
    if postgres_write_fence_is_released(&admin_config, &installed.evidence)? {
        let mut completed = state.clone();
        completed.finalize().context(
            "released fence is not paired with a fully verified durable migration state",
        )?;
        let copied_rows = completed
            .chunks
            .iter()
            .try_fold(0_u64, |total, chunk| total.checked_add(chunk.row_count))
            .ok_or_else(|| anyhow!("committed row count overflow"))?;
        replace_json(state_path, &completed)?;
        return Ok(PostgresExecutionReport {
            state: state_path.to_path_buf(),
            copied_rows,
            committed_chunks: u64::try_from(completed.chunks.len())?,
        });
    }
    let fence_inventory = attest_postgres_write_fence(&admin_config, &installed.evidence)?;

    let source = PostgresSourceFactory::new(source_config.clone());
    source.capabilities().require(&[
        "consistent_snapshot",
        "server_read_only",
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
    ])?;
    let snapshot = source.capture_snapshot()?;
    if snapshot.endpoint_identity != state.binding.source_endpoint
        || snapshot.endpoint_identity != reviewed.plan.source_endpoint_identity
    {
        return Err(anyhow!("source endpoint differs from durable binding"));
    }
    let (mut source_catalog, mut unsupported, _) = source.captured_catalog(&snapshot)?;
    remove_attested_fence_objects(&mut source_catalog, &mut unsupported, &fence_inventory)?;
    if unsupported.blocks_execution() {
        return Err(anyhow!(
            "resumed source contains execution-blocking unsupported semantics"
        ));
    }
    let source_fingerprint = catalog_fingerprint(&source_catalog)?;
    if source_fingerprint != state.binding.source_schema_fingerprint
        || source_fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&source_catalog)
    {
        return Err(anyhow!("resumed source catalog differs from reviewed plan"));
    }

    let target_snapshot = inspect_endpoint(&target_config)?;
    if target_snapshot.endpoint_identity != state.binding.target_endpoint
        || target_snapshot.endpoint_identity != reviewed.plan.target_endpoint_identity
    {
        return Err(anyhow!("target endpoint differs from durable binding"));
    }
    let target = PostgresTargetFactory::new(target_config.clone());
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
        "plain_insert",
    ])?;

    let cancellation = CancellationToken::default();
    let mut reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced || reader.snapshot() != &snapshot {
        return Err(anyhow!("resumed source snapshot evidence is invalid"));
    }

    resume_pre_data_schema(
        ResumeDdlContext {
            reviewed: &reviewed,
            source_catalog: &source_catalog,
            target: &target,
            target_config: &target_config,
            admin: &admin_config,
            installed: &installed,
        },
        &mut state,
        state_path,
    )?;

    let mut next_chunk_id = state.next_chunk_id()?;
    let mut copied_rows = state
        .chunks
        .iter()
        .filter(|chunk| chunk.state == ChunkState::Committed)
        .try_fold(0_u64, |total, chunk| total.checked_add(chunk.row_count))
        .ok_or_else(|| anyhow!("committed row count overflow"))?;

    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(&source_catalog, table)?;
        let operation_state = operation_state(&state, operation.id.as_str())?;
        if matches!(
            operation_state,
            OperationState::Committed | OperationState::Verified
        ) {
            continue;
        }
        if operation_state == OperationState::Pending {
            state.start_operation(operation.id.as_str())?;
            replace_json(state_path, &state)?;
        } else if operation_state != OperationState::Running {
            return Err(anyhow!("copy operation has an invalid resumable state"));
        }

        if let Some(prepared) = state.prepared_chunk()?.cloned() {
            if prepared.operation_id != operation.id.as_str() {
                return Err(anyhow!("prepared chunk belongs to a different operation"));
            }
            attest_exact_fence(&admin_config, &installed, &fence_inventory)?;
            let expected = reader.select_page(&KeysetPage {
                table: table.clone(),
                projection: shape.projection.clone(),
                key: shape.key.clone(),
                after: prepared.start_key.clone().map(KeyTuple::new),
                limit: u32::try_from(prepared.row_count)
                    .context("prepared chunk row count exceeds PostgreSQL page limit")?,
            })?;
            if u64::try_from(expected.len())? != prepared.row_count
                || batch_final_key(&expected, &shape)?.0 != prepared.final_key
                || batch_digest(table, &shape, &expected)? != prepared.canonical_digest
            {
                return Err(anyhow!(
                    "source rows no longer match the durable prepared chunk"
                ));
            }
            match reconcile_live_prepared_chunk(
                &target,
                &mut state,
                state_path,
                table,
                &shape,
                &expected,
                cancellation.clone(),
            )? {
                PreparedResolution::MarkedCommitted => {}
                PreparedResolution::RetryRequired => {
                    let mut writer = target.open_writer(cancellation.clone())?;
                    writer.begin()?;
                    if let Err(error) = writer.insert(table, &expected) {
                        let _ = writer.rollback();
                        return Err(error.into());
                    }
                    writer.commit()?;
                    state.commit(prepared.chunk_id)?;
                    replace_json(state_path, &state)?;
                }
                PreparedResolution::ManualReconciliationRequired => {
                    return Err(anyhow!(
                        "prepared target interval differs from durable intent"
                    ));
                }
            }
            copied_rows = copied_rows
                .checked_add(prepared.row_count)
                .ok_or_else(|| anyhow!("committed row count overflow"))?;
        }

        let mut after = state.resume_cursor(operation.id.as_str())?;
        loop {
            attest_exact_fence(&admin_config, &installed, &fence_inventory)?;
            let batch = reader.select_page(&KeysetPage {
                table: table.clone(),
                projection: shape.projection.clone(),
                key: shape.key.clone(),
                after: after.clone(),
                limit: execution_page_limit(&source_config)?,
            })?;
            if batch.is_empty() {
                break;
            }
            let final_key = batch_final_key(&batch, &shape)?;
            state.prepare(ChunkRecord {
                chunk_id: next_chunk_id,
                operation_id: operation.id.to_string(),
                start_key: after.as_ref().map(|key| key.0.clone()),
                final_key: final_key.0.clone(),
                row_count: u64::try_from(batch.len())?,
                canonical_digest: batch_digest(table, &shape, &batch)?,
                target_transaction_intent: format!(
                    "{}:{}:{next_chunk_id}",
                    reviewed.plan_hash, operation.id
                ),
                state: ChunkState::Prepared,
            })?;
            replace_json(state_path, &state)?;
            let mut writer = target.open_writer(cancellation.clone())?;
            writer.begin()?;
            if let Err(error) = writer.insert(table, &batch) {
                let _ = writer.rollback();
                return Err(error.into());
            }
            match writer.commit() {
                Ok(()) => state.commit(next_chunk_id)?,
                Err(ConnectionError::CommitOutcomeUnknown(_)) => {
                    match reconcile_live_prepared_chunk(
                        &target,
                        &mut state,
                        state_path,
                        table,
                        &shape,
                        &batch,
                        cancellation.clone(),
                    )? {
                        PreparedResolution::MarkedCommitted => {}
                        PreparedResolution::RetryRequired => {
                            return Err(anyhow!(
                                "commit was absent; restart resume to retry durable intent"
                            ));
                        }
                        PreparedResolution::ManualReconciliationRequired => {
                            return Err(anyhow!("commit effect differs from durable intent"));
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
            replace_json(state_path, &state)?;
            copied_rows = copied_rows
                .checked_add(u64::try_from(batch.len())?)
                .ok_or_else(|| anyhow!("copied row count overflow"))?;
            next_chunk_id = next_chunk_id
                .checked_add(1)
                .ok_or_else(|| anyhow!("chunk identifier overflow"))?;
            after = Some(final_key);
        }
        state.commit_operation(operation.id.as_str())?;
        replace_json(state_path, &state)?;
    }

    if state.status == MigrationStatus::Running {
        state.begin_verification()?;
        replace_json(state_path, &state)?;
    } else if state.status != MigrationStatus::Verifying {
        return Err(anyhow!("migration status cannot resume verification"));
    }
    attest_exact_fence(&admin_config, &installed, &fence_inventory)?;
    let mut verifier = target.open_verifier(cancellation)?;
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(&source_catalog, table)?;
        verify_live_table(
            reader.as_mut(),
            verifier.as_mut(),
            &state,
            operation.id.as_str(),
            table,
            &shape,
            execution_page_limit(&source_config)?,
        )?;
        if operation_state(&state, operation.id.as_str())? != OperationState::Verified {
            state.verify_operation(operation.id.as_str())?;
        }
        let verify = reviewed
            .plan
            .operations
            .iter()
            .find(|candidate| {
                candidate.kind == OperationKind::VerifyTable
                    && candidate.table.as_ref() == Some(table)
            })
            .ok_or_else(|| anyhow!("table has no reviewed verification operation"))?;
        complete_operation_if_needed(&mut state, verify.id.as_str())?;
        replace_json(state_path, &state)?;
    }
    process_postgres_foreign_keys(
        &reviewed,
        &source_catalog,
        &target,
        &mut state,
        state_path,
        None,
        || attest_exact_fence(&admin_config, &installed, &fence_inventory),
    )?;
    verify_schema_projection(&source_catalog, &target_config)?;
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("reviewed plan has no schema verification operation"))?;
    complete_operation_if_needed(&mut state, verify_schema.id.as_str())?;
    replace_json(state_path, &state)?;
    attest_exact_fence(&admin_config, &installed, &fence_inventory)?;
    let mut completed = state.clone();
    completed.finalize()?;
    drop(reader);
    let generation = match &installed.evidence {
        ConsistencyEvidence::WriteFence { generation, .. } => generation,
        ConsistencyEvidence::NativeSnapshot { .. } => unreachable!("validated above"),
    };
    release_postgres_write_fence(&admin_config, generation, &installed.token)?;
    replace_json(state_path, &completed)?;
    Ok(PostgresExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: u64::try_from(completed.chunks.len())?,
    })
}

#[cfg(feature = "enterprise-migration-spike")]
fn execute_postgres_plan_internal(
    plan_path: &Path,
    source_config_path: &Path,
    target_config_path: &Path,
    approval_reference: &str,
    state_path: &Path,
    fence_paths: Option<(&Path, &Path)>,
    interruption: Option<ExecutionInterruption>,
) -> anyhow::Result<PostgresExecutionReport> {
    if approval_reference.trim().is_empty() {
        return Err(anyhow!("approval reference must not be empty"));
    }
    let reviewed: ReviewedPlan = read_json(plan_path)?;
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    validate_postgres_execution_operations(&reviewed)?;
    let source_config = PostgresEndpointConfig::read(source_config_path)?;
    let target_config = PostgresEndpointConfig::read(target_config_path)?;
    if source_config.credential_env == target_config.credential_env {
        return Err(anyhow!(
            "source and target must use separate credential references"
        ));
    }
    let fenced = match (reviewed.plan.consistency_mode.as_str(), fence_paths) {
        ("consistent-snapshot", None) => None,
        ("write-fence", Some((admin_path, artifact_path))) => {
            let admin = PostgresEndpointConfig::read(admin_path)?;
            if [
                source_config.credential_env.as_str(),
                target_config.credential_env.as_str(),
            ]
            .contains(&admin.credential_env.as_str())
            {
                return Err(anyhow!(
                    "fence administration must use a separate credential reference"
                ));
            }
            let installed: InstalledPostgresFence = read_json(artifact_path)?;
            let inventory = attest_postgres_write_fence(&admin, &installed.evidence)?;
            Some((admin, installed, inventory))
        }
        ("consistent-snapshot", Some(_)) => {
            return Err(anyhow!(
                "a consistent-snapshot plan must not accept write-fence evidence"
            ));
        }
        ("write-fence", None) => {
            return Err(anyhow!(
                "reviewed plan requires a protected, active PostgreSQL write fence"
            ));
        }
        _ => return Err(anyhow!("reviewed plan has an unsupported consistency mode")),
    };

    let source = PostgresSourceFactory::new(source_config.clone());
    source.capabilities().require(&[
        "consistent_snapshot",
        "server_read_only",
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
    ])?;
    let snapshot = source.capture_snapshot()?;
    if snapshot.endpoint_identity != reviewed.plan.source_endpoint_identity {
        return Err(anyhow!(
            "source endpoint identity differs from reviewed plan"
        ));
    }
    let (mut source_catalog, mut execution_unsupported, mut source_fingerprint) =
        source.captured_catalog(&snapshot)?;
    if let Some((_, _, inventory)) = &fenced {
        remove_attested_fence_objects(&mut source_catalog, &mut execution_unsupported, inventory)?;
        source_fingerprint = catalog_fingerprint(&source_catalog)?;
    }
    if execution_unsupported.blocks_execution() {
        return Err(anyhow!(
            "fresh source snapshot contains execution-blocking unsupported semantics"
        ));
    }
    if source_fingerprint != reviewed.plan.source_catalog_fingerprint {
        return Err(anyhow!(
            "source catalog fingerprint changed after plan review"
        ));
    }
    if reviewed.plan.source_catalog.as_ref() != Some(&source_catalog) {
        return Err(anyhow!(
            "fresh source catalog differs from embedded reviewed catalog"
        ));
    }

    let target_preflight = inspect_endpoint(&target_config)?;
    if target_preflight.endpoint_identity != reviewed.plan.target_endpoint_identity {
        return Err(anyhow!(
            "target endpoint identity differs from reviewed plan"
        ));
    }
    if catalog_fingerprint(&target_preflight.catalog)? != reviewed.plan.target_catalog_fingerprint {
        return Err(anyhow!(
            "target catalog fingerprint changed after plan review"
        ));
    }
    let target = PostgresTargetFactory::new(target_config.clone());
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
        "plain_insert",
    ])?;
    target.assert_empty_and_owned()?;

    let binding = ResumeBinding {
        migration_id: reviewed.plan.migration_id.clone(),
        plan_hash: reviewed.plan_hash.to_string(),
        approval_reference: approval_reference.into(),
        tool_version: reviewed.plan.tool_version.clone(),
        source_endpoint: snapshot.endpoint_identity.clone(),
        target_endpoint: target_preflight.endpoint_identity,
        consistency_evidence: fenced.as_ref().map_or_else(
            || ConsistencyEvidence::NativeSnapshot {
                endpoint_identity: snapshot.endpoint_identity.clone(),
                database_identity: snapshot.database_identity.clone(),
                lifecycle_id: snapshot.lifecycle_id.clone(),
                snapshot_id: snapshot.snapshot_id.clone(),
                server_version: snapshot.server_version.clone(),
            },
            |(_, installed, _)| installed.evidence.clone(),
        ),
        source_schema_fingerprint: source_fingerprint,
        target_schema_fingerprint: reviewed.plan.target_catalog_fingerprint.clone(),
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    };
    let mut state = MigrationState::with_operations(
        binding,
        reviewed.plan.operations.iter().map(|operation| {
            (
                operation.id.to_string(),
                operation
                    .dependencies
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            )
        }),
    )?;
    state.bind_reviewed_plan(reviewed.clone())?;
    state.validate_for_operations(
        reviewed
            .plan
            .operations
            .iter()
            .map(|operation| operation.id.as_str()),
    )?;
    write_json_new(state_path, &state)?;

    let cancellation = CancellationToken::default();
    let mut reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced || reader.snapshot() != &snapshot {
        return Err(anyhow!(
            "source snapshot evidence failed after state creation"
        ));
    }

    let create_operation_ids = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CreateTable)
        .map(|operation| operation.id.as_str())
        .collect::<Vec<_>>();
    state.prepare_operations_atomic(create_operation_ids.iter().copied())?;
    replace_json(state_path, &state)?;
    interrupt_if(interruption, ExecutionInterruption::AfterDdlPrepared)?;
    attest_fence_if_present(fenced.as_ref())?;
    if let Err(initial_error) = target.create_pre_data_schema(&source_catalog) {
        if target_schema_matches(&source_catalog, &target_config)? {
            // The atomic DDL transaction committed, but its acknowledgement was lost.
        } else if target.assert_empty_and_owned().is_ok() {
            target.create_pre_data_schema(&source_catalog).with_context(|| {
                format!(
                    "retry create-only schema after an unacknowledged empty-target outcome ({initial_error})"
                )
            })?;
        } else {
            state.require_manual_reconciliation()?;
            replace_json(state_path, &state)?;
            return Err(anyhow!(
                "pre-data DDL has a partial or different target effect; manual reconciliation is required: {initial_error}"
            ));
        }
    }
    interrupt_if(interruption, ExecutionInterruption::AfterDdlCommitted)?;
    for operation_id in create_operation_ids {
        state.commit_prepared_operation(operation_id)?;
        state.verify_operation(operation_id)?;
    }
    replace_json(state_path, &state)?;

    let mut next_chunk_id = 1u64;
    let mut copied_rows = 0u64;
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(&source_catalog, table)?;
        state.start_operation(operation.id.as_str())?;
        replace_json(state_path, &state)?;
        let mut after = None;
        loop {
            attest_fence_if_present(fenced.as_ref())?;
            let batch = reader.select_page(&KeysetPage {
                table: table.clone(),
                projection: shape.projection.clone(),
                key: shape.key.clone(),
                after: after.clone(),
                limit: execution_page_limit(&source_config)?,
            })?;
            if batch.is_empty() {
                break;
            }
            let final_key = batch_final_key(&batch, &shape)?;
            let digest = batch_digest(table, &shape, &batch)?;
            let chunk_id = next_chunk_id;
            next_chunk_id = next_chunk_id
                .checked_add(1)
                .ok_or_else(|| anyhow!("chunk identifier overflow"))?;
            state.prepare(ChunkRecord {
                chunk_id,
                operation_id: operation.id.to_string(),
                start_key: after.as_ref().map(|key: &KeyTuple| key.0.clone()),
                final_key: final_key.0.clone(),
                row_count: u64::try_from(batch.len()).context("batch row count exceeds u64")?,
                canonical_digest: digest,
                target_transaction_intent: format!(
                    "{}:{}:{chunk_id}",
                    reviewed.plan_hash, operation.id
                ),
                state: ChunkState::Prepared,
            })?;
            replace_json(state_path, &state)?;
            if matches!(
                interruption,
                Some(ExecutionInterruption::AfterChunkPrepared)
            ) {
                return Err(injected_interruption(interruption));
            }
            let mut writer = target.open_writer(cancellation.clone())?;
            writer.begin()?;
            if let Err(error) = writer.insert(table, &batch) {
                let _ = writer.rollback();
                return Err(error.into());
            }
            if let Err(error) = writer.commit() {
                if !matches!(error, ConnectionError::CommitOutcomeUnknown(_)) {
                    return Err(error.into());
                }
                match reconcile_live_prepared_chunk(
                    &target,
                    &mut state,
                    state_path,
                    table,
                    &shape,
                    &batch,
                    cancellation.clone(),
                )? {
                    PreparedResolution::MarkedCommitted => {}
                    PreparedResolution::RetryRequired => {
                        let mut retry_writer = target.open_writer(cancellation.clone())?;
                        retry_writer.begin()?;
                        if let Err(error) = retry_writer.insert(table, &batch) {
                            let _ = retry_writer.rollback();
                            return Err(error.into());
                        }
                        retry_writer.commit()?;
                        state.commit(chunk_id)?;
                    }
                    PreparedResolution::ManualReconciliationRequired => {
                        return Err(anyhow!(
                            "prepared target interval differs from its durable intent; manual reconciliation is required"
                        ));
                    }
                }
            } else if interruption == Some(ExecutionInterruption::CommitUnknownAfterApply) {
                return Err(injected_interruption(interruption));
            } else {
                state.commit(chunk_id)?;
            }
            replace_json(state_path, &state)?;
            copied_rows = copied_rows
                .checked_add(u64::try_from(batch.len()).context("batch size exceeds u64")?)
                .ok_or_else(|| anyhow!("copied row count overflow"))?;
            if interruption.is_some_and(|point| {
                let ExecutionInterruption::AfterCommittedChunks(limit) = point else {
                    return false;
                };
                state
                    .chunks
                    .iter()
                    .filter(|chunk| chunk.state == ChunkState::Committed)
                    .count()
                    >= usize::try_from(limit).unwrap_or(usize::MAX)
            }) {
                return Err(anyhow!(
                    "injected interruption after a durable committed chunk"
                ));
            }
            after = Some(final_key);
        }
        state.commit_operation(operation.id.as_str())?;
        replace_json(state_path, &state)?;
    }

    state.begin_verification()?;
    replace_json(state_path, &state)?;
    attest_fence_if_present(fenced.as_ref())?;
    let mut verifier = target.open_verifier(cancellation)?;
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("validated copy operation lost its table"))?;
        let shape = table_shape(&source_catalog, table)?;
        verify_live_table(
            reader.as_mut(),
            verifier.as_mut(),
            &state,
            operation.id.as_str(),
            table,
            &shape,
            execution_page_limit(&source_config)?,
        )?;
        state.verify_operation(operation.id.as_str())?;
        let verify_operation = reviewed
            .plan
            .operations
            .iter()
            .find(|candidate| {
                candidate.kind == OperationKind::VerifyTable
                    && candidate.table.as_ref() == Some(table)
            })
            .ok_or_else(|| anyhow!("table has no reviewed verification operation"))?;
        state.start_operation(verify_operation.id.as_str())?;
        state.commit_operation(verify_operation.id.as_str())?;
        state.verify_operation(verify_operation.id.as_str())?;
        replace_json(state_path, &state)?;
    }
    process_postgres_foreign_keys(
        &reviewed,
        &source_catalog,
        &target,
        &mut state,
        state_path,
        interruption,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    verify_schema_projection(&source_catalog, &target_config)?;
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("reviewed plan has no schema verification operation"))?;
    state.start_operation(verify_schema.id.as_str())?;
    state.commit_operation(verify_schema.id.as_str())?;
    state.verify_operation(verify_schema.id.as_str())?;
    replace_json(state_path, &state)?;
    interrupt_if(interruption, ExecutionInterruption::AfterAllVerified)?;
    attest_fence_if_present(fenced.as_ref())?;
    let mut completed_state = state.clone();
    completed_state.finalize()?;
    drop(reader);
    if let Some((admin, installed, _)) = &fenced {
        let generation = match &installed.evidence {
            ConsistencyEvidence::WriteFence { generation, .. } => generation,
            ConsistencyEvidence::NativeSnapshot { .. } => {
                return Err(anyhow!("fence artifact contains native snapshot evidence"));
            }
        };
        release_postgres_write_fence(admin, generation, &installed.token)?;
    }
    interrupt_if(interruption, ExecutionInterruption::AfterFenceReleased)?;
    state = completed_state;
    replace_json(state_path, &state)?;
    Ok(PostgresExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: next_chunk_id - 1,
    })
}

#[cfg(feature = "enterprise-migration-spike")]
fn attest_fence_if_present(
    fenced: Option<&(
        PostgresEndpointConfig,
        InstalledPostgresFence,
        super::postgres_fence::FenceInventory,
    )>,
) -> anyhow::Result<()> {
    if let Some((admin, installed, expected_inventory)) = fenced {
        let observed = attest_postgres_write_fence(admin, &installed.evidence)?;
        if &observed != expected_inventory {
            return Err(anyhow!(
                "PostgreSQL fence inventory changed during execution"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn interrupt_if(
    configured: Option<ExecutionInterruption>,
    boundary: ExecutionInterruption,
) -> anyhow::Result<()> {
    if configured == Some(boundary) {
        return Err(injected_interruption(configured));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn injected_interruption(configured: Option<ExecutionInterruption>) -> anyhow::Error {
    anyhow!("injected interruption at PostgreSQL execution boundary {configured:?}")
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_resume_plan_binding(
    state: &MigrationState,
    reviewed: &ReviewedPlan,
) -> anyhow::Result<()> {
    if reviewed.plan.consistency_mode != PostgresConsistencyMode::WriteFence.as_str() {
        return Err(anyhow!("only a write-fence plan can be resumed"));
    }
    if state.binding.migration_id != reviewed.plan.migration_id
        || state.binding.plan_hash != reviewed.plan_hash.to_string()
        || state.binding.tool_version != reviewed.plan.tool_version
        || state.binding.source_endpoint != reviewed.plan.source_endpoint_identity
        || state.binding.target_endpoint != reviewed.plan.target_endpoint_identity
        || state.binding.source_schema_fingerprint != reviewed.plan.source_catalog_fingerprint
        || state.binding.target_schema_fingerprint != reviewed.plan.target_catalog_fingerprint
        || state.binding.conversion_policy != reviewed.plan.conversion_policy
        || state.binding.canonical_encoding_version != CANONICAL_ENCODING_VERSION
        || reviewed.plan.canonical_encoding_version != CANONICAL_ENCODING_VERSION
        || state.binding.tool_version != env!("CARGO_PKG_VERSION")
    {
        return Err(anyhow!(
            "migration state binding differs from reviewed plan or running tool"
        ));
    }
    if state.binding.approval_reference.trim().is_empty() {
        return Err(anyhow!("migration state has no approval reference"));
    }
    let ConsistencyEvidence::WriteFence {
        endpoint_identity,
        business_catalog_fingerprint,
        ..
    } = &state.binding.consistency_evidence
    else {
        return Err(anyhow!("migration state is not bound to a write fence"));
    };
    if endpoint_identity != &reviewed.plan.source_endpoint_identity
        || business_catalog_fingerprint != &reviewed.plan.source_catalog_fingerprint
    {
        return Err(anyhow!("write-fence binding differs from reviewed source"));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_separate_postgres_credentials(
    source: &PostgresEndpointConfig,
    target: &PostgresEndpointConfig,
    admin: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let credentials = [
        source.credential_env.as_str(),
        target.credential_env.as_str(),
        admin.credential_env.as_str(),
    ];
    if credentials[0] == credentials[1]
        || credentials[0] == credentials[2]
        || credentials[1] == credentials[2]
    {
        return Err(anyhow!(
            "source, target, and fence administration require separate credential references"
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_reviewed_tls(
    reviewed: &ReviewedPlan,
    capability: &str,
    config: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let expected = reviewed
        .plan
        .capabilities
        .get(capability)
        .ok_or_else(|| anyhow!("reviewed plan omits {capability}"))?;
    let observed = if config.tls.insecure {
        "insecure_explicit"
    } else {
        "hostname_verified"
    };
    if expected != observed {
        return Err(anyhow!("{capability} differs from the reviewed TLS policy"));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn attest_exact_fence(
    admin: &PostgresEndpointConfig,
    installed: &InstalledPostgresFence,
    expected: &super::postgres_fence::FenceInventory,
) -> anyhow::Result<()> {
    let observed = attest_postgres_write_fence(admin, &installed.evidence)?;
    if &observed != expected {
        return Err(anyhow!("PostgreSQL fence inventory changed during resume"));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn operation_state(state: &MigrationState, operation_id: &str) -> anyhow::Result<OperationState> {
    state
        .operations
        .iter()
        .find(|operation| operation.operation_id == operation_id)
        .map(|operation| operation.state)
        .ok_or_else(|| anyhow!("migration state has no reviewed operation {operation_id}"))
}

#[cfg(feature = "enterprise-migration-spike")]
fn complete_operation_if_needed(
    state: &mut MigrationState,
    operation_id: &str,
) -> anyhow::Result<()> {
    match operation_state(state, operation_id)? {
        OperationState::Pending => {
            state.start_operation(operation_id)?;
            state.commit_operation(operation_id)?;
            state.verify_operation(operation_id)?;
        }
        OperationState::Running => {
            state.commit_operation(operation_id)?;
            state.verify_operation(operation_id)?;
        }
        OperationState::Committed => state.verify_operation(operation_id)?,
        OperationState::Verified => {}
        OperationState::Prepared => {
            return Err(anyhow!(
                "verification operation has an unexpected prepared state"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
struct ResumeDdlContext<'a> {
    reviewed: &'a ReviewedPlan,
    source_catalog: &'a VendorCatalog,
    target: &'a PostgresTargetFactory,
    target_config: &'a PostgresEndpointConfig,
    admin: &'a PostgresEndpointConfig,
    installed: &'a InstalledPostgresFence,
}

#[cfg(feature = "enterprise-migration-spike")]
fn resume_pre_data_schema(
    context: ResumeDdlContext<'_>,
    state: &mut MigrationState,
    state_path: &Path,
) -> anyhow::Result<()> {
    let create_ids = context
        .reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CreateTable)
        .map(|operation| operation.id.as_str())
        .collect::<Vec<_>>();
    let states = create_ids
        .iter()
        .map(|id| operation_state(state, id))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if states
        .iter()
        .all(|state| *state == OperationState::Verified)
    {
        if !target_schema_matches(context.source_catalog, context.target_config)? {
            return Err(anyhow!("verified pre-data schema differs on resume"));
        }
        return Ok(());
    }
    attest_postgres_write_fence(context.admin, &context.installed.evidence)?;
    let schema_exists_exactly =
        target_schema_matches(context.source_catalog, context.target_config)?;
    if states.iter().all(|state| *state == OperationState::Pending) {
        if schema_exists_exactly {
            return Err(anyhow!(
                "target schema exists without a durable prepared DDL intent"
            ));
        }
        context.target.assert_empty_and_owned()?;
        state.prepare_operations_atomic(create_ids.iter().copied())?;
        replace_json(state_path, state)?;
        context
            .target
            .create_pre_data_schema(context.source_catalog)?;
    } else if states
        .iter()
        .all(|state| *state == OperationState::Prepared)
    {
        if schema_exists_exactly
            && !target_planned_tables_are_empty(context.source_catalog, context.target)?
        {
            state.require_manual_reconciliation()?;
            replace_json(state_path, state)?;
            return Err(anyhow!(
                "prepared DDL target contains rows outside durable migration intent"
            ));
        }
        if !schema_exists_exactly {
            if context.target.assert_empty_and_owned().is_ok() {
                context
                    .target
                    .create_pre_data_schema(context.source_catalog)?;
            } else {
                state.require_manual_reconciliation()?;
                replace_json(state_path, state)?;
                return Err(anyhow!(
                    "prepared DDL has a partial or different target effect"
                ));
            }
        }
    } else {
        return Err(anyhow!(
            "create-table operations have inconsistent atomic DDL states"
        ));
    }
    for operation_id in create_ids {
        state.commit_prepared_operation(operation_id)?;
        state.verify_operation(operation_id)?;
    }
    replace_json(state_path, state)?;
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn target_planned_tables_are_empty(
    source_catalog: &VendorCatalog,
    target: &dyn TargetConnectionFactory,
) -> anyhow::Result<bool> {
    let mut verifier = target.open_verifier(CancellationToken::default())?;
    for namespace in &source_catalog.namespaces {
        for object in namespace
            .objects
            .iter()
            .filter(|object| object.kind == super::model::CatalogObjectKind::Table)
        {
            let table = QualifiedTable {
                namespace: namespace.name.clone(),
                name: object.name.clone(),
            };
            let shape = table_shape(source_catalog, &table)?;
            let rows = verifier.select_page(&KeysetPage {
                table,
                projection: shape.projection,
                key: shape.key,
                after: None,
                limit: 1,
            })?;
            if !rows.is_empty() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[cfg(feature = "enterprise-migration-spike")]
fn process_postgres_foreign_keys(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    state: &mut MigrationState,
    state_path: &Path,
    interruption: Option<ExecutionInterruption>,
    mut attest: impl FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let foreign_keys = postgres_foreign_keys(catalog)?
        .into_iter()
        .map(|foreign_key| (foreign_key.catalog_object_id.clone(), foreign_key))
        .collect::<std::collections::BTreeMap<_, _>>();
    interrupt_if(interruption, ExecutionInterruption::BeforeForeignKeyChecks)?;
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CheckForeignKey)
    {
        let foreign_key = foreign_key_for_operation(operation, &foreign_keys)?;
        match operation_state(state, operation.id.as_str())? {
            OperationState::Pending => {
                state.start_operation(operation.id.as_str())?;
                replace_json(state_path, state)?;
            }
            OperationState::Running | OperationState::Committed | OperationState::Verified => {}
            OperationState::Prepared => {
                return Err(anyhow!(
                    "foreign-key check operation has an invalid prepared state"
                ));
            }
        }
        attest()?;
        if target.check_foreign_key(foreign_key)?.has_violation {
            state.require_manual_reconciliation()?;
            replace_json(state_path, state)?;
            return Err(anyhow!(
                "target rows violate reviewed foreign key {}",
                foreign_key.name
            ));
        }
        match operation_state(state, operation.id.as_str())? {
            OperationState::Running => {
                state.commit_operation(operation.id.as_str())?;
                state.verify_operation(operation.id.as_str())?;
                replace_json(state_path, state)?;
            }
            OperationState::Committed => {
                state.verify_operation(operation.id.as_str())?;
                replace_json(state_path, state)?;
            }
            OperationState::Verified => {}
            OperationState::Pending | OperationState::Prepared => {
                return Err(anyhow!("foreign-key check did not enter a runnable state"));
            }
        }
    }
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::AddForeignKey)
    {
        let foreign_key = foreign_key_for_operation(operation, &foreign_keys)?;
        match operation_state(state, operation.id.as_str())? {
            OperationState::Pending => {
                state.prepare_operations_atomic([operation.id.as_str()])?;
                replace_json(state_path, state)?;
                interrupt_if(interruption, ExecutionInterruption::AfterForeignKeyPrepared)?;
            }
            OperationState::Prepared | OperationState::Verified => {}
            OperationState::Running | OperationState::Committed => {
                return Err(anyhow!(
                    "foreign-key add operation has an invalid durable state"
                ));
            }
        }
        attest()?;
        let observed = match operation_state(state, operation.id.as_str())? {
            OperationState::Prepared => match target.reconcile_foreign_key(foreign_key) {
                Ok(observed) => observed,
                Err(ConnectionError::InvalidRequest(reason)) => {
                    state.require_manual_reconciliation()?;
                    replace_json(state_path, state)?;
                    return Err(anyhow!(
                        "foreign-key reconciliation requires manual intervention: {reason}"
                    ));
                }
                Err(error) => return Err(error.into()),
            },
            OperationState::Verified => target.inspect_foreign_key(foreign_key)?,
            _ => unreachable!("validated foreign-key add state"),
        };
        interrupt_if(
            interruption,
            ExecutionInterruption::AfterForeignKeyCommitted,
        )?;
        if observed != PostgresForeignKeyState::ExactValidated {
            state.require_manual_reconciliation()?;
            replace_json(state_path, state)?;
            return Err(anyhow!(
                "target foreign key {} differs from reviewed semantics",
                foreign_key.name
            ));
        }
        if operation_state(state, operation.id.as_str())? == OperationState::Prepared {
            state.commit_prepared_operation(operation.id.as_str())?;
            state.verify_operation(operation.id.as_str())?;
            replace_json(state_path, state)?;
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn foreign_key_for_operation<'a>(
    operation: &super::plan::PlanOperation,
    foreign_keys: &'a std::collections::BTreeMap<String, PostgresForeignKey>,
) -> anyhow::Result<&'a PostgresForeignKey> {
    let object: CatalogObject = serde_json::from_value(
        operation
            .parameters
            .get("catalog_object")
            .cloned()
            .ok_or_else(|| anyhow!("foreign-key operation omits catalog_object"))?,
    )?;
    if object.kind != CatalogObjectKind::ForeignKey {
        return Err(anyhow!(
            "foreign-key operation contains a different catalog object kind"
        ));
    }
    let foreign_key = foreign_keys
        .get(&object.id)
        .ok_or_else(|| anyhow!("foreign-key operation refers to an unknown catalog object"))?;
    if operation.table.as_ref() != Some(&foreign_key.table) {
        return Err(anyhow!(
            "foreign-key operation table differs from reviewed catalog"
        ));
    }
    Ok(foreign_key)
}

#[cfg(feature = "enterprise-migration-spike")]
fn reconcile_live_prepared_chunk(
    target: &dyn TargetConnectionFactory,
    state: &mut MigrationState,
    state_path: &Path,
    table: &QualifiedTable,
    shape: &TableShape,
    expected: &RowBatch,
    cancellation: CancellationToken,
) -> anyhow::Result<PreparedResolution> {
    let chunk = state
        .prepared_chunk()?
        .cloned()
        .ok_or_else(|| anyhow!("commit ambiguity has no durable prepared chunk"))?;
    let observed_limit = chunk
        .row_count
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .ok_or_else(|| anyhow!("prepared chunk is too large to reconcile"))?;
    let mut verifier = target.open_verifier(cancellation)?;
    let observed = verifier.select_page(&KeysetPage {
        table: table.clone(),
        projection: shape.projection.clone(),
        key: shape.key.clone(),
        after: chunk.start_key.clone().map(KeyTuple::new),
        limit: observed_limit,
    })?;
    let (row_count, digest) = if observed.is_empty() {
        (0, String::new())
    } else {
        let final_key = batch_final_key(&observed, shape)?;
        let observed_count =
            u64::try_from(observed.len()).context("observed chunk row count exceeds u64")?;
        let exact = observed.rows() == expected.rows()
            && observed_count == chunk.row_count
            && final_key.0 == chunk.final_key;
        if exact {
            (observed_count, batch_digest(table, shape, &observed)?)
        } else {
            (observed_count, "different".into())
        }
    };
    let resolution = state.reconcile_prepared_evidence(&PreparedChunkEvidence {
        chunk_id: chunk.chunk_id,
        operation_id: chunk.operation_id,
        start_key: chunk.start_key,
        final_key: chunk.final_key,
        row_count,
        canonical_digest: digest,
        target_transaction_intent: chunk.target_transaction_intent,
    })?;
    if resolution != PreparedResolution::RetryRequired {
        replace_json(state_path, state)?;
    }
    Ok(resolution)
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_postgres_execution_operations(reviewed: &ReviewedPlan) -> anyhow::Result<()> {
    for operation in &reviewed.plan.operations {
        if !matches!(
            operation.kind,
            OperationKind::CreateTable
                | OperationKind::CopyTable
                | OperationKind::CheckForeignKey
                | OperationKind::AddForeignKey
                | OperationKind::VerifyTable
                | OperationKind::VerifySchema
        ) {
            return Err(anyhow!(
                "PostgreSQL live runner does not implement operation {:?}",
                operation.kind
            ));
        }
        if matches!(
            operation.kind,
            OperationKind::CreateTable
                | OperationKind::CopyTable
                | OperationKind::CheckForeignKey
                | OperationKind::AddForeignKey
                | OperationKind::VerifyTable
        ) && operation.table.is_none()
        {
            return Err(anyhow!("table operation has no table identity"));
        }
    }
    let copy_tables = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
        .filter_map(|operation| operation.table.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    let verify_tables = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::VerifyTable)
        .filter_map(|operation| operation.table.as_ref())
        .collect::<std::collections::BTreeSet<_>>();
    if copy_tables != verify_tables {
        return Err(anyhow!("copy and table-verification operation sets differ"));
    }
    let source_catalog = reviewed
        .plan
        .source_catalog
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed PostgreSQL plan has no embedded source catalog"))?;
    let expected_foreign_keys = postgres_foreign_keys(source_catalog)?
        .into_iter()
        .map(|foreign_key| foreign_key.catalog_object_id)
        .collect::<std::collections::BTreeSet<_>>();
    let operation_foreign_keys = |kind: OperationKind| -> anyhow::Result<_> {
        reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| operation.kind == kind)
            .map(|operation| {
                let object: CatalogObject = serde_json::from_value(
                    operation
                        .parameters
                        .get("catalog_object")
                        .cloned()
                        .ok_or_else(|| anyhow!("foreign-key operation omits catalog_object"))?,
                )?;
                if object.kind != CatalogObjectKind::ForeignKey {
                    return Err(anyhow!(
                        "foreign-key operation contains a different object kind"
                    ));
                }
                let reviewed_object = source_catalog
                    .namespaces
                    .iter()
                    .flat_map(|namespace| namespace.objects.iter())
                    .find(|candidate| {
                        candidate.kind == CatalogObjectKind::ForeignKey && candidate.id == object.id
                    })
                    .ok_or_else(|| {
                        anyhow!("foreign-key operation refers to an unknown catalog object")
                    })?;
                if reviewed_object != &object {
                    return Err(anyhow!(
                        "foreign-key operation payload differs from embedded catalog"
                    ));
                }
                Ok(object.id)
            })
            .collect::<anyhow::Result<std::collections::BTreeSet<_>>>()
    };
    if operation_foreign_keys(OperationKind::CheckForeignKey)? != expected_foreign_keys
        || operation_foreign_keys(OperationKind::AddForeignKey)? != expected_foreign_keys
    {
        return Err(anyhow!(
            "foreign-key check/add operation sets differ from reviewed catalog"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct TableShape {
    projection: Vec<Identifier>,
    key: Vec<Identifier>,
    key_indexes: Vec<usize>,
}

#[cfg(feature = "enterprise-migration-spike")]
fn table_shape(catalog: &VendorCatalog, table: &QualifiedTable) -> anyhow::Result<TableShape> {
    let namespace = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == table.namespace)
        .ok_or_else(|| anyhow!("table namespace is absent from source catalog"))?;
    let table_object = namespace
        .objects
        .iter()
        .find(|object| object.kind == CatalogObjectKind::Table && object.name == table.name)
        .ok_or_else(|| anyhow!("table is absent from source catalog"))?;
    let mut columns = namespace
        .objects
        .iter()
        .filter(|object| {
            object.kind == CatalogObjectKind::Column
                && object
                    .attributes
                    .get("table_oid")
                    .and_then(serde_json::Value::as_str)
                    == Some(table_object.id.as_str())
        })
        .collect::<Vec<_>>();
    columns.sort_by_key(|column| {
        column
            .attributes
            .get("ordinal")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX)
    });
    if columns.is_empty() {
        return Err(anyhow!("table has no catalog columns"));
    }
    let projection = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let key_object = namespace
        .objects
        .iter()
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::PrimaryKey | CatalogObjectKind::UniqueConstraint
            ) && object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                == Some(table_object.id.as_str())
        })
        .min_by_key(|object| {
            if object.kind == CatalogObjectKind::PrimaryKey {
                0
            } else {
                1
            }
        })
        .ok_or_else(|| anyhow!("table has no resumable primary or unique key"))?;
    let key_names = key_object
        .attributes
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| anyhow!("resumable key has no ordered column metadata"))?;
    if key_names.is_empty() {
        return Err(anyhow!("resumable key is empty"));
    }
    let mut key = Vec::with_capacity(key_names.len());
    let mut key_indexes = Vec::with_capacity(key_names.len());
    for name in key_names {
        let name = name
            .as_str()
            .ok_or_else(|| anyhow!("resumable key column is not text"))?;
        let index = columns
            .iter()
            .position(|column| column.name.as_str() == name)
            .ok_or_else(|| anyhow!("resumable key column is absent from table"))?;
        if columns[index]
            .attributes
            .get("nullable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true)
        {
            return Err(anyhow!("resumable key column {name} is nullable"));
        }
        key.push(columns[index].name.clone());
        key_indexes.push(index);
    }
    Ok(TableShape {
        projection,
        key,
        key_indexes,
    })
}

#[cfg(feature = "enterprise-migration-spike")]
fn execution_page_limit(config: &PostgresEndpointConfig) -> anyhow::Result<u32> {
    u32::try_from(config.max_batch_rows.min(u32::MAX as usize))
        .context("configured row limit exceeds u32")
}

fn batch_final_key(batch: &RowBatch, shape: &TableShape) -> anyhow::Result<KeyTuple> {
    let row = batch
        .rows()
        .last()
        .ok_or_else(|| anyhow!("cannot derive a key from an empty batch"))?;
    let values = shape
        .key_indexes
        .iter()
        .map(|index| {
            let value = row
                .get(*index)
                .ok_or_else(|| anyhow!("key index exceeds row width"))?;
            if *value == DbValue::Null {
                return Err(anyhow!("resumable key contains NULL"));
            }
            Ok(value.clone())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(KeyTuple::new(values))
}

fn batch_digest(
    table: &QualifiedTable,
    shape: &TableShape,
    batch: &RowBatch,
) -> anyhow::Result<String> {
    let table_name = format!("{}.{}", table.namespace, table.name);
    let column_names = shape
        .projection
        .iter()
        .map(Identifier::as_str)
        .collect::<Vec<_>>();
    let keys = batch
        .rows()
        .iter()
        .map(|row| {
            shape
                .key_indexes
                .iter()
                .map(|index| {
                    row.get(*index)
                        .cloned()
                        .ok_or_else(|| anyhow!("key index exceeds row width"))
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let canonical = batch
        .rows()
        .iter()
        .zip(&keys)
        .map(|(row, key)| CanonicalRow {
            table: &table_name,
            columns: &column_names,
            key,
            values: row,
        })
        .collect::<Vec<_>>();
    Ok(hex::encode(digest_rows(canonical.iter())))
}

fn verify_live_table(
    reader: &mut dyn ReadSession,
    verifier: &mut dyn super::connection::VerificationSession,
    state: &MigrationState,
    operation_id: &str,
    table: &QualifiedTable,
    shape: &TableShape,
    page_limit: u32,
) -> anyhow::Result<()> {
    let chunks = state
        .chunks
        .iter()
        .filter(|chunk| chunk.operation_id == operation_id)
        .collect::<Vec<_>>();
    let mut after = None;
    for chunk in chunks {
        if chunk.state != ChunkState::Committed {
            return Err(anyhow!("verification encountered an uncommitted chunk"));
        }
        if chunk.start_key != after.as_ref().map(|key: &KeyTuple| key.0.clone()) {
            return Err(anyhow!("chunk manifest has a keyspace gap"));
        }
        let limit = u32::try_from(chunk.row_count).context("chunk row count exceeds u32")?;
        if limit == 0 || limit > page_limit {
            return Err(anyhow!("chunk row count exceeds the reviewed page limit"));
        }
        let request = KeysetPage {
            table: table.clone(),
            projection: shape.projection.clone(),
            key: shape.key.clone(),
            after: after.clone(),
            limit,
        };
        let expected = reader.select_page(&request)?;
        let actual = verifier.select_page(&request)?;
        if expected.len() as u64 != chunk.row_count || actual.len() as u64 != chunk.row_count {
            return Err(anyhow!("chunk row count differs during verification"));
        }
        let expected_final = batch_final_key(&expected, shape)?;
        let actual_final = batch_final_key(&actual, shape)?;
        if expected_final.0 != chunk.final_key || actual_final.0 != chunk.final_key {
            return Err(anyhow!("chunk final key differs during verification"));
        }
        let expected_digest = batch_digest(table, shape, &expected)?;
        let actual_digest = batch_digest(table, shape, &actual)?;
        if expected_digest != chunk.canonical_digest
            || actual_digest != chunk.canonical_digest
            || expected.rows() != actual.rows()
        {
            return Err(anyhow!("canonical chunk verification failed"));
        }
        after = Some(expected_final);
    }
    let tail_request = KeysetPage {
        table: table.clone(),
        projection: shape.projection.clone(),
        key: shape.key.clone(),
        after,
        limit: 1,
    };
    if !reader.select_page(&tail_request)?.is_empty()
        || !verifier.select_page(&tail_request)?.is_empty()
    {
        return Err(anyhow!(
            "source or target contains rows outside committed intervals"
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn verify_schema_projection(
    source: &VendorCatalog,
    target_config: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let target = inspect_endpoint(target_config)?;
    if schema_projection(source, true)? != schema_projection(&target.catalog, true)? {
        return Err(anyhow!(
            "final target schema differs from source projection"
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn target_schema_matches(
    source: &VendorCatalog,
    target_config: &PostgresEndpointConfig,
) -> anyhow::Result<bool> {
    let target = inspect_endpoint(target_config)?;
    Ok(schema_projection(source, false)? == schema_projection(&target.catalog, false)?)
}

fn schema_projection(
    catalog: &VendorCatalog,
    include_foreign_keys: bool,
) -> anyhow::Result<serde_json::Value> {
    let table_names = catalog
        .namespaces
        .iter()
        .flat_map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| object.kind == CatalogObjectKind::Table)
                .map(|table| {
                    (
                        table.id.as_str(),
                        format!("{}.{}", namespace.name, table.name),
                    )
                })
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut namespaces = Vec::new();
    for namespace in &catalog.namespaces {
        let mut objects = namespace
            .objects
            .iter()
            .filter(|object| {
                matches!(
                    object.kind,
                    CatalogObjectKind::Table
                        | CatalogObjectKind::Column
                        | CatalogObjectKind::PrimaryKey
                        | CatalogObjectKind::UniqueConstraint
                        | CatalogObjectKind::CheckConstraint
                ) || (include_foreign_keys && object.kind == CatalogObjectKind::ForeignKey)
            })
            .map(|object| {
                let definition = String::from_utf8(object.definition.clone())
                    .context("catalog definition is not UTF-8")?;
                let attributes = match object.kind {
                    CatalogObjectKind::Table => serde_json::json!({
                        "relkind": object.attributes.get("relkind"),
                        "persistence": object.attributes.get("persistence"),
                    }),
                    CatalogObjectKind::Column => serde_json::json!({
                        "table": object.attributes.get("table"),
                        "ordinal": object.attributes.get("ordinal"),
                        "nullable": object.attributes.get("nullable"),
                        "default": object.attributes.get("default"),
                        "identity": object.attributes.get("identity"),
                        "generated": object.attributes.get("generated"),
                        "collation": object.attributes.get("collation"),
                        "type_schema": object.attributes.get("type_schema"),
                        "type_name": object.attributes.get("type_name"),
                    }),
                    CatalogObjectKind::ForeignKey => serde_json::json!({
                        "type": object.attributes.get("type"),
                        "columns": object.attributes.get("columns"),
                        "referenced_table": object.attributes
                            .get("referenced_table_oid")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|oid| table_names.get(oid)),
                        "referenced_columns": object.attributes.get("referenced_columns"),
                        "match_type": object.attributes.get("match_type"),
                        "update_action": object.attributes.get("update_action"),
                        "delete_action": object.attributes.get("delete_action"),
                        "validated": object.attributes.get("validated"),
                        "deferrable": object.attributes.get("deferrable"),
                        "deferred": object.attributes.get("deferred"),
                    }),
                    _ => serde_json::json!({
                        "type": object.attributes.get("type"),
                        "columns": object.attributes.get("columns"),
                        "validated": object.attributes.get("validated"),
                        "deferrable": object.attributes.get("deferrable"),
                        "deferred": object.attributes.get("deferred"),
                    }),
                };
                Ok::<_, anyhow::Error>(serde_json::json!({
                    "kind": object.kind,
                    "name": object.name,
                    "definition": definition,
                    "attributes": attributes,
                }))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        objects.sort_by_key(serde_json::Value::to_string);
        namespaces.push(serde_json::json!({
            "name": namespace.name,
            "objects": objects,
        }));
    }
    namespaces.sort_by_key(serde_json::Value::to_string);
    serde_json::to_value(namespaces).map_err(Into::into)
}

/// Execute the Phase 1–5 contract path against deterministic in-memory adapters.
pub fn run_fixture_spike(directory: impl AsRef<Path>) -> anyhow::Result<SpikeArtifacts> {
    let directory = directory.as_ref();
    std::fs::create_dir_all(directory)
        .with_context(|| format!("create spike artifact directory {}", directory.display()))?;
    let plan_path = directory.join("migration-plan.json");
    let state_path = directory.join("migration-state.json");

    let table = QualifiedTable {
        namespace: Identifier::new("public")?,
        name: Identifier::new("accounts")?,
    };
    let columns = vec![column("id", 0, "bigint")?, column("name", 1, "text")?];
    let rows = vec![
        vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())],
        vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
        vec![DbValue::Unsigned(3), DbValue::Text("Linus".into())],
    ];
    let source = InMemorySource::new("fixture-source", "fixture-db");
    source.add_table(table.clone(), columns.clone(), rows);
    let target = InMemoryTarget::default();

    let copy = PlanOperation::new(
        OperationKind::CopyTable,
        Some(table.clone()),
        Vec::new(),
        BTreeMap::new(),
    )?;
    let verify = PlanOperation::new(
        OperationKind::VerifyTable,
        Some(table.clone()),
        vec![copy.id.clone()],
        BTreeMap::new(),
    )?;
    let reviewed = ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        migration_id: "fixture-phase-1-5".into(),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: "fixture-source".into(),
        target_endpoint_identity: "fixture-target".into(),
        source_catalog_fingerprint: "fixture-source-schema-v1".into(),
        target_catalog_fingerprint: "empty-target-v1".into(),
        source_catalog: None,
        target_catalog: None,
        consistency_mode: "consistent_snapshot".into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "same-dialect-exact".into(),
        capabilities: BTreeMap::from([
            ("consistent_snapshot".into(), "fixture_supported".into()),
            ("server_read_only".into(), "fixture_supported".into()),
        ]),
        operations: vec![copy.clone(), verify],
        unsupported_objects: UnsupportedObjectReport::default(),
    })?;
    reviewed.plan.validate_for_execution()?;
    write_json_new(&plan_path, &reviewed)?;

    let cancellation = CancellationToken::default();
    source.capabilities().require(&[
        "consistent_snapshot",
        "server_read_only",
        "typed_identifiers",
        "bound_parameters",
    ])?;
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
    ])?;
    let snapshot = source.capture_snapshot()?;
    let mut reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced {
        return Err(anyhow!("source read-only evidence is not server enforced"));
    }
    if reader.snapshot() != &snapshot {
        return Err(anyhow!(
            "source reader returned different snapshot evidence"
        ));
    }
    let binding = ResumeBinding {
        migration_id: reviewed.plan.migration_id.clone(),
        plan_hash: reviewed.plan_hash.to_string(),
        approval_reference: "fixture-spike-only".into(),
        tool_version: reviewed.plan.tool_version.clone(),
        source_endpoint: reviewed.plan.source_endpoint_identity.clone(),
        target_endpoint: reviewed.plan.target_endpoint_identity.clone(),
        consistency_evidence: ConsistencyEvidence::NativeSnapshot {
            endpoint_identity: snapshot.endpoint_identity.clone(),
            database_identity: snapshot.database_identity.clone(),
            lifecycle_id: snapshot.lifecycle_id.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            server_version: snapshot.server_version.clone(),
        },
        source_schema_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        target_schema_fingerprint: reviewed.plan.target_catalog_fingerprint.clone(),
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    };
    let mut state = MigrationState::with_operations(
        binding.clone(),
        reviewed.plan.operations.iter().map(|operation| {
            (
                operation.id.to_string(),
                operation
                    .dependencies
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            )
        }),
    )?;
    state.validate_for_operations(
        reviewed
            .plan
            .operations
            .iter()
            .map(|operation| operation.id.as_str()),
    )?;
    write_json_new(&state_path, &state)?;
    state.start_operation(copy.id.as_str())?;
    replace_json(&state_path, &state)?;

    let projection: Vec<_> = columns.iter().map(|column| column.name.clone()).collect();
    let key = vec![projection[0].clone()];
    let mut after = None;
    let mut expected = Vec::new();
    let mut chunk_id = 0;
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.clone(),
            key: key.clone(),
            after: after.clone(),
            limit: 2,
        })?;
        if batch.is_empty() {
            break;
        }
        chunk_id += 1;
        let canonical_rows: Vec<_> = batch
            .rows()
            .iter()
            .map(|row| CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            })
            .collect();
        let digest = hex::encode(digest_rows(canonical_rows.iter()));
        let final_row = batch
            .rows()
            .last()
            .ok_or_else(|| anyhow!("non-empty page unexpectedly lost its final row"))?;
        let final_key = KeyTuple::new(vec![final_row[0].clone()]);
        state.prepare(ChunkRecord {
            chunk_id,
            operation_id: copy.id.to_string(),
            start_key: after.as_ref().map(|key: &KeyTuple| key.0.clone()),
            final_key: final_key.0.clone(),
            row_count: batch.len() as u64,
            canonical_digest: digest,
            target_transaction_intent: format!("fixture-chunk-{chunk_id}"),
            state: ChunkState::Prepared,
        })?;
        state.validate_for_operations(
            reviewed
                .plan
                .operations
                .iter()
                .map(|operation| operation.id.as_str()),
        )?;
        replace_json(&state_path, &state)?;
        let mut writer = target.open_writer(cancellation.clone())?;
        writer.begin()?;
        if let Err(error) = writer.insert(&table, &batch).and_then(|()| writer.commit()) {
            let _ = writer.rollback();
            return Err(error.into());
        }
        state.commit(chunk_id)?;
        replace_json(&state_path, &state)?;
        expected.extend(batch.rows().iter().cloned());
        after = Some(final_key);
    }

    state.commit_operation(copy.id.as_str())?;
    state.begin_verification()?;
    replace_json(&state_path, &state)?;

    let mut verifier = target.open_verifier(cancellation)?;
    let target_rows = read_all_verifier(verifier.as_mut(), &table, &projection, &key, 2)?;
    let independently_read = read_all(
        reader.as_mut(),
        &QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        },
        &projection,
        &key,
    )?;
    verify_manifest(&state, &independently_read)?;
    let report = verify_keyed_rows(key_rows(&independently_read)?, key_rows(&target_rows)?);
    if !report.is_exact() {
        return Err(anyhow!("strict fixture verification failed: {report:?}"));
    }
    state.verify_operation(copy.id.as_str())?;
    let verify_operation = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifyTable)
        .ok_or_else(|| anyhow!("fixture plan has no verification operation"))?;
    state.start_operation(verify_operation.id.as_str())?;
    state.commit_operation(verify_operation.id.as_str())?;
    state.verify_operation(verify_operation.id.as_str())?;
    state.finalize()?;
    replace_json(&state_path, &state)?;
    state.validate_resume(&binding)?;
    Ok(SpikeArtifacts {
        plan: plan_path,
        state: state_path,
        plan_hash: reviewed.plan_hash.to_string(),
        copied_rows: expected.len(),
    })
}

fn column(name: &str, ordinal: u32, vendor_type: &str) -> anyhow::Result<ColumnMeta> {
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
}

fn key_rows(rows: &[Vec<DbValue>]) -> anyhow::Result<Vec<KeyedRow<Vec<u8>, Vec<u8>>>> {
    rows.iter()
        .map(|row| {
            let canonical = CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            };
            Ok(KeyedRow {
                key: encode_row(&CanonicalRow {
                    values: &[],
                    ..canonical.clone()
                }),
                value: encode_row(&canonical),
            })
        })
        .collect()
}

fn read_all(
    reader: &mut dyn ReadSession,
    table: &QualifiedTable,
    projection: &[Identifier],
    key: &[Identifier],
) -> anyhow::Result<Vec<Vec<DbValue>>> {
    let mut rows = Vec::new();
    let mut after = None;
    loop {
        let batch = reader.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.to_vec(),
            key: key.to_vec(),
            after: after.clone(),
            limit: 2,
        })?;
        if batch.is_empty() {
            return Ok(rows);
        }
        let final_row = batch
            .rows()
            .last()
            .ok_or_else(|| anyhow!("non-empty page unexpectedly lost its final row"))?;
        after = Some(KeyTuple::new(vec![final_row[0].clone()]));
        rows.extend(batch.rows().iter().cloned());
    }
}

fn read_all_verifier(
    verifier: &mut dyn super::connection::VerificationSession,
    table: &QualifiedTable,
    projection: &[Identifier],
    key: &[Identifier],
    page_limit: u32,
) -> anyhow::Result<Vec<Vec<DbValue>>> {
    if page_limit == 0 {
        return Err(anyhow!("verification page limit must be positive"));
    }
    let mut rows = Vec::new();
    let mut after = None;
    loop {
        let batch = verifier.select_page(&KeysetPage {
            table: table.clone(),
            projection: projection.to_vec(),
            key: key.to_vec(),
            after: after.clone(),
            limit: page_limit,
        })?;
        if batch.is_empty() {
            return Ok(rows);
        }
        let final_row = batch
            .rows()
            .last()
            .ok_or_else(|| anyhow!("non-empty page unexpectedly lost its final row"))?;
        after = Some(KeyTuple::new(vec![final_row[0].clone()]));
        rows.extend(batch.rows().iter().cloned());
    }
}

fn verify_manifest(state: &MigrationState, rows: &[Vec<DbValue>]) -> anyhow::Result<()> {
    let mut offset = 0usize;
    for chunk in &state.chunks {
        let count = usize::try_from(chunk.row_count).context("chunk row count exceeds usize")?;
        let end = offset
            .checked_add(count)
            .filter(|end| *end <= rows.len())
            .ok_or_else(|| anyhow!("chunk manifest exceeds source snapshot"))?;
        let canonical: Vec<_> = rows[offset..end]
            .iter()
            .map(|row| CanonicalRow {
                table: "public.accounts",
                columns: &["id", "name"],
                key: &row[..1],
                values: row,
            })
            .collect();
        if hex::encode(digest_rows(canonical.iter())) != chunk.canonical_digest {
            return Err(anyhow!("chunk {} canonical digest drift", chunk.chunk_id));
        }
        if rows[end - 1][..1] != chunk.final_key {
            return Err(anyhow!("chunk {} final key drift", chunk.chunk_id));
        }
        offset = end;
    }
    if offset != rows.len() {
        return Err(anyhow!(
            "chunk manifest does not cover the complete source snapshot"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::fixture::FailurePoint;

    #[test]
    fn vertical_spike_copies_journals_and_verifies() {
        let directory = tempfile::tempdir().unwrap();
        let artifacts = run_fixture_spike(directory.path()).unwrap();
        assert_eq!(artifacts.copied_rows, 3);
        let state: MigrationState = super::super::artifact::read_json(artifacts.state).unwrap();
        assert_eq!(state.chunks.len(), 2);
        assert!(state
            .chunks
            .iter()
            .all(|chunk| chunk.state == ChunkState::Committed));
    }

    #[test]
    fn manifest_digest_drift_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let artifacts = run_fixture_spike(directory.path()).unwrap();
        let mut state: MigrationState = super::super::artifact::read_json(artifacts.state).unwrap();
        state.chunks[0].canonical_digest = "tampered".into();
        let rows = vec![
            vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())],
            vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
            vec![DbValue::Unsigned(3), DbValue::Text("Linus".into())],
        ];
        assert!(verify_manifest(&state, &rows).is_err());
    }

    fn ambiguous_commit_fixture(
        failure: FailurePoint,
        replacement_rows: Option<Vec<Vec<DbValue>>>,
    ) -> anyhow::Result<(PreparedResolution, MigrationState, InMemoryTarget)> {
        let table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        };
        let columns = vec![column("id", 0, "bigint")?, column("name", 1, "text")?];
        let mut batch = RowBatch::new(columns.clone(), 2, 1_024);
        batch.try_push(vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())], 16)?;
        batch.try_push(
            vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
            20,
        )?;
        let shape = TableShape {
            projection: vec![Identifier::new("id")?, Identifier::new("name")?],
            key: vec![Identifier::new("id")?],
            key_indexes: vec![0],
        };
        let binding = ResumeBinding {
            migration_id: "m".into(),
            plan_hash: "p".into(),
            approval_reference: "a".into(),
            tool_version: "t".into(),
            source_endpoint: "s".into(),
            target_endpoint: "d".into(),
            consistency_evidence: ConsistencyEvidence::NativeSnapshot {
                endpoint_identity: "s".into(),
                database_identity: "db".into(),
                lifecycle_id: "lifecycle".into(),
                snapshot_id: "snapshot".into(),
                server_version: "fixture".into(),
            },
            source_schema_fingerprint: "source".into(),
            target_schema_fingerprint: "target".into(),
            conversion_policy: "exact".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        };
        let mut state =
            MigrationState::with_operations(binding, [("copy".to_owned(), Vec::new())])?;
        state.start_operation("copy")?;
        state.prepare(ChunkRecord {
            chunk_id: 1,
            operation_id: "copy".into(),
            start_key: None,
            final_key: batch_final_key(&batch, &shape)?.0,
            row_count: u64::try_from(batch.len())?,
            canonical_digest: batch_digest(&table, &shape, &batch)?,
            target_transaction_intent: "intent-1".into(),
            state: ChunkState::Prepared,
        })?;
        let directory = tempfile::tempdir()?;
        let state_path = directory.path().join("state.json");
        write_json_new(&state_path, &state)?;
        let target = InMemoryTarget::default();
        target.add_empty_table(table.clone(), columns);
        target.fail_once(failure);
        let mut writer = target.open_writer(CancellationToken::default())?;
        writer.begin()?;
        writer.insert(&table, &batch)?;
        assert!(matches!(
            writer.commit(),
            Err(ConnectionError::CommitOutcomeUnknown(_))
        ));
        if let Some(rows) = replacement_rows {
            let mut replacement = RowBatch::new(batch.columns().to_vec(), rows.len(), 1_024);
            for row in rows {
                replacement.try_push(row, 32)?;
            }
            let mut replacement_writer = target.open_writer(CancellationToken::default())?;
            replacement_writer.begin()?;
            replacement_writer.insert(&table, &replacement)?;
            replacement_writer.commit()?;
        }
        let resolution = reconcile_live_prepared_chunk(
            &target,
            &mut state,
            &state_path,
            &table,
            &shape,
            &batch,
            CancellationToken::default(),
        )?;
        Ok((resolution, state, target))
    }

    #[test]
    fn ambiguous_commit_after_apply_is_marked_committed_on_exact_equality() {
        let (resolution, state, target) =
            ambiguous_commit_fixture(FailurePoint::CommitOutcomeUnknownAfterApply, None).unwrap();
        assert_eq!(resolution, PreparedResolution::MarkedCommitted);
        assert_eq!(state.chunks[0].state, ChunkState::Committed);
        assert_eq!(
            target
                .rows(&QualifiedTable {
                    namespace: Identifier::new("public").unwrap(),
                    name: Identifier::new("accounts").unwrap(),
                })
                .len(),
            2
        );
    }

    #[test]
    fn ambiguous_commit_before_apply_requires_exact_retry() {
        let (resolution, state, target) =
            ambiguous_commit_fixture(FailurePoint::CommitOutcomeUnknownBeforeApply, None).unwrap();
        assert_eq!(resolution, PreparedResolution::RetryRequired);
        assert_eq!(state.chunks[0].state, ChunkState::Prepared);
        assert!(target
            .rows(&QualifiedTable {
                namespace: Identifier::new("public").unwrap(),
                name: Identifier::new("accounts").unwrap(),
            })
            .is_empty());
    }

    #[test]
    fn ambiguous_partial_or_different_effect_requires_manual_reconciliation() {
        for rows in [
            vec![vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())]],
            vec![
                vec![DbValue::Unsigned(1), DbValue::Text("changed".into())],
                vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
            ],
            vec![
                vec![DbValue::Unsigned(1), DbValue::Text("Ada".into())],
                vec![DbValue::Unsigned(2), DbValue::Text("Grace".into())],
                vec![DbValue::Unsigned(3), DbValue::Text("extra".into())],
            ],
        ] {
            let (resolution, state, _) =
                ambiguous_commit_fixture(FailurePoint::CommitOutcomeUnknownBeforeApply, Some(rows))
                    .unwrap();
            assert_eq!(resolution, PreparedResolution::ManualReconciliationRequired);
            assert_eq!(
                state.status,
                super::super::journal::MigrationStatus::ManualReconciliationRequired
            );
        }
    }

    #[cfg(feature = "enterprise-migration-spike")]
    fn endpoint(credential_env: &str) -> PostgresEndpointConfig {
        PostgresEndpointConfig {
            host: "localhost".into(),
            port: 5432,
            database: "db".into(),
            user: "role".into(),
            credential_env: credential_env.into(),
            tls: super::super::postgres::PostgresTlsConfig::default(),
            connect_timeout_seconds: 1,
            max_batch_rows: 10,
            max_batch_bytes: 1_024,
        }
    }

    #[cfg(feature = "enterprise-migration-spike")]
    #[test]
    fn resume_rejects_every_shared_credential_pair() {
        assert!(validate_separate_postgres_credentials(
            &endpoint("SOURCE"),
            &endpoint("SOURCE"),
            &endpoint("ADMIN")
        )
        .is_err());
        assert!(validate_separate_postgres_credentials(
            &endpoint("SOURCE"),
            &endpoint("TARGET"),
            &endpoint("SOURCE")
        )
        .is_err());
        assert!(validate_separate_postgres_credentials(
            &endpoint("SOURCE"),
            &endpoint("TARGET"),
            &endpoint("TARGET")
        )
        .is_err());
        assert!(validate_separate_postgres_credentials(
            &endpoint("SOURCE"),
            &endpoint("TARGET"),
            &endpoint("ADMIN")
        )
        .is_ok());
    }

    #[cfg(feature = "enterprise-migration-spike")]
    #[test]
    fn resumed_verification_completion_is_idempotent() {
        let binding = ResumeBinding {
            migration_id: "m".into(),
            plan_hash: "p".into(),
            approval_reference: "a".into(),
            tool_version: "t".into(),
            source_endpoint: "s".into(),
            target_endpoint: "d".into(),
            consistency_evidence: ConsistencyEvidence::NativeSnapshot {
                endpoint_identity: "s".into(),
                database_identity: "db".into(),
                lifecycle_id: "life".into(),
                snapshot_id: "snapshot".into(),
                server_version: "test".into(),
            },
            source_schema_fingerprint: "source".into(),
            target_schema_fingerprint: "target".into(),
            conversion_policy: "exact".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        };
        let mut state =
            MigrationState::with_operations(binding, [("verify".to_owned(), Vec::<String>::new())])
                .unwrap();
        complete_operation_if_needed(&mut state, "verify").unwrap();
        complete_operation_if_needed(&mut state, "verify").unwrap();
        assert_eq!(
            operation_state(&state, "verify").unwrap(),
            OperationState::Verified
        );
    }
}
