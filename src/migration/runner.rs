//! Fixture-backed orchestration for the migration contract spike.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
#[cfg(feature = "enterprise-migration-spike")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "enterprise-migration-spike")]
use std::sync::{mpsc, Arc};
#[cfg(feature = "enterprise-migration-spike")]
use std::thread::{self, JoinHandle};
#[cfg(feature = "enterprise-migration-spike")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
#[cfg(feature = "enterprise-migration-spike")]
use sha2::{Digest, Sha256};

#[cfg(feature = "migration-fault-injection")]
use super::append_journal::AppendJournalFault;
#[cfg(feature = "enterprise-migration-spike")]
use super::append_journal::{
    AppendJournal, CommittedChunkIter, Genesis, OperationPhase, OperationSpec, PreparedChunk,
};
use super::artifact::{read_json, replace_json, write_json_new};
use super::canonical::{digest_rows, encode_row, CanonicalRow, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, ConnectionError, KeyTuple, KeysetPage, ReadSession, SnapshotToken,
    SourceConnectionFactory, TargetConnectionFactory, WriteSession,
};
use super::fixture::{InMemorySource, InMemoryTarget};
use super::journal::{
    ChunkRecord, ChunkState, ConsistencyEvidence, MigrationState, MigrationStatus, OperationState,
    PreparedResolution, ResumeBinding,
};
use super::model::{
    CatalogObject, CatalogObjectKind, ColumnMeta, DbValue, Identifier, QualifiedTable, RowBatch,
    VendorCatalog,
};
use super::outage_projection::AcceptedOutageProjection;
use super::plan::{
    AssessmentStatus, MigrationPlan, OperationKind, PlanOperation, PlanPurpose, ReviewedPlan,
    UnsupportedObjectReport, PLAN_SCHEMA_VERSION, POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
};
use super::verify::{verify_keyed_rows, KeyedRow};

#[cfg(feature = "enterprise-migration-spike")]
use super::postgres::{
    catalog_fingerprint, postgres_foreign_keys, postgres_generated_columns,
    postgres_partition_topologies, postgres_post_data_indexes, postgres_programmable_objects,
    postgres_sequences, postgres_tls_binding, postgres_write_policy, select_resumable_key,
    PostgresConsistencyMode, PostgresEndpointConfig, PostgresForeignKey, PostgresForeignKeyState,
    PostgresGeneratedColumnState, PostgresIndex, PostgresIndexState, PostgresPartitionTopology,
    PostgresPartitionTopologyState, PostgresProgrammableObject, PostgresProgrammableObjectState,
    PostgresResumableKey, PostgresSequence, PostgresSequenceState, PostgresSourceFactory,
    PostgresTargetFactory, PostgresWritePolicy, CATALOG_FORMAT_VERSION,
};
#[cfg(feature = "enterprise-migration-spike")]
use super::postgres_ast::PostgresDurableAst;
#[cfg(feature = "enterprise-migration-spike")]
use super::postgres_fence::{
    attest_postgres_write_fence, postgres_write_fence_is_released, release_postgres_write_fence,
    remove_attested_fence_objects, InstalledPostgresFence, POSTGRES_FENCE_ARTIFACT_VERSION,
};
#[cfg(feature = "enterprise-migration-spike")]
use super::postgres_profile::{
    PostgresExternalQuiesceAttestation, PostgresExternalQuiesceRescanEvidence,
    PostgresExternalQuiesceRescanTableEvidence, PostgresSequenceEqualityEvidence,
    PostgresSourceProfileContract,
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

/// Inputs for a fault-test execution driven by an externally controlled token.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub struct PostgresCancellationExecution<'a> {
    pub plan_path: &'a Path,
    pub source_config_path: &'a Path,
    pub target_config_path: &'a Path,
    pub fence_admin_config_path: &'a Path,
    pub fence_artifact_path: &'a Path,
    pub approval_reference: &'a str,
    pub state_path: &'a Path,
}

/// Inputs for a fault-test resume driven by an externally controlled token.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub struct PostgresCancellationResume<'a> {
    pub state_path: &'a Path,
    pub source_config_path: &'a Path,
    pub target_config_path: &'a Path,
    pub fence_admin_config_path: &'a Path,
    pub fence_artifact_path: &'a Path,
}

#[cfg(feature = "enterprise-migration-spike")]
struct CancellationMonitor {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

#[cfg(feature = "enterprise-migration-spike")]
impl CancellationMonitor {
    fn start(
        source: Arc<PostgresSourceFactory>,
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
            match source.open_control() {
                Ok(mut control) => {
                    if let Err(error) = control.cancel_active_statement() {
                        if !matches!(error, ConnectionError::InvalidRequest(_)) {
                            errors.push(format!("source: {error}"));
                        }
                    }
                }
                Err(error) if !matches!(error, ConnectionError::InvalidRequest(_)) => {
                    errors.push(format!("source control: {error}"));
                }
                Err(_) => {}
            }
            match target.open_control() {
                Ok(mut control) => {
                    if let Err(error) = control.cancel_active_statement() {
                        if !matches!(error, ConnectionError::InvalidRequest(_)) {
                            errors.push(format!("target: {error}"));
                        }
                    }
                }
                Err(error) if !matches!(error, ConnectionError::InvalidRequest(_)) => {
                    errors.push(format!("target control: {error}"));
                }
                Err(_) => {}
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

#[cfg(feature = "enterprise-migration-spike")]
impl Drop for CancellationMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn check_cancellation_before_commit(
    writer: &mut dyn WriteSession,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    if let Err(cancelled) = cancellation.check() {
        writer.rollback().map_err(|rollback| {
            anyhow!("cancellation failed and target rollback also failed: {cancelled}; {rollback}")
        })?;
        return Err(cancelled.into());
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn rollback_cancelled_commit(
    writer: &mut dyn WriteSession,
    cancelled: ConnectionError,
    cancellation: &CancellationToken,
) -> anyhow::Error {
    let cause = cancellation.check().err().unwrap_or(cancelled);
    match writer.rollback() {
        Ok(()) => cause.into(),
        Err(rollback) => {
            anyhow!("target commit was cancelled and rollback also failed: {cause}; {rollback}")
        }
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn open_postgres_batch_writer(
    target: &dyn TargetConnectionFactory,
    table: &QualifiedTable,
    batch: &RowBatch,
    policy: PostgresWritePolicy,
    cancellation: &CancellationToken,
) -> anyhow::Result<Box<dyn WriteSession>> {
    let mut writer = target.open_writer(cancellation.clone())?;
    writer.begin()?;
    match policy {
        PostgresWritePolicy::PlainInsertIdentityAlwaysV1 => {
            if let Err(error) = writer.insert(table, batch) {
                let _ = writer.rollback();
                if error == ConnectionError::Cancelled {
                    return Err(cancellation.check().err().unwrap_or(error).into());
                }
                return Err(error.into());
            }
            Ok(writer)
        }
        PostgresWritePolicy::BinaryCopyWithInsertFallbackV1 => {
            match writer.bulk_write(table, batch) {
                Ok(()) => Ok(writer),
                Err(copy_error) => {
                    writer.rollback().map_err(|rollback| {
                        anyhow!(
                            "binary COPY failed and target rollback also failed: {copy_error}; {rollback}"
                        )
                    })?;
                    if copy_error == ConnectionError::Cancelled {
                        return Err(cancellation.check().err().unwrap_or(copy_error).into());
                    }
                    cancellation.check()?;
                    let mut diagnostic = target.open_writer(cancellation.clone())?;
                    diagnostic.begin()?;
                    if let Err(insert_error) = diagnostic.insert(table, batch) {
                        let rollback = diagnostic.rollback();
                        return Err(match rollback {
                            Ok(()) => anyhow!(
                                "binary COPY failed ({copy_error}); diagnostic INSERT failed: {insert_error}"
                            ),
                            Err(rollback_error) => anyhow!(
                                "binary COPY failed ({copy_error}); diagnostic INSERT failed ({insert_error}) and rollback failed: {rollback_error}"
                            ),
                        });
                    }
                    Ok(diagnostic)
                }
            }
        }
    }
}

#[cfg(feature = "enterprise-migration-spike")]
enum VerificationPipelineCommand {
    StartTable {
        operation_id: String,
        table: QualifiedTable,
        shape: TableShape,
        page_limit: u32,
    },
    Chunk(PreparedChunk),
    FinishTable,
    Finish,
}

#[cfg(feature = "enterprise-migration-spike")]
struct PipelinedTableEvidence {
    operation_id: String,
    chunk_count: u64,
    row_count: u64,
    manifest_hash: String,
    source_hash: String,
    target_hash: String,
}

#[cfg(feature = "enterprise-migration-spike")]
struct VerificationPipeline {
    sender: Option<mpsc::SyncSender<VerificationPipelineCommand>>,
    worker: Option<JoinHandle<anyhow::Result<Vec<PipelinedTableEvidence>>>>,
    cancellation: CancellationToken,
}

#[cfg(feature = "migration-fault-injection")]
#[derive(Clone, Default)]
struct VerificationPipelineHooks {
    before_chunk_verification: Option<Arc<dyn Fn(u64) -> anyhow::Result<()> + Send + Sync>>,
}

#[cfg(feature = "enterprise-migration-spike")]
impl VerificationPipeline {
    fn start(
        source: Arc<PostgresSourceFactory>,
        target: Arc<PostgresTargetFactory>,
        snapshot: SnapshotToken,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Option<Self>> {
        Self::start_with_import(
            target,
            cancellation.clone(),
            move || source.open_imported_snapshot_peer(&snapshot, cancellation),
            #[cfg(feature = "migration-fault-injection")]
            VerificationPipelineHooks::default(),
        )
    }

    fn start_with_import<F>(
        target: Arc<dyn TargetConnectionFactory>,
        cancellation: CancellationToken,
        import: F,
        #[cfg(feature = "migration-fault-injection")] hooks: VerificationPipelineHooks,
    ) -> anyhow::Result<Option<Self>>
    where
        F: FnOnce() -> Result<Box<dyn ReadSession>, ConnectionError> + Send + 'static,
    {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        let worker_cancellation = cancellation.clone();
        let worker = thread::spawn(move || {
            let reader = import();
            match reader {
                Ok(reader) => {
                    ready_sender.send(Ok(())).map_err(|_| {
                        anyhow!("verification pipeline readiness receiver was dropped")
                    })?;
                    let result = run_verification_pipeline(
                        reader,
                        target,
                        receiver,
                        worker_cancellation.clone(),
                        #[cfg(feature = "migration-fault-injection")]
                        hooks,
                    );
                    if let Err(error) = &result {
                        worker_cancellation.record_control_error(format!(
                            "pipelined verification failed: {error:#}"
                        ));
                        worker_cancellation.cancel();
                    }
                    result
                }
                Err(error) => {
                    let message = error.to_string();
                    let _ = ready_sender.send(Err(message.clone()));
                    Err(anyhow!("cannot import PostgreSQL snapshot peer: {message}"))
                }
            }
        });
        match ready_receiver.recv() {
            Ok(Ok(())) => Ok(Some(Self {
                sender: Some(sender),
                worker: Some(worker),
                cancellation,
            })),
            Ok(Err(_)) => {
                drop(sender);
                let _ = worker.join();
                Ok(None)
            }
            Err(_) => {
                drop(sender);
                let result = worker
                    .join()
                    .map_err(|_| anyhow!("verification pipeline worker panicked before startup"))?;
                result?;
                Err(anyhow!(
                    "verification pipeline stopped without reporting readiness"
                ))
            }
        }
    }

    fn send(&self, command: VerificationPipelineCommand) -> anyhow::Result<()> {
        self.sender
            .as_ref()
            .ok_or_else(|| anyhow!("verification pipeline is closed"))?
            .send(command)
            .map_err(|_| anyhow!("verification pipeline worker stopped unexpectedly"))
    }

    fn finish(mut self) -> anyhow::Result<Vec<PipelinedTableEvidence>> {
        self.send(VerificationPipelineCommand::Finish)?;
        self.sender.take();
        self.worker
            .take()
            .ok_or_else(|| anyhow!("verification pipeline worker is absent"))?
            .join()
            .map_err(|_| anyhow!("verification pipeline worker panicked"))?
    }
}

#[cfg(feature = "enterprise-migration-spike")]
impl Drop for VerificationPipeline {
    fn drop(&mut self) {
        if self.worker.is_some() {
            self.cancellation.cancel();
            self.sender.take();
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn run_verification_pipeline(
    mut reader: Box<dyn ReadSession>,
    target: Arc<dyn TargetConnectionFactory>,
    receiver: mpsc::Receiver<VerificationPipelineCommand>,
    cancellation: CancellationToken,
    #[cfg(feature = "migration-fault-injection")] hooks: VerificationPipelineHooks,
) -> anyhow::Result<Vec<PipelinedTableEvidence>> {
    struct ActiveTable {
        operation_id: String,
        table: QualifiedTable,
        shape: TableShape,
        page_limit: u32,
        chunk_count: u64,
        row_count: u64,
        accumulator: TableVerificationAccumulator,
    }

    let mut current: Option<ActiveTable> = None;
    let mut evidence = Vec::new();
    while let Ok(command) = receiver.recv() {
        cancellation.check()?;
        match command {
            VerificationPipelineCommand::StartTable {
                operation_id,
                table,
                shape,
                page_limit,
            } => {
                if current.is_some() {
                    return Err(anyhow!("verification pipeline received overlapping tables"));
                }
                current = Some(ActiveTable {
                    operation_id,
                    table,
                    shape,
                    page_limit,
                    chunk_count: 0,
                    row_count: 0,
                    accumulator: TableVerificationAccumulator::new(),
                });
            }
            VerificationPipelineCommand::Chunk(chunk) => {
                let active = current
                    .as_mut()
                    .ok_or_else(|| anyhow!("verification chunk has no active table"))?;
                #[cfg(feature = "migration-fault-injection")]
                if let Some(before_chunk_verification) = &hooks.before_chunk_verification {
                    before_chunk_verification(chunk.chunk_id)?;
                }
                let mut verifier = target.open_verifier(cancellation.clone())?;
                active.accumulator.verify_chunk(
                    reader.as_mut(),
                    verifier.as_mut(),
                    &chunk,
                    &ChunkVerificationContext {
                        operation_id: active.operation_id.as_str(),
                        table: &active.table,
                        shape: &active.shape,
                        page_limit: active.page_limit,
                    },
                )?;
                active.chunk_count = active
                    .chunk_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("verification chunk count overflow"))?;
                active.row_count = active
                    .row_count
                    .checked_add(chunk.row_count)
                    .ok_or_else(|| anyhow!("verification row count overflow"))?;
            }
            VerificationPipelineCommand::FinishTable => {
                let mut active = current
                    .take()
                    .ok_or_else(|| anyhow!("verification table finish has no active table"))?;
                let mut verifier = target.open_verifier(cancellation.clone())?;
                active.accumulator.verify_tail(
                    reader.as_mut(),
                    verifier.as_mut(),
                    &active.table,
                    &active.shape,
                )?;
                let (manifest_hash, source_hash, target_hash) = active.accumulator.finish();
                evidence.push(PipelinedTableEvidence {
                    operation_id: active.operation_id,
                    chunk_count: active.chunk_count,
                    row_count: active.row_count,
                    manifest_hash,
                    source_hash,
                    target_hash,
                });
            }
            VerificationPipelineCommand::Finish => {
                if current.is_some() {
                    return Err(anyhow!(
                        "verification pipeline finished with an active table"
                    ));
                }
                return Ok(evidence);
            }
        }
    }
    Err(anyhow!("verification pipeline command channel closed"))
}

#[cfg(feature = "enterprise-migration-spike")]
fn append_journal_genesis(
    binding: ResumeBinding,
    reviewed_plan: ReviewedPlan,
    accepted_outage_projection: Option<AcceptedOutageProjection>,
    accepted_external_quiesce: Option<super::postgres_profile::PostgresExternalQuiesceAttestation>,
) -> Genesis {
    let operations = reviewed_plan
        .plan
        .operations
        .iter()
        .map(|operation| OperationSpec {
            operation_id: operation.id.to_string(),
            dependencies: operation.dependencies.iter().map(ToString::to_string).collect(),
            is_copy: operation.kind == OperationKind::CopyTable,
            phase: if matches!(operation.kind, OperationKind::VerifyTable | OperationKind::VerifySchema)
                || matches!(&operation.kind, OperationKind::Vendor(name) if name == "verify_postgres_partition_topology")
            {
                OperationPhase::Verification
            } else {
                OperationPhase::Execution
            },
        })
        .collect();
    Genesis {
        binding,
        reviewed_plan,
        accepted_outage_projection,
        accepted_external_quiesce,
        accepted_mysql_freeze: None,
        operations,
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn journal_operation_state(
    journal: &AppendJournal,
    operation_id: &str,
) -> anyhow::Result<OperationState> {
    journal
        .projection()
        .operations
        .get(operation_id)
        .copied()
        .ok_or_else(|| anyhow!("migration journal has no state for operation {operation_id}"))
}

#[cfg(feature = "enterprise-migration-spike")]
fn journal_prepare_effect(journal: &mut AppendJournal, operation_id: &str) -> anyhow::Result<()> {
    match journal_operation_state(journal, operation_id)? {
        OperationState::Pending => {
            journal.prepare_operations_atomic([operation_id])?;
        }
        OperationState::Running => {
            journal.transition_operation(operation_id, OperationState::Prepared)?;
        }
        OperationState::Prepared => {}
        OperationState::Committed | OperationState::Verified => {
            return Err(anyhow!(
                "operation {operation_id} has already passed its effect preparation boundary"
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Some boundaries are constructed only by the opt-in fault feature.
enum ExecutionInterruption {
    AfterDdlPrepared,
    AfterDdlCommitted,
    AfterChunkPrepared,
    CommitUnknownAfterApply,
    AfterCommittedChunks(u64),
    AfterIndexPrepared,
    AfterIndexCommitted,
    AfterSequencePrepared,
    AfterSequenceCommitted,
    AfterAllVerified,
    BeforeForeignKeyChecks,
    AfterForeignKeyPrepared,
    AfterForeignKeyCommitted,
    AfterFenceReleased,
    #[cfg(feature = "migration-fault-injection")]
    NetworkCommitFault(u16),
    #[cfg(feature = "migration-fault-injection")]
    TornChunkCommittedEnospc,
    #[cfg(feature = "migration-fault-injection")]
    ChunkCommittedSyncAckLost,
    #[cfg(feature = "migration-fault-injection")]
    AfterPipelinedEvidence,
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
    AfterIndexPrepared,
    AfterIndexCommitted,
    AfterSequencePrepared,
    AfterSequenceCommitted,
    AfterAllVerified,
    BeforeForeignKeyChecks,
    AfterForeignKeyPrepared,
    AfterForeignKeyCommitted,
    AfterFenceReleased,
    NetworkCommitFault(u16),
    TornChunkCommittedEnospc,
    ChunkCommittedSyncAckLost,
    AfterPipelinedEvidence,
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
            PostgresExecutionInterruption::AfterIndexPrepared => Self::AfterIndexPrepared,
            PostgresExecutionInterruption::AfterIndexCommitted => Self::AfterIndexCommitted,
            PostgresExecutionInterruption::AfterSequencePrepared => Self::AfterSequencePrepared,
            PostgresExecutionInterruption::AfterSequenceCommitted => Self::AfterSequenceCommitted,
            PostgresExecutionInterruption::AfterAllVerified => Self::AfterAllVerified,
            PostgresExecutionInterruption::BeforeForeignKeyChecks => Self::BeforeForeignKeyChecks,
            PostgresExecutionInterruption::AfterForeignKeyPrepared => Self::AfterForeignKeyPrepared,
            PostgresExecutionInterruption::AfterForeignKeyCommitted => {
                Self::AfterForeignKeyCommitted
            }
            PostgresExecutionInterruption::AfterFenceReleased => Self::AfterFenceReleased,
            PostgresExecutionInterruption::NetworkCommitFault(port) => {
                Self::NetworkCommitFault(port)
            }
            PostgresExecutionInterruption::TornChunkCommittedEnospc => {
                Self::TornChunkCommittedEnospc
            }
            PostgresExecutionInterruption::ChunkCommittedSyncAckLost => {
                Self::ChunkCommittedSyncAckLost
            }
            PostgresExecutionInterruption::AfterPipelinedEvidence => Self::AfterPipelinedEvidence,
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
        None,
        None,
    )
}

/// Execute an external-quiesce plan after binding one active attestation.
#[cfg(feature = "enterprise-migration-spike")]
pub fn execute_postgres_plan_with_external_quiesce(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    external_quiesce_attestation_path: impl AsRef<Path>,
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
        Some(external_quiesce_attestation_path.as_ref()),
        None,
        None,
    )
}

/// Execute an external-quiesce plan until one deterministic recovery boundary.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
#[allow(clippy::too_many_arguments)] // Mirrors the operator API plus one deterministic boundary.
pub fn execute_postgres_external_quiesce_plan_with_interruption(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    external_quiesce_attestation_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
    interruption: PostgresExecutionInterruption,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_plan_internal(
        plan_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        approval_reference,
        state_path.as_ref(),
        None,
        Some(external_quiesce_attestation_path.as_ref()),
        Some(interruption.into()),
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
        None,
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
        None,
        Some(request.interruption.into()),
        None,
    )
}

/// Execute using a caller-controlled cancellation token for fault testing.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub fn execute_postgres_fenced_plan_with_cancellation(
    request: PostgresCancellationExecution<'_>,
    cancellation: CancellationToken,
) -> anyhow::Result<PostgresExecutionReport> {
    execute_postgres_plan_internal(
        request.plan_path,
        request.source_config_path,
        request.target_config_path,
        request.approval_reference,
        request.state_path,
        Some((request.fence_admin_config_path, request.fence_artifact_path)),
        None,
        None,
        Some(cancellation),
    )
}

/// Resume using a caller-controlled cancellation token for fault testing.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub fn resume_postgres_fenced_plan_with_cancellation(
    request: PostgresCancellationResume<'_>,
    cancellation: CancellationToken,
) -> anyhow::Result<PostgresExecutionReport> {
    resume_postgres_plan_internal(
        request.state_path,
        request.source_config_path,
        request.target_config_path,
        Some((request.fence_admin_config_path, request.fence_artifact_path)),
        None,
        Some(cancellation),
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
    resume_postgres_plan_internal(
        state_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        Some((
            fence_admin_config_path.as_ref(),
            fence_artifact_path.as_ref(),
        )),
        None,
        None,
    )
}

/// Resume an external-quiesce migration with the same accepted attestation.
#[cfg(feature = "enterprise-migration-spike")]
pub fn resume_postgres_plan_with_external_quiesce(
    state_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    external_quiesce_attestation_path: impl AsRef<Path>,
) -> anyhow::Result<PostgresExecutionReport> {
    resume_postgres_plan_internal(
        state_path.as_ref(),
        source_config_path.as_ref(),
        target_config_path.as_ref(),
        None,
        Some(external_quiesce_attestation_path.as_ref()),
        None,
    )
}

#[cfg(feature = "enterprise-migration-spike")]
fn resume_postgres_plan_internal(
    state_path: &Path,
    source_config_path: &Path,
    target_config_path: &Path,
    fence_paths: Option<(&Path, &Path)>,
    external_quiesce_attestation_path: Option<&Path>,
    injected_cancellation: Option<CancellationToken>,
) -> anyhow::Result<PostgresExecutionReport> {
    let mut journal = AppendJournal::open_resume(state_path).with_context(|| {
        "resume requires the append-only journal format; legacy JSON state requires explicit operator recovery"
    })?;
    let reviewed = journal.genesis().reviewed_plan.clone();
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    validate_postgres_execution_operations(&reviewed)?;
    validate_resume_binding(
        &journal.genesis().binding,
        &reviewed,
        journal.genesis().accepted_outage_projection.as_ref(),
    )?;
    if matches!(
        journal.projection().status,
        MigrationStatus::Completed | MigrationStatus::CompletedWithApprovedTransformations
    ) {
        return Err(anyhow!("completed migration state cannot be resumed"));
    }
    if journal.projection().status == MigrationStatus::ManualReconciliationRequired {
        return Err(anyhow!(
            "migration requires manual reconciliation and cannot resume automatically"
        ));
    }

    let source_config = PostgresEndpointConfig::read(source_config_path)?;
    let target_config = PostgresEndpointConfig::read(target_config_path)?;
    validate_reviewed_tls(&reviewed, "source_tls", &source_config)?;
    validate_reviewed_tls(&reviewed, "target_tls", &target_config)?;
    let accepted_external_quiesce = validate_external_quiesce_resume(
        &reviewed,
        journal.genesis().accepted_external_quiesce.as_ref(),
        &journal.genesis().binding,
        external_quiesce_attestation_path,
    )?;
    let fenced = match (
        reviewed.plan.consistency_mode.as_str(),
        fence_paths,
        accepted_external_quiesce.as_ref(),
    ) {
        ("write-fence", Some((admin_path, artifact_path)), None) => {
            let installed: InstalledPostgresFence = read_json(artifact_path)?;
            if installed.evidence != journal.genesis().binding.consistency_evidence {
                return Err(anyhow!(
                    "write-fence artifact differs from the state consistency evidence"
                ));
            }
            if !matches!(installed.evidence, ConsistencyEvidence::WriteFence { .. }) {
                return Err(anyhow!("resume requires write-fence consistency evidence"));
            }
            let admin = PostgresEndpointConfig::read(admin_path)?;
            validate_separate_postgres_credentials(&source_config, &target_config, &admin)?;
            if admin.tls.insecure {
                return Err(anyhow!(
                    "fence administration cannot resume with insecure TLS"
                ));
            }
            validate_fence_tls_binding(&installed, &admin)?;
            if postgres_write_fence_is_released(&admin, &installed.evidence)? {
                if journal.projection().status != MigrationStatus::Verifying
                    || !journal.projection().schema_verified
                {
                    return Err(anyhow!(
                        "released fence is not paired with a fully verified durable migration journal"
                    ));
                }
                journal.transition_status(MigrationStatus::Completed)?;
                let copied_rows = journal
                    .projection()
                    .copy_cursors
                    .values()
                    .try_fold(0_u64, |total, cursor| total.checked_add(cursor.rows))
                    .ok_or_else(|| anyhow!("committed row count overflow"))?;
                return Ok(PostgresExecutionReport {
                    state: state_path.to_path_buf(),
                    copied_rows,
                    committed_chunks: journal.projection().last_chunk_id,
                });
            }
            let inventory = attest_postgres_write_fence(&admin, &installed.evidence)?;
            Some((admin, installed, inventory))
        }
        ("consistent-snapshot", None, Some(_)) => None,
        ("write-fence", None, _) => {
            return Err(anyhow!("write-fence resume requires exact fence evidence"));
        }
        ("consistent-snapshot", Some(_), _) => {
            return Err(anyhow!(
                "external-quiesce resume must not accept write-fence evidence"
            ));
        }
        _ => return Err(anyhow!("resume source-profile evidence is incomplete")),
    };

    let cancellation = injected_cancellation.unwrap_or_default();
    cancellation.observe_process_sigint()?;
    let source = Arc::new(PostgresSourceFactory::new_with_cancellation(
        source_config.clone(),
        cancellation.clone(),
    ));
    let target = Arc::new(PostgresTargetFactory::new_with_cancellation(
        target_config.clone(),
        cancellation.clone(),
    ));
    let _cancellation_monitor = CancellationMonitor::start(
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
    let snapshot = source.capture_snapshot()?;
    if snapshot.endpoint_identity != journal.genesis().binding.source_endpoint
        || snapshot.endpoint_identity != reviewed.plan.source_endpoint_identity
    {
        return Err(anyhow!("source endpoint differs from durable binding"));
    }
    let (mut source_catalog, mut unsupported, _) = source.captured_catalog(&snapshot)?;
    if let Some((_, _, inventory)) = &fenced {
        remove_attested_fence_objects(&mut source_catalog, &mut unsupported, inventory)?;
    }
    if unsupported.blocks_execution() {
        return Err(anyhow!(
            "resumed source contains execution-blocking unsupported semantics"
        ));
    }
    let source_fingerprint = catalog_fingerprint(&source_catalog)?;
    if source_fingerprint != journal.genesis().binding.source_schema_fingerprint
        || source_fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&source_catalog)
    {
        if let Some(reviewed_catalog) = reviewed.plan.source_catalog.as_ref() {
            let drifted_sequences = postgres_sequence_drift_ids(reviewed_catalog, &source_catalog);
            if !drifted_sequences.is_empty() {
                return Err(anyhow!(
                    "resumed source sequence contracts changed: {}",
                    drifted_sequences.join(", ")
                ));
            }
        }
        return Err(anyhow!("resumed source catalog differs from reviewed plan"));
    }
    validate_copy_table_shapes(&reviewed, &source_catalog)?;

    let target_snapshot = target.inspect_endpoint()?;
    if target_snapshot.endpoint_identity != journal.genesis().binding.target_endpoint
        || target_snapshot.endpoint_identity
            != reviewed.plan.execution_target_endpoint_identity()?
    {
        return Err(anyhow!("target endpoint differs from durable binding"));
    }
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
        "plain_insert",
        "bulk_write",
    ])?;

    let mut reader = source.open_reader(&snapshot, cancellation.clone())?;
    if !reader.read_only_evidence().server_enforced || reader.snapshot() != &snapshot {
        return Err(anyhow!("resumed source snapshot evidence is invalid"));
    }

    resume_pre_data_schema(
        ResumeDdlContext {
            reviewed: &reviewed,
            source_catalog: &source_catalog,
            target: &target,
            fenced: fenced.as_ref(),
            cancellation: &cancellation,
        },
        &mut journal,
    )?;

    let mut copied_rows = journal
        .projection()
        .copy_cursors
        .values()
        .try_fold(0_u64, |total, cursor| total.checked_add(cursor.rows))
        .ok_or_else(|| anyhow!("committed row count overflow"))?;

    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        cancellation.check()?;
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(&source_catalog, table, Some(operation))?;
        let operation_state = operation_state(&journal, operation.id.as_str())?;
        if operation_state == OperationState::Verified {
            continue;
        }
        if operation_state == OperationState::Pending {
            cancellation.check()?;
            journal.transition_operation(operation.id.as_str(), OperationState::Running)?;
        } else if operation_state == OperationState::Prepared {
            journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
            continue;
        } else if operation_state == OperationState::Committed {
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
            continue;
        } else if operation_state != OperationState::Running {
            return Err(anyhow!("copy operation has an invalid resumable state"));
        }

        if let Some(prepared) = journal.projection().prepared_chunk.clone() {
            if prepared.operation_id != operation.id.as_str() {
                return Err(anyhow!("prepared chunk belongs to a different operation"));
            }
            attest_fence_if_present(fenced.as_ref())?;
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
                target.as_ref(),
                &mut journal,
                table,
                &shape,
                &expected,
                cancellation.clone(),
            )? {
                PreparedResolution::MarkedCommitted => {}
                PreparedResolution::RetryRequired => {
                    let writable = writable_batch(&expected, &shape)?;
                    let mut writer = open_postgres_batch_writer(
                        target.as_ref(),
                        table,
                        &writable,
                        shape.write_policy,
                        &cancellation,
                    )?;
                    check_cancellation_before_commit(writer.as_mut(), &cancellation)?;
                    if let Err(error) = writer.commit() {
                        if error == ConnectionError::Cancelled {
                            return Err(rollback_cancelled_commit(
                                writer.as_mut(),
                                error,
                                &cancellation,
                            ));
                        }
                        return Err(error.into());
                    }
                    journal.commit_chunk_after_ack()?;
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

        let mut after = journal
            .projection()
            .copy_cursors
            .get(operation.id.as_str())
            .map(|cursor| KeyTuple::new(cursor.final_key.clone()));
        loop {
            let next_chunk_id = journal
                .projection()
                .last_chunk_id
                .checked_add(1)
                .ok_or_else(|| anyhow!("chunk identifier overflow"))?;
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
            cancellation.check()?;
            journal.prepare_chunk(PreparedChunk {
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
            })?;
            let writable = writable_batch(&batch, &shape)?;
            let mut writer = open_postgres_batch_writer(
                target.as_ref(),
                table,
                &writable,
                shape.write_policy,
                &cancellation,
            )?;
            check_cancellation_before_commit(writer.as_mut(), &cancellation)?;
            match writer.commit() {
                Ok(()) => journal.commit_chunk_after_ack()?,
                Err(ConnectionError::CommitOutcomeUnknown(_)) => {
                    match reconcile_live_prepared_chunk(
                        target.as_ref(),
                        &mut journal,
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
                Err(error) => {
                    if error == ConnectionError::Cancelled {
                        return Err(rollback_cancelled_commit(
                            writer.as_mut(),
                            error,
                            &cancellation,
                        ));
                    }
                    return Err(error.into());
                }
            }
            copied_rows = copied_rows
                .checked_add(u64::try_from(batch.len())?)
                .ok_or_else(|| anyhow!("copied row count overflow"))?;
            after = Some(final_key);
        }
        journal.transition_operation(operation.id.as_str(), OperationState::Prepared)?;
        journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
        journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
    }
    verify_rows_before_sequence_equality(
        &reviewed,
        &source_catalog,
        &source_config,
        reader.as_mut(),
        target.as_ref(),
        &journal,
        None,
        cancellation.clone(),
    )?;
    record_postgres_sequence_equality(&reviewed, &source_catalog, source.as_ref(), &mut journal)?;
    process_postgres_sequences(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        None,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_indexes(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        None,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_foreign_keys(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        None,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_programmable_objects(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    if journal.projection().status == MigrationStatus::Running {
        journal.transition_status(MigrationStatus::Verifying)?;
    } else if journal.projection().status != MigrationStatus::Verifying {
        return Err(anyhow!("migration status cannot resume verification"));
    }
    attest_fence_if_present(fenced.as_ref())?;
    let mut verifier = target.open_verifier(cancellation.clone())?;
    let mut committed_chunks = journal.all_committed_chunks()?.peekable();
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
        let shape = table_shape(&source_catalog, table, Some(operation))?;
        let (manifest_hash, source_hash, target_hash) = verify_live_table(
            reader.as_mut(),
            verifier.as_mut(),
            &mut committed_chunks,
            operation.id.as_str(),
            table,
            &shape,
            execution_page_limit(&source_config)?,
        )?;
        if !journal
            .projection()
            .table_verifications
            .contains(operation.id.as_str())
        {
            journal.verify_table(
                operation.id.as_str(),
                manifest_hash,
                source_hash,
                target_hash,
            )?;
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
        complete_operation_if_needed(&mut journal, verify.id.as_str())?;
    }
    if let Some(chunk) = committed_chunks.next() {
        let chunk = chunk?;
        return Err(anyhow!(
            "journal contains an unexpected committed chunk for operation {}",
            chunk.operation_id
        ));
    }
    drop(verifier);
    let mut verifier = target.open_verifier(cancellation.clone())?;
    verify_postgres_partition_topologies(
        &reviewed,
        &source_catalog,
        &target,
        reader.as_mut(),
        verifier.as_mut(),
        &mut journal,
        execution_page_limit(&source_config)?,
    )?;
    cancellation.check()?;
    record_postgres_sequence_equality(&reviewed, &source_catalog, source.as_ref(), &mut journal)?;
    verify_postgres_sequence_states(&source_catalog, &target)?;
    verify_postgres_generated_columns(&source_catalog, &target)?;
    drop(verifier);
    drop(reader);
    record_external_quiesce_verified_rescan(
        &reviewed,
        &source_catalog,
        &source_config,
        source.as_ref(),
        target.as_ref(),
        &mut journal,
        cancellation.clone(),
    )?;
    verify_schema_projection(&source_catalog, target.as_ref())?;
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("reviewed plan has no schema verification operation"))?;
    complete_operation_if_needed(&mut journal, verify_schema.id.as_str())?;
    if !journal.projection().schema_verified {
        journal.verify_schema(catalog_fingerprint(&source_catalog)?)?;
    }
    attest_fence_if_present(fenced.as_ref())?;
    cancellation.check()?;
    if let Some((admin, installed, _)) = &fenced {
        let generation = match &installed.evidence {
            ConsistencyEvidence::WriteFence { generation, .. } => generation,
            ConsistencyEvidence::NativeSnapshot { .. }
            | ConsistencyEvidence::MySqlExternalFreeze { .. } => {
                unreachable!("validated above")
            }
        };
        cancellation.check()?;
        release_postgres_write_fence(admin, generation, &installed.token)?;
    }
    journal.transition_status(MigrationStatus::Completed)?;
    Ok(PostgresExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: journal.projection().last_chunk_id,
    })
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)] // Internal operator context plus injected cancellation/fault controls.
fn execute_postgres_plan_internal(
    plan_path: &Path,
    source_config_path: &Path,
    target_config_path: &Path,
    approval_reference: &str,
    state_path: &Path,
    fence_paths: Option<(&Path, &Path)>,
    external_quiesce_attestation_path: Option<&Path>,
    interruption: Option<ExecutionInterruption>,
    injected_cancellation: Option<CancellationToken>,
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
    validate_reviewed_tls(&reviewed, "source_tls", &source_config)?;
    validate_reviewed_tls(&reviewed, "target_tls", &target_config)?;
    if source_config.credential_env == target_config.credential_env {
        return Err(anyhow!(
            "source and target must use separate credential references"
        ));
    }
    let fenced = match (reviewed.plan.consistency_mode.as_str(), fence_paths) {
        ("consistent-snapshot", None) => None,
        ("write-fence", Some((admin_path, artifact_path))) => {
            let admin = PostgresEndpointConfig::read(admin_path)?;
            if admin.tls.insecure {
                return Err(anyhow!("fence administration requires authenticated TLS"));
            }
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
            validate_fence_tls_binding(&installed, &admin)?;
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

    let cancellation = injected_cancellation.unwrap_or_default();
    cancellation.observe_process_sigint()?;
    let source = Arc::new(PostgresSourceFactory::new_with_cancellation(
        source_config.clone(),
        cancellation.clone(),
    ));
    let target = Arc::new(PostgresTargetFactory::new_with_cancellation(
        target_config.clone(),
        cancellation.clone(),
    ));
    let _cancellation_monitor = CancellationMonitor::start(
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
            "fresh source snapshot contains execution-blocking unsupported semantics: {:?}",
            execution_unsupported.objects
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
    let accepted_external_quiesce = validate_external_quiesce_admission(
        &reviewed,
        external_quiesce_attestation_path,
        &snapshot.endpoint_identity,
        &source_fingerprint,
    )?;
    let external_quiesce_attestation_digest = accepted_external_quiesce
        .as_ref()
        .map(PostgresExternalQuiesceAttestation::canonical_hash)
        .transpose()
        .map_err(|error| anyhow!(error.to_string()))?;
    validate_copy_table_shapes(&reviewed, &source_catalog)?;
    let accepted_outage_projection = reviewed
        .plan
        .outage_policy
        .as_ref()
        .map(|policy| -> anyhow::Result<_> {
            let accepted = source.refresh_outage_projection(&snapshot, &source_catalog, policy)?;
            let digest = accepted
                .canonical_hash(policy)
                .map_err(|error| anyhow!(error.to_string()))?;
            Ok((accepted, digest))
        })
        .transpose()?;
    let outage_projection_digest = accepted_outage_projection
        .as_ref()
        .map(|(_, digest)| digest.clone());
    let accepted_outage_projection = accepted_outage_projection.map(|(accepted, _)| accepted);

    let target_preflight = target.inspect_endpoint()?;
    if target_preflight.endpoint_identity != reviewed.plan.execution_target_endpoint_identity()? {
        return Err(anyhow!(
            "target endpoint identity differs from reviewed plan"
        ));
    }
    if catalog_fingerprint(&target_preflight.catalog)?
        != reviewed.plan.execution_target_catalog_fingerprint()?
    {
        return Err(anyhow!(
            "target catalog fingerprint changed after plan review"
        ));
    }
    target.capabilities().require(&[
        "transactions",
        "cancellation",
        "typed_identifiers",
        "bound_parameters",
        "plain_insert",
        "bulk_write",
    ])?;
    target.assert_empty_and_owned()?;
    let mut verification_pipeline = VerificationPipeline::start(
        Arc::clone(&source),
        Arc::clone(&target),
        snapshot.clone(),
        cancellation.clone(),
    )?;

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
        target_schema_fingerprint: reviewed
            .plan
            .execution_target_catalog_fingerprint()?
            .to_owned(),
        outage_projection_digest,
        external_quiesce_attestation_digest,
        mysql_freeze_attestation_digest: None,
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    };
    let genesis = append_journal_genesis(
        binding,
        reviewed.clone(),
        accepted_outage_projection,
        accepted_external_quiesce,
    );
    let mut journal = AppendJournal::create_new(state_path, genesis)?;

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
        .filter(|operation| is_postgres_pre_data_operation(operation))
        .map(|operation| operation.id.as_str())
        .collect::<Vec<_>>();
    cancellation.check()?;
    journal.prepare_operations_atomic(create_operation_ids.iter().copied())?;
    interrupt_if(interruption, ExecutionInterruption::AfterDdlPrepared)?;
    attest_fence_if_present(fenced.as_ref())?;
    cancellation.check()?;
    if let Err(initial_error) = target.create_pre_data_schema(&source_catalog) {
        if target_schema_matches(&source_catalog, target.as_ref())? {
            // The atomic DDL transaction committed, but its acknowledgement was lost.
        } else if target.assert_empty_and_owned().is_ok() {
            target.create_pre_data_schema(&source_catalog).with_context(|| {
                format!(
                    "retry create-only schema after an unacknowledged empty-target outcome ({initial_error})"
                )
            })?;
        } else {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "pre-data DDL has a partial or different target effect; manual reconciliation is required: {initial_error}"
            ));
        }
    }
    interrupt_if(interruption, ExecutionInterruption::AfterDdlCommitted)?;
    for operation_id in create_operation_ids {
        journal.transition_operation(operation_id, OperationState::Committed)?;
        journal.transition_operation(operation_id, OperationState::Verified)?;
    }

    let mut next_chunk_id = 1u64;
    let mut copied_rows = 0u64;
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        cancellation.check()?;
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(&source_catalog, table, Some(operation))?;
        if let Some(pipeline) = &verification_pipeline {
            pipeline.send(VerificationPipelineCommand::StartTable {
                operation_id: operation.id.to_string(),
                table: table.clone(),
                shape: shape.clone(),
                page_limit: execution_page_limit(&source_config)?,
            })?;
        }
        cancellation.check()?;
        journal.transition_operation(operation.id.as_str(), OperationState::Running)?;
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
            cancellation.check()?;
            let prepared_chunk = PreparedChunk {
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
            };
            journal.prepare_chunk(prepared_chunk.clone())?;
            if matches!(
                interruption,
                Some(ExecutionInterruption::AfterChunkPrepared)
            ) {
                return Err(injected_interruption(interruption));
            }
            let writable = writable_batch(&batch, &shape)?;
            let mut writer = open_postgres_batch_writer(
                target.as_ref(),
                table,
                &writable,
                shape.write_policy,
                &cancellation,
            )?;
            #[cfg(feature = "migration-fault-injection")]
            if let Some(ExecutionInterruption::NetworkCommitFault(port)) = interruption {
                arm_network_commit_fault(port)?;
            }
            check_cancellation_before_commit(writer.as_mut(), &cancellation)?;
            if let Err(error) = writer.commit() {
                if error == ConnectionError::Cancelled {
                    return Err(rollback_cancelled_commit(
                        writer.as_mut(),
                        error,
                        &cancellation,
                    ));
                }
                if !matches!(error, ConnectionError::CommitOutcomeUnknown(_)) {
                    return Err(error.into());
                }
                match reconcile_live_prepared_chunk(
                    target.as_ref(),
                    &mut journal,
                    table,
                    &shape,
                    &batch,
                    cancellation.clone(),
                )? {
                    PreparedResolution::MarkedCommitted => {}
                    PreparedResolution::RetryRequired => {
                        return Err(anyhow!(
                            "commit outcome is absent on the target; resume is required to retry the durable prepared intent"
                        ));
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
                #[cfg(feature = "migration-fault-injection")]
                if interruption == Some(ExecutionInterruption::TornChunkCommittedEnospc) {
                    journal.arm_fault(AppendJournalFault::TornChunkCommittedEnospc)?;
                } else if interruption == Some(ExecutionInterruption::ChunkCommittedSyncAckLost) {
                    journal.arm_fault(AppendJournalFault::ChunkCommittedSyncAckLost)?;
                }
                journal.commit_chunk_after_ack()?;
            }
            if let Some(pipeline) = &verification_pipeline {
                pipeline.send(VerificationPipelineCommand::Chunk(prepared_chunk))?;
            }
            copied_rows = copied_rows
                .checked_add(u64::try_from(batch.len()).context("batch size exceeds u64")?)
                .ok_or_else(|| anyhow!("copied row count overflow"))?;
            if interruption.is_some_and(|point| {
                let ExecutionInterruption::AfterCommittedChunks(limit) = point else {
                    return false;
                };
                journal.projection().last_chunk_id >= limit
            }) {
                return Err(anyhow!(
                    "injected interruption after a durable committed chunk"
                ));
            }
            after = Some(final_key);
        }
        journal.transition_operation(operation.id.as_str(), OperationState::Prepared)?;
        journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
        journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        if let Some(pipeline) = &verification_pipeline {
            pipeline.send(VerificationPipelineCommand::FinishTable)?;
        }
    }
    let pipelined_evidence = verification_pipeline
        .take()
        .map(VerificationPipeline::finish)
        .transpose()?;
    interrupt_after_pipelined_evidence(interruption)?;

    verify_rows_before_sequence_equality(
        &reviewed,
        &source_catalog,
        &source_config,
        reader.as_mut(),
        target.as_ref(),
        &journal,
        pipelined_evidence.as_deref(),
        cancellation.clone(),
    )?;
    record_postgres_sequence_equality(&reviewed, &source_catalog, source.as_ref(), &mut journal)?;

    process_postgres_sequences(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        interruption,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_indexes(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        interruption,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_foreign_keys(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        interruption,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    process_postgres_programmable_objects(
        &reviewed,
        &source_catalog,
        &target,
        &mut journal,
        &cancellation,
        || attest_fence_if_present(fenced.as_ref()),
    )?;
    journal.transition_status(MigrationStatus::Verifying)?;
    attest_fence_if_present(fenced.as_ref())?;
    if let Some(evidence) = pipelined_evidence {
        let mut evidence = evidence.into_iter();
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
            let observed = evidence
                .next()
                .ok_or_else(|| anyhow!("pipelined table evidence is incomplete"))?;
            if observed.operation_id != operation.id.as_str() {
                return Err(anyhow!("pipelined table evidence is out of order"));
            }
            let cursor = journal.projection().copy_cursors.get(operation.id.as_str());
            if observed.chunk_count != cursor.map_or(0, |cursor| cursor.chunks)
                || observed.row_count != cursor.map_or(0, |cursor| cursor.rows)
            {
                return Err(anyhow!(
                    "pipelined table evidence differs from the durable chunk manifest"
                ));
            }
            journal.verify_table(
                operation.id.as_str(),
                observed.manifest_hash,
                observed.source_hash,
                observed.target_hash,
            )?;
            let verify_operation = reviewed
                .plan
                .operations
                .iter()
                .find(|candidate| {
                    candidate.kind == OperationKind::VerifyTable
                        && candidate.table.as_ref() == Some(table)
                })
                .ok_or_else(|| anyhow!("table has no reviewed verification operation"))?;
            complete_operation_if_needed(&mut journal, verify_operation.id.as_str())?;
        }
        if evidence.next().is_some() {
            return Err(anyhow!("pipelined table evidence contains an extra table"));
        }
    } else {
        let mut verifier = target.open_verifier(cancellation.clone())?;
        let mut committed_chunks = journal.all_committed_chunks()?.peekable();
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
            let shape = table_shape(&source_catalog, table, Some(operation))?;
            let (manifest_hash, source_hash, target_hash) = verify_live_table(
                reader.as_mut(),
                verifier.as_mut(),
                &mut committed_chunks,
                operation.id.as_str(),
                table,
                &shape,
                execution_page_limit(&source_config)?,
            )?;
            journal.verify_table(
                operation.id.as_str(),
                manifest_hash,
                source_hash,
                target_hash,
            )?;
            let verify_operation = reviewed
                .plan
                .operations
                .iter()
                .find(|candidate| {
                    candidate.kind == OperationKind::VerifyTable
                        && candidate.table.as_ref() == Some(table)
                })
                .ok_or_else(|| anyhow!("table has no reviewed verification operation"))?;
            complete_operation_if_needed(&mut journal, verify_operation.id.as_str())?;
        }
        if let Some(chunk) = committed_chunks.next() {
            let chunk = chunk?;
            return Err(anyhow!(
                "journal contains an unexpected committed chunk for operation {}",
                chunk.operation_id
            ));
        }
    }
    let mut verifier = target.open_verifier(cancellation.clone())?;
    verify_postgres_partition_topologies(
        &reviewed,
        &source_catalog,
        &target,
        reader.as_mut(),
        verifier.as_mut(),
        &mut journal,
        execution_page_limit(&source_config)?,
    )?;
    cancellation.check()?;
    record_postgres_sequence_equality(&reviewed, &source_catalog, source.as_ref(), &mut journal)?;
    verify_postgres_sequence_states(&source_catalog, &target)?;
    verify_postgres_generated_columns(&source_catalog, &target)?;
    drop(verifier);
    drop(reader);
    record_external_quiesce_verified_rescan(
        &reviewed,
        &source_catalog,
        &source_config,
        source.as_ref(),
        target.as_ref(),
        &mut journal,
        cancellation.clone(),
    )?;
    verify_schema_projection(&source_catalog, target.as_ref())?;
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("reviewed plan has no schema verification operation"))?;
    complete_operation_if_needed(&mut journal, verify_schema.id.as_str())?;
    journal.verify_schema(catalog_fingerprint(&source_catalog)?)?;
    interrupt_if(interruption, ExecutionInterruption::AfterAllVerified)?;
    attest_fence_if_present(fenced.as_ref())?;
    cancellation.check()?;
    if let Some((admin, installed, _)) = &fenced {
        let generation = match &installed.evidence {
            ConsistencyEvidence::WriteFence { generation, .. } => generation,
            ConsistencyEvidence::NativeSnapshot { .. } => {
                return Err(anyhow!("fence artifact contains native snapshot evidence"));
            }
            ConsistencyEvidence::MySqlExternalFreeze { .. } => {
                return Err(anyhow!("fence artifact contains MySQL freeze evidence"));
            }
        };
        cancellation.check()?;
        release_postgres_write_fence(admin, generation, &installed.token)?;
    }
    interrupt_if(interruption, ExecutionInterruption::AfterFenceReleased)?;
    journal.transition_status(MigrationStatus::Completed)?;
    Ok(PostgresExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: next_chunk_id - 1,
    })
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_external_quiesce_admission(
    reviewed: &ReviewedPlan,
    attestation_path: Option<&Path>,
    source_endpoint_identity: &str,
    source_catalog_fingerprint: &str,
) -> anyhow::Result<Option<PostgresExternalQuiesceAttestation>> {
    match (
        reviewed.plan.postgres_source_profile.as_ref(),
        attestation_path,
    ) {
        (Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }), Some(path)) => {
            let attestation: PostgresExternalQuiesceAttestation = read_json(path)?;
            let observed_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
                .as_secs();
            attestation
                .validate_for_plan(
                    source_endpoint_identity,
                    source_catalog_fingerprint,
                    observed_at,
                )
                .map_err(|error| anyhow!(error.to_string()))?;
            Ok(Some(attestation))
        }
        (Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }), None) => Err(
            anyhow!("external-quiesce execution requires its protected attestation artifact"),
        ),
        (_, Some(_)) => Err(anyhow!(
            "external-quiesce attestation is forbidden for this reviewed source profile"
        )),
        (_, None) => Ok(None),
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_external_quiesce_resume(
    reviewed: &ReviewedPlan,
    accepted: Option<&PostgresExternalQuiesceAttestation>,
    binding: &ResumeBinding,
    supplied_path: Option<&Path>,
) -> anyhow::Result<Option<PostgresExternalQuiesceAttestation>> {
    match (
        reviewed.plan.postgres_source_profile.as_ref(),
        accepted,
        binding.external_quiesce_attestation_digest.as_ref(),
        supplied_path,
    ) {
        (
            Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }),
            Some(accepted),
            Some(expected_digest),
            Some(path),
        ) => {
            accepted
                .validate()
                .map_err(|error| anyhow!(error.to_string()))?;
            let supplied: PostgresExternalQuiesceAttestation = read_json(path)?;
            supplied
                .validate()
                .map_err(|error| anyhow!(error.to_string()))?;
            if supplied.status == super::postgres_profile::PostgresExternalQuiesceStatus::Withdrawn
            {
                return Err(anyhow!("external-quiesce attestation is withdrawn"));
            }
            let supplied_digest = supplied
                .canonical_hash()
                .map_err(|error| anyhow!(error.to_string()))?;
            if &supplied_digest != expected_digest || &supplied != accepted {
                return Err(anyhow!(
                    "external-quiesce attestation differs from durable admission"
                ));
            }
            Ok(Some(supplied))
        }
        (Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }), _, _, _) => Err(
            anyhow!("external-quiesce resume evidence is incomplete or missing"),
        ),
        (_, None, None, None) => Ok(None),
        _ => Err(anyhow!(
            "external-quiesce resume evidence is forbidden for this reviewed source profile"
        )),
    }
}

#[cfg(feature = "enterprise-migration-spike")]
type ActivePostgresFence = (
    PostgresEndpointConfig,
    InstalledPostgresFence,
    super::postgres_fence::FenceInventory,
);

#[cfg(feature = "enterprise-migration-spike")]
fn attest_fence_if_present(fenced: Option<&ActivePostgresFence>) -> anyhow::Result<()> {
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
fn interrupt_after_pipelined_evidence(
    interruption: Option<ExecutionInterruption>,
) -> anyhow::Result<()> {
    #[cfg(feature = "migration-fault-injection")]
    interrupt_if(interruption, ExecutionInterruption::AfterPipelinedEvidence)?;
    #[cfg(not(feature = "migration-fault-injection"))]
    let _ = interruption;
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn arm_network_commit_fault(port: u16) -> anyhow::Result<()> {
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
    use std::time::Duration;

    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let mut control = TcpStream::connect_timeout(&address.into(), Duration::from_secs(10))?;
    control.set_read_timeout(Some(Duration::from_secs(10)))?;
    control.set_write_timeout(Some(Duration::from_secs(10)))?;
    control.write_all(b"ARM\n")?;
    let mut response = [0_u8; 6];
    control.read_exact(&mut response)?;
    if &response != b"ARMED\n" {
        return Err(anyhow!(
            "commit fault proxy returned an invalid arm response"
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_resume_binding(
    binding: &ResumeBinding,
    reviewed: &ReviewedPlan,
    accepted_outage_projection: Option<&AcceptedOutageProjection>,
) -> anyhow::Result<()> {
    let expected_consistency = match reviewed.plan.postgres_source_profile.as_ref() {
        Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }) => {
            PostgresConsistencyMode::ConsistentSnapshot
        }
        _ => PostgresConsistencyMode::WriteFence,
    };
    if reviewed.plan.consistency_mode != expected_consistency.as_str() {
        return Err(anyhow!(
            "reviewed source profile has incompatible resumable consistency evidence"
        ));
    }
    if binding.migration_id != reviewed.plan.migration_id
        || binding.plan_hash != reviewed.plan_hash.to_string()
        || binding.tool_version != reviewed.plan.tool_version
        || binding.source_endpoint != reviewed.plan.source_endpoint_identity
        || binding.target_endpoint != reviewed.plan.execution_target_endpoint_identity()?
        || binding.source_schema_fingerprint != reviewed.plan.source_catalog_fingerprint
        || binding.target_schema_fingerprint
            != reviewed.plan.execution_target_catalog_fingerprint()?
        || binding.conversion_policy != reviewed.plan.conversion_policy
        || binding.canonical_encoding_version != CANONICAL_ENCODING_VERSION
        || reviewed.plan.canonical_encoding_version != CANONICAL_ENCODING_VERSION
        || binding.tool_version != env!("CARGO_PKG_VERSION")
    {
        return Err(anyhow!(
            "migration state binding differs from reviewed plan or running tool"
        ));
    }
    if binding.approval_reference.trim().is_empty() {
        return Err(anyhow!("migration state has no approval reference"));
    }
    match (
        reviewed.plan.outage_policy.as_ref(),
        accepted_outage_projection,
        binding.outage_projection_digest.as_ref(),
    ) {
        (None, None, None) => {}
        (Some(policy), Some(accepted), Some(digest)) => {
            accepted
                .validate_against(policy)
                .map_err(|error| anyhow!(error.to_string()))?;
            if digest
                != &accepted
                    .canonical_hash(policy)
                    .map_err(|error| anyhow!(error.to_string()))?
            {
                return Err(anyhow!(
                    "migration state outage projection differs from reviewed plan"
                ));
            }
        }
        _ => {
            return Err(anyhow!(
                "migration state outage projection presence differs from reviewed plan"
            ));
        }
    }
    match (&expected_consistency, &binding.consistency_evidence) {
        (
            PostgresConsistencyMode::WriteFence,
            ConsistencyEvidence::WriteFence {
                endpoint_identity,
                business_catalog_fingerprint,
                ..
            },
        ) if endpoint_identity == &reviewed.plan.source_endpoint_identity
            && business_catalog_fingerprint == &reviewed.plan.source_catalog_fingerprint => {}
        (
            PostgresConsistencyMode::ConsistentSnapshot,
            ConsistencyEvidence::NativeSnapshot {
                endpoint_identity, ..
            },
        ) if endpoint_identity == &reviewed.plan.source_endpoint_identity => {}
        _ => {
            return Err(anyhow!(
                "durable consistency evidence differs from the reviewed source profile"
            ));
        }
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
    let typed_expected = match capability {
        "source_tls" => reviewed.plan.source_tls_binding.as_str(),
        "target_tls" => reviewed
            .plan
            .target_tls_binding
            .as_assessed()
            .map(String::as_str)
            .ok_or_else(|| anyhow!("reviewed plan omits target TLS evidence"))?,
        _ => return Err(anyhow!("unknown reviewed TLS capability {capability}")),
    };
    let legacy_expected = reviewed
        .plan
        .capabilities
        .get(capability)
        .ok_or_else(|| anyhow!("reviewed plan omits {capability}"))?;
    if legacy_expected != typed_expected {
        return Err(anyhow!(
            "{capability} typed evidence differs from the reviewed capability"
        ));
    }
    let observed = postgres_tls_binding(config)?;
    if typed_expected != observed {
        return Err(anyhow!("{capability} differs from the reviewed TLS policy"));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_fence_tls_binding(
    installed: &InstalledPostgresFence,
    config: &PostgresEndpointConfig,
) -> anyhow::Result<()> {
    let expected = installed.admin_tls_binding.as_ref().ok_or_else(|| {
        anyhow!(
            "legacy fence artifact has no TLS binding; automatic execution is refused, but authenticated explicit fence release remains available"
        )
    })?;
    if installed.format_version != POSTGRES_FENCE_ARTIFACT_VERSION {
        return Err(anyhow!("unsupported fence artifact format version"));
    }
    if expected != &postgres_tls_binding(config)? {
        return Err(anyhow!(
            "fence administration TLS policy differs from the installed fence artifact"
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
trait RunnerOperationProjection {
    fn runner_operation_state(&self, operation_id: &str) -> anyhow::Result<OperationState>;
}

#[cfg(feature = "enterprise-migration-spike")]
impl RunnerOperationProjection for AppendJournal {
    fn runner_operation_state(&self, operation_id: &str) -> anyhow::Result<OperationState> {
        journal_operation_state(self, operation_id)
    }
}

#[cfg(feature = "enterprise-migration-spike")]
impl RunnerOperationProjection for MigrationState {
    fn runner_operation_state(&self, operation_id: &str) -> anyhow::Result<OperationState> {
        self.operations
            .iter()
            .find(|operation| operation.operation_id == operation_id)
            .map(|operation| operation.state)
            .ok_or_else(|| anyhow!("migration state has no reviewed operation {operation_id}"))
    }
}

#[cfg(feature = "enterprise-migration-spike")]
fn operation_state(
    projection: &impl RunnerOperationProjection,
    operation_id: &str,
) -> anyhow::Result<OperationState> {
    projection.runner_operation_state(operation_id)
}

#[cfg(feature = "enterprise-migration-spike")]
fn complete_operation_if_needed(
    journal: &mut AppendJournal,
    operation_id: &str,
) -> anyhow::Result<()> {
    match operation_state(journal, operation_id)? {
        OperationState::Pending => {
            journal.transition_operation(operation_id, OperationState::Running)?;
            journal.transition_operation(operation_id, OperationState::Verified)?;
        }
        OperationState::Running => {
            journal.transition_operation(operation_id, OperationState::Verified)?;
        }
        OperationState::Committed => {
            journal.transition_operation(operation_id, OperationState::Verified)?;
        }
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
    fenced: Option<&'a ActivePostgresFence>,
    cancellation: &'a CancellationToken,
}

#[cfg(feature = "enterprise-migration-spike")]
fn resume_pre_data_schema(
    context: ResumeDdlContext<'_>,
    journal: &mut AppendJournal,
) -> anyhow::Result<()> {
    let create_ids = context
        .reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| is_postgres_pre_data_operation(operation))
        .map(|operation| operation.id.as_str())
        .collect::<Vec<_>>();
    let states = create_ids
        .iter()
        .map(|id| operation_state(journal, id))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if states
        .iter()
        .all(|state| *state == OperationState::Verified)
    {
        if !target_schema_matches(context.source_catalog, context.target)? {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "verified pre-data schema differs on resume; manual intervention is required"
            ));
        }
        return Ok(());
    }
    attest_fence_if_present(context.fenced)?;
    let schema_exists_exactly = target_schema_matches(context.source_catalog, context.target)?;
    if states.iter().all(|state| *state == OperationState::Pending) {
        if schema_exists_exactly {
            return Err(anyhow!(
                "target schema exists without a durable prepared DDL intent"
            ));
        }
        context.target.assert_empty_and_owned()?;
        context.cancellation.check()?;
        journal.prepare_operations_atomic(create_ids.iter().copied())?;
        context.cancellation.check()?;
        context
            .target
            .create_pre_data_schema(context.source_catalog)?;
    } else if states.iter().all(|state| {
        matches!(
            state,
            OperationState::Prepared | OperationState::Committed | OperationState::Verified
        )
    }) {
        if schema_exists_exactly
            && !target_planned_tables_are_empty(
                context.source_catalog,
                context.target,
                context.cancellation.clone(),
            )?
        {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "prepared DDL target contains rows outside durable migration intent"
            ));
        }
        if !schema_exists_exactly
            && states
                .iter()
                .all(|state| *state == OperationState::Prepared)
        {
            if context.target.assert_empty_and_owned().is_ok() {
                context
                    .target
                    .create_pre_data_schema(context.source_catalog)?;
            } else {
                journal.require_manual_reconciliation()?;
                return Err(anyhow!(
                    "prepared DDL has a partial or different target effect"
                ));
            }
        } else if !schema_exists_exactly {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "committed pre-data DDL differs from reviewed target semantics"
            ));
        }
    } else {
        return Err(anyhow!(
            "create-table operations have inconsistent atomic DDL states"
        ));
    }
    for operation_id in create_ids {
        match operation_state(journal, operation_id)? {
            OperationState::Prepared => {
                journal.transition_operation(operation_id, OperationState::Committed)?;
                journal.transition_operation(operation_id, OperationState::Verified)?;
            }
            OperationState::Committed => {
                journal.transition_operation(operation_id, OperationState::Verified)?;
            }
            OperationState::Verified => {}
            OperationState::Pending | OperationState::Running => {
                return Err(anyhow!("pre-data operation was not durably prepared"));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn is_postgres_pre_data_operation(operation: &PlanOperation) -> bool {
    matches!(
        operation.kind,
        OperationKind::CreateTable | OperationKind::CreateSequence
    ) || (operation.kind == OperationKind::CreateIndex
        && !operation.parameters.contains_key("postgres_index"))
        || matches!(
            &operation.kind,
            OperationKind::Vendor(name)
                if name == "create_postgres_partitioned_table"
                    || name == "create_postgres_partition"
        )
}

#[cfg(feature = "enterprise-migration-spike")]
fn target_planned_tables_are_empty(
    source_catalog: &VendorCatalog,
    target: &dyn TargetConnectionFactory,
    cancellation: CancellationToken,
) -> anyhow::Result<bool> {
    let mut verifier = target.open_verifier(cancellation)?;
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
            let shape = table_shape(source_catalog, &table, None)?;
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
#[allow(clippy::too_many_arguments)] // Exact pre-restore verification needs both endpoints and journal evidence.
fn verify_rows_before_sequence_equality(
    reviewed: &ReviewedPlan,
    source_catalog: &VendorCatalog,
    source_config: &PostgresEndpointConfig,
    reader: &mut dyn ReadSession,
    target: &dyn TargetConnectionFactory,
    journal: &AppendJournal,
    pipelined_evidence: Option<&[PipelinedTableEvidence]>,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    if reviewed.plan.consistency_mode == PostgresConsistencyMode::WriteFence.as_str()
        || postgres_sequences(source_catalog)?.is_empty()
    {
        return Ok(());
    }
    let copy_operations = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
        .collect::<Vec<_>>();
    if let Some(evidence) = pipelined_evidence {
        if evidence.len() != copy_operations.len() {
            return Err(anyhow!(
                "pipelined row evidence is incomplete before sequence equality"
            ));
        }
        for (operation, observed) in copy_operations.iter().zip(evidence) {
            let cursor = journal.projection().copy_cursors.get(operation.id.as_str());
            if observed.operation_id != operation.id.as_str()
                || observed.chunk_count != cursor.map_or(0, |cursor| cursor.chunks)
                || observed.row_count != cursor.map_or(0, |cursor| cursor.rows)
                || observed.source_hash != observed.target_hash
            {
                return Err(anyhow!(
                    "pipelined row evidence differs before sequence equality"
                ));
            }
        }
        return Ok(());
    }

    let mut verifier = target.open_verifier(cancellation)?;
    let mut committed_chunks = journal.all_committed_chunks()?.peekable();
    for operation in copy_operations {
        let table = operation
            .table
            .as_ref()
            .ok_or_else(|| anyhow!("copy operation has no table"))?;
        let shape = table_shape(source_catalog, table, Some(operation))?;
        verify_live_table(
            reader,
            verifier.as_mut(),
            &mut committed_chunks,
            operation.id.as_str(),
            table,
            &shape,
            execution_page_limit(source_config)?,
        )?;
    }
    if let Some(chunk) = committed_chunks.next() {
        return Err(anyhow!(
            "unexpected committed chunk before sequence equality: {}",
            chunk?.operation_id
        ));
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn record_postgres_sequence_equality(
    reviewed: &ReviewedPlan,
    source_catalog: &VendorCatalog,
    source: &PostgresSourceFactory,
    journal: &mut AppendJournal,
) -> anyhow::Result<()> {
    if reviewed.plan.consistency_mode == PostgresConsistencyMode::WriteFence.as_str() {
        return Ok(());
    }
    let initial = postgres_sequences(source_catalog)?;
    if initial.is_empty() {
        return Ok(());
    }
    let final_observation = source.observe_sequence_states(&initial)?;
    let drifted = initial
        .iter()
        .zip(&final_observation)
        .filter(|(before, after)| before != after)
        .map(|(sequence, _)| sequence.catalog_object_id.as_str())
        .collect::<Vec<_>>();
    if !drifted.is_empty() {
        return Err(anyhow!(
            "source sequence state changed during migration: {}",
            drifted.join(", ")
        ));
    }
    let evidence = PostgresSequenceEqualityEvidence::new(
        catalog_fingerprint(source_catalog)?,
        initial,
        final_observation,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
            .as_secs(),
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    if journal.projection().sequence_equality.is_none() {
        journal.record_sequence_equality(evidence)?;
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn postgres_sequence_drift_ids(
    reviewed_catalog: &VendorCatalog,
    observed_catalog: &VendorCatalog,
) -> Vec<String> {
    let Ok(reviewed) = postgres_sequences(reviewed_catalog) else {
        return Vec::new();
    };
    let Ok(observed) = postgres_sequences(observed_catalog) else {
        return reviewed
            .into_iter()
            .map(|sequence| sequence.catalog_object_id)
            .collect();
    };
    let reviewed = reviewed
        .into_iter()
        .map(|sequence| (sequence.catalog_object_id.clone(), sequence))
        .collect::<BTreeMap<_, _>>();
    let observed = observed
        .into_iter()
        .map(|sequence| (sequence.catalog_object_id.clone(), sequence))
        .collect::<BTreeMap<_, _>>();
    let mut object_ids = reviewed
        .keys()
        .chain(observed.keys())
        .cloned()
        .collect::<Vec<_>>();
    object_ids.sort();
    object_ids.dedup();
    object_ids.retain(|object_id| reviewed.get(object_id) != observed.get(object_id));
    object_ids
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)] // The proof binds reviewed intent, two endpoints, and journal state.
fn record_external_quiesce_verified_rescan(
    reviewed: &ReviewedPlan,
    source_catalog: &VendorCatalog,
    source_config: &PostgresEndpointConfig,
    source: &PostgresSourceFactory,
    target: &dyn TargetConnectionFactory,
    journal: &mut AppendJournal,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    if !matches!(
        reviewed.plan.postgres_source_profile.as_ref(),
        Some(PostgresSourceProfileContract::AttestedExternalQuiesce {
            verified_rescan: true,
            ..
        })
    ) {
        return Ok(());
    }

    cancellation.check()?;
    let snapshot = source.capture_snapshot()?;
    let (fresh_catalog, unsupported, _) = source.captured_catalog(&snapshot)?;
    if unsupported.blocks_execution()
        || fresh_catalog != *source_catalog
        || catalog_fingerprint(&fresh_catalog)? != reviewed.plan.source_catalog_fingerprint
    {
        return Err(anyhow!(
            "external-quiesce verified re-scan found source catalog drift"
        ));
    }
    let mut fresh_reader = source.open_reader(&snapshot, cancellation.clone())?;
    let source_read_only_server_enforced = fresh_reader.read_only_evidence().server_enforced;
    if !source_read_only_server_enforced || fresh_reader.snapshot() != &snapshot {
        return Err(anyhow!(
            "external-quiesce verified re-scan lacks exact read-only snapshot evidence"
        ));
    }
    let mut verifier = target.open_verifier(cancellation)?;
    let mut committed_chunks = journal.all_committed_chunks()?.peekable();
    let mut tables = Vec::new();
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
        let shape = table_shape(source_catalog, table, Some(operation))?;
        let (manifest_hash, fresh_source_hash, target_hash) = verify_live_table(
            fresh_reader.as_mut(),
            verifier.as_mut(),
            &mut committed_chunks,
            operation.id.as_str(),
            table,
            &shape,
            execution_page_limit(source_config)?,
        )?;
        let cursor = journal.projection().copy_cursors.get(operation.id.as_str());
        tables.push(PostgresExternalQuiesceRescanTableEvidence {
            operation_id: operation.id.to_string(),
            chunk_count: cursor.map_or(0, |cursor| cursor.chunks),
            row_count: cursor.map_or(0, |cursor| cursor.rows),
            manifest_hash,
            fresh_source_hash,
            target_hash,
        });
    }
    if let Some(chunk) = committed_chunks.next() {
        return Err(anyhow!(
            "external-quiesce verified re-scan found an unexpected chunk for {}",
            chunk?.operation_id
        ));
    }
    let evidence = PostgresExternalQuiesceRescanEvidence::new(
        catalog_fingerprint(source_catalog)?,
        &snapshot,
        source_read_only_server_enforced,
        tables,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| anyhow!("system clock is before the Unix epoch"))?
            .as_secs(),
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    if journal.projection().external_quiesce_rescan.is_none() {
        journal.record_external_quiesce_rescan(evidence)?;
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)] // Durable operation context and shared cancellation/fence checks.
fn process_postgres_sequences(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    journal: &mut AppendJournal,
    interruption: Option<ExecutionInterruption>,
    cancellation: &CancellationToken,
    mut attest: impl FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let sequences = postgres_sequences(catalog)?
        .into_iter()
        .map(|sequence| (sequence.catalog_object_id.clone(), sequence))
        .collect::<BTreeMap<_, _>>();
    for operation in reviewed.plan.operations.iter().filter(|operation| {
        matches!(
            &operation.kind,
            OperationKind::Vendor(name) if name == "restore_postgres_sequence"
        )
    }) {
        cancellation.check()?;
        let reviewed_sequence: PostgresSequence = serde_json::from_value(
            operation
                .parameters
                .get("postgres_sequence")
                .cloned()
                .ok_or_else(|| anyhow!("sequence restore operation omits postgres_sequence"))?,
        )?;
        let sequence = sequences
            .get(&reviewed_sequence.catalog_object_id)
            .ok_or_else(|| anyhow!("sequence restore operation refers to an unknown sequence"))?;
        if sequence != &reviewed_sequence {
            return Err(anyhow!(
                "sequence restore operation differs from the reviewed catalog"
            ));
        }
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Pending | OperationState::Running => {
                journal_prepare_effect(journal, operation.id.as_str())?;
                interrupt_if(interruption, ExecutionInterruption::AfterSequencePrepared)?;
            }
            OperationState::Prepared | OperationState::Committed | OperationState::Verified => {}
        }
        attest()?;
        let observed = match operation_state(journal, operation.id.as_str())? {
            OperationState::Prepared => match target.restore_sequence(sequence) {
                Ok(observed) => observed,
                Err(ConnectionError::InvalidRequest(reason)) => {
                    journal.require_manual_reconciliation()?;
                    return Err(anyhow!(
                        "sequence reconciliation requires manual intervention: {reason}"
                    ));
                }
                Err(error) => return Err(error.into()),
            },
            OperationState::Committed => target.inspect_sequence(sequence)?,
            OperationState::Verified => target.inspect_sequence(sequence)?,
            _ => unreachable!("validated sequence restore state"),
        };
        if observed != PostgresSequenceState::ExactState {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "target sequence {}.{} differs from reviewed state",
                sequence.namespace,
                sequence.name
            ));
        }
        if operation_state(journal, operation.id.as_str())? == OperationState::Prepared {
            journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
            interrupt_if(interruption, ExecutionInterruption::AfterSequenceCommitted)?;
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        } else if operation_state(journal, operation.id.as_str())? == OperationState::Committed {
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn verify_postgres_sequence_states(
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
) -> anyhow::Result<()> {
    for sequence in postgres_sequences(catalog)? {
        if target.inspect_sequence(&sequence)? != PostgresSequenceState::ExactState {
            return Err(anyhow!(
                "target sequence {}.{} changed before final verification",
                sequence.namespace,
                sequence.name
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn process_postgres_programmable_objects(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    mut attest: impl FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let programmable_objects = postgres_programmable_objects(catalog)?
        .into_iter()
        .map(|object| (object.catalog_object_id.clone(), object))
        .collect::<BTreeMap<_, _>>();
    for operation in reviewed.plan.operations.iter().filter(|operation| {
        operation.kind == OperationKind::CreateView
            || matches!(&operation.kind, OperationKind::Vendor(name) if name == "create_postgres_sql_function")
    }) {
        cancellation.check()?;
        let reviewed_object: PostgresProgrammableObject = serde_json::from_value(
            operation
                .parameters
                .get("postgres_programmable_object")
                .cloned()
                .ok_or_else(|| anyhow!("programmable-object operation omits postgres_programmable_object"))?,
        )?;
        let object = programmable_objects
            .get(&reviewed_object.catalog_object_id)
            .ok_or_else(|| anyhow!("programmable-object operation refers to an unknown object"))?;
        if object != &reviewed_object {
            return Err(anyhow!(
                "programmable-object operation differs from the reviewed catalog"
            ));
        }
        let expected_kind = match &object.ast {
            PostgresDurableAst::View(_) => OperationKind::CreateView,
            PostgresDurableAst::SqlFunction(_) => {
                OperationKind::Vendor("create_postgres_sql_function".into())
            }
        };
        let expected_identity = QualifiedTable {
            namespace: object.namespace.clone(),
            name: object.name.clone(),
        };
        if operation.kind != expected_kind || operation.table.as_ref() != Some(&expected_identity) {
            return Err(anyhow!(
                "programmable-object operation kind or identity differs from the reviewed catalog"
            ));
        }

        let state = operation_state(journal, operation.id.as_str())?;
        if matches!(state, OperationState::Pending | OperationState::Running) {
            journal_prepare_effect(journal, operation.id.as_str())?;
        }
        attest()?;
        let observed = match operation_state(journal, operation.id.as_str())? {
            OperationState::Prepared => match target.reconcile_programmable_object(object) {
                Ok(observed) => observed,
                Err(ConnectionError::InvalidRequest(reason)) => {
                    journal.require_manual_reconciliation()?;
                    return Err(anyhow!(
                        "programmable-object reconciliation requires manual intervention: {reason}"
                    ));
                }
                Err(error) => return Err(error.into()),
            },
            OperationState::Committed | OperationState::Verified => {
                target.inspect_programmable_object(object)?
            }
            OperationState::Pending | OperationState::Running => {
                return Err(anyhow!("programmable object was not durably prepared"));
            }
        };
        if observed != PostgresProgrammableObjectState::Exact {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "target programmable object {} differs from reviewed semantics",
                object.catalog_object_id
            ));
        }
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Prepared => {
                journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
                journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
            }
            OperationState::Committed => {
                journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
            }
            OperationState::Verified => {}
            OperationState::Pending | OperationState::Running => {
                return Err(anyhow!("programmable object has an incomplete durable state"));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn verify_postgres_generated_columns(
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
) -> anyhow::Result<()> {
    for generated in postgres_generated_columns(catalog)? {
        if target.inspect_generated_column(&generated)? != PostgresGeneratedColumnState::Exact {
            return Err(anyhow!(
                "target generated column {}.{}.{} differs from the reviewed expression or dependency contract",
                generated.table.namespace,
                generated.table.name,
                generated.column
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)]
fn verify_postgres_partition_topologies(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    reader: &mut dyn ReadSession,
    verifier: &mut dyn super::connection::VerificationSession,
    journal: &mut AppendJournal,
    page_limit: u32,
) -> anyhow::Result<()> {
    let catalog_topologies = postgres_partition_topologies(catalog)?;
    for operation in reviewed.plan.operations.iter().filter(|operation| {
        matches!(&operation.kind, OperationKind::Vendor(name) if name == "verify_postgres_partition_topology")
    }) {
        let reviewed_topology: PostgresPartitionTopology = serde_json::from_value(
            operation
                .parameters
                .get("postgres_partition_topology")
                .cloned()
                .ok_or_else(|| anyhow!("partition verification omits its topology contract"))?,
        )?;
        if !catalog_topologies
            .iter()
            .any(|topology| topology == &reviewed_topology)
        {
            return Err(anyhow!(
                "partition verification differs from the reviewed source catalog"
            ));
        }
        if target.inspect_partition_topology(&reviewed_topology)?
            != PostgresPartitionTopologyState::Exact
        {
            return Err(anyhow!(
                "target partition topology for {}.{} is not exact",
                reviewed_topology.root.namespace,
                reviewed_topology.root.name
            ));
        }
        let copy = reviewed
            .plan
            .operations
            .iter()
            .find(|candidate| {
                candidate.kind == OperationKind::CopyTable
                    && candidate.table.as_ref() == Some(&reviewed_topology.root)
            })
            .ok_or_else(|| anyhow!("partition root has no reviewed copy operation"))?;
        let shape = table_shape(catalog, &reviewed_topology.root, Some(copy))?;
        for leaf in &reviewed_topology.leaves {
            verify_physical_partition_leaf(
                reader,
                verifier,
                &leaf.table,
                &shape,
                page_limit,
            )?;
        }
        complete_operation_if_needed(journal, operation.id.as_str())?;
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn verify_physical_partition_leaf(
    reader: &mut dyn ReadSession,
    verifier: &mut dyn super::connection::VerificationSession,
    leaf: &QualifiedTable,
    shape: &TableShape,
    page_limit: u32,
) -> anyhow::Result<()> {
    let mut after = None;
    loop {
        let request = KeysetPage {
            table: leaf.clone(),
            projection: shape.projection.clone(),
            key: shape.key.clone(),
            after: after.clone(),
            limit: page_limit,
        };
        let expected = reader.select_page_only(&request)?;
        let actual = verifier.select_page_only(&request)?;
        if expected.rows() != actual.rows() {
            return Err(anyhow!(
                "physical partition {}.{} differs from the reviewed source",
                leaf.namespace,
                leaf.name
            ));
        }
        if expected.is_empty() {
            return Ok(());
        }
        if expected.len() > page_limit as usize {
            return Err(anyhow!("physical partition page exceeds its limit"));
        }
        after = Some(batch_final_key(&expected, shape)?);
        if expected.len() < page_limit as usize {
            let tail = KeysetPage {
                after: after.clone(),
                limit: 1,
                ..request
            };
            if !reader.select_page_only(&tail)?.is_empty()
                || !verifier.select_page_only(&tail)?.is_empty()
            {
                return Err(anyhow!("physical partition has an unverified tail"));
            }
            return Ok(());
        }
    }
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)] // Durable operation context and shared cancellation/fence checks.
fn process_postgres_indexes(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    journal: &mut AppendJournal,
    interruption: Option<ExecutionInterruption>,
    cancellation: &CancellationToken,
    mut attest: impl FnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let indexes = postgres_post_data_indexes(catalog)?
        .into_iter()
        .map(|index| (index.catalog_object_id.clone(), index))
        .collect::<BTreeMap<_, _>>();
    for operation in reviewed.plan.operations.iter().filter(|operation| {
        operation.kind == OperationKind::CreateIndex
            && operation.parameters.contains_key("postgres_index")
    }) {
        cancellation.check()?;
        let reviewed_index: PostgresIndex = serde_json::from_value(
            operation
                .parameters
                .get("postgres_index")
                .cloned()
                .ok_or_else(|| anyhow!("post-data index operation omits postgres_index"))?,
        )?;
        let index = indexes
            .get(&reviewed_index.catalog_object_id)
            .ok_or_else(|| anyhow!("post-data index operation refers to an unknown index"))?;
        if index != &reviewed_index || operation.table.as_ref() != Some(&index.table) {
            return Err(anyhow!(
                "post-data index operation differs from the reviewed catalog"
            ));
        }
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Pending | OperationState::Running => {
                journal_prepare_effect(journal, operation.id.as_str())?;
                interrupt_if(interruption, ExecutionInterruption::AfterIndexPrepared)?;
            }
            OperationState::Prepared | OperationState::Committed | OperationState::Verified => {}
        }
        attest()?;
        let observed = match operation_state(journal, operation.id.as_str())? {
            OperationState::Prepared => match target.reconcile_index(index) {
                Ok(observed) => observed,
                Err(ConnectionError::InvalidRequest(reason)) => {
                    journal.require_manual_reconciliation()?;
                    return Err(anyhow!(
                        "index reconciliation requires manual intervention: {reason}"
                    ));
                }
                Err(error) => return Err(error.into()),
            },
            OperationState::Committed => target.inspect_index(index)?,
            OperationState::Verified => target.inspect_index(index)?,
            _ => unreachable!("validated post-data index state"),
        };
        if observed != PostgresIndexState::Exact {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "target index {} differs from reviewed semantics",
                index.name
            ));
        }
        if operation_state(journal, operation.id.as_str())? == OperationState::Prepared {
            journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
            interrupt_if(interruption, ExecutionInterruption::AfterIndexCommitted)?;
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        } else if operation_state(journal, operation.id.as_str())? == OperationState::Committed {
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        }
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
#[allow(clippy::too_many_arguments)] // Durable operation context and shared cancellation/fence checks.
fn process_postgres_foreign_keys(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
    target: &PostgresTargetFactory,
    journal: &mut AppendJournal,
    interruption: Option<ExecutionInterruption>,
    cancellation: &CancellationToken,
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
        cancellation.check()?;
        let foreign_key = foreign_key_for_operation(operation, &foreign_keys)?;
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Pending => {
                journal.transition_operation(operation.id.as_str(), OperationState::Running)?;
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
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "target rows violate reviewed foreign key {}",
                foreign_key.name
            ));
        }
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Running => {
                journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
            }
            OperationState::Committed => {
                journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
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
        cancellation.check()?;
        let foreign_key = foreign_key_for_operation(operation, &foreign_keys)?;
        match operation_state(journal, operation.id.as_str())? {
            OperationState::Pending | OperationState::Running => {
                journal_prepare_effect(journal, operation.id.as_str())?;
                interrupt_if(interruption, ExecutionInterruption::AfterForeignKeyPrepared)?;
            }
            OperationState::Prepared | OperationState::Committed | OperationState::Verified => {}
        }
        attest()?;
        let observed = match operation_state(journal, operation.id.as_str())? {
            OperationState::Prepared => match target.reconcile_foreign_key(foreign_key) {
                Ok(observed) => observed,
                Err(ConnectionError::InvalidRequest(reason)) => {
                    journal.require_manual_reconciliation()?;
                    return Err(anyhow!(
                        "foreign-key reconciliation requires manual intervention: {reason}"
                    ));
                }
                Err(error) => return Err(error.into()),
            },
            OperationState::Committed => target.inspect_foreign_key(foreign_key)?,
            OperationState::Verified => target.inspect_foreign_key(foreign_key)?,
            _ => unreachable!("validated foreign-key add state"),
        };
        if observed != PostgresForeignKeyState::ExactValidated {
            journal.require_manual_reconciliation()?;
            return Err(anyhow!(
                "target foreign key {} differs from reviewed semantics",
                foreign_key.name
            ));
        }
        if operation_state(journal, operation.id.as_str())? == OperationState::Prepared {
            journal.transition_operation(operation.id.as_str(), OperationState::Committed)?;
            interrupt_if(
                interruption,
                ExecutionInterruption::AfterForeignKeyCommitted,
            )?;
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
        } else if operation_state(journal, operation.id.as_str())? == OperationState::Committed {
            journal.transition_operation(operation.id.as_str(), OperationState::Verified)?;
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
    journal: &mut AppendJournal,
    table: &QualifiedTable,
    shape: &TableShape,
    expected: &RowBatch,
    cancellation: CancellationToken,
) -> anyhow::Result<PreparedResolution> {
    let chunk = journal
        .projection()
        .prepared_chunk
        .as_ref()
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
    let resolution = if row_count == 0 {
        PreparedResolution::RetryRequired
    } else if row_count == chunk.row_count && digest == chunk.canonical_digest {
        journal.commit_chunk_after_ack()?;
        PreparedResolution::MarkedCommitted
    } else {
        journal.require_manual_reconciliation()?;
        PreparedResolution::ManualReconciliationRequired
    };
    Ok(resolution)
}

#[cfg(all(test, feature = "enterprise-migration-spike"))]
fn reconcile_legacy_prepared_chunk(
    target: &dyn TargetConnectionFactory,
    state: &mut MigrationState,
    state_path: &Path,
    table: &QualifiedTable,
    shape: &TableShape,
    expected: &RowBatch,
    cancellation: CancellationToken,
) -> anyhow::Result<PreparedResolution> {
    use super::journal::PreparedChunkEvidence;

    let chunk = state
        .prepared_chunk()?
        .cloned()
        .ok_or_else(|| anyhow!("commit ambiguity has no durable prepared chunk"))?;
    let mut verifier = target.open_verifier(cancellation)?;
    let observed = verifier.select_page(&KeysetPage {
        table: table.clone(),
        projection: shape.projection.clone(),
        key: shape.key.clone(),
        after: chunk.start_key.clone().map(KeyTuple::new),
        limit: u32::try_from(
            chunk
                .row_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("row count overflow"))?,
        )?,
    })?;
    let (row_count, digest) = if observed.is_empty() {
        (0, String::new())
    } else {
        (
            u64::try_from(observed.len())?,
            batch_digest(table, shape, &observed)?,
        )
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
    let _ = expected;
    Ok(resolution)
}

#[cfg(all(test, feature = "enterprise-migration-spike"))]
fn complete_legacy_operation_if_needed(
    state: &mut MigrationState,
    operation_id: &str,
) -> anyhow::Result<()> {
    match operation_state(state, operation_id)? {
        OperationState::Pending => state.start_operation(operation_id)?,
        OperationState::Running | OperationState::Committed | OperationState::Verified => {}
        OperationState::Prepared => return Err(anyhow!("unexpected prepared state")),
    }
    if operation_state(state, operation_id)? == OperationState::Running {
        state.commit_operation(operation_id)?;
    }
    if operation_state(state, operation_id)? == OperationState::Committed {
        state.verify_operation(operation_id)?;
    }
    Ok(())
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_postgres_execution_operations(reviewed: &ReviewedPlan) -> anyhow::Result<()> {
    for operation in &reviewed.plan.operations {
        if !matches!(
            operation.kind,
            OperationKind::CreateTable
                | OperationKind::CreateSequence
                | OperationKind::CreateIndex
                | OperationKind::CreateView
                | OperationKind::CopyTable
                | OperationKind::CheckForeignKey
                | OperationKind::AddForeignKey
                | OperationKind::VerifyTable
                | OperationKind::VerifySchema
        ) && !matches!(
            &operation.kind,
            OperationKind::Vendor(name)
                if name == "restore_postgres_sequence"
                    || name == "create_postgres_partitioned_table"
                    || name == "create_postgres_partition"
                    || name == "create_postgres_sql_function"
                    || name == "verify_postgres_partition_topology"
        ) {
            return Err(anyhow!(
                "PostgreSQL live runner does not implement operation {:?}",
                operation.kind
            ));
        }
        if matches!(
            operation.kind,
            OperationKind::CreateTable
                | OperationKind::CreateIndex
                | OperationKind::CreateView
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
    let expected_programmable_objects = postgres_programmable_objects(source_catalog)?
        .into_iter()
        .map(|object| serde_json::to_string(&object))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    let operation_programmable_objects = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| {
            operation.kind == OperationKind::CreateView
                || matches!(&operation.kind, OperationKind::Vendor(name) if name == "create_postgres_sql_function")
        })
        .map(|operation| {
            let object: PostgresProgrammableObject = serde_json::from_value(
                operation
                    .parameters
                    .get("postgres_programmable_object")
                    .cloned()
                    .ok_or_else(|| anyhow!("programmable-object operation omits postgres_programmable_object"))?,
            )?;
            let expected_kind = match &object.ast {
                PostgresDurableAst::View(_) => OperationKind::CreateView,
                PostgresDurableAst::SqlFunction(_) => {
                    OperationKind::Vendor("create_postgres_sql_function".into())
                }
            };
            let expected_identity = QualifiedTable {
                namespace: object.namespace.clone(),
                name: object.name.clone(),
            };
            if operation.kind != expected_kind
                || operation.table.as_ref() != Some(&expected_identity)
            {
                return Err(anyhow!(
                    "programmable-object operation kind or identity differs from the reviewed catalog"
                ));
            }
            Ok(serde_json::to_string(&object)?)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let operation_programmable_object_set = operation_programmable_objects
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if operation_programmable_objects.len() != operation_programmable_object_set.len()
        || operation_programmable_object_set != expected_programmable_objects
    {
        return Err(anyhow!(
            "reviewed PostgreSQL programmable-object operation set differs from the catalog"
        ));
    }
    let expected_topologies = postgres_partition_topologies(source_catalog)?;
    for topology in &expected_topologies {
        let root_creates = reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| {
                matches!(&operation.kind, OperationKind::Vendor(name) if name == "create_postgres_partitioned_table")
                    && operation.table.as_ref() == Some(&topology.root)
                    && operation.parameters.get("postgres_partition_topology")
                        == Some(&serde_json::to_value(topology).expect("topology serialization cannot fail"))
            })
            .count();
        let leaf_creates = reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| {
                matches!(&operation.kind, OperationKind::Vendor(name) if name == "create_postgres_partition")
                    && topology.leaves.iter().any(|leaf| {
                        operation.table.as_ref() == Some(&leaf.table)
                            && operation.parameters.get("postgres_partition_leaf")
                                == Some(&serde_json::to_value(leaf).expect("leaf serialization cannot fail"))
                    })
            })
            .count();
        let verifies = reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| {
                matches!(&operation.kind, OperationKind::Vendor(name) if name == "verify_postgres_partition_topology")
                    && operation.table.as_ref() == Some(&topology.root)
                    && operation.parameters.get("postgres_partition_topology")
                        == Some(&serde_json::to_value(topology).expect("topology serialization cannot fail"))
            })
            .count();
        if root_creates != 1 || leaf_creates != topology.leaves.len() || verifies != 1 {
            return Err(anyhow!(
                "partition topology operation set differs from the reviewed catalog"
            ));
        }
        if topology
            .leaves
            .iter()
            .any(|leaf| copy_tables.contains(&leaf.table))
            || !copy_tables.contains(&topology.root)
        {
            return Err(anyhow!(
                "partition topology must copy exactly once through its root"
            ));
        }
    }
    let sequences = postgres_sequences(source_catalog)?;
    let expected_sequences = sequences
        .iter()
        .map(|sequence| sequence.catalog_object_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if reviewed.plan.consistency_mode != PostgresConsistencyMode::WriteFence.as_str()
        && sequences.iter().any(|sequence| sequence.cache_size != 1)
    {
        return Err(anyhow!(
            "PostgreSQL snapshot sequence equality requires CACHE 1"
        ));
    }
    let sequence_operation_ids =
        |restore: bool| -> anyhow::Result<std::collections::BTreeSet<String>> {
            reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| {
                if restore {
                    matches!(&operation.kind, OperationKind::Vendor(name) if name == "restore_postgres_sequence")
                } else {
                    operation.kind == OperationKind::CreateSequence
                }
            })
            .map(|operation| {
                serde_json::from_value::<PostgresSequence>(
                    operation
                        .parameters
                        .get("postgres_sequence")
                        .cloned()
                        .ok_or_else(|| anyhow!("sequence operation omits postgres_sequence"))?,
                )
                .map(|sequence| sequence.catalog_object_id)
                .map_err(Into::into)
            })
            .collect()
        };
    if sequence_operation_ids(false)? != expected_sequences
        || sequence_operation_ids(true)? != expected_sequences
    {
        return Err(anyhow!(
            "reviewed PostgreSQL sequence operation sets differ from the catalog"
        ));
    }
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
    writable_indexes: Vec<usize>,
    write_policy: PostgresWritePolicy,
}

#[cfg(feature = "enterprise-migration-spike")]
fn validate_copy_table_shapes(
    reviewed: &ReviewedPlan,
    catalog: &VendorCatalog,
) -> anyhow::Result<()> {
    if catalog.format_version != CATALOG_FORMAT_VERSION {
        return Err(anyhow!(
            "unsupported PostgreSQL catalog format version {}",
            catalog.format_version
        ));
    }
    postgres_generated_columns(catalog)
        .context("PostgreSQL generated-column contract is invalid")?;
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
        table_shape(catalog, table, Some(operation))
            .with_context(|| format!("table {table:?} has no safe resumable key"))?;
    }
    Ok(())
}

fn table_shape(
    catalog: &VendorCatalog,
    table: &QualifiedTable,
    operation: Option<&PlanOperation>,
) -> anyhow::Result<TableShape> {
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
    let selected = select_resumable_key(catalog, table)
        .map_err(|error| anyhow!("cannot select resumable key: {error}"))?;
    let write_policy = postgres_write_policy(catalog, table)
        .map_err(|error| anyhow!("cannot select PostgreSQL write policy: {error}"))?;
    if let Some(operation) = operation {
        let persisted: PostgresResumableKey = serde_json::from_value(
            operation
                .parameters
                .get("resumable_key")
                .cloned()
                .ok_or_else(|| anyhow!("copy operation has no persisted resumable key"))?,
        )
        .context("copy operation has an invalid persisted resumable key")?;
        if persisted != selected {
            return Err(anyhow!(
                "copy operation resumable key differs from deterministic catalog selection"
            ));
        }
        let persisted_policy: PostgresWritePolicy = serde_json::from_value(
            operation
                .parameters
                .get("postgres_write_policy")
                .cloned()
                .ok_or_else(|| {
                    anyhow!("copy operation has no persisted PostgreSQL write policy")
                })?,
        )
        .context("copy operation has an invalid PostgreSQL write policy")?;
        if persisted_policy != write_policy {
            return Err(anyhow!(
                "copy operation write policy differs from the current catalog"
            ));
        }
    }
    let key_indexes = selected
        .columns
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| anyhow!("selected resumable-key column is absent"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if key_indexes.iter().any(|index| {
        columns[*index]
            .attributes
            .get("generated")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|mode| !mode.is_empty())
    }) {
        return Err(anyhow!(
            "a generated column cannot be the selected resumable key"
        ));
    }
    let writable_indexes = columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let generated = column
                .attributes
                .get("generated")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            generated.is_empty().then_some(index)
        })
        .collect::<Vec<_>>();
    if writable_indexes.is_empty() {
        return Err(anyhow!("table has no writable columns"));
    }
    Ok(TableShape {
        projection,
        key: selected.columns,
        key_indexes,
        writable_indexes,
        write_policy,
    })
}

fn writable_batch(batch: &RowBatch, shape: &TableShape) -> anyhow::Result<RowBatch> {
    let columns = shape
        .writable_indexes
        .iter()
        .map(|index| {
            batch
                .columns()
                .get(*index)
                .cloned()
                .ok_or_else(|| anyhow!("writable column index exceeds batch width"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut writable = RowBatch::new(columns, batch.len(), usize::MAX);
    for row in batch.rows() {
        let values = shape
            .writable_indexes
            .iter()
            .map(|index| {
                row.get(*index)
                    .cloned()
                    .ok_or_else(|| anyhow!("writable column index exceeds row width"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        writable.try_push(values, 0)?;
    }
    Ok(writable)
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
    Ok(hex::encode(digest_rows(canonical.iter())?))
}

#[cfg(feature = "enterprise-migration-spike")]
struct TableVerificationAccumulator {
    after: Option<KeyTuple>,
    manifest: Sha256,
    source: Sha256,
    target: Sha256,
}

#[cfg(feature = "enterprise-migration-spike")]
struct ChunkVerificationContext<'a> {
    operation_id: &'a str,
    table: &'a QualifiedTable,
    shape: &'a TableShape,
    page_limit: u32,
}

#[cfg(feature = "enterprise-migration-spike")]
impl TableVerificationAccumulator {
    fn new() -> Self {
        Self {
            after: None,
            manifest: Sha256::new(),
            source: Sha256::new(),
            target: Sha256::new(),
        }
    }

    fn verify_chunk(
        &mut self,
        reader: &mut dyn ReadSession,
        verifier: &mut dyn super::connection::VerificationSession,
        chunk: &PreparedChunk,
        context: &ChunkVerificationContext<'_>,
    ) -> anyhow::Result<()> {
        if chunk.operation_id != context.operation_id {
            return Err(anyhow!("committed chunk belongs to a different operation"));
        }
        if chunk.start_key != self.after.as_ref().map(|key| key.0.clone()) {
            return Err(anyhow!("chunk manifest has a keyspace gap"));
        }
        let limit = u32::try_from(chunk.row_count).context("chunk row count exceeds u32")?;
        if limit == 0 || limit > context.page_limit {
            return Err(anyhow!("chunk row count exceeds the reviewed page limit"));
        }
        let request = KeysetPage {
            table: context.table.clone(),
            projection: context.shape.projection.clone(),
            key: context.shape.key.clone(),
            after: self.after.clone(),
            limit,
        };
        let expected = reader.select_page(&request)?;
        let actual = verifier.select_page(&request)?;
        if expected.len() as u64 != chunk.row_count || actual.len() as u64 != chunk.row_count {
            return Err(anyhow!("chunk row count differs during verification"));
        }
        let expected_final = batch_final_key(&expected, context.shape)?;
        let actual_final = batch_final_key(&actual, context.shape)?;
        if expected_final.0 != chunk.final_key || actual_final.0 != chunk.final_key {
            return Err(anyhow!("chunk final key differs during verification"));
        }
        let expected_digest = batch_digest(context.table, context.shape, &expected)?;
        let actual_digest = batch_digest(context.table, context.shape, &actual)?;
        if expected_digest != chunk.canonical_digest
            || actual_digest != chunk.canonical_digest
            || expected.rows() != actual.rows()
        {
            return Err(anyhow!("canonical chunk verification failed"));
        }
        let encoded_chunk = serde_json::to_vec(chunk)?;
        self.manifest
            .update(u64::try_from(encoded_chunk.len())?.to_be_bytes());
        self.manifest.update(encoded_chunk);
        self.source.update(expected_digest.as_bytes());
        self.target.update(actual_digest.as_bytes());
        self.after = Some(expected_final);
        Ok(())
    }

    fn verify_tail(
        &mut self,
        reader: &mut dyn ReadSession,
        verifier: &mut dyn super::connection::VerificationSession,
        table: &QualifiedTable,
        shape: &TableShape,
    ) -> anyhow::Result<()> {
        let request = KeysetPage {
            table: table.clone(),
            projection: shape.projection.clone(),
            key: shape.key.clone(),
            after: self.after.clone(),
            limit: 1,
        };
        if !reader.select_page(&request)?.is_empty() || !verifier.select_page(&request)?.is_empty()
        {
            return Err(anyhow!(
                "source or target contains rows outside committed intervals"
            ));
        }
        Ok(())
    }

    fn finish(self) -> (String, String, String) {
        (
            hex::encode(self.manifest.finalize()),
            hex::encode(self.source.finalize()),
            hex::encode(self.target.finalize()),
        )
    }
}

fn verify_live_table(
    reader: &mut dyn ReadSession,
    verifier: &mut dyn super::connection::VerificationSession,
    chunks: &mut std::iter::Peekable<CommittedChunkIter>,
    operation_id: &str,
    table: &QualifiedTable,
    shape: &TableShape,
    page_limit: u32,
) -> anyhow::Result<(String, String, String)> {
    let mut accumulator = TableVerificationAccumulator::new();
    while let Some(chunk) = chunks.peek() {
        let chunk = chunk.as_ref().map_err(|error| anyhow!(error.to_string()))?;
        if chunk.operation_id != operation_id {
            break;
        }
        let chunk = chunks
            .next()
            .ok_or_else(|| anyhow!("committed chunk stream ended unexpectedly"))??;
        accumulator.verify_chunk(
            reader,
            verifier,
            &chunk,
            &ChunkVerificationContext {
                operation_id,
                table,
                shape,
                page_limit,
            },
        )?;
    }
    accumulator.verify_tail(reader, verifier, table, shape)?;
    Ok(accumulator.finish())
}

#[cfg(feature = "enterprise-migration-spike")]
fn verify_schema_projection(
    source: &VendorCatalog,
    target_factory: &PostgresTargetFactory,
) -> anyhow::Result<()> {
    let target = target_factory.inspect_endpoint()?;
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
    target_factory: &PostgresTargetFactory,
) -> anyhow::Result<bool> {
    let target = target_factory.inspect_endpoint()?;
    let source_projection = schema_projection(source, false)?;
    let target_projection = schema_projection(&target.catalog, false)?;
    Ok(source_projection == target_projection)
}

fn schema_projection(
    catalog: &VendorCatalog,
    include_post_data_objects: bool,
) -> anyhow::Result<serde_json::Value> {
    let table_names = catalog
        .namespaces
        .iter()
        .flat_map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| {
                    matches!(
                        object.kind,
                        CatalogObjectKind::Table | CatalogObjectKind::Partition
                    )
                })
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
                        | CatalogObjectKind::Partition
                        | CatalogObjectKind::Sequence
                        | CatalogObjectKind::Column
                        | CatalogObjectKind::PrimaryKey
                        | CatalogObjectKind::UniqueConstraint
                        | CatalogObjectKind::CheckConstraint
                ) || (object.kind == CatalogObjectKind::Index
                    && object
                        .attributes
                        .get("constraint_oid")
                        .is_none_or(serde_json::Value::is_null)
                    && (include_post_data_objects
                        || object
                            .attributes
                            .get("unique")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)))
                    || (include_post_data_objects && object.kind == CatalogObjectKind::ForeignKey)
                    || (include_post_data_objects
                        && matches!(
                            object.kind,
                            CatalogObjectKind::View | CatalogObjectKind::Routine
                        ))
            })
            .map(|object| {
                let definition = String::from_utf8(object.definition.clone())
                    .context("catalog definition is not UTF-8")?;
                let attributes = match object.kind {
                    CatalogObjectKind::Table => serde_json::json!({
                        "relkind": object.attributes.get("relkind"),
                        "persistence": object.attributes.get("persistence"),
                        "partition_strategy": object.attributes.get("partition_strategy"),
                        "partition_key_column": object.attributes.get("partition_key_column"),
                        "partition_key_type": object.attributes.get("partition_key_type"),
                        "default_partition": object.attributes
                            .get("default_partition_oid")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|oid| table_names.get(oid)),
                    }),
                    CatalogObjectKind::Partition => serde_json::json!({
                        "relkind": object.attributes.get("relkind"),
                        "persistence": object.attributes.get("persistence"),
                        "parent": object.attributes
                            .get("partition_parent_oid")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|oid| table_names.get(oid)),
                        "bound": object.attributes.get("partition_bound"),
                    }),
                    CatalogObjectKind::Sequence => serde_json::json!({
                        "persistence": object.attributes.get("persistence"),
                        "type": object.attributes.get("type"),
                        "start": object.attributes.get("start"),
                        "increment": object.attributes.get("increment"),
                        "minimum": object.attributes.get("minimum"),
                        "maximum": object.attributes.get("maximum"),
                        "cache": object.attributes.get("cache"),
                        "cycle": object.attributes.get("cycle"),
                        "ownership": object.attributes.get("ownership"),
                        "ownership_count": object.attributes.get("ownership_count"),
                    }),
                    CatalogObjectKind::Column => serde_json::json!({
                        "table": object.attributes.get("table"),
                        "ordinal": object.attributes.get("ordinal"),
                        "nullable": object.attributes.get("nullable"),
                        "default": object.attributes.get("default"),
                        "identity": object.attributes.get("identity"),
                        "generated": object.attributes.get("generated"),
                        "generated_expression": object.attributes.get("generated_expression"),
                        "generated_dependencies": object.attributes.get("generated_dependencies"),
                        "collation": object.attributes.get("collation"),
                        "collation_schema": object.attributes.get("collation_schema"),
                        "collation_provider": object.attributes.get("collation_provider"),
                        "collation_deterministic": object.attributes.get("collation_deterministic"),
                        "collation_version": object.attributes.get("collation_version"),
                        "collation_actual_version": object.attributes.get("collation_actual_version"),
                        "type_schema": object.attributes.get("type_schema"),
                        "type_name": object.attributes.get("type_name"),
                    }),
                    CatalogObjectKind::View | CatalogObjectKind::Routine => serde_json::json!({
                        "postgres_durable_ast": object.attributes.get("postgres_durable_ast"),
                        "postgres_authoritative_identity": object.attributes.get("postgres_authoritative_identity"),
                        "postgres_authoritative_dependencies": object.attributes.get("postgres_authoritative_dependencies"),
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
                    CatalogObjectKind::Index => serde_json::json!({
                        "table": object.attributes
                            .get("table_oid")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|oid| table_names.get(oid)),
                        "unique": object.attributes.get("unique"),
                        "primary": object.attributes.get("primary"),
                        "valid": object.attributes.get("valid"),
                        "ready": object.attributes.get("ready"),
                        "live": object.attributes.get("live"),
                        "immediate": object.attributes.get("immediate"),
                        "clustered": object.attributes.get("clustered"),
                        "replica_identity": object.attributes.get("replica_identity"),
                        "exclusion": object.attributes.get("exclusion"),
                        "nulls_not_distinct": object.attributes.get("nulls_not_distinct"),
                        "access_method": object.attributes.get("access_method"),
                        "persistence": object.attributes.get("persistence"),
                        "reloptions": object.attributes.get("reloptions"),
                        "tablespace": object.attributes.get("tablespace"),
                        "predicate": object.attributes.get("predicate"),
                        "has_expressions": object.attributes.get("has_expressions"),
                        "columns": object.attributes.get("columns"),
                        "included_columns": object.attributes.get("included_columns"),
                        "options": object.attributes.get("options"),
                        "opclasses": object.attributes.get("opclasses"),
                        "collations": object.attributes.get("collations"),
                        "collations_default": object.attributes.get("collations_default"),
                        "constraint_oid": object.attributes.get("constraint_oid"),
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
    let source_catalog = VendorCatalog {
        format_version: 1,
        dialect: "postgresql".into(),
        server_version: "1".into(),
        database: Identifier::new("source")?,
        namespaces: Vec::new(),
        dependencies: Vec::new(),
        vendor_metadata: BTreeMap::new(),
    };
    let target_catalog = VendorCatalog {
        database: Identifier::new("target")?,
        ..source_catalog.clone()
    };
    let source_catalog_fingerprint =
        hex::encode(Sha256::digest(serde_json::to_vec(&source_catalog)?));
    let target_catalog_fingerprint =
        hex::encode(Sha256::digest(serde_json::to_vec(&target_catalog)?));
    let reviewed = ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Execution,
        migration_id: "fixture-phase-1-5".into(),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: "fixture-source".into(),
        target_endpoint_identity: AssessmentStatus::Assessed("fixture-target".into()),
        source_catalog_fingerprint,
        target_catalog_fingerprint: AssessmentStatus::Assessed(target_catalog_fingerprint),
        source_catalog: Some(source_catalog),
        target_catalog: AssessmentStatus::Assessed(target_catalog),
        source_tls_binding: "fixture-source-tls".into(),
        target_tls_binding: AssessmentStatus::Assessed("fixture-target-tls".into()),
        consistency_mode: PostgresConsistencyMode::ConsistentSnapshot.as_str().into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: None,
        mysql_snapshot_evidence: None,
        mysql_target_snapshot_evidence: None,
        mysql_metadata_visibility: None,
        mysql_target_metadata_visibility: None,
        mysql_authorization: None,
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
        target_endpoint: reviewed
            .plan
            .execution_target_endpoint_identity()?
            .to_owned(),
        consistency_evidence: ConsistencyEvidence::NativeSnapshot {
            endpoint_identity: snapshot.endpoint_identity.clone(),
            database_identity: snapshot.database_identity.clone(),
            lifecycle_id: snapshot.lifecycle_id.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            server_version: snapshot.server_version.clone(),
        },
        source_schema_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        target_schema_fingerprint: reviewed
            .plan
            .execution_target_catalog_fingerprint()?
            .to_owned(),
        outage_projection_digest: None,
        external_quiesce_attestation_digest: None,
        mysql_freeze_attestation_digest: None,
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
        let digest = hex::encode(digest_rows(canonical_rows.iter())?);
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
                })?,
                value: encode_row(&canonical)?,
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
        if hex::encode(digest_rows(canonical.iter())?) != chunk.canonical_digest {
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

    #[cfg(feature = "migration-fault-injection")]
    fn pipeline_fixture() -> anyhow::Result<(
        InMemorySource,
        InMemoryTarget,
        SnapshotToken,
        QualifiedTable,
        TableShape,
        Vec<PreparedChunk>,
    )> {
        let table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        };
        let columns = vec![column("id", 0, "bigint")?, column("name", 1, "text")?];
        let rows = vec![
            vec![DbValue::Signed(1), DbValue::Text("Ada".into())],
            vec![DbValue::Signed(2), DbValue::Text("Grace".into())],
        ];
        let source = InMemorySource::new("source", "database");
        source.add_table(table.clone(), columns.clone(), rows.clone());
        let snapshot = source.capture_snapshot()?;
        let target = InMemoryTarget::default();
        target.add_empty_table(table.clone(), columns.clone());
        let mut batch = RowBatch::new(columns, rows.len(), 1_024);
        for row in &rows {
            batch.try_push(row.clone(), 32)?;
        }
        let mut writer = target.open_writer(CancellationToken::default())?;
        writer.begin()?;
        writer.insert(&table, &batch)?;
        writer.commit()?;
        let shape = TableShape {
            projection: vec![Identifier::new("id")?, Identifier::new("name")?],
            key: vec![Identifier::new("id")?],
            key_indexes: vec![0],
            writable_indexes: vec![0, 1],
            write_policy: PostgresWritePolicy::BinaryCopyWithInsertFallbackV1,
        };
        let chunks = rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                let mut one = RowBatch::new(batch.columns().to_vec(), 1, 1_024);
                one.try_push(row.clone(), 32)?;
                Ok(PreparedChunk {
                    chunk_id: u64::try_from(index + 1)?,
                    operation_id: "copy-accounts".into(),
                    start_key: (index > 0).then(|| vec![rows[index - 1][0].clone()]),
                    final_key: vec![row[0].clone()],
                    row_count: 1,
                    canonical_digest: batch_digest(&table, &shape, &one)?,
                    target_transaction_intent: format!("intent-{index}"),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((source, target, snapshot, table, shape, chunks))
    }

    #[cfg(feature = "migration-fault-injection")]
    #[test]
    fn pipeline_import_failure_selects_fallback_before_effects() -> anyhow::Result<()> {
        let target: Arc<dyn TargetConnectionFactory> = Arc::new(InMemoryTarget::default());
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pipeline = VerificationPipeline::start_with_import(
            target,
            CancellationToken::default(),
            || Err(ConnectionError::SnapshotMismatch),
            VerificationPipelineHooks::default(),
        )?;
        if pipeline.is_some() {
            effects.fetch_add(1, Ordering::Release);
        }
        assert!(pipeline.is_none());
        assert_eq!(effects.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(feature = "migration-fault-injection")]
    #[test]
    fn capacity_one_pipeline_overlaps_copy_and_applies_backpressure() -> anyhow::Result<()> {
        let (source, target, snapshot, table, shape, chunks) = pipeline_fixture()?;
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let hooks = VerificationPipelineHooks {
            before_chunk_verification: Some(Arc::new(move |chunk_id| {
                if chunk_id == 1 {
                    entered_tx
                        .send(())
                        .map_err(|_| anyhow!("barrier receiver dropped"))?;
                    release_rx
                        .lock()
                        .map_err(|_| anyhow!("barrier lock poisoned"))?
                        .recv()
                        .map_err(|_| anyhow!("barrier sender dropped"))?;
                }
                Ok(())
            })),
        };
        let imported_source = source.clone();
        let imported_snapshot = snapshot.clone();
        let target: Arc<dyn TargetConnectionFactory> = Arc::new(target);
        let pipeline = VerificationPipeline::start_with_import(
            target,
            CancellationToken::default(),
            move || imported_source.open_reader(&imported_snapshot, CancellationToken::default()),
            hooks,
        )?
        .ok_or_else(|| anyhow!("fixture pipeline unexpectedly selected fallback"))?;
        pipeline.send(VerificationPipelineCommand::StartTable {
            operation_id: "copy-accounts".into(),
            table,
            shape,
            page_limit: 1,
        })?;
        pipeline.send(VerificationPipelineCommand::Chunk(chunks[0].clone()))?;
        entered_rx.recv()?;
        pipeline.send(VerificationPipelineCommand::Chunk(chunks[1].clone()))?;
        let finish_sender = pipeline.sender.as_ref().unwrap().clone();
        let (attempted_tx, attempted_rx) = mpsc::sync_channel(0);
        let (sent_tx, sent_rx) = mpsc::sync_channel(0);
        let blocked = thread::spawn(move || {
            let _ = attempted_tx.send(());
            let result = finish_sender.send(VerificationPipelineCommand::FinishTable);
            let _ = sent_tx.send(result);
        });
        attempted_rx.recv()?;
        assert!(matches!(sent_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        release_tx.send(())?;
        sent_rx.recv()??;
        blocked.join().map_err(|_| anyhow!("producer panicked"))?;
        let evidence = pipeline.finish()?;
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].chunk_count, 2);
        assert_eq!(evidence[0].row_count, 2);
        Ok(())
    }

    #[cfg(feature = "migration-fault-injection")]
    #[test]
    fn interruption_after_pipeline_evidence_publishes_nothing_and_allows_sequential_retry(
    ) -> anyhow::Result<()> {
        let (source, target, snapshot, table, shape, chunks) = pipeline_fixture()?;
        let imported_source = source.clone();
        let imported_snapshot = snapshot.clone();
        let target_factory: Arc<dyn TargetConnectionFactory> = Arc::new(target.clone());
        let pipeline = VerificationPipeline::start_with_import(
            Arc::clone(&target_factory),
            CancellationToken::default(),
            move || imported_source.open_reader(&imported_snapshot, CancellationToken::default()),
            VerificationPipelineHooks::default(),
        )?
        .ok_or_else(|| anyhow!("fixture pipeline unexpectedly selected fallback"))?;
        pipeline.send(VerificationPipelineCommand::StartTable {
            operation_id: "copy-accounts".into(),
            table: table.clone(),
            shape: shape.clone(),
            page_limit: 1,
        })?;
        for chunk in &chunks {
            pipeline.send(VerificationPipelineCommand::Chunk(chunk.clone()))?;
        }
        pipeline.send(VerificationPipelineCommand::FinishTable)?;
        let pipelined = pipeline.finish()?;
        assert_eq!(pipelined.len(), 1);

        let durable_verification_events = std::sync::atomic::AtomicUsize::new(0);
        let interrupted = (|| -> anyhow::Result<()> {
            interrupt_after_pipelined_evidence(Some(
                ExecutionInterruption::AfterPipelinedEvidence,
            ))?;
            durable_verification_events.fetch_add(1, Ordering::Release);
            Ok(())
        })();
        assert!(interrupted.is_err());
        assert_eq!(durable_verification_events.load(Ordering::Acquire), 0);

        let mut reader = source.open_reader(&snapshot, CancellationToken::default())?;
        let mut verifier = target_factory.open_verifier(CancellationToken::default())?;
        let mut sequential = TableVerificationAccumulator::new();
        for chunk in &chunks {
            sequential.verify_chunk(
                reader.as_mut(),
                verifier.as_mut(),
                chunk,
                &ChunkVerificationContext {
                    operation_id: "copy-accounts",
                    table: &table,
                    shape: &shape,
                    page_limit: 1,
                },
            )?;
        }
        sequential.verify_tail(reader.as_mut(), verifier.as_mut(), &table, &shape)?;
        let (manifest_hash, source_hash, target_hash) = sequential.finish();
        assert_eq!(manifest_hash, pipelined[0].manifest_hash);
        assert_eq!(source_hash, pipelined[0].source_hash);
        assert_eq!(target_hash, pipelined[0].target_hash);
        Ok(())
    }

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
            writable_indexes: vec![0, 1],
            write_policy: PostgresWritePolicy::BinaryCopyWithInsertFallbackV1,
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
            outage_projection_digest: None,
            external_quiesce_attestation_digest: None,
            mysql_freeze_attestation_digest: None,
            conversion_policy: POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
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
        let resolution = reconcile_legacy_prepared_chunk(
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
    fn empty_test_catalog(database: &str) -> VendorCatalog {
        VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new(database).unwrap(),
            namespaces: Vec::new(),
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
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
    fn reviewed_tls_policy_rejects_downgrade_and_authentication_changes() {
        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("certificate.pem");
        let private_key = directory.path().join("private-key.pem");
        std::fs::write(&certificate, b"certificate-a").unwrap();
        std::fs::write(&private_key, b"private-key").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&private_key, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut config = endpoint("SOURCE");
        config.tls.client_certificate = Some(certificate.to_string_lossy().into_owned());
        config.tls.client_private_key = Some(private_key.to_string_lossy().into_owned());
        let binding = postgres_tls_binding(&config).unwrap();
        let source_catalog = empty_test_catalog("source");
        let target_catalog = empty_test_catalog("target");
        let reviewed = ReviewedPlan::new(MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            purpose: PlanPurpose::Execution,
            migration_id: "tls-policy".into(),
            tool_version: env!("CARGO_PKG_VERSION").into(),
            source_endpoint_identity: "source".into(),
            target_endpoint_identity: AssessmentStatus::Assessed("target".into()),
            source_catalog_fingerprint: catalog_fingerprint(&source_catalog).unwrap(),
            target_catalog_fingerprint: AssessmentStatus::Assessed(
                catalog_fingerprint(&target_catalog).unwrap(),
            ),
            source_catalog: Some(source_catalog),
            target_catalog: AssessmentStatus::Assessed(target_catalog),
            source_tls_binding: binding.clone(),
            target_tls_binding: AssessmentStatus::Assessed("target-tls".into()),
            consistency_mode: "write-fence".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
            conversion_policy: POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
            outage_policy: None,
            postgres_source_profile: None,
            mysql_source_profile: None,
            mysql_snapshot_evidence: None,
            mysql_target_snapshot_evidence: None,
            mysql_metadata_visibility: None,
            mysql_target_metadata_visibility: None,
            mysql_authorization: None,
            capabilities: BTreeMap::from([("source_tls".into(), binding)]),
            operations: Vec::new(),
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap();
        validate_reviewed_tls(&reviewed, "source_tls", &config).unwrap();
        let replacement = directory.path().join("replacement.pem");
        std::fs::write(&replacement, b"certificate-b").unwrap();
        config.tls.client_certificate = Some(replacement.to_string_lossy().into_owned());
        assert!(validate_reviewed_tls(&reviewed, "source_tls", &config).is_err());
        config.tls.client_certificate = None;
        config.tls.client_private_key = None;
        assert!(validate_reviewed_tls(&reviewed, "source_tls", &config).is_err());
        config.tls.insecure = true;
        assert!(validate_reviewed_tls(&reviewed, "source_tls", &config).is_err());
    }

    #[cfg(feature = "enterprise-migration-spike")]
    #[test]
    fn generated_values_remain_in_evidence_but_are_omitted_from_insert() -> anyhow::Result<()> {
        let columns = vec![
            column("id", 0, "bigint")?,
            column("generated_total", 1, "bigint")?,
            column("amount", 2, "bigint")?,
        ];
        let mut batch = RowBatch::new(columns, 1, 1_024);
        batch.try_push(
            vec![DbValue::Signed(1), DbValue::Signed(42), DbValue::Signed(21)],
            24,
        )?;
        let shape = TableShape {
            projection: vec![
                Identifier::new("id")?,
                Identifier::new("generated_total")?,
                Identifier::new("amount")?,
            ],
            key: vec![Identifier::new("id")?],
            key_indexes: vec![0],
            writable_indexes: vec![0, 2],
            write_policy: PostgresWritePolicy::BinaryCopyWithInsertFallbackV1,
        };
        let writable = writable_batch(&batch, &shape)?;
        assert_eq!(
            writable
                .columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "amount"]
        );
        assert_eq!(
            writable.rows(),
            &[vec![DbValue::Signed(1), DbValue::Signed(21)]]
        );
        assert_eq!(batch.rows()[0][1], DbValue::Signed(42));
        Ok(())
    }

    #[cfg(feature = "enterprise-migration-spike")]
    #[test]
    fn bulk_write_falls_back_to_one_transactional_insert_attempt() -> anyhow::Result<()> {
        let table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        };
        let columns = vec![column("id", 0, "bigint")?, column("name", 1, "text")?];
        let mut batch = RowBatch::new(columns.clone(), 1, 1_024);
        batch.try_push(vec![DbValue::Signed(1), DbValue::Text("Ada".into())], 16)?;
        let target = InMemoryTarget::default();
        target.add_empty_table(table.clone(), columns);

        let cancellation = CancellationToken::default();
        let mut writer = open_postgres_batch_writer(
            &target,
            &table,
            &batch,
            PostgresWritePolicy::BinaryCopyWithInsertFallbackV1,
            &cancellation,
        )?;
        writer.commit()?;

        assert_eq!(target.rows(&table), batch.rows());
        Ok(())
    }

    #[cfg(feature = "enterprise-migration-spike")]
    #[test]
    fn failed_bulk_and_diagnostic_insert_leave_no_target_rows() -> anyhow::Result<()> {
        let table = QualifiedTable {
            namespace: Identifier::new("public")?,
            name: Identifier::new("accounts")?,
        };
        let columns = vec![column("id", 0, "bigint")?];
        let mut batch = RowBatch::new(columns.clone(), 1, 1_024);
        batch.try_push(vec![DbValue::Signed(1)], 8)?;
        let target = InMemoryTarget::default();
        target.add_empty_table(table.clone(), columns);
        target.fail_once(FailurePoint::Insert);

        let error = match open_postgres_batch_writer(
            &target,
            &table,
            &batch,
            PostgresWritePolicy::BinaryCopyWithInsertFallbackV1,
            &CancellationToken::default(),
        ) {
            Ok(_) => {
                return Err(anyhow!(
                    "bulk write and diagnostic INSERT unexpectedly succeeded"
                ))
            }
            Err(error) => error,
        };

        assert!(error.to_string().contains("diagnostic INSERT failed"));
        assert!(target.rows(&table).is_empty());
        Ok(())
    }

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
            outage_projection_digest: None,
            external_quiesce_attestation_digest: None,
            mysql_freeze_attestation_digest: None,
            conversion_policy: POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        };
        let mut state =
            MigrationState::with_operations(binding, [("verify".to_owned(), Vec::<String>::new())])
                .unwrap();
        complete_legacy_operation_if_needed(&mut state, "verify").unwrap();
        complete_legacy_operation_if_needed(&mut state, "verify").unwrap();
        assert_eq!(
            operation_state(&state, "verify").unwrap(),
            OperationState::Verified
        );
    }
}
