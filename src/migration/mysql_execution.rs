//! Durable MySQL target-effect reconciliation.
//!
//! MySQL DDL implicitly commits. Each create-only statement therefore owns one
//! journal intent and is inspected independently after any error or restart.

use std::collections::{BTreeMap, BTreeSet};
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
    CancellationToken, ConnectionError, ConnectionResult, KeysetPage, ReadSession,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession,
};
use super::journal::{ConsistencyEvidence, MigrationStatus, OperationState, ResumeBinding};
use super::model::{DbValue, Identifier, KeyTuple, QualifiedTable, RowBatch, VendorCatalog};
use super::mysql::{
    attest_mysql_external_freeze, collect_mysql_metadata_visibility, mysql_auto_increment_states,
    mysql_catalog_fingerprint, mysql_table_definitions, validate_mysql_external_freeze_continuity,
    MySqlAutoIncrementState, MySqlAutoIncrementTargetState, MySqlCatalogSnapshot,
    MySqlEndpointConfig, MySqlResumableKey, MySqlSourceFactory, MySqlTableDefinition,
    MySqlTableMapping, MySqlTableState, MySqlTargetFactory,
};
use super::mysql_profile::{MySqlExternalFreezeAssertion, MySqlExternalFreezeAttestation};
use super::plan::{MySqlSnapshotEvidence, OperationKind, PlanOperation, ReviewedPlan};

trait MySqlDdlTarget {
    fn inspect(&self, expected: &MySqlTableDefinition) -> ConnectionResult<MySqlTableState>;
    fn create(&self, expected: &MySqlTableDefinition) -> ConnectionResult<()>;
}

impl MySqlDdlTarget for MySqlTargetFactory {
    fn inspect(&self, expected: &MySqlTableDefinition) -> ConnectionResult<MySqlTableState> {
        self.inspect_table(expected)
    }

    fn create(&self, expected: &MySqlTableDefinition) -> ConnectionResult<()> {
        self.create_table(expected)
    }
}

trait MySqlAutoIncrementTarget {
    fn inspect_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<MySqlAutoIncrementTargetState>;

    fn restore_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<()>;
}

impl MySqlAutoIncrementTarget for MySqlTargetFactory {
    fn inspect_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<MySqlAutoIncrementTargetState> {
        self.inspect_auto_increment(expected, mapping)
    }

    fn restore_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<()> {
        self.restore_auto_increment(expected, mapping)
    }
}

trait MySqlVerificationTarget {
    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>>;
}

impl MySqlVerificationTarget for MySqlTargetFactory {
    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>> {
        TargetConnectionFactory::open_verifier(self, cancellation)
    }
}

/// Reconcile every typed create-table statement against its durable journal
/// state. This function never drops, replaces, or alters an observed object.
pub fn reconcile_mysql_pre_data_schema(
    reviewed: &ReviewedPlan,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    reconcile_mysql_pre_data_schema_with(
        reviewed,
        target,
        journal,
        cancellation,
        &mut || Ok(()),
        None,
    )
}

fn reconcile_mysql_pre_data_schema_with<G>(
    reviewed: &ReviewedPlan,
    target: &impl MySqlDdlTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    reviewed
        .validate()
        .context("validate reviewed MySQL plan")?;
    let mut target_tables = BTreeSet::new();
    for operation in reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CreateTable)
    {
        cancellation.check()?;
        let expected = mysql_create_table_contract(operation)?;
        if !target_tables.insert(expected.table.clone()) {
            return Err(anyhow!(
                "reviewed MySQL plan creates target table {} more than once",
                table_name(&expected)
            ));
        }
        reconcile_mysql_create_table(
            operation,
            &expected,
            target,
            journal,
            cancellation,
            before_effect,
            interruption,
        )?;
    }
    Ok(())
}

fn mysql_create_table_contract(operation: &PlanOperation) -> anyhow::Result<MySqlTableDefinition> {
    let source_table = operation
        .table
        .as_ref()
        .ok_or_else(|| anyhow!("MySQL create-table operation has no source table"))?;
    let expected: MySqlTableDefinition = serde_json::from_value(
        operation
            .parameters
            .get("mysql_table_definition")
            .cloned()
            .ok_or_else(|| anyhow!("MySQL create-table operation has no typed definition"))?,
    )
    .context("decode reviewed MySQL table definition")?;
    let mapping: MySqlTableMapping = serde_json::from_value(
        operation
            .parameters
            .get("mysql_table_mapping")
            .cloned()
            .ok_or_else(|| anyhow!("MySQL create-table operation has no table mapping"))?,
    )
    .context("decode reviewed MySQL table mapping")?;
    if &mapping.source != source_table || mapping.target != expected.table {
        return Err(anyhow!(
            "MySQL create-table definition and source-to-target mapping disagree"
        ));
    }
    Ok(expected)
}

fn reconcile_mysql_create_table<G>(
    operation: &PlanOperation,
    expected: &MySqlTableDefinition,
    target: &impl MySqlDdlTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    let operation_id = operation.id.as_str();
    let state = operation_state(journal, operation_id)?;
    let observed = target.inspect(expected)?;

    match state {
        OperationState::Pending | OperationState::Running => {
            if observed != MySqlTableState::Absent {
                return require_manual(
                    journal,
                    format!(
                        "MySQL target table {} exists without a durable prepared DDL intent",
                        table_name(expected)
                    ),
                );
            }
            if state == OperationState::Pending {
                journal.prepare_operations_atomic([operation_id])?;
            } else {
                journal.transition_operation(operation_id, OperationState::Prepared)?;
            }
            interrupt_mysql_if(interruption, MySqlInterruptionPoint::DdlPrepared)?;
            cancellation.check()?;
            before_effect()?;
            if let Err(create_error) = target.create(expected) {
                return match target.inspect(expected) {
                    Ok(MySqlTableState::Exact) => acknowledge_exact_mysql_ddl(journal, operation_id),
                    Ok(MySqlTableState::Absent) => Err(anyhow!(create_error).context(format!(
                        "MySQL CREATE TABLE {} did not take effect; durable intent remains prepared",
                        table_name(expected)
                    ))),
                    Ok(MySqlTableState::Different) => require_manual(
                        journal,
                        format!(
                            "MySQL CREATE TABLE {} produced a different target effect: {create_error}",
                            table_name(expected)
                        ),
                    ),
                    Err(inspect_error) => Err(anyhow!(create_error).context(format!(
                        "MySQL CREATE TABLE {} failed and its effect could not be inspected: {inspect_error}",
                        table_name(expected)
                    ))),
                };
            }
            interrupt_mysql_if(interruption, MySqlInterruptionPoint::DdlCommitted)?;
            cancellation.check()?;
            match target.inspect(expected)? {
                MySqlTableState::Exact => acknowledge_exact_mysql_ddl(journal, operation_id),
                MySqlTableState::Absent => Err(anyhow!(
                    "MySQL CREATE TABLE {} returned success without an observable effect",
                    table_name(expected)
                )),
                MySqlTableState::Different => require_manual(
                    journal,
                    format!(
                        "MySQL CREATE TABLE {} committed with different semantics",
                        table_name(expected)
                    ),
                ),
            }
        }
        OperationState::Prepared => match observed {
            MySqlTableState::Exact => acknowledge_exact_mysql_ddl(journal, operation_id),
            MySqlTableState::Absent => {
                interrupt_mysql_if(interruption, MySqlInterruptionPoint::DdlPrepared)?;
                cancellation.check()?;
                before_effect()?;
                if let Err(create_error) = target.create(expected) {
                    return match target.inspect(expected) {
                        Ok(MySqlTableState::Exact) => {
                            acknowledge_exact_mysql_ddl(journal, operation_id)
                        }
                        Ok(MySqlTableState::Absent) => Err(anyhow!(create_error).context(format!(
                            "retry MySQL CREATE TABLE {} from durable prepared intent",
                            table_name(expected)
                        ))),
                        Ok(MySqlTableState::Different) => require_manual(
                            journal,
                            format!(
                                "retried MySQL CREATE TABLE {} produced a different target effect: {create_error}",
                                table_name(expected)
                            ),
                        ),
                        Err(inspect_error) => Err(anyhow!(create_error).context(format!(
                            "retried MySQL CREATE TABLE {} failed and inspection also failed: {inspect_error}",
                            table_name(expected)
                        ))),
                    };
                }
                interrupt_mysql_if(interruption, MySqlInterruptionPoint::DdlCommitted)?;
                match target.inspect(expected)? {
                    MySqlTableState::Exact => acknowledge_exact_mysql_ddl(journal, operation_id),
                    MySqlTableState::Absent => Err(anyhow!(
                        "retried MySQL CREATE TABLE {} returned success without an observable effect",
                        table_name(expected)
                    )),
                    MySqlTableState::Different => require_manual(
                        journal,
                        format!(
                            "retried MySQL CREATE TABLE {} committed with different semantics",
                            table_name(expected)
                        ),
                    ),
                }
            }
            MySqlTableState::Different => require_manual(
                journal,
                format!(
                    "prepared MySQL CREATE TABLE {} has a different target effect",
                    table_name(expected)
                ),
            ),
        },
        OperationState::Committed => match observed {
            MySqlTableState::Exact => {
                journal.transition_operation(operation_id, OperationState::Verified)?;
                Ok(())
            }
            MySqlTableState::Absent | MySqlTableState::Different => require_manual(
                journal,
                format!(
                    "committed MySQL CREATE TABLE {} no longer has the exact reviewed effect",
                    table_name(expected)
                ),
            ),
        },
        OperationState::Verified => {
            if observed == MySqlTableState::Exact {
                Ok(())
            } else {
                require_manual(
                    journal,
                    format!(
                        "verified MySQL CREATE TABLE {} drifted on the target",
                        table_name(expected)
                    ),
                )
            }
        }
    }
}

fn operation_state(journal: &AppendJournal, operation_id: &str) -> anyhow::Result<OperationState> {
    journal
        .projection()
        .operations
        .get(operation_id)
        .copied()
        .ok_or_else(|| anyhow!("migration journal has no state for operation {operation_id}"))
}

fn table_name(expected: &MySqlTableDefinition) -> String {
    format!("{}.{}", expected.table.namespace, expected.table.name)
}

fn acknowledge_exact_mysql_ddl(
    journal: &mut AppendJournal,
    operation_id: &str,
) -> anyhow::Result<()> {
    if operation_state(journal, operation_id)? == OperationState::Prepared {
        journal.transition_operation(operation_id, OperationState::Committed)?;
    }
    if operation_state(journal, operation_id)? == OperationState::Committed {
        journal.transition_operation(operation_id, OperationState::Verified)?;
    }
    if operation_state(journal, operation_id)? != OperationState::Verified {
        return Err(anyhow!(
            "MySQL DDL operation {operation_id} did not reach its verified state"
        ));
    }
    Ok(())
}

fn require_manual<T>(journal: &mut AppendJournal, message: String) -> anyhow::Result<T> {
    journal.require_manual_reconciliation()?;
    Err(anyhow!("{message}; manual reconciliation is required"))
}

#[derive(Debug, Clone)]
struct MySqlCopyContract {
    operation_id: String,
    source: QualifiedTable,
    target: QualifiedTable,
    projection: Vec<Identifier>,
    key: Vec<Identifier>,
    key_indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
struct MySqlAutoIncrementContract {
    operation_id: String,
    state: MySqlAutoIncrementState,
    mapping: MySqlTableMapping,
}

#[derive(Debug, Clone, Copy)]
pub struct MySqlExecutionAdminConfigs<'a> {
    pub freeze: &'a MySqlEndpointConfig,
    pub source_metadata: &'a MySqlEndpointConfig,
    pub target_metadata: &'a MySqlEndpointConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlExecutionReport {
    pub state: PathBuf,
    pub copied_rows: u64,
    pub committed_chunks: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Constructed only by the opt-in fault-injection feature.
enum MySqlInterruptionPoint {
    DdlPrepared,
    DdlCommitted,
    ChunkPrepared,
    ChunkCommitBeforeJournal,
    CommittedChunks(u64),
    AutoIncrementPrepared,
    AutoIncrementCommitted,
    NetworkCommitFault(u16),
}

/// Deterministic MySQL recovery boundaries used by the opt-in live matrix.
#[cfg(feature = "migration-fault-injection")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlExecutionInterruption {
    DdlPrepared,
    DdlCommitted,
    ChunkPrepared,
    ChunkCommitBeforeJournal,
    CommittedChunks(u64),
    AutoIncrementPrepared,
    AutoIncrementCommitted,
    NetworkCommitFault(u16),
}

#[cfg(feature = "migration-fault-injection")]
impl From<MySqlExecutionInterruption> for MySqlInterruptionPoint {
    fn from(value: MySqlExecutionInterruption) -> Self {
        match value {
            MySqlExecutionInterruption::DdlPrepared => Self::DdlPrepared,
            MySqlExecutionInterruption::DdlCommitted => Self::DdlCommitted,
            MySqlExecutionInterruption::ChunkPrepared => Self::ChunkPrepared,
            MySqlExecutionInterruption::ChunkCommitBeforeJournal => Self::ChunkCommitBeforeJournal,
            MySqlExecutionInterruption::CommittedChunks(count) => Self::CommittedChunks(count),
            MySqlExecutionInterruption::AutoIncrementPrepared => Self::AutoIncrementPrepared,
            MySqlExecutionInterruption::AutoIncrementCommitted => Self::AutoIncrementCommitted,
            MySqlExecutionInterruption::NetworkCommitFault(port) => Self::NetworkCommitFault(port),
        }
    }
}

/// Inputs for one fault-injected MySQL execution.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub struct MySqlInterruptedExecution<'a> {
    pub plan_path: &'a Path,
    pub source_config_path: &'a Path,
    pub source_metadata_config_path: &'a Path,
    pub freeze_config_path: &'a Path,
    pub target_config_path: &'a Path,
    pub target_metadata_config_path: &'a Path,
    pub freeze_assertion_path: &'a Path,
    pub approval_reference: &'a str,
    pub state_path: &'a Path,
    pub interruption: MySqlExecutionInterruption,
}

/// Inputs for a fault-test resume driven by an externally controlled token.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub struct MySqlCancellationResume<'a> {
    pub state_path: &'a Path,
    pub source_config_path: &'a Path,
    pub source_metadata_config_path: &'a Path,
    pub freeze_config_path: &'a Path,
    pub target_config_path: &'a Path,
    pub target_metadata_config_path: &'a Path,
    pub freeze_assertion_path: &'a Path,
}

struct MySqlCancellationMonitor {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl MySqlCancellationMonitor {
    fn start(
        source: Arc<MySqlSourceFactory>,
        target: Arc<MySqlTargetFactory>,
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
                ("source", source.open_control()),
                ("target", target.open_control()),
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
                        errors.push(format!("{label} control: {error}"));
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

impl Drop for MySqlCancellationMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn interrupt_mysql_if(
    interruption: Option<MySqlInterruptionPoint>,
    expected: MySqlInterruptionPoint,
) -> anyhow::Result<()> {
    if interruption == Some(expected) {
        return Err(injected_mysql_interruption(interruption));
    }
    Ok(())
}

fn injected_mysql_interruption(interruption: Option<MySqlInterruptionPoint>) -> anyhow::Error {
    anyhow!("injected MySQL execution interruption at {interruption:?}")
}

#[allow(clippy::too_many_arguments)]
pub fn execute_live_mysql_frozen_plan(
    plan_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    source_metadata_config_path: impl AsRef<Path>,
    freeze_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    target_metadata_config_path: impl AsRef<Path>,
    freeze_assertion_path: impl AsRef<Path>,
    approval_reference: &str,
    state_path: impl AsRef<Path>,
) -> anyhow::Result<MySqlExecutionReport> {
    execute_live_mysql_frozen_plan_internal(
        plan_path.as_ref(),
        source_config_path.as_ref(),
        source_metadata_config_path.as_ref(),
        freeze_config_path.as_ref(),
        target_config_path.as_ref(),
        target_metadata_config_path.as_ref(),
        freeze_assertion_path.as_ref(),
        approval_reference,
        state_path.as_ref(),
        None,
    )
}

/// Execute until one exact MySQL recovery boundary and return an injected error.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub fn execute_live_mysql_frozen_plan_interrupted(
    request: MySqlInterruptedExecution<'_>,
) -> anyhow::Result<MySqlExecutionReport> {
    execute_live_mysql_frozen_plan_internal(
        request.plan_path,
        request.source_config_path,
        request.source_metadata_config_path,
        request.freeze_config_path,
        request.target_config_path,
        request.target_metadata_config_path,
        request.freeze_assertion_path,
        request.approval_reference,
        request.state_path,
        Some(request.interruption.into()),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_live_mysql_frozen_plan_internal(
    plan_path: &Path,
    source_config_path: &Path,
    source_metadata_config_path: &Path,
    freeze_config_path: &Path,
    target_config_path: &Path,
    target_metadata_config_path: &Path,
    freeze_assertion_path: &Path,
    approval_reference: &str,
    state_path: &Path,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<MySqlExecutionReport> {
    if approval_reference.trim().is_empty() {
        return Err(anyhow!("approval reference must not be empty"));
    }
    let cancellation = CancellationToken::default();
    cancellation.observe_process_sigint()?;
    let reviewed: ReviewedPlan = read_json(plan_path)?;
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    let source_config = MySqlEndpointConfig::read(source_config_path)?;
    let source_metadata_config = MySqlEndpointConfig::read(source_metadata_config_path)?;
    let freeze_config = MySqlEndpointConfig::read(freeze_config_path)?;
    let target_config = MySqlEndpointConfig::read(target_config_path)?;
    let target_metadata_config = MySqlEndpointConfig::read(target_metadata_config_path)?;
    validate_mysql_execution_credential_separation(
        &source_config,
        &source_metadata_config,
        &freeze_config,
        &target_config,
        &target_metadata_config,
    )?;
    let assertion: MySqlExternalFreezeAssertion = read_json(freeze_assertion_path)?;
    let accepted = attest_mysql_external_freeze(&freeze_config, &reviewed, &assertion)?;
    let source = Arc::new(MySqlSourceFactory::new_with_cancellation(
        source_config,
        cancellation.clone(),
    ));
    let source_catalog = reviewed
        .plan
        .source_catalog
        .clone()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no source catalog"))?;
    let target_evidence = reviewed
        .plan
        .mysql_target_snapshot_evidence
        .clone()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target snapshot evidence"))?;
    let target = Arc::new(MySqlTargetFactory::new_with_cancellation(
        target_config,
        source_catalog,
        target_evidence,
        cancellation.clone(),
    )?);
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
    let _monitor = MySqlCancellationMonitor::start(
        Arc::clone(&source),
        Arc::clone(&target),
        cancellation.clone(),
    );
    source
        .validate_reviewed_binding(&reviewed)
        .context("validate reviewed MySQL source factory binding before journal creation")?;
    let (preflight_reader, _) = capture_exact_source(
        &reviewed,
        source.as_ref(),
        &cancellation,
        &source_metadata_config,
        &freeze_config,
    )
    .context("re-attest the reviewed MySQL source before journal creation")?;
    drop(preflight_reader);
    attest_initial_mysql_target(&reviewed, target.endpoint_config(), &target_metadata_config)?;
    target
        .assert_empty()
        .context("require the reviewed MySQL target to remain empty before execution")?;
    let binding = mysql_resume_binding(&reviewed, &accepted, approval_reference)?;
    let mut journal = AppendJournal::create_new(
        state_path,
        mysql_journal_genesis(binding, reviewed.clone(), accepted),
    )?;
    execute_mysql_frozen_plan_internal(
        &reviewed,
        source.as_ref(),
        target.as_ref(),
        &mut journal,
        &cancellation,
        MySqlExecutionAdminConfigs {
            freeze: &freeze_config,
            source_metadata: &source_metadata_config,
            target_metadata: &target_metadata_config,
        },
        &assertion,
        interruption,
    )?;
    mysql_execution_report(state_path, &journal)
}

#[allow(clippy::too_many_arguments)]
pub fn resume_live_mysql_frozen_plan(
    state_path: impl AsRef<Path>,
    source_config_path: impl AsRef<Path>,
    source_metadata_config_path: impl AsRef<Path>,
    freeze_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    target_metadata_config_path: impl AsRef<Path>,
    freeze_assertion_path: impl AsRef<Path>,
) -> anyhow::Result<MySqlExecutionReport> {
    let cancellation = CancellationToken::default();
    cancellation.observe_process_sigint()?;
    resume_live_mysql_frozen_plan_internal(
        state_path.as_ref(),
        source_config_path.as_ref(),
        source_metadata_config_path.as_ref(),
        freeze_config_path.as_ref(),
        target_config_path.as_ref(),
        target_metadata_config_path.as_ref(),
        freeze_assertion_path.as_ref(),
        cancellation,
    )
}

/// Resume using a caller-controlled cancellation token for fault testing.
#[doc(hidden)]
#[cfg(feature = "migration-fault-injection")]
pub fn resume_live_mysql_frozen_plan_with_cancellation(
    request: MySqlCancellationResume<'_>,
    cancellation: CancellationToken,
) -> anyhow::Result<MySqlExecutionReport> {
    resume_live_mysql_frozen_plan_internal(
        request.state_path,
        request.source_config_path,
        request.source_metadata_config_path,
        request.freeze_config_path,
        request.target_config_path,
        request.target_metadata_config_path,
        request.freeze_assertion_path,
        cancellation,
    )
}

#[allow(clippy::too_many_arguments)]
fn resume_live_mysql_frozen_plan_internal(
    state_path: &Path,
    source_config_path: &Path,
    source_metadata_config_path: &Path,
    freeze_config_path: &Path,
    target_config_path: &Path,
    target_metadata_config_path: &Path,
    freeze_assertion_path: &Path,
    cancellation: CancellationToken,
) -> anyhow::Result<MySqlExecutionReport> {
    let mut journal = AppendJournal::open_resume(state_path)?;
    let reviewed = journal.reviewed_plan().clone();
    reviewed.validate()?;
    reviewed.plan.validate_for_execution()?;
    let source_config = MySqlEndpointConfig::read(source_config_path)?;
    let source_metadata_config = MySqlEndpointConfig::read(source_metadata_config_path)?;
    let freeze_config = MySqlEndpointConfig::read(freeze_config_path)?;
    let target_config = MySqlEndpointConfig::read(target_config_path)?;
    let target_metadata_config = MySqlEndpointConfig::read(target_metadata_config_path)?;
    validate_mysql_execution_credential_separation(
        &source_config,
        &source_metadata_config,
        &freeze_config,
        &target_config,
        &target_metadata_config,
    )?;
    let assertion: MySqlExternalFreezeAssertion = read_json(freeze_assertion_path)?;
    let source = Arc::new(MySqlSourceFactory::new_with_cancellation(
        source_config,
        cancellation.clone(),
    ));
    let source_catalog = reviewed
        .plan
        .source_catalog
        .clone()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no source catalog"))?;
    let target_evidence = reviewed
        .plan
        .mysql_target_snapshot_evidence
        .clone()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target snapshot evidence"))?;
    let target = Arc::new(MySqlTargetFactory::new_with_cancellation(
        target_config,
        source_catalog,
        target_evidence,
        cancellation.clone(),
    )?);
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
    let _monitor = MySqlCancellationMonitor::start(
        Arc::clone(&source),
        Arc::clone(&target),
        cancellation.clone(),
    );
    execute_mysql_frozen_plan(
        &reviewed,
        source.as_ref(),
        target.as_ref(),
        &mut journal,
        &cancellation,
        MySqlExecutionAdminConfigs {
            freeze: &freeze_config,
            source_metadata: &source_metadata_config,
            target_metadata: &target_metadata_config,
        },
        &assertion,
    )?;
    mysql_execution_report(state_path, &journal)
}

fn mysql_resume_binding(
    reviewed: &ReviewedPlan,
    accepted: &MySqlExternalFreezeAttestation,
    approval_reference: &str,
) -> anyhow::Result<ResumeBinding> {
    let target_endpoint = reviewed
        .plan
        .target_endpoint_identity
        .as_assessed()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target endpoint"))?
        .clone();
    let target_fingerprint = reviewed
        .plan
        .target_catalog_fingerprint
        .as_assessed()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target catalog fingerprint"))?
        .clone();
    Ok(ResumeBinding {
        migration_id: reviewed.plan.migration_id.clone(),
        plan_hash: reviewed.plan_hash.to_string(),
        approval_reference: approval_reference.into(),
        tool_version: reviewed.plan.tool_version.clone(),
        source_endpoint: reviewed.plan.source_endpoint_identity.clone(),
        target_endpoint,
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
        target_schema_fingerprint: target_fingerprint,
        outage_projection_digest: None,
        external_quiesce_attestation_digest: None,
        mysql_freeze_attestation_digest: Some(accepted.canonical_hash()?),
        conversion_policy: reviewed.plan.conversion_policy.clone(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
    })
}

fn mysql_journal_genesis(
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

fn mysql_execution_report(
    state_path: &Path,
    journal: &AppendJournal,
) -> anyhow::Result<MySqlExecutionReport> {
    if journal.projection().status != MigrationStatus::Completed {
        return Err(anyhow!("MySQL migration did not reach Completed state"));
    }
    let copied_rows = journal
        .projection()
        .copy_cursors
        .values()
        .try_fold(0_u64, |total, cursor| total.checked_add(cursor.rows))
        .ok_or_else(|| anyhow!("MySQL copied-row count overflow"))?;
    Ok(MySqlExecutionReport {
        state: state_path.to_path_buf(),
        copied_rows,
        committed_chunks: journal.projection().last_chunk_id,
    })
}

/// Execute or resume the reviewed MySQL plan while continuously re-attesting
/// the externally owned source freeze. The journal must have been created with
/// the same reviewed plan and accepted freeze attestation.
pub fn execute_mysql_frozen_plan(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    admin_configs: MySqlExecutionAdminConfigs<'_>,
    assertion: &MySqlExternalFreezeAssertion,
) -> anyhow::Result<()> {
    execute_mysql_frozen_plan_internal(
        reviewed,
        source,
        target,
        journal,
        cancellation,
        admin_configs,
        assertion,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_mysql_frozen_plan_internal(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    admin_configs: MySqlExecutionAdminConfigs<'_>,
    assertion: &MySqlExternalFreezeAssertion,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()> {
    execute_mysql_frozen_plan_with(
        reviewed,
        source,
        target,
        journal,
        cancellation,
        &admin_configs,
        &mut || {
            attest_mysql_external_freeze(admin_configs.freeze, reviewed, assertion)
                .map_err(anyhow::Error::from)
        },
        interruption,
    )
}

#[allow(clippy::too_many_arguments)] // Internal executor plus one test-only fault selector.
fn execute_mysql_frozen_plan_with<F>(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    admin_configs: &MySqlExecutionAdminConfigs<'_>,
    attest: &mut F,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    F: FnMut() -> anyhow::Result<MySqlExternalFreezeAttestation>,
{
    validate_mysql_execution_credential_separation(
        source.endpoint_config(),
        admin_configs.source_metadata,
        admin_configs.freeze,
        target.endpoint_config(),
        admin_configs.target_metadata,
    )?;
    reviewed
        .validate()
        .context("validate reviewed MySQL plan hash")?;
    reviewed
        .plan
        .validate_for_execution()
        .context("validate MySQL plan for execution")?;
    source
        .validate_reviewed_binding(reviewed)
        .context("validate reviewed MySQL source factory binding")?;
    target
        .validate_reviewed_binding(reviewed)
        .context("validate reviewed MySQL target factory binding")?;
    if journal.reviewed_plan() != reviewed {
        return Err(anyhow!(
            "MySQL journal genesis contains a different reviewed plan"
        ));
    }
    let accepted = journal
        .accepted_mysql_freeze()
        .cloned()
        .ok_or_else(|| anyhow!("MySQL journal genesis has no accepted freeze attestation"))?;
    validate_accepted_freeze(reviewed, &accepted)?;
    let mut require_freeze = || -> anyhow::Result<()> {
        cancellation.check()?;
        let current = attest().context("re-attest the external MySQL freeze")?;
        validate_mysql_external_freeze_continuity(&accepted, &current)
            .context("validate continuous MySQL freeze evidence")?;
        attest_current_mysql_source_visibility(
            reviewed,
            source.endpoint_config(),
            admin_configs.source_metadata,
            admin_configs.freeze,
        )
        .context("re-attest MySQL source metadata visibility and authorization inventory")?;
        attest_current_mysql_target_visibility(
            reviewed,
            target.endpoint_config(),
            admin_configs.target_metadata,
        )
        .context("re-attest MySQL target metadata visibility and authorization inventory")?;
        cancellation.check()?;
        Ok(())
    };

    match journal.projection().status {
        MigrationStatus::ManualReconciliationRequired | MigrationStatus::Cancelled => {
            return Err(anyhow!(
                "MySQL migration journal is in a terminal non-success state"
            ));
        }
        MigrationStatus::Running | MigrationStatus::Verifying | MigrationStatus::Completed => {}
    }

    require_freeze()?;
    let (mut reader, initial_catalog) = capture_exact_source(
        reviewed,
        source,
        cancellation,
        admin_configs.source_metadata,
        admin_configs.freeze,
    )?;
    let copy_contracts = mysql_copy_contracts(reviewed, &initial_catalog)?;
    let auto_increment_contracts = mysql_auto_increment_contracts(reviewed)?;

    if journal.projection().status == MigrationStatus::Running {
        reconcile_mysql_pre_data_schema_with(
            reviewed,
            target,
            journal,
            cancellation,
            &mut require_freeze,
            interruption,
        )?;
        copy_mysql_tables(
            reviewed,
            reader.as_mut(),
            target,
            journal,
            cancellation,
            &copy_contracts,
            &mut require_freeze,
            interruption,
        )?;
    }
    drop(reader);

    require_freeze()?;
    let (mut verification_reader, final_catalog) = capture_exact_source(
        reviewed,
        source,
        cancellation,
        admin_configs.source_metadata,
        admin_configs.freeze,
    )?;
    verify_auto_increment_source_equality(reviewed, &final_catalog)?;
    if journal.projection().status == MigrationStatus::Running {
        reconcile_mysql_auto_increment(
            target,
            journal,
            cancellation,
            &auto_increment_contracts,
            &mut require_freeze,
            interruption,
        )?;
        journal.transition_status(MigrationStatus::Verifying)?;
    }
    verify_mysql_auto_increment_target(target, journal, &auto_increment_contracts)?;

    require_freeze()?;
    verify_mysql_tables(
        reviewed,
        verification_reader.as_mut(),
        target,
        journal,
        cancellation,
        &copy_contracts,
    )?;
    require_freeze()?;
    target.assert_exact_schema()?;
    finish_mysql_schema_verification(reviewed, journal)?;
    Ok(())
}

fn validate_mysql_execution_credential_separation(
    source: &MySqlEndpointConfig,
    source_metadata: &MySqlEndpointConfig,
    freeze: &MySqlEndpointConfig,
    target: &MySqlEndpointConfig,
    target_metadata: &MySqlEndpointConfig,
) -> anyhow::Result<()> {
    let references = [
        source.credential_env.as_str(),
        source_metadata.credential_env.as_str(),
        freeze.credential_env.as_str(),
        target.credential_env.as_str(),
        target_metadata.credential_env.as_str(),
    ];
    if references
        .iter()
        .enumerate()
        .any(|(index, value)| references[..index].contains(value))
    {
        return Err(anyhow!(
            "MySQL execution requires separate source reader, source metadata administrator, freeze administrator, target writer, and target metadata administrator credential references"
        ));
    }
    Ok(())
}

fn attest_current_mysql_source_visibility(
    reviewed: &ReviewedPlan,
    source_config: &MySqlEndpointConfig,
    metadata_admin_config: &MySqlEndpointConfig,
    freeze_admin_config: &MySqlEndpointConfig,
) -> anyhow::Result<()> {
    let current_source = super::mysql::inspect_live_endpoint(source_config.clone())?;
    let current = collect_mysql_metadata_visibility(
        &current_source,
        source_config,
        metadata_admin_config,
        Some(freeze_admin_config),
    )?;
    let accepted = reviewed
        .plan
        .mysql_metadata_visibility
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no metadata-visibility evidence"))?;
    let fingerprint = mysql_catalog_fingerprint(&current.authoritative_catalog)?;
    if current.evidence != *accepted
        || !super::mysql::mysql_catalog_visibility_is_complete(&current_source, &current)?
        || fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&current.authoritative_catalog)
        || !current.authoritative_blockers.is_empty()
    {
        return Err(anyhow!(
            "current MySQL metadata visibility, grants, roles, partial revokes, or catalog differs from the reviewed contract"
        ));
    }
    Ok(())
}

fn attest_current_mysql_target_visibility(
    reviewed: &ReviewedPlan,
    target_config: &MySqlEndpointConfig,
    metadata_admin_config: &MySqlEndpointConfig,
) -> anyhow::Result<()> {
    let current_target = super::mysql::inspect_live_endpoint(target_config.clone())?;
    let current = collect_mysql_metadata_visibility(
        &current_target,
        target_config,
        metadata_admin_config,
        None,
    )?;
    let accepted = reviewed
        .plan
        .mysql_target_metadata_visibility
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target metadata-visibility evidence"))?;
    if !current.evidence.same_authorization_as(accepted)
        || !super::mysql::mysql_catalog_visibility_is_complete(&current_target, &current)?
        || !current.authoritative_blockers.is_empty()
    {
        return Err(anyhow!(
            "current MySQL target metadata visibility, grants, roles, partial revokes, or catalog differs from the reviewed contract"
        ));
    }
    Ok(())
}

fn attest_initial_mysql_target(
    reviewed: &ReviewedPlan,
    target_config: &MySqlEndpointConfig,
    metadata_admin_config: &MySqlEndpointConfig,
) -> anyhow::Result<()> {
    let current_target = super::mysql::inspect_live_endpoint(target_config.clone())?;
    let current = collect_mysql_metadata_visibility(
        &current_target,
        target_config,
        metadata_admin_config,
        None,
    )?;
    let accepted_visibility = reviewed
        .plan
        .mysql_target_metadata_visibility
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target metadata-visibility evidence"))?;
    let accepted_catalog = reviewed
        .plan
        .target_catalog
        .as_assessed()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target catalog"))?;
    let accepted_fingerprint = reviewed
        .plan
        .target_catalog_fingerprint
        .as_assessed()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no target catalog fingerprint"))?;
    let current_fingerprint = mysql_catalog_fingerprint(&current.authoritative_catalog)?;
    if current.evidence != *accepted_visibility
        || current.authoritative_catalog != *accepted_catalog
        || current_fingerprint != *accepted_fingerprint
        || !super::mysql::mysql_catalog_visibility_is_complete(&current_target, &current)?
        || !current.authoritative_blockers.is_empty()
    {
        return Err(anyhow!(
            "current MySQL target catalog, visibility, or authorization differs from the reviewed empty target"
        ));
    }
    Ok(())
}

fn validate_accepted_freeze(
    reviewed: &ReviewedPlan,
    accepted: &MySqlExternalFreezeAttestation,
) -> anyhow::Result<()> {
    accepted.validate()?;
    if accepted.source_endpoint_identity != reviewed.plan.source_endpoint_identity
        || accepted.source_catalog_fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed
            .plan
            .mysql_snapshot_evidence
            .as_ref()
            .is_none_or(|evidence| {
                accepted.source_database_identity != evidence.database_identity
                    || accepted.server_uuid != evidence.server_uuid
            })
    {
        return Err(anyhow!(
            "accepted MySQL freeze attestation differs from the reviewed source"
        ));
    }
    Ok(())
}

fn capture_exact_source(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    cancellation: &CancellationToken,
    metadata_admin_config: &MySqlEndpointConfig,
    freeze_admin_config: &MySqlEndpointConfig,
) -> anyhow::Result<(Box<dyn ReadSession>, VendorCatalog)> {
    cancellation.check()?;
    let snapshot = source.capture_snapshot()?;
    let current_evidence = source.snapshot_evidence(&snapshot)?;
    let reviewed_evidence = reviewed
        .plan
        .mysql_snapshot_evidence
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no source snapshot evidence"))?;
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
        metadata_admin_config,
        Some(freeze_admin_config),
    )?;
    let reviewed_visibility = reviewed
        .plan
        .mysql_metadata_visibility
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no metadata-visibility evidence"))?;
    let fingerprint = mysql_catalog_fingerprint(&visibility.authoritative_catalog)?;
    if !visibility.authoritative_blockers.is_empty()
        || visibility.evidence != *reviewed_visibility
        || !super::mysql::mysql_catalog_visibility_is_complete(&source_snapshot, &visibility)?
        || reader_fingerprint != reviewed_visibility.catalog_reader_fingerprint
        || fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&visibility.authoritative_catalog)
        || !mysql_execution_snapshot_binding_is_exact(&current_evidence, reviewed_evidence)
    {
        return Err(anyhow!(
            "fresh MySQL source catalog differs from the reviewed executable catalog"
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

fn mysql_execution_snapshot_binding_is_exact(
    current: &MySqlSnapshotEvidence,
    reviewed: &MySqlSnapshotEvidence,
) -> bool {
    mysql_execution_snapshot_binding(current) == mysql_execution_snapshot_binding(reviewed)
}

#[derive(Debug, PartialEq, Eq)]
struct MySqlExecutionSnapshotBinding<'a> {
    endpoint_identity: &'a str,
    database_identity: &'a str,
    server_uuid: &'a str,
    server_version: &'a str,
    authenticated_account: &'a str,
    transaction_isolation: &'a str,
    transaction_read_only: bool,
    session_time_zone: &'a str,
    catalog_snapshot_protected: bool,
    information_schema_stats_expiry: u64,
    lower_case_table_names: u8,
    session_sql_mode: &'a str,
    character_set_client: &'a str,
    character_set_connection: &'a str,
    character_set_results: &'a str,
    collation_connection: &'a str,
    catalog_fingerprint: &'a str,
}

fn mysql_execution_snapshot_binding(
    evidence: &MySqlSnapshotEvidence,
) -> MySqlExecutionSnapshotBinding<'_> {
    let MySqlSnapshotEvidence {
        endpoint_identity,
        database_identity,
        server_uuid,
        server_version,
        authenticated_account,
        lifecycle_id: _,
        connection_id: _,
        transaction_isolation,
        transaction_read_only,
        session_time_zone,
        catalog_snapshot_protected,
        information_schema_stats_expiry,
        lower_case_table_names,
        session_sql_mode,
        character_set_client,
        character_set_connection,
        character_set_results,
        collation_connection,
        gtid_executed_observation: _,
        catalog_fingerprint,
    } = evidence;
    MySqlExecutionSnapshotBinding {
        endpoint_identity,
        database_identity,
        server_uuid,
        server_version,
        authenticated_account,
        transaction_isolation,
        transaction_read_only: *transaction_read_only,
        session_time_zone,
        catalog_snapshot_protected: *catalog_snapshot_protected,
        information_schema_stats_expiry: *information_schema_stats_expiry,
        lower_case_table_names: *lower_case_table_names,
        session_sql_mode,
        character_set_client,
        character_set_connection,
        character_set_results,
        collation_connection,
        catalog_fingerprint,
    }
}

fn mysql_copy_contracts(
    reviewed: &ReviewedPlan,
    source_catalog: &VendorCatalog,
) -> anyhow::Result<Vec<MySqlCopyContract>> {
    let definitions = mysql_table_definitions(source_catalog)?
        .into_iter()
        .map(|definition| (definition.table.clone(), definition))
        .collect::<BTreeMap<_, _>>();
    reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
        .map(|operation| {
            let source = operation
                .table
                .clone()
                .ok_or_else(|| anyhow!("MySQL copy operation has no source table"))?;
            let definition = definitions
                .get(&source)
                .ok_or_else(|| anyhow!("MySQL copy operation refers to an unknown source table"))?;
            let mapping: MySqlTableMapping = operation_parameter(operation, "mysql_table_mapping")?;
            let key: MySqlResumableKey = operation_parameter(operation, "resumable_key")?;
            if mapping.source != source
                || key.columns.is_empty()
                || key.columns.len() != key.column_types.len()
                || key.columns.len() != key.collations.len()
                || operation
                    .parameters
                    .get("mysql_write_policy")
                    .and_then(serde_json::Value::as_str)
                    != Some("plain_insert_transaction_v1")
            {
                return Err(anyhow!("invalid reviewed MySQL copy contract"));
            }
            let projection = definition
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let key_indexes = key
                .columns
                .iter()
                .map(|key_column| {
                    projection
                        .iter()
                        .position(|column| column == key_column)
                        .ok_or_else(|| anyhow!("MySQL resumable key is absent from the projection"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(MySqlCopyContract {
                operation_id: operation.id.to_string(),
                source,
                target: mapping.target,
                projection,
                key: key.columns,
                key_indexes,
            })
        })
        .collect()
}

fn mysql_auto_increment_contracts(
    reviewed: &ReviewedPlan,
) -> anyhow::Result<Vec<MySqlAutoIncrementContract>> {
    reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| {
            matches!(
                &operation.kind,
                OperationKind::Vendor(name) if name == "restore_mysql_auto_increment"
            )
        })
        .map(|operation| {
            let state: MySqlAutoIncrementState =
                operation_parameter(operation, "mysql_auto_increment_state")?;
            let mapping: MySqlTableMapping = operation_parameter(operation, "mysql_table_mapping")?;
            if operation.table.as_ref() != Some(&state.table)
                || mapping.source != state.table
                || state.next_value.is_none()
                || state.stats_expiry != 0
            {
                return Err(anyhow!("invalid reviewed MySQL AUTO_INCREMENT contract"));
            }
            Ok(MySqlAutoIncrementContract {
                operation_id: operation.id.to_string(),
                state,
                mapping,
            })
        })
        .collect()
}

fn operation_parameter<T: serde::de::DeserializeOwned>(
    operation: &PlanOperation,
    name: &str,
) -> anyhow::Result<T> {
    serde_json::from_value(
        operation
            .parameters
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("MySQL operation has no {name} parameter"))?,
    )
    .with_context(|| format!("decode reviewed MySQL operation parameter {name}"))
}

#[allow(clippy::too_many_arguments)] // Keeps the durable copy inputs explicit at the effect boundary.
fn copy_mysql_tables<G>(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlCopyContract],
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    if let Some(prepared) = journal.projection().prepared_chunk.clone() {
        let contract = contracts
            .iter()
            .find(|contract| contract.operation_id == prepared.operation_id)
            .ok_or_else(|| anyhow!("prepared MySQL chunk has no reviewed copy contract"))?;
        reconcile_prepared_mysql_chunk(
            reviewed,
            reader,
            target,
            journal,
            cancellation,
            contract,
            before_effect,
            interruption,
        )?;
    }

    for contract in contracts {
        match operation_state(journal, &contract.operation_id)? {
            OperationState::Pending => {
                journal.transition_operation(&contract.operation_id, OperationState::Running)?;
            }
            OperationState::Running
            | OperationState::Prepared
            | OperationState::Committed
            | OperationState::Verified => {}
        }
        if operation_state(journal, &contract.operation_id)? == OperationState::Running {
            let mut after = journal
                .projection()
                .copy_cursors
                .get(&contract.operation_id)
                .map(|cursor| KeyTuple::new(cursor.final_key.clone()));
            loop {
                cancellation.check()?;
                let batch = reader.select_page(&page_request(
                    &contract.source,
                    contract,
                    after.clone(),
                    u32::MAX,
                ))?;
                if batch.is_empty() {
                    break;
                }
                let final_key = batch_final_key(&batch, contract)?;
                let chunk_id = journal
                    .projection()
                    .last_chunk_id
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("MySQL chunk identifier overflow"))?;
                journal.prepare_chunk(PreparedChunk {
                    chunk_id,
                    operation_id: contract.operation_id.clone(),
                    start_key: after.as_ref().map(|key| key.0.clone()),
                    final_key: final_key.0.clone(),
                    row_count: u64::try_from(batch.len())?,
                    canonical_digest: batch_digest(&contract.source, contract, &batch)?,
                    target_transaction_intent: format!(
                        "{}:{}:{chunk_id}",
                        reviewed.plan_hash, contract.operation_id
                    ),
                })?;
                interrupt_mysql_if(interruption, MySqlInterruptionPoint::ChunkPrepared)?;
                write_mysql_chunk(
                    target,
                    journal,
                    cancellation,
                    contract,
                    &batch,
                    before_effect,
                    interruption,
                )?;
                after = Some(final_key);
            }
        }
        complete_effect_operation(journal, &contract.operation_id)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Mirrors the copy boundary for exact prepared reconciliation.
fn reconcile_prepared_mysql_chunk<G>(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contract: &MySqlCopyContract,
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    let prepared = journal
        .projection()
        .prepared_chunk
        .clone()
        .ok_or_else(|| anyhow!("MySQL resume has no durable prepared chunk"))?;
    let expected_limit =
        u32::try_from(prepared.row_count).context("prepared MySQL chunk row count exceeds u32")?;
    let expected = reader.select_page(&page_request(
        &contract.source,
        contract,
        prepared.start_key.clone().map(KeyTuple::new),
        expected_limit,
    ))?;
    if expected.len() as u64 != prepared.row_count
        || batch_final_key(&expected, contract)?.0 != prepared.final_key
        || batch_digest(&contract.source, contract, &expected)? != prepared.canonical_digest
        || prepared.target_transaction_intent
            != format!(
                "{}:{}:{}",
                reviewed.plan_hash, contract.operation_id, prepared.chunk_id
            )
    {
        return require_manual(
            journal,
            "prepared MySQL chunk differs from the continuously frozen source".into(),
        );
    }
    match inspect_target_interval(target, cancellation, contract, &prepared, &expected)? {
        TargetIntervalState::Exact => journal.commit_chunk_after_ack().map_err(Into::into),
        TargetIntervalState::Absent => write_mysql_chunk(
            target,
            journal,
            cancellation,
            contract,
            &expected,
            before_effect,
            interruption,
        ),
        TargetIntervalState::Different => require_manual(
            journal,
            "prepared MySQL target interval differs from durable intent".into(),
        ),
    }
}

fn write_mysql_chunk<G>(
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contract: &MySqlCopyContract,
    batch: &RowBatch,
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    before_effect()?;
    cancellation.check()?;
    let mut writer = target.open_writer(cancellation.clone())?;
    writer.begin()?;
    if let Err(error) = writer.insert(&contract.target, batch) {
        let rollback = writer.rollback();
        return match rollback {
            Ok(()) => Err(error.into()),
            Err(rollback_error) => Err(anyhow!(error).context(format!(
                "MySQL insert failed and rollback also failed: {rollback_error}"
            ))),
        };
    }
    if let Err(error) = cancellation.check() {
        let rollback = writer.rollback();
        return match rollback {
            Ok(()) => Err(error.into()),
            Err(rollback_error) => Err(anyhow!(error).context(format!(
                "MySQL cancellation rollback failed: {rollback_error}"
            ))),
        };
    }
    #[cfg(feature = "migration-fault-injection")]
    if let Some(MySqlInterruptionPoint::NetworkCommitFault(port)) = interruption {
        let prepared_chunk_id = journal
            .projection()
            .prepared_chunk
            .as_ref()
            .map(|chunk| chunk.chunk_id);
        if prepared_chunk_id == Some(1) {
            arm_mysql_network_commit_fault(port)?;
        }
    }
    match writer.commit() {
        Ok(()) => {
            interrupt_mysql_if(
                interruption,
                MySqlInterruptionPoint::ChunkCommitBeforeJournal,
            )?;
            journal
                .commit_chunk_after_ack()
                .map_err(anyhow::Error::from)
        }
        Err(ConnectionError::CommitOutcomeUnknown(error)) => {
            let prepared =
                journal.projection().prepared_chunk.clone().ok_or_else(|| {
                    anyhow!("ambiguous MySQL commit has no durable prepared chunk")
                })?;
            match inspect_target_interval(target, cancellation, contract, &prepared, batch)? {
                TargetIntervalState::Exact => journal.commit_chunk_after_ack().map_err(Into::into),
                TargetIntervalState::Absent => Err(anyhow!(
                    "MySQL commit outcome is unknown and its effect is absent ({error}); restart resume to retry the durable intent"
                )),
                TargetIntervalState::Different => require_manual(
                    journal,
                    format!("ambiguous MySQL commit produced a different target interval: {error}"),
                ),
            }
        }
        Err(error) => Err(error.into()),
    }?;
    if interruption.is_some_and(|point| {
        matches!(point, MySqlInterruptionPoint::CommittedChunks(limit) if journal.projection().last_chunk_id >= limit)
    }) {
        return Err(injected_mysql_interruption(interruption));
    }
    Ok(())
}

#[cfg(feature = "migration-fault-injection")]
fn arm_mysql_network_commit_fault(port: u16) -> anyhow::Result<()> {
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};

    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let mut control = TcpStream::connect_timeout(&address.into(), Duration::from_secs(10))?;
    control.set_read_timeout(Some(Duration::from_secs(10)))?;
    control.set_write_timeout(Some(Duration::from_secs(10)))?;
    control.write_all(b"ARM\n")?;
    let mut response = [0_u8; 6];
    control.read_exact(&mut response)?;
    if &response != b"ARMED\n" {
        return Err(anyhow!(
            "MySQL commit fault proxy returned an invalid arm response"
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetIntervalState {
    Absent,
    Exact,
    Different,
}

fn inspect_target_interval(
    target: &MySqlTargetFactory,
    cancellation: &CancellationToken,
    contract: &MySqlCopyContract,
    prepared: &PreparedChunk,
    expected: &RowBatch,
) -> anyhow::Result<TargetIntervalState> {
    let limit = prepared
        .row_count
        .checked_add(1)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| anyhow!("prepared MySQL chunk is too large to inspect"))?;
    let mut verifier = TargetConnectionFactory::open_verifier(target, cancellation.clone())?;
    let observed = verifier.select_page(&page_request(
        &contract.target,
        contract,
        prepared.start_key.clone().map(KeyTuple::new),
        limit,
    ))?;
    if observed.is_empty() {
        return Ok(TargetIntervalState::Absent);
    }
    let exact = observed.rows() == expected.rows()
        && observed.len() as u64 == prepared.row_count
        && batch_final_key(&observed, contract)?.0 == prepared.final_key
        && batch_digest(&contract.source, contract, &observed)? == prepared.canonical_digest;
    Ok(if exact {
        TargetIntervalState::Exact
    } else {
        TargetIntervalState::Different
    })
}

fn verify_auto_increment_source_equality(
    reviewed: &ReviewedPlan,
    final_catalog: &VendorCatalog,
) -> anyhow::Result<()> {
    let initial = reviewed
        .plan
        .source_catalog
        .as_ref()
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no source catalog"))?;
    if mysql_auto_increment_states(initial)? != mysql_auto_increment_states(final_catalog)? {
        return Err(anyhow!(
            "MySQL AUTO_INCREMENT state changed after freeze activation"
        ));
    }
    Ok(())
}

fn reconcile_mysql_auto_increment<G>(
    target: &impl MySqlAutoIncrementTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlAutoIncrementContract],
    before_effect: &mut G,
    interruption: Option<MySqlInterruptionPoint>,
) -> anyhow::Result<()>
where
    G: FnMut() -> anyhow::Result<()>,
{
    for contract in contracts {
        let operation_id = contract.operation_id.as_str();
        let mut state = operation_state(journal, operation_id)?;
        let mut observed = target.inspect_auto_increment(&contract.state, &contract.mapping)?;
        match state {
            OperationState::Pending | OperationState::Running => {
                if observed == MySqlAutoIncrementTargetState::Different {
                    return require_manual(
                        journal,
                        "MySQL AUTO_INCREMENT differs before its durable restore intent".into(),
                    );
                }
                if state == OperationState::Pending {
                    journal.prepare_operations_atomic([operation_id])?;
                } else {
                    journal.transition_operation(operation_id, OperationState::Prepared)?;
                }
                state = OperationState::Prepared;
            }
            OperationState::Prepared | OperationState::Committed | OperationState::Verified => {}
        }
        if state == OperationState::Prepared {
            interrupt_mysql_if(interruption, MySqlInterruptionPoint::AutoIncrementPrepared)?;
            if observed == MySqlAutoIncrementTargetState::BeforeDesired {
                cancellation.check()?;
                before_effect()?;
                if let Err(error) =
                    target.restore_auto_increment(&contract.state, &contract.mapping)
                {
                    observed = target
                        .inspect_auto_increment(&contract.state, &contract.mapping)
                        .context("inspect MySQL AUTO_INCREMENT after restore error")?;
                    if observed != MySqlAutoIncrementTargetState::Exact {
                        return if observed == MySqlAutoIncrementTargetState::Different {
                            require_manual(
                                journal,
                                format!(
                                    "MySQL AUTO_INCREMENT restore produced a different effect: {error}"
                                ),
                            )
                        } else {
                            Err(anyhow!(error).context(
                                "MySQL AUTO_INCREMENT restore did not take effect; durable intent remains prepared",
                            ))
                        };
                    }
                }
            } else if observed == MySqlAutoIncrementTargetState::Different {
                return require_manual(
                    journal,
                    "prepared MySQL AUTO_INCREMENT target differs from reviewed state".into(),
                );
            }
            if target.inspect_auto_increment(&contract.state, &contract.mapping)?
                != MySqlAutoIncrementTargetState::Exact
            {
                return Err(anyhow!(
                    "MySQL AUTO_INCREMENT restore is not exactly observable"
                ));
            }
            journal.transition_operation(operation_id, OperationState::Committed)?;
            state = OperationState::Committed;
            interrupt_mysql_if(interruption, MySqlInterruptionPoint::AutoIncrementCommitted)?;
        }
        if state == OperationState::Committed {
            if target.inspect_auto_increment(&contract.state, &contract.mapping)?
                != MySqlAutoIncrementTargetState::Exact
            {
                return require_manual(
                    journal,
                    "committed MySQL AUTO_INCREMENT effect drifted".into(),
                );
            }
            journal.transition_operation(operation_id, OperationState::Verified)?;
        } else if state == OperationState::Verified
            && target.inspect_auto_increment(&contract.state, &contract.mapping)?
                != MySqlAutoIncrementTargetState::Exact
        {
            return require_manual(
                journal,
                "verified MySQL AUTO_INCREMENT effect drifted".into(),
            );
        }
    }
    Ok(())
}

fn verify_mysql_auto_increment_target(
    target: &impl MySqlAutoIncrementTarget,
    journal: &mut AppendJournal,
    contracts: &[MySqlAutoIncrementContract],
) -> anyhow::Result<()> {
    let status = journal.projection().status;
    for contract in contracts {
        if operation_state(journal, &contract.operation_id)? != OperationState::Verified {
            return Err(anyhow!(
                "MySQL AUTO_INCREMENT operation {} is not verified",
                contract.operation_id
            ));
        }
        if target.inspect_auto_increment(&contract.state, &contract.mapping)?
            == MySqlAutoIncrementTargetState::Exact
        {
            continue;
        }
        if status == MigrationStatus::Completed {
            return Err(anyhow!(
                "completed MySQL migration has target AUTO_INCREMENT drift"
            ));
        }
        return require_manual(
            journal,
            "verified MySQL AUTO_INCREMENT effect drifted before final verification".into(),
        );
    }
    Ok(())
}

fn verify_mysql_tables(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &impl MySqlVerificationTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlCopyContract],
) -> anyhow::Result<()> {
    let status = journal.projection().status;
    if !matches!(
        status,
        MigrationStatus::Verifying | MigrationStatus::Completed
    ) {
        return Err(anyhow!(
            "MySQL table verification is outside Verifying or Completed state"
        ));
    }
    let mut verifier = target.open_verifier(cancellation.clone())?;
    let mut chunks = journal.all_committed_chunks()?.peekable();
    let mut fresh_evidence = Vec::with_capacity(contracts.len());
    for contract in contracts {
        let evidence = verify_mysql_table(reader, verifier.as_mut(), &mut chunks, contract)?;
        fresh_evidence.push((
            contract.operation_id.clone(),
            contract.source.clone(),
            evidence,
        ));
    }
    if chunks.next().transpose()?.is_some() {
        return Err(anyhow!(
            "MySQL journal contains committed chunks outside the reviewed table order"
        ));
    }
    drop(chunks);

    for (operation_id, source_table, evidence) in fresh_evidence {
        match journal.table_verification_evidence(&operation_id) {
            Some(stored) if stored == &evidence => {}
            Some(_) => {
                return Err(anyhow!(
                    "fresh MySQL table verification differs from durable evidence"
                ));
            }
            None if status == MigrationStatus::Verifying => journal.verify_table(
                &operation_id,
                evidence.0.clone(),
                evidence.1.clone(),
                evidence.2.clone(),
            )?,
            None => {
                return Err(anyhow!(
                    "completed MySQL migration lacks durable table verification evidence"
                ));
            }
        }
        let verify_operation = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| {
                operation.kind == OperationKind::VerifyTable
                    && operation.table.as_ref() == Some(&source_table)
            })
            .ok_or_else(|| anyhow!("MySQL copy table has no verification operation"))?;
        if status == MigrationStatus::Verifying {
            complete_effect_operation(journal, verify_operation.id.as_str())?;
        } else if operation_state(journal, verify_operation.id.as_str())?
            != OperationState::Verified
        {
            return Err(anyhow!(
                "completed MySQL migration has an unverified table operation"
            ));
        }
    }
    Ok(())
}

fn verify_mysql_table(
    reader: &mut dyn ReadSession,
    verifier: &mut dyn VerificationSession,
    chunks: &mut std::iter::Peekable<CommittedChunkIter>,
    contract: &MySqlCopyContract,
) -> anyhow::Result<(String, String, String)> {
    let mut after = None;
    let mut manifest = Sha256::new();
    let mut source_hash = Sha256::new();
    let mut target_hash = Sha256::new();
    while let Some(next) = chunks.peek() {
        let next = next.as_ref().map_err(|error| anyhow!(error.to_string()))?;
        if next.operation_id != contract.operation_id {
            break;
        }
        let chunk = chunks
            .next()
            .ok_or_else(|| anyhow!("MySQL committed chunk stream ended unexpectedly"))??;
        if chunk.start_key != after.as_ref().map(|key: &KeyTuple| key.0.clone()) {
            return Err(anyhow!("MySQL chunk manifest has a keyspace gap"));
        }
        let limit = u32::try_from(chunk.row_count)
            .context("MySQL committed chunk row count exceeds u32")?;
        let source_batch = reader.select_page(&page_request(
            &contract.source,
            contract,
            after.clone(),
            limit,
        ))?;
        let target_batch = verifier.select_page(&page_request(
            &contract.target,
            contract,
            after.clone(),
            limit,
        ))?;
        if source_batch.len() as u64 != chunk.row_count
            || target_batch.len() as u64 != chunk.row_count
            || source_batch.rows() != target_batch.rows()
            || batch_final_key(&source_batch, contract)?.0 != chunk.final_key
            || batch_final_key(&target_batch, contract)?.0 != chunk.final_key
        {
            return Err(anyhow!("MySQL committed chunk row verification failed"));
        }
        let source_digest = batch_digest(&contract.source, contract, &source_batch)?;
        let target_digest = batch_digest(&contract.source, contract, &target_batch)?;
        if source_digest != chunk.canonical_digest || target_digest != chunk.canonical_digest {
            return Err(anyhow!("MySQL committed chunk digest verification failed"));
        }
        let encoded = serde_json::to_vec(&chunk)?;
        manifest.update(u64::try_from(encoded.len())?.to_be_bytes());
        manifest.update(encoded);
        source_hash.update(source_digest.as_bytes());
        target_hash.update(target_digest.as_bytes());
        after = Some(KeyTuple::new(chunk.final_key));
    }
    let source_tail =
        reader.select_page(&page_request(&contract.source, contract, after.clone(), 1))?;
    let target_tail = verifier.select_page(&page_request(&contract.target, contract, after, 1))?;
    if !source_tail.is_empty() || !target_tail.is_empty() {
        return Err(anyhow!(
            "MySQL source or target contains rows outside committed intervals"
        ));
    }
    Ok((
        hex::encode(manifest.finalize()),
        hex::encode(source_hash.finalize()),
        hex::encode(target_hash.finalize()),
    ))
}

fn finish_mysql_schema_verification(
    reviewed: &ReviewedPlan,
    journal: &mut AppendJournal,
) -> anyhow::Result<()> {
    let verify_schema = reviewed
        .plan
        .operations
        .iter()
        .find(|operation| operation.kind == OperationKind::VerifySchema)
        .ok_or_else(|| anyhow!("reviewed MySQL plan has no schema verification operation"))?;
    if !journal.projection().schema_verified {
        journal.verify_schema(reviewed.plan.source_catalog_fingerprint.clone())?;
    }
    complete_effect_operation(journal, verify_schema.id.as_str())?;
    if journal.projection().status == MigrationStatus::Verifying {
        journal.transition_status(MigrationStatus::Completed)?;
    }
    Ok(())
}

fn complete_effect_operation(
    journal: &mut AppendJournal,
    operation_id: &str,
) -> anyhow::Result<()> {
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
            "MySQL operation {operation_id} did not reach Verified state"
        ));
    }
    Ok(())
}

fn page_request(
    table: &QualifiedTable,
    contract: &MySqlCopyContract,
    after: Option<KeyTuple>,
    limit: u32,
) -> KeysetPage {
    KeysetPage {
        table: table.clone(),
        projection: contract.projection.clone(),
        key: contract.key.clone(),
        after,
        limit,
    }
}

fn batch_final_key(batch: &RowBatch, contract: &MySqlCopyContract) -> anyhow::Result<KeyTuple> {
    let row = batch
        .rows()
        .last()
        .ok_or_else(|| anyhow!("cannot derive a key from an empty MySQL batch"))?;
    let values = contract
        .key_indexes
        .iter()
        .map(|index| {
            let value = row
                .get(*index)
                .ok_or_else(|| anyhow!("MySQL key index exceeds row width"))?;
            if *value == DbValue::Null {
                return Err(anyhow!("MySQL resumable key contains NULL"));
            }
            Ok(value.clone())
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(KeyTuple::new(values))
}

fn batch_digest(
    source_table: &QualifiedTable,
    contract: &MySqlCopyContract,
    batch: &RowBatch,
) -> anyhow::Result<String> {
    let table_name = format!("{}.{}", source_table.namespace, source_table.name);
    let columns = contract
        .projection
        .iter()
        .map(Identifier::as_str)
        .collect::<Vec<_>>();
    let keys = batch
        .rows()
        .iter()
        .map(|row| {
            contract
                .key_indexes
                .iter()
                .map(|index| {
                    row.get(*index)
                        .cloned()
                        .ok_or_else(|| anyhow!("MySQL key index exceeds row width"))
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
            columns: &columns,
            key,
            values: row,
        })
        .collect::<Vec<_>>();
    Ok(hex::encode(digest_rows(canonical.iter())))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::migration::append_journal::{Genesis, OperationPhase, OperationSpec};
    use crate::migration::canonical::CANONICAL_ENCODING_VERSION;
    use crate::migration::connection::{ConnectionError, ReadOnlyEvidence, SnapshotToken};
    use crate::migration::journal::{ConsistencyEvidence, MigrationStatus, ResumeBinding};
    use crate::migration::model::{
        CatalogNamespace, ColumnMeta, Identifier, QualifiedTable, VendorCatalog,
    };
    use crate::migration::mysql::{
        mysql_tls_binding, MySqlColumnDefinition, MySqlColumnType, MySqlEndpointConfig,
        MySqlIndexDefinition, MySqlTlsConfig, MYSQL_CATALOG_FORMAT_VERSION,
        MYSQL_CONSISTENCY_SNAPSHOT,
    };
    use crate::migration::mysql_profile::{
        MySqlDdlFreezeMechanism, MySqlDmlFreezeMechanism, MySqlExternalFreezeAttestation,
        MySqlFreezeAttestationStatus, MySqlFreezeProfileKind,
        MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
    };
    use crate::migration::mysql_visibility::{
        MySqlAccountIdentity, MySqlGrantInventory, MySqlGrantRecord, MySqlGrantTableColumn,
        MySqlMetadataVisibilityEvidence, MySqlOperationalAccountExclusion,
        MySqlOperationalAccountPurpose, MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
    };
    use crate::migration::plan::{
        AssessmentStatus, MigrationPlan, MySqlSnapshotEvidence, PlanPurpose,
        UnsupportedObjectReport, MYSQL_SESSION_CHARACTER_SET, MYSQL_SESSION_COLLATION,
        MYSQL_STRICT_SQL_MODE, PLAN_SCHEMA_VERSION,
    };

    #[derive(Debug, Clone, Copy)]
    enum CreateBehavior {
        Success,
        FailBeforeEffect,
        FailAfterEffect,
        Different,
    }

    #[derive(Debug)]
    struct FakeTarget {
        state: Mutex<MySqlTableState>,
        behavior: Mutex<CreateBehavior>,
    }

    #[derive(Debug)]
    struct FakeAutoIncrementTarget {
        state: Mutex<MySqlAutoIncrementTargetState>,
    }

    #[derive(Debug)]
    struct FakeReader {
        token: SnapshotToken,
        evidence: ReadOnlyEvidence,
        first_page: RowBatch,
    }

    impl ReadSession for FakeReader {
        fn read_only_evidence(&self) -> &ReadOnlyEvidence {
            &self.evidence
        }

        fn snapshot(&self) -> &SnapshotToken {
            &self.token
        }

        fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
            Ok(if request.after.is_none() {
                self.first_page.clone()
            } else {
                empty_batch()
            })
        }
    }

    #[derive(Debug)]
    struct FakeVerifier {
        first_page: RowBatch,
    }

    impl VerificationSession for FakeVerifier {
        fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
            Ok(if request.after.is_none() {
                self.first_page.clone()
            } else {
                empty_batch()
            })
        }
    }

    #[derive(Debug)]
    struct FakeVerificationTarget {
        first_page: RowBatch,
    }

    impl MySqlVerificationTarget for FakeVerificationTarget {
        fn open_verifier(
            &self,
            _cancellation: CancellationToken,
        ) -> ConnectionResult<Box<dyn VerificationSession>> {
            Ok(Box::new(FakeVerifier {
                first_page: self.first_page.clone(),
            }))
        }
    }

    impl MySqlAutoIncrementTarget for FakeAutoIncrementTarget {
        fn inspect_auto_increment(
            &self,
            _expected: &MySqlAutoIncrementState,
            _mapping: &MySqlTableMapping,
        ) -> ConnectionResult<MySqlAutoIncrementTargetState> {
            Ok(*self.state.lock().unwrap())
        }

        fn restore_auto_increment(
            &self,
            _expected: &MySqlAutoIncrementState,
            _mapping: &MySqlTableMapping,
        ) -> ConnectionResult<()> {
            *self.state.lock().unwrap() = MySqlAutoIncrementTargetState::Exact;
            Ok(())
        }
    }

    impl FakeTarget {
        fn new(state: MySqlTableState, behavior: CreateBehavior) -> Self {
            Self {
                state: Mutex::new(state),
                behavior: Mutex::new(behavior),
            }
        }
    }

    impl MySqlDdlTarget for FakeTarget {
        fn inspect(&self, _expected: &MySqlTableDefinition) -> ConnectionResult<MySqlTableState> {
            Ok(*self.state.lock().unwrap())
        }

        fn create(&self, _expected: &MySqlTableDefinition) -> ConnectionResult<()> {
            match *self.behavior.lock().unwrap() {
                CreateBehavior::Success => {
                    *self.state.lock().unwrap() = MySqlTableState::Exact;
                    Ok(())
                }
                CreateBehavior::FailBeforeEffect => {
                    Err(ConnectionError::Database("injected before effect".into()))
                }
                CreateBehavior::FailAfterEffect => {
                    *self.state.lock().unwrap() = MySqlTableState::Exact;
                    Err(ConnectionError::Database("injected after effect".into()))
                }
                CreateBehavior::Different => {
                    *self.state.lock().unwrap() = MySqlTableState::Different;
                    Err(ConnectionError::Database(
                        "injected different effect".into(),
                    ))
                }
            }
        }
    }

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).unwrap()
    }

    fn empty_batch() -> RowBatch {
        RowBatch::new(
            vec![ColumnMeta {
                name: identifier("id"),
                ordinal: 1,
                vendor_type: "bigint unsigned".into(),
                nullable: false,
                collation: None,
                precision: None,
                scale: None,
                timezone_semantics: None,
            }],
            1,
            1024,
        )
    }

    fn one_row_batch(value: u64) -> RowBatch {
        let mut batch = empty_batch();
        batch
            .try_push(vec![DbValue::Unsigned(value.into())], 8)
            .unwrap();
        batch
    }

    fn table_definition() -> (QualifiedTable, MySqlTableDefinition, MySqlTableMapping) {
        let source = QualifiedTable {
            namespace: identifier("source_db"),
            name: identifier("items"),
        };
        let target = QualifiedTable {
            namespace: identifier("target_db"),
            name: identifier("items"),
        };
        let definition = MySqlTableDefinition {
            table: target.clone(),
            engine: "InnoDB".into(),
            character_set: "utf8mb4".into(),
            collation: "utf8mb4_0900_bin".into(),
            columns: vec![MySqlColumnDefinition {
                name: identifier("id"),
                ordinal: 1,
                data_type: MySqlColumnType::Integer {
                    name: "bigint".into(),
                    unsigned: true,
                    display_width: None,
                },
                nullable: false,
                character_set: None,
                collation: None,
                auto_increment: false,
            }],
            indexes: vec![MySqlIndexDefinition {
                name: identifier("PRIMARY"),
                primary: true,
                unique: true,
                constraint_backed: true,
                columns: vec![identifier("id")],
            }],
        };
        let mapping = MySqlTableMapping {
            source: source.clone(),
            target,
        };
        (source, definition, mapping)
    }

    fn catalog(database: &str) -> VendorCatalog {
        VendorCatalog {
            format_version: MYSQL_CATALOG_FORMAT_VERSION,
            dialect: "mysql".into(),
            server_version: "8.4.0".into(),
            database: identifier(database),
            namespaces: vec![CatalogNamespace {
                id: format!("schema:{database}"),
                name: identifier(database),
                owner: None,
                charset: Some("utf8mb4".into()),
                collation: Some("utf8mb4_0900_bin".into()),
                objects: Vec::new(),
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::from([
                ("information_schema_stats_expiry".into(), "0".into()),
                ("lower_case_table_names".into(), "0".into()),
            ]),
        }
    }

    fn fingerprint(catalog: &VendorCatalog) -> String {
        hex::encode(Sha256::digest(serde_json::to_vec(catalog).unwrap()))
    }

    fn endpoint_config(database: &str) -> MySqlEndpointConfig {
        MySqlEndpointConfig {
            host: database.into(),
            port: 3306,
            database: database.into(),
            user: "migration".into(),
            credential_env: format!("{}_PASSWORD", database.to_uppercase()),
            operational_server_administrators: Vec::new(),
            tls: MySqlTlsConfig::default(),
            connect_timeout_seconds: 10,
            max_batch_rows: 100,
            max_batch_bytes: 1024 * 1024,
        }
    }

    fn visibility_evidence(
        endpoint_identity: &str,
        database: &str,
        server_uuid: &str,
        reader_user: &str,
        reader_tls_binding: &str,
        reader_catalog_fingerprint: &str,
        authoritative_catalog_fingerprint: &str,
    ) -> MySqlMetadataVisibilityEvidence {
        let reader = MySqlAccountIdentity {
            user: reader_user.into(),
            host: "%".into(),
        };
        let administrator = MySqlAccountIdentity {
            user: format!("{reader_user}_metadata_admin"),
            host: "%".into(),
        };
        let mut records = ["EVENT", "SELECT", "SHOW VIEW", "TRIGGER"]
            .into_iter()
            .map(|privilege| MySqlGrantRecord::StaticGlobal {
                account: administrator.clone(),
                privilege: privilege.into(),
            })
            .chain(std::iter::once(MySqlGrantRecord::DynamicGlobal {
                account: administrator.clone(),
                privilege: "SHOW_ROUTINE".into(),
                grantable: false,
            }))
            .collect::<Vec<_>>();
        records.sort();
        let mut accounts = vec![reader.clone(), administrator.clone()];
        accounts.sort();
        let grant_inventory = MySqlGrantInventory {
            partial_revokes_enabled: true,
            grant_table_columns: vec![MySqlGrantTableColumn {
                table: "user".into(),
                column: "Host".into(),
                data_type: "char".into(),
            }],
            accounts,
            records,
            unknown_privilege_classes: Vec::new(),
        };
        let grant_inventory_digest = grant_inventory.canonical_hash().unwrap();
        MySqlMetadataVisibilityEvidence {
            schema_version: MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
            endpoint_identity: endpoint_identity.into(),
            database_identity: database.into(),
            server_uuid: server_uuid.into(),
            catalog_reader_tls_binding: reader_tls_binding.into(),
            metadata_administrator_tls_binding: "metadata-admin-tls".into(),
            catalog_reader_account: reader.clone(),
            metadata_administrator_account: administrator.clone(),
            active_administrator_roles: Vec::new(),
            effective_administrator_privileges: vec![
                "EVENT".into(),
                "SELECT".into(),
                "SHOW VIEW".into(),
                "SHOW_ROUTINE".into(),
                "TRIGGER".into(),
            ],
            operational_exclusions: vec![
                MySqlOperationalAccountExclusion {
                    purpose: MySqlOperationalAccountPurpose::CatalogReader,
                    account: reader,
                },
                MySqlOperationalAccountExclusion {
                    purpose: MySqlOperationalAccountPurpose::MetadataAdministrator,
                    account: administrator,
                },
            ],
            catalog_reader_fingerprint: reader_catalog_fingerprint.into(),
            authoritative_catalog_fingerprint: authoritative_catalog_fingerprint.into(),
            grant_inventory_digest,
            grant_inventory,
        }
    }

    fn reviewed_plan() -> (ReviewedPlan, String) {
        let source_config = endpoint_config("source_db");
        let target_config = endpoint_config("target_db");
        let source_catalog = catalog("source_db");
        let target_catalog = catalog("target_db");
        let source_fingerprint = fingerprint(&source_catalog);
        let target_fingerprint = fingerprint(&target_catalog);
        let source_tls_binding = mysql_tls_binding(&source_config).unwrap();
        let target_tls_binding = mysql_tls_binding(&target_config).unwrap();
        let source_visibility = visibility_evidence(
            "mysql://source/source_db",
            "source_db",
            "server-uuid",
            "source",
            &source_tls_binding,
            &source_fingerprint,
            &source_fingerprint,
        );
        let target_visibility = visibility_evidence(
            "mysql://target/target_db",
            "target_db",
            "target-server-uuid",
            "target",
            &target_tls_binding,
            &target_fingerprint,
            &target_fingerprint,
        );
        let (source_table, definition, mapping) = table_definition();
        let operation = PlanOperation::new(
            OperationKind::CreateTable,
            Some(source_table),
            Vec::new(),
            BTreeMap::from([
                (
                    "mysql_table_definition".into(),
                    serde_json::to_value(definition).unwrap(),
                ),
                (
                    "mysql_table_mapping".into(),
                    serde_json::to_value(mapping).unwrap(),
                ),
            ]),
        )
        .unwrap();
        let operation_id = operation.id.to_string();
        let reviewed = ReviewedPlan::new(MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            purpose: PlanPurpose::Execution,
            migration_id: "mysql-ddl-test".into(),
            tool_version: env!("CARGO_PKG_VERSION").into(),
            source_endpoint_identity: "mysql://source/source_db".into(),
            target_endpoint_identity: AssessmentStatus::Assessed("mysql://target/target_db".into()),
            source_catalog_fingerprint: source_fingerprint.clone(),
            target_catalog_fingerprint: AssessmentStatus::Assessed(target_fingerprint.clone()),
            source_catalog: Some(source_catalog),
            target_catalog: AssessmentStatus::Assessed(target_catalog),
            source_tls_binding,
            target_tls_binding: AssessmentStatus::Assessed(target_tls_binding),
            consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
            conversion_policy: "mysql_same_dialect_exact".into(),
            outage_policy: None,
            postgres_source_profile: None,
            mysql_source_profile: Some(
                crate::migration::mysql_profile::MySqlFreezeProfileContract::external_continuous_freeze(),
            ),
            mysql_snapshot_evidence: Some(MySqlSnapshotEvidence {
                endpoint_identity: "mysql://source/source_db".into(),
                database_identity: "source_db".into(),
                server_uuid: "server-uuid".into(),
                server_version: "8.4.0".into(),
                authenticated_account: "source@%".into(),
                lifecycle_id: "lifecycle".into(),
                connection_id: 7,
                transaction_isolation: "REPEATABLE-READ".into(),
                transaction_read_only: true,
                session_time_zone: "+00:00".into(),
                catalog_snapshot_protected: false,
                information_schema_stats_expiry: 0,
                lower_case_table_names: 0,
                session_sql_mode: MYSQL_STRICT_SQL_MODE.into(),
                character_set_client: MYSQL_SESSION_CHARACTER_SET.into(),
                character_set_connection: MYSQL_SESSION_CHARACTER_SET.into(),
                character_set_results: MYSQL_SESSION_CHARACTER_SET.into(),
                collation_connection: MYSQL_SESSION_COLLATION.into(),
                gtid_executed_observation: String::new(),
                catalog_fingerprint: source_fingerprint,
            }),
            mysql_target_snapshot_evidence: Some(MySqlSnapshotEvidence {
                endpoint_identity: "mysql://target/target_db".into(),
                database_identity: "target_db".into(),
                server_uuid: "target-server-uuid".into(),
                server_version: "8.4.0".into(),
                authenticated_account: "target@%".into(),
                lifecycle_id: "target-lifecycle".into(),
                connection_id: 8,
                transaction_isolation: "REPEATABLE-READ".into(),
                transaction_read_only: true,
                session_time_zone: "+00:00".into(),
                catalog_snapshot_protected: false,
                information_schema_stats_expiry: 0,
                lower_case_table_names: 0,
                session_sql_mode: MYSQL_STRICT_SQL_MODE.into(),
                character_set_client: MYSQL_SESSION_CHARACTER_SET.into(),
                character_set_connection: MYSQL_SESSION_CHARACTER_SET.into(),
                character_set_results: MYSQL_SESSION_CHARACTER_SET.into(),
                collation_connection: MYSQL_SESSION_COLLATION.into(),
                gtid_executed_observation: String::new(),
                catalog_fingerprint: target_fingerprint,
            }),
            mysql_metadata_visibility: Some(source_visibility),
            mysql_target_metadata_visibility: Some(target_visibility),
            capabilities: BTreeMap::new(),
            operations: vec![operation],
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap();
        (reviewed, operation_id)
    }

    #[test]
    fn factories_must_match_the_reviewed_endpoint_and_catalog_bindings() {
        let (reviewed, _) = reviewed_plan();
        let source = MySqlSourceFactory::new(endpoint_config("source_db"));
        source.validate_reviewed_binding(&reviewed).unwrap();

        let source_catalog = reviewed.plan.source_catalog.clone().unwrap();
        let target_evidence = reviewed
            .plan
            .mysql_target_snapshot_evidence
            .clone()
            .unwrap();
        let target = MySqlTargetFactory::new(
            endpoint_config("target_db"),
            source_catalog.clone(),
            target_evidence.clone(),
        )
        .unwrap();
        target.validate_reviewed_binding(&reviewed).unwrap();

        let wrong_source = MySqlSourceFactory::new(endpoint_config("source_clone"));
        assert!(wrong_source.validate_reviewed_binding(&reviewed).is_err());

        let mut wrong_target_evidence = target_evidence;
        wrong_target_evidence.server_uuid = "clone-server-uuid".into();
        let wrong_target = MySqlTargetFactory::new(
            endpoint_config("target_db"),
            source_catalog,
            wrong_target_evidence,
        )
        .unwrap();
        assert!(wrong_target.validate_reviewed_binding(&reviewed).is_err());
    }

    #[test]
    fn fresh_snapshot_binding_rejects_a_catalog_identical_source_clone() {
        let (reviewed, _) = reviewed_plan();
        let expected = reviewed.plan.mysql_snapshot_evidence.as_ref().unwrap();
        let mut fresh = expected.clone();
        fresh.lifecycle_id = "fresh-lifecycle".into();
        fresh.connection_id = 99;
        assert!(mysql_execution_snapshot_binding_is_exact(&fresh, expected));

        fresh.server_uuid = "clone-server-uuid".into();
        assert!(!mysql_execution_snapshot_binding_is_exact(&fresh, expected));
    }

    #[test]
    fn execution_requires_five_distinct_credential_references() {
        let source = endpoint_config("source");
        let source_metadata = endpoint_config("source_metadata");
        let freeze = endpoint_config("freeze");
        let target = endpoint_config("target");
        let mut target_metadata = endpoint_config("target_metadata");
        assert!(validate_mysql_execution_credential_separation(
            &source,
            &source_metadata,
            &freeze,
            &target,
            &target_metadata,
        )
        .is_ok());

        target_metadata.credential_env = freeze.credential_env.clone();
        assert!(validate_mysql_execution_credential_separation(
            &source,
            &source_metadata,
            &freeze,
            &target,
            &target_metadata,
        )
        .is_err());
    }

    fn auto_increment_reviewed_plan() -> (ReviewedPlan, MySqlAutoIncrementContract, String) {
        let (base, _) = reviewed_plan();
        let (source, _, mapping) = table_definition();
        let state = MySqlAutoIncrementState {
            table: source.clone(),
            column: identifier("id"),
            next_value: Some(42),
            stats_expiry: 0,
        };
        let restore = PlanOperation::new(
            OperationKind::Vendor("restore_mysql_auto_increment".into()),
            Some(source.clone()),
            Vec::new(),
            BTreeMap::from([
                (
                    "mysql_auto_increment_state".into(),
                    serde_json::to_value(&state).unwrap(),
                ),
                (
                    "mysql_table_mapping".into(),
                    serde_json::to_value(&mapping).unwrap(),
                ),
            ]),
        )
        .unwrap();
        let verify_schema = PlanOperation::new(
            OperationKind::VerifySchema,
            None,
            vec![restore.id.clone()],
            BTreeMap::new(),
        )
        .unwrap();
        let restore_id = restore.id.to_string();
        let verify_schema_id = verify_schema.id.to_string();
        let mut plan = base.plan;
        plan.operations = vec![restore, verify_schema];
        let reviewed = ReviewedPlan::new(plan).unwrap();
        (
            reviewed,
            MySqlAutoIncrementContract {
                operation_id: restore_id,
                state,
                mapping,
            },
            verify_schema_id,
        )
    }

    #[test]
    fn auto_increment_drift_in_verifying_requires_manual_reconciliation() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, contract, _) = auto_increment_reviewed_plan();
        let mut journal = journal(&directory.path().join("auto-drift.journal"), reviewed);
        complete_effect_operation(&mut journal, &contract.operation_id).unwrap();
        journal
            .transition_status(MigrationStatus::Verifying)
            .unwrap();
        let target = FakeAutoIncrementTarget {
            state: Mutex::new(MySqlAutoIncrementTargetState::Different),
        };

        assert!(verify_mysql_auto_increment_target(&target, &mut journal, &[contract]).is_err());
        assert_eq!(
            journal.projection().status,
            MigrationStatus::ManualReconciliationRequired
        );
    }

    #[test]
    fn completed_auto_increment_drift_is_rejected_without_mutating_the_terminal_journal() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, contract, verify_schema_id) = auto_increment_reviewed_plan();
        let source_fingerprint = reviewed.plan.source_catalog_fingerprint.clone();
        let mut journal = journal(&directory.path().join("completed-drift.journal"), reviewed);
        complete_effect_operation(&mut journal, &contract.operation_id).unwrap();
        journal
            .transition_status(MigrationStatus::Verifying)
            .unwrap();
        journal.verify_schema(source_fingerprint).unwrap();
        complete_effect_operation(&mut journal, &verify_schema_id).unwrap();
        journal
            .transition_status(MigrationStatus::Completed)
            .unwrap();
        let target = FakeAutoIncrementTarget {
            state: Mutex::new(MySqlAutoIncrementTargetState::Different),
        };

        assert!(verify_mysql_auto_increment_target(&target, &mut journal, &[contract]).is_err());
        assert_eq!(journal.projection().status, MigrationStatus::Completed);
    }

    fn copy_reviewed_plan() -> (ReviewedPlan, MySqlCopyContract, String) {
        let (base, _) = reviewed_plan();
        let (source, _, mapping) = table_definition();
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(source.clone()),
            Vec::new(),
            BTreeMap::from([
                (
                    "mysql_table_mapping".into(),
                    serde_json::to_value(&mapping).unwrap(),
                ),
                (
                    "mysql_write_policy".into(),
                    serde_json::json!("plain_insert_transaction_v1"),
                ),
            ]),
        )
        .unwrap();
        let verify_table = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(source.clone()),
            vec![copy.id.clone()],
            BTreeMap::new(),
        )
        .unwrap();
        let verify_schema = PlanOperation::new(
            OperationKind::VerifySchema,
            None,
            vec![verify_table.id.clone()],
            BTreeMap::new(),
        )
        .unwrap();
        let verify_schema_id = verify_schema.id.to_string();
        let contract = MySqlCopyContract {
            operation_id: copy.id.to_string(),
            source,
            target: mapping.target,
            projection: vec![identifier("id")],
            key: vec![identifier("id")],
            key_indexes: vec![0],
        };
        let mut plan = base.plan;
        plan.operations = vec![copy, verify_table, verify_schema];
        (ReviewedPlan::new(plan).unwrap(), contract, verify_schema_id)
    }

    fn fake_reader(batch: RowBatch) -> FakeReader {
        FakeReader {
            token: SnapshotToken {
                endpoint_identity: "mysql://source/source_db".into(),
                database_identity: "source_db".into(),
                snapshot_id: "snapshot".into(),
                consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
                server_version: "8.4.0".into(),
                lifecycle_id: "lifecycle".into(),
            },
            evidence: ReadOnlyEvidence {
                server_enforced: true,
                description: "test".into(),
            },
            first_page: batch,
        }
    }

    #[test]
    fn completed_table_data_is_reverified_against_current_target_rows() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, contract, verify_schema_id) = copy_reviewed_plan();
        let source_fingerprint = reviewed.plan.source_catalog_fingerprint.clone();
        let source_batch = one_row_batch(1);
        let digest = batch_digest(&contract.source, &contract, &source_batch).unwrap();
        let mut journal = journal(
            &directory.path().join("completed-rows.journal"),
            reviewed.clone(),
        );
        journal
            .transition_operation(&contract.operation_id, OperationState::Running)
            .unwrap();
        journal
            .prepare_chunk(PreparedChunk {
                chunk_id: 1,
                operation_id: contract.operation_id.clone(),
                start_key: None,
                final_key: vec![DbValue::Unsigned(1)],
                row_count: 1,
                canonical_digest: digest,
                target_transaction_intent: "intent-1".into(),
            })
            .unwrap();
        journal.commit_chunk_after_ack().unwrap();
        complete_effect_operation(&mut journal, &contract.operation_id).unwrap();
        journal
            .transition_status(MigrationStatus::Verifying)
            .unwrap();
        let matching_target = FakeVerificationTarget {
            first_page: source_batch.clone(),
        };
        verify_mysql_tables(
            &reviewed,
            &mut fake_reader(source_batch.clone()),
            &matching_target,
            &mut journal,
            &CancellationToken::default(),
            std::slice::from_ref(&contract),
        )
        .unwrap();
        journal.verify_schema(source_fingerprint).unwrap();
        complete_effect_operation(&mut journal, &verify_schema_id).unwrap();
        journal
            .transition_status(MigrationStatus::Completed)
            .unwrap();

        let drifted_target = FakeVerificationTarget {
            first_page: one_row_batch(2),
        };
        assert!(verify_mysql_tables(
            &reviewed,
            &mut fake_reader(source_batch),
            &drifted_target,
            &mut journal,
            &CancellationToken::default(),
            &[contract],
        )
        .is_err());
        assert_eq!(journal.projection().status, MigrationStatus::Completed);
    }

    fn journal(path: &std::path::Path, reviewed: ReviewedPlan) -> AppendJournal {
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
        let freeze = MySqlExternalFreezeAttestation {
            schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
            profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
            status: MySqlFreezeAttestationStatus::Active,
            source_endpoint_identity: reviewed.plan.source_endpoint_identity.clone(),
            source_database_identity: "source_db".into(),
            source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
            server_uuid: "server-uuid".into(),
            server_start_lower_bound_unix_seconds: 99,
            server_start_upper_bound_unix_seconds: 101,
            administrator_tls_binding: "admin-tls".into(),
            profile_generation: "generation-1".into(),
            provider_reference: "test-provider".into(),
            activated_at_unix_seconds: 10,
            expires_at_unix_seconds: 30,
            continuity_token_hash: "c".repeat(64),
            backup_lock_connection_id: 42,
            backup_lock_owner_thread_id: 23,
            backup_lock_owner_user: "freeze_owner".into(),
            backup_lock_owner_host: "localhost".into(),
            read_only: true,
            super_read_only: true,
            super_read_only_persisted: true,
            active_replication_channels: Vec::new(),
            dml_mechanism: MySqlDmlFreezeMechanism::PersistedSuperReadOnly,
            ddl_mechanism: MySqlDdlFreezeMechanism::ExternalBackupLock,
            freeze_enforced_by_tool: false,
            attested_at_unix_seconds: 20,
        };
        let binding = ResumeBinding {
            migration_id: reviewed.plan.migration_id.clone(),
            plan_hash: reviewed.plan_hash.to_string(),
            approval_reference: "approval".into(),
            tool_version: reviewed.plan.tool_version.clone(),
            source_endpoint: reviewed.plan.source_endpoint_identity.clone(),
            target_endpoint: reviewed
                .plan
                .target_endpoint_identity
                .as_assessed()
                .unwrap()
                .clone(),
            consistency_evidence: ConsistencyEvidence::MySqlExternalFreeze {
                endpoint_identity: reviewed.plan.source_endpoint_identity.clone(),
                database_identity: "source_db".into(),
                server_uuid: freeze.server_uuid.clone(),
                source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
                profile_generation: freeze.profile_generation.clone(),
                continuity_token_hash: freeze.continuity_token_hash.clone(),
                backup_lock_connection_id: freeze.backup_lock_connection_id,
            },
            source_schema_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
            target_schema_fingerprint: reviewed
                .plan
                .target_catalog_fingerprint
                .as_assessed()
                .unwrap()
                .clone(),
            outage_projection_digest: None,
            external_quiesce_attestation_digest: None,
            mysql_freeze_attestation_digest: Some(freeze.canonical_hash().unwrap()),
            conversion_policy: reviewed.plan.conversion_policy.clone(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        };
        AppendJournal::create_new(
            path,
            Genesis {
                binding,
                reviewed_plan: reviewed,
                accepted_outage_projection: None,
                accepted_external_quiesce: None,
                accepted_mysql_freeze: Some(freeze),
                operations,
            },
        )
        .unwrap()
    }

    #[test]
    fn implicit_ddl_ack_loss_is_reconciled_to_the_same_verified_operation() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, operation_id) = reviewed_plan();
        let mut journal = journal(&directory.path().join("state.journal"), reviewed.clone());
        let target = FakeTarget::new(MySqlTableState::Absent, CreateBehavior::FailAfterEffect);

        reconcile_mysql_pre_data_schema_with(
            &reviewed,
            &target,
            &mut journal,
            &CancellationToken::default(),
            &mut || Ok(()),
            None,
        )
        .unwrap();

        assert_eq!(
            journal.projection().operations.get(&operation_id),
            Some(&OperationState::Verified)
        );
    }

    #[test]
    fn prepared_absent_ddl_retries_but_different_effect_stops_manual() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, operation_id) = reviewed_plan();
        let mut retry_journal = journal(&directory.path().join("retry.journal"), reviewed.clone());
        retry_journal
            .prepare_operations_atomic([operation_id.as_str()])
            .unwrap();
        let retry = FakeTarget::new(MySqlTableState::Absent, CreateBehavior::Success);
        reconcile_mysql_pre_data_schema_with(
            &reviewed,
            &retry,
            &mut retry_journal,
            &CancellationToken::default(),
            &mut || Ok(()),
            None,
        )
        .unwrap();
        assert_eq!(
            retry_journal.projection().operations.get(&operation_id),
            Some(&OperationState::Verified)
        );

        let (reviewed, _) = reviewed_plan();
        let mut conflict = journal(&directory.path().join("conflict.journal"), reviewed.clone());
        let target = FakeTarget::new(MySqlTableState::Absent, CreateBehavior::Different);
        assert!(reconcile_mysql_pre_data_schema_with(
            &reviewed,
            &target,
            &mut conflict,
            &CancellationToken::default(),
            &mut || Ok(()),
            None,
        )
        .is_err());
        assert_eq!(
            conflict.projection().status,
            MigrationStatus::ManualReconciliationRequired
        );
    }

    #[test]
    fn absent_effect_error_leaves_the_intent_prepared_for_resume() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let (reviewed, operation_id) = reviewed_plan();
        let mut journal = journal(&directory.path().join("absent.journal"), reviewed.clone());
        let target = FakeTarget::new(MySqlTableState::Absent, CreateBehavior::FailBeforeEffect);
        assert!(reconcile_mysql_pre_data_schema_with(
            &reviewed,
            &target,
            &mut journal,
            &CancellationToken::default(),
            &mut || Ok(()),
            None,
        )
        .is_err());
        assert_eq!(
            journal.projection().operations.get(&operation_id),
            Some(&OperationState::Prepared)
        );
        assert_eq!(journal.projection().status, MigrationStatus::Running);
    }
}
