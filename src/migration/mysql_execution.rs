//! Durable MySQL target-effect reconciliation.
//!
//! MySQL DDL implicitly commits. Each create-only statement therefore owns one
//! journal intent and is inspected independently after any error or restart.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Context};
use sha2::{Digest, Sha256};

use super::append_journal::{AppendJournal, CommittedChunkIter, PreparedChunk};
use super::canonical::{digest_rows, CanonicalRow};
use super::connection::{
    CancellationToken, ConnectionError, ConnectionResult, KeysetPage, ReadSession,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession,
};
use super::journal::{MigrationStatus, OperationState};
use super::model::{DbValue, Identifier, KeyTuple, QualifiedTable, RowBatch, VendorCatalog};
use super::mysql::{
    mysql_auto_increment_states, mysql_table_definitions,
    validate_mysql_external_freeze_continuity, MySqlAutoIncrementState,
    MySqlAutoIncrementTargetState, MySqlResumableKey, MySqlSourceFactory, MySqlTableDefinition,
    MySqlTableMapping, MySqlTableState, MySqlTargetFactory,
};
use super::mysql_profile::MySqlExternalFreezeAttestation;
use super::plan::{OperationKind, PlanOperation, ReviewedPlan};

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

/// Reconcile every typed create-table statement against its durable journal
/// state. This function never drops, replaces, or alters an observed object.
pub fn reconcile_mysql_pre_data_schema(
    reviewed: &ReviewedPlan,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
) -> anyhow::Result<()> {
    reconcile_mysql_pre_data_schema_with(reviewed, target, journal, cancellation, &mut || Ok(()))
}

fn reconcile_mysql_pre_data_schema_with<G>(
    reviewed: &ReviewedPlan,
    target: &impl MySqlDdlTarget,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    before_effect: &mut G,
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

/// Execute or resume the reviewed MySQL plan while continuously re-attesting
/// the externally owned source freeze. The journal must have been created with
/// the same reviewed plan and accepted freeze attestation.
pub fn execute_mysql_frozen_plan<F>(
    reviewed: &ReviewedPlan,
    source: &MySqlSourceFactory,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    mut attest: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> anyhow::Result<MySqlExternalFreezeAttestation>,
{
    reviewed
        .validate()
        .context("validate reviewed MySQL plan hash")?;
    reviewed
        .plan
        .validate_for_execution()
        .context("validate MySQL plan for execution")?;
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
        cancellation.check()?;
        Ok(())
    };

    match journal.projection().status {
        MigrationStatus::ManualReconciliationRequired | MigrationStatus::Cancelled => {
            return Err(anyhow!(
                "MySQL migration journal is in a terminal non-success state"
            ));
        }
        MigrationStatus::Completed => {
            require_freeze()?;
            target.assert_exact_schema()?;
            return Ok(());
        }
        MigrationStatus::Running | MigrationStatus::Verifying => {}
    }

    require_freeze()?;
    let (mut reader, initial_catalog) = capture_exact_source(reviewed, source, cancellation)?;
    let copy_contracts = mysql_copy_contracts(reviewed, &initial_catalog)?;
    let auto_increment_contracts = mysql_auto_increment_contracts(reviewed)?;

    if journal.projection().status == MigrationStatus::Running {
        reconcile_mysql_pre_data_schema_with(
            reviewed,
            target,
            journal,
            cancellation,
            &mut require_freeze,
        )?;
        copy_mysql_tables(
            reviewed,
            reader.as_mut(),
            target,
            journal,
            cancellation,
            &copy_contracts,
            &mut require_freeze,
        )?;
    }
    drop(reader);

    require_freeze()?;
    let (mut verification_reader, final_catalog) =
        capture_exact_source(reviewed, source, cancellation)?;
    verify_auto_increment_source_equality(reviewed, &final_catalog)?;
    if journal.projection().status == MigrationStatus::Running {
        reconcile_mysql_auto_increment(
            target,
            journal,
            cancellation,
            &auto_increment_contracts,
            &mut require_freeze,
        )?;
        journal.transition_status(MigrationStatus::Verifying)?;
    }

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
) -> anyhow::Result<(Box<dyn ReadSession>, VendorCatalog)> {
    cancellation.check()?;
    let snapshot = source.capture_snapshot()?;
    let (catalog, blockers, fingerprint) = source.captured_catalog(&snapshot)?;
    if !blockers.is_empty()
        || fingerprint != reviewed.plan.source_catalog_fingerprint
        || reviewed.plan.source_catalog.as_ref() != Some(&catalog)
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
    Ok((reader, catalog))
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

fn copy_mysql_tables<G>(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlCopyContract],
    before_effect: &mut G,
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
                write_mysql_chunk(
                    target,
                    journal,
                    cancellation,
                    contract,
                    &batch,
                    before_effect,
                )?;
                after = Some(final_key);
            }
        }
        complete_effect_operation(journal, &contract.operation_id)?;
    }
    Ok(())
}

fn reconcile_prepared_mysql_chunk<G>(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contract: &MySqlCopyContract,
    before_effect: &mut G,
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
    match writer.commit() {
        Ok(()) => journal.commit_chunk_after_ack().map_err(Into::into),
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
    }
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
    let mut verifier = target.open_verifier(cancellation.clone())?;
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
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlAutoIncrementContract],
    before_effect: &mut G,
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

fn verify_mysql_tables(
    reviewed: &ReviewedPlan,
    reader: &mut dyn ReadSession,
    target: &MySqlTargetFactory,
    journal: &mut AppendJournal,
    cancellation: &CancellationToken,
    contracts: &[MySqlCopyContract],
) -> anyhow::Result<()> {
    if journal.projection().status != MigrationStatus::Verifying {
        return Err(anyhow!(
            "MySQL table verification is outside Verifying state"
        ));
    }
    let mut verifier = target.open_verifier(cancellation.clone())?;
    let mut chunks = journal.all_committed_chunks()?.peekable();
    for contract in contracts {
        let evidence = verify_mysql_table(reader, verifier.as_mut(), &mut chunks, contract)?;
        match journal.table_verification_evidence(&contract.operation_id) {
            Some(stored) if stored == &evidence => {}
            Some(_) => {
                return Err(anyhow!(
                    "fresh MySQL table verification differs from durable evidence"
                ));
            }
            None => journal.verify_table(
                &contract.operation_id,
                evidence.0.clone(),
                evidence.1.clone(),
                evidence.2.clone(),
            )?,
        }
        let verify_operation = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| {
                operation.kind == OperationKind::VerifyTable
                    && operation.table.as_ref() == Some(&contract.source)
            })
            .ok_or_else(|| anyhow!("MySQL copy table has no verification operation"))?;
        complete_effect_operation(journal, verify_operation.id.as_str())?;
    }
    if chunks.next().transpose()?.is_some() {
        return Err(anyhow!(
            "MySQL journal contains committed chunks outside the reviewed table order"
        ));
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
    use crate::migration::connection::ConnectionError;
    use crate::migration::journal::{ConsistencyEvidence, MigrationStatus, ResumeBinding};
    use crate::migration::model::{CatalogNamespace, Identifier, QualifiedTable, VendorCatalog};
    use crate::migration::mysql::{
        MySqlColumnDefinition, MySqlColumnType, MySqlIndexDefinition, MYSQL_CATALOG_FORMAT_VERSION,
        MYSQL_CONSISTENCY_SNAPSHOT,
    };
    use crate::migration::mysql_profile::{
        MySqlDdlFreezeMechanism, MySqlDmlFreezeMechanism, MySqlExternalFreezeAttestation,
        MySqlFreezeAttestationStatus, MySqlFreezeProfileKind,
        MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
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

    fn reviewed_plan() -> (ReviewedPlan, String) {
        let source_catalog = catalog("source_db");
        let target_catalog = catalog("target_db");
        let source_fingerprint = fingerprint(&source_catalog);
        let target_fingerprint = fingerprint(&target_catalog);
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
            source_tls_binding: "source-tls".into(),
            target_tls_binding: AssessmentStatus::Assessed("target-tls".into()),
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
            capabilities: BTreeMap::new(),
            operations: vec![operation],
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap();
        (reviewed, operation_id)
    }

    fn journal(path: &std::path::Path, reviewed: ReviewedPlan) -> AppendJournal {
        let operation_id = reviewed.plan.operations[0].id.to_string();
        let freeze = MySqlExternalFreezeAttestation {
            schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
            profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
            status: MySqlFreezeAttestationStatus::Active,
            source_endpoint_identity: reviewed.plan.source_endpoint_identity.clone(),
            source_database_identity: "source_db".into(),
            source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
            server_uuid: "server-uuid".into(),
            administrator_tls_binding: "admin-tls".into(),
            profile_generation: "generation-1".into(),
            provider_reference: "test-provider".into(),
            activated_at_unix_seconds: 10,
            expires_at_unix_seconds: 30,
            continuity_token_hash: "c".repeat(64),
            backup_lock_connection_id: 42,
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
                operations: vec![OperationSpec {
                    operation_id,
                    dependencies: Vec::new(),
                    is_copy: false,
                    phase: OperationPhase::Execution,
                }],
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
        )
        .is_err());
        assert_eq!(
            journal.projection().operations.get(&operation_id),
            Some(&OperationState::Prepared)
        );
        assert_eq!(journal.projection().status, MigrationStatus::Running);
    }
}
