//! Durable migration state and fail-closed resume validation.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::model::{DbValue, KeyTuple};
use super::plan::ReviewedPlan;

pub const STATE_SCHEMA_VERSION: u16 = 7;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ConsistencyEvidence {
    NativeSnapshot {
        endpoint_identity: String,
        database_identity: String,
        lifecycle_id: String,
        snapshot_id: String,
        server_version: String,
    },
    WriteFence {
        generation: String,
        token_hash: String,
        endpoint_identity: String,
        database_oid: u32,
        system_identifier: String,
        business_catalog_fingerprint: String,
        fence_inventory_fingerprint: String,
        activation_xid: u64,
        activated_at: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeBinding {
    pub migration_id: String,
    pub plan_hash: String,
    pub approval_reference: String,
    pub tool_version: String,
    pub source_endpoint: String,
    pub target_endpoint: String,
    pub consistency_evidence: ConsistencyEvidence,
    pub source_schema_fingerprint: String,
    pub target_schema_fingerprint: String,
    pub outage_projection_digest: Option<String>,
    pub external_quiesce_attestation_digest: Option<String>,
    pub conversion_policy: String,
    pub canonical_encoding_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkRecord {
    pub chunk_id: u64,
    pub operation_id: String,
    pub start_key: Option<Vec<DbValue>>,
    pub final_key: Vec<DbValue>,
    pub row_count: u64,
    pub canonical_digest: String,
    pub target_transaction_intent: String,
    pub state: ChunkState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkState {
    Prepared,
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Running,
    Prepared,
    Committed,
    Verified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRecord {
    pub operation_id: String,
    pub dependencies: Vec<String>,
    pub state: OperationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    Running,
    Verifying,
    ManualReconciliationRequired,
    Cancelled,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationState {
    pub state_schema_version: u16,
    pub binding: ResumeBinding,
    pub reviewed_plan: Option<ReviewedPlan>,
    pub status: MigrationStatus,
    pub operations: Vec<OperationRecord>,
    pub chunks: Vec<ChunkRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    BindingMismatch {
        field: &'static str,
    },
    UnsupportedStateVersion {
        found: u16,
    },
    DuplicateChunk {
        chunk_id: u64,
    },
    UnknownChunk {
        chunk_id: u64,
    },
    AlreadyCommitted {
        chunk_id: u64,
    },
    NonMonotonicChunk {
        previous: u64,
        next: u64,
    },
    EmptyBindingField {
        field: &'static str,
    },
    InvalidBindingDigest {
        field: &'static str,
    },
    EmptyOperationId {
        chunk_id: u64,
    },
    EmptyFinalKey {
        chunk_id: u64,
    },
    ZeroRowChunk {
        chunk_id: u64,
    },
    InvalidDigest {
        chunk_id: u64,
    },
    EmptyTransactionIntent {
        chunk_id: u64,
    },
    DuplicateTransactionIntent {
        chunk_id: u64,
    },
    ChunkAfterPrepared {
        chunk_id: u64,
    },
    DiscontinuousInterval {
        chunk_id: u64,
    },
    UnknownOperation {
        chunk_id: u64,
        operation_id: String,
    },
    DuplicateOperation {
        operation_id: String,
    },
    UnknownOperationDependency {
        operation_id: String,
        dependency: String,
    },
    OperationDependencyIncomplete {
        operation_id: String,
        dependency: String,
    },
    InvalidOperationTransition {
        operation_id: String,
    },
    MultipleRunningOperations,
    CompletionGateFailed,
    InvalidMigrationStatus,
    PreparedChunkNotLast {
        chunk_id: u64,
    },
    PreparedEvidenceMismatch {
        chunk_id: u64,
    },
    ChunkOperationIncomplete {
        operation_id: String,
    },
    InvalidReviewedPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedResolution {
    MarkedCommitted,
    RetryRequired,
    ManualReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedChunkEvidence {
    pub chunk_id: u64,
    pub operation_id: String,
    pub start_key: Option<Vec<DbValue>>,
    pub final_key: Vec<DbValue>,
    pub row_count: u64,
    pub canonical_digest: String,
    pub target_transaction_intent: String,
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for JournalError {}

impl MigrationState {
    pub fn new(binding: ResumeBinding) -> Self {
        Self {
            state_schema_version: STATE_SCHEMA_VERSION,
            binding,
            reviewed_plan: None,
            status: MigrationStatus::Running,
            operations: Vec::new(),
            chunks: Vec::new(),
        }
    }

    pub fn with_operations(
        binding: ResumeBinding,
        operations: impl IntoIterator<Item = (String, Vec<String>)>,
    ) -> Result<Self, JournalError> {
        let mut state = Self::new(binding);
        state.operations = operations
            .into_iter()
            .map(|(operation_id, mut dependencies)| {
                dependencies.sort();
                dependencies.dedup();
                OperationRecord {
                    operation_id,
                    dependencies,
                    state: OperationState::Pending,
                }
            })
            .collect();
        state.validate_structure(None)?;
        Ok(state)
    }

    pub fn validate_resume(&self, expected: &ResumeBinding) -> Result<(), JournalError> {
        self.validate_structure(None)?;
        if self.state_schema_version != STATE_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedStateVersion {
                found: self.state_schema_version,
            });
        }
        macro_rules! check {
            ($field:ident) => {
                if self.binding.$field != expected.$field {
                    return Err(JournalError::BindingMismatch {
                        field: stringify!($field),
                    });
                }
            };
        }
        check!(migration_id);
        check!(plan_hash);
        check!(approval_reference);
        check!(tool_version);
        check!(source_endpoint);
        check!(target_endpoint);
        if self.binding.consistency_evidence != expected.consistency_evidence {
            return Err(JournalError::BindingMismatch {
                field: "consistency_evidence",
            });
        }
        check!(source_schema_fingerprint);
        check!(target_schema_fingerprint);
        check!(outage_projection_digest);
        check!(external_quiesce_attestation_digest);
        check!(conversion_policy);
        check!(canonical_encoding_version);
        Ok(())
    }

    pub fn bind_reviewed_plan(&mut self, plan: ReviewedPlan) -> Result<(), JournalError> {
        plan.validate()
            .map_err(|_| JournalError::InvalidReviewedPlan)?;
        if plan.plan_hash.to_string() != self.binding.plan_hash {
            return Err(JournalError::BindingMismatch { field: "plan_hash" });
        }
        self.reviewed_plan = Some(plan);
        Ok(())
    }

    pub fn validate_for_operations<'a>(
        &self,
        operation_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), JournalError> {
        let operation_ids = operation_ids.into_iter().collect::<BTreeSet<_>>();
        self.validate_structure(Some(&operation_ids))?;
        if !self.operations.is_empty()
            && self
                .operations
                .iter()
                .map(|operation| operation.operation_id.as_str())
                .collect::<BTreeSet<_>>()
                != operation_ids
        {
            return Err(JournalError::BindingMismatch {
                field: "operations",
            });
        }
        Ok(())
    }

    fn validate_structure(
        &self,
        operation_ids: Option<&BTreeSet<&str>>,
    ) -> Result<(), JournalError> {
        if self.state_schema_version != STATE_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedStateVersion {
                found: self.state_schema_version,
            });
        }
        if let Some(plan) = &self.reviewed_plan {
            plan.validate()
                .map_err(|_| JournalError::InvalidReviewedPlan)?;
            if plan.plan_hash.to_string() != self.binding.plan_hash {
                return Err(JournalError::BindingMismatch { field: "plan_hash" });
            }
        }
        for (field, value) in [
            ("migration_id", self.binding.migration_id.as_str()),
            ("plan_hash", self.binding.plan_hash.as_str()),
            (
                "approval_reference",
                self.binding.approval_reference.as_str(),
            ),
            ("tool_version", self.binding.tool_version.as_str()),
            ("source_endpoint", self.binding.source_endpoint.as_str()),
            ("target_endpoint", self.binding.target_endpoint.as_str()),
            (
                "source_schema_fingerprint",
                self.binding.source_schema_fingerprint.as_str(),
            ),
            (
                "target_schema_fingerprint",
                self.binding.target_schema_fingerprint.as_str(),
            ),
            ("conversion_policy", self.binding.conversion_policy.as_str()),
        ] {
            if value.is_empty() {
                return Err(JournalError::EmptyBindingField { field });
            }
        }
        if let Some(digest) = &self.binding.outage_projection_digest {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(JournalError::InvalidBindingDigest {
                    field: "outage_projection_digest",
                });
            }
        }
        if let Some(digest) = &self.binding.external_quiesce_attestation_digest {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(JournalError::InvalidBindingDigest {
                    field: "external_quiesce_attestation_digest",
                });
            }
        }
        let consistency_fields: Vec<(&'static str, &str)> = match &self.binding.consistency_evidence
        {
            ConsistencyEvidence::NativeSnapshot {
                endpoint_identity,
                database_identity,
                lifecycle_id,
                snapshot_id,
                server_version,
            } => vec![
                ("consistency_endpoint_identity", endpoint_identity),
                ("consistency_database_identity", database_identity),
                ("snapshot_lifecycle_id", lifecycle_id),
                ("snapshot_id", snapshot_id),
                ("snapshot_server_version", server_version),
            ],
            ConsistencyEvidence::WriteFence {
                generation,
                token_hash,
                endpoint_identity,
                system_identifier,
                business_catalog_fingerprint,
                fence_inventory_fingerprint,
                activated_at,
                ..
            } => vec![
                ("fence_generation", generation),
                ("fence_token_hash", token_hash),
                ("consistency_endpoint_identity", endpoint_identity),
                ("fence_system_identifier", system_identifier),
                (
                    "fence_business_catalog_fingerprint",
                    business_catalog_fingerprint,
                ),
                ("fence_inventory_fingerprint", fence_inventory_fingerprint),
                ("fence_activated_at", activated_at),
            ],
        };
        for (field, value) in consistency_fields {
            if value.is_empty() {
                return Err(JournalError::EmptyBindingField { field });
            }
        }
        let consistency_endpoint = match &self.binding.consistency_evidence {
            ConsistencyEvidence::NativeSnapshot {
                endpoint_identity, ..
            }
            | ConsistencyEvidence::WriteFence {
                endpoint_identity, ..
            } => endpoint_identity,
        };
        if consistency_endpoint != &self.binding.source_endpoint {
            return Err(JournalError::BindingMismatch {
                field: "consistency_endpoint_identity",
            });
        }

        let mut durable_operation_ids = BTreeSet::new();
        let mut running_operations = 0usize;
        for operation in &self.operations {
            if operation.operation_id.is_empty()
                || !durable_operation_ids.insert(operation.operation_id.as_str())
            {
                return Err(JournalError::DuplicateOperation {
                    operation_id: operation.operation_id.clone(),
                });
            }
            if operation.state == OperationState::Running {
                running_operations += 1;
            }
        }
        if running_operations > 1 {
            return Err(JournalError::MultipleRunningOperations);
        }
        for operation in &self.operations {
            for dependency in &operation.dependencies {
                if !durable_operation_ids.contains(dependency.as_str()) {
                    return Err(JournalError::UnknownOperationDependency {
                        operation_id: operation.operation_id.clone(),
                        dependency: dependency.clone(),
                    });
                }
                let dependency_state = self
                    .operations
                    .iter()
                    .find(|candidate| candidate.operation_id == *dependency)
                    .map(|candidate| &candidate.state);
                if operation.state != OperationState::Pending
                    && dependency_state != Some(&OperationState::Verified)
                    && !(operation.state == OperationState::Prepared
                        && dependency_state == Some(&OperationState::Prepared))
                {
                    return Err(JournalError::OperationDependencyIncomplete {
                        operation_id: operation.operation_id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        if self.status == MigrationStatus::Completed
            && self
                .operations
                .iter()
                .any(|operation| operation.state != OperationState::Verified)
        {
            return Err(JournalError::CompletionGateFailed);
        }

        let mut previous_id = None;
        let mut previous_by_operation = std::collections::BTreeMap::<&str, &[DbValue]>::new();
        let mut intents = BTreeSet::new();
        let mut saw_prepared = false;
        for chunk in &self.chunks {
            if let Some(previous) = previous_id {
                if chunk.chunk_id <= previous {
                    return Err(JournalError::NonMonotonicChunk {
                        previous,
                        next: chunk.chunk_id,
                    });
                }
            }
            previous_id = Some(chunk.chunk_id);
            if chunk.operation_id.is_empty() {
                return Err(JournalError::EmptyOperationId {
                    chunk_id: chunk.chunk_id,
                });
            }
            if let Some(ids) = operation_ids {
                if !ids.contains(chunk.operation_id.as_str()) {
                    return Err(JournalError::UnknownOperation {
                        chunk_id: chunk.chunk_id,
                        operation_id: chunk.operation_id.clone(),
                    });
                }
            }
            if !self.operations.is_empty()
                && !durable_operation_ids.contains(chunk.operation_id.as_str())
            {
                return Err(JournalError::UnknownOperation {
                    chunk_id: chunk.chunk_id,
                    operation_id: chunk.operation_id.clone(),
                });
            }
            if self
                .operations
                .iter()
                .find(|operation| operation.operation_id == chunk.operation_id)
                .is_some_and(|operation| operation.state == OperationState::Pending)
            {
                return Err(JournalError::InvalidOperationTransition {
                    operation_id: chunk.operation_id.clone(),
                });
            }
            if chunk.final_key.is_empty() {
                return Err(JournalError::EmptyFinalKey {
                    chunk_id: chunk.chunk_id,
                });
            }
            if chunk.row_count == 0 {
                return Err(JournalError::ZeroRowChunk {
                    chunk_id: chunk.chunk_id,
                });
            }
            if chunk.canonical_digest.len() != 64
                || !chunk
                    .canonical_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(JournalError::InvalidDigest {
                    chunk_id: chunk.chunk_id,
                });
            }
            if chunk.target_transaction_intent.is_empty() {
                return Err(JournalError::EmptyTransactionIntent {
                    chunk_id: chunk.chunk_id,
                });
            }
            if !intents.insert(chunk.target_transaction_intent.as_str()) {
                return Err(JournalError::DuplicateTransactionIntent {
                    chunk_id: chunk.chunk_id,
                });
            }
            if saw_prepared {
                return Err(JournalError::ChunkAfterPrepared {
                    chunk_id: chunk.chunk_id,
                });
            }
            saw_prepared = chunk.state == ChunkState::Prepared;
            if let Some(previous_final) = previous_by_operation.get(chunk.operation_id.as_str()) {
                if chunk.start_key.as_deref() != Some(*previous_final) {
                    return Err(JournalError::DiscontinuousInterval {
                        chunk_id: chunk.chunk_id,
                    });
                }
            } else if chunk.start_key.is_some() {
                return Err(JournalError::DiscontinuousInterval {
                    chunk_id: chunk.chunk_id,
                });
            }
            previous_by_operation.insert(chunk.operation_id.as_str(), &chunk.final_key);
        }
        Ok(())
    }

    pub fn start_operation(&mut self, operation_id: &str) -> Result<(), JournalError> {
        if !matches!(
            self.status,
            MigrationStatus::Running | MigrationStatus::Verifying
        ) {
            return Err(JournalError::InvalidMigrationStatus);
        }
        if self.operations.iter().any(|operation| {
            matches!(
                operation.state,
                OperationState::Running | OperationState::Prepared
            )
        }) {
            return Err(JournalError::MultipleRunningOperations);
        }
        let dependencies = self
            .operations
            .iter()
            .find(|operation| operation.operation_id == operation_id)
            .ok_or_else(|| JournalError::UnknownOperation {
                chunk_id: 0,
                operation_id: operation_id.into(),
            })?
            .dependencies
            .clone();
        for dependency in dependencies {
            if self
                .operations
                .iter()
                .find(|operation| operation.operation_id == dependency)
                .is_none_or(|operation| operation.state != OperationState::Verified)
            {
                return Err(JournalError::OperationDependencyIncomplete {
                    operation_id: operation_id.into(),
                    dependency,
                });
            }
        }
        let operation = self
            .operations
            .iter_mut()
            .find(|operation| operation.operation_id == operation_id)
            .ok_or_else(|| JournalError::UnknownOperation {
                chunk_id: 0,
                operation_id: operation_id.into(),
            })?;
        if operation.state != OperationState::Pending {
            return Err(JournalError::InvalidOperationTransition {
                operation_id: operation_id.into(),
            });
        }
        operation.state = OperationState::Running;
        Ok(())
    }

    pub fn commit_operation(&mut self, operation_id: &str) -> Result<(), JournalError> {
        if self
            .chunks
            .iter()
            .any(|chunk| chunk.operation_id == operation_id && chunk.state != ChunkState::Committed)
        {
            return Err(JournalError::ChunkOperationIncomplete {
                operation_id: operation_id.into(),
            });
        }
        self.transition_operation(
            operation_id,
            OperationState::Running,
            OperationState::Committed,
        )
    }

    pub fn prepare_operations_atomic<'a>(
        &mut self,
        operation_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), JournalError> {
        if !matches!(
            self.status,
            MigrationStatus::Running | MigrationStatus::Verifying
        ) || self.operations.iter().any(|operation| {
            matches!(
                operation.state,
                OperationState::Running | OperationState::Prepared
            )
        }) {
            return Err(JournalError::InvalidMigrationStatus);
        }
        let operation_ids = operation_ids.into_iter().collect::<BTreeSet<_>>();
        if operation_ids.is_empty() {
            return Ok(());
        }
        for operation_id in &operation_ids {
            let operation = self
                .operations
                .iter()
                .find(|operation| operation.operation_id == *operation_id)
                .ok_or_else(|| JournalError::UnknownOperation {
                    chunk_id: 0,
                    operation_id: (*operation_id).into(),
                })?;
            if operation.state != OperationState::Pending
                || operation.dependencies.iter().any(|dependency| {
                    !operation_ids.contains(dependency.as_str())
                        && self
                            .operations
                            .iter()
                            .find(|candidate| candidate.operation_id == *dependency)
                            .is_none_or(|candidate| candidate.state != OperationState::Verified)
                })
            {
                return Err(JournalError::InvalidOperationTransition {
                    operation_id: (*operation_id).into(),
                });
            }
        }
        for operation in &mut self.operations {
            if operation_ids.contains(operation.operation_id.as_str()) {
                operation.state = OperationState::Prepared;
            }
        }
        Ok(())
    }

    pub fn commit_prepared_operation(&mut self, operation_id: &str) -> Result<(), JournalError> {
        let dependencies = self
            .operations
            .iter()
            .find(|operation| operation.operation_id == operation_id)
            .ok_or_else(|| JournalError::UnknownOperation {
                chunk_id: 0,
                operation_id: operation_id.into(),
            })?
            .dependencies
            .clone();
        for dependency in dependencies {
            if self
                .operations
                .iter()
                .find(|operation| operation.operation_id == dependency)
                .is_none_or(|operation| operation.state != OperationState::Verified)
            {
                return Err(JournalError::OperationDependencyIncomplete {
                    operation_id: operation_id.into(),
                    dependency,
                });
            }
        }
        self.transition_operation(
            operation_id,
            OperationState::Prepared,
            OperationState::Committed,
        )
    }

    pub fn verify_operation(&mut self, operation_id: &str) -> Result<(), JournalError> {
        self.transition_operation(
            operation_id,
            OperationState::Committed,
            OperationState::Verified,
        )
    }

    fn transition_operation(
        &mut self,
        operation_id: &str,
        expected: OperationState,
        next: OperationState,
    ) -> Result<(), JournalError> {
        let operation = self
            .operations
            .iter_mut()
            .find(|operation| operation.operation_id == operation_id)
            .ok_or_else(|| JournalError::UnknownOperation {
                chunk_id: 0,
                operation_id: operation_id.into(),
            })?;
        if operation.state != expected {
            return Err(JournalError::InvalidOperationTransition {
                operation_id: operation_id.into(),
            });
        }
        operation.state = next;
        Ok(())
    }

    pub fn begin_verification(&mut self) -> Result<(), JournalError> {
        if self.status != MigrationStatus::Running
            || self.operations.iter().any(|operation| {
                matches!(
                    operation.state,
                    OperationState::Running | OperationState::Prepared
                )
            })
        {
            return Err(JournalError::CompletionGateFailed);
        }
        self.status = MigrationStatus::Verifying;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(), JournalError> {
        if self.status != MigrationStatus::Verifying
            || self
                .operations
                .iter()
                .any(|operation| operation.state != OperationState::Verified)
            || self
                .chunks
                .iter()
                .any(|chunk| chunk.state != ChunkState::Committed)
        {
            return Err(JournalError::CompletionGateFailed);
        }
        self.status = MigrationStatus::Completed;
        self.validate_structure(None)
    }

    pub fn require_manual_reconciliation(&mut self) -> Result<(), JournalError> {
        if self.status == MigrationStatus::Completed {
            return Err(JournalError::InvalidMigrationStatus);
        }
        self.status = MigrationStatus::ManualReconciliationRequired;
        Ok(())
    }

    pub fn prepare(&mut self, chunk: ChunkRecord) -> Result<(), JournalError> {
        if self.status != MigrationStatus::Running {
            return Err(JournalError::InvalidMigrationStatus);
        }
        if self
            .chunks
            .last()
            .is_some_and(|item| item.state == ChunkState::Prepared)
        {
            return Err(JournalError::ChunkAfterPrepared {
                chunk_id: chunk.chunk_id,
            });
        }
        if self
            .chunks
            .iter()
            .any(|item| item.chunk_id == chunk.chunk_id)
        {
            return Err(JournalError::DuplicateChunk {
                chunk_id: chunk.chunk_id,
            });
        }
        if let Some(previous) = self.chunks.last().map(|item| item.chunk_id) {
            if chunk.chunk_id <= previous {
                return Err(JournalError::NonMonotonicChunk {
                    previous,
                    next: chunk.chunk_id,
                });
            }
        }
        let mut chunk = chunk;
        chunk.state = ChunkState::Prepared;
        self.chunks.push(chunk);
        if let Err(error) = self.validate_structure(None) {
            self.chunks.pop();
            return Err(error);
        }
        Ok(())
    }

    pub fn commit(&mut self, chunk_id: u64) -> Result<(), JournalError> {
        let index = self
            .chunks
            .iter()
            .position(|item| item.chunk_id == chunk_id)
            .ok_or(JournalError::UnknownChunk { chunk_id })?;
        if index + 1 != self.chunks.len() {
            return Err(JournalError::PreparedChunkNotLast { chunk_id });
        }
        if self.chunks[index].state == ChunkState::Committed {
            return Err(JournalError::AlreadyCommitted { chunk_id });
        }
        self.chunks[index].state = ChunkState::Committed;
        if let Err(error) = self.validate_structure(None) {
            self.chunks[index].state = ChunkState::Prepared;
            return Err(error);
        }
        Ok(())
    }

    pub fn prepared_chunk(&self) -> Result<Option<&ChunkRecord>, JournalError> {
        self.validate_structure(None)?;
        Ok(self
            .chunks
            .last()
            .filter(|chunk| chunk.state == ChunkState::Prepared))
    }

    pub fn resume_cursor(&self, operation_id: &str) -> Result<Option<KeyTuple>, JournalError> {
        self.validate_structure(None)?;
        Ok(self
            .chunks
            .iter()
            .rev()
            .find(|chunk| {
                chunk.operation_id == operation_id && chunk.state == ChunkState::Committed
            })
            .map(|chunk| KeyTuple::new(chunk.final_key.clone())))
    }

    pub fn next_chunk_id(&self) -> Result<u64, JournalError> {
        self.validate_structure(None)?;
        self.chunks.last().map_or(Ok(1), |chunk| {
            chunk
                .chunk_id
                .checked_add(1)
                .ok_or(JournalError::NonMonotonicChunk {
                    previous: chunk.chunk_id,
                    next: 0,
                })
        })
    }

    /// Reconcile a durable prepared record after an ambiguous target commit.
    pub fn reconcile_prepared(
        &mut self,
        chunk_id: u64,
        observed_row_count: u64,
        observed_digest: &str,
    ) -> Result<PreparedResolution, JournalError> {
        let chunk = self
            .chunks
            .iter_mut()
            .find(|item| item.chunk_id == chunk_id)
            .ok_or(JournalError::UnknownChunk { chunk_id })?;
        if chunk.state == ChunkState::Committed {
            return Err(JournalError::AlreadyCommitted { chunk_id });
        }
        if observed_row_count == 0 {
            return Ok(PreparedResolution::RetryRequired);
        }
        if observed_row_count == chunk.row_count && observed_digest == chunk.canonical_digest {
            chunk.state = ChunkState::Committed;
            return Ok(PreparedResolution::MarkedCommitted);
        }
        Ok(PreparedResolution::ManualReconciliationRequired)
    }

    pub fn reconcile_prepared_evidence(
        &mut self,
        evidence: &PreparedChunkEvidence,
    ) -> Result<PreparedResolution, JournalError> {
        let chunk = self.prepared_chunk()?.ok_or(JournalError::UnknownChunk {
            chunk_id: evidence.chunk_id,
        })?;
        if chunk.chunk_id != evidence.chunk_id
            || chunk.operation_id != evidence.operation_id
            || chunk.start_key != evidence.start_key
            || chunk.final_key != evidence.final_key
            || chunk.target_transaction_intent != evidence.target_transaction_intent
        {
            return Err(JournalError::PreparedEvidenceMismatch {
                chunk_id: evidence.chunk_id,
            });
        }
        let resolution = self.reconcile_prepared(
            evidence.chunk_id,
            evidence.row_count,
            &evidence.canonical_digest,
        )?;
        if resolution == PreparedResolution::ManualReconciliationRequired {
            self.status = MigrationStatus::ManualReconciliationRequired;
        }
        Ok(resolution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn binding() -> ResumeBinding {
        ResumeBinding {
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
                snapshot_id: "x".into(),
                server_version: "fixture".into(),
            },
            source_schema_fingerprint: "sf".into(),
            target_schema_fingerprint: "tf".into(),
            outage_projection_digest: None,
            external_quiesce_attestation_digest: None,
            conversion_policy: "exact".into(),
            canonical_encoding_version: 1,
        }
    }
    fn chunk(id: u64) -> ChunkRecord {
        ChunkRecord {
            chunk_id: id,
            operation_id: "op".into(),
            start_key: None,
            final_key: vec![DbValue::Unsigned(id as u128)],
            row_count: 1,
            canonical_digest: "d".repeat(64),
            target_transaction_intent: "tx".into(),
            state: ChunkState::Prepared,
        }
    }
    #[test]
    fn transition_is_monotonic() {
        let mut state = MigrationState::new(binding());
        state.prepare(chunk(1)).unwrap();
        state.commit(1).unwrap();
        assert_eq!(
            state.commit(1),
            Err(JournalError::AlreadyCommitted { chunk_id: 1 })
        );
    }
    #[test]
    fn resume_rejects_drift() {
        let mut changed = binding();
        changed.consistency_evidence = ConsistencyEvidence::NativeSnapshot {
            endpoint_identity: "s".into(),
            database_identity: "db".into(),
            lifecycle_id: "other".into(),
            snapshot_id: "other".into(),
            server_version: "fixture".into(),
        };
        let state = MigrationState::new(binding());
        assert_eq!(
            state.validate_resume(&changed),
            Err(JournalError::BindingMismatch {
                field: "consistency_evidence"
            })
        );
    }

    #[test]
    fn prepared_reconciliation_fails_closed() {
        let mut state = MigrationState::new(binding());
        state.prepare(chunk(1)).unwrap();
        assert_eq!(
            state.reconcile_prepared(1, 1, "different").unwrap(),
            PreparedResolution::ManualReconciliationRequired
        );
        assert_eq!(
            state.reconcile_prepared(1, 0, "").unwrap(),
            PreparedResolution::RetryRequired
        );
        assert_eq!(
            state.reconcile_prepared(1, 1, &"d".repeat(64)).unwrap(),
            PreparedResolution::MarkedCommitted
        );
    }

    #[test]
    fn resume_rejects_malformed_chunk_structure() {
        let mut state = MigrationState::new(binding());
        let mut malformed = chunk(1);
        malformed.row_count = 0;
        state.chunks.push(malformed);
        assert_eq!(
            state.validate_resume(&binding()),
            Err(JournalError::ZeroRowChunk { chunk_id: 1 })
        );
    }

    #[test]
    fn resume_rejects_unknown_operation_and_interval_gap() {
        let mut state = MigrationState::new(binding());
        state.prepare(chunk(1)).unwrap();
        state.commit(1).unwrap();
        assert_eq!(
            state.validate_for_operations(["other"]),
            Err(JournalError::UnknownOperation {
                chunk_id: 1,
                operation_id: "op".into(),
            })
        );

        let mut next = chunk(2);
        next.start_key = Some(vec![DbValue::Unsigned(99)]);
        next.target_transaction_intent = "tx-2".into();
        assert_eq!(
            state.prepare(next),
            Err(JournalError::DiscontinuousInterval { chunk_id: 2 })
        );
    }

    #[test]
    fn operation_dependencies_and_completion_gate_are_monotonic() {
        let mut state = MigrationState::with_operations(
            binding(),
            [
                ("create".into(), Vec::new()),
                ("copy".into(), vec!["create".into()]),
            ],
        )
        .unwrap();
        assert!(matches!(
            state.start_operation("copy"),
            Err(JournalError::OperationDependencyIncomplete { .. })
        ));
        state.start_operation("create").unwrap();
        state.commit_operation("create").unwrap();
        state.verify_operation("create").unwrap();
        state.start_operation("copy").unwrap();
        state.commit_operation("copy").unwrap();
        state.begin_verification().unwrap();
        assert_eq!(state.finalize(), Err(JournalError::CompletionGateFailed));
        state.verify_operation("copy").unwrap();
        state.finalize().unwrap();
        assert_eq!(state.status, MigrationStatus::Completed);
    }

    #[test]
    fn recovery_cursor_and_prepared_evidence_are_exact() {
        let mut state =
            MigrationState::with_operations(binding(), [("op".to_owned(), Vec::new())]).unwrap();
        state.start_operation("op").unwrap();
        let first = chunk(1);
        state.prepare(first.clone()).unwrap();
        state.commit(1).unwrap();
        let mut second = chunk(2);
        second.start_key = Some(first.final_key.clone());
        second.target_transaction_intent = "tx-2".into();
        state.prepare(second.clone()).unwrap();

        assert_eq!(state.next_chunk_id().unwrap(), 3);
        assert_eq!(
            state.resume_cursor("op").unwrap(),
            Some(KeyTuple::new(first.final_key))
        );
        assert_eq!(state.prepared_chunk().unwrap(), Some(&second));

        let mut wrong = PreparedChunkEvidence {
            chunk_id: second.chunk_id,
            operation_id: second.operation_id.clone(),
            start_key: second.start_key.clone(),
            final_key: second.final_key.clone(),
            row_count: second.row_count,
            canonical_digest: second.canonical_digest.clone(),
            target_transaction_intent: "wrong-intent".into(),
        };
        assert!(matches!(
            state.reconcile_prepared_evidence(&wrong),
            Err(JournalError::PreparedEvidenceMismatch { chunk_id: 2 })
        ));
        wrong.target_transaction_intent = second.target_transaction_intent;
        wrong.canonical_digest = "different".into();
        assert_eq!(
            state.reconcile_prepared_evidence(&wrong).unwrap(),
            PreparedResolution::ManualReconciliationRequired
        );
        assert_eq!(state.status, MigrationStatus::ManualReconciliationRequired);
    }

    #[test]
    fn operation_cannot_commit_with_a_prepared_chunk() {
        let mut state =
            MigrationState::with_operations(binding(), [("op".to_owned(), Vec::new())]).unwrap();
        state.start_operation("op").unwrap();
        state.prepare(chunk(1)).unwrap();
        assert_eq!(
            state.commit_operation("op"),
            Err(JournalError::ChunkOperationIncomplete {
                operation_id: "op".into()
            })
        );
    }

    #[test]
    fn verification_operations_run_only_in_verification_status() {
        let mut state = MigrationState::with_operations(
            binding(),
            [
                ("copy".to_owned(), Vec::new()),
                ("verify".to_owned(), vec!["copy".to_owned()]),
            ],
        )
        .unwrap();
        state.start_operation("copy").unwrap();
        state.commit_operation("copy").unwrap();
        state.begin_verification().unwrap();
        state.verify_operation("copy").unwrap();
        state.start_operation("verify").unwrap();
        state.commit_operation("verify").unwrap();
        state.verify_operation("verify").unwrap();
        state.finalize().unwrap();
        assert_eq!(state.status, MigrationStatus::Completed);
    }

    #[test]
    fn atomic_operation_intent_is_durable_before_effect_commit() {
        let mut state = MigrationState::with_operations(
            binding(),
            [
                ("create-a".to_owned(), Vec::new()),
                ("create-b".to_owned(), vec!["create-a".to_owned()]),
            ],
        )
        .unwrap();
        state
            .prepare_operations_atomic(["create-a", "create-b"])
            .unwrap();
        assert!(state
            .operations
            .iter()
            .all(|operation| operation.state == OperationState::Prepared));
        assert!(matches!(
            state.start_operation("create-a"),
            Err(JournalError::InvalidMigrationStatus | JournalError::MultipleRunningOperations)
        ));
        assert!(matches!(
            state.commit_prepared_operation("create-b"),
            Err(JournalError::OperationDependencyIncomplete { .. })
        ));
        state.commit_prepared_operation("create-a").unwrap();
        state.verify_operation("create-a").unwrap();
        state.commit_prepared_operation("create-b").unwrap();
        state.verify_operation("create-b").unwrap();
    }

    #[test]
    fn atomic_operation_intent_is_allowed_during_verification() {
        let mut state = MigrationState::with_operations(
            binding(),
            [
                ("copy".to_owned(), Vec::new()),
                ("add-fk".to_owned(), vec!["copy".to_owned()]),
            ],
        )
        .unwrap();
        state.start_operation("copy").unwrap();
        state.commit_operation("copy").unwrap();
        state.begin_verification().unwrap();
        state.verify_operation("copy").unwrap();

        state.prepare_operations_atomic(["add-fk"]).unwrap();

        assert_eq!(
            state
                .operations
                .iter()
                .find(|operation| operation.operation_id == "add-fk")
                .unwrap()
                .state,
            OperationState::Prepared
        );
    }

    #[test]
    fn write_fence_evidence_is_typed_and_endpoint_bound() {
        let mut binding = binding();
        binding.consistency_evidence = ConsistencyEvidence::WriteFence {
            generation: "generation-1".into(),
            token_hash: "a".repeat(64),
            endpoint_identity: binding.source_endpoint.clone(),
            database_oid: 42,
            system_identifier: "system-1".into(),
            business_catalog_fingerprint: "b".repeat(64),
            fence_inventory_fingerprint: "c".repeat(64),
            activation_xid: 100,
            activated_at: "2026-08-11T00:00:00Z".into(),
        };
        let state = MigrationState::new(binding.clone());
        state.validate_resume(&binding).unwrap();

        let mut changed = binding.clone();
        if let ConsistencyEvidence::WriteFence { generation, .. } =
            &mut changed.consistency_evidence
        {
            *generation = "generation-2".into();
        }
        assert!(matches!(
            state.validate_resume(&changed),
            Err(JournalError::BindingMismatch {
                field: "consistency_evidence"
            })
        ));

        let mut wrong_endpoint = binding;
        if let ConsistencyEvidence::WriteFence {
            endpoint_identity, ..
        } = &mut wrong_endpoint.consistency_evidence
        {
            *endpoint_identity = "other".into();
        }
        assert!(matches!(
            MigrationState::new(wrong_endpoint).validate_for_operations([]),
            Err(JournalError::BindingMismatch {
                field: "consistency_endpoint_identity"
            })
        ));
    }
}
