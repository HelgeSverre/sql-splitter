//! Durable migration state and fail-closed resume validation.

use serde::{Deserialize, Serialize};

use super::model::DbValue;

pub const STATE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeBinding {
    pub migration_id: String,
    pub plan_hash: String,
    pub approval_reference: String,
    pub tool_version: String,
    pub source_endpoint: String,
    pub target_endpoint: String,
    pub snapshot_identity: String,
    pub source_schema_fingerprint: String,
    pub target_schema_fingerprint: String,
    pub conversion_policy: String,
    pub canonical_encoding_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationState {
    pub state_schema_version: u16,
    pub binding: ResumeBinding,
    pub chunks: Vec<ChunkRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalError {
    BindingMismatch { field: &'static str },
    UnsupportedStateVersion { found: u16 },
    DuplicateChunk { chunk_id: u64 },
    UnknownChunk { chunk_id: u64 },
    AlreadyCommitted { chunk_id: u64 },
    NonMonotonicChunk { previous: u64, next: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedResolution {
    MarkedCommitted,
    RetryRequired,
    ManualReconciliationRequired,
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
            chunks: Vec::new(),
        }
    }

    pub fn validate_resume(&self, expected: &ResumeBinding) -> Result<(), JournalError> {
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
        check!(snapshot_identity);
        check!(source_schema_fingerprint);
        check!(target_schema_fingerprint);
        check!(conversion_policy);
        check!(canonical_encoding_version);
        Ok(())
    }

    pub fn prepare(&mut self, chunk: ChunkRecord) -> Result<(), JournalError> {
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
        Ok(())
    }

    pub fn commit(&mut self, chunk_id: u64) -> Result<(), JournalError> {
        let chunk = self
            .chunks
            .iter_mut()
            .find(|item| item.chunk_id == chunk_id)
            .ok_or(JournalError::UnknownChunk { chunk_id })?;
        if chunk.state == ChunkState::Committed {
            return Err(JournalError::AlreadyCommitted { chunk_id });
        }
        chunk.state = ChunkState::Committed;
        Ok(())
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
            snapshot_identity: "x".into(),
            source_schema_fingerprint: "sf".into(),
            target_schema_fingerprint: "tf".into(),
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
            canonical_digest: "d".into(),
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
        changed.snapshot_identity = "other".into();
        let state = MigrationState::new(binding());
        assert_eq!(
            state.validate_resume(&changed),
            Err(JournalError::BindingMismatch {
                field: "snapshot_identity"
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
            state.reconcile_prepared(1, 1, "d").unwrap(),
            PreparedResolution::MarkedCommitted
        );
    }
}
