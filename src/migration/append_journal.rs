//! Secure append-only migration journal.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::journal::{MigrationStatus, OperationState, ResumeBinding};
use super::model::DbValue;
use super::plan::{OperationKind, ReviewedPlan};

const FILE_MAGIC: &[u8; 8] = b"SSJNL001";
const FRAME_MAGIC: u32 = 0x534a_4631;
const FORMAT_VERSION: u16 = 1;
const FILE_HEADER_LEN: u64 = 10;
const FRAME_HEADER_LEN: usize = 84;
const FRAME_TRAILER_LEN: usize = 32;
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum AppendJournalError {
    #[error("journal I/O failed")]
    Io(#[from] io::Error),
    #[error("journal JSON failed")]
    Json(#[from] serde_json::Error),
    #[error("unsafe journal path: {0}")]
    UnsafePath(PathBuf),
    #[error("journal has unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("journal frame is corrupt at byte {offset}")]
    CorruptFrame { offset: u64 },
    #[error("journal payload exceeds the configured bound")]
    PayloadTooLarge,
    #[error("journal sequence overflow")]
    SequenceOverflow,
    #[error("journal transition is invalid: {0}")]
    InvalidTransition(&'static str),
    #[error("journal plan or binding is invalid")]
    InvalidGenesis,
    #[error("journal already exists: {0}")]
    AlreadyExists(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationSpec {
    pub operation_id: String,
    pub dependencies: Vec<String>,
    pub is_copy: bool,
    pub phase: OperationPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationPhase {
    Execution,
    Verification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Genesis {
    pub binding: ResumeBinding,
    pub reviewed_plan: ReviewedPlan,
    pub operations: Vec<OperationSpec>,
}

impl Genesis {
    fn validate(&self) -> Result<(), AppendJournalError> {
        self.reviewed_plan
            .validate()
            .map_err(|_| AppendJournalError::InvalidGenesis)?;
        if self.binding.plan_hash != self.reviewed_plan.plan_hash.to_string() {
            return Err(AppendJournalError::InvalidGenesis);
        }
        let plan_ids = self
            .reviewed_plan
            .plan
            .operations
            .iter()
            .map(|operation| operation.id.as_str())
            .collect::<BTreeSet<_>>();
        let graph_ids = self
            .operations
            .iter()
            .map(|operation| operation.operation_id.as_str())
            .collect::<BTreeSet<_>>();
        if graph_ids.len() != self.operations.len() || graph_ids != plan_ids {
            return Err(AppendJournalError::InvalidGenesis);
        }
        for operation in &self.operations {
            if operation.operation_id.is_empty()
                || operation
                    .dependencies
                    .iter()
                    .any(|dependency| !graph_ids.contains(dependency.as_str()))
            {
                return Err(AppendJournalError::InvalidGenesis);
            }
        }
        let plan_operations = self
            .reviewed_plan
            .plan
            .operations
            .iter()
            .map(|operation| (operation.id.as_str(), operation))
            .collect::<BTreeMap<_, _>>();
        for operation in &self.operations {
            let planned = plan_operations
                .get(operation.operation_id.as_str())
                .ok_or(AppendJournalError::InvalidGenesis)?;
            let dependencies = planned
                .dependencies
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            if operation.dependencies != dependencies
                || operation.is_copy != (planned.kind == OperationKind::CopyTable)
                || operation.phase != operation_phase(&planned.kind)
            {
                return Err(AppendJournalError::InvalidGenesis);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedChunk {
    pub chunk_id: u64,
    pub operation_id: String,
    pub start_key: Option<Vec<DbValue>>,
    pub final_key: Vec<DbValue>,
    pub row_count: u64,
    pub canonical_digest: String,
    pub target_transaction_intent: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopyCursor {
    pub last_chunk_id: u64,
    pub final_key: Vec<DbValue>,
    pub chunks: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum JournalEvent {
    Genesis(Box<Genesis>),
    MigrationStatus {
        from: MigrationStatus,
        to: MigrationStatus,
    },
    OperationTransition {
        operation_id: String,
        from: OperationState,
        to: OperationState,
    },
    OperationsPreparedAtomic {
        operation_ids: Vec<String>,
    },
    ChunkPrepared(PreparedChunk),
    ChunkCommitted {
        chunk_id: u64,
        operation_id: String,
        target_transaction_intent: String,
    },
    ManualReconciliationRequired,
    TableVerificationComplete {
        operation_id: String,
        chunk_count: u64,
        row_count: u64,
        manifest_hash: String,
        source_hash: String,
        target_hash: String,
    },
    SchemaVerificationComplete {
        catalog_fingerprint: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalProjection {
    pub status: MigrationStatus,
    pub operations: BTreeMap<String, OperationState>,
    pub prepared_chunk: Option<PreparedChunk>,
    pub last_chunk_id: u64,
    pub copy_cursors: BTreeMap<String, CopyCursor>,
    pub table_verifications: BTreeSet<String>,
    pub schema_verified: bool,
    dependencies: BTreeMap<String, Vec<String>>,
    copy_operations: BTreeSet<String>,
    phases: BTreeMap<String, OperationPhase>,
}

impl JournalProjection {
    fn from_genesis(genesis: &Genesis) -> Self {
        Self {
            status: MigrationStatus::Running,
            operations: genesis
                .operations
                .iter()
                .map(|operation| (operation.operation_id.clone(), OperationState::Pending))
                .collect(),
            prepared_chunk: None,
            last_chunk_id: 0,
            copy_cursors: BTreeMap::new(),
            table_verifications: BTreeSet::new(),
            schema_verified: false,
            dependencies: genesis
                .operations
                .iter()
                .map(|operation| {
                    (
                        operation.operation_id.clone(),
                        operation.dependencies.clone(),
                    )
                })
                .collect(),
            copy_operations: genesis
                .operations
                .iter()
                .filter(|operation| operation.is_copy)
                .map(|operation| operation.operation_id.clone())
                .collect(),
            phases: genesis
                .operations
                .iter()
                .map(|operation| (operation.operation_id.clone(), operation.phase))
                .collect(),
        }
    }

    fn apply(&mut self, event: &JournalEvent) -> Result<(), AppendJournalError> {
        if matches!(
            self.status,
            MigrationStatus::ManualReconciliationRequired
                | MigrationStatus::Cancelled
                | MigrationStatus::Completed
        ) {
            return Err(AppendJournalError::InvalidTransition(
                "migration status is terminal",
            ));
        }
        match event {
            JournalEvent::Genesis(_) => {
                return Err(AppendJournalError::InvalidTransition("duplicate genesis"));
            }
            JournalEvent::MigrationStatus { from, to } => {
                if &self.status != from || !valid_status_transition(*from, *to) {
                    return Err(AppendJournalError::InvalidTransition("migration status"));
                }
                if *to == MigrationStatus::Verifying
                    && (self.prepared_chunk.is_some()
                        || self.operations.iter().any(|(operation_id, state)| {
                            self.phases.get(operation_id) == Some(&OperationPhase::Execution)
                                && *state != OperationState::Verified
                        }))
                {
                    return Err(AppendJournalError::InvalidTransition(
                        "verification started with incomplete writes",
                    ));
                }
                if *to == MigrationStatus::Completed
                    && (!self.schema_verified
                        || self.prepared_chunk.is_some()
                        || self.table_verifications.len() != self.copy_operations.len()
                        || self
                            .operations
                            .values()
                            .any(|state| *state != OperationState::Verified))
                {
                    return Err(AppendJournalError::InvalidTransition("completion gate"));
                }
                self.status = *to;
            }
            JournalEvent::OperationTransition {
                operation_id,
                from,
                to,
            } => {
                let phase = self
                    .phases
                    .get(operation_id)
                    .ok_or(AppendJournalError::InvalidTransition("unknown operation"))?;
                if (self.status == MigrationStatus::Running && *phase != OperationPhase::Execution)
                    || (self.status == MigrationStatus::Verifying
                        && *phase != OperationPhase::Verification)
                {
                    return Err(AppendJournalError::InvalidTransition("operation phase"));
                }
                let current = self
                    .operations
                    .get(operation_id)
                    .ok_or(AppendJournalError::InvalidTransition("unknown operation"))?;
                if current != from || !valid_operation_transition(*from, *to) {
                    return Err(AppendJournalError::InvalidTransition("operation state"));
                }
                if *from == OperationState::Pending {
                    let dependencies = self
                        .dependencies
                        .get(operation_id)
                        .ok_or(AppendJournalError::InvalidTransition("unknown operation"))?;
                    if dependencies.iter().any(|dependency| {
                        self.operations.get(dependency) != Some(&OperationState::Verified)
                    }) {
                        return Err(AppendJournalError::InvalidTransition(
                            "operation dependency incomplete",
                        ));
                    }
                }
                self.operations.insert(operation_id.clone(), *to);
            }
            JournalEvent::OperationsPreparedAtomic { operation_ids } => {
                self.apply_operations_prepared_atomic(operation_ids)?;
            }
            JournalEvent::ChunkPrepared(chunk) => {
                if self.status != MigrationStatus::Running || self.schema_verified {
                    return Err(AppendJournalError::InvalidTransition("chunk phase"));
                }
                self.apply_chunk_prepared(chunk)?;
            }
            JournalEvent::ChunkCommitted {
                chunk_id,
                operation_id,
                target_transaction_intent,
            } => {
                if self.status != MigrationStatus::Running || self.schema_verified {
                    return Err(AppendJournalError::InvalidTransition("chunk phase"));
                }
                self.apply_chunk_committed(*chunk_id, operation_id, target_transaction_intent)?;
            }
            JournalEvent::ManualReconciliationRequired => {
                self.status = MigrationStatus::ManualReconciliationRequired;
            }
            JournalEvent::TableVerificationComplete {
                operation_id,
                chunk_count,
                row_count,
                manifest_hash,
                source_hash,
                target_hash,
            } => {
                if self.status != MigrationStatus::Verifying
                    || !self.copy_operations.contains(operation_id)
                    || self.operations.get(operation_id) != Some(&OperationState::Verified)
                    || self.schema_verified
                {
                    return Err(AppendJournalError::InvalidTransition(
                        "table verification outside verifying state",
                    ));
                }
                let cursor = self.copy_cursors.get(operation_id);
                let expected_chunks = cursor.map_or(0, |cursor| cursor.chunks);
                let expected_rows = cursor.map_or(0, |cursor| cursor.rows);
                if expected_chunks != *chunk_count
                    || expected_rows != *row_count
                    || manifest_hash.len() != 64
                    || source_hash.len() != 64
                    || target_hash.len() != 64
                    || source_hash != target_hash
                    || !self.table_verifications.insert(operation_id.clone())
                {
                    return Err(AppendJournalError::InvalidTransition("table verification"));
                }
            }
            JournalEvent::SchemaVerificationComplete {
                catalog_fingerprint,
            } => {
                if self.status != MigrationStatus::Verifying
                    || catalog_fingerprint.len() != 64
                    || self.schema_verified
                    || self.table_verifications.len() != self.copy_operations.len()
                {
                    return Err(AppendJournalError::InvalidTransition("schema verification"));
                }
                self.schema_verified = true;
            }
        }
        Ok(())
    }

    fn apply_chunk_prepared(&mut self, chunk: &PreparedChunk) -> Result<(), AppendJournalError> {
        if self.prepared_chunk.is_some()
            || !self.copy_operations.contains(&chunk.operation_id)
            || !matches!(
                self.operations.get(&chunk.operation_id),
                Some(OperationState::Running | OperationState::Prepared)
            )
            || chunk.chunk_id != self.last_chunk_id.saturating_add(1)
            || chunk.row_count == 0
            || chunk.final_key.is_empty()
            || chunk.canonical_digest.len() != 64
            || chunk.target_transaction_intent.is_empty()
        {
            return Err(AppendJournalError::InvalidTransition("chunk prepare"));
        }
        if let Some(cursor) = self.copy_cursors.get(&chunk.operation_id) {
            if chunk.start_key.as_ref() != Some(&cursor.final_key) {
                return Err(AppendJournalError::InvalidTransition("chunk continuity"));
            }
        } else if chunk.start_key.is_some() {
            return Err(AppendJournalError::InvalidTransition("first chunk"));
        }
        self.prepared_chunk = Some(chunk.clone());
        Ok(())
    }

    fn apply_operations_prepared_atomic(
        &mut self,
        operation_ids: &[String],
    ) -> Result<(), AppendJournalError> {
        if self.status != MigrationStatus::Running
            || self.prepared_chunk.is_some()
            || self.operations.values().any(|state| {
                matches!(state, OperationState::Running | OperationState::Prepared)
            })
        {
            return Err(AppendJournalError::InvalidTransition(
                "atomic operation prepare phase",
            ));
        }
        let group = operation_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if group.is_empty() || group.len() != operation_ids.len() {
            return Err(AppendJournalError::InvalidTransition(
                "atomic operation prepare set",
            ));
        }
        for operation_id in &group {
            if self.operations.get(*operation_id) != Some(&OperationState::Pending)
                || self.phases.get(*operation_id) != Some(&OperationPhase::Execution)
            {
                return Err(AppendJournalError::InvalidTransition(
                    "atomic operation prepare state",
                ));
            }
            let dependencies = self
                .dependencies
                .get(*operation_id)
                .ok_or(AppendJournalError::InvalidTransition("unknown operation"))?;
            if dependencies.iter().any(|dependency| {
                !group.contains(dependency.as_str())
                    && self.operations.get(dependency) != Some(&OperationState::Verified)
            }) {
                return Err(AppendJournalError::InvalidTransition(
                    "atomic operation dependency incomplete",
                ));
            }
        }
        for operation_id in group {
            self.operations
                .insert(operation_id.to_owned(), OperationState::Prepared);
        }
        Ok(())
    }

    fn apply_chunk_committed(
        &mut self,
        chunk_id: u64,
        operation_id: &str,
        intent: &str,
    ) -> Result<(), AppendJournalError> {
        let chunk = self
            .prepared_chunk
            .take()
            .ok_or(AppendJournalError::InvalidTransition("no prepared chunk"))?;
        if chunk.chunk_id != chunk_id
            || chunk.operation_id != operation_id
            || chunk.target_transaction_intent != intent
        {
            self.prepared_chunk = Some(chunk);
            return Err(AppendJournalError::InvalidTransition(
                "chunk commit evidence",
            ));
        }
        let previous = self.copy_cursors.get(operation_id);
        let chunks = previous.map_or(1, |cursor| cursor.chunks.saturating_add(1));
        let rows = previous.map_or(chunk.row_count, |cursor| {
            cursor.rows.saturating_add(chunk.row_count)
        });
        self.copy_cursors.insert(
            operation_id.to_owned(),
            CopyCursor {
                last_chunk_id: chunk_id,
                final_key: chunk.final_key,
                chunks,
                rows,
            },
        );
        self.last_chunk_id = chunk_id;
        Ok(())
    }
}

fn operation_phase(kind: &OperationKind) -> OperationPhase {
    match kind {
        OperationKind::VerifyTable | OperationKind::VerifySchema => OperationPhase::Verification,
        OperationKind::Vendor(name) if name == "verify_postgres_partition_topology" => {
            OperationPhase::Verification
        }
        OperationKind::CreateNamespace
        | OperationKind::CreateTable
        | OperationKind::CreateSequence
        | OperationKind::CopyTable
        | OperationKind::CreateIndex
        | OperationKind::CheckForeignKey
        | OperationKind::AddForeignKey
        | OperationKind::CreateView
        | OperationKind::CreateRoutine
        | OperationKind::CreateTrigger
        | OperationKind::Vendor(_) => OperationPhase::Execution,
    }
}

pub struct AppendJournal {
    path: PathBuf,
    file: File,
    genesis: Genesis,
    projection: JournalProjection,
    next_sequence: u64,
    head_hash: [u8; 32],
}

impl std::fmt::Debug for AppendJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppendJournal")
            .field("path", &self.path)
            .field("next_sequence", &self.next_sequence)
            .finish_non_exhaustive()
    }
}

impl AppendJournal {
    pub fn create_new(
        path: impl AsRef<Path>,
        genesis: Genesis,
    ) -> Result<Self, AppendJournalError> {
        genesis.validate()?;
        let path = path.as_ref();
        validate_parent(path)?;
        let mut options = OpenOptions::new();
        options.read(true).append(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(path).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                AppendJournalError::AlreadyExists(path.to_path_buf())
            } else {
                AppendJournalError::Io(error)
            }
        })?;
        validate_open_file(path, &file)?;
        lock_exclusive(&file)?;
        file.write_all(FILE_MAGIC)?;
        file.write_all(&FORMAT_VERSION.to_le_bytes())?;
        file.sync_all()?;
        sync_parent(path)?;
        let projection = JournalProjection::from_genesis(&genesis);
        let mut journal = Self {
            path: path.to_path_buf(),
            file,
            genesis: genesis.clone(),
            projection,
            next_sequence: 1,
            head_hash: [0; 32],
        };
        journal.append_event(&JournalEvent::Genesis(Box::new(genesis)))?;
        Ok(journal)
    }

    pub fn open_resume(path: impl AsRef<Path>) -> Result<Self, AppendJournalError> {
        let path = path.as_ref();
        validate_parent(path)?;
        let mut options = OpenOptions::new();
        options.read(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(path)?;
        validate_open_file(path, &file)?;
        lock_exclusive(&file)?;
        validate_file_header(&mut file)?;
        let scan = scan_frames(&mut file, true)?;
        if scan.valid_end < file.metadata()?.len() {
            file.set_len(scan.valid_end)?;
            file.sync_all()?;
            sync_parent(path)?;
        }
        let genesis = scan.genesis.ok_or(AppendJournalError::InvalidGenesis)?;
        let projection = scan.projection.ok_or(AppendJournalError::InvalidGenesis)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            genesis,
            projection,
            next_sequence: scan
                .last_sequence
                .checked_add(1)
                .ok_or(AppendJournalError::SequenceOverflow)?,
            head_hash: scan.head_hash,
        })
    }

    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    pub fn projection(&self) -> &JournalProjection {
        &self.projection
    }

    pub fn transition_operation(
        &mut self,
        operation_id: &str,
        to: OperationState,
    ) -> Result<(), AppendJournalError> {
        let from = *self
            .projection
            .operations
            .get(operation_id)
            .ok_or(AppendJournalError::InvalidTransition("unknown operation"))?;
        self.append_validated(JournalEvent::OperationTransition {
            operation_id: operation_id.to_owned(),
            from,
            to,
        })
    }

    pub fn transition_status(&mut self, to: MigrationStatus) -> Result<(), AppendJournalError> {
        self.append_validated(JournalEvent::MigrationStatus {
            from: self.projection.status,
            to,
        })
    }

    /// Durably prepares one transactional group before any group effect is sent.
    pub fn prepare_operations_atomic<'a>(
        &mut self,
        operation_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), AppendJournalError> {
        self.append_validated(JournalEvent::OperationsPreparedAtomic {
            operation_ids: operation_ids.into_iter().map(str::to_owned).collect(),
        })
    }

    /// Durably records intent. Call this before starting the target write.
    pub fn prepare_chunk(&mut self, chunk: PreparedChunk) -> Result<(), AppendJournalError> {
        self.append_validated(JournalEvent::ChunkPrepared(chunk))
    }

    /// Durably records a commit acknowledgement for the prepared chunk.
    pub fn commit_chunk_after_ack(&mut self) -> Result<(), AppendJournalError> {
        let chunk = self
            .projection
            .prepared_chunk
            .as_ref()
            .ok_or(AppendJournalError::InvalidTransition("no prepared chunk"))?;
        self.append_validated(JournalEvent::ChunkCommitted {
            chunk_id: chunk.chunk_id,
            operation_id: chunk.operation_id.clone(),
            target_transaction_intent: chunk.target_transaction_intent.clone(),
        })
    }

    pub fn require_manual_reconciliation(&mut self) -> Result<(), AppendJournalError> {
        self.append_validated(JournalEvent::ManualReconciliationRequired)
    }

    pub fn verify_table(
        &mut self,
        operation_id: &str,
        manifest_hash: String,
        source_hash: String,
        target_hash: String,
    ) -> Result<(), AppendJournalError> {
        if !self.projection.copy_operations.contains(operation_id) {
            return Err(AppendJournalError::InvalidTransition(
                "unknown copy operation",
            ));
        }
        let cursor = self.projection.copy_cursors.get(operation_id);
        self.append_validated(JournalEvent::TableVerificationComplete {
            operation_id: operation_id.to_owned(),
            chunk_count: cursor.map_or(0, |cursor| cursor.chunks),
            row_count: cursor.map_or(0, |cursor| cursor.rows),
            manifest_hash,
            source_hash,
            target_hash,
        })
    }

    pub fn verify_schema(&mut self, catalog_fingerprint: String) -> Result<(), AppendJournalError> {
        self.append_validated(JournalEvent::SchemaVerificationComplete {
            catalog_fingerprint,
        })
    }

    pub fn committed_chunks(
        &self,
        operation_id: impl Into<String>,
    ) -> Result<CommittedChunkIter, AppendJournalError> {
        let file = self.file.try_clone()?;
        let snapshot_end = self.file.metadata()?.len();
        Ok(CommittedChunkIter {
            file,
            operation_id: operation_id.into(),
            previous_hash: [0; 32],
            expected_sequence: 1,
            pending: None,
            target_cursor: None,
            last_global_chunk_id: 0,
            position: FILE_HEADER_LEN,
            snapshot_end,
            snapshot_head: self.head_hash,
            done: false,
        })
    }

    fn append_validated(&mut self, event: JournalEvent) -> Result<(), AppendJournalError> {
        let mut next = self.projection.clone();
        next.apply(&event)?;
        self.append_event(&event)?;
        self.projection = next;
        Ok(())
    }

    fn append_event(&mut self, event: &JournalEvent) -> Result<(), AppendJournalError> {
        let frame = encode_event_frame(self.next_sequence, self.head_hash, event)?;
        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        self.head_hash.copy_from_slice(&frame[frame.len() - 32..]);
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(AppendJournalError::SequenceOverflow)?;
        Ok(())
    }
}

pub struct CommittedChunkIter {
    file: File,
    operation_id: String,
    previous_hash: [u8; 32],
    expected_sequence: u64,
    pending: Option<PreparedChunk>,
    target_cursor: Option<CopyCursor>,
    last_global_chunk_id: u64,
    position: u64,
    snapshot_end: u64,
    snapshot_head: [u8; 32],
    done: bool,
}

impl Iterator for CommittedChunkIter {
    type Item = Result<PreparedChunk, AppendJournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.position == self.snapshot_end {
                self.done = true;
                return if self.previous_hash == self.snapshot_head {
                    None
                } else {
                    Some(Err(AppendJournalError::CorruptFrame {
                        offset: self.position,
                    }))
                };
            }
            match read_frame_at(
                &self.file,
                self.position,
                self.snapshot_end,
                self.expected_sequence,
                self.previous_hash,
            ) {
                Ok(Some((event, hash, _))) => {
                    self.previous_hash = hash;
                    self.position = match frame_end_at(&self.file, self.position, self.snapshot_end)
                    {
                        Ok(position) => position,
                        Err(error) => {
                            self.done = true;
                            return Some(Err(error));
                        }
                    };
                    self.expected_sequence = match self.expected_sequence.checked_add(1) {
                        Some(sequence) => sequence,
                        None => {
                            self.done = true;
                            return Some(Err(AppendJournalError::SequenceOverflow));
                        }
                    };
                    match event {
                        JournalEvent::ChunkPrepared(chunk) => {
                            if chunk.chunk_id != self.last_global_chunk_id.saturating_add(1) {
                                self.done = true;
                                return Some(Err(AppendJournalError::InvalidTransition(
                                    "global chunk continuity",
                                )));
                            }
                            if self.pending.replace(chunk).is_some() {
                                self.done = true;
                                return Some(Err(AppendJournalError::InvalidTransition(
                                    "multiple prepared chunks",
                                )));
                            }
                        }
                        JournalEvent::ChunkCommitted {
                            chunk_id,
                            operation_id,
                            target_transaction_intent,
                        } => {
                            let chunk = match self.pending.take() {
                                Some(chunk) => chunk,
                                None => {
                                    self.done = true;
                                    return Some(Err(AppendJournalError::InvalidTransition(
                                        "commit without prepare",
                                    )));
                                }
                            };
                            if chunk.chunk_id != chunk_id
                                || chunk.operation_id != operation_id
                                || chunk.target_transaction_intent != target_transaction_intent
                            {
                                self.done = true;
                                return Some(Err(AppendJournalError::InvalidTransition(
                                    "chunk commit evidence",
                                )));
                            }
                            self.last_global_chunk_id = chunk_id;
                            if operation_id == self.operation_id {
                                if let Some(cursor) = &self.target_cursor {
                                    if chunk.start_key.as_ref() != Some(&cursor.final_key) {
                                        self.done = true;
                                        return Some(Err(AppendJournalError::InvalidTransition(
                                            "chunk continuity",
                                        )));
                                    }
                                } else if chunk.start_key.is_some() {
                                    self.done = true;
                                    return Some(Err(AppendJournalError::InvalidTransition(
                                        "first chunk",
                                    )));
                                }
                                let previous = self.target_cursor.as_ref();
                                self.target_cursor = Some(CopyCursor {
                                    last_chunk_id: chunk.chunk_id,
                                    final_key: chunk.final_key.clone(),
                                    chunks: previous.map_or(1, |cursor| cursor.chunks + 1),
                                    rows: previous.map_or(chunk.row_count, |cursor| {
                                        cursor.rows + chunk.row_count
                                    }),
                                });
                                return Some(Ok(chunk));
                            }
                        }
                        _ => {}
                    }
                }
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

struct ScanResult {
    genesis: Option<Genesis>,
    projection: Option<JournalProjection>,
    valid_end: u64,
    last_sequence: u64,
    head_hash: [u8; 32],
}

fn scan_frames(file: &mut File, permit_torn_tail: bool) -> Result<ScanResult, AppendJournalError> {
    file.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
    let file_len = file.metadata()?.len();
    let mut genesis = None;
    let mut projection = None;
    let mut sequence = 1_u64;
    let mut previous_hash = [0; 32];
    let mut valid_end = FILE_HEADER_LEN;
    loop {
        let offset = file.stream_position()?;
        match read_frame(file, sequence, previous_hash) {
            Ok(Some((event, hash, end))) => {
                match event {
                    JournalEvent::Genesis(value) if sequence == 1 => {
                        value.validate()?;
                        projection = Some(JournalProjection::from_genesis(&value));
                        genesis = Some(*value);
                    }
                    JournalEvent::Genesis(_) => {
                        return Err(AppendJournalError::InvalidTransition("duplicate genesis"));
                    }
                    event => projection
                        .as_mut()
                        .ok_or(AppendJournalError::InvalidGenesis)?
                        .apply(&event)?,
                }
                previous_hash = hash;
                valid_end = end;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(AppendJournalError::SequenceOverflow)?;
            }
            Ok(None) => break,
            Err(AppendJournalError::Io(error))
                if permit_torn_tail
                    && error.kind() == io::ErrorKind::UnexpectedEof
                    && offset < file_len =>
            {
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(ScanResult {
        genesis,
        projection,
        valid_end,
        last_sequence: sequence.saturating_sub(1),
        head_hash: previous_hash,
    })
}

fn encode_event_frame(
    sequence: u64,
    previous_hash: [u8; 32],
    event: &JournalEvent,
) -> Result<Vec<u8>, AppendJournalError> {
    let (kind, payload) = match event {
        JournalEvent::ChunkPrepared(chunk) => (1, serde_json::to_vec(chunk)?),
        event => (0, serde_json::to_vec(event)?),
    };
    encode_frame(sequence, previous_hash, kind, &payload)
}

fn encode_frame(
    sequence: u64,
    previous_hash: [u8; 32],
    kind: u16,
    payload: &[u8],
) -> Result<Vec<u8>, AppendJournalError> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(AppendJournalError::PayloadTooLarge);
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| AppendJournalError::PayloadTooLarge)?;
    let payload_hash: [u8; 32] = Sha256::digest(payload).into();
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN);
    frame.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    frame.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    frame.extend_from_slice(&kind.to_le_bytes());
    frame.extend_from_slice(&sequence.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&previous_hash);
    frame.extend_from_slice(&payload_hash);
    frame.extend_from_slice(payload);
    let frame_hash: [u8; 32] = Sha256::digest(&frame).into();
    frame.extend_from_slice(&frame_hash);
    Ok(frame)
}

fn read_frame(
    file: &mut File,
    expected_sequence: u64,
    previous_hash: [u8; 32],
) -> Result<Option<(JournalEvent, [u8; 32], u64)>, AppendJournalError> {
    let offset = file.stream_position()?;
    let mut header = [0_u8; FRAME_HEADER_LEN];
    let read = file.read(&mut header)?;
    if read == 0 {
        return Ok(None);
    }
    if read != FRAME_HEADER_LEN {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }
    let magic = u32::from_le_bytes(
        header[0..4]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let version = u16::from_le_bytes(
        header[4..6]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let kind = u16::from_le_bytes(
        header[6..8]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let sequence = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let payload_len = u32::from_le_bytes(
        header[16..20]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    ) as usize;
    if magic != FRAME_MAGIC || version != FORMAT_VERSION || sequence != expected_sequence {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    if payload_len > MAX_PAYLOAD_LEN || header[20..52] != previous_hash {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)?;
    let mut stored_frame_hash = [0_u8; 32];
    file.read_exact(&mut stored_frame_hash)?;
    let payload_hash: [u8; 32] = Sha256::digest(&payload).into();
    if header[52..84] != payload_hash {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let mut hasher = Sha256::new();
    hasher.update(header);
    hasher.update(&payload);
    let actual_frame_hash: [u8; 32] = hasher.finalize().into();
    if actual_frame_hash != stored_frame_hash {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let event = match kind {
        0 => serde_json::from_slice(&payload),
        1 => serde_json::from_slice(&payload).map(JournalEvent::ChunkPrepared),
        _ => return Err(AppendJournalError::CorruptFrame { offset }),
    }
    .map_err(|_| AppendJournalError::CorruptFrame { offset })?;
    Ok(Some((event, stored_frame_hash, file.stream_position()?)))
}

fn frame_end_at(file: &File, offset: u64, snapshot_end: u64) -> Result<u64, AppendJournalError> {
    let mut prefix = [0_u8; 20];
    read_exact_at(file, &mut prefix, offset)?;
    let payload_len = u32::from_le_bytes(
        prefix[16..20]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    ) as u64;
    if payload_len > MAX_PAYLOAD_LEN as u64 {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let end = offset
        .checked_add(FRAME_HEADER_LEN as u64)
        .and_then(|value| value.checked_add(payload_len))
        .and_then(|value| value.checked_add(FRAME_TRAILER_LEN as u64))
        .ok_or(AppendJournalError::CorruptFrame { offset })?;
    if end > snapshot_end {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    Ok(end)
}

fn read_frame_at(
    file: &File,
    offset: u64,
    snapshot_end: u64,
    expected_sequence: u64,
    previous_hash: [u8; 32],
) -> Result<Option<(JournalEvent, [u8; 32], u64)>, AppendJournalError> {
    if offset == snapshot_end {
        return Ok(None);
    }
    let end = frame_end_at(file, offset, snapshot_end)?;
    let frame_len =
        usize::try_from(end - offset).map_err(|_| AppendJournalError::CorruptFrame { offset })?;
    let mut frame = vec![0; frame_len];
    read_exact_at(file, &mut frame, offset)?;
    let header = &frame[..FRAME_HEADER_LEN];
    let magic = u32::from_le_bytes(
        header[0..4]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let version = u16::from_le_bytes(
        header[4..6]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let kind = u16::from_le_bytes(
        header[6..8]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let sequence = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| AppendJournalError::CorruptFrame { offset })?,
    );
    let payload_len = frame_len - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
    if magic != FRAME_MAGIC
        || version != FORMAT_VERSION
        || sequence != expected_sequence
        || header[20..52] != previous_hash
    {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let payload = &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload_len];
    let payload_hash: [u8; 32] = Sha256::digest(payload).into();
    if header[52..84] != payload_hash {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let stored_hash: [u8; 32] = frame[FRAME_HEADER_LEN + payload_len..]
        .try_into()
        .map_err(|_| AppendJournalError::CorruptFrame { offset })?;
    let actual_hash: [u8; 32] = Sha256::digest(&frame[..FRAME_HEADER_LEN + payload_len]).into();
    if stored_hash != actual_hash {
        return Err(AppendJournalError::CorruptFrame { offset });
    }
    let event = match kind {
        0 => serde_json::from_slice(payload),
        1 => serde_json::from_slice(payload).map(JournalEvent::ChunkPrepared),
        _ => return Err(AppendJournalError::CorruptFrame { offset }),
    }
    .map_err(|_| AppendJournalError::CorruptFrame { offset })?;
    Ok(Some((event, stored_hash, end)))
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buffer.is_empty() {
        let read = file.read_at(buffer, offset)?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or(io::ErrorKind::InvalidData)?;
        buffer = &mut buffer[read..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(offset))?;
    reader.read_exact(buffer)
}

fn valid_operation_transition(from: OperationState, to: OperationState) -> bool {
    matches!(
        (from, to),
        (OperationState::Pending, OperationState::Running)
            | (OperationState::Running, OperationState::Prepared)
            | (OperationState::Prepared, OperationState::Committed)
            | (OperationState::Committed, OperationState::Verified)
            | (OperationState::Running, OperationState::Verified)
    )
}

fn valid_status_transition(from: MigrationStatus, to: MigrationStatus) -> bool {
    matches!(
        (from, to),
        (MigrationStatus::Running, MigrationStatus::Verifying)
            | (MigrationStatus::Running, MigrationStatus::Cancelled)
            | (MigrationStatus::Verifying, MigrationStatus::Completed)
            | (_, MigrationStatus::ManualReconciliationRequired)
    )
}

fn validate_file_header(file: &mut File) -> Result<(), AppendJournalError> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != FILE_MAGIC {
        return Err(AppendJournalError::CorruptFrame { offset: 0 });
    }
    let mut version = [0_u8; 2];
    file.read_exact(&mut version)?;
    let version = u16::from_le_bytes(version);
    if version != FORMAT_VERSION {
        return Err(AppendJournalError::UnsupportedVersion(version));
    }
    Ok(())
}

fn validate_parent(path: &Path) -> Result<(), AppendJournalError> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    reject_symlink_ancestors(parent)?;
    let metadata = fs::symlink_metadata(parent)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AppendJournalError::UnsafePath(parent.to_path_buf()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: `geteuid` has no arguments and only reads process credentials.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid || metadata.mode() & 0o022 != 0 {
            return Err(AppendJournalError::UnsafePath(parent.to_path_buf()));
        }
    }
    Ok(())
}

fn reject_symlink_ancestors(path: &Path) -> Result<(), AppendJournalError> {
    let mut current = Some(path);
    while let Some(component) = current {
        let metadata = fs::symlink_metadata(component)?;
        if metadata.file_type().is_symlink() {
            return Err(AppendJournalError::UnsafePath(component.to_path_buf()));
        }
        current = component.parent().filter(|parent| *parent != component);
    }
    Ok(())
}

fn validate_open_file(path: &Path, file: &File) -> Result<(), AppendJournalError> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(AppendJournalError::UnsafePath(path.to_path_buf()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: `geteuid` has no arguments and only reads process credentials.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid
            || metadata.mode() & 0o777 != 0o600
            || metadata.nlink() != 1
        {
            return Err(AppendJournalError::UnsafePath(path.to_path_buf()));
        }
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), AppendJournalError> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn lock_exclusive(file: &File) -> Result<(), AppendJournalError> {
    use std::os::fd::AsRawFd;
    // SAFETY: the fd remains owned by `file`; flock does not take ownership.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == -1 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn lock_exclusive(_file: &File) -> Result<(), AppendJournalError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::journal::ConsistencyEvidence;
    use crate::migration::plan::{
        MigrationPlan, OperationKind, PlanOperation, ReviewedPlan, UnsupportedObjectReport,
        PLAN_SCHEMA_VERSION,
    };

    fn genesis() -> Genesis {
        genesis_with_copy_operations(1)
    }

    fn genesis_with_copy_operations(count: usize) -> Genesis {
        let operations = (0..count)
            .map(|ordinal| {
                PlanOperation::new(
                    OperationKind::CopyTable,
                    None,
                    Vec::new(),
                    BTreeMap::from([("ordinal".into(), serde_json::json!(ordinal))]),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        genesis_from_operations(operations)
    }

    fn genesis_from_operations(operations: Vec<PlanOperation>) -> Genesis {
        let specs = operations
            .iter()
            .map(|operation| OperationSpec {
                operation_id: operation.id.to_string(),
                dependencies: operation.dependencies.iter().map(ToString::to_string).collect(),
                is_copy: operation.kind == OperationKind::CopyTable,
                phase: operation_phase(&operation.kind),
            })
            .collect();
        let reviewed_plan = ReviewedPlan::new(MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            migration_id: "migration".into(),
            tool_version: "test".into(),
            source_endpoint_identity: "source".into(),
            target_endpoint_identity: "target".into(),
            source_catalog_fingerprint: "source-fingerprint".into(),
            target_catalog_fingerprint: "target-fingerprint".into(),
            source_catalog: None,
            target_catalog: None,
            consistency_mode: "consistent_snapshot".into(),
            canonical_encoding_version: 1,
            conversion_policy: "exact".into(),
            capabilities: BTreeMap::new(),
            operations,
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap();
        let binding = ResumeBinding {
            migration_id: "migration".into(),
            plan_hash: reviewed_plan.plan_hash.to_string(),
            approval_reference: "approval".into(),
            tool_version: "test".into(),
            source_endpoint: "source".into(),
            target_endpoint: "target".into(),
            consistency_evidence: ConsistencyEvidence::NativeSnapshot {
                endpoint_identity: "source".into(),
                database_identity: "database".into(),
                lifecycle_id: "lifecycle".into(),
                snapshot_id: "snapshot".into(),
                server_version: "17".into(),
            },
            source_schema_fingerprint: "source-fingerprint".into(),
            target_schema_fingerprint: "target-fingerprint".into(),
            conversion_policy: "exact".into(),
            canonical_encoding_version: 1,
        };
        Genesis {
            binding,
            reviewed_plan,
            operations: specs,
        }
    }

    fn private_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("append-journal-")
            .tempdir_in(".")
            .unwrap()
    }

    fn hash() -> String {
        "0".repeat(64)
    }

    // Genesis construction is exercised by integration with real ReviewedPlan values.
    #[test]
    fn frame_round_trip_and_hash_corruption() {
        let event = JournalEvent::SchemaVerificationComplete {
            catalog_fingerprint: hash(),
        };
        let mut bytes = encode_event_frame(1, [0; 32], &event).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("frame");
        fs::write(&path, &bytes).unwrap();
        let mut file = File::open(&path).unwrap();
        assert!(read_frame(&mut file, 1, [0; 32]).unwrap().is_some());
        bytes[FRAME_HEADER_LEN] ^= 1;
        fs::write(&path, bytes).unwrap();
        let mut file = File::open(path).unwrap();
        assert!(matches!(
            read_frame(&mut file, 1, [0; 32]),
            Err(AppendJournalError::CorruptFrame { .. })
        ));
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let payload = vec![0; MAX_PAYLOAD_LEN + 1];
        assert!(matches!(
            encode_frame(1, [0; 32], 0, &payload),
            Err(AppendJournalError::PayloadTooLarge)
        ));
    }

    #[test]
    fn create_append_open_and_stream() {
        let directory = private_tempdir();
        let path = directory.path().join("state");
        let mut journal = AppendJournal::create_new(&path, genesis()).unwrap();
        let operation_id = journal.genesis.operations[0].operation_id.clone();
        journal
            .transition_operation(&operation_id, OperationState::Running)
            .unwrap();
        journal
            .prepare_chunk(PreparedChunk {
                chunk_id: 1,
                operation_id: operation_id.clone(),
                start_key: None,
                final_key: vec![DbValue::Signed(1)],
                row_count: 1,
                canonical_digest: hash(),
                target_transaction_intent: "intent-1".into(),
            })
            .unwrap();
        journal.commit_chunk_after_ack().unwrap();
        drop(journal);
        let journal = AppendJournal::open_resume(&path).unwrap();
        assert_eq!(journal.projection.copy_cursors[&operation_id].rows, 1);
        let chunks = journal
            .committed_chunks(operation_id)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn final_torn_tail_is_truncated() {
        let directory = private_tempdir();
        let path = directory.path().join("state");
        let journal = AppendJournal::create_new(&path, genesis()).unwrap();
        let valid_len = journal.file.metadata().unwrap().len();
        drop(journal);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&FRAME_MAGIC.to_le_bytes())
            .unwrap();
        let journal = AppendJournal::open_resume(&path).unwrap();
        assert_eq!(journal.file.metadata().unwrap().len(), valid_len);
    }

    #[test]
    fn invalid_transition_is_not_appended() {
        let directory = private_tempdir();
        let path = directory.path().join("state");
        let mut journal = AppendJournal::create_new(&path, genesis()).unwrap();
        let length = journal.file.metadata().unwrap().len();
        let operation_id = journal.genesis.operations[0].operation_id.clone();
        assert!(journal
            .transition_operation(&operation_id, OperationState::Verified)
            .is_err());
        assert_eq!(journal.file.metadata().unwrap().len(), length);
    }

    #[test]
    fn genesis_rejects_substituted_operation_metadata() {
        let mut value = genesis();
        value.operations[0].is_copy = false;
        assert!(value.validate().is_err());
        value.operations[0].is_copy = true;
        value.operations[0].dependencies.push("substitute".into());
        assert!(value.validate().is_err());
        let mut value = genesis();
        value.operations[0].phase = OperationPhase::Verification;
        assert!(value.validate().is_err());
    }

    #[test]
    fn atomic_prepare_accepts_dependencies_inside_the_group() {
        let namespace = PlanOperation::new(
            OperationKind::CreateNamespace,
            None,
            Vec::new(),
            BTreeMap::new(),
        )
        .unwrap();
        let table = PlanOperation::new(
            OperationKind::CreateTable,
            None,
            vec![namespace.id.clone()],
            BTreeMap::new(),
        )
        .unwrap();
        let genesis = genesis_from_operations(vec![namespace, table]);
        let operation_ids = genesis
            .operations
            .iter()
            .map(|operation| operation.operation_id.clone())
            .collect::<Vec<_>>();
        let mut projection = JournalProjection::from_genesis(&genesis);
        projection
            .apply(&JournalEvent::OperationsPreparedAtomic {
                operation_ids: operation_ids.clone(),
            })
            .unwrap();
        assert!(operation_ids.iter().all(|operation_id| {
            projection.operations.get(operation_id) == Some(&OperationState::Prepared)
        }));

        let mut invalid = JournalProjection::from_genesis(&genesis);
        assert!(invalid
            .apply(&JournalEvent::OperationsPreparedAtomic {
                operation_ids: vec![operation_ids[1].clone()],
            })
            .is_err());
        assert!(invalid
            .operations
            .values()
            .all(|state| *state == OperationState::Pending));
    }

    #[test]
    fn terminal_states_reject_all_later_work() {
        for status in [
            MigrationStatus::Cancelled,
            MigrationStatus::Completed,
            MigrationStatus::ManualReconciliationRequired,
        ] {
            let genesis = genesis();
            let operation_id = genesis.operations[0].operation_id.clone();
            let mut projection = JournalProjection::from_genesis(&genesis);
            projection.status = status;
            assert!(projection
                .apply(&JournalEvent::OperationTransition {
                    operation_id,
                    from: OperationState::Pending,
                    to: OperationState::Running,
                })
                .is_err());
        }
    }

    #[test]
    fn global_chunk_ids_allow_interleaved_copy_operations() {
        let genesis = genesis_with_copy_operations(2);
        let first = genesis.operations[0].operation_id.clone();
        let second = genesis.operations[1].operation_id.clone();
        let mut projection = JournalProjection::from_genesis(&genesis);
        for operation_id in [&first, &second] {
            projection
                .apply(&JournalEvent::OperationTransition {
                    operation_id: operation_id.clone(),
                    from: OperationState::Pending,
                    to: OperationState::Running,
                })
                .unwrap();
        }
        for (chunk_id, operation_id, start, end) in [
            (1, &first, None, 1),
            (2, &second, None, 10),
            (3, &first, Some(1), 2),
        ] {
            let intent = format!("intent-{chunk_id}");
            projection
                .apply(&JournalEvent::ChunkPrepared(PreparedChunk {
                    chunk_id,
                    operation_id: operation_id.clone(),
                    start_key: start.map(|value| vec![DbValue::Signed(value)]),
                    final_key: vec![DbValue::Signed(end)],
                    row_count: 1,
                    canonical_digest: hash(),
                    target_transaction_intent: intent.clone(),
                }))
                .unwrap();
            projection
                .apply(&JournalEvent::ChunkCommitted {
                    chunk_id,
                    operation_id: operation_id.clone(),
                    target_transaction_intent: intent,
                })
                .unwrap();
        }
        assert_eq!(projection.last_chunk_id, 3);
        assert_eq!(projection.copy_cursors[&first].chunks, 2);
        assert_eq!(projection.copy_cursors[&second].last_chunk_id, 2);
    }

    #[test]
    fn completion_gates_allow_verified_empty_copy() {
        let genesis = genesis();
        let operation_id = genesis.operations[0].operation_id.clone();
        let mut projection = JournalProjection::from_genesis(&genesis);
        projection
            .apply(&JournalEvent::OperationTransition {
                operation_id: operation_id.clone(),
                from: OperationState::Pending,
                to: OperationState::Running,
            })
            .unwrap();
        assert!(projection
            .apply(&JournalEvent::MigrationStatus {
                from: MigrationStatus::Running,
                to: MigrationStatus::Verifying,
            })
            .is_err());
        projection
            .apply(&JournalEvent::OperationTransition {
                operation_id: operation_id.clone(),
                from: OperationState::Running,
                to: OperationState::Verified,
            })
            .unwrap();
        projection
            .apply(&JournalEvent::MigrationStatus {
                from: MigrationStatus::Running,
                to: MigrationStatus::Verifying,
            })
            .unwrap();
        assert!(projection
            .apply(&JournalEvent::MigrationStatus {
                from: MigrationStatus::Verifying,
                to: MigrationStatus::Completed,
            })
            .is_err());
        projection
            .apply(&JournalEvent::TableVerificationComplete {
                operation_id,
                chunk_count: 0,
                row_count: 0,
                manifest_hash: hash(),
                source_hash: hash(),
                target_hash: hash(),
            })
            .unwrap();
        projection
            .apply(&JournalEvent::SchemaVerificationComplete {
                catalog_fingerprint: hash(),
            })
            .unwrap();
        projection
            .apply(&JournalEvent::MigrationStatus {
                from: MigrationStatus::Verifying,
                to: MigrationStatus::Completed,
            })
            .unwrap();
    }

    #[test]
    fn stale_empty_verification_cannot_be_followed_by_copy() {
        let genesis = genesis();
        let operation_id = genesis.operations[0].operation_id.clone();
        let mut projection = JournalProjection::from_genesis(&genesis);
        projection
            .apply(&JournalEvent::OperationTransition {
                operation_id: operation_id.clone(),
                from: OperationState::Pending,
                to: OperationState::Running,
            })
            .unwrap();
        projection
            .apply(&JournalEvent::OperationTransition {
                operation_id: operation_id.clone(),
                from: OperationState::Running,
                to: OperationState::Verified,
            })
            .unwrap();
        projection
            .apply(&JournalEvent::MigrationStatus {
                from: MigrationStatus::Running,
                to: MigrationStatus::Verifying,
            })
            .unwrap();
        projection
            .apply(&JournalEvent::TableVerificationComplete {
                operation_id: operation_id.clone(),
                chunk_count: 0,
                row_count: 0,
                manifest_hash: hash(),
                source_hash: hash(),
                target_hash: hash(),
            })
            .unwrap();
        assert!(projection
            .apply(&JournalEvent::ChunkPrepared(PreparedChunk {
                chunk_id: 1,
                operation_id,
                start_key: None,
                final_key: vec![DbValue::Signed(1)],
                row_count: 1,
                canonical_digest: hash(),
                target_transaction_intent: "late-write".into(),
            }))
            .is_err());
    }

    #[test]
    #[ignore = "explicit scale test"]
    fn one_hundred_thousand_events_replay_with_bounded_projection() {
        let directory = private_tempdir();
        let path = directory.path().join("state");
        let genesis = genesis();
        let operation_id = genesis.operations[0].operation_id.clone();
        let mut file = File::create(&path).unwrap();
        file.write_all(FILE_MAGIC).unwrap();
        file.write_all(&FORMAT_VERSION.to_le_bytes()).unwrap();
        let mut sequence = 1_u64;
        let mut previous_hash = [0; 32];
        let mut write_event = |event: JournalEvent| {
            let frame = encode_event_frame(sequence, previous_hash, &event).unwrap();
            previous_hash.copy_from_slice(&frame[frame.len() - 32..]);
            sequence += 1;
            file.write_all(&frame).unwrap();
        };
        write_event(JournalEvent::Genesis(Box::new(genesis)));
        write_event(JournalEvent::OperationTransition {
            operation_id: operation_id.clone(),
            from: OperationState::Pending,
            to: OperationState::Running,
        });
        for chunk_id in 1..=49_999_u64 {
            write_event(JournalEvent::ChunkPrepared(PreparedChunk {
                chunk_id,
                operation_id: operation_id.clone(),
                start_key: (chunk_id > 1).then(|| vec![DbValue::Unsigned((chunk_id - 1).into())]),
                final_key: vec![DbValue::Unsigned(chunk_id.into())],
                row_count: 1,
                canonical_digest: hash(),
                target_transaction_intent: format!("intent-{chunk_id}"),
            }));
            write_event(JournalEvent::ChunkCommitted {
                chunk_id,
                operation_id: operation_id.clone(),
                target_transaction_intent: format!("intent-{chunk_id}"),
            });
        }
        drop(write_event);
        file.sync_all().unwrap();
        drop(file);
        let mut file = File::open(&path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        validate_file_header(&mut file).unwrap();
        let scan = scan_frames(&mut file, false).unwrap();
        let projection = scan.projection.unwrap();
        assert_eq!(projection.operations.len(), 1);
        assert_eq!(projection.copy_cursors.len(), 1);
        assert!(projection.prepared_chunk.is_none());
        assert_eq!(projection.copy_cursors[&operation_id].chunks, 49_999);
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_file_metadata_is_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal");
        fs::write(&path, b"x").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let file = File::open(&path).unwrap();
        assert!(validate_open_file(&path, &file).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = directory.path().join("hard-link");
        fs::hard_link(&path, link).unwrap();
        let file = File::open(&path).unwrap();
        assert!(validate_open_file(&path, &file).is_err());
    }
}
