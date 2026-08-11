use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::model::{Identifier, RowBatch};
pub use super::model::{KeyTuple, QualifiedTable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySet {
    pub consistent_snapshot: bool,
    pub read_only_evidence: bool,
    pub transactions: bool,
    pub cancellation: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeysetPage {
    pub table: QualifiedTable,
    pub projection: Vec<Identifier>,
    pub key: Vec<Identifier>,
    pub after: Option<KeyTuple>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotToken {
    pub endpoint_identity: String,
    pub database_identity: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOnlyEvidence {
    pub server_enforced: bool,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionError {
    Cancelled,
    InvalidRequest(String),
    SnapshotMismatch,
    InjectedFailure(String),
    TableNotFound(QualifiedTable),
    TransactionRequired,
    TransactionAlreadyOpen,
    UnsupportedKeyValue,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ConnectionError {}

pub type ConnectionResult<T> = Result<T, ConnectionError>;

#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn check(&self) -> ConnectionResult<()> {
        if self.0.load(Ordering::Acquire) {
            Err(ConnectionError::Cancelled)
        } else {
            Ok(())
        }
    }
}

pub trait SourceConnectionFactory: Send + Sync {
    fn capabilities(&self) -> CapabilitySet;
    fn capture_snapshot(&self) -> ConnectionResult<SnapshotToken>;
    fn open_reader(
        &self,
        snapshot: &SnapshotToken,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn ReadSession>>;
}

pub trait TargetConnectionFactory: Send + Sync {
    fn capabilities(&self) -> CapabilitySet;
    fn open_writer(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn WriteSession>>;
    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>>;
}

pub trait ReadSession {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence;
    fn snapshot(&self) -> &SnapshotToken;
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch>;
}

pub trait WriteSession {
    fn begin(&mut self) -> ConnectionResult<()>;
    fn insert(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()>;
    fn commit(&mut self) -> ConnectionResult<()>;
    fn rollback(&mut self) -> ConnectionResult<()>;
}

pub trait VerificationSession {
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch>;
}
