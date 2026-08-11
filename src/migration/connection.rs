use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::model::{Identifier, RowBatch};
pub use super::model::{KeyTuple, QualifiedTable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Capability {
    Supported,
    Unsupported { reason: String },
    Unknown { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilitySet {
    capabilities: BTreeMap<&'static str, Capability>,
}

impl CapabilitySet {
    pub fn from_entries(entries: impl IntoIterator<Item = (&'static str, Capability)>) -> Self {
        Self {
            capabilities: entries.into_iter().collect(),
        }
    }

    pub fn require(&self, names: &[&'static str]) -> ConnectionResult<()> {
        for name in names {
            match self.capabilities.get(name) {
                Some(Capability::Supported) => {}
                Some(Capability::Unsupported { reason }) => {
                    return Err(ConnectionError::RequiredCapabilityUnavailable {
                        capability: name,
                        reason: reason.clone(),
                    });
                }
                Some(Capability::Unknown { reason }) => {
                    return Err(ConnectionError::RequiredCapabilityUnknown {
                        capability: name,
                        reason: reason.clone(),
                    });
                }
                None => {
                    return Err(ConnectionError::RequiredCapabilityUnknown {
                        capability: name,
                        reason: "adapter did not report the capability".into(),
                    });
                }
            }
        }
        Ok(())
    }
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
    RequiredCapabilityUnavailable {
        capability: &'static str,
        reason: String,
    },
    RequiredCapabilityUnknown {
        capability: &'static str,
        reason: String,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_unknown_required_capabilities_fail_closed() {
        let capabilities = CapabilitySet::from_entries([
            ("transactions", Capability::Supported),
            (
                "cancellation",
                Capability::Unknown {
                    reason: "not probed".into(),
                },
            ),
        ]);
        assert!(matches!(
            capabilities.require(&["cancellation"]),
            Err(ConnectionError::RequiredCapabilityUnknown { .. })
        ));
        assert!(matches!(
            capabilities.require(&["server_read_only"]),
            Err(ConnectionError::RequiredCapabilityUnknown { .. })
        ));
    }
}
