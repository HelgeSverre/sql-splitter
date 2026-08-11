use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(unix)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
static PROCESS_INTERRUPT_GENERATION: AtomicU64 = AtomicU64::new(0);
#[cfg(unix)]
static SIGINT_HANDLER: OnceLock<Result<(), i32>> = OnceLock::new();

#[cfg(unix)]
extern "C" fn record_sigint(_signal: libc::c_int) {
    PROCESS_INTERRUPT_GENERATION.fetch_add(1, Ordering::AcqRel);
}

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
    pub consistency_mode: String,
    pub server_version: String,
    pub lifecycle_id: String,
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
    Database(String),
    BatchLimit(String),
    CommitOutcomeUnknown(String),
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

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    observe_sigint: AtomicBool,
    sigint_baseline: AtomicU64,
    control_error: Mutex<Option<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<CancellationState>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
    }

    /// Install the process SIGINT handler and make this token observe it.
    pub fn observe_process_sigint(&self) -> ConnectionResult<()> {
        #[cfg(not(unix))]
        return Ok(());

        #[cfg(unix)]
        {
            let baseline = PROCESS_INTERRUPT_GENERATION.load(Ordering::Acquire);
            let result = SIGINT_HANDLER.get_or_init(|| {
                // SAFETY: `record_sigint` has C signal-handler ABI and performs only an
                // atomic store, which is async-signal-safe. The sigaction is initialized
                // before it is installed and remains valid for the process lifetime.
                unsafe {
                    let mut action: libc::sigaction = std::mem::zeroed();
                    action.sa_sigaction = record_sigint as *const () as usize;
                    libc::sigemptyset(&mut action.sa_mask);
                    action.sa_flags = 0;
                    if libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut()) == 0 {
                        Ok(())
                    } else {
                        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(-1))
                    }
                }
            });
            result.map_err(|code| {
                ConnectionError::Database(format!("failed to install SIGINT handler: errno {code}"))
            })?;
            self.0.sigint_baseline.store(baseline, Ordering::Release);
            self.0.observe_sigint.store(true, Ordering::Release);
            Ok(())
        }
    }

    pub fn is_cancelled(&self) -> bool {
        if self.0.cancelled.load(Ordering::Acquire) {
            return true;
        }
        #[cfg(unix)]
        {
            let baseline = self.0.sigint_baseline.load(Ordering::Acquire);
            self.0.observe_sigint.load(Ordering::Acquire)
                && PROCESS_INTERRUPT_GENERATION.load(Ordering::Acquire) != baseline
        }
        #[cfg(not(unix))]
        false
    }

    pub(crate) fn record_control_error(&self, error: impl Into<String>) {
        if let Ok(mut slot) = self.0.control_error.lock() {
            slot.get_or_insert_with(|| error.into());
        }
    }

    pub(crate) fn control_error(&self) -> Option<String> {
        self.0.control_error.lock().ok()?.clone()
    }

    pub fn check(&self) -> ConnectionResult<()> {
        if let Some(error) = self.control_error() {
            Err(ConnectionError::Database(format!(
                "statement cancellation control failed: {error}"
            )))
        } else if self.is_cancelled() {
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
    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>>;
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
    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>>;
}

pub trait ControlSession: Send {
    fn cancel_active_statement(&mut self) -> ConnectionResult<()>;
}

pub trait ReadSession {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence;
    fn snapshot(&self) -> &SnapshotToken;
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch>;
    fn select_page_only(&mut self, _request: &KeysetPage) -> ConnectionResult<RowBatch> {
        Err(ConnectionError::InvalidRequest(
            "adapter does not support physical-relation paging".into(),
        ))
    }
}

pub trait WriteSession {
    fn begin(&mut self) -> ConnectionResult<()>;
    fn insert(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()>;
    fn bulk_write(&mut self, _table: &QualifiedTable, _batch: &RowBatch) -> ConnectionResult<()> {
        Err(ConnectionError::RequiredCapabilityUnavailable {
            capability: "bulk_write",
            reason: "adapter does not support bulk writes".into(),
        })
    }
    fn commit(&mut self) -> ConnectionResult<()>;
    fn rollback(&mut self) -> ConnectionResult<()>;
}

pub trait VerificationSession {
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch>;
    fn select_page_only(&mut self, _request: &KeysetPage) -> ConnectionResult<RowBatch> {
        Err(ConnectionError::InvalidRequest(
            "adapter does not support physical-relation paging".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct InsertOnlyWriter;

    impl WriteSession for InsertOnlyWriter {
        fn begin(&mut self) -> ConnectionResult<()> {
            Ok(())
        }

        fn insert(&mut self, _table: &QualifiedTable, _batch: &RowBatch) -> ConnectionResult<()> {
            Ok(())
        }

        fn commit(&mut self) -> ConnectionResult<()> {
            Ok(())
        }

        fn rollback(&mut self) -> ConnectionResult<()> {
            Ok(())
        }
    }

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

    #[test]
    fn bulk_write_defaults_to_explicitly_unsupported() {
        let mut writer = InsertOnlyWriter;
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("accounts").unwrap(),
        };
        let batch = RowBatch::new(Vec::new(), 1, 1);
        assert!(matches!(
            writer.bulk_write(&table, &batch),
            Err(ConnectionError::RequiredCapabilityUnavailable {
                capability: "bulk_write",
                ..
            })
        ));
    }

    #[test]
    fn cancellation_is_shared_by_every_token_clone() {
        let cancellation = CancellationToken::default();
        let session_token = cancellation.clone();

        assert_eq!(session_token.check(), Ok(()));
        cancellation.cancel();

        assert!(session_token.is_cancelled());
        assert_eq!(session_token.check(), Err(ConnectionError::Cancelled));
    }

    #[test]
    fn cancellation_control_failure_is_retained() {
        let cancellation = CancellationToken::default();
        cancellation.cancel();
        cancellation.record_control_error("control connection failed");

        assert!(matches!(
            cancellation.check(),
            Err(ConnectionError::Database(message))
                if message.contains("control connection failed")
        ));
    }
}
