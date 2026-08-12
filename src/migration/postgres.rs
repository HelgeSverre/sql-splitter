//! Read-only PostgreSQL plan adapter for the enterprise migration spike.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres::binary_copy::BinaryCopyInWriter;
use postgres::config::SslMode;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use postgres::{CancelToken, Client, Config, IsolationLevel};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::{
    prepare_json_text_pair_new, read_json, write_json_new, write_json_text_pair_new,
};
use super::assessment::{
    project_outage_window, read_throughput_profile, render_markdown, AssessmentArtifact,
    AssessmentError, EvidenceStatus, ExecutionRequirement, ScopeEstimate, SourceAssessmentEvidence,
    ThroughputProfile, ASSESSMENT_SCHEMA_VERSION,
};
use super::canonical::{canonicalize_json, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, Capability, CapabilitySet, ConnectionError, ConnectionResult,
    ControlSession, KeysetPage, ReadOnlyEvidence, ReadSession, SnapshotToken,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession, WriteSession,
};
use super::model::{
    CatalogDependency, CatalogNamespace, CatalogObject, CatalogObjectKind, ColumnMeta, DbValue,
    Identifier, QualifiedTable, RowBatch, RowBatchError, ValueFormat, VendorCatalog,
};
use super::outage_projection::{
    projected_seconds, AcceptedOutageProjection, ByteBasis, OutageProjectionError,
    ReviewedOutagePolicy, OUTAGE_PROJECTION_SCHEMA_VERSION,
};
use super::plan::{
    AssessmentStatus, MigrationPlan, OperationId, OperationKind, PlanOperation, PlanPurpose,
    ReviewedPlan, UnsupportedObject, UnsupportedObjectCode, UnsupportedObjectReport,
    PLAN_SCHEMA_VERSION,
};
use super::postgres_ast::{
    parse_postgres_create_view, parse_postgres_sql_function, PostgresDurableAst,
};
use super::postgres_profile::{
    PostgresSourceProbeArtifact, PostgresSourceProbeRequirement, PostgresSourceProbeResult,
    PostgresSourceProbeStatus, PostgresSourceProfileContract, PostgresSourceProfileError,
    PostgresSourceProfileKind, POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
};

pub(crate) const CATALOG_FORMAT_VERSION: u32 = 5;
pub const POSTGRES_CONSISTENCY_SNAPSHOT: &str = "consistent-snapshot";
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_BATCH_BYTES: usize = 64 * 1024 * 1024;
static SNAPSHOT_LIFECYCLE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PostgresConsistencyMode {
    ConsistentSnapshot,
    WriteFence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresWritePolicy {
    BinaryCopyWithInsertFallbackV1,
    PlainInsertIdentityAlwaysV1,
}

impl PostgresConsistencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConsistentSnapshot => POSTGRES_CONSISTENCY_SNAPSHOT,
            Self::WriteFence => "write-fence",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresEndpointConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub credential_env: String,
    #[serde(default)]
    pub tls: PostgresTlsConfig,
    #[serde(default = "default_timeout_seconds")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_batch_rows")]
    pub max_batch_rows: usize,
    #[serde(default = "default_batch_bytes")]
    pub max_batch_bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresTlsConfig {
    #[serde(default)]
    pub ca_certificate: Option<String>,
    #[serde(default)]
    pub client_certificate: Option<String>,
    #[serde(default)]
    pub client_private_key: Option<String>,
    #[serde(default)]
    pub insecure: bool,
}

fn default_port() -> u16 {
    5432
}

fn default_timeout_seconds() -> u64 {
    10
}

fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}

fn default_batch_bytes() -> usize {
    DEFAULT_BATCH_BYTES
}

#[derive(Debug, Error)]
pub enum PostgresPlanError {
    #[error("cannot read PostgreSQL endpoint configuration")]
    ReadConfig(#[source] std::io::Error),
    #[error("invalid PostgreSQL endpoint configuration")]
    ParseConfig(#[from] toml::de::Error),
    #[error("invalid endpoint configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("assessment source transaction is not server-side read-only")]
    SourceNotReadOnly,
    #[error("assessment source role holds a direct database-object write privilege")]
    SourceRoleHoldsDirectWritePrivilege,
    #[error("unsupported generated-column dependency: {0}")]
    UnsupportedGeneratedDependency(String),
    #[error("credential environment variable {0} is not set or is not Unicode")]
    MissingCredential(String),
    #[error("cannot read configured CA certificate")]
    ReadCa(#[source] std::io::Error),
    #[error("cannot read configured TLS client certificate")]
    ReadClientCertificate(#[source] std::io::Error),
    #[error("cannot read configured TLS client private key")]
    ReadClientPrivateKey(#[source] std::io::Error),
    #[error("invalid TLS configuration")]
    Tls(#[from] native_tls::Error),
    #[error("PostgreSQL operation failed")]
    Database(#[from] postgres::Error),
    #[error("invalid database identifier")]
    Identifier(#[from] super::model::IdentifierError),
    #[error("plan construction failed")]
    Plan(#[from] super::plan::PlanError),
    #[error("artifact publication failed")]
    Artifact(#[from] super::artifact::ArtifactError),
    #[error("assessment construction failed")]
    Assessment(#[from] AssessmentError),
    #[error("outage projection validation failed")]
    OutageProjection(#[from] OutageProjectionError),
    #[error("PostgreSQL source-profile validation failed")]
    SourceProfile(#[from] PostgresSourceProfileError),
    #[error("catalog serialization failed")]
    Serialize(#[from] serde_json::Error),
}

impl PostgresEndpointConfig {
    pub fn read(path: impl AsRef<Path>) -> Result<Self, PostgresPlanError> {
        let text = fs::read_to_string(path).map_err(PostgresPlanError::ReadConfig)?;
        let config: Self = toml::from_str(&text)?;
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), PostgresPlanError> {
        if self.host.trim().is_empty() {
            return Err(PostgresPlanError::InvalidConfig("host must not be empty"));
        }
        if self.database.trim().is_empty() {
            return Err(PostgresPlanError::InvalidConfig(
                "database must not be empty",
            ));
        }
        if self.user.trim().is_empty() {
            return Err(PostgresPlanError::InvalidConfig("user must not be empty"));
        }
        if self.credential_env.trim().is_empty() || self.credential_env.contains('=') {
            return Err(PostgresPlanError::InvalidConfig(
                "credential_env must name one environment variable",
            ));
        }
        if self.connect_timeout_seconds == 0 {
            return Err(PostgresPlanError::InvalidConfig(
                "connect_timeout_seconds must be positive",
            ));
        }
        if self.max_batch_rows == 0 || self.max_batch_bytes == 0 {
            return Err(PostgresPlanError::InvalidConfig(
                "max_batch_rows and max_batch_bytes must be positive",
            ));
        }
        if self.tls.client_certificate.is_some() != self.tls.client_private_key.is_some() {
            return Err(PostgresPlanError::InvalidConfig(
                "client_certificate and client_private_key must be configured together",
            ));
        }
        if let Some(path) = &self.tls.client_private_key {
            validate_client_private_key(Path::new(path))?;
        }
        Ok(())
    }

    fn connect(&self) -> Result<Client, PostgresPlanError> {
        self.connect_with_application_name("sql-splitter-migration-plan")
    }

    fn connect_with_application_name(
        &self,
        application_name: &str,
    ) -> Result<Client, PostgresPlanError> {
        self.validate()?;
        let password = std::env::var(&self.credential_env)
            .map_err(|_| PostgresPlanError::MissingCredential(self.credential_env.clone()))?;
        let mut config = Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(password)
            .application_name(application_name)
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(self.connect_timeout_seconds));

        Ok(config.connect(self.tls_connector()?)?)
    }

    fn tls_connector(&self) -> Result<MakeTlsConnector, PostgresPlanError> {
        let mut tls = TlsConnector::builder();
        if let Some(path) = &self.tls.ca_certificate {
            let pem = fs::read(path).map_err(PostgresPlanError::ReadCa)?;
            tls.add_root_certificate(Certificate::from_pem(&pem)?);
        }
        if let (Some(certificate_path), Some(key_path)) =
            (&self.tls.client_certificate, &self.tls.client_private_key)
        {
            let certificate =
                fs::read(certificate_path).map_err(PostgresPlanError::ReadClientCertificate)?;
            let private_key =
                fs::read(key_path).map_err(PostgresPlanError::ReadClientPrivateKey)?;
            tls.identity(Identity::from_pkcs8(&certificate, &private_key)?);
        }
        if self.tls.insecure {
            tls.danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }
        Ok(MakeTlsConnector::new(tls.build()?))
    }
}

fn validate_client_private_key(path: &Path) -> Result<(), PostgresPlanError> {
    let metadata = fs::symlink_metadata(path).map_err(PostgresPlanError::ReadClientPrivateKey)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(PostgresPlanError::InvalidConfig(
            "TLS client private key must be a regular file and not a symlink",
        ));
    }
    #[cfg(unix)]
    {
        // SAFETY: `geteuid` has no arguments and only reads process credentials.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid || metadata.permissions().mode() & 0o077 != 0 {
            return Err(PostgresPlanError::InvalidConfig(
                "TLS client private key must be owned by the current user with mode 0600",
            ));
        }
    }
    Ok(())
}

pub(crate) fn postgres_tls_binding(
    config: &PostgresEndpointConfig,
) -> Result<String, PostgresPlanError> {
    config.validate()?;
    let policy = match (config.tls.insecure, config.tls.client_certificate.is_some()) {
        (true, true) => "insecure_explicit+mtls",
        (true, false) => "insecure_explicit",
        (false, true) => "hostname_verified+mtls",
        (false, false) => "hostname_verified",
    };
    let roots = match &config.tls.ca_certificate {
        Some(path) => format!(
            "sha256:{}",
            hex::encode(Sha256::digest(
                fs::read(path).map_err(PostgresPlanError::ReadCa)?
            ))
        ),
        None => "platform".into(),
    };
    let client = match &config.tls.client_certificate {
        Some(path) => format!(
            "sha256:{}",
            hex::encode(Sha256::digest(
                fs::read(path).map_err(PostgresPlanError::ReadClientCertificate)?
            ))
        ),
        None => "none".into(),
    };
    Ok(format!("{policy};roots={roots};client={client}"))
}

struct PendingSnapshot {
    _cancel_registration: SessionRegistration<CancelToken>,
    client: Client,
    exporter_lifetime: Arc<ExporterLifetime>,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    catalog: VendorCatalog,
    unsupported: UnsupportedObjectReport,
}

struct ExportedSnapshotBinding {
    token: SnapshotToken,
    exported_snapshot_id: String,
    exporter_lifetime: Weak<ExporterLifetime>,
}

struct ExporterLifetime {
    alive: Mutex<bool>,
}

struct SessionRegistry<T> {
    next_id: AtomicU64,
    sessions: Mutex<BTreeMap<u64, T>>,
}

impl<T> Default for SessionRegistry<T> {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            sessions: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<T: Clone> SessionRegistry<T> {
    fn register(self: &Arc<Self>, session: T) -> ConnectionResult<SessionRegistration<T>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.sessions
            .lock()
            .map_err(|_| ConnectionError::Database("session registry lock is poisoned".into()))?
            .insert(id, session);
        Ok(SessionRegistration {
            registry: Arc::clone(self),
            id,
        })
    }

    fn sessions(&self) -> ConnectionResult<Vec<T>> {
        Ok(self
            .sessions
            .lock()
            .map_err(|_| ConnectionError::Database("session registry lock is poisoned".into()))?
            .values()
            .cloned()
            .collect())
    }
}

struct SessionRegistration<T> {
    registry: Arc<SessionRegistry<T>>,
    id: u64,
}

impl<T> Drop for SessionRegistration<T> {
    fn drop(&mut self) {
        if let Ok(mut sessions) = self.registry.sessions.lock() {
            sessions.remove(&self.id);
        }
    }
}

type CancelRegistry = SessionRegistry<CancelToken>;

/// PostgreSQL source factory that transfers ownership of one live snapshot session.
pub struct PostgresSourceFactory {
    config: PostgresEndpointConfig,
    pending: Mutex<Option<PendingSnapshot>>,
    exported_snapshot: Mutex<Option<ExportedSnapshotBinding>>,
    cancel_registry: Arc<CancelRegistry>,
    cancellation: CancellationToken,
}

impl PostgresSourceFactory {
    pub fn new(config: PostgresEndpointConfig) -> Self {
        Self::new_with_cancellation(config, CancellationToken::default())
    }

    pub fn new_with_cancellation(
        config: PostgresEndpointConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config,
            pending: Mutex::new(None),
            exported_snapshot: Mutex::new(None),
            cancel_registry: Arc::default(),
            cancellation,
        }
    }

    fn controlled_connect(&self) -> ConnectionResult<(Client, SessionRegistration<CancelToken>)> {
        self.cancellation.check()?;
        let client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        let registration = self.cancel_registry.register(client.cancel_token())?;
        self.cancellation.check()?;
        Ok((client, registration))
    }

    pub fn inspect_endpoint(&self) -> ConnectionResult<CatalogSnapshot> {
        let (mut client, _registration) = self.controlled_connect()?;
        let snapshot = inspect_connected_endpoint(&self.config, &mut client)
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        self.cancellation.check()?;
        Ok(snapshot)
    }

    /// Open a read-only peer transaction imported from the active source snapshot.
    pub fn open_imported_snapshot_peer(
        &self,
        snapshot: &SnapshotToken,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn ReadSession>> {
        let (exported_snapshot_id, exporter_lifetime) = {
            let exported = self.exported_snapshot.lock().map_err(|_| {
                ConnectionError::Database("exported snapshot lock is poisoned".into())
            })?;
            let binding = exported.as_ref().ok_or(ConnectionError::SnapshotMismatch)?;
            if &binding.token != snapshot {
                return Err(ConnectionError::SnapshotMismatch);
            }
            let lifetime = binding
                .exporter_lifetime
                .upgrade()
                .ok_or(ConnectionError::SnapshotMismatch)?;
            (binding.exported_snapshot_id.clone(), lifetime)
        };
        let exporter_alive = exporter_lifetime
            .alive
            .lock()
            .map_err(|_| ConnectionError::Database("exporter lifetime lock is poisoned".into()))?;
        if !*exporter_alive {
            return Err(ConnectionError::SnapshotMismatch);
        }
        cancellation.check()?;
        let (mut client, cancel_registration) = self.controlled_connect()?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(database_error)?;
        client
            .batch_execute(&import_snapshot_statement(&exported_snapshot_id)?)
            .map_err(database_error)?;
        let row = client
            .query_one(
                "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, 'local'), COALESCE(inet_server_port(), 0), current_setting('server_version'), pg_current_snapshot()::text, current_setting('transaction_read_only')::boolean",
                &[],
            )
            .map_err(database_error)?;
        let database: String = row.get(0);
        let user: String = row.get(1);
        let address: String = row.get(2);
        let port: i32 = row.get(3);
        let server_version: String = row.get(4);
        let snapshot_id: String = row.get(5);
        let read_only: bool = row.get(6);
        let endpoint_identity = format!("postgres://{address}:{port}/{database}?user={user}");
        if !imported_snapshot_is_exact(
            snapshot,
            &endpoint_identity,
            &database,
            &server_version,
            &snapshot_id,
            read_only,
        ) {
            return Err(ConnectionError::SnapshotMismatch);
        }
        cancellation.check()?;
        drop(exporter_alive);
        Ok(Box::new(PostgresSnapshotReader {
            client,
            _cancel_registration: cancel_registration,
            exporter_lifetime: None,
            token: snapshot.clone(),
            evidence: ReadOnlyEvidence {
                server_enforced: true,
                description: "read-only peer transaction imported and attested against the primary PostgreSQL snapshot".into(),
            },
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            metadata_cache: HashMap::new(),
            validated_keys: BTreeSet::new(),
        }))
    }

    /// Return the exact catalog captured inside the pending execution snapshot.
    pub fn captured_catalog(
        &self,
        snapshot: &SnapshotToken,
    ) -> ConnectionResult<(VendorCatalog, UnsupportedObjectReport, String)> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("snapshot factory lock is poisoned".into()))?;
        let pending = pending.as_ref().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending.token != snapshot {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let fingerprint = catalog_fingerprint(&pending.catalog)
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        Ok((
            pending.catalog.clone(),
            pending.unsupported.clone(),
            fingerprint,
        ))
    }

    /// Refresh the reviewed outage projection inside the still-active source snapshot.
    pub fn refresh_outage_projection(
        &self,
        snapshot: &SnapshotToken,
        source_catalog: &VendorCatalog,
        policy: &ReviewedOutagePolicy,
    ) -> ConnectionResult<AcceptedOutageProjection> {
        self.cancellation.check()?;
        policy
            .validate()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("snapshot factory lock is poisoned".into()))?;
        let pending = pending.as_mut().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending.token != snapshot {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let fingerprint = catalog_fingerprint(source_catalog)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        if fingerprint != policy.source_catalog_fingerprint {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let relation_oids = physical_business_relation_oids(source_catalog)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let assessed_bytes = query_exact_total_relation_bytes(&mut pending.client, &relation_oids)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let identity = pending
            .client
            .query_one(
                "SELECT current_setting('server_version_num')::integer, extract(epoch FROM clock_timestamp())::bigint",
                &[],
            )
            .map_err(database_error)?;
        let server_version_num = u32::try_from(identity.get::<_, i32>(0)).map_err(|_| {
            ConnectionError::InvalidRequest("negative PostgreSQL server version number".into())
        })?;
        let refreshed_at_unix_seconds = u64::try_from(identity.get::<_, i64>(1)).map_err(|_| {
            ConnectionError::InvalidRequest("PostgreSQL clock is before the Unix epoch".into())
        })?;
        let projection = AcceptedOutageProjection {
            schema_version: OUTAGE_PROJECTION_SCHEMA_VERSION,
            policy_hash: policy
                .canonical_hash()
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
            source_catalog_fingerprint: fingerprint,
            source_server_version_num: server_version_num,
            byte_basis: ByteBasis::PostgresTotalRelationBytesV1,
            refreshed_at_unix_seconds,
            refreshed_assessed_bytes: assessed_bytes,
            projected_seconds: projected_seconds(assessed_bytes, &policy.throughput_profile)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
            throughput_profile: policy.throughput_profile.clone(),
        };
        projection
            .validate_against(policy)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        self.cancellation.check()?;
        Ok(projection)
    }

    /// Re-read exact sequence configuration and state through a fresh session.
    ///
    /// The caller supplies the reviewed sequence contracts. Every static
    /// attribute and ownership link must still match; only `last_value` and
    /// `is_called` are observed from the live relation.
    pub fn observe_sequence_states(
        &self,
        reviewed: &[PostgresSequence],
    ) -> ConnectionResult<Vec<PostgresSequence>> {
        self.cancellation.check()?;
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(database_error)?;
        let mut observed = Vec::with_capacity(reviewed.len());
        for sequence in reviewed {
            self.cancellation.check()?;
            let state = transaction
                .query_one(
                    &format!(
                        "SELECT last_value, is_called FROM {}.{}",
                        quote_identifier(&sequence.namespace),
                        quote_identifier(&sequence.name)
                    ),
                    &[],
                )
                .map_err(database_error)?;
            let mut candidate = sequence.clone();
            candidate.last_value = state.get(0);
            candidate.is_called = state.get(1);
            if inspect_sequence(&mut transaction, &candidate)? != PostgresSequenceState::ExactState
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "source sequence {}.{} differs from its reviewed contract",
                    sequence.namespace, sequence.name
                )));
            }
            observed.push(candidate);
        }
        self.cancellation.check()?;
        transaction.commit().map_err(database_error)?;
        Ok(observed)
    }
}

impl SourceConnectionFactory for PostgresSourceFactory {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::from_entries([
            ("consistent_snapshot", Capability::Supported),
            ("imported_snapshot_peer", Capability::Supported),
            ("server_read_only", Capability::Supported),
            ("transactions", Capability::Supported),
            ("typed_identifiers", Capability::Supported),
            ("bound_parameters", Capability::Supported),
            ("cancellation", Capability::Supported),
        ])
    }

    fn capture_snapshot(&self) -> ConnectionResult<SnapshotToken> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("snapshot factory lock is poisoned".into()))?;
        if pending.is_some() {
            return Err(ConnectionError::InvalidRequest(
                "a snapshot is already waiting for a reader".into(),
            ));
        }
        {
            let exported = self.exported_snapshot.lock().map_err(|_| {
                ConnectionError::Database("exported snapshot lock is poisoned".into())
            })?;
            if exported
                .as_ref()
                .and_then(|binding| binding.exporter_lifetime.upgrade())
                .is_some()
            {
                return Err(ConnectionError::InvalidRequest(
                    "an exported PostgreSQL snapshot is still active".into(),
                ));
            }
        }
        self.cancellation.check()?;
        let (mut client, cancel_registration) = self.controlled_connect()?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(database_error)?;
        let exported_snapshot_id: String = client
            .query_one("SELECT pg_export_snapshot()", &[])
            .map_err(database_error)?
            .get(0);
        validate_exported_snapshot_id(&exported_snapshot_id)?;
        let initial_sequence_states = capture_sequence_states_at_snapshot(&mut client)?;
        let row = client
            .query_one(
                "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, 'local'), COALESCE(inet_server_port(), 0), current_setting('server_version'), pg_current_snapshot()::text, current_setting('transaction_read_only')::boolean",
                &[],
            )
            .map_err(database_error)?;
        let database: String = row.get(0);
        let user: String = row.get(1);
        let address: String = row.get(2);
        let port: i32 = row.get(3);
        let server_version: String = row.get(4);
        let snapshot_id: String = row.get(5);
        let transaction_read_only: bool = row.get(6);
        if !transaction_read_only {
            return Err(ConnectionError::InvalidRequest(
                "PostgreSQL did not establish a read-only transaction".into(),
            ));
        }
        let privilege_row = client
            .query_one(
                "SELECT NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls AND NOT has_database_privilege(current_user, current_database(), 'CREATE,TEMP') AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND has_table_privilege(current_user, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'S' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND has_sequence_privilege(current_user, c.oid, 'USAGE,UPDATE')) AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND has_schema_privilege(current_user, n.oid, 'CREATE')) FROM pg_roles r WHERE r.rolname = current_user",
                &[],
            )
            .map_err(database_error)?;
        let role_has_no_write_privileges: bool = privilege_row.get(0);
        if !role_has_no_write_privileges {
            return Err(ConnectionError::InvalidRequest(
                "source role has database, schema, table, sequence, or privileged-role write capability"
                    .into(),
            ));
        }
        let endpoint_identity = format!("postgres://{address}:{port}/{database}?user={user}");
        let lifecycle_id = format!(
            "pg-session-{}",
            SNAPSHOT_LIFECYCLE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let token = SnapshotToken {
            endpoint_identity,
            database_identity: database,
            snapshot_id,
            consistency_mode: "postgres_repeatable_read_read_only".into(),
            server_version,
            lifecycle_id,
        };
        let evidence = ReadOnlyEvidence {
            server_enforced: true,
            description: "read-only transaction and source role ACL probe deny database, schema, relation, sequence, and privileged-role writes".into(),
        };
        let (mut catalog, unsupported) =
            extract_catalog(&mut client, &token.database_identity, &token.server_version)
                .map_err(|error| ConnectionError::Database(error.to_string()))?;
        apply_initial_sequence_states(&mut catalog, &initial_sequence_states)?;
        self.cancellation.check()?;
        let exporter_lifetime = Arc::new(ExporterLifetime {
            alive: Mutex::new(true),
        });
        *self.exported_snapshot.lock().map_err(|_| {
            ConnectionError::Database("exported snapshot lock is poisoned".into())
        })? = Some(ExportedSnapshotBinding {
            token: token.clone(),
            exported_snapshot_id,
            exporter_lifetime: Arc::downgrade(&exporter_lifetime),
        });
        *pending = Some(PendingSnapshot {
            client,
            _cancel_registration: cancel_registration,
            exporter_lifetime,
            token: token.clone(),
            evidence,
            catalog,
            unsupported,
        });
        Ok(token)
    }

    fn open_reader(
        &self,
        snapshot: &SnapshotToken,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn ReadSession>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("snapshot factory lock is poisoned".into()))?;
        let pending_snapshot = pending.take().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending_snapshot.token != snapshot {
            *pending = Some(pending_snapshot);
            return Err(ConnectionError::SnapshotMismatch);
        }
        Ok(Box::new(PostgresSnapshotReader {
            client: pending_snapshot.client,
            _cancel_registration: pending_snapshot._cancel_registration,
            exporter_lifetime: Some(pending_snapshot.exporter_lifetime),
            token: pending_snapshot.token,
            evidence: pending_snapshot.evidence,
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            metadata_cache: HashMap::new(),
            validated_keys: BTreeSet::new(),
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        let tokens = self.cancel_registry.sessions()?;
        if tokens.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "capture a PostgreSQL snapshot before opening its control session".into(),
            ));
        }
        Ok(Box::new(PostgresControlSession {
            registry: Arc::clone(&self.cancel_registry),
            config: self.config.clone(),
        }))
    }
}

fn validate_exported_snapshot_id(snapshot_id: &str) -> ConnectionResult<()> {
    let mut parts = snapshot_id.split('-');
    let first = parts.next().unwrap_or_default();
    let second = parts.next().unwrap_or_default();
    let third = parts.next().unwrap_or_default();
    let valid_hex = |part: &str| {
        part.len() == 8
            && part
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(&byte))
    };
    if !valid_hex(first)
        || !valid_hex(second)
        || third.is_empty()
        || !third.bytes().all(|byte| byte.is_ascii_digit())
        || parts.next().is_some()
    {
        return Err(ConnectionError::Database(
            "PostgreSQL returned an invalid exported snapshot identifier".into(),
        ));
    }
    Ok(())
}

fn capture_sequence_states_at_snapshot(
    client: &mut Client,
) -> ConnectionResult<BTreeMap<String, (i64, bool)>> {
    let sequences = client
        .query(
            "SELECT 'relation:' || c.oid::text, n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND n.nspname <> 'sql_splitter_migration_fence' ORDER BY c.oid",
            &[],
        )
        .map_err(database_error)?;
    let mut states = BTreeMap::new();
    for sequence in sequences {
        let object_id = sequence.get::<_, String>(0);
        let namespace = Identifier::new(sequence.get::<_, String>(1))
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let name = Identifier::new(sequence.get::<_, String>(2))
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let state = client
            .query_one(
                &format!(
                    "SELECT last_value, is_called FROM {}.{}",
                    quote_identifier(&namespace),
                    quote_identifier(&name)
                ),
                &[],
            )
            .map_err(database_error)?;
        if states
            .insert(object_id, (state.get(0), state.get(1)))
            .is_some()
        {
            return Err(ConnectionError::InvalidRequest(
                "PostgreSQL snapshot contains a duplicate sequence identity".into(),
            ));
        }
    }
    Ok(states)
}

fn apply_initial_sequence_states(
    catalog: &mut VendorCatalog,
    initial: &BTreeMap<String, (i64, bool)>,
) -> ConnectionResult<()> {
    let mut observed = BTreeSet::new();
    for object in catalog
        .namespaces
        .iter_mut()
        .filter(|namespace| namespace.name.as_str() != "sql_splitter_migration_fence")
        .flat_map(|namespace| namespace.objects.iter_mut())
        .filter(|object| object.kind == CatalogObjectKind::Sequence)
    {
        let (last_value, is_called) = initial
            .get(&object.id)
            .ok_or(ConnectionError::SnapshotMismatch)?;
        object.attributes.insert(
            "last_value".into(),
            serde_json::Value::String(last_value.to_string()),
        );
        object
            .attributes
            .insert("is_called".into(), serde_json::Value::Bool(*is_called));
        observed.insert(object.id.as_str());
    }
    if observed.len() != initial.len()
        || initial
            .keys()
            .any(|object_id| !observed.contains(object_id.as_str()))
    {
        return Err(ConnectionError::SnapshotMismatch);
    }
    Ok(())
}

fn import_snapshot_statement(snapshot_id: &str) -> ConnectionResult<String> {
    validate_exported_snapshot_id(snapshot_id)?;
    Ok(format!("SET TRANSACTION SNAPSHOT '{snapshot_id}'"))
}

fn imported_snapshot_is_exact(
    expected: &SnapshotToken,
    endpoint_identity: &str,
    database: &str,
    server_version: &str,
    snapshot_id: &str,
    read_only: bool,
) -> bool {
    read_only
        && database == expected.database_identity
        && server_version == expected.server_version
        && endpoint_identity == expected.endpoint_identity
        && snapshot_id == expected.snapshot_id
}

struct PostgresControlSession {
    registry: Arc<CancelRegistry>,
    config: PostgresEndpointConfig,
}

impl ControlSession for PostgresControlSession {
    fn cancel_active_statement(&mut self) -> ConnectionResult<()> {
        let sessions = self.registry.sessions()?;
        let mut first_error = None;
        for token in &sessions {
            let result = self
                .config
                .tls_connector()
                .map_err(|error| ConnectionError::Database(error.to_string()))
                .and_then(|connector| token.cancel_query(connector).map_err(database_error));
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

/// PostgreSQL target factory for transactional same-dialect chunk writes.
pub struct PostgresTargetFactory {
    config: PostgresEndpointConfig,
    cancel_registry: Arc<CancelRegistry>,
    cancellation: CancellationToken,
}

impl PostgresTargetFactory {
    pub fn new(config: PostgresEndpointConfig) -> Self {
        Self::new_with_cancellation(config, CancellationToken::default())
    }

    pub fn new_with_cancellation(
        config: PostgresEndpointConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config,
            cancel_registry: Arc::default(),
            cancellation,
        }
    }

    fn controlled_connect(&self) -> ConnectionResult<(Client, SessionRegistration<CancelToken>)> {
        self.cancellation.check()?;
        let client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        let registration = self.cancel_registry.register(client.cancel_token())?;
        self.cancellation.check()?;
        Ok((client, registration))
    }

    pub fn inspect_endpoint(&self) -> ConnectionResult<CatalogSnapshot> {
        let (mut client, _registration) = self.controlled_connect()?;
        let snapshot = inspect_connected_endpoint(&self.config, &mut client)
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        self.cancellation.check()?;
        Ok(snapshot)
    }

    /// Recheck that the target is empty and owned by the configured role.
    pub fn assert_empty_and_owned(&self) -> ConnectionResult<()> {
        let (mut client, _registration) = self.controlled_connect()?;
        assert_target_empty_and_owned(&mut client)?;
        self.cancellation.check()
    }

    /// Create supported namespaces and tables in one atomic PostgreSQL transaction.
    pub fn create_pre_data_schema(&self, catalog: &VendorCatalog) -> ConnectionResult<()> {
        if catalog.dialect != "postgresql" {
            return Err(ConnectionError::InvalidRequest(
                "PostgreSQL target requires a PostgreSQL vendor catalog".into(),
            ));
        }
        let statements = pre_data_statements(catalog)?;
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        assert_target_empty_and_owned(&mut transaction)?;
        for statement in statements {
            if let Err(cancelled) = self.cancellation.check() {
                transaction.rollback().map_err(database_error)?;
                return Err(cancelled);
            }
            transaction
                .batch_execute(&statement)
                .map_err(database_error)?;
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction.commit().map_err(database_error)?;
        self.cancellation.check()
    }

    /// Inspect the exact configuration and current state of one PostgreSQL sequence.
    pub fn inspect_sequence(
        &self,
        sequence: &PostgresSequence,
    ) -> ConnectionResult<PostgresSequenceState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let state = inspect_sequence(&mut client, sequence)?;
        self.cancellation.check()?;
        Ok(state)
    }

    /// Inspect one target stored generated column and its immutable dependency closure.
    pub fn inspect_generated_column(
        &self,
        generated: &PostgresGeneratedColumn,
    ) -> ConnectionResult<PostgresGeneratedColumnState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let state = inspect_generated_column(&mut client, generated)?;
        self.cancellation.check()?;
        Ok(state)
    }

    pub fn inspect_programmable_object(
        &self,
        expected: &PostgresProgrammableObject,
    ) -> ConnectionResult<PostgresProgrammableObjectState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let relation_kind = matches!(expected.ast, PostgresDurableAst::View(_));
        let occupied: bool = client
            .query_one(
                "SELECT CASE WHEN $3 THEN EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2) ELSE EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 AND p.proname=$2) END",
                &[&expected.namespace.as_str(), &expected.name.as_str(), &relation_kind],
            )
            .map_err(database_error)?
            .get(0);
        if !occupied {
            return Ok(PostgresProgrammableObjectState::Absent);
        }
        let snapshot = inspect_connected_endpoint(&self.config, &mut client)
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        let exact = postgres_programmable_objects(&snapshot.catalog)
            .map_err(|error| ConnectionError::Database(error.to_string()))?
            .into_iter()
            .any(|actual| programmable_object_semantically_equal(expected, &actual));
        self.cancellation.check()?;
        Ok(if exact {
            PostgresProgrammableObjectState::Exact
        } else {
            PostgresProgrammableObjectState::Different
        })
    }

    pub fn reconcile_programmable_object(
        &self,
        expected: &PostgresProgrammableObject,
    ) -> ConnectionResult<PostgresProgrammableObjectState> {
        match self.inspect_programmable_object(expected)? {
            PostgresProgrammableObjectState::Exact => {
                return Ok(PostgresProgrammableObjectState::Exact);
            }
            PostgresProgrammableObjectState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target programmable object {}.{} has different semantics",
                    expected.namespace, expected.name
                )));
            }
            PostgresProgrammableObjectState::Absent => {}
        }
        let statement = expected
            .ast
            .render_canonical()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        self.cancellation.check()?;
        transaction
            .batch_execute(&statement)
            .map_err(database_error)?;
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction.commit().map_err(database_error)?;
        self.inspect_programmable_object(expected)
    }

    /// Inspect an entire partition root/leaf topology by semantic identity.
    pub fn inspect_partition_topology(
        &self,
        expected: &PostgresPartitionTopology,
    ) -> ConnectionResult<PostgresPartitionTopologyState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let mut expected_relations = vec![expected.root.clone()];
        expected_relations.extend(expected.leaves.iter().map(|leaf| leaf.table.clone()));
        let mut present = 0usize;
        for table in &expected_relations {
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2)",
                    &[&table.namespace.as_str(), &table.name.as_str()],
                )
                .map_err(database_error)?
                .get(0);
            present += usize::from(exists);
        }
        if present == 0 {
            return Ok(PostgresPartitionTopologyState::Absent);
        }
        if present != expected_relations.len() {
            return Ok(PostgresPartitionTopologyState::Partial);
        }
        drop(client);
        let target = self.inspect_endpoint()?;
        let topologies = postgres_partition_topologies(&target.catalog)
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        Ok(
            if topologies
                .iter()
                .any(|actual| partition_topology_semantically_equal(expected, actual))
            {
                PostgresPartitionTopologyState::Exact
            } else {
                PostgresPartitionTopologyState::Different
            },
        )
    }

    /// Create an absent sequence contract or accept an exact existing configuration.
    pub fn reconcile_sequence_config(
        &self,
        sequence: &PostgresSequence,
    ) -> ConnectionResult<PostgresSequenceState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        match inspect_sequence(&mut transaction, sequence)? {
            PostgresSequenceState::ExactConfig | PostgresSequenceState::ExactState => {
                transaction.commit().map_err(database_error)?;
            }
            PostgresSequenceState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target sequence {}.{} exists with different semantics",
                    sequence.namespace, sequence.name
                )));
            }
            PostgresSequenceState::Absent => {
                for statement in sequence_create_statements(sequence) {
                    if let Err(cancelled) = self.cancellation.check() {
                        transaction.rollback().map_err(database_error)?;
                        return Err(cancelled);
                    }
                    transaction
                        .batch_execute(&statement)
                        .map_err(database_error)?;
                }
                if !matches!(
                    inspect_sequence(&mut transaction, sequence)?,
                    PostgresSequenceState::ExactConfig | PostgresSequenceState::ExactState
                ) {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "target sequence {}.{} did not match after creation",
                        sequence.namespace, sequence.name
                    )));
                }
                if let Err(cancelled) = self.cancellation.check() {
                    transaction.rollback().map_err(database_error)?;
                    return Err(cancelled);
                }
                transaction.commit().map_err(database_error)?;
            }
        }
        self.inspect_sequence(sequence)
    }

    /// Restore the exact `last_value` and `is_called` state with a bound `setval` call.
    pub fn restore_sequence(
        &self,
        sequence: &PostgresSequence,
    ) -> ConnectionResult<PostgresSequenceState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        match inspect_sequence(&mut transaction, sequence)? {
            PostgresSequenceState::ExactState => {
                transaction.commit().map_err(database_error)?;
                return Ok(PostgresSequenceState::ExactState);
            }
            PostgresSequenceState::ExactConfig => {}
            PostgresSequenceState::Absent => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target sequence {}.{} is absent",
                    sequence.namespace, sequence.name
                )));
            }
            PostgresSequenceState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target sequence {}.{} exists with different semantics",
                    sequence.namespace, sequence.name
                )));
            }
        }
        let qualified_name = qualified_regclass_name(&sequence.namespace, &sequence.name);
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction
            .query_one(
                "SELECT pg_catalog.setval($1::text::regclass, $2, $3)",
                &[&qualified_name, &sequence.last_value, &sequence.is_called],
            )
            .map_err(database_error)?;
        if inspect_sequence(&mut transaction, sequence)? != PostgresSequenceState::ExactState {
            return Err(ConnectionError::InvalidRequest(format!(
                "target sequence {}.{} state differs after setval",
                sequence.namespace, sequence.name
            )));
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction.commit().map_err(database_error)?;
        Ok(PostgresSequenceState::ExactState)
    }

    /// Inspect one target foreign key against exact typed source metadata.
    pub fn inspect_foreign_key(
        &self,
        foreign_key: &PostgresForeignKey,
    ) -> ConnectionResult<PostgresForeignKeyState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let state = inspect_foreign_key(&mut client, foreign_key)?;
        self.cancellation.check()?;
        Ok(state)
    }

    /// Check the exact PostgreSQL null and match semantics without changing target state.
    pub fn check_foreign_key(
        &self,
        foreign_key: &PostgresForeignKey,
    ) -> ConnectionResult<PostgresForeignKeyCheck> {
        let (mut client, _registration) = self.controlled_connect()?;
        let check = check_foreign_key(&mut client, foreign_key)?;
        self.cancellation.check()?;
        Ok(check)
    }

    /// Reconcile an absent or unvalidated constraint and require an exact result.
    ///
    /// Creation uses one transaction containing the exact anti-join, `ADD
    /// CONSTRAINT ... NOT VALID`, and `VALIDATE CONSTRAINT`.
    pub fn reconcile_foreign_key(
        &self,
        foreign_key: &PostgresForeignKey,
    ) -> ConnectionResult<PostgresForeignKeyState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        match inspect_foreign_key(&mut transaction, foreign_key)? {
            PostgresForeignKeyState::ExactValidated => {
                transaction.commit().map_err(database_error)?;
                return Ok(PostgresForeignKeyState::ExactValidated);
            }
            PostgresForeignKeyState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target constraint {} exists with different semantics",
                    foreign_key.name
                )));
            }
            PostgresForeignKeyState::Missing => {
                let check = check_foreign_key(&mut transaction, foreign_key)?;
                if check.has_violation {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "target rows violate foreign key {}",
                        foreign_key.name
                    )));
                }
                if let Err(cancelled) = self.cancellation.check() {
                    transaction.rollback().map_err(database_error)?;
                    return Err(cancelled);
                }
                transaction
                    .batch_execute(&foreign_key_add_statement(foreign_key))
                    .map_err(database_error)?;
            }
            PostgresForeignKeyState::ExactNotValidated => {}
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction
            .batch_execute(&format!(
                "ALTER TABLE {}.{} VALIDATE CONSTRAINT {}",
                quote_identifier(&foreign_key.table.namespace),
                quote_identifier(&foreign_key.table.name),
                quote_identifier(&foreign_key.name)
            ))
            .map_err(database_error)?;
        if inspect_foreign_key(&mut transaction, foreign_key)?
            != PostgresForeignKeyState::ExactValidated
        {
            return Err(ConnectionError::InvalidRequest(format!(
                "target constraint {} did not validate exactly",
                foreign_key.name
            )));
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction.commit().map_err(database_error)?;
        Ok(PostgresForeignKeyState::ExactValidated)
    }

    /// Inspect one ordinary target index against exact typed source metadata.
    pub fn inspect_index(&self, index: &PostgresIndex) -> ConnectionResult<PostgresIndexState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let state = inspect_index(&mut client, index)?;
        self.cancellation.check()?;
        Ok(state)
    }

    /// Create an absent ordinary index atomically and require an exact result.
    pub fn reconcile_index(&self, index: &PostgresIndex) -> ConnectionResult<PostgresIndexState> {
        let (mut client, _registration) = self.controlled_connect()?;
        let mut transaction = client.transaction().map_err(database_error)?;
        match inspect_index(&mut transaction, index)? {
            PostgresIndexState::Exact => {
                transaction.commit().map_err(database_error)?;
                return Ok(PostgresIndexState::Exact);
            }
            PostgresIndexState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "target index {} exists with different semantics",
                    index.name
                )));
            }
            PostgresIndexState::Absent => {}
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction
            .batch_execute(&ordinary_index_create_statement(index))
            .map_err(database_error)?;
        if inspect_index(&mut transaction, index)? != PostgresIndexState::Exact {
            return Err(ConnectionError::InvalidRequest(format!(
                "target index {} was not created exactly",
                index.name
            )));
        }
        if let Err(cancelled) = self.cancellation.check() {
            transaction.rollback().map_err(database_error)?;
            return Err(cancelled);
        }
        transaction.commit().map_err(database_error)?;
        Ok(PostgresIndexState::Exact)
    }
}

/// Exact PostgreSQL sequence configuration, state, and ownership metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSequence {
    pub catalog_object_id: String,
    pub namespace: Identifier,
    pub name: Identifier,
    pub persistence: String,
    pub data_type: String,
    pub start_value: i64,
    pub increment: i64,
    pub minimum_value: i64,
    pub maximum_value: i64,
    pub cache_size: i64,
    pub cycle: bool,
    pub last_value: i64,
    pub is_called: bool,
    pub ownership: Option<PostgresSequenceOwnership>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSequenceOwnership {
    pub table: QualifiedTable,
    pub column: Identifier,
    pub kind: PostgresSequenceOwnershipKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSequenceOwnershipKind {
    IdentityAlways,
    IdentityByDefault,
    Serial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSequenceState {
    Absent,
    ExactConfig,
    ExactState,
    Different,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresGeneratedDependencyKind {
    Column,
    Function,
    Operator,
    Type,
    Collation,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresGeneratedDependency {
    pub kind: PostgresGeneratedDependencyKind,
    pub identity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresGeneratedColumn {
    pub catalog_object_id: String,
    pub table: QualifiedTable,
    pub column: Identifier,
    pub expression: String,
    pub data_type: String,
    pub collation_schema: Option<Identifier>,
    pub collation: Option<Identifier>,
    pub dependencies: Vec<PostgresGeneratedDependency>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresGeneratedColumnState {
    Absent,
    Exact,
    Different,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPartitionBoundValue {
    MinValue,
    Value(i64),
    MaxValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPartitionBound {
    Range {
        lower: PostgresPartitionBoundValue,
        upper: PostgresPartitionBoundValue,
    },
    List {
        values: Vec<Option<i64>>,
    },
    Hash {
        modulus: u32,
        remainder: u32,
    },
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresPartitionLeaf {
    pub catalog_object_id: String,
    pub table: QualifiedTable,
    pub bound: PostgresPartitionBound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresPartitionTopology {
    pub root_catalog_object_id: String,
    pub root: QualifiedTable,
    pub key_column: Identifier,
    pub key_type: String,
    pub strategy: PostgresPartitionStrategy,
    pub leaves: Vec<PostgresPartitionLeaf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresPartitionTopologyState {
    Absent,
    Exact,
    Partial,
    Different,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresProgrammableObject {
    pub catalog_object_id: String,
    pub namespace: Identifier,
    pub name: Identifier,
    pub ast: PostgresDurableAst,
    pub authoritative_identity: PostgresProgrammableIdentity,
    /// Complete, catalog-resolved `pg_depend` identities, not syntax-derived hints.
    pub authoritative_dependencies: Vec<PostgresProgrammableIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresProgrammableIdentity {
    Relation {
        namespace: Identifier,
        name: Identifier,
    },
    Function {
        namespace: Identifier,
        name: Identifier,
        identity_arguments: String,
        return_type: String,
    },
    Type {
        namespace: Identifier,
        name: Identifier,
    },
    Collation {
        namespace: Identifier,
        name: Identifier,
    },
    Operator {
        namespace: Identifier,
        name: Identifier,
        left_type: String,
        right_type: String,
        result_type: String,
    },
    Namespace {
        name: Identifier,
    },
    Language {
        name: Identifier,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresProgrammableObjectState {
    Absent,
    Exact,
    Different,
}

/// Parse and validate supported single-level PostgreSQL partition topologies.
pub fn postgres_partition_topologies(
    catalog: &VendorCatalog,
) -> Result<Vec<PostgresPartitionTopology>, PostgresPlanError> {
    if catalog.dialect != "postgresql" {
        return Err(PostgresPlanError::InvalidConfig(
            "partition metadata requires a PostgreSQL catalog",
        ));
    }
    let mut topologies = Vec::new();
    for namespace in &catalog.namespaces {
        for root in namespace.objects.iter().filter(|object| {
            object.kind == CatalogObjectKind::Table
                && object
                    .attributes
                    .get("relkind")
                    .and_then(serde_json::Value::as_str)
                    == Some("p")
        }) {
            let strategy = match required_catalog_string(root, "partition_strategy")? {
                "r" => PostgresPartitionStrategy::Range,
                "l" => PostgresPartitionStrategy::List,
                "h" => PostgresPartitionStrategy::Hash,
                _ => {
                    return Err(PostgresPlanError::InvalidConfig(
                        "partition strategy is unsupported",
                    ));
                }
            };
            let key_column =
                Identifier::new(required_catalog_string(root, "partition_key_column")?)?;
            let key_type = required_catalog_string(root, "partition_key_type")?.to_owned();
            if !matches!(key_type.as_str(), "smallint" | "integer" | "bigint") {
                return Err(PostgresPlanError::InvalidConfig(
                    "partition key is not a built-in integer",
                ));
            }
            if required_catalog_string(root, "persistence")? != "p" {
                return Err(PostgresPlanError::InvalidConfig(
                    "partition root is not a logged relation",
                ));
            }
            let root_columns = namespace.objects.iter().filter(|object| {
                object.kind == CatalogObjectKind::Column
                    && object
                        .attributes
                        .get("table_oid")
                        .and_then(serde_json::Value::as_str)
                        == Some(root.id.as_str())
            });
            for column in root_columns {
                if column
                    .attributes
                    .get("generated")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                    || column
                        .attributes
                        .get("identity")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| !value.is_empty())
                    || column
                        .attributes
                        .get("sequence_default_oid")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
                {
                    return Err(PostgresPlanError::InvalidConfig(
                        "partition topology contains generated, identity, or sequence-backed columns",
                    ));
                }
            }
            let default_partition_id = root
                .attributes
                .get("default_partition_oid")
                .and_then(serde_json::Value::as_str);
            let mut leaves = catalog
                .namespaces
                .iter()
                .flat_map(|leaf_namespace| {
                    leaf_namespace.objects.iter().filter_map(move |leaf| {
                        (leaf.kind == CatalogObjectKind::Partition
                            && leaf
                                .attributes
                                .get("partition_parent_oid")
                                .and_then(serde_json::Value::as_str)
                                == Some(root.id.as_str()))
                        .then_some((leaf_namespace, leaf))
                    })
                })
                .map(|(leaf_namespace, leaf)| {
                    if required_catalog_string(leaf, "relkind")? != "r"
                        || required_catalog_string(leaf, "persistence")? != "p"
                    {
                        return Err(PostgresPlanError::InvalidConfig(
                            "partition leaf is not an ordinary logged relation",
                        ));
                    }
                    let bound_text = required_catalog_string(leaf, "partition_bound")?;
                    let bound = parse_partition_bound(bound_text, strategy)?;
                    if matches!(bound, PostgresPartitionBound::Default)
                        != (default_partition_id == Some(leaf.id.as_str()))
                    {
                        return Err(PostgresPlanError::InvalidConfig(
                            "default partition identity differs from its bound",
                        ));
                    }
                    Ok(PostgresPartitionLeaf {
                        catalog_object_id: leaf.id.clone(),
                        table: QualifiedTable {
                            namespace: leaf_namespace.name.clone(),
                            name: leaf.name.clone(),
                        },
                        bound,
                    })
                })
                .collect::<Result<Vec<_>, PostgresPlanError>>()?;
            leaves.sort_by(|left, right| left.table.cmp(&right.table));
            validate_partition_bounds(strategy, &leaves)?;
            if leaves.is_empty() {
                return Err(PostgresPlanError::InvalidConfig(
                    "partition topology has no leaves",
                ));
            }
            let mut topology_relation_ids = BTreeSet::from([root.id.as_str()]);
            topology_relation_ids.extend(leaves.iter().map(|leaf| leaf.catalog_object_id.as_str()));
            for object in catalog
                .namespaces
                .iter()
                .flat_map(|catalog_namespace| catalog_namespace.objects.iter())
            {
                let table_id = object
                    .attributes
                    .get("table_oid")
                    .and_then(serde_json::Value::as_str);
                let referenced_table_id = object
                    .attributes
                    .get("referenced_table_oid")
                    .and_then(serde_json::Value::as_str);
                if (matches!(
                    object.kind,
                    CatalogObjectKind::ForeignKey | CatalogObjectKind::Trigger
                ) && (table_id.is_some_and(|id| topology_relation_ids.contains(id))
                    || referenced_table_id.is_some_and(|id| topology_relation_ids.contains(id))))
                    || (matches!(
                        object.kind,
                        CatalogObjectKind::PrimaryKey
                            | CatalogObjectKind::UniqueConstraint
                            | CatalogObjectKind::CheckConstraint
                    ) && table_id
                        .is_some_and(|id| id != root.id && topology_relation_ids.contains(id))
                        && object
                            .attributes
                            .get("parent_constraint_oid")
                            .is_none_or(serde_json::Value::is_null))
                {
                    return Err(PostgresPlanError::InvalidConfig(
                        "partition topology contains excluded foreign-key, trigger, or local leaf constraint semantics",
                    ));
                }
            }
            topologies.push(PostgresPartitionTopology {
                root_catalog_object_id: root.id.clone(),
                root: QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: root.name.clone(),
                },
                key_column,
                key_type,
                strategy,
                leaves,
            });
        }
    }
    topologies.sort_by(|left, right| left.root.cmp(&right.root));
    Ok(topologies)
}

fn parse_partition_bound(
    text: &str,
    strategy: PostgresPartitionStrategy,
) -> Result<PostgresPartitionBound, PostgresPlanError> {
    let text = text.trim();
    if text == "DEFAULT" {
        return Ok(PostgresPartitionBound::Default);
    }
    match strategy {
        PostgresPartitionStrategy::Range => {
            let body = text
                .strip_prefix("FOR VALUES FROM (")
                .and_then(|value| value.strip_suffix(')'))
                .ok_or(PostgresPlanError::InvalidConfig(
                    "range partition bound is malformed",
                ))?;
            let (lower, upper) =
                body.split_once(") TO (")
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "range partition bound is malformed",
                    ))?;
            Ok(PostgresPartitionBound::Range {
                lower: parse_partition_bound_value(lower)?,
                upper: parse_partition_bound_value(upper)?,
            })
        }
        PostgresPartitionStrategy::List => {
            let body = text
                .strip_prefix("FOR VALUES IN (")
                .and_then(|value| value.strip_suffix(')'))
                .ok_or(PostgresPlanError::InvalidConfig(
                    "list partition bound is malformed",
                ))?;
            let values = body
                .split(',')
                .map(|value| {
                    if value.trim() == "NULL" {
                        Ok(None)
                    } else {
                        parse_partition_integer(value).map(Some)
                    }
                })
                .collect::<Result<Vec<_>, PostgresPlanError>>()?;
            if values.is_empty() {
                return Err(PostgresPlanError::InvalidConfig(
                    "list partition bound is empty",
                ));
            }
            Ok(PostgresPartitionBound::List { values })
        }
        PostgresPartitionStrategy::Hash => {
            let body = text
                .strip_prefix("FOR VALUES WITH (modulus ")
                .and_then(|value| value.strip_suffix(')'))
                .ok_or(PostgresPlanError::InvalidConfig(
                    "hash partition bound is malformed",
                ))?;
            let (modulus, remainder) =
                body.split_once(", remainder ")
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "hash partition bound is malformed",
                    ))?;
            let modulus = modulus.parse::<u32>().map_err(|_| {
                PostgresPlanError::InvalidConfig("hash partition modulus is malformed")
            })?;
            let remainder = remainder.parse::<u32>().map_err(|_| {
                PostgresPlanError::InvalidConfig("hash partition remainder is malformed")
            })?;
            if modulus == 0 || remainder >= modulus {
                return Err(PostgresPlanError::InvalidConfig(
                    "hash partition bound is out of range",
                ));
            }
            Ok(PostgresPartitionBound::Hash { modulus, remainder })
        }
    }
}

fn parse_partition_bound_value(
    value: &str,
) -> Result<PostgresPartitionBoundValue, PostgresPlanError> {
    match value.trim() {
        "MINVALUE" => Ok(PostgresPartitionBoundValue::MinValue),
        "MAXVALUE" => Ok(PostgresPartitionBoundValue::MaxValue),
        value => parse_partition_integer(value).map(PostgresPartitionBoundValue::Value),
    }
}

fn parse_partition_integer(value: &str) -> Result<i64, PostgresPlanError> {
    let value = value.trim();
    let value = value.split_once("::").map_or(value, |(literal, cast)| {
        if matches!(cast.trim(), "smallint" | "integer" | "bigint") {
            literal.trim()
        } else {
            ""
        }
    });
    let value = value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .unwrap_or(value);
    value
        .parse()
        .map_err(|_| PostgresPlanError::InvalidConfig("partition integer bound is malformed"))
}

fn validate_partition_bounds(
    strategy: PostgresPartitionStrategy,
    leaves: &[PostgresPartitionLeaf],
) -> Result<(), PostgresPlanError> {
    let defaults = leaves
        .iter()
        .filter(|leaf| matches!(leaf.bound, PostgresPartitionBound::Default))
        .count();
    if defaults > 1 {
        return Err(PostgresPlanError::InvalidConfig(
            "partition topology has multiple default leaves",
        ));
    }
    match strategy {
        PostgresPartitionStrategy::Range => {
            let mut ranges = leaves
                .iter()
                .filter_map(|leaf| match &leaf.bound {
                    PostgresPartitionBound::Range { lower, upper } => {
                        Some((lower.clone(), upper.clone()))
                    }
                    PostgresPartitionBound::Default => None,
                    _ => Some((
                        PostgresPartitionBoundValue::MaxValue,
                        PostgresPartitionBoundValue::MinValue,
                    )),
                })
                .collect::<Vec<_>>();
            ranges.sort();
            if ranges.iter().any(|(lower, upper)| lower >= upper)
                || ranges.windows(2).any(|pair| pair[0].1 > pair[1].0)
            {
                return Err(PostgresPlanError::InvalidConfig(
                    "range partition bounds overlap or are invalid",
                ));
            }
        }
        PostgresPartitionStrategy::List => {
            let mut values = BTreeSet::new();
            for bound in leaves.iter().map(|leaf| &leaf.bound) {
                match bound {
                    PostgresPartitionBound::List {
                        values: leaf_values,
                    } => {
                        if leaf_values.iter().any(|value| !values.insert(*value)) {
                            return Err(PostgresPlanError::InvalidConfig(
                                "list partition bounds overlap",
                            ));
                        }
                    }
                    PostgresPartitionBound::Default => {}
                    _ => {
                        return Err(PostgresPlanError::InvalidConfig(
                            "partition bound does not match list strategy",
                        ));
                    }
                }
            }
        }
        PostgresPartitionStrategy::Hash => {
            let mut modulus = None;
            let mut remainders = BTreeSet::new();
            for bound in leaves.iter().map(|leaf| &leaf.bound) {
                match bound {
                    PostgresPartitionBound::Hash {
                        modulus: leaf_modulus,
                        remainder,
                    } => {
                        if modulus
                            .replace(*leaf_modulus)
                            .is_some_and(|value| value != *leaf_modulus)
                            || !remainders.insert(*remainder)
                        {
                            return Err(PostgresPlanError::InvalidConfig(
                                "hash partition bounds have mixed moduli or duplicate remainders",
                            ));
                        }
                    }
                    PostgresPartitionBound::Default => {
                        return Err(PostgresPlanError::InvalidConfig(
                            "hash partitioning does not support a default leaf",
                        ));
                    }
                    _ => {
                        return Err(PostgresPlanError::InvalidConfig(
                            "partition bound does not match hash strategy",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn partition_topology_semantically_equal(
    expected: &PostgresPartitionTopology,
    actual: &PostgresPartitionTopology,
) -> bool {
    expected.root == actual.root
        && expected.key_column == actual.key_column
        && expected.key_type == actual.key_type
        && expected.strategy == actual.strategy
        && expected.leaves.len() == actual.leaves.len()
        && expected.leaves.iter().all(|expected_leaf| {
            actual.leaves.iter().any(|actual_leaf| {
                expected_leaf.table == actual_leaf.table && expected_leaf.bound == actual_leaf.bound
            })
        })
}

/// Parse the conservative stored-generated-column subset from a catalog.
pub fn postgres_generated_columns(
    catalog: &VendorCatalog,
) -> Result<Vec<PostgresGeneratedColumn>, PostgresPlanError> {
    if catalog.dialect != "postgresql" {
        return Err(PostgresPlanError::InvalidConfig(
            "generated-column metadata requires a PostgreSQL catalog",
        ));
    }
    let mut generated = Vec::new();
    for namespace in &catalog.namespaces {
        for object in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Column)
        {
            let mode = object
                .attributes
                .get("generated")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if mode.is_empty() {
                continue;
            }
            if mode != "s" {
                return Err(PostgresPlanError::InvalidConfig(
                    "only stored PostgreSQL generated columns are supported",
                ));
            }
            if object
                .attributes
                .get("identity")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.is_empty())
                || object
                    .attributes
                    .get("default")
                    .is_some_and(|value| !value.is_null())
            {
                return Err(PostgresPlanError::InvalidConfig(
                    "generated column also has identity or default semantics",
                ));
            }
            if required_catalog_string(object, "type_schema")? != "pg_catalog" {
                return Err(PostgresPlanError::InvalidConfig(
                    "generated column uses a non-pg_catalog type",
                ));
            }
            let mut dependencies: Vec<PostgresGeneratedDependency> = serde_json::from_value(
                object
                    .attributes
                    .get("generated_dependencies")
                    .cloned()
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "generated column dependency inventory is absent",
                    ))?,
            )?;
            dependencies.sort();
            dependencies.dedup();
            if dependencies.is_empty() {
                return Err(PostgresPlanError::InvalidConfig(
                    "generated column dependency inventory is empty",
                ));
            }
            let expression = required_catalog_string(object, "generated_expression")?.to_owned();
            if expression.trim().is_empty() || expression.contains(';') {
                return Err(PostgresPlanError::InvalidConfig(
                    "generated expression is empty or contains a statement separator",
                ));
            }
            let table_oid = required_catalog_string(object, "table_oid")?;
            for dependency in &dependencies {
                let valid = match dependency.kind {
                    PostgresGeneratedDependencyKind::Column => dependency.identity.strip_prefix("column:").is_some_and(|name| namespace.objects.iter().any(|candidate| {
                        candidate.name.as_str() == name
                            && candidate.kind == CatalogObjectKind::Column
                            && candidate.attributes.get("table_oid").and_then(serde_json::Value::as_str)
                                == Some(table_oid)
                            && candidate.attributes.get("generated").and_then(serde_json::Value::as_str)
                                .is_none_or(str::is_empty)
                    })),
                    PostgresGeneratedDependencyKind::Function => dependency.identity.starts_with("function:pg_catalog."),
                    PostgresGeneratedDependencyKind::Operator => dependency.identity.starts_with("operator:pg_catalog.") && dependency.identity.contains(":function=pg_catalog."),
                    PostgresGeneratedDependencyKind::Type => dependency.identity.starts_with("type:pg_catalog."),
                    PostgresGeneratedDependencyKind::Collation => dependency.identity.starts_with("collation:pg_catalog.") && dependency.identity.contains(":deterministic=true") && dependency.identity.split(":version=").nth(1).is_some_and(|suffix| {
                        let mut parts = suffix.split(":actual=");
                        matches!((parts.next(), parts.next()), (Some(version), Some(actual)) if version.is_empty() || version == actual)
                    }),
                };
                if !valid {
                    return Err(PostgresPlanError::UnsupportedGeneratedDependency(
                        dependency.identity.clone(),
                    ));
                }
            }
            generated.push(PostgresGeneratedColumn {
                catalog_object_id: object.id.clone(),
                table: qualified_table_for_oid(catalog, table_oid)?,
                column: object.name.clone(),
                expression,
                data_type: String::from_utf8(object.definition.clone()).map_err(|_| {
                    PostgresPlanError::InvalidConfig(
                        "generated column type declaration is not UTF-8",
                    )
                })?,
                collation_schema: optional_catalog_identifier(object, "collation_schema")?,
                collation: optional_catalog_identifier(object, "collation")?,
                dependencies,
            });
        }
    }
    generated.sort_by(|left, right| {
        left.table
            .cmp(&right.table)
            .then_with(|| left.column.cmp(&right.column))
    });
    Ok(generated)
}

fn optional_catalog_identifier(
    object: &CatalogObject,
    name: &'static str,
) -> Result<Option<Identifier>, PostgresPlanError> {
    object
        .attributes
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(Identifier::new)
        .transpose()
        .map_err(PostgresPlanError::from)
}

/// Parse and validate every sequence in a PostgreSQL vendor catalog.
pub fn postgres_sequences(
    catalog: &VendorCatalog,
) -> Result<Vec<PostgresSequence>, PostgresPlanError> {
    if catalog.dialect != "postgresql" {
        return Err(PostgresPlanError::InvalidConfig(
            "sequence metadata requires a PostgreSQL catalog",
        ));
    }
    let mut sequences = Vec::new();
    for namespace in &catalog.namespaces {
        for object in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Sequence)
        {
            let ownership = match object.attributes.get("ownership") {
                Some(serde_json::Value::Null) | None => None,
                Some(value) => Some(serde_json::from_value(value.clone())?),
            };
            let ownership_count = object
                .attributes
                .get("ownership_count")
                .and_then(serde_json::Value::as_u64)
                .ok_or(PostgresPlanError::InvalidConfig(
                    "sequence ownership count is absent or malformed",
                ))?;
            if ownership_count > 1 || (ownership_count == 1) != ownership.is_some() {
                return Err(PostgresPlanError::InvalidConfig(
                    "sequence ownership is dangling or multiply defined",
                ));
            }
            let sequence = PostgresSequence {
                catalog_object_id: object.id.clone(),
                namespace: namespace.name.clone(),
                name: object.name.clone(),
                persistence: required_catalog_string(object, "persistence")?.to_owned(),
                data_type: required_catalog_string(object, "type")?.to_owned(),
                start_value: required_catalog_i64(object, "start")?,
                increment: required_catalog_i64(object, "increment")?,
                minimum_value: required_catalog_i64(object, "minimum")?,
                maximum_value: required_catalog_i64(object, "maximum")?,
                cache_size: required_catalog_i64(object, "cache")?,
                cycle: required_catalog_bool(object, "cycle")?,
                last_value: required_catalog_i64(object, "last_value")?,
                is_called: required_catalog_bool(object, "is_called")?,
                ownership,
            };
            validate_sequence(&sequence)?;
            sequences.push(sequence);
        }
    }
    sequences.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.catalog_object_id.cmp(&right.catalog_object_id))
    });
    validate_sequence_links(catalog, &sequences)?;
    Ok(sequences)
}

pub fn postgres_programmable_objects(
    catalog: &VendorCatalog,
) -> Result<Vec<PostgresProgrammableObject>, PostgresPlanError> {
    let mut objects = Vec::new();
    for namespace in &catalog.namespaces {
        for object in namespace.objects.iter().filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::View | CatalogObjectKind::Routine
            ) && object.attributes.contains_key("postgres_durable_ast")
        }) {
            let ast_json = object
                .attributes
                .get("postgres_durable_ast")
                .and_then(serde_json::Value::as_str)
                .ok_or(PostgresPlanError::InvalidConfig(
                    "programmable object omits its durable AST",
                ))?;
            let ast = PostgresDurableAst::from_canonical_json(ast_json)
                .map_err(|_| PostgresPlanError::InvalidConfig("programmable AST is invalid"))?;
            let authoritative_dependencies: Vec<PostgresProgrammableIdentity> =
                serde_json::from_value(
                    object
                        .attributes
                        .get("postgres_authoritative_dependencies")
                        .cloned()
                        .ok_or(PostgresPlanError::InvalidConfig(
                            "programmable object omits authoritative dependencies",
                        ))?,
                )?;
            let authoritative_identity: PostgresProgrammableIdentity = serde_json::from_value(
                object
                    .attributes
                    .get("postgres_authoritative_identity")
                    .cloned()
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "programmable object omits authoritative identity",
                    ))?,
            )?;
            if !authoritative_dependencies
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            {
                return Err(PostgresPlanError::InvalidConfig(
                    "programmable dependencies are not sorted and unique",
                ));
            }
            let hints = ast.syntactic_dependency_hints();
            let relation_hints_resolved = hints.relations.iter().all(|identity| {
                identity
                    .parts
                    .as_slice()
                    .first()
                    .zip(identity.parts.get(1))
                    .is_some_and(|(namespace, name)| {
                        authoritative_dependencies.iter().any(|dependency| {
                            matches!(
                                dependency,
                                PostgresProgrammableIdentity::Relation {
                                    namespace: dependency_namespace,
                                    name: dependency_name,
                                } if dependency_namespace.as_str() == namespace.value
                                    && dependency_name.as_str() == name.value
                            )
                        })
                    })
            });
            let function_hints_resolved = hints.functions.iter().all(|identity| {
                identity
                    .parts
                    .as_slice()
                    .first()
                    .zip(identity.parts.get(1))
                    .is_some_and(|(namespace, name)| {
                        authoritative_dependencies.iter().any(|dependency| {
                            matches!(
                                dependency,
                                PostgresProgrammableIdentity::Function {
                                    namespace: dependency_namespace,
                                    name: dependency_name,
                                    ..
                                } if dependency_namespace.as_str() == namespace.value
                                    && dependency_name.as_str() == name.value
                            )
                        })
                    })
            });
            if !relation_hints_resolved || !function_hints_resolved {
                return Err(PostgresPlanError::InvalidConfig(
                    "syntactic dependency hint is absent from authoritative pg_depend closure",
                ));
            }
            objects.push(PostgresProgrammableObject {
                catalog_object_id: object.id.clone(),
                namespace: namespace.name.clone(),
                name: object.name.clone(),
                ast,
                authoritative_identity,
                authoritative_dependencies,
            });
        }
    }
    objects.sort_by(|left, right| left.catalog_object_id.cmp(&right.catalog_object_id));
    Ok(objects)
}

fn programmable_object_semantically_equal(
    expected: &PostgresProgrammableObject,
    actual: &PostgresProgrammableObject,
) -> bool {
    expected.namespace == actual.namespace
        && expected.name == actual.name
        && expected.ast == actual.ast
        && expected.authoritative_identity == actual.authoritative_identity
        && expected.authoritative_dependencies == actual.authoritative_dependencies
}

fn programmable_builtin_dependency(
    dependency: &PostgresProgrammableIdentity,
    object_namespace: &Identifier,
) -> bool {
    match dependency {
        PostgresProgrammableIdentity::Function { namespace, .. }
        | PostgresProgrammableIdentity::Type { namespace, .. }
        | PostgresProgrammableIdentity::Collation { namespace, .. }
        | PostgresProgrammableIdentity::Operator { namespace, .. } => {
            namespace.as_str() == "pg_catalog"
        }
        PostgresProgrammableIdentity::Namespace { name } => name == object_namespace,
        PostgresProgrammableIdentity::Language { name } => name.as_str() == "sql",
        PostgresProgrammableIdentity::Relation { .. } => false,
    }
}

fn view_security_is_supported(
    persistence: &str,
    relation_options_and_acl_clear: bool,
    column_acl_clear: bool,
) -> bool {
    persistence == "p" && relation_options_and_acl_clear && column_acl_clear
}

fn unsupported_view_security(
    object_id: &str,
    persistence: &str,
    relation_options_and_acl_clear: bool,
    column_acl_clear: bool,
) -> Option<UnsupportedObject> {
    if column_acl_clear && persistence == "p" && relation_options_and_acl_clear {
        return None;
    }
    Some(UnsupportedObject {
        code: if column_acl_clear {
            UnsupportedObjectCode::ViewSecurity
        } else {
            UnsupportedObjectCode::ViewColumnAcl
        },
        object_id: object_id.to_owned(),
        object_kind: if column_acl_clear {
            "view_security"
        } else {
            "view_column_acl"
        }
        .into(),
        reason: if column_acl_clear {
            "view has unsupported persistence, options, privileges, or row security"
        } else {
            "custom view-column privileges are not implemented"
        }
        .into(),
        required_semantics: true,
    })
}

fn validate_sequence_links(
    catalog: &VendorCatalog,
    sequences: &[PostgresSequence],
) -> Result<(), PostgresPlanError> {
    let mut linked_columns = BTreeSet::new();
    for sequence in sequences {
        if let Some(ownership) = &sequence.ownership {
            if !linked_columns.insert((ownership.table.clone(), ownership.column.clone())) {
                return Err(PostgresPlanError::InvalidConfig(
                    "multiple sequences own one table column",
                ));
            }
        }
    }
    for namespace in &catalog.namespaces {
        for column in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Column)
        {
            let identity = column
                .attributes
                .get("identity")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let sequence_default_id = column
                .attributes
                .get("sequence_default_oid")
                .and_then(serde_json::Value::as_str);
            let serial = identity.is_empty() && sequence_default_id.is_some();
            if identity.is_empty() && !serial {
                continue;
            }
            let table =
                qualified_table_for_oid(catalog, required_catalog_string(column, "table_oid")?)?;
            let matching = sequences
                .iter()
                .filter_map(|sequence| sequence.ownership.as_ref())
                .filter(|ownership| ownership.table == table && ownership.column == column.name)
                .collect::<Vec<_>>();
            let exact = matches!(
                (identity, serial, matching.as_slice()),
                ("a", false, [ownership])
                    if ownership.kind == PostgresSequenceOwnershipKind::IdentityAlways
            ) || matches!(
                (identity, serial, matching.as_slice()),
                ("d", false, [ownership])
                    if ownership.kind == PostgresSequenceOwnershipKind::IdentityByDefault
            ) || matches!(
                (identity, serial, matching.as_slice()),
                ("", true, [ownership])
                    if ownership.kind == PostgresSequenceOwnershipKind::Serial
                        && sequences.iter().any(|sequence| {
                            sequence.catalog_object_id == sequence_default_id.unwrap_or_default()
                                && sequence.ownership.as_ref() == Some(*ownership)
                        })
            );
            if !exact {
                return Err(PostgresPlanError::InvalidConfig(
                    "identity or serial column has no exact sequence ownership link",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_sequence(sequence: &PostgresSequence) -> Result<(), PostgresPlanError> {
    if sequence.persistence != "p" {
        return Err(PostgresPlanError::InvalidConfig(
            "only logged PostgreSQL sequences are supported",
        ));
    }
    if !matches!(
        sequence.data_type.as_str(),
        "smallint" | "integer" | "bigint"
    ) {
        return Err(PostgresPlanError::InvalidConfig(
            "sequence has an unsupported data type",
        ));
    }
    if sequence.increment == 0 || sequence.cache_size <= 0 {
        return Err(PostgresPlanError::InvalidConfig(
            "sequence increment and cache must be valid",
        ));
    }
    if sequence.minimum_value >= sequence.maximum_value
        || !(sequence.minimum_value..=sequence.maximum_value).contains(&sequence.start_value)
        || !(sequence.minimum_value..=sequence.maximum_value).contains(&sequence.last_value)
    {
        return Err(PostgresPlanError::InvalidConfig(
            "sequence bounds or state are invalid",
        ));
    }
    let (type_minimum, type_maximum) = match sequence.data_type.as_str() {
        "smallint" => (i64::from(i16::MIN), i64::from(i16::MAX)),
        "integer" => (i64::from(i32::MIN), i64::from(i32::MAX)),
        "bigint" => (i64::MIN, i64::MAX),
        _ => unreachable!("data type was checked above"),
    };
    if sequence.minimum_value < type_minimum || sequence.maximum_value > type_maximum {
        return Err(PostgresPlanError::InvalidConfig(
            "sequence bounds exceed its data type",
        ));
    }
    Ok(())
}

impl PostgresSequence {
    /// Compute the next value without mutating the source or target sequence.
    pub fn expected_next_value(&self) -> Result<Option<i64>, PostgresPlanError> {
        validate_sequence(self)?;
        if !self.is_called {
            return Ok(Some(self.last_value));
        }
        let candidate = self.last_value.checked_add(self.increment);
        let within_bounds =
            candidate.filter(|value| (self.minimum_value..=self.maximum_value).contains(value));
        if within_bounds.is_some() {
            return Ok(within_bounds);
        }
        if self.cycle {
            Ok(Some(if self.increment > 0 {
                self.minimum_value
            } else {
                self.maximum_value
            }))
        } else {
            Ok(None)
        }
    }
}

fn required_catalog_i64(
    object: &CatalogObject,
    name: &'static str,
) -> Result<i64, PostgresPlanError> {
    let value = required_catalog_string(object, name)?;
    value.parse().map_err(|_| {
        PostgresPlanError::InvalidConfig("sequence numeric catalog metadata is malformed")
    })
}

fn required_catalog_bool(
    object: &CatalogObject,
    name: &'static str,
) -> Result<bool, PostgresPlanError> {
    object
        .attributes
        .get(name)
        .and_then(serde_json::Value::as_bool)
        .ok_or(PostgresPlanError::InvalidConfig(
            "sequence boolean catalog metadata is malformed",
        ))
}

fn sequence_create_statements(sequence: &PostgresSequence) -> Vec<String> {
    let qualified_sequence = format!(
        "{}.{}",
        quote_identifier(&sequence.namespace),
        quote_identifier(&sequence.name)
    );
    let options = sequence_options(sequence);
    match &sequence.ownership {
        Some(ownership)
            if matches!(
                ownership.kind,
                PostgresSequenceOwnershipKind::IdentityAlways
                    | PostgresSequenceOwnershipKind::IdentityByDefault
            ) =>
        {
            let generation = match ownership.kind {
                PostgresSequenceOwnershipKind::IdentityAlways => "ALWAYS",
                PostgresSequenceOwnershipKind::IdentityByDefault => "BY DEFAULT",
                PostgresSequenceOwnershipKind::Serial => unreachable!(),
            };
            vec![format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} ADD GENERATED {generation} AS IDENTITY (SEQUENCE NAME {qualified_sequence} {options})",
                    quote_identifier(&ownership.table.namespace),
                    quote_identifier(&ownership.table.name),
                    quote_identifier(&ownership.column),
                )]
        }
        ownership => {
            let mut statements = vec![format!(
                "CREATE SEQUENCE {qualified_sequence} AS {} {options}",
                sequence.data_type
            )];
            if let Some(ownership) = ownership {
                statements.push(format!(
                    "ALTER SEQUENCE {qualified_sequence} OWNED BY {}.{}.{}",
                    quote_identifier(&ownership.table.namespace),
                    quote_identifier(&ownership.table.name),
                    quote_identifier(&ownership.column)
                ));
                statements.push(format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT nextval({}::regclass)",
                    quote_identifier(&ownership.table.namespace),
                    quote_identifier(&ownership.table.name),
                    quote_identifier(&ownership.column),
                    quote_literal(&qualified_regclass_name(
                        &sequence.namespace,
                        &sequence.name
                    ))
                ));
            }
            statements
        }
    }
}

fn standalone_sequence_create_statement(sequence: &PostgresSequence) -> String {
    format!(
        "CREATE SEQUENCE {}.{} AS {} {}",
        quote_identifier(&sequence.namespace),
        quote_identifier(&sequence.name),
        sequence.data_type,
        sequence_options(sequence)
    )
}

fn serial_sequence_association_statements(sequence: &PostgresSequence) -> Vec<String> {
    let Some(ownership) = &sequence.ownership else {
        return Vec::new();
    };
    if ownership.kind != PostgresSequenceOwnershipKind::Serial {
        return Vec::new();
    }
    let qualified_sequence = format!(
        "{}.{}",
        quote_identifier(&sequence.namespace),
        quote_identifier(&sequence.name)
    );
    vec![
        format!(
            "ALTER SEQUENCE {qualified_sequence} OWNED BY {}.{}.{}",
            quote_identifier(&ownership.table.namespace),
            quote_identifier(&ownership.table.name),
            quote_identifier(&ownership.column)
        ),
        format!(
            "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT nextval({}::regclass)",
            quote_identifier(&ownership.table.namespace),
            quote_identifier(&ownership.table.name),
            quote_identifier(&ownership.column),
            quote_literal(&qualified_regclass_name(
                &sequence.namespace,
                &sequence.name
            ))
        ),
    ]
}

fn sequence_options(sequence: &PostgresSequence) -> String {
    format!(
        "INCREMENT BY {} MINVALUE {} MAXVALUE {} START WITH {} CACHE {} {}",
        sequence.increment,
        sequence.minimum_value,
        sequence.maximum_value,
        sequence.start_value,
        sequence.cache_size,
        if sequence.cycle { "CYCLE" } else { "NO CYCLE" }
    )
}

fn inspect_generated_column(
    client: &mut impl postgres::GenericClient,
    expected: &PostgresGeneratedColumn,
) -> ConnectionResult<PostgresGeneratedColumnState> {
    let rows = client
        .query(
            "SELECT a.attgenerated::text, pg_catalog.format_type(a.atttypid, a.atttypmod), cn.nspname, co.collname, pg_get_expr(ad.adbin, ad.adrelid, false), a.attrelid, a.attnum FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum LEFT JOIN pg_collation co ON co.oid=a.attcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace WHERE n.nspname=$1 AND c.relname=$2 AND a.attname=$3 AND a.attnum>0 AND NOT a.attisdropped",
            &[&expected.table.namespace.as_str(), &expected.table.name.as_str(), &expected.column.as_str()],
        )
        .map_err(database_error)?;
    let Some(row) = rows.first() else {
        return Ok(PostgresGeneratedColumnState::Absent);
    };
    if rows.len() != 1
        || row.get::<_, String>(0) != "s"
        || row.get::<_, String>(1) != expected.data_type
        || row.get::<_, Option<String>>(2).as_deref()
            != expected.collation_schema.as_ref().map(Identifier::as_str)
        || row.get::<_, Option<String>>(3).as_deref()
            != expected.collation.as_ref().map(Identifier::as_str)
        || row.get::<_, Option<String>>(4).as_deref() != Some(expected.expression.as_str())
    {
        return Ok(PostgresGeneratedColumnState::Different);
    }
    let table_oid: u32 = row.get(5);
    let attribute_number: i16 = row.get(6);
    let dependency_rows = client
        .query(
            "SELECT CASE d.refclassid WHEN 'pg_class'::regclass THEN 'column' WHEN 'pg_proc'::regclass THEN 'function' WHEN 'pg_operator'::regclass THEN 'operator' WHEN 'pg_type'::regclass THEN 'type' WHEN 'pg_collation'::regclass THEN 'collation' ELSE 'unknown' END,
                    CASE d.refclassid
                      WHEN 'pg_class'::regclass THEN 'column:' || refa.attname
                      WHEN 'pg_proc'::regclass THEN 'function:' || pn.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')->' || pg_catalog.format_type(p.prorettype, NULL)
                      WHEN 'pg_operator'::regclass THEN 'operator:' || opn.nspname || '.' || o.oprname || '(' || pg_catalog.format_type(o.oprleft, NULL) || ',' || pg_catalog.format_type(o.oprright, NULL) || ')->' || pg_catalog.format_type(o.oprresult, NULL) || ':function=' || ofn_n.nspname || '.' || ofn.proname || '(' || pg_get_function_identity_arguments(ofn.oid) || ')'
                      WHEN 'pg_type'::regclass THEN 'type:' || tyn.nspname || '.' || ty.typname
                      WHEN 'pg_collation'::regclass THEN 'collation:' || cn.nspname || '.' || co.collname || ':provider=' || co.collprovider::text || ':deterministic=' || co.collisdeterministic::text || ':version=' || COALESCE(co.collversion, '') || ':actual=' || COALESCE(pg_collation_actual_version(co.oid), '')
                      ELSE d.refclassid::regclass::text || ':' || d.refobjid::text || ':' || d.refobjsubid::text END,
                    CASE d.refclassid
                      WHEN 'pg_class'::regclass THEN d.refobjid=$1 AND d.refobjsubid>0 AND refa.attgenerated=''
                      WHEN 'pg_proc'::regclass THEN pn.nspname='pg_catalog' AND p.provolatile='i' AND NOT p.prosecdef
                      WHEN 'pg_operator'::regclass THEN opn.nspname='pg_catalog' AND ofn_n.nspname='pg_catalog' AND ofn.provolatile='i' AND NOT ofn.prosecdef
                      WHEN 'pg_type'::regclass THEN tyn.nspname='pg_catalog'
                      WHEN 'pg_collation'::regclass THEN cn.nspname='pg_catalog' AND co.collisdeterministic AND (co.collversion IS NULL OR co.collversion IS NOT DISTINCT FROM pg_collation_actual_version(co.oid))
                      ELSE false END
             FROM pg_attrdef ad JOIN pg_depend d ON d.classid='pg_attrdef'::regclass AND d.objid=ad.oid AND d.deptype IN ('n','a','i')
             LEFT JOIN pg_attribute refa ON d.refclassid='pg_class'::regclass AND refa.attrelid=d.refobjid AND refa.attnum=d.refobjsubid
             LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid LEFT JOIN pg_namespace pn ON pn.oid=p.pronamespace
             LEFT JOIN pg_operator o ON d.refclassid='pg_operator'::regclass AND o.oid=d.refobjid LEFT JOIN pg_namespace opn ON opn.oid=o.oprnamespace LEFT JOIN pg_proc ofn ON ofn.oid=o.oprcode LEFT JOIN pg_namespace ofn_n ON ofn_n.oid=ofn.pronamespace
             LEFT JOIN pg_type ty ON d.refclassid='pg_type'::regclass AND ty.oid=d.refobjid LEFT JOIN pg_namespace tyn ON tyn.oid=ty.typnamespace
             LEFT JOIN pg_collation co ON d.refclassid='pg_collation'::regclass AND co.oid=d.refobjid LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace
             WHERE ad.adrelid=$1 AND ad.adnum=$2 AND NOT (d.refclassid='pg_class'::regclass AND d.refobjid=$1 AND d.refobjsubid=$2) ORDER BY 1,2",
            &[&table_oid, &attribute_number],
        )
        .map_err(database_error)?;
    let mut dependencies = Vec::new();
    for row in dependency_rows {
        if !row.get::<_, bool>(2) {
            return Ok(PostgresGeneratedColumnState::Different);
        }
        let kind = match row.get::<_, String>(0).as_str() {
            "column" => PostgresGeneratedDependencyKind::Column,
            "function" => PostgresGeneratedDependencyKind::Function,
            "operator" => PostgresGeneratedDependencyKind::Operator,
            "type" => PostgresGeneratedDependencyKind::Type,
            "collation" => PostgresGeneratedDependencyKind::Collation,
            _ => return Ok(PostgresGeneratedColumnState::Different),
        };
        dependencies.push(PostgresGeneratedDependency {
            kind,
            identity: row.get(1),
        });
    }
    dependencies.sort();
    dependencies.dedup();
    Ok(if dependencies == expected.dependencies {
        PostgresGeneratedColumnState::Exact
    } else {
        PostgresGeneratedColumnState::Different
    })
}

fn inspect_sequence(
    client: &mut impl postgres::GenericClient,
    expected: &PostgresSequence,
) -> ConnectionResult<PostgresSequenceState> {
    let rows = client
        .query(
            "SELECT c.relkind::text, c.relpersistence::text, CASE WHEN c.relkind = 'S' THEN pg_catalog.format_type(s.seqtypid, NULL) END, s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcache, s.seqcycle, c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_sequence s ON s.seqrelid = c.oid WHERE n.nspname = $1 AND c.relname = $2",
            &[&expected.namespace.as_str(), &expected.name.as_str()],
        )
        .map_err(database_error)?;
    let Some(row) = rows.first() else {
        return Ok(PostgresSequenceState::Absent);
    };
    if rows.len() != 1
        || row.get::<_, String>(0) != "S"
        || row.get::<_, String>(1) != expected.persistence
        || row.get::<_, Option<String>>(2).as_deref() != Some(expected.data_type.as_str())
        || row.get::<_, Option<i64>>(3) != Some(expected.start_value)
        || row.get::<_, Option<i64>>(4) != Some(expected.increment)
        || row.get::<_, Option<i64>>(5) != Some(expected.minimum_value)
        || row.get::<_, Option<i64>>(6) != Some(expected.maximum_value)
        || row.get::<_, Option<i64>>(7) != Some(expected.cache_size)
        || row.get::<_, Option<bool>>(8) != Some(expected.cycle)
    {
        return Ok(PostgresSequenceState::Different);
    }
    let sequence_oid: u32 = row.get(9);
    let ownership_rows = client
        .query(
            "SELECT tn.nspname, t.relname, a.attname, a.attidentity::text, d.deptype::text, EXISTS (SELECT 1 FROM pg_attrdef ad JOIN pg_depend dd ON dd.classid = 'pg_attrdef'::regclass AND dd.objid = ad.oid AND dd.refclassid = 'pg_class'::regclass AND dd.refobjid = $1 AND dd.deptype = 'n' WHERE ad.adrelid = a.attrelid AND ad.adnum = a.attnum) FROM pg_depend d JOIN pg_class t ON t.oid = d.refobjid JOIN pg_namespace tn ON tn.oid = t.relnamespace JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid WHERE d.classid = 'pg_class'::regclass AND d.objid = $1 AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i') ORDER BY tn.nspname, t.relname, a.attnum",
            &[&sequence_oid],
        )
        .map_err(database_error)?;
    let actual_ownership = match ownership_rows.as_slice() {
        [] => None,
        [row] => {
            let identity: String = row.get(3);
            let dependency_type: String = row.get(4);
            let default_depends_on_sequence: bool = row.get(5);
            let kind = match (identity.as_str(), dependency_type.as_str()) {
                ("a", "i") => PostgresSequenceOwnershipKind::IdentityAlways,
                ("d", "i") => PostgresSequenceOwnershipKind::IdentityByDefault,
                ("", "a") if default_depends_on_sequence => PostgresSequenceOwnershipKind::Serial,
                _ => return Ok(PostgresSequenceState::Different),
            };
            Some(PostgresSequenceOwnership {
                table: QualifiedTable {
                    namespace: Identifier::new(row.get::<_, String>(0))
                        .map_err(|error| ConnectionError::Database(error.to_string()))?,
                    name: Identifier::new(row.get::<_, String>(1))
                        .map_err(|error| ConnectionError::Database(error.to_string()))?,
                },
                column: Identifier::new(row.get::<_, String>(2))
                    .map_err(|error| ConnectionError::Database(error.to_string()))?,
                kind,
            })
        }
        _ => return Ok(PostgresSequenceState::Different),
    };
    if actual_ownership != expected.ownership {
        return Ok(PostgresSequenceState::Different);
    }
    let state = client
        .query_one(
            &format!(
                "SELECT last_value, is_called FROM {}.{}",
                quote_identifier(&expected.namespace),
                quote_identifier(&expected.name)
            ),
            &[],
        )
        .map_err(database_error)?;
    let last_value = state.get::<_, i64>(0);
    let is_called = state.get::<_, bool>(1);
    if last_value == expected.last_value && is_called == expected.is_called {
        Ok(PostgresSequenceState::ExactState)
    } else if last_value == expected.start_value && !is_called {
        Ok(PostgresSequenceState::ExactConfig)
    } else {
        Ok(PostgresSequenceState::Different)
    }
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn qualified_regclass_name(namespace: &Identifier, name: &Identifier) -> String {
    format!("{}.{}", quote_identifier(namespace), quote_identifier(name))
}

/// A conservative, portable PostgreSQL ordinary B-tree index contract.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresIndex {
    pub catalog_object_id: String,
    pub name: Identifier,
    pub table: QualifiedTable,
    pub columns: Vec<Identifier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresIndexState {
    Absent,
    Exact,
    Different,
}

/// Parse all supported post-data ordinary indexes from typed catalog metadata.
pub fn postgres_post_data_indexes(
    catalog: &VendorCatalog,
) -> Result<Vec<PostgresIndex>, PostgresPlanError> {
    if catalog.dialect != "postgresql" {
        return Err(PostgresPlanError::InvalidConfig(
            "ordinary-index metadata requires a PostgreSQL catalog",
        ));
    }
    let mut indexes = Vec::new();
    for namespace in &catalog.namespaces {
        for object in namespace.objects.iter().filter(|object| {
            object.kind == CatalogObjectKind::Index
                && object
                    .attributes
                    .get("constraint_oid")
                    .is_none_or(serde_json::Value::is_null)
        }) {
            if object
                .attributes
                .get("unique")
                .and_then(serde_json::Value::as_bool)
                != Some(false)
            {
                continue;
            }
            let columns = ordinary_index_columns(object, Some(namespace))?;
            let table =
                qualified_table_for_oid(catalog, required_catalog_string(object, "table_oid")?)?;
            indexes.push(PostgresIndex {
                catalog_object_id: object.id.clone(),
                name: object.name.clone(),
                table,
                columns,
            });
        }
    }
    indexes.sort_by(|left, right| {
        left.table
            .cmp(&right.table)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.catalog_object_id.cmp(&right.catalog_object_id))
    });
    Ok(indexes)
}

fn ordinary_index_create_statement(index: &PostgresIndex) -> String {
    format!(
        "CREATE INDEX {} ON {}.{} USING btree ({})",
        quote_identifier(&index.name),
        quote_identifier(&index.table.namespace),
        quote_identifier(&index.table.name),
        index
            .columns
            .iter()
            .map(quote_identifier)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn inspect_index(
    client: &mut impl postgres::GenericClient,
    expected: &PostgresIndex,
) -> ConnectionResult<PostgresIndexState> {
    if expected.columns.is_empty() {
        return Err(ConnectionError::InvalidRequest(format!(
            "ordinary index {} has no columns",
            expected.name
        )));
    }
    let occupied_kind = client
        .query_opt(
            "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
            &[&expected.table.namespace.as_str(), &expected.name.as_str()],
        )
        .map_err(database_error)?
        .map(|row| row.get::<_, String>(0));
    let Some(occupied_kind) = occupied_kind else {
        return Ok(PostgresIndexState::Absent);
    };
    if occupied_kind != "i" && occupied_kind != "I" {
        return Ok(PostgresIndexState::Different);
    }
    let rows = client
        .query(
            "SELECT tn.nspname, tc.relname, i.indisunique, i.indisprimary, i.indisvalid, i.indisready, i.indislive, i.indimmediate, i.indisclustered, i.indisreplident, i.indisexclusion, i.indnullsnotdistinct, am.amname, ci.relpersistence::text, ci.reloptions IS NULL, ci.reltablespace = 0, i.indpred IS NULL, i.indexprs IS NULL, i.indnkeyatts, i.indnatts, ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum, ordinality) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE k.ordinality <= i.indnkeyatts ORDER BY k.ordinality)::text[], NOT EXISTS (SELECT 1 FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum, ordinality) WHERE k.ordinality > i.indnkeyatts), NOT EXISTS (SELECT 1 FROM unnest(i.indoption::smallint[]) option_value WHERE option_value <> 0), NOT EXISTS (SELECT 1 FROM unnest(i.indclass::oid[]) opclass_oid JOIN pg_opclass opc ON opc.oid=opclass_oid WHERE NOT opc.opcdefault), NOT EXISTS (SELECT 1 FROM unnest(i.indcollation::oid[]) WITH ORDINALITY co(collation_oid, ordinality) JOIN unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum, ordinality) USING (ordinality) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE co.collation_oid <> a.attcollation), NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid=i.indexrelid AND con.contype IN ('p','u','x')) FROM pg_class ci JOIN pg_namespace ni ON ni.oid=ci.relnamespace JOIN pg_index i ON i.indexrelid=ci.oid JOIN pg_class tc ON tc.oid=i.indrelid JOIN pg_namespace tn ON tn.oid=tc.relnamespace JOIN pg_am am ON am.oid=ci.relam WHERE ni.nspname=$1 AND ci.relname=$2",
            &[&expected.table.namespace.as_str(), &expected.name.as_str()],
        )
        .map_err(database_error)?;
    if rows.is_empty() {
        return Ok(PostgresIndexState::Different);
    }
    if rows.len() != 1 {
        return Ok(PostgresIndexState::Different);
    }
    let row = &rows[0];
    let columns: Vec<String> = row.get(20);
    let exact = row.get::<_, String>(0) == expected.table.namespace.as_str()
        && row.get::<_, String>(1) == expected.table.name.as_str()
        && !row.get::<_, bool>(2)
        && !row.get::<_, bool>(3)
        && row.get::<_, bool>(4)
        && row.get::<_, bool>(5)
        && row.get::<_, bool>(6)
        && row.get::<_, bool>(7)
        && !row.get::<_, bool>(8)
        && !row.get::<_, bool>(9)
        && !row.get::<_, bool>(10)
        && !row.get::<_, bool>(11)
        && row.get::<_, String>(12) == "btree"
        && row.get::<_, String>(13) == "p"
        && (14..=17).all(|column| row.get::<_, bool>(column))
        && row.get::<_, i16>(18) as usize == expected.columns.len()
        && row.get::<_, i16>(19) as usize == expected.columns.len()
        && columns
            .iter()
            .map(String::as_str)
            .eq(expected.columns.iter().map(Identifier::as_str))
        && (21..=25).all(|column| row.get::<_, bool>(column));
    Ok(if exact {
        PostgresIndexState::Exact
    } else {
        PostgresIndexState::Different
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresForeignKeyMatch {
    Simple,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresForeignKey {
    pub catalog_object_id: String,
    pub name: Identifier,
    pub table: QualifiedTable,
    pub columns: Vec<Identifier>,
    pub referenced_table: QualifiedTable,
    pub referenced_columns: Vec<Identifier>,
    pub match_type: PostgresForeignKeyMatch,
    pub update_action: PostgresForeignKeyAction,
    pub delete_action: PostgresForeignKeyAction,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresForeignKeyCheck {
    pub has_violation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresForeignKeyState {
    Missing,
    ExactNotValidated,
    ExactValidated,
    Different,
}

/// Parse every PostgreSQL foreign key from typed catalog attributes.
pub fn postgres_foreign_keys(catalog: &VendorCatalog) -> ConnectionResult<Vec<PostgresForeignKey>> {
    if catalog.dialect != "postgresql" {
        return Err(ConnectionError::InvalidRequest(
            "foreign-key metadata requires a PostgreSQL catalog".into(),
        ));
    }
    let tables = catalog
        .namespaces
        .iter()
        .flat_map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| object.kind == CatalogObjectKind::Table)
                .map(|object| {
                    (
                        object.id.as_str(),
                        QualifiedTable {
                            namespace: namespace.name.clone(),
                            name: object.name.clone(),
                        },
                    )
                })
        })
        .collect::<BTreeMap<_, _>>();
    let mut foreign_keys = Vec::new();
    for namespace in &catalog.namespaces {
        for object in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::ForeignKey)
        {
            let table_oid = required_attribute_text(object, "table_oid")?;
            let referenced_oid = required_attribute_text(object, "referenced_table_oid")?;
            let table = tables.get(table_oid).cloned().ok_or_else(|| {
                ConnectionError::InvalidRequest(format!(
                    "foreign key {} references an absent child table",
                    object.name
                ))
            })?;
            if table.namespace != namespace.name {
                return Err(ConnectionError::InvalidRequest(format!(
                    "foreign key {} has an inconsistent namespace",
                    object.name
                )));
            }
            let referenced_table = tables.get(referenced_oid).cloned().ok_or_else(|| {
                ConnectionError::InvalidRequest(format!(
                    "foreign key {} references an absent parent table",
                    object.name
                ))
            })?;
            let columns = identifier_array(object, "columns")?;
            let referenced_columns = identifier_array(object, "referenced_columns")?;
            if columns.is_empty() || columns.len() != referenced_columns.len() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "foreign key {} has invalid column metadata",
                    object.name
                )));
            }
            let match_type = match required_attribute_text(object, "match_type")? {
                "s" => PostgresForeignKeyMatch::Simple,
                "f" => PostgresForeignKeyMatch::Full,
                value => {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "foreign key {} has unsupported match type {value}",
                        object.name
                    )));
                }
            };
            let update_action = parse_foreign_key_action(object, "update_action")?;
            let delete_action = parse_foreign_key_action(object, "delete_action")?;
            if object
                .attributes
                .get("delete_set_columns")
                .is_some_and(|value| !value.is_null())
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "foreign key {} uses a targeted ON DELETE SET column list that is not modeled",
                    object.name
                )));
            }
            let deferrable = required_attribute_bool(object, "deferrable")?;
            let initially_deferred = required_attribute_bool(object, "deferred")?;
            if initially_deferred && !deferrable {
                return Err(ConnectionError::InvalidRequest(format!(
                    "foreign key {} is initially deferred but not deferrable",
                    object.name
                )));
            }
            if !required_attribute_bool(object, "validated")? {
                return Err(ConnectionError::InvalidRequest(format!(
                    "source foreign key {} is not validated",
                    object.name
                )));
            }
            foreign_keys.push(PostgresForeignKey {
                catalog_object_id: object.id.clone(),
                name: object.name.clone(),
                table,
                columns,
                referenced_table,
                referenced_columns,
                match_type,
                update_action,
                delete_action,
                deferrable,
                initially_deferred,
            });
        }
    }
    foreign_keys.sort_by(|left, right| {
        left.table
            .cmp(&right.table)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(foreign_keys)
}

fn required_attribute_text<'a>(object: &'a CatalogObject, name: &str) -> ConnectionResult<&'a str> {
    object
        .attributes
        .get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            ConnectionError::InvalidRequest(format!(
                "foreign key {} has no valid {name} attribute",
                object.name
            ))
        })
}

fn required_attribute_bool(object: &CatalogObject, name: &str) -> ConnectionResult<bool> {
    object
        .attributes
        .get(name)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            ConnectionError::InvalidRequest(format!(
                "foreign key {} has no valid {name} attribute",
                object.name
            ))
        })
}

fn identifier_array(object: &CatalogObject, name: &str) -> ConnectionResult<Vec<Identifier>> {
    object
        .attributes
        .get(name)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ConnectionError::InvalidRequest(format!(
                "foreign key {} has no valid {name} array",
                object.name
            ))
        })?
        .iter()
        .map(|value| {
            let value = value.as_str().ok_or_else(|| {
                ConnectionError::InvalidRequest(format!(
                    "foreign key {} has a non-text {name} entry",
                    object.name
                ))
            })?;
            Identifier::new(value).map_err(|error| {
                ConnectionError::InvalidRequest(format!(
                    "foreign key {} has an invalid {name} entry: {error}",
                    object.name
                ))
            })
        })
        .collect()
}

fn parse_foreign_key_action(
    object: &CatalogObject,
    name: &str,
) -> ConnectionResult<PostgresForeignKeyAction> {
    match required_attribute_text(object, name)? {
        "a" => Ok(PostgresForeignKeyAction::NoAction),
        "r" => Ok(PostgresForeignKeyAction::Restrict),
        "c" => Ok(PostgresForeignKeyAction::Cascade),
        "n" => Ok(PostgresForeignKeyAction::SetNull),
        "d" => Ok(PostgresForeignKeyAction::SetDefault),
        value => Err(ConnectionError::InvalidRequest(format!(
            "foreign key {} has unsupported {name} value {value}",
            object.name
        ))),
    }
}

fn check_foreign_key(
    client: &mut impl postgres::GenericClient,
    foreign_key: &PostgresForeignKey,
) -> ConnectionResult<PostgresForeignKeyCheck> {
    let sql = foreign_key_violation_query(foreign_key)?;
    let has_violation = client.query_one(&sql, &[]).map_err(database_error)?.get(0);
    Ok(PostgresForeignKeyCheck { has_violation })
}

fn foreign_key_violation_query(foreign_key: &PostgresForeignKey) -> ConnectionResult<String> {
    validate_foreign_key_shape(foreign_key)?;
    let child_columns = foreign_key
        .columns
        .iter()
        .map(|column| format!("child.{}", quote_identifier(column)))
        .collect::<Vec<_>>();
    let equality = foreign_key
        .columns
        .iter()
        .zip(&foreign_key.referenced_columns)
        .map(|(child, parent)| {
            format!(
                "parent.{} = child.{}",
                quote_identifier(parent),
                quote_identifier(child)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let all_non_null = child_columns
        .iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let any_null = child_columns
        .iter()
        .map(|column| format!("{column} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let any_non_null = child_columns
        .iter()
        .map(|column| format!("{column} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let missing_parent = format!(
        "NOT EXISTS (SELECT 1 FROM {}.{} AS parent WHERE {equality})",
        quote_identifier(&foreign_key.referenced_table.namespace),
        quote_identifier(&foreign_key.referenced_table.name),
    );
    let violation = match foreign_key.match_type {
        PostgresForeignKeyMatch::Simple => format!("({all_non_null}) AND {missing_parent}"),
        PostgresForeignKeyMatch::Full => format!(
            "(({any_null}) AND ({any_non_null})) OR (({all_non_null}) AND {missing_parent})"
        ),
    };
    Ok(format!(
        "SELECT EXISTS (SELECT 1 FROM {}.{} AS child WHERE {violation})",
        quote_identifier(&foreign_key.table.namespace),
        quote_identifier(&foreign_key.table.name),
    ))
}

fn validate_foreign_key_shape(foreign_key: &PostgresForeignKey) -> ConnectionResult<()> {
    if foreign_key.columns.is_empty()
        || foreign_key.columns.len() != foreign_key.referenced_columns.len()
    {
        return Err(ConnectionError::InvalidRequest(format!(
            "foreign key {} has invalid column cardinality",
            foreign_key.name
        )));
    }
    if foreign_key.initially_deferred && !foreign_key.deferrable {
        return Err(ConnectionError::InvalidRequest(format!(
            "foreign key {} is initially deferred but not deferrable",
            foreign_key.name
        )));
    }
    Ok(())
}

fn foreign_key_add_statement(foreign_key: &PostgresForeignKey) -> String {
    let columns = foreign_key
        .columns
        .iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(", ");
    let referenced_columns = foreign_key
        .referenced_columns
        .iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({columns}) REFERENCES {}.{} ({referenced_columns}) MATCH {} ON UPDATE {} ON DELETE {} {} {} NOT VALID",
        quote_identifier(&foreign_key.table.namespace),
        quote_identifier(&foreign_key.table.name),
        quote_identifier(&foreign_key.name),
        quote_identifier(&foreign_key.referenced_table.namespace),
        quote_identifier(&foreign_key.referenced_table.name),
        match foreign_key.match_type {
            PostgresForeignKeyMatch::Simple => "SIMPLE",
            PostgresForeignKeyMatch::Full => "FULL",
        },
        foreign_key_action_sql(foreign_key.update_action),
        foreign_key_action_sql(foreign_key.delete_action),
        if foreign_key.deferrable { "DEFERRABLE" } else { "NOT DEFERRABLE" },
        if foreign_key.initially_deferred { "INITIALLY DEFERRED" } else { "INITIALLY IMMEDIATE" },
    )
}

fn foreign_key_action_sql(action: PostgresForeignKeyAction) -> &'static str {
    match action {
        PostgresForeignKeyAction::NoAction => "NO ACTION",
        PostgresForeignKeyAction::Restrict => "RESTRICT",
        PostgresForeignKeyAction::Cascade => "CASCADE",
        PostgresForeignKeyAction::SetNull => "SET NULL",
        PostgresForeignKeyAction::SetDefault => "SET DEFAULT",
    }
}

fn inspect_foreign_key(
    client: &mut impl postgres::GenericClient,
    expected: &PostgresForeignKey,
) -> ConnectionResult<PostgresForeignKeyState> {
    validate_foreign_key_shape(expected)?;
    let rows = client
        .query(
            "SELECT pn.nspname, pc.relname, array_agg(ca.attname ORDER BY ck.ordinality)::text[], rn.nspname, rc.relname, array_agg(ra.attname ORDER BY rk.ordinality)::text[], con.confmatchtype::text, con.confupdtype::text, con.confdeltype::text, con.condeferrable, con.condeferred, con.convalidated, (SELECT count(*) FROM pg_trigger t WHERE t.tgconstraint=con.oid), (SELECT COALESCE(bool_and(t.tgisinternal AND t.tgenabled='O'), false) FROM pg_trigger t WHERE t.tgconstraint=con.oid), (to_jsonb(con)->'confdelsetcols')::text FROM pg_constraint con JOIN pg_class pc ON pc.oid=con.conrelid JOIN pg_namespace pn ON pn.oid=pc.relnamespace JOIN pg_class rc ON rc.oid=con.confrelid JOIN pg_namespace rn ON rn.oid=rc.relnamespace JOIN unnest(con.conkey) WITH ORDINALITY ck(attnum, ordinality) ON true JOIN pg_attribute ca ON ca.attrelid=con.conrelid AND ca.attnum=ck.attnum JOIN unnest(con.confkey) WITH ORDINALITY rk(attnum, ordinality) ON rk.ordinality=ck.ordinality JOIN pg_attribute ra ON ra.attrelid=con.confrelid AND ra.attnum=rk.attnum WHERE con.contype='f' AND pn.nspname=$1 AND pc.relname=$2 AND con.conname=$3 GROUP BY pn.nspname, pc.relname, rn.nspname, rc.relname, con.confmatchtype, con.confupdtype, con.confdeltype, con.condeferrable, con.condeferred, con.convalidated, con.oid",
            &[&expected.table.namespace.as_str(), &expected.table.name.as_str(), &expected.name.as_str()],
        )
        .map_err(database_error)?;
    if rows.is_empty() {
        return Ok(PostgresForeignKeyState::Missing);
    }
    if rows.len() != 1 {
        return Ok(PostgresForeignKeyState::Different);
    }
    let row = &rows[0];
    let columns: Vec<String> = row.get(2);
    let referenced_columns: Vec<String> = row.get(5);
    let exact = row.get::<_, String>(0) == expected.table.namespace.as_str()
        && row.get::<_, String>(1) == expected.table.name.as_str()
        && columns
            .iter()
            .map(String::as_str)
            .eq(expected.columns.iter().map(Identifier::as_str))
        && row.get::<_, String>(3) == expected.referenced_table.namespace.as_str()
        && row.get::<_, String>(4) == expected.referenced_table.name.as_str()
        && referenced_columns
            .iter()
            .map(String::as_str)
            .eq(expected.referenced_columns.iter().map(Identifier::as_str))
        && row.get::<_, String>(6) == foreign_key_match_code(expected.match_type)
        && row.get::<_, String>(7) == foreign_key_action_code(expected.update_action)
        && row.get::<_, String>(8) == foreign_key_action_code(expected.delete_action)
        && row.get::<_, bool>(9) == expected.deferrable
        && row.get::<_, bool>(10) == expected.initially_deferred
        && row.get::<_, i64>(12) == 4
        && row.get::<_, bool>(13)
        && row
            .get::<_, Option<String>>(14)
            .is_none_or(|value| value == "null");
    if !exact {
        return Ok(PostgresForeignKeyState::Different);
    }
    Ok(if row.get(11) {
        PostgresForeignKeyState::ExactValidated
    } else {
        PostgresForeignKeyState::ExactNotValidated
    })
}

fn foreign_key_match_code(value: PostgresForeignKeyMatch) -> &'static str {
    match value {
        PostgresForeignKeyMatch::Simple => "s",
        PostgresForeignKeyMatch::Full => "f",
    }
}

fn foreign_key_action_code(value: PostgresForeignKeyAction) -> &'static str {
    match value {
        PostgresForeignKeyAction::NoAction => "a",
        PostgresForeignKeyAction::Restrict => "r",
        PostgresForeignKeyAction::Cascade => "c",
        PostgresForeignKeyAction::SetNull => "n",
        PostgresForeignKeyAction::SetDefault => "d",
    }
}

fn assert_target_empty_and_owned(
    client: &mut impl postgres::GenericClient,
) -> ConnectionResult<()> {
    let row = client
        .query_one(
            "SELECT COALESCE(pg_has_role(current_user, d.datdba, 'MEMBER') AND has_database_privilege(current_user, current_database(), 'CREATE') AND has_schema_privilege(current_user, 'public', 'CREATE'), false), EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname <> 'public' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'), EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p','S','v','m','f')), EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'), EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND t.typtype IN ('c','d','e','r')), EXISTS (SELECT 1 FROM pg_extension WHERE extname <> 'plpgsql'), EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtenabled <> 'D') FROM pg_database d WHERE d.datname = current_database()",
            &[],
        )
        .map_err(database_error)?;
    let owned: bool = row.get(0);
    let has_extra_namespace: bool = row.get(1);
    let has_relation: bool = row.get(2);
    let has_routine: bool = row.get(3);
    let has_type: bool = row.get(4);
    let has_extension: bool = row.get(5);
    let has_event_trigger: bool = row.get(6);
    if !owned {
        return Err(ConnectionError::InvalidRequest(
            "target role does not own the database or lacks create privileges".into(),
        ));
    }
    if has_extra_namespace
        || has_relation
        || has_routine
        || has_type
        || has_extension
        || has_event_trigger
    {
        return Err(ConnectionError::InvalidRequest(
            "target database is not empty".into(),
        ));
    }
    Ok(())
}

fn pre_data_statements(catalog: &VendorCatalog) -> ConnectionResult<Vec<String>> {
    let mut statements = Vec::new();
    let sequences = postgres_sequences(catalog)
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let generated_columns = postgres_generated_columns(catalog)
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let partition_topologies = postgres_partition_topologies(catalog)
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    for namespace in &catalog.namespaces {
        if namespace.name.as_str() != "public" {
            statements.push(format!(
                "CREATE SCHEMA {}",
                quote_identifier(&namespace.name)
            ));
        }
    }
    for sequence in &sequences {
        if sequence
            .ownership
            .as_ref()
            .is_none_or(|ownership| ownership.kind == PostgresSequenceOwnershipKind::Serial)
        {
            statements.push(standalone_sequence_create_statement(sequence));
        }
    }
    for namespace in &catalog.namespaces {
        let mut tables = namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Table)
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        for table in tables {
            let relkind = table
                .attributes
                .get("relkind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if !matches!(relkind, "r" | "p") {
                return Err(ConnectionError::InvalidRequest(format!(
                    "table {}.{} is not an ordinary or supported partitioned PostgreSQL table",
                    namespace.name, table.name
                )));
            }
            let persistence = table
                .attributes
                .get("persistence")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("p");
            let create = match persistence {
                "p" => "CREATE TABLE",
                "u" => "CREATE UNLOGGED TABLE",
                _ => {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "unsupported table persistence {persistence} for {}.{}",
                        namespace.name, table.name
                    )));
                }
            };
            let mut definitions =
                table_column_definitions(namespace, table, &sequences, &generated_columns)?;
            definitions.extend(table_constraint_definitions(namespace, table)?);
            if definitions.is_empty() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "table {}.{} has no catalog columns",
                    namespace.name, table.name
                )));
            }
            let partition_clause = if relkind == "p" {
                let topology = partition_topologies
                    .iter()
                    .find(|topology| {
                        topology.root.namespace == namespace.name
                            && topology.root.name == table.name
                    })
                    .ok_or_else(|| {
                        ConnectionError::InvalidRequest(
                            "partition root has no validated topology".into(),
                        )
                    })?;
                format!(
                    " PARTITION BY {} ({})",
                    partition_strategy_sql(topology.strategy),
                    quote_identifier(&topology.key_column)
                )
            } else {
                String::new()
            };
            statements.push(format!(
                "{create} {}.{} ({}){partition_clause}",
                quote_identifier(&namespace.name),
                quote_identifier(&table.name),
                definitions.join(", ")
            ));
        }
    }
    for topology in &partition_topologies {
        for leaf in &topology.leaves {
            statements.push(partition_leaf_create_statement(topology, leaf));
        }
    }
    for sequence in &sequences {
        statements.extend(serial_sequence_association_statements(sequence));
    }
    for namespace in &catalog.namespaces {
        let mut indexes = namespace
            .objects
            .iter()
            .filter(|object| {
                object.kind == CatalogObjectKind::Index
                    && object
                        .attributes
                        .get("constraint_oid")
                        .is_none_or(serde_json::Value::is_null)
            })
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        for index in indexes {
            if ordinary_index_columns(index, Some(namespace)).is_ok() {
                continue;
            }
            let columns = standalone_unique_index_columns(index, Some(namespace))
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let table_oid = required_catalog_string(index, "table_oid")
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let table = namespace
                .objects
                .iter()
                .find(|object| object.kind == CatalogObjectKind::Table && object.id == table_oid)
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(
                        "standalone index refers to an unknown table".into(),
                    )
                })?;
            statements.push(format!(
                "CREATE UNIQUE INDEX {} ON {}.{} USING btree ({})",
                quote_identifier(&index.name),
                quote_identifier(&namespace.name),
                quote_identifier(&table.name),
                columns
                    .iter()
                    .map(quote_identifier)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(statements)
}

fn partition_strategy_sql(strategy: PostgresPartitionStrategy) -> &'static str {
    match strategy {
        PostgresPartitionStrategy::Range => "RANGE",
        PostgresPartitionStrategy::List => "LIST",
        PostgresPartitionStrategy::Hash => "HASH",
    }
}

fn partition_leaf_create_statement(
    topology: &PostgresPartitionTopology,
    leaf: &PostgresPartitionLeaf,
) -> String {
    format!(
        "CREATE TABLE {}.{} PARTITION OF {}.{} {}",
        quote_identifier(&leaf.table.namespace),
        quote_identifier(&leaf.table.name),
        quote_identifier(&topology.root.namespace),
        quote_identifier(&topology.root.name),
        render_partition_bound(&leaf.bound)
    )
}

fn render_partition_bound(bound: &PostgresPartitionBound) -> String {
    match bound {
        PostgresPartitionBound::Range { lower, upper } => format!(
            "FOR VALUES FROM ({}) TO ({})",
            render_partition_bound_value(lower),
            render_partition_bound_value(upper)
        ),
        PostgresPartitionBound::List { values } => format!(
            "FOR VALUES IN ({})",
            values
                .iter()
                .map(|value| value.map_or_else(|| "NULL".into(), |value| value.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        PostgresPartitionBound::Hash { modulus, remainder } => {
            format!("FOR VALUES WITH (modulus {modulus}, remainder {remainder})")
        }
        PostgresPartitionBound::Default => "DEFAULT".into(),
    }
}

fn render_partition_bound_value(value: &PostgresPartitionBoundValue) -> String {
    match value {
        PostgresPartitionBoundValue::MinValue => "MINVALUE".into(),
        PostgresPartitionBoundValue::Value(value) => value.to_string(),
        PostgresPartitionBoundValue::MaxValue => "MAXVALUE".into(),
    }
}

fn table_column_definitions(
    namespace: &CatalogNamespace,
    table: &CatalogObject,
    sequences: &[PostgresSequence],
    generated_columns: &[PostgresGeneratedColumn],
) -> ConnectionResult<Vec<String>> {
    let mut columns = namespace
        .objects
        .iter()
        .filter(|object| {
            object.kind == CatalogObjectKind::Column
                && object
                    .attributes
                    .get("table_oid")
                    .and_then(serde_json::Value::as_str)
                    == Some(table.id.as_str())
        })
        .collect::<Vec<_>>();
    columns.sort_by_key(|column| {
        column
            .attributes
            .get("ordinal")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(i64::MAX)
    });
    columns
        .into_iter()
        .map(|column| {
            let type_declaration = std::str::from_utf8(&column.definition).map_err(|_| {
                ConnectionError::InvalidRequest(format!(
                    "column {} has a non-UTF-8 type declaration",
                    column.name
                ))
            })?;
            if column
                .attributes
                .get("type_schema")
                .and_then(serde_json::Value::as_str)
                != Some("pg_catalog")
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "column {} uses an unsupported user-defined type",
                    column.name
                )));
            }
            let mut definition = format!("{} {type_declaration}", quote_identifier(&column.name));
            let generated_mode = column
                .attributes
                .get("generated")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if let Some(collation) = column
                .attributes
                .get("collation")
                .and_then(serde_json::Value::as_str)
            {
                definition.push_str(" COLLATE ");
                if let Some(schema) = column
                    .attributes
                    .get("collation_schema")
                    .and_then(serde_json::Value::as_str)
                {
                    definition
                        .push_str(&quote_identifier(&Identifier::new(schema).map_err(
                            |error| ConnectionError::InvalidRequest(error.to_string()),
                        )?));
                    definition.push('.');
                }
                definition
                    .push_str(&quote_identifier(&Identifier::new(collation).map_err(
                        |error| ConnectionError::InvalidRequest(error.to_string()),
                    )?));
            }
            if generated_mode == "s" {
                let generated = generated_columns
                    .iter()
                    .find(|generated| {
                        generated.table.namespace == namespace.name
                            && generated.table.name == table.name
                            && generated.column == column.name
                    })
                    .ok_or_else(|| {
                        ConnectionError::InvalidRequest(format!(
                            "generated column {} has no validated expression contract",
                            column.name
                        ))
                    })?;
                definition.push_str(" GENERATED ALWAYS AS (");
                definition.push_str(&generated.expression);
                definition.push_str(") STORED");
            } else if !generated_mode.is_empty() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "column {} has unsupported generated mode {generated_mode}",
                    column.name
                )));
            }
            let identity = column
                .attributes
                .get("identity")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match (generated_mode, identity) {
                ("s", "") => {}
                ("s", _) => {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "generated column {} also has identity semantics",
                        column.name
                    )));
                }
                ("", "a" | "d") => {
                    let ownership_kind = if identity == "a" {
                        PostgresSequenceOwnershipKind::IdentityAlways
                    } else {
                        PostgresSequenceOwnershipKind::IdentityByDefault
                    };
                    let sequence = sequences
                        .iter()
                        .find(|sequence| {
                            sequence.ownership.as_ref().is_some_and(|ownership| {
                                ownership.table.namespace == namespace.name
                                    && ownership.table.name == table.name
                                    && ownership.column == column.name
                                    && ownership.kind == ownership_kind
                            })
                        })
                        .ok_or_else(|| {
                            ConnectionError::InvalidRequest(format!(
                                "identity column {} has no exact sequence",
                                column.name
                            ))
                        })?;
                    definition.push_str(if identity == "a" {
                        " GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME "
                    } else {
                        " GENERATED BY DEFAULT AS IDENTITY (SEQUENCE NAME "
                    });
                    definition.push_str(&format!(
                        "{}.{} {})",
                        quote_identifier(&sequence.namespace),
                        quote_identifier(&sequence.name),
                        sequence_options(sequence)
                    ));
                }
                ("", "") => {
                    let has_typed_sequence_default = column
                        .attributes
                        .get("sequence_default_oid")
                        .and_then(serde_json::Value::as_str)
                        .is_some();
                    if let Some(default) = column
                        .attributes
                        .get("default")
                        .and_then(serde_json::Value::as_str)
                    {
                        if has_typed_sequence_default {
                            // The typed sequence contract creates the exact default and ownership.
                        } else {
                            definition.push_str(" DEFAULT ");
                            definition.push_str(default);
                        }
                    }
                }
                (_, value) => {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "column {} has unknown identity mode {value}",
                        column.name
                    )));
                }
            }
            if !column
                .attributes
                .get("nullable")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true)
            {
                definition.push_str(" NOT NULL");
            }
            Ok(definition)
        })
        .collect()
}

fn table_constraint_definitions(
    namespace: &CatalogNamespace,
    table: &CatalogObject,
) -> ConnectionResult<Vec<String>> {
    let mut constraints = namespace
        .objects
        .iter()
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::PrimaryKey
                    | CatalogObjectKind::UniqueConstraint
                    | CatalogObjectKind::CheckConstraint
            ) && object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                == Some(table.id.as_str())
        })
        .collect::<Vec<_>>();
    constraints.sort_by(|left, right| left.name.cmp(&right.name));
    constraints
        .into_iter()
        .map(|constraint| {
            let definition = std::str::from_utf8(&constraint.definition).map_err(|_| {
                ConnectionError::InvalidRequest(format!(
                    "constraint {} has a non-UTF-8 definition",
                    constraint.name
                ))
            })?;
            Ok(format!(
                "CONSTRAINT {} {definition}",
                quote_identifier(&constraint.name)
            ))
        })
        .collect()
}

impl TargetConnectionFactory for PostgresTargetFactory {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::from_entries([
            ("transactions", Capability::Supported),
            ("cancellation", Capability::Supported),
            ("typed_identifiers", Capability::Supported),
            ("bound_parameters", Capability::Supported),
            ("plain_insert", Capability::Supported),
            ("bulk_write", Capability::Supported),
        ])
    }

    fn open_writer(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn WriteSession>> {
        let (client, cancel_registration) = self.controlled_connect()?;
        Ok(Box::new(PostgresWriter {
            client,
            _cancel_registration: cancel_registration,
            cancellation,
            transaction_open: false,
        }))
    }

    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>> {
        let (mut client, cancel_registration) = self.controlled_connect()?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(database_error)?;
        let row = client
            .query_one(
                "SELECT current_database(), current_setting('server_version'), pg_current_snapshot()::text",
                &[],
            )
            .map_err(database_error)?;
        let token = SnapshotToken {
            endpoint_identity: "target-verification-session".into(),
            database_identity: row.get(0),
            server_version: row.get(1),
            snapshot_id: row.get(2),
            consistency_mode: "postgres_repeatable_read_read_only_target_verification".into(),
            lifecycle_id: format!(
                "pg-target-verifier-{}",
                SNAPSHOT_LIFECYCLE_COUNTER.fetch_add(1, Ordering::Relaxed)
            ),
        };
        Ok(Box::new(PostgresTargetVerifier {
            reader: PostgresSnapshotReader {
                client,
                _cancel_registration: cancel_registration,
                exporter_lifetime: None,
                token,
                evidence: ReadOnlyEvidence {
                    server_enforced: true,
                    description: "target verification transaction is read-only".into(),
                },
                cancellation,
                max_batch_rows: self.config.max_batch_rows,
                max_batch_bytes: self.config.max_batch_bytes,
                metadata_cache: HashMap::new(),
                validated_keys: BTreeSet::new(),
            },
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        let tokens = self.cancel_registry.sessions()?;
        if tokens.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "open a PostgreSQL target session before its control session".into(),
            ));
        }
        Ok(Box::new(PostgresControlSession {
            registry: Arc::clone(&self.cancel_registry),
            config: self.config.clone(),
        }))
    }
}

struct PostgresTargetVerifier {
    reader: PostgresSnapshotReader,
}

impl VerificationSession for PostgresTargetVerifier {
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.reader.select_page(request)
    }

    fn select_page_only(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.reader.select_page_only(request)
    }
}

struct PostgresWriter {
    _cancel_registration: SessionRegistration<CancelToken>,
    client: Client,
    cancellation: CancellationToken,
    transaction_open: bool,
}

impl WriteSession for PostgresWriter {
    fn begin(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if self.transaction_open {
            return Err(ConnectionError::TransactionAlreadyOpen);
        }
        self.client.batch_execute("BEGIN").map_err(database_error)?;
        self.transaction_open = true;
        Ok(())
    }

    fn insert(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        if batch.columns().is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "insert batch must contain columns".into(),
            ));
        }
        let columns = batch
            .columns()
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (1..=batch.columns().len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {}.{} ({columns}) OVERRIDING SYSTEM VALUE VALUES ({placeholders})",
            quote_identifier(&table.namespace),
            quote_identifier(&table.name)
        );
        let statement = self.client.prepare(&sql).map_err(database_error)?;
        for row in batch.rows() {
            self.cancellation.check()?;
            if row.len() != statement.params().len() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "row has {} values but INSERT expects {}",
                    row.len(),
                    statement.params().len()
                )));
            }
            let parameters = row
                .iter()
                .zip(statement.params())
                .map(|(value, ty)| write_parameter(value, ty))
                .collect::<ConnectionResult<Vec<_>>>()?;
            let parameter_refs = parameters
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();
            let affected = self
                .client
                .execute(&statement, &parameter_refs)
                .map_err(database_error)?;
            if affected != 1 {
                return Err(ConnectionError::Database(format!(
                    "plain INSERT affected {affected} rows instead of one"
                )));
            }
        }
        Ok(())
    }

    fn bulk_write(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        validate_bulk_batch(batch)?;
        let probe = self
            .client
            .prepare(&bulk_type_probe_sql(table, batch))
            .map_err(database_error)?;
        let types = probe
            .columns()
            .iter()
            .map(|column| column.type_().clone())
            .collect::<Vec<_>>();
        if types.len() != batch.columns().len() {
            return Err(ConnectionError::Database(format!(
                "PostgreSQL type probe returned {} columns for a {}-column batch",
                types.len(),
                batch.columns().len()
            )));
        }
        let copy = self
            .client
            .copy_in(&bulk_copy_sql(table, batch))
            .map_err(database_error)?;
        let mut writer = BinaryCopyInWriter::new(copy, &types);
        for row in batch.rows() {
            self.cancellation.check()?;
            let parameters = row
                .iter()
                .zip(&types)
                .map(|(value, ty)| write_parameter(value, ty))
                .collect::<ConnectionResult<Vec<_>>>()?;
            let parameter_refs = parameters
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();
            writer.write(&parameter_refs).map_err(database_error)?;
        }
        self.cancellation.check()?;
        let affected = writer.finish().map_err(database_error)?;
        self.cancellation.check()?;
        let expected = u64::try_from(batch.len()).map_err(|_| {
            ConnectionError::InvalidRequest("bulk-write row count exceeds u64".into())
        })?;
        if affected != expected {
            return Err(ConnectionError::Database(format!(
                "binary COPY affected {affected} rows instead of {expected}"
            )));
        }
        Ok(())
    }

    fn commit(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        self.transaction_open = false;
        self.client
            .batch_execute("COMMIT")
            .map_err(|error| ConnectionError::CommitOutcomeUnknown(error.to_string()))
    }

    fn rollback(&mut self) -> ConnectionResult<()> {
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        self.transaction_open = false;
        self.client
            .batch_execute("ROLLBACK")
            .map_err(database_error)
    }
}

fn validate_bulk_batch(batch: &RowBatch) -> ConnectionResult<()> {
    if batch.columns().is_empty() {
        return Err(ConnectionError::InvalidRequest(
            "bulk-write batch must contain columns".into(),
        ));
    }
    for row in batch.rows() {
        if row.len() != batch.columns().len() {
            return Err(ConnectionError::InvalidRequest(format!(
                "row has {} values but binary COPY expects {}",
                row.len(),
                batch.columns().len()
            )));
        }
    }
    Ok(())
}

fn bulk_copy_sql(table: &QualifiedTable, batch: &RowBatch) -> String {
    let columns = batch
        .columns()
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY {}.{} ({columns}) FROM STDIN (FORMAT binary)",
        quote_identifier(&table.namespace),
        quote_identifier(&table.name)
    )
}

fn bulk_type_probe_sql(table: &QualifiedTable, batch: &RowBatch) -> String {
    let columns = batch
        .columns()
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {columns} FROM {}.{} WHERE FALSE",
        quote_identifier(&table.namespace),
        quote_identifier(&table.name)
    )
}

#[derive(Debug)]
struct NullParameter;

impl ToSql for NullParameter {
    fn to_sql(
        &self,
        _: &Type,
        _: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    postgres::types::to_sql_checked!();
}

#[derive(Debug)]
struct RawParameter {
    oid: u32,
    bytes: Vec<u8>,
}

impl ToSql for RawParameter {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if ty.oid() != self.oid {
            return Err(format!(
                "source PostgreSQL type OID {} differs from target type OID {}",
                self.oid,
                ty.oid()
            )
            .into());
        }
        out.extend_from_slice(&self.bytes);
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    postgres::types::to_sql_checked!();
}

fn write_parameter(value: &DbValue, ty: &Type) -> ConnectionResult<Box<dyn ToSql + Sync>> {
    match value {
        DbValue::Null => Ok(Box::new(NullParameter)),
        DbValue::Bool(value) if *ty == Type::BOOL => Ok(Box::new(*value)),
        DbValue::Signed(value) if *ty == Type::INT2 => i16::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::InvalidRequest("int2 value is out of range".into())),
        DbValue::Signed(value) if *ty == Type::INT4 => i32::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::InvalidRequest("int4 value is out of range".into())),
        DbValue::Signed(value) if *ty == Type::INT8 => i64::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::InvalidRequest("int8 value is out of range".into())),
        DbValue::Unsigned(value) if *ty == Type::OID => u32::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::InvalidRequest("oid value is out of range".into())),
        DbValue::Float32(bits) if *ty == Type::FLOAT4 => Ok(Box::new(f32::from_bits(*bits))),
        DbValue::Float64(bits) if *ty == Type::FLOAT8 => Ok(Box::new(f64::from_bits(*bits))),
        DbValue::Text(value)
            if matches!(*ty, Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME) =>
        {
            Ok(Box::new(RawParameter {
                oid: ty.oid(),
                bytes: value.as_bytes().to_vec(),
            }))
        }
        DbValue::Bytes(value) if *ty == Type::BYTEA => Ok(Box::new(RawParameter {
            oid: ty.oid(),
            bytes: value.clone(),
        })),
        DbValue::Json(value) if *ty == Type::JSON => Ok(Box::new(RawParameter {
            oid: ty.oid(),
            bytes: canonicalize_json(value)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
        })),
        DbValue::Json(value) if *ty == Type::JSONB => {
            let value = canonicalize_json(value)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let mut bytes = Vec::with_capacity(value.len() + 1);
            bytes.push(1);
            bytes.extend_from_slice(&value);
            Ok(Box::new(RawParameter {
                oid: ty.oid(),
                bytes,
            }))
        }
        DbValue::Vendor {
            type_id,
            format: ValueFormat::Binary,
            bytes,
        } => {
            let oid = type_id
                .strip_prefix("postgres:")
                .and_then(|value| value.split(':').next())
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(
                        "invalid PostgreSQL vendor type identity".into(),
                    )
                })?;
            Ok(Box::new(RawParameter {
                oid,
                bytes: bytes.clone(),
            }))
        }
        _ => Err(ConnectionError::InvalidRequest(format!(
            "cannot bind canonical value to PostgreSQL type {}",
            ty.name()
        ))),
    }
}

struct PostgresSnapshotReader {
    _cancel_registration: SessionRegistration<CancelToken>,
    client: Client,
    exporter_lifetime: Option<Arc<ExporterLifetime>>,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    cancellation: CancellationToken,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    metadata_cache: HashMap<(QualifiedTable, Vec<Identifier>), Vec<ColumnMeta>>,
    validated_keys: BTreeSet<(QualifiedTable, Vec<Identifier>)>,
}

impl Drop for PostgresSnapshotReader {
    fn drop(&mut self) {
        if let Some(lifetime) = &self.exporter_lifetime {
            if let Ok(mut alive) = lifetime.alive.lock() {
                *alive = false;
            }
        }
    }
}

impl ReadSession for PostgresSnapshotReader {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence {
        &self.evidence
    }

    fn snapshot(&self) -> &SnapshotToken {
        &self.token
    }

    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.select_page_scoped(request, false)
    }

    fn select_page_only(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.select_page_scoped(request, true)
    }
}

impl PostgresSnapshotReader {
    fn select_page_scoped(
        &mut self,
        request: &KeysetPage,
        only: bool,
    ) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        validate_page(request, self.max_batch_rows)?;
        let key_cache_entry = (request.table.clone(), request.key.clone());
        if !self.validated_keys.contains(&key_cache_entry) {
            validate_resumable_key(&mut self.client, &request.table, &request.key)?;
            self.validated_keys.insert(key_cache_entry);
        }
        let metadata_key = (request.table.clone(), request.projection.clone());
        let metadata = if let Some(metadata) = self.metadata_cache.get(&metadata_key) {
            metadata.clone()
        } else {
            let metadata =
                load_projection_metadata(&mut self.client, &request.table, &request.projection)?;
            self.metadata_cache.insert(metadata_key, metadata.clone());
            metadata
        };
        let projection = request
            .projection
            .iter()
            .map(quote_identifier)
            .collect::<Vec<_>>()
            .join(", ");
        let key = request
            .key
            .iter()
            .map(quote_identifier)
            .collect::<Vec<_>>()
            .join(", ");
        let table = format!(
            "{}{}.{}",
            if only { "ONLY " } else { "" },
            quote_identifier(&request.table.namespace),
            quote_identifier(&request.table.name)
        );
        let mut parameters: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let predicate = if let Some(after) = &request.after {
            if after.0.len() != request.key.len() {
                return Err(ConnectionError::InvalidRequest(
                    "key tuple width differs from key".into(),
                ));
            }
            for (key, value) in request.key.iter().zip(&after.0) {
                let column = request
                    .projection
                    .iter()
                    .position(|name| name == key)
                    .and_then(|index| metadata.get(index))
                    .ok_or_else(|| {
                        ConnectionError::InvalidRequest(format!(
                            "key column {key} is not present in projection metadata"
                        ))
                    })?;
                parameters.push(key_parameter(value, &column.vendor_type)?);
            }
            let placeholders = (1..=parameters.len())
                .map(|index| format!("${index}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" WHERE ({key}) > ({placeholders})")
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT {projection} FROM {table}{predicate} ORDER BY {key} ASC LIMIT {}",
            request.limit
        );
        let parameter_refs = parameters
            .iter()
            .map(|value| value.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let statement = self.client.prepare(&sql).map_err(database_error)?;
        let mut rows = self
            .client
            .query_raw(&statement, parameter_refs)
            .map_err(database_error)?;
        let mut batch = RowBatch::new(metadata, request.limit as usize, self.max_batch_bytes);
        while let Some(row) = rows.next().map_err(database_error)? {
            self.cancellation.check()?;
            let mut values = Vec::with_capacity(row.len());
            let mut encoded_bytes = 0usize;
            for (index, column) in row.columns().iter().enumerate() {
                let raw: Option<RawBinary> = row.try_get(index).map_err(database_error)?;
                let value = decode_value(column.type_(), raw)?;
                encoded_bytes = encoded_bytes
                    .checked_add(encoded_size(&value))
                    .ok_or_else(|| ConnectionError::BatchLimit("row byte count overflow".into()))?;
                values.push(value);
            }
            match batch.try_push(values, encoded_bytes) {
                Ok(()) => {}
                Err(RowBatchError::ByteLimit { .. }) if !batch.is_empty() => break,
                Err(error) => return Err(ConnectionError::BatchLimit(error.to_string())),
            }
        }
        Ok(batch)
    }
}

fn validate_page(request: &KeysetPage, max_batch_rows: usize) -> ConnectionResult<()> {
    if request.limit == 0 || request.key.is_empty() || request.projection.is_empty() {
        return Err(ConnectionError::InvalidRequest(
            "projection, key, and limit must be non-empty".into(),
        ));
    }
    if request.limit as usize > max_batch_rows {
        return Err(ConnectionError::InvalidRequest(format!(
            "requested row limit {} exceeds configured maximum {max_batch_rows}",
            request.limit
        )));
    }
    if request
        .key
        .iter()
        .any(|key| !request.projection.contains(key))
    {
        return Err(ConnectionError::InvalidRequest(
            "every key column must be present in the projection".into(),
        ));
    }
    Ok(())
}

fn validate_resumable_key(
    client: &mut Client,
    table: &QualifiedTable,
    key: &[Identifier],
) -> ConnectionResult<()> {
    let key_names = key
        .iter()
        .map(|identifier| identifier.as_str().to_owned())
        .collect::<Vec<_>>();
    let accepted: bool = client
        .query_one(
            "SELECT EXISTS (
               SELECT 1
               FROM pg_constraint con
               JOIN pg_class rel ON rel.oid = con.conrelid
               JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
               WHERE nsp.nspname = $1
                 AND rel.relname = $2
                 AND con.contype IN ('p', 'u')
                 AND con.convalidated
                 AND NOT EXISTS (
                   SELECT 1
                   FROM unnest(con.conkey) AS key_attnum
                   JOIN pg_attribute att
                     ON att.attrelid = con.conrelid AND att.attnum = key_attnum
                   LEFT JOIN pg_collation coll ON coll.oid = att.attcollation
                   WHERE NOT att.attnotnull
                      OR (coll.collversion IS NOT NULL
                          AND coll.collversion IS DISTINCT FROM pg_collation_actual_version(coll.oid))
                 )
                 AND ARRAY(
                   SELECT att.attname::text
                   FROM unnest(con.conkey) WITH ORDINALITY AS key_col(attnum, position)
                   JOIN pg_attribute att
                     ON att.attrelid = con.conrelid AND att.attnum = key_col.attnum
                   ORDER BY key_col.position
                 ) = $3::text[]
             ) OR EXISTS (
               SELECT 1
               FROM pg_index idx
               JOIN pg_class index_rel ON index_rel.oid = idx.indexrelid
               JOIN pg_class table_rel ON table_rel.oid = idx.indrelid
               JOIN pg_namespace nsp ON nsp.oid = table_rel.relnamespace
               JOIN pg_am am ON am.oid = index_rel.relam
               LEFT JOIN pg_constraint con
                 ON con.conindid = idx.indexrelid AND con.contype IN ('p','u','x')
               WHERE nsp.nspname = $1
                 AND table_rel.relname = $2
                 AND idx.indisunique AND NOT idx.indisprimary
                 AND idx.indisvalid AND idx.indisready AND idx.indislive
                 AND idx.indimmediate AND NOT idx.indisexclusion
                 AND NOT idx.indnullsnotdistinct
                 AND NOT idx.indisclustered AND NOT idx.indisreplident
                 AND con.oid IS NULL AND am.amname = 'btree'
                 AND idx.indpred IS NULL AND idx.indexprs IS NULL
                 AND idx.indnkeyatts = idx.indnatts
                 AND idx.indnkeyatts = cardinality($3::text[])
                 AND index_rel.relpersistence = 'p'
                 AND index_rel.reloptions IS NULL AND index_rel.reltablespace = 0
                 AND ARRAY(
                   SELECT att.attname::text
                   FROM unnest(idx.indkey::smallint[]) WITH ORDINALITY AS key_col(attnum, position)
                   JOIN pg_attribute att
                     ON att.attrelid = idx.indrelid AND att.attnum = key_col.attnum
                   ORDER BY key_col.position
                 ) = $3::text[]
                 AND NOT EXISTS (
                   SELECT 1
                   FROM unnest(idx.indkey::smallint[]) WITH ORDINALITY AS key_col(attnum, position)
                   LEFT JOIN pg_attribute att
                     ON att.attrelid = idx.indrelid AND att.attnum = key_col.attnum
                   LEFT JOIN pg_collation coll ON coll.oid = att.attcollation
                   JOIN unnest(idx.indoption::smallint[]) WITH ORDINALITY AS opt(value, position)
                     USING (position)
                   JOIN unnest(idx.indclass::oid[]) WITH ORDINALITY AS cls(oid, position)
                     USING (position)
                   JOIN pg_opclass opc ON opc.oid = cls.oid
                   JOIN unnest(idx.indcollation::oid[]) WITH ORDINALITY AS idx_coll(oid, position)
                     USING (position)
                   WHERE att.attnum IS NULL OR NOT att.attnotnull
                      OR opt.value <> 0 OR NOT opc.opcdefault
                      OR idx_coll.oid <> att.attcollation
                      OR (coll.collversion IS NOT NULL
                          AND coll.collversion IS DISTINCT FROM pg_collation_actual_version(coll.oid))
                 )
             )",
            &[&table.namespace.as_str(), &table.name.as_str(), &key_names],
        )
        .map_err(database_error)?
        .get(0);
    if !accepted {
        return Err(ConnectionError::InvalidRequest(format!(
            "pagination key for {table:?} is not the reviewed exact validated non-null primary key, unique constraint, or standalone unique index"
        )));
    }
    Ok(())
}

fn quote_identifier(identifier: &Identifier) -> String {
    format!("\"{}\"", identifier.as_str().replace('"', "\"\""))
}

fn key_parameter(value: &DbValue, vendor_type: &str) -> ConnectionResult<Box<dyn ToSql + Sync>> {
    match (value, vendor_type) {
        (DbValue::Bool(value), "pg_catalog.bool") => Ok(Box::new(*value)),
        (DbValue::Signed(value), "pg_catalog.int2") => i16::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        (DbValue::Signed(value), "pg_catalog.int4") => i32::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        (DbValue::Signed(value), "pg_catalog.int8") => i64::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        (DbValue::Unsigned(value), "pg_catalog.oid") => u32::try_from(*value)
            .map(|value| Box::new(value) as Box<dyn ToSql + Sync>)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        (DbValue::Float32(bits), "pg_catalog.float4") => Ok(Box::new(f32::from_bits(*bits))),
        (DbValue::Float64(bits), "pg_catalog.float8") => Ok(Box::new(f64::from_bits(*bits))),
        (
            DbValue::Text(value),
            "pg_catalog.text" | "pg_catalog.varchar" | "pg_catalog.bpchar" | "pg_catalog.name",
        ) => Ok(Box::new(value.clone())),
        (DbValue::Bytes(value), "pg_catalog.bytea") => Ok(Box::new(value.clone())),
        (
            DbValue::Vendor {
                type_id,
                format: ValueFormat::Binary,
                bytes,
            },
            _,
        ) => {
            let oid = type_id
                .strip_prefix("postgres:")
                .and_then(|value| value.split(':').next())
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or(ConnectionError::UnsupportedKeyValue)?;
            Ok(Box::new(RawParameter {
                oid,
                bytes: bytes.clone(),
            }))
        }
        _ => Err(ConnectionError::UnsupportedKeyValue),
    }
}

#[derive(Debug)]
struct RawBinary(Vec<u8>);

impl<'a> FromSql<'a> for RawBinary {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw.to_vec()))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

fn decode_value(ty: &Type, raw: Option<RawBinary>) -> ConnectionResult<DbValue> {
    let Some(RawBinary(bytes)) = raw else {
        return Ok(DbValue::Null);
    };
    let invalid = || ConnectionError::Database(format!("invalid binary value for PostgreSQL {ty}"));
    match *ty {
        Type::BOOL if bytes.as_slice() == [0] => Ok(DbValue::Bool(false)),
        Type::BOOL if bytes.as_slice() == [1] => Ok(DbValue::Bool(true)),
        Type::INT2 if bytes.len() == 2 => Ok(DbValue::Signed(i16::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ) as i128)),
        Type::INT4 if bytes.len() == 4 => Ok(DbValue::Signed(i32::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ) as i128)),
        Type::INT8 if bytes.len() == 8 => Ok(DbValue::Signed(i64::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ) as i128)),
        Type::OID if bytes.len() == 4 => Ok(DbValue::Unsigned(u32::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ) as u128)),
        Type::FLOAT4 if bytes.len() == 4 => Ok(DbValue::Float32(u32::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ))),
        Type::FLOAT8 if bytes.len() == 8 => Ok(DbValue::Float64(u64::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        ))),
        Type::BYTEA => Ok(DbValue::Bytes(bytes)),
        Type::JSON => canonicalize_json(&bytes)
            .map(DbValue::Json)
            .map_err(|error| {
                ConnectionError::Database(format!(
                    "invalid canonical JSON for PostgreSQL {ty}: {error}"
                ))
            }),
        Type::JSONB if bytes.first() == Some(&1) => canonicalize_json(&bytes[1..])
            .map(DbValue::Json)
            .map_err(|error| {
                ConnectionError::Database(format!(
                    "invalid canonical JSON for PostgreSQL {ty}: {error}"
                ))
            }),
        Type::JSONB => Err(invalid()),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::XML => {
            String::from_utf8(bytes)
                .map(DbValue::Text)
                .map_err(|_| invalid())
        }
        Type::BOOL
        | Type::INT2
        | Type::INT4
        | Type::INT8
        | Type::OID
        | Type::FLOAT4
        | Type::FLOAT8 => Err(invalid()),
        _ => Ok(DbValue::Vendor {
            type_id: format!("postgres:{}:{}", ty.oid(), ty.name()),
            format: ValueFormat::Binary,
            bytes,
        }),
    }
}

fn load_projection_metadata(
    client: &mut Client,
    table: &QualifiedTable,
    projection: &[Identifier],
) -> ConnectionResult<Vec<ColumnMeta>> {
    let rows = client
        .query(
            "SELECT column_name, ordinal_position::integer, udt_schema || '.' || udt_name, is_nullable = 'YES', collation_name, numeric_precision::integer, numeric_scale::integer, datetime_precision::integer, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            &[&table.namespace.as_str(), &table.name.as_str()],
        )
        .map_err(database_error)?;
    let mut by_name = BTreeMap::new();
    for row in rows {
        let name: String = row.get(0);
        let data_type: String = row.get(8);
        by_name.insert(
            name.clone(),
            ColumnMeta {
                name: Identifier::new(name).map_err(|error| {
                    ConnectionError::Database(format!("invalid catalog column name: {error}"))
                })?,
                ordinal: u32::try_from(row.get::<_, i32>(1)).map_err(|_| {
                    ConnectionError::Database("negative PostgreSQL column ordinal".into())
                })?,
                vendor_type: row.get(2),
                nullable: row.get(3),
                collation: row.get(4),
                precision: row
                    .get::<_, Option<i32>>(5)
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| ConnectionError::Database("negative numeric precision".into()))?,
                scale: row.get(6),
                timezone_semantics: match data_type.as_str() {
                    "timestamp with time zone" | "time with time zone" => {
                        Some("with_time_zone".into())
                    }
                    "timestamp without time zone" | "time without time zone" => {
                        Some("without_time_zone".into())
                    }
                    _ => None,
                },
            },
        );
    }
    projection
        .iter()
        .map(|name| {
            by_name.get(name.as_str()).cloned().ok_or_else(|| {
                ConnectionError::InvalidRequest(format!(
                    "column {} is not present in {}.{}",
                    name, table.namespace, table.name
                ))
            })
        })
        .collect()
}

fn encoded_size(value: &DbValue) -> usize {
    match value {
        DbValue::Null => 1,
        DbValue::Bool(_) => 2,
        DbValue::Signed(_) | DbValue::Unsigned(_) | DbValue::Time { .. } => 17,
        DbValue::Float32(_) => 5,
        DbValue::Float64(_) => 9,
        DbValue::Date { .. } => 7,
        DbValue::Timestamp { local, .. } => 8 + local.len(),
        DbValue::Decimal { coefficient, .. } => 8 + coefficient.len(),
        DbValue::Text(value) => 9 + value.len(),
        DbValue::Bytes(value) | DbValue::Json(value) => 9 + value.len(),
        DbValue::Vendor { type_id, bytes, .. } => 16 + type_id.len() + bytes.len(),
    }
}

fn database_error(error: postgres::Error) -> ConnectionError {
    if error.code() == Some(&postgres::error::SqlState::QUERY_CANCELED) {
        ConnectionError::Cancelled
    } else {
        ConnectionError::Database(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub endpoint_identity: String,
    pub server_version: String,
    pub server_version_num: i32,
    pub catalog: VendorCatalog,
    pub unsupported: UnsupportedObjectReport,
    pub tls_binding: String,
}

pub fn inspect_endpoint(
    config: &PostgresEndpointConfig,
) -> Result<CatalogSnapshot, PostgresPlanError> {
    let mut client = config.connect()?;
    inspect_connected_endpoint(config, &mut client)
}

fn inspect_connected_endpoint(
    config: &PostgresEndpointConfig,
    client: &mut Client,
) -> Result<CatalogSnapshot, PostgresPlanError> {
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()?;
    let identity = transaction.query_one(
        "SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, 'local'), COALESCE(inet_server_port(), 0), current_setting('server_version'), current_setting('server_version_num')::integer, current_setting('transaction_read_only')::boolean",
        &[],
    )?;
    let database: String = identity.get(0);
    let user: String = identity.get(1);
    let address: String = identity.get(2);
    let port: i32 = identity.get(3);
    let server_version: String = identity.get(4);
    let server_version_num: i32 = identity.get(5);
    transaction.batch_execute("SET LOCAL search_path = pg_catalog")?;
    let read_only: bool = identity.get(6);
    if !read_only {
        return Err(PostgresPlanError::InvalidConfig(
            "catalog transaction is not read-only",
        ));
    }
    let endpoint_identity = format!("postgres://{address}:{port}/{database}?user={user}");
    let (catalog, unsupported) = extract_catalog(&mut transaction, &database, &server_version)?;
    transaction.commit()?;
    Ok(CatalogSnapshot {
        endpoint_identity,
        server_version,
        server_version_num,
        catalog,
        unsupported,
        tls_binding: postgres_tls_binding(config)?,
    })
}

fn extract_catalog(
    transaction: &mut impl postgres::GenericClient,
    database: &str,
    server_version: &str,
) -> Result<(VendorCatalog, UnsupportedObjectReport), PostgresPlanError> {
    transaction.batch_execute("SET LOCAL search_path = pg_catalog")?;
    let database_settings = transaction.query_one(
        "SELECT pg_encoding_to_char(encoding), datcollate, datctype FROM pg_database WHERE datname = current_database()",
        &[],
    )?;
    let server_encoding: String = database_settings.get(0);
    let database_collation: String = database_settings.get(1);
    let database_ctype: String = database_settings.get(2);
    let namespace_rows = transaction.query(
        "SELECT 'namespace:' || n.oid::text, n.nspname, pg_get_userbyid(n.nspowner) FROM pg_namespace n WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname",
        &[],
    )?;
    let mut namespaces = BTreeMap::new();
    for row in namespace_rows {
        let name: String = row.get(1);
        namespaces.insert(
            name.clone(),
            CatalogNamespace {
                id: row.get(0),
                name: Identifier::new(name)?,
                owner: Some(row.get(2)),
                charset: Some(server_encoding.clone()),
                collation: Some(database_collation.clone()),
                objects: Vec::new(),
            },
        );
    }

    let relation_rows = transaction.query(
        "SELECT 'relation:' || c.oid::text, n.nspname, c.relname, c.relkind::text, pg_get_userbyid(c.relowner), c.relpersistence::text, c.relrowsecurity, CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid, true) ELSE NULL END, seq.seqstart::text, seq.seqincrement::text, seq.seqmax::text, seq.seqmin::text, seq.seqcache::text, seq.seqcycle, CASE WHEN seq.seqtypid IS NULL THEN NULL ELSE pg_catalog.format_type(seq.seqtypid, NULL) END, c.relispartition, CASE WHEN parent.oid IS NULL THEN NULL ELSE 'relation:' || parent.oid::text END, CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid, false) END, c.reloptions IS NULL AND c.relacl IS NULL, NOT EXISTS (SELECT 1 FROM pg_attribute va WHERE va.attrelid=c.oid AND va.attnum>0 AND NOT va.attisdropped AND va.attacl IS NOT NULL) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_sequence seq ON seq.seqrelid = c.oid LEFT JOIN pg_inherits inh ON inh.inhrelid = c.oid LEFT JOIN pg_class parent ON parent.oid = inh.inhparent WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p','S','v','m') ORDER BY n.nspname, c.relname, c.relkind",
        &[],
    )?;
    let mut unsupported = Vec::new();
    for row in relation_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let kind: String = row.get(3);
        let relrowsecurity: bool = row.get(6);
        let definition: Option<String> = row.get(7);
        let is_partition: bool = row.get(15);
        let object_kind = match kind.as_str() {
            "r" if is_partition => CatalogObjectKind::Partition,
            "r" | "p" => CatalogObjectKind::Table,
            "S" => CatalogObjectKind::Sequence,
            "v" | "m" => CatalogObjectKind::View,
            _ => CatalogObjectKind::Vendor(kind.clone()),
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("owner".into(), serde_json::Value::String(row.get(4)));
        attributes.insert("persistence".into(), serde_json::Value::String(row.get(5)));
        attributes.insert("relkind".into(), serde_json::Value::String(kind.clone()));
        attributes.insert(
            "row_security".into(),
            serde_json::Value::Bool(relrowsecurity),
        );
        attributes.insert("is_partition".into(), serde_json::Value::Bool(is_partition));
        if is_partition {
            attributes.insert(
                "partition_parent_oid".into(),
                row.get::<_, Option<String>>(16)
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
            );
            attributes.insert(
                "partition_bound".into(),
                row.get::<_, Option<String>>(17)
                    .map(serde_json::Value::String)
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        if kind == "S" {
            for (index, attribute) in [
                (8, "start"),
                (9, "increment"),
                (10, "maximum"),
                (11, "minimum"),
                (12, "cache"),
                (14, "type"),
            ] {
                if let Some(value) = row.get::<_, Option<String>>(index) {
                    attributes.insert(attribute.into(), serde_json::Value::String(value));
                }
            }
            attributes.insert(
                "cycle".into(),
                serde_json::Value::Bool(row.get::<_, Option<bool>>(13).unwrap_or(false)),
            );
            if row.get::<_, String>(5) != "p" {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::SequencePersistence,
                    object_id: id.clone(),
                    object_kind: "sequence_persistence".into(),
                    reason: "unlogged or temporary sequence persistence is not implemented".into(),
                    required_semantics: true,
                });
            }
        }
        if kind == "v" && !relrowsecurity {
            let view_name = Identifier::new(name.clone())?;
            let namespace_name = Identifier::new(namespace.clone())?;
            let create_sql = format!(
                "CREATE VIEW {}.{} AS {}",
                quote_identifier(&namespace_name),
                quote_identifier(&view_name),
                definition.as_deref().unwrap_or_default()
            );
            match parse_postgres_create_view(&create_sql) {
                Ok(ast)
                    if view_security_is_supported(
                        &row.get::<_, String>(5),
                        row.get(18),
                        row.get(19),
                    ) =>
                {
                    let object_oid = id
                        .strip_prefix("relation:")
                        .and_then(|value| value.parse::<u32>().ok())
                        .ok_or(PostgresPlanError::InvalidConfig("view OID is invalid"))?;
                    let dependencies = programmable_dependencies(transaction, object_oid, true)?;
                    let durable = PostgresDurableAst::View(Box::new(ast));
                    attributes.insert(
                        "postgres_durable_ast".into(),
                        serde_json::Value::String(durable.canonical_json().map_err(|_| {
                            PostgresPlanError::InvalidConfig("view AST is invalid")
                        })?),
                    );
                    attributes.insert(
                        "postgres_authoritative_dependencies".into(),
                        serde_json::to_value(dependencies)?,
                    );
                    attributes.insert(
                        "postgres_authoritative_identity".into(),
                        serde_json::to_value(PostgresProgrammableIdentity::Relation {
                            namespace: namespace_name,
                            name: view_name,
                        })?,
                    );
                }
                Ok(_) => unsupported.push(
                    unsupported_view_security(
                        &id,
                        &row.get::<_, String>(5),
                        row.get(18),
                        row.get(19),
                    )
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "supported view unexpectedly reached security rejection",
                    ))?,
                ),
                Err(error) => unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::ViewAst,
                    object_id: id.clone(),
                    object_kind: "view".into(),
                    reason: format!("view is outside the strict ordinary AST subset: {error}"),
                    required_semantics: true,
                }),
            }
        }
        if kind == "m" || relrowsecurity {
            unsupported.push(UnsupportedObject {
                code: if relrowsecurity {
                    UnsupportedObjectCode::RowSecurity
                } else {
                    UnsupportedObjectCode::MaterializedView
                },
                object_id: id.clone(),
                object_kind: if relrowsecurity {
                    "row_security"
                } else if kind == "m" {
                    "materialized_view"
                } else {
                    "partitioned_table"
                }
                .into(),
                reason: "the spike executor cannot reproduce these PostgreSQL semantics".into(),
                required_semantics: true,
            });
        }
        push_object(
            &mut namespaces,
            &namespace,
            CatalogObject {
                id,
                kind: object_kind,
                name: Identifier::new(name)?,
                definition: definition.unwrap_or_default().into_bytes(),
                attributes,
            },
        )?;
    }

    let partition_root_rows = transaction.query(
        "SELECT 'relation:' || root.oid::text, pt.partstrat::text, pt.partnatts, pt.partattrs::smallint[], pt.partexprs IS NULL, a.attname, tn.nspname, t.typname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attcollation = 0 AND NOT EXISTS (SELECT 1 FROM unnest(pt.partcollation::oid[]) coll(oid) WHERE coll.oid<>0), CASE WHEN pt.partdefid = 0 THEN NULL ELSE 'relation:' || pt.partdefid::text END, NOT root.relispartition AND NOT EXISTS (SELECT 1 FROM pg_inherits nested JOIN pg_class nested_root ON nested_root.oid=nested.inhrelid WHERE nested.inhparent=root.oid AND nested_root.relkind='p'), NOT EXISTS (SELECT 1 FROM unnest(pt.partclass::oid[]) cls(oid) JOIN pg_opclass opc ON opc.oid=cls.oid JOIN pg_namespace onsp ON onsp.oid=opc.opcnamespace WHERE onsp.nspname<>'pg_catalog' OR NOT opc.opcdefault) FROM pg_partitioned_table pt JOIN pg_class root ON root.oid=pt.partrelid JOIN pg_namespace rn ON rn.oid=root.relnamespace LEFT JOIN pg_attribute a ON a.attrelid=root.oid AND a.attnum=(pt.partattrs::smallint[])[0] LEFT JOIN pg_type t ON t.oid=a.atttypid LEFT JOIN pg_namespace tn ON tn.oid=t.typnamespace WHERE rn.nspname <> 'information_schema' AND rn.nspname !~ '^pg_' ORDER BY root.oid",
        &[],
    )?;
    let mut unsafe_partition_roots = BTreeSet::new();
    for row in partition_root_rows {
        let root_id: String = row.get(0);
        let strategy: String = row.get(1);
        let key_count: i16 = row.get(2);
        let key_attributes: Vec<i16> = row.get(3);
        let no_expressions: bool = row.get(4);
        let key_column: Option<String> = row.get(5);
        let type_schema: Option<String> = row.get(6);
        let type_name: Option<String> = row.get(7);
        let key_type: Option<String> = row.get(8);
        let no_collation: bool = row.get(9);
        let default_partition: Option<String> = row.get(10);
        let single_level: bool = row.get(11);
        let default_opclasses: bool = row.get(12);
        let supported = matches!(strategy.as_str(), "r" | "l" | "h")
            && key_count == 1
            && key_attributes.len() == 1
            && key_attributes[0] > 0
            && no_expressions
            && type_schema.as_deref() == Some("pg_catalog")
            && matches!(type_name.as_deref(), Some("int2" | "int4" | "int8"))
            && no_collation
            && single_level
            && default_opclasses;
        if !supported {
            unsafe_partition_roots.insert(root_id.clone());
        }
        let Some((namespace_name, object)) = namespaces.iter_mut().find_map(|(name, namespace)| {
            namespace
                .objects
                .iter_mut()
                .find(|object| object.id == root_id && object.kind == CatalogObjectKind::Table)
                .map(|object| (name.clone(), object))
        }) else {
            return Err(PostgresPlanError::InvalidConfig(
                "partition root catalog object is absent",
            ));
        };
        let _ = namespace_name;
        object.attributes.insert(
            "partition_strategy".into(),
            serde_json::Value::String(strategy),
        );
        object.attributes.insert(
            "partition_key_column".into(),
            key_column
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
        object.attributes.insert(
            "partition_key_type".into(),
            key_type
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
        object.attributes.insert(
            "default_partition_oid".into(),
            default_partition
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
    }
    for root_id in unsafe_partition_roots {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::PartitionTopology,
            object_id: root_id,
            object_kind: "partition_topology".into(),
            reason: "partition root is not a single-level RANGE/LIST/HASH topology on one built-in integer column".into(),
            required_semantics: true,
        });
    }
    let unsupported_partition_rows = transaction.query(
        "SELECT object_id, code, reason FROM (
           SELECT 'index:' || i.indexrelid::text AS object_id, 'partition_local_index' AS code, 'partition leaf has a local or unattached index' AS reason FROM pg_index i JOIN pg_class leaf ON leaf.oid=i.indrelid WHERE leaf.relispartition AND NOT EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid=i.indexrelid)
           UNION ALL SELECT 'index:' || idx.oid::text, 'partition_child_index_storage', 'attached partition child index has custom storage options or tablespace' FROM pg_class idx JOIN pg_index i ON i.indexrelid=idx.oid JOIN pg_class leaf ON leaf.oid=i.indrelid WHERE leaf.relispartition AND EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid=idx.oid) AND (idx.reloptions IS NOT NULL OR idx.reltablespace<>0)
           UNION ALL SELECT 'index:' || child_idx.oid::text, 'partition_child_index_name', 'attached partition child index name is not reproducible from its parent index' FROM pg_inherits child_inh JOIN pg_class child_idx ON child_idx.oid=child_inh.inhrelid JOIN pg_class parent_idx ON parent_idx.oid=child_inh.inhparent JOIN pg_index child_i ON child_i.indexrelid=child_idx.oid JOIN pg_class leaf ON leaf.oid=child_i.indrelid JOIN pg_inherits leaf_inh ON leaf_inh.inhrelid=leaf.oid JOIN pg_class root ON root.oid=leaf_inh.inhparent WHERE child_idx.relkind='i' AND parent_idx.relkind='I' AND (left(parent_idx.relname,length(root.relname))<>root.relname OR child_idx.relname<>leaf.relname || substring(parent_idx.relname FROM length(root.relname)+1))
           UNION ALL SELECT 'constraint:' || con.oid::text, 'partition_local_constraint', 'partition leaf has a local constraint' FROM pg_constraint con JOIN pg_class leaf ON leaf.oid=con.conrelid WHERE leaf.relispartition AND con.conparentid=0
           UNION ALL SELECT 'relation:' || leaf.oid::text, 'partition_storage', 'partition leaf has custom storage options or tablespace' FROM pg_class leaf WHERE leaf.relispartition AND (leaf.reloptions IS NOT NULL OR leaf.reltablespace<>0)
           UNION ALL SELECT 'trigger:' || t.oid::text, 'partition_trigger', 'partition leaf has a user trigger' FROM pg_trigger t JOIN pg_class leaf ON leaf.oid=t.tgrelid WHERE leaf.relispartition AND NOT t.tgisinternal
           UNION ALL SELECT 'relation:' || child.oid::text, 'traditional_inheritance', 'traditional table inheritance is not implemented' FROM pg_inherits inh JOIN pg_class child ON child.oid=inh.inhrelid WHERE child.relkind IN ('r','p') AND NOT child.relispartition
         ) excluded ORDER BY object_id",
        &[],
    )?;
    for row in unsupported_partition_rows {
        let object_id: String = row.get(0);
        let code = match row.get::<_, String>(1).as_str() {
            "partition_local_index" => UnsupportedObjectCode::PartitionLocalIndex,
            "partition_child_index_storage" => UnsupportedObjectCode::PartitionChildIndexStorage,
            "partition_child_index_name" => UnsupportedObjectCode::PartitionChildIndexName,
            "partition_local_constraint" => UnsupportedObjectCode::PartitionLocalConstraint,
            "partition_storage" => UnsupportedObjectCode::PartitionStorage,
            "partition_trigger" => UnsupportedObjectCode::PartitionTrigger,
            "traditional_inheritance" => UnsupportedObjectCode::TraditionalInheritance,
            _ => {
                return Err(PostgresPlanError::InvalidConfig(
                    "unknown partition exclusion class",
                ));
            }
        };
        unsupported.push(UnsupportedObject {
            code,
            object_id,
            object_kind: "partition_excluded_semantics".into(),
            reason: row.get(2),
            required_semantics: true,
        });
    }

    let sequence_ownership_rows = transaction.query(
        "SELECT 'relation:' || seq.oid::text, tn.nspname, tbl.relname, a.attname, a.attidentity::text, d.deptype::text, EXISTS (SELECT 1 FROM pg_attrdef ad JOIN pg_depend dd ON dd.classid = 'pg_attrdef'::regclass AND dd.objid = ad.oid AND dd.refclassid = 'pg_class'::regclass AND dd.refobjid = seq.oid AND dd.deptype = 'n' WHERE ad.adrelid = a.attrelid AND ad.adnum = a.attnum) FROM pg_class seq JOIN pg_namespace sn ON sn.oid = seq.relnamespace LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = seq.oid AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a','i') LEFT JOIN pg_class tbl ON tbl.oid = d.refobjid LEFT JOIN pg_namespace tn ON tn.oid = tbl.relnamespace LEFT JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = d.refobjsubid WHERE seq.relkind = 'S' AND sn.nspname <> 'information_schema' AND sn.nspname !~ '^pg_' ORDER BY seq.oid, tn.nspname, tbl.relname, a.attnum",
        &[],
    )?;
    let mut sequence_ownership = BTreeMap::<String, Vec<PostgresSequenceOwnership>>::new();
    let mut malformed_sequence_ownership = BTreeSet::new();
    for row in sequence_ownership_rows {
        let sequence_id: String = row.get(0);
        let Some(table_namespace) = row.get::<_, Option<String>>(1) else {
            sequence_ownership.entry(sequence_id).or_default();
            continue;
        };
        let values = (
            row.get::<_, Option<String>>(2),
            row.get::<_, Option<String>>(3),
            row.get::<_, Option<String>>(4),
            row.get::<_, Option<String>>(5),
            row.get::<_, Option<bool>>(6),
        );
        let (Some(table_name), Some(column_name), Some(identity), Some(dependency_type), default) =
            values
        else {
            malformed_sequence_ownership.insert(sequence_id);
            continue;
        };
        let kind = match (identity.as_str(), dependency_type.as_str()) {
            ("a", "i") => Some(PostgresSequenceOwnershipKind::IdentityAlways),
            ("d", "i") => Some(PostgresSequenceOwnershipKind::IdentityByDefault),
            ("", "a") if default == Some(true) => Some(PostgresSequenceOwnershipKind::Serial),
            _ => None,
        };
        let Some(kind) = kind else {
            malformed_sequence_ownership.insert(sequence_id);
            continue;
        };
        sequence_ownership
            .entry(sequence_id)
            .or_default()
            .push(PostgresSequenceOwnership {
                table: QualifiedTable {
                    namespace: Identifier::new(table_namespace)?,
                    name: Identifier::new(table_name)?,
                },
                column: Identifier::new(column_name)?,
                kind,
            });
    }
    for namespace in namespaces.values_mut() {
        if namespace.name.as_str() == "sql_splitter_migration_fence" {
            continue;
        }
        for object in namespace
            .objects
            .iter_mut()
            .filter(|object| object.kind == CatalogObjectKind::Sequence)
        {
            let state = transaction.query_one(
                &format!(
                    "SELECT last_value, is_called FROM {}.{}",
                    quote_identifier(&namespace.name),
                    quote_identifier(&object.name)
                ),
                &[],
            )?;
            object.attributes.insert(
                "last_value".into(),
                serde_json::Value::String(state.get::<_, i64>(0).to_string()),
            );
            object
                .attributes
                .insert("is_called".into(), serde_json::Value::Bool(state.get(1)));
            let owners = sequence_ownership.remove(&object.id).unwrap_or_default();
            object
                .attributes
                .insert("ownership_count".into(), serde_json::json!(owners.len()));
            if owners.len() == 1 && !malformed_sequence_ownership.contains(&object.id) {
                object
                    .attributes
                    .insert("ownership".into(), serde_json::to_value(&owners[0])?);
            } else {
                object
                    .attributes
                    .insert("ownership".into(), serde_json::Value::Null);
            }
            if owners.len() > 1 || malformed_sequence_ownership.contains(&object.id) {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::SequenceOwnership,
                    object_id: object.id.clone(),
                    object_kind: "sequence_ownership".into(),
                    reason: "sequence ownership is malformed, dangling, or multiply defined".into(),
                    required_semantics: true,
                });
            }
        }
    }

    let user_type_rows = transaction.query(
        "SELECT 'type:' || t.oid::text, n.nspname, t.typname, t.typtype::text, pg_catalog.format_type(t.oid, NULL) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND t.typtype IN ('c','d','e','r') AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid) ORDER BY n.nspname, t.typname",
        &[],
    )?;
    for row in user_type_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let type_kind: String = row.get(3);
        let definition: String = row.get(4);
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::UserTypeDdl,
            object_id: id.clone(),
            object_kind: format!("postgres_type:{type_kind}"),
            reason: "user-defined PostgreSQL type DDL is not implemented".into(),
            required_semantics: true,
        });
        push_object(
            &mut namespaces,
            &namespace,
            CatalogObject {
                id,
                kind: CatalogObjectKind::Vendor(format!("postgres_type:{type_kind}")),
                name: Identifier::new(name)?,
                definition: definition.into_bytes(),
                attributes: BTreeMap::from([(
                    "type_kind".into(),
                    serde_json::Value::String(type_kind),
                )]),
            },
        )?;
    }

    let extension_rows = transaction.query(
        "SELECT 'extension:' || e.oid::text, n.nspname, e.extname, e.extversion FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname <> 'plpgsql' ORDER BY e.extname",
        &[],
    )?;
    for row in extension_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let version: String = row.get(3);
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::Extension,
            object_id: id.clone(),
            object_kind: "extension".into(),
            reason: "PostgreSQL extension installation and version validation are not implemented"
                .into(),
            required_semantics: true,
        });
        if namespaces.contains_key(&namespace) {
            push_object(
                &mut namespaces,
                &namespace,
                CatalogObject {
                    id,
                    kind: CatalogObjectKind::Vendor("extension".into()),
                    name: Identifier::new(name)?,
                    definition: Vec::new(),
                    attributes: BTreeMap::from([(
                        "version".into(),
                        serde_json::Value::String(version),
                    )]),
                },
            )?;
        }
    }

    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT ('column:' || a.attrelid::text || ':' || a.attnum::text), n.nspname, a.attname, 'column', pg_catalog.format_type(a.atttypid, a.atttypmod), jsonb_build_object('table_oid', 'relation:' || c.oid::text, 'table', c.relname, 'ordinal', a.attnum, 'nullable', NOT a.attnotnull, 'default', CASE WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid, false) END, 'generated_expression', CASE WHEN a.attgenerated = 's' THEN pg_get_expr(ad.adbin, ad.adrelid, false) END, 'sequence_default_oid', (SELECT 'relation:' || dd.refobjid::text FROM pg_depend dd JOIN pg_class seq ON seq.oid = dd.refobjid AND seq.relkind = 'S' WHERE ad.oid IS NOT NULL AND a.attgenerated = '' AND dd.classid = 'pg_attrdef'::regclass AND dd.objid = ad.oid AND dd.refclassid = 'pg_class'::regclass AND dd.deptype = 'n'), 'identity', a.attidentity::text, 'generated', a.attgenerated::text, 'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collname END, 'collation_schema', CASE WHEN a.attcollation = 0 THEN NULL ELSE colln.nspname END, 'collation_provider', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collprovider::text END, 'collation_deterministic', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collisdeterministic END, 'collation_version', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collversion END, 'collation_actual_version', CASE WHEN a.attcollation = 0 THEN NULL ELSE pg_collation_actual_version(coll.oid) END, 'type_schema', typen.nspname, 'type_name', typ.typname)::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_type typ ON typ.oid = a.atttypid JOIN pg_namespace typen ON typen.oid = typ.typnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum LEFT JOIN pg_collation coll ON coll.oid = a.attcollation LEFT JOIN pg_namespace colln ON colln.oid = coll.collnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY n.nspname, c.relname, a.attnum",
        CatalogObjectKind::Column,
    )?;
    let generated_dependency_rows = transaction.query(
        "SELECT 'column:' || a.attrelid::text || ':' || a.attnum::text,
                CASE d.refclassid
                  WHEN 'pg_class'::regclass THEN 'column'
                  WHEN 'pg_proc'::regclass THEN 'function'
                  WHEN 'pg_operator'::regclass THEN 'operator'
                  WHEN 'pg_type'::regclass THEN 'type'
                  WHEN 'pg_collation'::regclass THEN 'collation'
                  ELSE 'unknown'
                END,
                CASE d.refclassid
                  WHEN 'pg_class'::regclass THEN 'column:' || refa.attname
                  WHEN 'pg_proc'::regclass THEN 'function:' || pn.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')->' || pg_catalog.format_type(p.prorettype, NULL)
                  WHEN 'pg_operator'::regclass THEN 'operator:' || opn.nspname || '.' || o.oprname || '(' || pg_catalog.format_type(o.oprleft, NULL) || ',' || pg_catalog.format_type(o.oprright, NULL) || ')->' || pg_catalog.format_type(o.oprresult, NULL) || ':function=' || ofn_n.nspname || '.' || ofn.proname || '(' || pg_get_function_identity_arguments(ofn.oid) || ')'
                  WHEN 'pg_type'::regclass THEN 'type:' || tyn.nspname || '.' || ty.typname
                  WHEN 'pg_collation'::regclass THEN 'collation:' || cn.nspname || '.' || co.collname || ':provider=' || co.collprovider::text || ':deterministic=' || co.collisdeterministic::text || ':version=' || COALESCE(co.collversion, '') || ':actual=' || COALESCE(pg_collation_actual_version(co.oid), '')
                  ELSE d.refclassid::regclass::text || ':' || d.refobjid::text || ':' || d.refobjsubid::text
                END,
                CASE d.refclassid
                  WHEN 'pg_class'::regclass THEN d.refobjid = a.attrelid AND d.refobjsubid > 0 AND refa.attgenerated = ''
                  WHEN 'pg_proc'::regclass THEN pn.nspname = 'pg_catalog' AND p.provolatile = 'i' AND NOT p.prosecdef
                  WHEN 'pg_operator'::regclass THEN opn.nspname = 'pg_catalog' AND ofn_n.nspname = 'pg_catalog' AND ofn.provolatile = 'i' AND NOT ofn.prosecdef
                  WHEN 'pg_type'::regclass THEN tyn.nspname = 'pg_catalog'
                  WHEN 'pg_collation'::regclass THEN cn.nspname = 'pg_catalog' AND co.collisdeterministic AND (co.collversion IS NULL OR co.collversion IS NOT DISTINCT FROM pg_collation_actual_version(co.oid))
                  ELSE false
                END
         FROM pg_attribute a
         JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
         JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid AND d.deptype IN ('n','a','i')
         LEFT JOIN pg_attribute refa ON d.refclassid = 'pg_class'::regclass AND refa.attrelid = d.refobjid AND refa.attnum = d.refobjsubid
         LEFT JOIN pg_proc p ON d.refclassid = 'pg_proc'::regclass AND p.oid = d.refobjid
         LEFT JOIN pg_namespace pn ON pn.oid = p.pronamespace
         LEFT JOIN pg_operator o ON d.refclassid = 'pg_operator'::regclass AND o.oid = d.refobjid
         LEFT JOIN pg_namespace opn ON opn.oid = o.oprnamespace
         LEFT JOIN pg_proc ofn ON ofn.oid = o.oprcode
         LEFT JOIN pg_namespace ofn_n ON ofn_n.oid = ofn.pronamespace
         LEFT JOIN pg_type ty ON d.refclassid = 'pg_type'::regclass AND ty.oid = d.refobjid
         LEFT JOIN pg_namespace tyn ON tyn.oid = ty.typnamespace
         LEFT JOIN pg_collation co ON d.refclassid = 'pg_collation'::regclass AND co.oid = d.refobjid
         LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
         WHERE a.attgenerated = 's' AND a.attnum > 0 AND NOT a.attisdropped
           AND NOT (d.refclassid = 'pg_class'::regclass AND d.refobjid = a.attrelid AND d.refobjsubid = a.attnum)
         ORDER BY a.attrelid, a.attnum, 2, 3",
        &[],
    )?;
    let mut generated_dependencies = BTreeMap::<String, Vec<PostgresGeneratedDependency>>::new();
    let mut unsafe_generated_columns = BTreeSet::new();
    for row in generated_dependency_rows {
        let column_id: String = row.get(0);
        let kind = match row.get::<_, String>(1).as_str() {
            "column" => PostgresGeneratedDependencyKind::Column,
            "function" => PostgresGeneratedDependencyKind::Function,
            "operator" => PostgresGeneratedDependencyKind::Operator,
            "type" => PostgresGeneratedDependencyKind::Type,
            "collation" => PostgresGeneratedDependencyKind::Collation,
            _ => {
                unsafe_generated_columns.insert(column_id);
                continue;
            }
        };
        if !row.get::<_, bool>(3) {
            unsafe_generated_columns.insert(column_id.clone());
        }
        generated_dependencies
            .entry(column_id)
            .or_default()
            .push(PostgresGeneratedDependency {
                kind,
                identity: row.get(2),
            });
    }
    for namespace in namespaces.values_mut() {
        for object in namespace.objects.iter_mut().filter(|object| {
            object.kind == CatalogObjectKind::Column
                && object
                    .attributes
                    .get("generated")
                    .and_then(serde_json::Value::as_str)
                    == Some("s")
        }) {
            let mut dependencies = generated_dependencies
                .remove(&object.id)
                .unwrap_or_default();
            dependencies.sort();
            dependencies.dedup();
            object.attributes.insert(
                "generated_dependencies".into(),
                serde_json::to_value(dependencies)?,
            );
            if unsafe_generated_columns.contains(&object.id) {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::GeneratedDependency,
                    object_id: object.id.clone(),
                    object_kind: "generated_column_dependency".into(),
                    reason: "generated expression has a non-pg_catalog, mutable, generated-column, or unknown dependency".into(),
                    required_semantics: true,
                });
            }
        }
    }
    for object in namespaces
        .values()
        .flat_map(|namespace| namespace.objects.iter())
        .filter(|object| object.kind == CatalogObjectKind::Column)
    {
        if object
            .attributes
            .get("generated")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value != "s" && !value.is_empty())
        {
            unsupported.push(UnsupportedObject {
                code: UnsupportedObjectCode::GeneratedMode,
                object_id: object.id.clone(),
                object_kind: "generated_column".into(),
                reason: "only stored PostgreSQL generated columns are implemented".into(),
                required_semantics: true,
            });
        }
        if object
            .attributes
            .get("type_schema")
            .and_then(serde_json::Value::as_str)
            != Some("pg_catalog")
        {
            unsupported.push(UnsupportedObject {
                code: UnsupportedObjectCode::UserDefinedColumnType,
                object_id: object.id.clone(),
                object_kind: "user_defined_column_type".into(),
                reason: "user-defined PostgreSQL types are not reproduced by the executor".into(),
                required_semantics: true,
            });
        }
        let recorded_collation_version = object
            .attributes
            .get("collation_version")
            .and_then(serde_json::Value::as_str);
        let actual_collation_version = object
            .attributes
            .get("collation_actual_version")
            .and_then(serde_json::Value::as_str);
        if recorded_collation_version.is_some()
            && recorded_collation_version != actual_collation_version
        {
            unsupported.push(UnsupportedObject {
                code: UnsupportedObjectCode::CollationVersion,
                object_id: object.id.clone(),
                object_kind: "collation_version_mismatch".into(),
                reason: "recorded collation version differs from the provider's actual version"
                    .into(),
                required_semantics: true,
            });
        }
    }
    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT 'constraint:' || con.oid::text, n.nspname, con.conname, 'constraint', pg_get_constraintdef(con.oid, true), jsonb_build_object('table_oid', 'relation:' || con.conrelid::text, 'type', con.contype::text, 'validated', con.convalidated, 'deferrable', con.condeferrable, 'deferred', con.condeferred, 'parent_constraint_oid', CASE WHEN con.conparentid = 0 THEN NULL ELSE 'constraint:' || con.conparentid::text END, 'referenced_table_oid', CASE WHEN con.confrelid = 0 THEN NULL ELSE 'relation:' || con.confrelid::text END, 'columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(con.conkey) WITH ORDINALITY keys(attnum, ordinality) JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = keys.attnum), '[]'::jsonb), 'referenced_columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(con.confkey) WITH ORDINALITY keys(attnum, ordinality) JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = keys.attnum), '[]'::jsonb), 'match_type', con.confmatchtype::text, 'update_action', con.confupdtype::text, 'delete_action', con.confdeltype::text, 'delete_set_columns', to_jsonb(con)->'confdelsetcols')::text FROM pg_constraint con JOIN pg_namespace n ON n.oid = con.connamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, con.conname, con.oid",
        CatalogObjectKind::CheckConstraint,
    )?;
    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT 'index:' || i.indexrelid::text, n.nspname, ci.relname, 'index', pg_get_indexdef(i.indexrelid), jsonb_build_object('table_oid', 'relation:' || i.indrelid::text, 'unique', i.indisunique, 'primary', i.indisprimary, 'valid', i.indisvalid, 'ready', i.indisready, 'live', i.indislive, 'immediate', i.indimmediate, 'clustered', i.indisclustered, 'replica_identity', i.indisreplident, 'exclusion', i.indisexclusion, 'nulls_not_distinct', i.indnullsnotdistinct, 'access_method', am.amname, 'persistence', ci.relpersistence::text, 'reloptions', to_jsonb(ci.reloptions), 'tablespace', CASE WHEN ci.reltablespace = 0 THEN NULL ELSE ts.spcname END, 'predicate', pg_get_expr(i.indpred, i.indrelid), 'has_expressions', i.indexprs IS NOT NULL, 'key_attribute_count', i.indnkeyatts, 'attribute_count', i.indnatts, 'columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY keys(attnum, ordinality) LEFT JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = keys.attnum WHERE keys.ordinality <= i.indnkeyatts), '[]'::jsonb), 'included_columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(i.indkey::smallint[]) WITH ORDINALITY keys(attnum, ordinality) LEFT JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = keys.attnum WHERE keys.ordinality > i.indnkeyatts), '[]'::jsonb), 'options', COALESCE((SELECT jsonb_agg(option_value ORDER BY options.ordinality) FROM unnest(i.indoption::smallint[]) WITH ORDINALITY options(option_value, ordinality)), '[]'::jsonb), 'opclasses', COALESCE((SELECT jsonb_agg(jsonb_build_object('schema', opn.nspname, 'name', opc.opcname, 'default', opc.opcdefault) ORDER BY classes.ordinality) FROM unnest(i.indclass::oid[]) WITH ORDINALITY classes(opclass_oid, ordinality) JOIN pg_opclass opc ON opc.oid = classes.opclass_oid JOIN pg_namespace opn ON opn.oid = opc.opcnamespace), '[]'::jsonb), 'collations', COALESCE((SELECT jsonb_agg(CASE WHEN classes.collation_oid = 0 THEN NULL ELSE jsonb_build_object('schema', colln.nspname, 'name', coll.collname) END ORDER BY classes.ordinality) FROM unnest(i.indcollation::oid[]) WITH ORDINALITY classes(collation_oid, ordinality) LEFT JOIN pg_collation coll ON coll.oid = classes.collation_oid LEFT JOIN pg_namespace colln ON colln.oid = coll.collnamespace), '[]'::jsonb), 'collations_default', COALESCE((SELECT jsonb_agg(classes.collation_oid = att.attcollation ORDER BY classes.ordinality) FROM unnest(i.indcollation::oid[]) WITH ORDINALITY classes(collation_oid, ordinality) JOIN unnest(i.indkey::smallint[]) WITH ORDINALITY keys(attnum, ordinality) USING (ordinality) LEFT JOIN pg_attribute att ON att.attrelid = i.indrelid AND att.attnum = keys.attnum), '[]'::jsonb), 'constraint_oid', CASE WHEN con.oid IS NULL THEN NULL ELSE 'constraint:' || con.oid::text END)::text FROM pg_index i JOIN pg_class ci ON ci.oid = i.indexrelid JOIN pg_class ct ON ct.oid = i.indrelid JOIN pg_namespace n ON n.oid = ct.relnamespace JOIN pg_am am ON am.oid = ci.relam LEFT JOIN pg_tablespace ts ON ts.oid = ci.reltablespace LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.contype IN ('p','u','x') WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, ci.relname, i.indexrelid",
        CatalogObjectKind::Index,
    )?;
    for namespace in namespaces.values() {
        for object in namespace.objects.iter().filter(|object| {
            object.kind == CatalogObjectKind::Index
                && object
                    .attributes
                    .get("constraint_oid")
                    .is_none_or(serde_json::Value::is_null)
        }) {
            if let (Err(unique_error), Err(ordinary_error)) = (
                standalone_unique_index_columns(object, Some(namespace)),
                ordinary_index_columns(object, Some(namespace)),
            ) {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::StandaloneIndex,
                    object_id: object.id.clone(),
                    object_kind: "standalone_index".into(),
                    reason: format!(
                        "standalone PostgreSQL index is not supported: unique form: {unique_error}; ordinary form: {ordinary_error}"
                    ),
                    required_semantics: true,
                });
            }
        }
    }

    let trigger_rows = transaction.query(
        "SELECT 'trigger:' || t.oid::text, n.nspname, t.tgname, pg_get_triggerdef(t.oid, true), 'relation:' || c.oid::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE NOT t.tgisinternal AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, t.tgname, t.oid",
        &[],
    )?;
    for row in trigger_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let definition: String = row.get(3);
        let table_oid: String = row.get(4);
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::Trigger,
            object_id: id.clone(),
            object_kind: "trigger".into(),
            reason: "trigger execution semantics are not implemented".into(),
            required_semantics: true,
        });
        push_object(
            &mut namespaces,
            &namespace,
            CatalogObject {
                id,
                kind: CatalogObjectKind::Trigger,
                name: Identifier::new(name)?,
                definition: definition.into_bytes(),
                attributes: BTreeMap::from([(
                    "table_oid".into(),
                    serde_json::Value::String(table_oid),
                )]),
            },
        )?;
    }

    let routine_rows = transaction.query(
        "SELECT 'routine:' || p.oid::text, n.nspname, p.proname, pg_get_functiondef(p.oid), p.oid,
                p.prokind='f' AND l.lanname='sql' AND p.provolatile='i' AND NOT p.prosecdef
                AND NOT p.proleakproof AND p.proparallel='s' AND p.proisstrict
                AND NOT p.proretset AND p.provariadic=0 AND p.pronargdefaults=0
                AND p.proconfig IS NULL AND p.prosupport=0 AND p.proacl IS NULL
                AND NOT EXISTS (SELECT 1 FROM pg_proc sibling WHERE sibling.pronamespace=p.pronamespace AND sibling.proname=p.proname AND sibling.oid<>p.oid),
                pg_get_function_identity_arguments(p.oid), pg_catalog.format_type(p.prorettype,NULL)
         FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
         WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
         ORDER BY n.nspname,p.proname,p.oid",
        &[],
    )?;
    for row in routine_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let definition: String = row.get(3);
        let allowed: bool = row.get(5);
        let create_only = definition.replacen("CREATE OR REPLACE FUNCTION ", "CREATE FUNCTION ", 1);
        let ast = allowed
            .then(|| parse_postgres_sql_function(&create_only))
            .transpose();
        let mut attributes = BTreeMap::new();
        match ast {
            Ok(Some(ast)) if create_only != definition => {
                let dependencies =
                    programmable_dependencies(transaction, row.get::<_, u32>(4), false)?;
                let durable = PostgresDurableAst::SqlFunction(Box::new(ast));
                attributes.insert(
                    "postgres_durable_ast".into(),
                    serde_json::Value::String(durable.canonical_json().map_err(|_| {
                        PostgresPlanError::InvalidConfig("SQL function AST is invalid")
                    })?),
                );
                attributes.insert(
                    "postgres_authoritative_identity".into(),
                    serde_json::to_value(PostgresProgrammableIdentity::Function {
                        namespace: Identifier::new(namespace.clone())?,
                        name: Identifier::new(name.clone())?,
                        identity_arguments: row.get(6),
                        return_type: row.get(7),
                    })?,
                );
                attributes.insert(
                    "postgres_authoritative_dependencies".into(),
                    serde_json::to_value(dependencies)?,
                );
            }
            _ => unsupported.push(UnsupportedObject {
                code: UnsupportedObjectCode::Routine,
                object_id: id.clone(),
                object_kind: "routine".into(),
                reason: "routine is outside the immutable SQL scalar create-only AST subset".into(),
                required_semantics: true,
            }),
        }
        push_object(
            &mut namespaces,
            &namespace,
            CatalogObject {
                id,
                kind: CatalogObjectKind::Routine,
                name: Identifier::new(name)?,
                definition: definition.into_bytes(),
                attributes,
            },
        )?;
    }

    let policy_rows = transaction.query(
        "SELECT 'policy:' || pol.oid::text, n.nspname, pol.polname FROM pg_policy pol JOIN pg_class c ON c.oid = pol.polrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, pol.polname, pol.oid",
        &[],
    )?;
    for row in policy_rows {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::RowSecurityPolicy,
            object_id: row.get(0),
            object_kind: "row_security_policy".into(),
            reason: format!(
                "row security policy {}.{} is not implemented",
                row.get::<_, String>(1),
                row.get::<_, String>(2)
            ),
            required_semantics: true,
        });
    }

    let unsupported_catalog_rows = transaction.query(
        "SELECT object_id, object_kind, reason FROM (\
         SELECT 'namespace-acl:' || n.oid::text AS object_id, 'namespace_acl' AS object_kind, 'custom namespace privileges are not implemented' AS reason FROM pg_namespace n WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND n.nspacl IS NOT NULL \
         UNION ALL SELECT 'relation-acl:' || c.oid::text, 'relation_acl', 'custom relation privileges are not implemented' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relacl IS NOT NULL \
         UNION ALL SELECT 'routine-acl:' || p.oid::text, 'routine_acl', 'custom routine privileges are not implemented' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND p.proacl IS NOT NULL \
         UNION ALL SELECT 'default-acl:' || d.oid::text, 'default_privileges', 'default privileges are not implemented' FROM pg_default_acl d \
         UNION ALL SELECT 'event-trigger:' || e.oid::text, 'event_trigger', 'event-trigger semantics are not implemented' FROM pg_event_trigger e \
         UNION ALL SELECT 'rule:' || r.oid::text, 'rewrite_rule', 'rewrite-rule semantics are not implemented' FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND r.rulename <> '_RETURN' \
         UNION ALL SELECT 'publication:' || p.oid::text, 'logical_replication_publication', 'logical replication publications are not implemented' FROM pg_publication p \
         UNION ALL SELECT 'foreign-server:' || s.oid::text, 'foreign_server', 'foreign-data server semantics are not implemented' FROM pg_foreign_server s \
         UNION ALL SELECT 'foreign-table:' || c.oid::text, 'foreign_table', 'foreign-table semantics are not implemented' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind = 'f' \
         UNION ALL SELECT 'extended-statistics:' || s.oid::text, 'extended_statistics', 'extended statistics are not implemented' FROM pg_statistic_ext s JOIN pg_namespace n ON n.oid = s.stxnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' \
         UNION ALL SELECT 'collation:' || c.oid::text, 'user_collation', 'user-defined collation installation and version validation are not implemented' FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'\
         ) unsupported_catalog ORDER BY object_id",
        &[],
    )?;
    for row in unsupported_catalog_rows {
        let object_kind: String = row.get(1);
        let code = match object_kind.as_str() {
            "namespace_acl" => UnsupportedObjectCode::NamespaceAcl,
            "relation_acl" => UnsupportedObjectCode::RelationAcl,
            "routine_acl" => UnsupportedObjectCode::RoutineAcl,
            "default_privileges" => UnsupportedObjectCode::DefaultPrivileges,
            "event_trigger" => UnsupportedObjectCode::EventTrigger,
            "rewrite_rule" => UnsupportedObjectCode::RewriteRule,
            "logical_replication_publication" => UnsupportedObjectCode::Publication,
            "foreign_server" => UnsupportedObjectCode::ForeignServer,
            "foreign_table" => UnsupportedObjectCode::ForeignTable,
            "extended_statistics" => UnsupportedObjectCode::ExtendedStatistics,
            "user_collation" => UnsupportedObjectCode::UserCollation,
            _ => {
                return Err(PostgresPlanError::InvalidConfig(
                    "unknown unsupported PostgreSQL catalog class",
                ));
            }
        };
        unsupported.push(UnsupportedObject {
            code,
            object_id: row.get(0),
            required_semantics: !matches!(
                object_kind.as_str(),
                "namespace_acl" | "relation_acl" | "routine_acl" | "default_privileges"
            ),
            object_kind,
            reason: row.get(2),
        });
    }

    let dependencies = catalog_dependencies(&namespaces);
    for namespace in namespaces.values_mut() {
        namespace.objects.sort_by(|left, right| {
            object_kind_key(&left.kind)
                .cmp(&object_kind_key(&right.kind))
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.id.cmp(&right.id))
        });
    }
    unsupported.sort_by(|left, right| left.object_id.cmp(&right.object_id));
    let catalog = VendorCatalog {
        format_version: CATALOG_FORMAT_VERSION,
        dialect: "postgresql".into(),
        server_version: server_version.into(),
        database: Identifier::new(database)?,
        namespaces: namespaces.into_values().collect(),
        dependencies,
        vendor_metadata: BTreeMap::from([
            ("server_encoding".into(), server_encoding),
            ("lc_collate".into(), database_collation),
            ("lc_ctype".into(), database_ctype),
        ]),
    };
    validate_catalog_identity(&catalog)?;
    Ok((
        catalog,
        UnsupportedObjectReport {
            objects: unsupported,
        },
    ))
}

fn programmable_dependencies(
    client: &mut impl postgres::GenericClient,
    object_oid: u32,
    view: bool,
) -> Result<Vec<PostgresProgrammableIdentity>, PostgresPlanError> {
    let rows = client.query(
        "SELECT DISTINCT CASE d.refclassid WHEN 'pg_class'::regclass THEN 'relation' WHEN 'pg_proc'::regclass THEN 'function' WHEN 'pg_type'::regclass THEN 'type' WHEN 'pg_collation'::regclass THEN 'collation' WHEN 'pg_operator'::regclass THEN 'operator' WHEN 'pg_namespace'::regclass THEN 'namespace' WHEN 'pg_language'::regclass THEN 'language' ELSE NULL END,
           COALESCE(rn.nspname,pn.nspname,tn.nspname,cn.nspname,onsp.nspname),
           COALESCE(rc.relname,p.proname,t.typname,coll.collname,op.oprname,dn.nspname,lang.lanname),
           CASE WHEN p.oid IS NULL THEN NULL ELSE pg_get_function_identity_arguments(p.oid) END,
           CASE WHEN p.oid IS NULL THEN NULL ELSE pg_catalog.format_type(p.prorettype,NULL) END,
           CASE WHEN op.oid IS NULL THEN NULL ELSE pg_catalog.format_type(op.oprleft,NULL) END,
           CASE WHEN op.oid IS NULL THEN NULL ELSE pg_catalog.format_type(op.oprright,NULL) END,
           CASE WHEN op.oid IS NULL THEN NULL ELSE pg_catalog.format_type(op.oprresult,NULL) END
         FROM pg_depend d
         LEFT JOIN pg_class rc ON d.refclassid='pg_class'::regclass AND rc.oid=d.refobjid
         LEFT JOIN pg_namespace rn ON rn.oid=rc.relnamespace
         LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
         LEFT JOIN pg_namespace pn ON pn.oid=p.pronamespace
         LEFT JOIN pg_type t ON d.refclassid='pg_type'::regclass AND t.oid=d.refobjid
         LEFT JOIN pg_namespace tn ON tn.oid=t.typnamespace
         LEFT JOIN pg_collation coll ON d.refclassid='pg_collation'::regclass AND coll.oid=d.refobjid
         LEFT JOIN pg_namespace cn ON cn.oid=coll.collnamespace
         LEFT JOIN pg_operator op ON d.refclassid='pg_operator'::regclass AND op.oid=d.refobjid
         LEFT JOIN pg_namespace onsp ON onsp.oid=op.oprnamespace
         LEFT JOIN pg_namespace dn ON d.refclassid='pg_namespace'::regclass AND dn.oid=d.refobjid
         LEFT JOIN pg_language lang ON d.refclassid='pg_language'::regclass AND lang.oid=d.refobjid
         WHERE (( $2 AND d.classid='pg_rewrite'::regclass AND d.objid=(SELECT oid FROM pg_rewrite WHERE ev_class=$1 AND rulename='_RETURN'))
             OR (NOT $2 AND d.classid='pg_proc'::regclass AND d.objid=$1))
           AND d.deptype IN ('n','a')
           AND NOT (d.refclassid='pg_class'::regclass AND d.refobjid=$1)
         ORDER BY 1,2,3,4,5,6,7,8",
        &[&object_oid, &view],
    )?;
    let mut identities = rows
        .into_iter()
        .map(programmable_identity_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    identities.sort();
    if !identities.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(PostgresPlanError::InvalidConfig(
            "programmable pg_depend closure contains duplicate identities",
        ));
    }
    Ok(identities)
}

fn programmable_identity_from_row(
    row: postgres::Row,
) -> Result<PostgresProgrammableIdentity, PostgresPlanError> {
    let kind = row
        .get::<_, Option<String>>(0)
        .ok_or(PostgresPlanError::InvalidConfig(
            "programmable object has unsupported pg_depend class",
        ))?;
    let namespace = || {
        row.get::<_, Option<String>>(1)
            .ok_or(PostgresPlanError::InvalidConfig(
                "dependency namespace is absent",
            ))
            .and_then(|value| Identifier::new(value).map_err(Into::into))
    };
    let name = || {
        row.get::<_, Option<String>>(2)
            .ok_or(PostgresPlanError::InvalidConfig(
                "dependency name is absent",
            ))
            .and_then(|value| Identifier::new(value).map_err(Into::into))
    };
    Ok(match kind.as_str() {
        "relation" => PostgresProgrammableIdentity::Relation {
            namespace: namespace()?,
            name: name()?,
        },
        "function" => PostgresProgrammableIdentity::Function {
            namespace: namespace()?,
            name: name()?,
            identity_arguments: row.get(3),
            return_type: row.get(4),
        },
        "type" => PostgresProgrammableIdentity::Type {
            namespace: namespace()?,
            name: name()?,
        },
        "collation" => PostgresProgrammableIdentity::Collation {
            namespace: namespace()?,
            name: name()?,
        },
        "operator" => PostgresProgrammableIdentity::Operator {
            namespace: namespace()?,
            name: name()?,
            left_type: row.get(5),
            right_type: row.get(6),
            result_type: row.get(7),
        },
        "namespace" => PostgresProgrammableIdentity::Namespace { name: name()? },
        "language" => PostgresProgrammableIdentity::Language { name: name()? },
        _ => {
            return Err(PostgresPlanError::InvalidConfig(
                "programmable object has unsupported pg_depend class",
            ));
        }
    })
}

fn validate_catalog_identity(catalog: &VendorCatalog) -> Result<(), PostgresPlanError> {
    let mut identities = BTreeSet::new();
    for namespace in &catalog.namespaces {
        if !identities.insert(namespace.id.as_str()) {
            return Err(PostgresPlanError::InvalidConfig(
                "PostgreSQL catalog contains a duplicate namespace identity",
            ));
        }
        for object in &namespace.objects {
            if !identities.insert(object.id.as_str()) {
                return Err(PostgresPlanError::InvalidConfig(
                    "PostgreSQL catalog contains a duplicate object identity",
                ));
            }
        }
    }
    for dependency in &catalog.dependencies {
        if !identities.contains(dependency.from_object_id.as_str())
            || !identities.contains(dependency.to_object_id.as_str())
        {
            return Err(PostgresPlanError::InvalidConfig(
                "PostgreSQL catalog dependency refers to an unknown identity",
            ));
        }
    }
    Ok(())
}

fn append_query_objects(
    transaction: &mut impl postgres::GenericClient,
    namespaces: &mut BTreeMap<String, CatalogNamespace>,
    query: &str,
    kind: CatalogObjectKind,
) -> Result<(), PostgresPlanError> {
    for row in transaction.query(query, &[])? {
        let definition: String = row.get(4);
        let attributes_json: String = row.get(5);
        let attributes: BTreeMap<String, serde_json::Value> =
            serde_json::from_str(&attributes_json)?;
        let object_kind = if kind == CatalogObjectKind::CheckConstraint {
            match attributes.get("type").and_then(serde_json::Value::as_str) {
                Some("p") => CatalogObjectKind::PrimaryKey,
                Some("u") => CatalogObjectKind::UniqueConstraint,
                Some("f") => CatalogObjectKind::ForeignKey,
                Some("c") => CatalogObjectKind::CheckConstraint,
                Some(value) => CatalogObjectKind::Vendor(format!("constraint:{value}")),
                None => CatalogObjectKind::CheckConstraint,
            }
        } else {
            kind.clone()
        };
        push_object(
            namespaces,
            &row.get::<_, String>(1),
            CatalogObject {
                id: row.get(0),
                kind: object_kind,
                name: Identifier::new(row.get::<_, String>(2))?,
                definition: definition.into_bytes(),
                attributes,
            },
        )?;
    }
    Ok(())
}

fn push_object(
    namespaces: &mut BTreeMap<String, CatalogNamespace>,
    namespace: &str,
    object: CatalogObject,
) -> Result<(), PostgresPlanError> {
    let entry = namespaces
        .get_mut(namespace)
        .ok_or(PostgresPlanError::InvalidConfig(
            "catalog object refers to an unknown namespace",
        ))?;
    entry.objects.push(object);
    Ok(())
}

fn object_kind_key(kind: &CatalogObjectKind) -> String {
    serde_json::to_string(kind).unwrap_or_else(|_| format!("{kind:?}"))
}

fn catalog_dependencies(namespaces: &BTreeMap<String, CatalogNamespace>) -> Vec<CatalogDependency> {
    let mut dependencies = Vec::new();
    for object in namespaces
        .values()
        .flat_map(|namespace| namespace.objects.iter())
    {
        for (attribute, dependency_type) in [
            ("table_oid", "owned_by_table"),
            ("referenced_table_oid", "references_table"),
            ("partition_parent_oid", "partition_of"),
        ] {
            if let Some(target) = object
                .attributes
                .get(attribute)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
            {
                dependencies.push(CatalogDependency {
                    from_object_id: object.id.clone(),
                    to_object_id: target.to_owned(),
                    dependency_type: dependency_type.into(),
                });
            }
        }
    }
    dependencies.sort_by(|left, right| {
        left.from_object_id
            .cmp(&right.from_object_id)
            .then_with(|| left.to_object_id.cmp(&right.to_object_id))
            .then_with(|| left.dependency_type.cmp(&right.dependency_type))
    });
    dependencies
}

pub fn catalog_fingerprint(catalog: &VendorCatalog) -> Result<String, PostgresPlanError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(catalog)?)))
}

pub fn build_plan(
    source: &CatalogSnapshot,
    target: &CatalogSnapshot,
) -> Result<ReviewedPlan, PostgresPlanError> {
    build_plan_with_consistency(source, target, PostgresConsistencyMode::ConsistentSnapshot)
}

pub fn build_plan_with_consistency(
    source: &CatalogSnapshot,
    target: &CatalogSnapshot,
    consistency_mode: PostgresConsistencyMode,
) -> Result<ReviewedPlan, PostgresPlanError> {
    build_plan_with_consistency_and_outage_policy(source, target, consistency_mode, None)
}

pub fn build_plan_with_consistency_and_outage_policy(
    source: &CatalogSnapshot,
    target: &CatalogSnapshot,
    consistency_mode: PostgresConsistencyMode,
    outage_policy: Option<ReviewedOutagePolicy>,
) -> Result<ReviewedPlan, PostgresPlanError> {
    build_plan_with_consistency_and_contracts(source, target, consistency_mode, outage_policy, None)
}

pub fn build_plan_with_consistency_and_contracts(
    source: &CatalogSnapshot,
    target: &CatalogSnapshot,
    consistency_mode: PostgresConsistencyMode,
    outage_policy: Option<ReviewedOutagePolicy>,
    postgres_source_profile: Option<PostgresSourceProfileContract>,
) -> Result<ReviewedPlan, PostgresPlanError> {
    let mut operations = Vec::new();
    let mut table_names = BTreeSet::new();
    let mut foreign_keys = Vec::new();
    let mut standalone_indexes = Vec::new();
    let mut post_data_indexes = Vec::new();
    let sequences = postgres_sequences(&source.catalog)?;
    let generated_columns = postgres_generated_columns(&source.catalog)?;
    let partition_topologies = postgres_partition_topologies(&source.catalog)?;
    for namespace in &source.catalog.namespaces {
        for object in &namespace.objects {
            if object.kind == CatalogObjectKind::Table {
                table_names.insert(QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: object.name.clone(),
                });
            } else if object.kind == CatalogObjectKind::ForeignKey {
                foreign_keys.push(object.clone());
            } else if object.kind == CatalogObjectKind::Index
                && !catalog_object_targets_partition_leaf(&source.catalog, object)
                && object
                    .attributes
                    .get("constraint_oid")
                    .is_none_or(serde_json::Value::is_null)
                && standalone_unique_index_columns(object, Some(namespace)).is_ok()
            {
                standalone_indexes.push((namespace.name.clone(), object.clone()));
            } else if object.kind == CatalogObjectKind::Index
                && !catalog_object_targets_partition_leaf(&source.catalog, object)
                && object
                    .attributes
                    .get("constraint_oid")
                    .is_none_or(serde_json::Value::is_null)
                && ordinary_index_columns(object, Some(namespace)).is_ok()
            {
                post_data_indexes.push((namespace.name.clone(), object.clone()));
            }
        }
    }
    let mut copy_operations = BTreeMap::new();
    let mut create_table_operations = BTreeMap::new();
    let mut key_unsupported = Vec::new();
    for table in table_names {
        let resumable_key = select_resumable_key(&source.catalog, &table);
        if resumable_key.is_err() {
            let table_object_id = source
                .catalog
                .namespaces
                .iter()
                .find(|namespace| namespace.name == table.namespace)
                .and_then(|namespace| {
                    namespace.objects.iter().find(|object| {
                        object.kind == CatalogObjectKind::Table && object.name == table.name
                    })
                })
                .ok_or(PostgresPlanError::InvalidConfig(
                    "planned table is absent from the source catalog",
                ))?
                .id
                .clone();
            key_unsupported.push(UnsupportedObject {
                code: UnsupportedObjectCode::ResumableKey,
                object_id: table_object_id,
                object_kind: "resumable_key".into(),
                reason: "table has no complete validated non-null primary key, unique constraint, or supported standalone unique index".into(),
                required_semantics: true,
            });
        }
        let topology = partition_topologies
            .iter()
            .find(|topology| topology.root == table);
        let mut parameters = table_parameters(&source.catalog, &table)?;
        if let Some(topology) = topology {
            parameters.insert(
                "postgres_partition_topology".into(),
                serde_json::to_value(topology)?,
            );
        }
        let create = PlanOperation::new(
            topology.map_or(OperationKind::CreateTable, |_| {
                OperationKind::Vendor("create_postgres_partitioned_table".into())
            }),
            Some(table.clone()),
            Vec::new(),
            parameters,
        )?;
        create_table_operations.insert(table.clone(), create.id.clone());
        let mut copy_dependencies = vec![create.id.clone()];
        let mut leaf_creates = Vec::new();
        if let Some(topology) = topology {
            for leaf in &topology.leaves {
                let leaf_create = PlanOperation::new(
                    OperationKind::Vendor("create_postgres_partition".into()),
                    Some(leaf.table.clone()),
                    vec![create.id.clone()],
                    BTreeMap::from([
                        (
                            "root_catalog_object_id".into(),
                            serde_json::json!(topology.root_catalog_object_id),
                        ),
                        (
                            "postgres_partition_leaf".into(),
                            serde_json::to_value(leaf)?,
                        ),
                    ]),
                )?;
                copy_dependencies.push(leaf_create.id.clone());
                leaf_creates.push(leaf_create);
            }
        }
        let mut copy_parameters = BTreeMap::from([(
            "postgres_write_policy".into(),
            serde_json::to_value(postgres_write_policy(&source.catalog, &table)?)?,
        )]);
        if let Ok(key) = resumable_key {
            copy_parameters.insert("resumable_key".into(), serde_json::to_value(key)?);
        }
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            copy_dependencies,
            copy_parameters,
        )?;
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table.clone()),
            vec![copy.id.clone()],
            BTreeMap::new(),
        )?;
        copy_operations.insert(table.clone(), copy.id.clone());
        operations.push(create);
        operations.extend(leaf_creates);
        operations.extend([copy, verify]);
    }
    standalone_indexes.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.name.cmp(&right.1.name))
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    for (_, index) in standalone_indexes {
        let table = qualified_table_for_oid(
            &source.catalog,
            required_catalog_string(&index, "table_oid")?,
        )?;
        let create_table =
            create_table_operations
                .get(&table)
                .ok_or(PostgresPlanError::InvalidConfig(
                    "standalone index table has no create-table operation",
                ))?;
        let create_index = PlanOperation::new(
            OperationKind::CreateIndex,
            Some(table.clone()),
            vec![create_table.clone()],
            BTreeMap::from([("catalog_object".into(), serde_json::to_value(&index)?)]),
        )?;
        let copy_position = operations
            .iter()
            .position(|operation| {
                operation.kind == OperationKind::CopyTable
                    && operation.table.as_ref() == Some(&table)
            })
            .ok_or(PostgresPlanError::InvalidConfig(
                "standalone index table has no copy operation",
            ))?;
        let old_copy = operations[copy_position].clone();
        let mut dependencies = old_copy.dependencies.clone();
        if dependencies.is_empty() {
            dependencies.push(create_table.clone());
        }
        dependencies.push(create_index.id.clone());
        let replacement = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            dependencies,
            old_copy.parameters,
        )?;
        let verify_position = operations
            .iter()
            .position(|operation| {
                operation.kind == OperationKind::VerifyTable
                    && operation.table.as_ref() == Some(&table)
            })
            .ok_or(PostgresPlanError::InvalidConfig(
                "standalone index table has no verify operation",
            ))?;
        let old_verify = operations[verify_position].clone();
        let replacement_verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table.clone()),
            vec![replacement.id.clone()],
            old_verify.parameters,
        )?;
        operations[copy_position] = replacement.clone();
        operations[verify_position] = replacement_verify;
        copy_operations.insert(table, replacement.id);
        operations.push(create_index);
    }
    let mut sequence_creates = Vec::new();
    let mut sequence_create_ids = BTreeMap::<QualifiedTable, Vec<OperationId>>::new();
    for sequence in &sequences {
        let table = sequence
            .ownership
            .as_ref()
            .map(|ownership| ownership.table.clone());
        let create_dependencies = table
            .as_ref()
            .map(|table| {
                create_table_operations
                    .get(table)
                    .cloned()
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "owned sequence table has no create-table operation",
                    ))
            })
            .transpose()?
            .into_iter()
            .collect::<Vec<_>>();
        let create = PlanOperation::new(
            OperationKind::CreateSequence,
            table.clone().or_else(|| {
                Some(QualifiedTable {
                    namespace: sequence.namespace.clone(),
                    name: sequence.name.clone(),
                })
            }),
            create_dependencies,
            BTreeMap::from([("postgres_sequence".into(), serde_json::to_value(sequence)?)]),
        )?;
        if let Some(table) = &table {
            sequence_create_ids
                .entry(table.clone())
                .or_default()
                .push(create.id.clone());
        }
        sequence_creates.push((sequence, create));
    }
    for (table, create_ids) in sequence_create_ids {
        let copy_position = operations
            .iter()
            .position(|operation| {
                operation.kind == OperationKind::CopyTable
                    && operation.table.as_ref() == Some(&table)
            })
            .ok_or(PostgresPlanError::InvalidConfig(
                "owned sequence table has no copy operation",
            ))?;
        let old_copy = operations[copy_position].clone();
        let mut dependencies = old_copy.dependencies.clone();
        dependencies.extend(create_ids);
        let replacement = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            dependencies,
            old_copy.parameters,
        )?;
        let verify_position = operations
            .iter()
            .position(|operation| {
                operation.kind == OperationKind::VerifyTable
                    && operation.table.as_ref() == Some(&table)
            })
            .ok_or(PostgresPlanError::InvalidConfig(
                "owned sequence table has no verify operation",
            ))?;
        let old_verify = operations[verify_position].clone();
        operations[copy_position] = replacement.clone();
        operations[verify_position] = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table.clone()),
            vec![replacement.id.clone()],
            old_verify.parameters,
        )?;
        copy_operations.insert(table.clone(), replacement.id);
    }
    for (sequence, create) in sequence_creates {
        let table = sequence
            .ownership
            .as_ref()
            .map(|ownership| ownership.table.clone());
        let restore_dependency = table
            .as_ref()
            .and_then(|table| copy_operations.get(table))
            .cloned()
            .unwrap_or_else(|| create.id.clone());
        let restore = PlanOperation::new(
            OperationKind::Vendor("restore_postgres_sequence".into()),
            table,
            vec![restore_dependency],
            BTreeMap::from([("postgres_sequence".into(), serde_json::to_value(sequence)?)]),
        )?;
        operations.extend([create, restore]);
    }
    post_data_indexes.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.name.cmp(&right.1.name))
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    for (namespace_name, object) in post_data_indexes {
        let namespace = source
            .catalog
            .namespaces
            .iter()
            .find(|namespace| namespace.name == namespace_name)
            .ok_or(PostgresPlanError::InvalidConfig(
                "ordinary index namespace is absent",
            ))?;
        let index = PostgresIndex {
            catalog_object_id: object.id.clone(),
            name: object.name.clone(),
            table: qualified_table_for_oid(
                &source.catalog,
                required_catalog_string(&object, "table_oid")?,
            )?,
            columns: ordinary_index_columns(&object, Some(namespace))?,
        };
        let copy = copy_operations
            .get(&index.table)
            .ok_or(PostgresPlanError::InvalidConfig(
                "ordinary index table has no copy operation",
            ))?
            .clone();
        operations.push(PlanOperation::new(
            OperationKind::CreateIndex,
            Some(index.table.clone()),
            vec![copy],
            BTreeMap::from([("postgres_index".into(), serde_json::to_value(index)?)]),
        )?);
    }
    foreign_keys.sort_by(|left, right| left.id.cmp(&right.id));
    for foreign_key in foreign_keys {
        let table_oid = required_catalog_string(&foreign_key, "table_oid")?;
        let referenced_table_oid = required_catalog_string(&foreign_key, "referenced_table_oid")?;
        let table = qualified_table_for_oid(&source.catalog, table_oid)?;
        let referenced_table = qualified_table_for_oid(&source.catalog, referenced_table_oid)?;
        let mut dependencies = vec![
            copy_operations
                .get(&table)
                .ok_or(PostgresPlanError::InvalidConfig(
                    "foreign key table has no copy operation",
                ))?
                .clone(),
            copy_operations
                .get(&referenced_table)
                .ok_or(PostgresPlanError::InvalidConfig(
                    "referenced table has no copy operation",
                ))?
                .clone(),
        ];
        dependencies.sort();
        dependencies.dedup();
        let parameters =
            BTreeMap::from([("catalog_object".into(), serde_json::to_value(&foreign_key)?)]);
        let check = PlanOperation::new(
            OperationKind::CheckForeignKey,
            Some(table.clone()),
            dependencies,
            parameters.clone(),
        )?;
        let add = PlanOperation::new(
            OperationKind::AddForeignKey,
            Some(table),
            vec![check.id.clone()],
            parameters,
        )?;
        operations.extend([check, add]);
    }
    let mut programmable_objects = postgres_programmable_objects(&source.catalog)?;
    let programmable_identities = programmable_objects
        .iter()
        .map(|object| object.authoritative_identity.clone())
        .collect::<BTreeSet<_>>();
    let mut programmable_operation_ids: BTreeMap<PostgresProgrammableIdentity, OperationId> =
        BTreeMap::new();
    while !programmable_objects.is_empty() {
        let next = programmable_objects.iter().position(|object| {
            object.authoritative_dependencies.iter().all(|dependency| {
                !programmable_identities.contains(dependency)
                    || programmable_operation_ids.contains_key(dependency)
            })
        });
        let Some(next) = next else {
            return Err(PostgresPlanError::InvalidConfig(
                "programmable dependency graph is cyclic",
            ));
        };
        let programmable = programmable_objects.remove(next);
        let mut dependencies = Vec::new();
        for dependency in &programmable.authoritative_dependencies {
            if let Some(operation_id) = programmable_operation_ids.get(dependency) {
                dependencies.push(operation_id.clone());
            } else if let Some((_, operation_id)) = copy_operations.iter().find(|(table, _)| {
                matches!(dependency,
                    PostgresProgrammableIdentity::Relation { namespace, name }
                    if namespace == &table.namespace && name == &table.name)
            }) {
                dependencies.push(operation_id.clone());
            } else if !programmable_builtin_dependency(dependency, &programmable.namespace) {
                return Err(PostgresPlanError::InvalidConfig(
                    "programmable dependency has no reviewed operation",
                ));
            }
        }
        dependencies.sort();
        dependencies.dedup();
        let kind = match &programmable.ast {
            PostgresDurableAst::View(_) => OperationKind::CreateView,
            PostgresDurableAst::SqlFunction(_) => {
                OperationKind::Vendor("create_postgres_sql_function".into())
            }
        };
        let operation = PlanOperation::new(
            kind,
            Some(QualifiedTable {
                namespace: programmable.namespace.clone(),
                name: programmable.name.clone(),
            }),
            dependencies,
            BTreeMap::from([(
                "postgres_programmable_object".into(),
                serde_json::to_value(&programmable)?,
            )]),
        )?;
        programmable_operation_ids.insert(
            programmable.authoritative_identity.clone(),
            operation.id.clone(),
        );
        operations.push(operation);
    }
    for topology in &partition_topologies {
        let verify = operations
            .iter()
            .find(|operation| {
                operation.kind == OperationKind::VerifyTable
                    && operation.table.as_ref() == Some(&topology.root)
            })
            .ok_or(PostgresPlanError::InvalidConfig(
                "partition root has no final table-verification operation",
            ))?;
        operations.push(PlanOperation::new(
            OperationKind::Vendor("verify_postgres_partition_topology".into()),
            Some(topology.root.clone()),
            vec![verify.id.clone()],
            BTreeMap::from([(
                "postgres_partition_topology".into(),
                serde_json::to_value(topology)?,
            )]),
        )?);
    }
    let verify_schema = PlanOperation::new(
        OperationKind::VerifySchema,
        None,
        operations
            .iter()
            .map(|operation| operation.id.clone())
            .collect(),
        BTreeMap::new(),
    )?;
    operations.push(verify_schema);
    let mut unsupported = source.unsupported.clone();
    unsupported.objects.extend(key_unsupported);
    let unstable_sequences = sequences
        .iter()
        .filter(|sequence| sequence.cache_size != 1)
        .map(|sequence| sequence.catalog_object_id.clone())
        .collect::<Vec<_>>();
    if !unstable_sequences.is_empty() && consistency_mode != PostgresConsistencyMode::WriteFence {
        unsupported.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::SequenceConsistency,
            object_id: "postgres-sequence-write-fence-required".into(),
            object_kind: "sequence_consistency".into(),
            reason: format!(
                "sequence equality requires CACHE 1; these sequences remain write-fence-only: {}",
                unstable_sequences.join(", ")
            ),
            required_semantics: true,
        });
    }
    if !generated_columns.is_empty()
        && source.server_version_num / 10_000 != target.server_version_num / 10_000
    {
        unsupported.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::GeneratedCrossMajor,
            object_id: "postgres-generated-column-major-version".into(),
            object_kind: "generated_column_compatibility".into(),
            reason: "stored generated columns require the same PostgreSQL major version".into(),
            required_semantics: true,
        });
    }
    let target_object_count: usize = target
        .catalog
        .namespaces
        .iter()
        .map(|namespace| namespace.objects.len())
        .sum();
    if target_object_count > 0 {
        unsupported.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::TargetNotEmpty,
            object_id: "target-catalog-not-empty".into(),
            object_kind: "target_precondition".into(),
            reason: format!("target catalog contains {target_object_count} user objects"),
            required_semantics: true,
        });
    }
    if source.endpoint_identity == target.endpoint_identity {
        unsupported.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::SameEndpoint,
            object_id: "source-target-endpoint-collision".into(),
            object_kind: "endpoint_precondition".into(),
            reason: "source and target resolve to the same endpoint identity".into(),
            required_semantics: true,
        });
    }
    unsupported
        .objects
        .sort_by(|left, right| left.object_id.cmp(&right.object_id));
    ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Execution,
        migration_id: format!("pg-{}", &catalog_fingerprint(&source.catalog)?[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity.clone(),
        target_endpoint_identity: AssessmentStatus::Assessed(target.endpoint_identity.clone()),
        source_catalog_fingerprint: catalog_fingerprint(&source.catalog)?,
        target_catalog_fingerprint: AssessmentStatus::Assessed(catalog_fingerprint(
            &target.catalog,
        )?),
        source_catalog: Some(source.catalog.clone()),
        target_catalog: AssessmentStatus::Assessed(target.catalog.clone()),
        source_tls_binding: source.tls_binding.clone(),
        target_tls_binding: AssessmentStatus::Assessed(target.tls_binding.clone()),
        consistency_mode: consistency_mode.as_str().into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "postgresql_same_dialect_exact".into(),
        outage_policy,
        postgres_source_profile,
        mysql_source_profile: None,
        mysql_snapshot_evidence: None,
        mysql_target_snapshot_evidence: None,
        mysql_metadata_visibility: None,
        mysql_target_metadata_visibility: None,
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "repeatable_read_read_only".into(),
            ),
            ("source_tls".into(), source.tls_binding.clone()),
            ("target_tls".into(), target.tls_binding.clone()),
            ("acl.report_only".into(), "approval_required".into()),
        ]),
        operations,
        unsupported_objects: unsupported,
    })
    .map_err(PostgresPlanError::from)
}

fn catalog_object_targets_partition_leaf(catalog: &VendorCatalog, object: &CatalogObject) -> bool {
    let Some(table_oid) = object
        .attributes
        .get("table_oid")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    catalog.namespaces.iter().any(|namespace| {
        namespace.objects.iter().any(|candidate| {
            candidate.kind == CatalogObjectKind::Partition && candidate.id == table_oid
        })
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresResumableKey {
    pub catalog_object_id: String,
    pub kind: String,
    pub columns: Vec<Identifier>,
}

pub(crate) fn select_resumable_key(
    catalog: &VendorCatalog,
    table: &QualifiedTable,
) -> Result<PostgresResumableKey, PostgresPlanError> {
    let Some(namespace) = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == table.namespace)
    else {
        return Err(PostgresPlanError::InvalidConfig(
            "resumable-key table namespace is absent",
        ));
    };
    let Some(table_object) = namespace
        .objects
        .iter()
        .find(|object| object.kind == CatalogObjectKind::Table && object.name == table.name)
    else {
        return Err(PostgresPlanError::InvalidConfig(
            "resumable-key table is absent",
        ));
    };
    let columns = namespace
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
    let mut candidates = namespace
        .objects
        .iter()
        .filter(|object| {
            object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                == Some(table_object.id.as_str())
        })
        .filter_map(|object| {
            let (rank, kind, names) = match object.kind {
                CatalogObjectKind::PrimaryKey | CatalogObjectKind::UniqueConstraint
                    if object
                        .attributes
                        .get("validated")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false) =>
                {
                    let rank = if object.kind == CatalogObjectKind::PrimaryKey {
                        0
                    } else {
                        1
                    };
                    let kind = if rank == 0 {
                        "primary_key"
                    } else {
                        "unique_constraint"
                    };
                    (
                        rank,
                        kind,
                        catalog_identifier_array(object, "columns").ok()?,
                    )
                }
                CatalogObjectKind::Index => (
                    2,
                    "standalone_unique_index",
                    standalone_unique_index_columns(object, Some(namespace)).ok()?,
                ),
                _ => return None,
            };
            if names.is_empty()
                || names.iter().any(|name| {
                    !columns.iter().any(|column| {
                        column.name == *name
                            && !column
                                .attributes
                                .get("nullable")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(true)
                            && column
                                .attributes
                                .get("generated")
                                .and_then(serde_json::Value::as_str)
                                .is_none_or(str::is_empty)
                    })
                })
            {
                return None;
            }
            Some((
                rank,
                names.len(),
                names.clone(),
                object.name.clone(),
                object.id.as_str(),
                kind,
                names,
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, &left.3, &left.4)
            .cmp(&(&right.0, &right.1, &right.2, &right.3, &right.4))
    });
    let (_, _, _, _, object_id, kind, columns) =
        candidates
            .into_iter()
            .next()
            .ok_or(PostgresPlanError::InvalidConfig(
                "table has no safe resumable key",
            ))?;
    Ok(PostgresResumableKey {
        catalog_object_id: object_id.into(),
        kind: kind.into(),
        columns,
    })
}

fn catalog_identifier_array(
    object: &CatalogObject,
    attribute: &'static str,
) -> Result<Vec<Identifier>, PostgresPlanError> {
    object
        .attributes
        .get(attribute)
        .and_then(serde_json::Value::as_array)
        .ok_or(PostgresPlanError::InvalidConfig(attribute))?
        .iter()
        .map(|value| {
            Identifier::new(
                value
                    .as_str()
                    .ok_or(PostgresPlanError::InvalidConfig(attribute))?,
            )
            .map_err(PostgresPlanError::from)
        })
        .collect()
}

fn standalone_unique_index_columns(
    object: &CatalogObject,
    namespace: Option<&CatalogNamespace>,
) -> Result<Vec<Identifier>, PostgresPlanError> {
    plain_standalone_btree_index_columns(object, namespace, true, true)
}

fn ordinary_index_columns(
    object: &CatalogObject,
    namespace: Option<&CatalogNamespace>,
) -> Result<Vec<Identifier>, PostgresPlanError> {
    plain_standalone_btree_index_columns(object, namespace, false, false)
}

fn plain_standalone_btree_index_columns(
    object: &CatalogObject,
    namespace: Option<&CatalogNamespace>,
    unique: bool,
    require_non_null: bool,
) -> Result<Vec<Identifier>, PostgresPlanError> {
    for (attribute, expected) in [
        ("unique", unique),
        ("primary", false),
        ("valid", true),
        ("ready", true),
        ("live", true),
        ("immediate", true),
        ("clustered", false),
        ("replica_identity", false),
        ("exclusion", false),
        ("nulls_not_distinct", false),
        ("has_expressions", false),
    ] {
        if object
            .attributes
            .get(attribute)
            .and_then(serde_json::Value::as_bool)
            != Some(expected)
        {
            return Err(PostgresPlanError::InvalidConfig(attribute));
        }
    }
    if object
        .attributes
        .get("access_method")
        .and_then(serde_json::Value::as_str)
        != Some("btree")
        || object
            .attributes
            .get("predicate")
            .is_some_and(|value| !value.is_null())
        || object
            .attributes
            .get("constraint_oid")
            .is_some_and(|value| !value.is_null())
        || object
            .attributes
            .get("persistence")
            .and_then(serde_json::Value::as_str)
            != Some("p")
        || object
            .attributes
            .get("reloptions")
            .is_some_and(|value| !value.is_null())
        || object
            .attributes
            .get("tablespace")
            .is_some_and(|value| !value.is_null())
    {
        return Err(PostgresPlanError::InvalidConfig("standalone index shape"));
    }
    let columns = catalog_identifier_array(object, "columns")?;
    if columns.is_empty()
        || object
            .attributes
            .get("key_attribute_count")
            .and_then(serde_json::Value::as_u64)
            != u64::try_from(columns.len()).ok()
        || object
            .attributes
            .get("attribute_count")
            .and_then(serde_json::Value::as_u64)
            != u64::try_from(columns.len()).ok()
        || !object
            .attributes
            .get("included_columns")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
        || !object
            .attributes
            .get("options")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| {
                values.len() == columns.len() && values.iter().all(|v| v.as_i64() == Some(0))
            })
        || !object
            .attributes
            .get("collations_default")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| {
                values.len() == columns.len()
                    && values.iter().all(|value| value.as_bool() == Some(true))
            })
        || !object
            .attributes
            .get("opclasses")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| {
                values.len() == columns.len()
                    && values.iter().all(|value| {
                        value.get("default").and_then(serde_json::Value::as_bool) == Some(true)
                    })
            })
    {
        return Err(PostgresPlanError::InvalidConfig(
            "standalone index is not a plain default-btree index",
        ));
    }
    if let Some(namespace) = namespace {
        let table_oid = required_catalog_string(object, "table_oid")?;
        if columns.iter().any(|name| {
            !namespace.objects.iter().any(|column| {
                column.kind == CatalogObjectKind::Column
                    && column.name == *name
                    && column
                        .attributes
                        .get("table_oid")
                        .and_then(serde_json::Value::as_str)
                        == Some(table_oid)
                    && (!require_non_null
                        || !column
                            .attributes
                            .get("nullable")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true))
            })
        }) {
            return Err(PostgresPlanError::InvalidConfig(
                "standalone index contains an unknown column or a disallowed nullable column",
            ));
        }
    }
    Ok(columns)
}

fn required_catalog_string<'a>(
    object: &'a CatalogObject,
    attribute: &'static str,
) -> Result<&'a str, PostgresPlanError> {
    object
        .attributes
        .get(attribute)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(PostgresPlanError::InvalidConfig(attribute))
}

fn qualified_table_for_oid(
    catalog: &VendorCatalog,
    table_oid: &str,
) -> Result<QualifiedTable, PostgresPlanError> {
    catalog
        .namespaces
        .iter()
        .find_map(|namespace| {
            namespace
                .objects
                .iter()
                .find(|object| object.kind == CatalogObjectKind::Table && object.id == table_oid)
                .map(|table| QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: table.name.clone(),
                })
        })
        .ok_or(PostgresPlanError::InvalidConfig(
            "foreign key refers to an unknown table OID",
        ))
}

pub fn postgres_write_policy(
    catalog: &VendorCatalog,
    table: &QualifiedTable,
) -> Result<PostgresWritePolicy, PostgresPlanError> {
    let namespace = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == table.namespace)
        .ok_or(PostgresPlanError::InvalidConfig(
            "write-policy table namespace is absent from the catalog",
        ))?;
    let table_object = namespace
        .objects
        .iter()
        .find(|object| object.kind == CatalogObjectKind::Table && object.name == table.name)
        .ok_or(PostgresPlanError::InvalidConfig(
            "write-policy table is absent from the catalog",
        ))?;
    let mut has_columns = false;
    let mut has_identity_always = false;
    for column in namespace.objects.iter().filter(|object| {
        object.kind == CatalogObjectKind::Column
            && object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                == Some(table_object.id.as_str())
    }) {
        has_columns = true;
        match column
            .attributes
            .get("identity")
            .and_then(serde_json::Value::as_str)
            .ok_or(PostgresPlanError::InvalidConfig(
                "write-policy column omits identity metadata",
            ))? {
            "" | "d" => {}
            "a" => has_identity_always = true,
            _ => {
                return Err(PostgresPlanError::InvalidConfig(
                    "write-policy column has unknown identity metadata",
                ));
            }
        }
    }
    if !has_columns {
        return Err(PostgresPlanError::InvalidConfig(
            "write-policy table has no catalog columns",
        ));
    }
    Ok(if has_identity_always {
        PostgresWritePolicy::PlainInsertIdentityAlwaysV1
    } else {
        PostgresWritePolicy::BinaryCopyWithInsertFallbackV1
    })
}

fn table_parameters(
    catalog: &VendorCatalog,
    table: &QualifiedTable,
) -> Result<BTreeMap<String, serde_json::Value>, PostgresPlanError> {
    let namespace = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == table.namespace)
        .ok_or(PostgresPlanError::InvalidConfig(
            "planned table namespace is absent from the catalog",
        ))?;
    let table_object = namespace
        .objects
        .iter()
        .find(|object| object.kind == CatalogObjectKind::Table && object.name == table.name)
        .ok_or(PostgresPlanError::InvalidConfig(
            "planned table is absent from the catalog",
        ))?;
    let owned_objects: Vec<_> = namespace
        .objects
        .iter()
        .filter(|object| {
            object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                == Some(table_object.id.as_str())
        })
        .collect();
    Ok(BTreeMap::from([
        ("table".into(), serde_json::to_value(table_object)?),
        ("owned_objects".into(), serde_json::to_value(owned_objects)?),
    ]))
}

pub fn write_live_plan(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<ReviewedPlan, PostgresPlanError> {
    write_live_plan_with_consistency(
        source_config,
        target_config,
        output,
        PostgresConsistencyMode::ConsistentSnapshot,
    )
}

/// Collect a source-only assessment in exactly one read-only repeatable-read transaction.
pub fn collect_live_assessment(
    source_config: &PostgresEndpointConfig,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    collect_live_assessment_with_profile(source_config, None)
}

pub fn collect_live_assessment_with_profile(
    source_config: &PostgresEndpointConfig,
    throughput_profile: Option<&ThroughputProfile>,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    let mut client =
        source_config.connect_with_application_name("sql-splitter-migration-assessment")?;
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()?;
    let identity = transaction.query_one(
        "SELECT current_database(),current_user,COALESCE(inet_server_addr()::text,'local'),COALESCE(inet_server_port(),0),current_setting('server_version'),current_setting('server_version_num')::integer,current_setting('transaction_read_only')::boolean",
        &[],
    )?;
    let database: String = identity.get(0);
    let user: String = identity.get(1);
    let address: String = identity.get(2);
    let port: i32 = identity.get(3);
    let server_version: String = identity.get(4);
    let server_version_num: i32 = identity.get(5);
    let transaction_read_only: bool = identity.get(6);
    let direct_write_privileges_absent: bool = transaction
        .query_one(
            "SELECT NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls AND NOT has_database_privilege(current_user,current_database(),'CREATE,TEMP') AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_' AND has_schema_privilege(current_user,n.oid,'CREATE')) AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_' AND c.relkind IN ('r','p','f','v') AND has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_' AND c.relkind='S' AND has_sequence_privilege(current_user,c.oid,'USAGE,UPDATE')) AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_' AND p.prosecdef AND has_function_privilege(current_user,p.oid,'EXECUTE')) FROM pg_roles r WHERE r.rolname=current_user",
            &[],
        )?
        .get(0);
    validate_assessment_source_security(transaction_read_only, direct_write_privileges_absent)?;
    let (catalog, unsupported_objects) =
        extract_catalog(&mut transaction, &database, &server_version)?;
    let estimate_rows = transaction.query(
        "SELECT n.nspname,c.relname,GREATEST(c.reltuples::bigint,0),pg_relation_size(c.oid),pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>'information_schema' AND n.nspname!~'^pg_' AND c.relkind IN ('r','p') ORDER BY n.nspname,c.relname,c.oid",
        &[],
    )?;
    let mut scope_estimates = estimate_rows
        .into_iter()
        .map(|row| {
            Ok(ScopeEstimate {
                table: QualifiedTable {
                    namespace: Identifier::new(row.get::<_, String>(0))?,
                    name: Identifier::new(row.get::<_, String>(1))?,
                },
                estimated_rows: row.get(2),
                relation_bytes: u64::try_from(row.get::<_, i64>(3)).map_err(|_| {
                    PostgresPlanError::InvalidConfig("negative PostgreSQL relation size")
                })?,
                total_relation_bytes: u64::try_from(row.get::<_, i64>(4)).map_err(|_| {
                    PostgresPlanError::InvalidConfig("negative PostgreSQL total relation size")
                })?,
            })
        })
        .collect::<Result<Vec<_>, PostgresPlanError>>()?;
    scope_estimates.sort_by(|left, right| left.table.cmp(&right.table));
    transaction.commit()?;

    let assessed_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresPlanError::InvalidConfig("system clock is before the Unix epoch"))?
        .as_secs();
    build_source_assessment_with_profile(
        CatalogSnapshot {
            endpoint_identity: format!("postgres://{address}:{port}/{database}?user={user}"),
            server_version,
            server_version_num,
            catalog,
            unsupported: unsupported_objects,
            tls_binding: postgres_tls_binding(source_config)?,
        },
        transaction_read_only,
        direct_write_privileges_absent,
        scope_estimates,
        throughput_profile,
        assessed_at_unix_seconds,
    )
}

fn validate_assessment_source_security(
    transaction_read_only: bool,
    direct_write_privileges_absent: bool,
) -> Result<(), PostgresPlanError> {
    if !transaction_read_only {
        return Err(PostgresPlanError::SourceNotReadOnly);
    }
    if !direct_write_privileges_absent {
        return Err(PostgresPlanError::SourceRoleHoldsDirectWritePrivilege);
    }
    Ok(())
}

fn assessment_projection_source(source: &CatalogSnapshot) -> CatalogSnapshot {
    let mut blocked = source
        .unsupported
        .objects
        .iter()
        .filter(|finding| finding.required_semantics)
        .map(|finding| finding.object_id.as_str())
        .collect::<BTreeSet<_>>();
    loop {
        let previous_len = blocked.len();
        for dependency in &source.catalog.dependencies {
            if blocked.contains(dependency.to_object_id.as_str()) {
                blocked.insert(dependency.from_object_id.as_str());
            }
            if dependency.dependency_type == "partition_of"
                && blocked.contains(dependency.from_object_id.as_str())
            {
                blocked.insert(dependency.to_object_id.as_str());
            }
        }
        if blocked.len() == previous_len {
            break;
        }
    }
    let mut projected = source.clone();
    for namespace in &mut projected.catalog.namespaces {
        namespace
            .objects
            .retain(|object| !blocked.contains(object.id.as_str()));
    }
    let retained = projected
        .catalog
        .namespaces
        .iter()
        .flat_map(|namespace| &namespace.objects)
        .map(|object| object.id.as_str())
        .collect::<BTreeSet<_>>();
    projected.catalog.dependencies.retain(|dependency| {
        retained.contains(dependency.from_object_id.as_str())
            && retained.contains(dependency.to_object_id.as_str())
    });
    projected.unsupported = UnsupportedObjectReport::default();
    projected
}

/// Build a durable source-only assessment from evidence collected in one snapshot.
pub fn build_source_assessment(
    source: CatalogSnapshot,
    transaction_read_only: bool,
    direct_write_privileges_absent: bool,
    scope_estimates: Vec<ScopeEstimate>,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    build_source_assessment_with_profile(
        source,
        transaction_read_only,
        direct_write_privileges_absent,
        scope_estimates,
        None,
        0,
    )
}

pub fn build_source_assessment_with_profile(
    source: CatalogSnapshot,
    transaction_read_only: bool,
    direct_write_privileges_absent: bool,
    mut scope_estimates: Vec<ScopeEstimate>,
    throughput_profile: Option<&ThroughputProfile>,
    assessed_at_unix_seconds: u64,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    let sequence_objects = source
        .catalog
        .namespaces
        .iter()
        .flat_map(|namespace| &namespace.objects)
        .filter(|object| object.kind == CatalogObjectKind::Sequence)
        .collect::<Vec<_>>();
    let has_sequences = !sequence_objects.is_empty();
    let sequence_cache_requires_fence = sequence_objects.iter().any(|sequence| {
        sequence
            .attributes
            .get("cache")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<i64>().ok())
            .is_none_or(|cache| cache > 1)
    });
    let projection_source = assessment_projection_source(&source);
    let projection_target = CatalogSnapshot {
        endpoint_identity: "assessment://target-not-assessed".into(),
        server_version: source.server_version.clone(),
        server_version_num: source.server_version_num,
        catalog: VendorCatalog {
            format_version: source.catalog.format_version,
            dialect: source.catalog.dialect.clone(),
            server_version: source.catalog.server_version.clone(),
            database: source.catalog.database.clone(),
            namespaces: Vec::new(),
            dependencies: Vec::new(),
            vendor_metadata: source.catalog.vendor_metadata.clone(),
        },
        unsupported: UnsupportedObjectReport::default(),
        tls_binding: "not_assessed".into(),
    };
    let projected = build_plan_with_consistency(
        &projection_source,
        &projection_target,
        PostgresConsistencyMode::WriteFence,
    )?;
    let mut unsupported_objects = source.unsupported.clone();
    unsupported_objects.objects.extend(
        projected
            .plan
            .unsupported_objects
            .objects
            .into_iter()
            .filter(|finding| {
                !matches!(
                    finding.object_kind.as_str(),
                    "target_precondition" | "endpoint_precondition"
                )
            }),
    );
    unsupported_objects
        .objects
        .sort_by(|left, right| left.object_id.cmp(&right.object_id));
    scope_estimates.sort_by(|left, right| left.table.cmp(&right.table));
    let source_catalog_fingerprint = catalog_fingerprint(&source.catalog)?;
    let projected_physical_tables =
        copied_physical_relations(&projection_source.catalog, &projected.plan.operations)?
            .into_iter()
            .map(|(namespace, relation)| QualifiedTable {
                namespace: namespace.name.clone(),
                name: relation.name.clone(),
            })
            .collect::<BTreeSet<_>>();
    let projection_scope_estimates = scope_estimates
        .iter()
        .filter(|estimate| projected_physical_tables.contains(&estimate.table))
        .cloned()
        .collect::<Vec<_>>();
    let operations = projected.plan.operations;
    let reviewed_plan = ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Assessment,
        migration_id: format!("pg-assessment-{}", &source_catalog_fingerprint[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity,
        target_endpoint_identity: AssessmentStatus::NotAssessed,
        source_catalog_fingerprint: source_catalog_fingerprint.clone(),
        target_catalog_fingerprint: AssessmentStatus::NotAssessed,
        source_catalog: Some(source.catalog),
        target_catalog: AssessmentStatus::NotAssessed,
        source_tls_binding: source.tls_binding.clone(),
        target_tls_binding: AssessmentStatus::NotAssessed,
        consistency_mode: "not_assessed".into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "postgresql_source_only_assessment".into(),
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: None,
        mysql_snapshot_evidence: None,
        mysql_target_snapshot_evidence: None,
        mysql_metadata_visibility: None,
        mysql_target_metadata_visibility: None,
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "repeatable_read_read_only".into(),
            ),
            ("source_tls".into(), source.tls_binding.clone()),
            ("acl.report_only".into(), "approval_required".into()),
        ]),
        operations,
        unsupported_objects,
    })?;
    let consistent_snapshot_status = if !has_sequences {
        EvidenceStatus::Proven
    } else {
        EvidenceStatus::NotAssessed {
            reason: if sequence_cache_requires_fence {
                "one or more sequences use CACHE greater than 1 and require a write fence".into()
            } else {
                "CACHE 1 sequence equality is planned but its execution gates have not run".into()
            },
        }
    };
    let assessment = AssessmentArtifact {
        schema_version: ASSESSMENT_SCHEMA_VERSION,
        reviewed_plan,
        source_evidence: SourceAssessmentEvidence {
            server_version: source.server_version,
            server_version_num: u32::try_from(source.server_version_num).map_err(|_| {
                PostgresPlanError::InvalidConfig("negative PostgreSQL server version number")
            })?,
            tls_binding: source.tls_binding,
            transaction_read_only,
            direct_write_privileges_absent,
        },
        execution_requirements: vec![
            ExecutionRequirement {
                name: "consistent_snapshot".into(),
                status: consistent_snapshot_status,
                detail: "table data uses one REPEATABLE READ READ ONLY snapshot; sequence state requires separate evidence".into(),
            },
            ExecutionRequirement {
                name: "write_fence".into(),
                status: EvidenceStatus::NotAssessed {
                    reason: "source-only assessment does not install or probe a write fence".into(),
                },
                detail: "required for sequence-bearing online execution".into(),
            },
            ExecutionRequirement {
                name: "attested_external_quiesce".into(),
                status: EvidenceStatus::NotAssessed {
                    reason: "operator attestation is supplied only at execution time".into(),
                },
                detail: "the tool does not enforce the external source freeze".into(),
            },
            ExecutionRequirement {
                name: "target_compatibility".into(),
                status: EvidenceStatus::NotAssessed {
                    reason: "no target endpoint was provided".into(),
                },
                detail: "target version, ownership, emptiness, and schema compatibility".into(),
            },
        ],
        projected_window: project_outage_window(
            &projection_scope_estimates,
            throughput_profile,
            u16::try_from(source.server_version_num / 10_000).unwrap_or_default(),
            assessed_at_unix_seconds,
        ),
        scope_estimates,
    };
    assessment.validate()?;
    Ok(assessment)
}

/// Collect and publish protected machine-readable and Markdown assessment artifacts.
pub fn write_live_assessment(
    source_config: impl AsRef<Path>,
    assessment_output: impl AsRef<Path>,
    report_output: impl AsRef<Path>,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    write_live_assessment_with_profile(
        source_config,
        assessment_output,
        report_output,
        None::<&Path>,
    )
}

pub fn write_live_assessment_with_profile(
    source_config: impl AsRef<Path>,
    assessment_output: impl AsRef<Path>,
    report_output: impl AsRef<Path>,
    throughput_profile: Option<impl AsRef<Path>>,
) -> Result<AssessmentArtifact, PostgresPlanError> {
    let assessment_output = assessment_output.as_ref();
    let report_output = report_output.as_ref();
    if assessment_output == report_output {
        return Err(PostgresPlanError::InvalidConfig(
            "assessment JSON and Markdown outputs must be different paths",
        ));
    }
    prepare_json_text_pair_new(assessment_output, report_output)?;
    let source_config = PostgresEndpointConfig::read(source_config)?;
    let throughput_profile = throughput_profile
        .map(read_throughput_profile)
        .transpose()?;
    let assessment =
        collect_live_assessment_with_profile(&source_config, throughput_profile.as_ref())?;
    let report = render_markdown(&assessment)?;
    write_json_text_pair_new(assessment_output, &assessment, report_output, &report)?;
    Ok(assessment)
}

fn relation_oid(object_id: &str) -> Result<u32, PostgresPlanError> {
    object_id
        .strip_prefix("relation:")
        .and_then(|value| value.parse().ok())
        .ok_or(PostgresPlanError::InvalidConfig(
            "catalog relation has an invalid object ID",
        ))
}

fn copied_physical_relations<'a>(
    catalog: &'a VendorCatalog,
    operations: &[PlanOperation],
) -> Result<Vec<(&'a CatalogNamespace, &'a CatalogObject)>, PostgresPlanError> {
    let mut relations = BTreeMap::new();
    for operation in operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
    {
        let table = operation
            .table
            .as_ref()
            .ok_or(PostgresPlanError::InvalidConfig(
                "CopyTable operation has no table",
            ))?;
        let namespace = catalog
            .namespaces
            .iter()
            .find(|namespace| namespace.name == table.namespace)
            .ok_or(PostgresPlanError::InvalidConfig(
                "copied table namespace is absent from the source catalog",
            ))?;
        let root = namespace
            .objects
            .iter()
            .find(|object| object.kind == CatalogObjectKind::Table && object.name == table.name)
            .ok_or(PostgresPlanError::InvalidConfig(
                "copied table is absent from the source catalog",
            ))?;
        match required_catalog_string(root, "relkind")? {
            "r" => {
                relations.insert(root.id.as_str(), (namespace, root));
            }
            "p" => {
                let mut leaf_count = 0_usize;
                for leaf_namespace in &catalog.namespaces {
                    for leaf in &leaf_namespace.objects {
                        if leaf.kind == CatalogObjectKind::Partition
                            && leaf
                                .attributes
                                .get("partition_parent_oid")
                                .and_then(serde_json::Value::as_str)
                                == Some(root.id.as_str())
                        {
                            if required_catalog_string(leaf, "relkind")? != "r" {
                                return Err(PostgresPlanError::InvalidConfig(
                                    "copied partition leaf is not a physical ordinary relation",
                                ));
                            }
                            relations.insert(leaf.id.as_str(), (leaf_namespace, leaf));
                            leaf_count += 1;
                        }
                    }
                }
                if leaf_count == 0 {
                    return Err(PostgresPlanError::InvalidConfig(
                        "copied partition root has no physical leaf inventory",
                    ));
                }
            }
            _ => {
                return Err(PostgresPlanError::InvalidConfig(
                    "CopyTable coverage contains a non-physical source relation",
                ));
            }
        }
    }
    Ok(relations.into_values().collect())
}

fn physical_business_relation_oids(catalog: &VendorCatalog) -> Result<Vec<u32>, PostgresPlanError> {
    let mut oids = BTreeSet::new();
    for object in catalog
        .namespaces
        .iter()
        .flat_map(|namespace| &namespace.objects)
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::Table | CatalogObjectKind::Partition
            )
        })
    {
        match required_catalog_string(object, "relkind")? {
            "r" => {
                oids.insert(relation_oid(&object.id)?);
            }
            "p" if object.kind == CatalogObjectKind::Table => {}
            _ => {
                return Err(PostgresPlanError::InvalidConfig(
                    "business relation catalog contains an unexpected physical kind",
                ));
            }
        }
    }
    Ok(oids.into_iter().collect())
}

fn query_exact_total_relation_bytes(
    client: &mut impl postgres::GenericClient,
    expected_oids: &[u32],
) -> Result<u64, PostgresPlanError> {
    let rows = client.query(
        "SELECT c.oid, pg_total_relation_size(c.oid) FROM pg_class c WHERE c.relkind='r' AND c.oid=ANY($1::oid[]) ORDER BY c.oid",
        &[&expected_oids],
    )?;
    let returned_oids = rows
        .iter()
        .map(|row| row.get::<_, u32>(0))
        .collect::<Vec<_>>();
    let mut sorted_expected = expected_oids.to_vec();
    sorted_expected.sort_unstable();
    if returned_oids != sorted_expected {
        return Err(PostgresPlanError::InvalidConfig(
            "copied physical relation inventory does not match pg_class",
        ));
    }
    rows.into_iter().try_fold(0_u64, |total, row| {
        let bytes = u64::try_from(row.get::<_, i64>(1)).map_err(|_| {
            PostgresPlanError::InvalidConfig("negative PostgreSQL total relation size")
        })?;
        total
            .checked_add(bytes)
            .ok_or(PostgresPlanError::InvalidConfig(
                "PostgreSQL total relation size overflowed",
            ))
    })
}

/// Run the explicit, side-effecting PostgreSQL administrator probe suite and
/// publish its protected evidence artifact.
pub fn probe_live_postgres_source_profile(
    source_config: impl AsRef<Path>,
    admin_config: impl AsRef<Path>,
    profile: PostgresSourceProfileKind,
    output: impl AsRef<Path>,
) -> Result<PostgresSourceProbeArtifact, PostgresPlanError> {
    if !profile.requires_privilege_probe() {
        return Err(PostgresPlanError::InvalidConfig(
            "external-quiesce profiles do not use administrator probe evidence",
        ));
    }
    let source_config = PostgresEndpointConfig::read(source_config)?;
    let admin_config = PostgresEndpointConfig::read(admin_config)?;
    if source_config.credential_env == admin_config.credential_env
        || source_config.user == admin_config.user
    {
        return Err(PostgresPlanError::InvalidConfig(
            "source-profile probing requires distinct source and administrator roles",
        ));
    }
    if admin_config.tls.insecure {
        return Err(PostgresPlanError::InvalidConfig(
            "source-profile administrator TLS must authenticate the server",
        ));
    }

    let source = inspect_endpoint(&source_config)?;
    let source_catalog_fingerprint = catalog_fingerprint(&source.catalog)?;
    let mut admin =
        admin_config.connect_with_application_name("sql-splitter-source-profile-probe")?;
    let identity = admin.query_one(
        "SELECT current_database(),current_user,COALESCE(inet_server_addr()::text,'local'),COALESCE(inet_server_port(),0)",
        &[],
    )?;
    let admin_database: String = identity.get(0);
    let administrator_role: String = identity.get(1);
    let admin_address: String = identity.get(2);
    let admin_port: i32 = identity.get(3);
    let admin_endpoint = format!("postgres://{admin_address}:{admin_port}/{admin_database}");
    let source_endpoint = source
        .endpoint_identity
        .split_once("?user=")
        .map(|(endpoint, _)| endpoint)
        .ok_or(PostgresPlanError::InvalidConfig(
            "source endpoint identity is malformed",
        ))?;
    if admin_endpoint != source_endpoint {
        return Err(PostgresPlanError::InvalidConfig(
            "source and administrator configurations resolve to different databases",
        ));
    }

    let relations = probe_relation_manifest(&source.catalog)?;
    let relation_oids = relations.iter().map(|(_, oid)| *oid).collect::<Vec<_>>();
    let relation_ids = relations
        .iter()
        .map(|(object_id, _)| object_id.clone())
        .collect::<Vec<_>>();
    let sequences = postgres_sequences(&source.catalog)?;
    let sequence_oids = sequences
        .iter()
        .map(|sequence| catalog_oid(&sequence.catalog_object_id, "relation:"))
        .collect::<Result<Vec<_>, _>>()?;
    let mut sequence_ids = sequences
        .iter()
        .map(|sequence| sequence.catalog_object_id.clone())
        .collect::<Vec<_>>();
    sequence_ids.sort();

    let lock_failures = query_failed_object_ids(
        &mut admin,
        "SELECT c.oid, r.rolsuper OR pg_has_role(current_user,c.relowner,'MEMBER') OR has_table_privilege(current_user,c.oid,'UPDATE') OR has_table_privilege(current_user,c.oid,'DELETE') OR has_table_privilege(current_user,c.oid,'TRUNCATE') FROM pg_class c CROSS JOIN pg_roles r WHERE r.rolname=current_user AND c.oid=ANY($1::oid[]) ORDER BY c.oid",
        &relation_oids,
        &relations,
    )?;
    let trigger_failures = query_failed_object_ids(
        &mut admin,
        "SELECT c.oid, has_table_privilege(current_user,c.oid,'TRIGGER') AND (r.rolsuper OR pg_has_role(current_user,c.relowner,'MEMBER')) FROM pg_class c CROSS JOIN pg_roles r WHERE r.rolname=current_user AND c.oid=ANY($1::oid[]) ORDER BY c.oid",
        &relation_oids,
        &relations,
    )?;
    let sequence_failures = query_sequence_probe_failures(&mut admin, &sequence_oids, &sequences)?;

    let mut results = vec![
        catalog_probe_result(
            PostgresSourceProbeRequirement::PlannedTableLockPrivileges,
            relation_ids.clone(),
            lock_failures,
            "catalog privileges required for ACCESS EXCLUSIVE locks were checked",
        ),
        catalog_probe_result(
            PostgresSourceProbeRequirement::PlannedTableTriggerPrivileges,
            relation_ids,
            trigger_failures,
            "TRIGGER privilege and ownership authority required for ENABLE ALWAYS were checked",
        ),
        catalog_probe_result(
            PostgresSourceProbeRequirement::SequenceOwnershipAndPrivileges,
            sequence_ids,
            sequence_failures,
            "sequence ownership-transfer, schema, table-owner, and ACL authority were checked",
        ),
        transactional_event_trigger_probe(&mut admin)?,
        transactional_registry_probe(&mut admin)?,
        sacrificial_backend_probe(&mut admin, &source_config, &administrator_role)?,
    ];
    results.sort_by_key(|result| result.requirement);
    let artifact = PostgresSourceProbeArtifact {
        schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
        profile,
        source_endpoint_identity: source.endpoint_identity,
        source_catalog_fingerprint,
        administrator_role,
        probed_at_unix_seconds: unix_seconds_now()?,
        results,
    };
    artifact.validate()?;
    write_json_new(output, &artifact)?;
    Ok(artifact)
}

fn probe_relation_manifest(
    catalog: &VendorCatalog,
) -> Result<Vec<(String, u32)>, PostgresPlanError> {
    let mut relations = catalog
        .namespaces
        .iter()
        .flat_map(|namespace| namespace.objects.iter())
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::Table | CatalogObjectKind::Partition
            )
        })
        .map(|object| Ok((object.id.clone(), catalog_oid(&object.id, "relation:")?)))
        .collect::<Result<Vec<_>, PostgresPlanError>>()?;
    relations.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(relations)
}

fn catalog_oid(object_id: &str, prefix: &'static str) -> Result<u32, PostgresPlanError> {
    object_id
        .strip_prefix(prefix)
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(PostgresPlanError::InvalidConfig(
            "catalog object identity does not contain a valid PostgreSQL OID",
        ))
}

fn query_failed_object_ids(
    admin: &mut Client,
    query: &str,
    expected_oids: &[u32],
    manifest: &[(String, u32)],
) -> Result<Vec<String>, PostgresPlanError> {
    let rows = admin.query(query, &[&expected_oids])?;
    query_failed_rows(rows, expected_oids, manifest)
}

fn query_sequence_probe_failures(
    admin: &mut Client,
    sequence_oids: &[u32],
    sequences: &[PostgresSequence],
) -> Result<Vec<String>, PostgresPlanError> {
    let rows = admin.query(
        "SELECT s.oid, r.rolsuper OR (pg_has_role(current_user,s.relowner,'MEMBER') AND has_schema_privilege(current_user,n.oid,'CREATE') AND NOT EXISTS (SELECT 1 FROM aclexplode(coalesce(s.relacl,acldefault('S'::\"char\",s.relowner))) a WHERE a.grantor<>s.relowner OR a.privilege_type NOT IN ('SELECT','USAGE','UPDATE')) AND NOT EXISTS (SELECT 1 FROM pg_depend d JOIN pg_class t ON t.oid=d.refobjid JOIN pg_namespace tn ON tn.oid=t.relnamespace WHERE d.classid='pg_class'::regclass AND d.objid=s.oid AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') AND (NOT pg_has_role(current_user,t.relowner,'MEMBER') OR NOT has_schema_privilege(current_user,tn.oid,'CREATE')))) FROM pg_class s JOIN pg_namespace n ON n.oid=s.relnamespace CROSS JOIN pg_roles r WHERE r.rolname=current_user AND s.relkind='S' AND s.oid=ANY($1::oid[]) ORDER BY s.oid",
        &[&sequence_oids],
    )?;
    let manifest = sequences
        .iter()
        .zip(sequence_oids)
        .map(|(sequence, oid)| (sequence.catalog_object_id.clone(), *oid))
        .collect::<Vec<_>>();
    query_failed_rows(rows, sequence_oids, &manifest)
}

fn query_failed_rows(
    rows: Vec<postgres::Row>,
    expected_oids: &[u32],
    manifest: &[(String, u32)],
) -> Result<Vec<String>, PostgresPlanError> {
    let observed = rows
        .iter()
        .map(|row| row.get::<_, u32>(0))
        .collect::<Vec<_>>();
    let mut expected = expected_oids.to_vec();
    expected.sort_unstable();
    if observed != expected {
        return Err(PostgresPlanError::InvalidConfig(
            "probe object inventory differs from the reviewed catalog",
        ));
    }
    Ok(rows
        .into_iter()
        .filter(|row| !row.get::<_, bool>(1))
        .filter_map(|row| {
            let oid = row.get::<_, u32>(0);
            manifest
                .iter()
                .find(|(_, candidate)| *candidate == oid)
                .map(|(object_id, _)| object_id.clone())
        })
        .collect())
}

fn catalog_probe_result(
    requirement: PostgresSourceProbeRequirement,
    object_ids: Vec<String>,
    failed_object_ids: Vec<String>,
    detail: &str,
) -> PostgresSourceProbeResult {
    let status = if failed_object_ids.is_empty() {
        PostgresSourceProbeStatus::Proven
    } else {
        PostgresSourceProbeStatus::Unavailable {
            reason: format!(
                "administrator lacks required authority for {} object(s): {}",
                failed_object_ids.len(),
                failed_object_ids.join(", ")
            ),
        }
    };
    PostgresSourceProbeResult {
        requirement,
        status,
        object_ids,
        detail: detail.into(),
    }
}

fn transactional_event_trigger_probe(
    admin: &mut Client,
) -> Result<PostgresSourceProbeResult, PostgresPlanError> {
    let backend_pid: i32 = admin.query_one("SELECT pg_backend_pid()", &[])?.get(0);
    let trigger = Identifier::new(format!("sql_splitter_profile_probe_{backend_pid}"))?;
    let function = Identifier::new(format!("sql_splitter_profile_probe_function_{backend_pid}"))?;
    let mut transaction = admin.transaction()?;
    let outcome = transaction.batch_execute(&format!(
        "CREATE FUNCTION pg_temp.{}() RETURNS event_trigger LANGUAGE plpgsql AS $body$BEGIN NULL; END$body$; CREATE EVENT TRIGGER {} ON ddl_command_start EXECUTE FUNCTION pg_temp.{}(); DROP EVENT TRIGGER {}; DROP FUNCTION pg_temp.{}()",
        quote_identifier(&function),
        quote_identifier(&trigger),
        quote_identifier(&function),
        quote_identifier(&trigger),
        quote_identifier(&function),
    ));
    transaction.rollback()?;
    Ok(exercise_probe_result(
        PostgresSourceProbeRequirement::TransactionalEventTriggerExercise,
        "database:event-trigger".into(),
        "transactional CREATE EVENT TRIGGER exercise was rolled back",
        outcome,
    ))
}

fn transactional_registry_probe(
    admin: &mut Client,
) -> Result<PostgresSourceProbeResult, PostgresPlanError> {
    let backend_pid: i32 = admin.query_one("SELECT pg_backend_pid()", &[])?.get(0);
    let schema = Identifier::new(format!("sql_splitter_profile_probe_{backend_pid}"))?;
    let mut transaction = admin.transaction()?;
    let outcome = transaction.batch_execute(&format!(
        "SELECT (pg_control_system()).system_identifier; CREATE SCHEMA {}; CREATE TABLE {}.registry (singleton boolean PRIMARY KEY, state text NOT NULL); INSERT INTO {}.registry VALUES (true,'probe'); UPDATE {}.registry SET state='checked' WHERE singleton; DELETE FROM {}.registry WHERE singleton; DROP SCHEMA {} CASCADE",
        quote_identifier(&schema),
        quote_identifier(&schema),
        quote_identifier(&schema),
        quote_identifier(&schema),
        quote_identifier(&schema),
        quote_identifier(&schema),
    ));
    transaction.rollback()?;
    Ok(exercise_probe_result(
        PostgresSourceProbeRequirement::TransactionalRegistryWriteExercise,
        "database:fence-registry".into(),
        "transactional registry schema and write exercise was rolled back",
        outcome,
    ))
}

fn exercise_probe_result(
    requirement: PostgresSourceProbeRequirement,
    object_id: String,
    detail: &str,
    outcome: Result<(), postgres::Error>,
) -> PostgresSourceProbeResult {
    let status = match outcome {
        Ok(()) => PostgresSourceProbeStatus::Proven,
        Err(error) => PostgresSourceProbeStatus::Unavailable {
            reason: format!("PostgreSQL rejected the transactional exercise: {error}"),
        },
    };
    PostgresSourceProbeResult {
        requirement,
        status,
        object_ids: vec![object_id],
        detail: detail.into(),
    }
}

fn sacrificial_backend_probe(
    admin: &mut Client,
    source_config: &PostgresEndpointConfig,
    administrator_role: &str,
) -> Result<PostgresSourceProbeResult, PostgresPlanError> {
    let signal_capability: bool = admin
        .query_one(
            "SELECT rolsuper OR pg_has_role(current_user,'pg_signal_backend','MEMBER') FROM pg_roles WHERE rolname=current_user",
            &[],
        )?
        .get(0);
    let mut sacrificial =
        source_config.connect_with_application_name("sql-splitter-source-profile-sacrificial")?;
    let identity = sacrificial.query_one("SELECT pg_backend_pid(),current_user", &[])?;
    let backend_pid: i32 = identity.get(0);
    let sacrificial_role: String = identity.get(1);
    let mut status = if sacrificial_role == administrator_role {
        PostgresSourceProbeStatus::Unavailable {
            reason: "sacrificial backend did not use a distinct role".into(),
        }
    } else if !signal_capability {
        PostgresSourceProbeStatus::Unavailable {
            reason: "administrator lacks pg_signal_backend or superuser authority".into(),
        }
    } else {
        match admin.query_one("SELECT pg_terminate_backend($1)", &[&backend_pid]) {
            Ok(row) if row.get::<_, bool>(0) => PostgresSourceProbeStatus::Proven,
            Ok(_) => PostgresSourceProbeStatus::Unavailable {
                reason: "pg_terminate_backend returned false for the sacrificial backend".into(),
            },
            Err(error) => PostgresSourceProbeStatus::Unavailable {
                reason: format!("PostgreSQL rejected sacrificial backend termination: {error}"),
            },
        }
    };
    if status == PostgresSourceProbeStatus::Proven && sacrificial.simple_query("SELECT 1").is_ok() {
        status = PostgresSourceProbeStatus::Unavailable {
            reason: "sacrificial backend remained usable after termination".into(),
        };
    }
    Ok(PostgresSourceProbeResult {
        requirement: PostgresSourceProbeRequirement::SacrificialBackendTermination,
        status,
        object_ids: vec![format!("role:{sacrificial_role}")],
        detail: "a distinct source-role backend was terminated and observed disconnected".into(),
    })
}

fn unix_seconds_now() -> Result<u64, PostgresPlanError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresPlanError::InvalidConfig("system clock is before the Unix epoch"))
        .map(|duration| duration.as_secs())
}

pub fn write_live_plan_with_consistency(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency_mode: PostgresConsistencyMode,
) -> Result<ReviewedPlan, PostgresPlanError> {
    let source_config = PostgresEndpointConfig::read(source_config)?;
    let target_config = PostgresEndpointConfig::read(target_config)?;
    if source_config.credential_env == target_config.credential_env {
        return Err(PostgresPlanError::InvalidConfig(
            "source and target must use separate credential references",
        ));
    }
    let source = inspect_endpoint(&source_config)?;
    let target = inspect_endpoint(&target_config)?;
    let plan = build_plan_with_consistency(&source, &target, consistency_mode)?;
    write_json_new(output, &plan)?;
    Ok(plan)
}

pub fn write_live_plan_with_outage_policy(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency_mode: PostgresConsistencyMode,
    assessment_input: Option<&Path>,
    maximum_approved_seconds: Option<u64>,
) -> Result<ReviewedPlan, PostgresPlanError> {
    write_live_plan_with_policies(
        source_config,
        target_config,
        output,
        consistency_mode,
        assessment_input,
        maximum_approved_seconds,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn write_live_plan_with_policies(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency_mode: PostgresConsistencyMode,
    assessment_input: Option<&Path>,
    maximum_approved_seconds: Option<u64>,
    source_profile: Option<PostgresSourceProfileKind>,
    source_profile_evidence: Option<&Path>,
) -> Result<ReviewedPlan, PostgresPlanError> {
    write_live_plan_with_profile_tier(
        source_config,
        target_config,
        output,
        consistency_mode,
        assessment_input,
        maximum_approved_seconds,
        source_profile,
        source_profile_evidence,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn write_live_plan_with_profile_tier(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
    consistency_mode: PostgresConsistencyMode,
    assessment_input: Option<&Path>,
    maximum_approved_seconds: Option<u64>,
    source_profile: Option<PostgresSourceProfileKind>,
    source_profile_evidence: Option<&Path>,
    verified_external_quiesce_rescan: bool,
) -> Result<ReviewedPlan, PostgresPlanError> {
    let outage_input = match (assessment_input, maximum_approved_seconds) {
        (None, None) => None,
        (Some(assessment_input), Some(maximum_approved_seconds)) => {
            Some((assessment_input, maximum_approved_seconds))
        }
        _ => {
            return Err(PostgresPlanError::InvalidConfig(
                "assessment input and maximum outage seconds must be supplied together",
            ));
        }
    };
    let source_config = PostgresEndpointConfig::read(source_config)?;
    let target_config = PostgresEndpointConfig::read(target_config)?;
    if source_config.credential_env == target_config.credential_env {
        return Err(PostgresPlanError::InvalidConfig(
            "source and target must use separate credential references",
        ));
    }
    let source = inspect_endpoint(&source_config)?;
    let target = inspect_endpoint(&target_config)?;
    let source_catalog_fingerprint = catalog_fingerprint(&source.catalog)?;
    let source_profile_contract = load_source_profile_contract(
        source_profile,
        source_profile_evidence,
        verified_external_quiesce_rescan,
        &source,
        &source_catalog_fingerprint,
    )?;
    let Some((assessment_input, maximum_approved_seconds)) = outage_input else {
        let plan = build_plan_with_consistency_and_contracts(
            &source,
            &target,
            consistency_mode,
            None,
            source_profile_contract,
        )?;
        write_json_new(output, &plan)?;
        return Ok(plan);
    };
    let assessment: AssessmentArtifact = read_json(assessment_input)?;
    assessment.validate()?;
    if assessment.reviewed_plan.plan.source_catalog_fingerprint != source_catalog_fingerprint {
        return Err(PostgresPlanError::InvalidConfig(
            "assessment source catalog fingerprint does not match the live source",
        ));
    }
    let source_major = u16::try_from(source.server_version_num / 10_000).map_err(|_| {
        PostgresPlanError::InvalidConfig("negative PostgreSQL server version number")
    })?;
    if assessment.source_evidence.server_version_num / 10_000 != u32::from(source_major) {
        return Err(PostgresPlanError::InvalidConfig(
            "assessment PostgreSQL major version does not match the live source",
        ));
    }
    let unbound = build_plan_with_consistency(&source, &target, consistency_mode)?;
    let physical_relations = copied_physical_relations(&source.catalog, &unbound.plan.operations)?;
    let estimates = assessment
        .scope_estimates
        .iter()
        .map(|estimate| (&estimate.table, estimate))
        .collect::<BTreeMap<_, _>>();
    let reviewed_assessed_bytes =
        physical_relations
            .into_iter()
            .try_fold(0_u64, |total, (namespace, relation)| {
                let table = QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: relation.name.clone(),
                };
                let estimate = estimates
                    .get(&table)
                    .ok_or(PostgresPlanError::InvalidConfig(
                        "assessment is missing a copied physical relation estimate",
                    ))?;
                total.checked_add(estimate.total_relation_bytes).ok_or(
                    PostgresPlanError::InvalidConfig("assessment physical byte total overflowed"),
                )
            })?;
    let (assessed_bytes, included_relations, assessed_at_unix_seconds, throughput_profile) =
        match &assessment.projected_window {
            super::assessment::ProjectedWindow::Estimated {
                assessed_bytes,
                included_relations,
                assessed_at_unix_seconds,
                throughput_profile,
                ..
            } => (
                *assessed_bytes,
                included_relations,
                *assessed_at_unix_seconds,
                throughput_profile.clone(),
            ),
            super::assessment::ProjectedWindow::NotAssessed { .. } => {
                return Err(PostgresPlanError::InvalidConfig(
                    "assessment has no valid estimated outage profile",
                ));
            }
        };
    let expected_relations = copied_physical_relations(&source.catalog, &unbound.plan.operations)?
        .into_iter()
        .map(|(namespace, relation)| QualifiedTable {
            namespace: namespace.name.clone(),
            name: relation.name.clone(),
        })
        .collect::<BTreeSet<_>>();
    if assessed_bytes != reviewed_assessed_bytes
        || included_relations.iter().cloned().collect::<BTreeSet<_>>() != expected_relations
    {
        return Err(PostgresPlanError::InvalidConfig(
            "assessment byte basis does not match copied physical relations",
        ));
    }
    throughput_profile.validate_at(source_major, assessed_at_unix_seconds)?;
    let reviewed_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PostgresPlanError::InvalidConfig("system clock is before the Unix epoch"))?
        .as_secs();
    throughput_profile.validate_at(source_major, reviewed_at_unix_seconds)?;
    let reviewed_projected_seconds =
        projected_seconds(reviewed_assessed_bytes, &throughput_profile)?;
    let assessment_digest = hex::encode(Sha256::digest(serde_json::to_vec(&assessment)?));
    let policy = ReviewedOutagePolicy {
        schema_version: OUTAGE_PROJECTION_SCHEMA_VERSION,
        assessment_digest,
        source_catalog_fingerprint,
        byte_basis: ByteBasis::PostgresTotalRelationBytesV1,
        throughput_profile,
        reviewed_at_unix_seconds,
        reviewed_assessed_bytes,
        reviewed_projected_seconds,
        maximum_approved_seconds,
    };
    policy.validate()?;
    let plan = build_plan_with_consistency_and_contracts(
        &source,
        &target,
        consistency_mode,
        Some(policy),
        source_profile_contract,
    )?;
    write_json_new(output, &plan)?;
    Ok(plan)
}

fn load_source_profile_contract(
    profile: Option<PostgresSourceProfileKind>,
    evidence_path: Option<&Path>,
    verified_external_quiesce_rescan: bool,
    source: &CatalogSnapshot,
    source_catalog_fingerprint: &str,
) -> Result<Option<PostgresSourceProfileContract>, PostgresPlanError> {
    if verified_external_quiesce_rescan
        && profile != Some(PostgresSourceProfileKind::AttestedExternalQuiesce)
    {
        return Err(PostgresPlanError::InvalidConfig(
            "verified external-quiesce re-scan requires the attested-external-quiesce profile",
        ));
    }
    let contract = match (profile, evidence_path) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            return Err(PostgresPlanError::InvalidConfig(
                "source-profile evidence requires an explicit source profile",
            ));
        }
        (Some(PostgresSourceProfileKind::AttestedExternalQuiesce), None) => {
            PostgresSourceProfileContract::AttestedExternalQuiesce {
                verified_rescan: verified_external_quiesce_rescan,
                freeze_enforced_by_tool: false,
            }
        }
        (Some(PostgresSourceProfileKind::AttestedExternalQuiesce), Some(_)) => {
            return Err(PostgresPlanError::InvalidConfig(
                "external-quiesce profiles take attestation only at execute and resume",
            ));
        }
        (
            Some(
                profile @ (PostgresSourceProfileKind::SelfManagedAdministrator
                | PostgresSourceProfileKind::ManagedAdministrator),
            ),
            Some(path),
        ) => {
            let probe_artifact: PostgresSourceProbeArtifact = read_json(path)?;
            probe_artifact.require_all_proven()?;
            let probe_artifact_digest = probe_artifact.canonical_hash()?;
            match profile {
                PostgresSourceProfileKind::SelfManagedAdministrator => {
                    PostgresSourceProfileContract::SelfManagedAdministrator {
                        probe_artifact,
                        probe_artifact_digest,
                    }
                }
                PostgresSourceProfileKind::ManagedAdministrator => {
                    PostgresSourceProfileContract::ManagedAdministrator {
                        probe_artifact,
                        probe_artifact_digest,
                    }
                }
                PostgresSourceProfileKind::AttestedExternalQuiesce => unreachable!(),
            }
        }
        (Some(_), None) => {
            return Err(PostgresPlanError::InvalidConfig(
                "administrator source profiles require a protected probe artifact",
            ));
        }
    };
    contract.validate_for_plan(
        &source.endpoint_identity,
        source_catalog_fingerprint,
        match profile {
            Some(PostgresSourceProfileKind::AttestedExternalQuiesce) => "consistent-snapshot",
            Some(_) => "write-fence",
            None => unreachable!(),
        },
    )?;
    Ok(Some(contract))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assessment_outputs_must_be_distinct() {
        let error = write_live_assessment("missing.toml", "same.out", "same.out").unwrap_err();
        assert!(matches!(error, PostgresPlanError::InvalidConfig(_)));
    }

    #[test]
    fn assessment_preflights_both_outputs_before_reading_the_source_config() {
        let directory = tempfile::tempdir().unwrap();
        let assessment = directory.path().join("assessment.json");
        let report = directory.path().join("assessment.md");
        std::fs::write(&report, "existing").unwrap();
        let error = write_live_assessment("missing.toml", &assessment, &report).unwrap_err();
        assert!(matches!(error, PostgresPlanError::Artifact(_)));
        assert!(!assessment.exists());
    }

    #[test]
    fn assessment_security_failures_have_typed_errors() {
        assert!(matches!(
            validate_assessment_source_security(false, true),
            Err(PostgresPlanError::SourceNotReadOnly)
        ));
        assert!(matches!(
            validate_assessment_source_security(true, false),
            Err(PostgresPlanError::SourceRoleHoldsDirectWritePrivilege)
        ));
    }

    fn snapshot(endpoint: &str, with_table: bool) -> CatalogSnapshot {
        let mut objects = Vec::new();
        if with_table {
            objects.push(CatalogObject {
                id: "table-1".into(),
                kind: CatalogObjectKind::Table,
                name: Identifier::new("accounts").unwrap(),
                definition: Vec::new(),
                attributes: BTreeMap::from([("relkind".into(), serde_json::json!("r"))]),
            });
            objects.push(CatalogObject {
                id: "table-1:1".into(),
                kind: CatalogObjectKind::Column,
                name: Identifier::new("accounts.id").unwrap(),
                definition: b"bigint".to_vec(),
                attributes: BTreeMap::from([
                    (
                        "table_oid".into(),
                        serde_json::Value::String("table-1".into()),
                    ),
                    ("identity".into(), serde_json::json!("")),
                ]),
            });
        }
        CatalogSnapshot {
            endpoint_identity: endpoint.into(),
            server_version: "17.0".into(),
            server_version_num: 170000,
            catalog: VendorCatalog {
                format_version: CATALOG_FORMAT_VERSION,
                dialect: "postgresql".into(),
                server_version: "17.0".into(),
                database: Identifier::new("app").unwrap(),
                namespaces: vec![CatalogNamespace {
                    id: "namespace-public".into(),
                    name: Identifier::new("public").unwrap(),
                    owner: Some("owner".into()),
                    charset: Some("UTF8".into()),
                    collation: None,
                    objects,
                }],
                dependencies: Vec::new(),
                vendor_metadata: BTreeMap::new(),
            },
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "hostname_verified;roots=platform;client=none".into(),
        }
    }

    #[test]
    fn config_rejects_inline_and_unknown_fields() {
        let inline = r#"
host = "db.example.com"
database = "app"
user = "reader"
credential_env = "PGPASSWORD"
password = "secret"
"#;
        assert!(toml::from_str::<PostgresEndpointConfig>(inline).is_err());
    }

    #[test]
    fn source_assessment_marks_every_target_field_not_assessed() {
        let source = snapshot("postgres://source/app?user=reader", true);
        let assessment = build_source_assessment(
            source,
            true,
            true,
            vec![ScopeEstimate {
                table: QualifiedTable {
                    namespace: Identifier::new("public").unwrap(),
                    name: Identifier::new("accounts").unwrap(),
                },
                estimated_rows: 42,
                relation_bytes: 4096,
                total_relation_bytes: 8192,
            }],
        )
        .unwrap();
        let plan = &assessment.reviewed_plan.plan;
        assert_eq!(plan.purpose, PlanPurpose::Assessment);
        assert_eq!(plan.target_endpoint_identity, AssessmentStatus::NotAssessed);
        assert_eq!(
            plan.target_catalog_fingerprint,
            AssessmentStatus::NotAssessed
        );
        assert_eq!(plan.target_catalog, AssessmentStatus::NotAssessed);
        assert_eq!(plan.target_tls_binding, AssessmentStatus::NotAssessed);
        assert_eq!(plan.capabilities["acl.report_only"], "approval_required");
        assert!(plan.unsupported_objects.objects.iter().all(|finding| {
            !matches!(
                finding.object_kind.as_str(),
                "target_precondition" | "endpoint_precondition"
            )
        }));
        assert!(plan
            .operations
            .iter()
            .any(|operation| operation.kind == OperationKind::CreateTable));
        assert!(plan
            .operations
            .iter()
            .any(|operation| operation.kind == OperationKind::CopyTable));
        assert!(plan
            .operations
            .iter()
            .any(|operation| operation.kind == OperationKind::VerifySchema));
        assert!(matches!(
            assessment.projected_window,
            crate::migration::assessment::ProjectedWindow::NotAssessed { .. }
        ));
        assert_eq!(assessment.scope_estimates[0].estimated_rows, 42);
        assert!(render_markdown(&assessment)
            .unwrap()
            .contains("Target endpoint: **not assessed**"));
    }

    #[test]
    fn source_assessment_requires_proven_read_only_role() {
        let error = build_source_assessment(snapshot("source", false), true, false, Vec::new())
            .unwrap_err();
        assert!(matches!(
            error,
            PostgresPlanError::Assessment(AssessmentError::SourceRoleHasDirectWritePrivilege)
        ));
    }

    #[test]
    fn default_tls_verifies_certificates_and_hostnames() {
        let parsed: PostgresEndpointConfig = toml::from_str(
            r#"
host = "db.example.com"
database = "app"
user = "reader"
credential_env = "PGPASSWORD"
"#,
        )
        .unwrap();
        assert!(!parsed.tls.insecure);
        assert_eq!(parsed.port, 5432);
    }

    #[test]
    fn client_certificate_and_private_key_are_an_atomic_policy() {
        let certificate_only = r#"
host = "db.example.com"
database = "app"
user = "reader"
credential_env = "PGPASSWORD"

[tls]
client_certificate = "/cert.pem"
"#;
        let config: PostgresEndpointConfig = toml::from_str(certificate_only).unwrap();
        assert!(config.validate().is_err());

        let mutual_tls = r#"
host = "db.example.com"
database = "app"
user = "reader"
credential_env = "PGPASSWORD"

[tls]
client_certificate = "/cert.pem"
client_private_key = "/key.pem"
"#;
        let config: PostgresEndpointConfig = toml::from_str(mutual_tls).unwrap();
        assert!(config.tls.client_certificate.is_some());
        assert!(config.tls.client_private_key.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn directly_constructed_config_rejects_unprotected_client_key() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let certificate = directory.path().join("client.pem");
        let key = directory.path().join("client-key.pem");
        fs::write(&certificate, b"certificate").unwrap();
        fs::write(&key, b"private-key").unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
        let mut config: PostgresEndpointConfig = toml::from_str(
            r#"
host = "db.example.com"
database = "app"
user = "reader"
credential_env = "PGPASSWORD"
"#,
        )
        .unwrap();
        config.tls.client_certificate = Some(certificate.to_string_lossy().into_owned());
        config.tls.client_private_key = Some(key.to_string_lossy().into_owned());

        assert!(postgres_tls_binding(&config).is_err());
    }

    #[test]
    fn identifier_quoting_doubles_embedded_quotes() {
        let identifier = Identifier::new("accounts\"; DROP TABLE audit; --").unwrap();
        assert_eq!(
            quote_identifier(&identifier),
            "\"accounts\"\"; DROP TABLE audit; --\""
        );
    }

    #[test]
    fn binary_decoder_rejects_invalid_boolean_and_jsonb_versions() {
        assert!(matches!(
            decode_value(&Type::BOOL, Some(RawBinary(vec![2]))),
            Err(ConnectionError::Database(_))
        ));
        assert!(matches!(
            decode_value(&Type::JSONB, Some(RawBinary(vec![2, b'{', b'}']))),
            Err(ConnectionError::Database(_))
        ));
        assert_eq!(
            decode_value(
                &Type::JSON,
                Some(RawBinary(br#"{"b":1.00,"a":1e0}"#.to_vec()))
            )
            .unwrap(),
            DbValue::Json(br#"{"a":1,"b":1}"#.to_vec())
        );
        assert!(matches!(
            decode_value(&Type::JSON, Some(RawBinary(br#"{"a":1,"a":2}"#.to_vec()))),
            Err(ConnectionError::Database(_))
        ));
    }

    #[test]
    fn table_operation_id_is_bound_to_exact_catalog_payload() {
        let mut source = snapshot("source", true);
        let target = snapshot("target", false);
        let first = build_plan(&source, &target).unwrap();
        source.catalog.namespaces[0].objects[1].definition = b"numeric".to_vec();
        let second = build_plan(&source, &target).unwrap();
        assert_ne!(first.plan.operations[0].id, second.plan.operations[0].id);
        assert!(first.plan.source_catalog.is_some());
        first.validate().unwrap();
        second.validate().unwrap();
    }

    #[test]
    fn catalog_identity_rejects_cross_class_collisions_and_dangling_dependencies() {
        let mut catalog = snapshot("source", true).catalog;
        catalog.namespaces[0].objects[1].id = catalog.namespaces[0].objects[0].id.clone();
        assert!(validate_catalog_identity(&catalog).is_err());

        let mut catalog = snapshot("source", true).catalog;
        catalog.dependencies.push(CatalogDependency {
            from_object_id: catalog.namespaces[0].objects[0].id.clone(),
            to_object_id: "relation:absent".into(),
            dependency_type: "test".into(),
        });
        assert!(validate_catalog_identity(&catalog).is_err());
    }

    #[test]
    fn reviewed_plan_binds_the_selected_consistency_contract() {
        let source = snapshot("source", true);
        let target = snapshot("target", false);
        let native = build_plan_with_consistency(
            &source,
            &target,
            PostgresConsistencyMode::ConsistentSnapshot,
        )
        .unwrap();
        let fenced =
            build_plan_with_consistency(&source, &target, PostgresConsistencyMode::WriteFence)
                .unwrap();
        assert_eq!(native.plan.consistency_mode, "consistent-snapshot");
        assert_eq!(fenced.plan.consistency_mode, "write-fence");
        assert_ne!(native.plan_hash, fenced.plan_hash);
    }

    fn composite_foreign_key(match_type: PostgresForeignKeyMatch) -> PostgresForeignKey {
        PostgresForeignKey {
            catalog_object_id: "fk-oid-42".into(),
            name: Identifier::new("orders_customer_fk").unwrap(),
            table: QualifiedTable {
                namespace: Identifier::new("sales").unwrap(),
                name: Identifier::new("orders").unwrap(),
            },
            columns: vec![
                Identifier::new("tenant_id").unwrap(),
                Identifier::new("customer_id").unwrap(),
            ],
            referenced_table: QualifiedTable {
                namespace: Identifier::new("identity").unwrap(),
                name: Identifier::new("customers").unwrap(),
            },
            referenced_columns: vec![
                Identifier::new("tenant_id").unwrap(),
                Identifier::new("id").unwrap(),
            ],
            match_type,
            update_action: PostgresForeignKeyAction::Cascade,
            delete_action: PostgresForeignKeyAction::SetNull,
            deferrable: true,
            initially_deferred: true,
        }
    }

    #[test]
    fn composite_match_simple_anti_join_exempts_any_null_child_key() {
        let query =
            foreign_key_violation_query(&composite_foreign_key(PostgresForeignKeyMatch::Simple))
                .unwrap();
        assert!(query.contains(
            "(child.\"tenant_id\" IS NOT NULL AND child.\"customer_id\" IS NOT NULL) AND NOT EXISTS"
        ));
        assert!(query.contains(
            "parent.\"tenant_id\" = child.\"tenant_id\" AND parent.\"id\" = child.\"customer_id\""
        ));
    }

    #[test]
    fn composite_match_full_anti_join_rejects_partially_null_child_key() {
        let query =
            foreign_key_violation_query(&composite_foreign_key(PostgresForeignKeyMatch::Full))
                .unwrap();
        assert!(query.contains(
            "(child.\"tenant_id\" IS NULL OR child.\"customer_id\" IS NULL) AND (child.\"tenant_id\" IS NOT NULL OR child.\"customer_id\" IS NOT NULL)"
        ));
    }

    #[test]
    fn foreign_key_ddl_is_typed_quoted_and_not_valid() {
        let statement =
            foreign_key_add_statement(&composite_foreign_key(PostgresForeignKeyMatch::Full));
        assert_eq!(
            statement,
            "ALTER TABLE \"sales\".\"orders\" ADD CONSTRAINT \"orders_customer_fk\" FOREIGN KEY (\"tenant_id\", \"customer_id\") REFERENCES \"identity\".\"customers\" (\"tenant_id\", \"id\") MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED NOT VALID"
        );
    }

    #[test]
    fn targeted_set_column_metadata_fails_closed() {
        let object = CatalogObject {
            id: "42".into(),
            kind: CatalogObjectKind::ForeignKey,
            name: Identifier::new("fk").unwrap(),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("child")),
                ("referenced_table_oid".into(), serde_json::json!("parent")),
                ("columns".into(), serde_json::json!(["parent_id"])),
                ("referenced_columns".into(), serde_json::json!(["id"])),
                ("match_type".into(), serde_json::json!("s")),
                ("update_action".into(), serde_json::json!("a")),
                ("delete_action".into(), serde_json::json!("n")),
                ("delete_set_columns".into(), serde_json::json!([2])),
                ("deferrable".into(), serde_json::json!(false)),
                ("deferred".into(), serde_json::json!(false)),
                ("validated".into(), serde_json::json!(true)),
            ]),
        };
        let catalog = VendorCatalog {
            format_version: CATALOG_FORMAT_VERSION,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new("db").unwrap(),
            namespaces: vec![CatalogNamespace {
                id: "n".into(),
                name: Identifier::new("public").unwrap(),
                owner: None,
                charset: None,
                collation: None,
                objects: vec![
                    CatalogObject {
                        id: "child".into(),
                        kind: CatalogObjectKind::Table,
                        name: Identifier::new("child").unwrap(),
                        definition: Vec::new(),
                        attributes: BTreeMap::new(),
                    },
                    CatalogObject {
                        id: "parent".into(),
                        kind: CatalogObjectKind::Table,
                        name: Identifier::new("parent").unwrap(),
                        definition: Vec::new(),
                        attributes: BTreeMap::new(),
                    },
                    object,
                ],
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };
        assert!(postgres_foreign_keys(&catalog).is_err());
    }

    fn key_catalog(objects: Vec<CatalogObject>) -> VendorCatalog {
        let mut all = vec![
            CatalogObject {
                id: "relation:1".into(),
                kind: CatalogObjectKind::Table,
                name: Identifier::new("accounts").unwrap(),
                definition: Vec::new(),
                attributes: BTreeMap::from([
                    ("relkind".into(), serde_json::json!("r")),
                    ("persistence".into(), serde_json::json!("p")),
                ]),
            },
            CatalogObject {
                id: "column:1:1".into(),
                kind: CatalogObjectKind::Column,
                name: Identifier::new("id").unwrap(),
                definition: b"bigint".to_vec(),
                attributes: BTreeMap::from([
                    ("table_oid".into(), serde_json::json!("relation:1")),
                    ("ordinal".into(), serde_json::json!(1)),
                    ("nullable".into(), serde_json::json!(false)),
                    ("identity".into(), serde_json::json!("")),
                    ("generated".into(), serde_json::json!("")),
                    ("type_schema".into(), serde_json::json!("pg_catalog")),
                ]),
            },
        ];
        all.extend(objects);
        VendorCatalog {
            format_version: CATALOG_FORMAT_VERSION,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new("source").unwrap(),
            namespaces: vec![CatalogNamespace {
                id: "namespace:public".into(),
                name: Identifier::new("public").unwrap(),
                owner: None,
                charset: Some("UTF8".into()),
                collation: None,
                objects: all,
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        }
    }

    fn standalone_index(id: &str) -> CatalogObject {
        CatalogObject {
            id: id.into(),
            kind: CatalogObjectKind::Index,
            name: Identifier::new("accounts_id_uidx").unwrap(),
            definition: b"CREATE UNIQUE INDEX accounts_id_uidx ON public.accounts USING btree (id)"
                .to_vec(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("relation:1")),
                ("unique".into(), serde_json::json!(true)),
                ("primary".into(), serde_json::json!(false)),
                ("valid".into(), serde_json::json!(true)),
                ("ready".into(), serde_json::json!(true)),
                ("live".into(), serde_json::json!(true)),
                ("immediate".into(), serde_json::json!(true)),
                ("clustered".into(), serde_json::json!(false)),
                ("replica_identity".into(), serde_json::json!(false)),
                ("exclusion".into(), serde_json::json!(false)),
                ("nulls_not_distinct".into(), serde_json::json!(false)),
                ("access_method".into(), serde_json::json!("btree")),
                ("persistence".into(), serde_json::json!("p")),
                ("reloptions".into(), serde_json::Value::Null),
                ("tablespace".into(), serde_json::Value::Null),
                ("predicate".into(), serde_json::Value::Null),
                ("has_expressions".into(), serde_json::json!(false)),
                ("key_attribute_count".into(), serde_json::json!(1)),
                ("attribute_count".into(), serde_json::json!(1)),
                ("columns".into(), serde_json::json!(["id"])),
                ("included_columns".into(), serde_json::json!([])),
                ("options".into(), serde_json::json!([0])),
                (
                    "opclasses".into(),
                    serde_json::json!([{"schema":"pg_catalog","name":"int8_ops","default":true}]),
                ),
                ("collations".into(), serde_json::json!([null])),
                ("collations_default".into(), serde_json::json!([true])),
                ("constraint_oid".into(), serde_json::Value::Null),
            ]),
        }
    }

    fn ordinary_index(id: &str) -> CatalogObject {
        let mut index = standalone_index(id);
        index.name = Identifier::new("accounts_id_idx").unwrap();
        index.definition =
            b"CREATE INDEX accounts_id_idx ON public.accounts USING btree (id)".to_vec();
        index
            .attributes
            .insert("unique".into(), serde_json::json!(false));
        index
    }

    fn sequence_object(
        ownership: Option<PostgresSequenceOwnership>,
        last_value: i64,
        is_called: bool,
    ) -> CatalogObject {
        CatalogObject {
            id: "relation:sequence-1".into(),
            kind: CatalogObjectKind::Sequence,
            name: Identifier::new("accounts_id_seq").unwrap(),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("relkind".into(), serde_json::json!("S")),
                ("persistence".into(), serde_json::json!("p")),
                ("type".into(), serde_json::json!("bigint")),
                ("start".into(), serde_json::json!("-10")),
                ("increment".into(), serde_json::json!("-2")),
                ("minimum".into(), serde_json::json!("-100")),
                ("maximum".into(), serde_json::json!("100")),
                ("cache".into(), serde_json::json!("7")),
                ("cycle".into(), serde_json::json!(true)),
                (
                    "last_value".into(),
                    serde_json::json!(last_value.to_string()),
                ),
                ("is_called".into(), serde_json::json!(is_called)),
                (
                    "ownership_count".into(),
                    serde_json::json!(usize::from(ownership.is_some())),
                ),
                (
                    "ownership".into(),
                    ownership
                        .map(|value| serde_json::to_value(value).unwrap())
                        .unwrap_or(serde_json::Value::Null),
                ),
            ]),
        }
    }

    #[test]
    fn identity_sequence_is_created_inline_once_with_exact_options() {
        let ownership = PostgresSequenceOwnership {
            table: QualifiedTable {
                namespace: Identifier::new("public").unwrap(),
                name: Identifier::new("accounts").unwrap(),
            },
            column: Identifier::new("id").unwrap(),
            kind: PostgresSequenceOwnershipKind::IdentityAlways,
        };
        let mut catalog = key_catalog(vec![sequence_object(Some(ownership), -10, false)]);
        let column = catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::Column)
            .unwrap();
        column
            .attributes
            .insert("identity".into(), serde_json::json!("a"));
        let statements = pre_data_statements(&catalog).unwrap();
        assert_eq!(
            statements
                .iter()
                .filter(|statement| statement.contains("accounts_id_seq"))
                .count(),
            1
        );
        let table = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE"))
            .unwrap();
        assert!(table.contains("GENERATED ALWAYS AS IDENTITY"));
        assert!(table.contains("SEQUENCE NAME \"public\".\"accounts_id_seq\""));
        assert!(table
            .contains("INCREMENT BY -2 MINVALUE -100 MAXVALUE 100 START WITH -10 CACHE 7 CYCLE"));
        assert!(!statements
            .iter()
            .any(|statement| statement.starts_with("ALTER TABLE")
                && statement.contains("ADD GENERATED")));
    }

    #[test]
    fn sequence_plan_is_blocked_without_write_fence_and_restores_after_copy() {
        let mut source_catalog = key_catalog(vec![
            sequence_object(None, 42, true),
            CatalogObject {
                id: "constraint:key".into(),
                kind: CatalogObjectKind::PrimaryKey,
                name: Identifier::new("accounts_pkey").unwrap(),
                definition: b"PRIMARY KEY (id)".to_vec(),
                attributes: BTreeMap::from([
                    ("table_oid".into(), serde_json::json!("relation:1")),
                    ("validated".into(), serde_json::json!(true)),
                    ("columns".into(), serde_json::json!(["id"])),
                ]),
            },
        ]);
        source_catalog.format_version = CATALOG_FORMAT_VERSION;
        let source = CatalogSnapshot {
            endpoint_identity: "source".into(),
            server_version: "17".into(),
            server_version_num: 170000,
            catalog: source_catalog,
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "tls-source".into(),
        };
        let mut target = snapshot("target", false);
        target.catalog.format_version = CATALOG_FORMAT_VERSION;
        let snapshot_plan = build_plan(&source, &target).unwrap();
        assert!(snapshot_plan.plan.unsupported_objects.blocks_execution());
        let fenced =
            build_plan_with_consistency(&source, &target, PostgresConsistencyMode::WriteFence)
                .unwrap();
        let create = fenced
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CreateSequence)
            .unwrap();
        let restore = fenced
            .plan
            .operations
            .iter()
            .find(|operation| {
                operation.kind == OperationKind::Vendor("restore_postgres_sequence".into())
            })
            .unwrap();
        assert_eq!(restore.dependencies, vec![create.id.clone()]);
        let planned: PostgresSequence =
            serde_json::from_value(restore.parameters["postgres_sequence"].clone()).unwrap();
        assert_eq!(planned.last_value, 42);
        assert!(planned.is_called);
    }

    #[test]
    fn sequence_next_value_checks_bounds_cycle_and_identifier_quoting() {
        let catalog = key_catalog(vec![sequence_object(None, -100, true)]);
        let mut sequence = postgres_sequences(&catalog).unwrap().remove(0);
        sequence.last_value = sequence.minimum_value;
        sequence.is_called = true;
        assert_eq!(sequence.expected_next_value().unwrap(), Some(100));
        sequence.cycle = false;
        assert_eq!(sequence.expected_next_value().unwrap(), None);

        let namespace = Identifier::new("Mixed.Schema").unwrap();
        let name = Identifier::new("seq\"name").unwrap();
        assert_eq!(
            qualified_regclass_name(&namespace, &name),
            "\"Mixed.Schema\".\"seq\"\"name\""
        );

        sequence.data_type = "smallint".into();
        sequence.maximum_value = i64::from(i16::MAX) + 1;
        assert!(sequence.expected_next_value().is_err());
    }

    fn generated_column_object(dependencies: Vec<PostgresGeneratedDependency>) -> CatalogObject {
        CatalogObject {
            id: "column:1:2".into(),
            kind: CatalogObjectKind::Column,
            name: Identifier::new("doubled").unwrap(),
            definition: b"bigint".to_vec(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("relation:1")),
                ("table".into(), serde_json::json!("accounts")),
                ("ordinal".into(), serde_json::json!(2)),
                ("nullable".into(), serde_json::json!(true)),
                ("default".into(), serde_json::Value::Null),
                ("generated_expression".into(), serde_json::json!("(id * 2)")),
                ("identity".into(), serde_json::json!("")),
                ("generated".into(), serde_json::json!("s")),
                ("collation".into(), serde_json::Value::Null),
                ("collation_schema".into(), serde_json::Value::Null),
                ("type_schema".into(), serde_json::json!("pg_catalog")),
                ("type_name".into(), serde_json::json!("int8")),
                (
                    "generated_dependencies".into(),
                    serde_json::to_value(dependencies).unwrap(),
                ),
            ]),
        }
    }

    #[test]
    fn stored_generated_column_is_typed_and_rendered_without_a_default() {
        let dependencies = vec![
            PostgresGeneratedDependency {
                kind: PostgresGeneratedDependencyKind::Column,
                identity: "column:id".into(),
            },
            PostgresGeneratedDependency {
                kind: PostgresGeneratedDependencyKind::Operator,
                identity: "operator:pg_catalog.*(bigint,integer)->bigint:function=pg_catalog.int8mul(bigint,bigint)".into(),
            },
        ];
        let catalog = key_catalog(vec![generated_column_object(dependencies.clone())]);
        let parsed = postgres_generated_columns(&catalog).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].dependencies, dependencies);
        let statements = pre_data_statements(&catalog).unwrap();
        let table = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE"))
            .unwrap();
        assert!(table.contains("\"doubled\" bigint GENERATED ALWAYS AS ((id * 2)) STORED"));
        assert!(!table.contains("\"doubled\" bigint DEFAULT"));
    }

    #[test]
    fn generated_column_rejects_user_function_dependency_and_pagination_key() {
        let unsafe_dependency = PostgresGeneratedDependency {
            kind: PostgresGeneratedDependencyKind::Function,
            identity: "function:public.bank_round(numeric)->numeric".into(),
        };
        let catalog = key_catalog(vec![generated_column_object(vec![unsafe_dependency])]);
        assert!(postgres_generated_columns(&catalog).is_err());

        let mut generated = generated_column_object(vec![PostgresGeneratedDependency {
            kind: PostgresGeneratedDependencyKind::Column,
            identity: "column:id".into(),
        }]);
        generated
            .attributes
            .insert("nullable".into(), serde_json::json!(false));
        let constraint = CatalogObject {
            id: "constraint:generated-key".into(),
            kind: CatalogObjectKind::UniqueConstraint,
            name: Identifier::new("accounts_doubled_key").unwrap(),
            definition: b"UNIQUE (doubled)".to_vec(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("relation:1")),
                ("validated".into(), serde_json::json!(true)),
                ("columns".into(), serde_json::json!(["doubled"])),
            ]),
        };
        let catalog = key_catalog(vec![generated, constraint]);
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("accounts").unwrap(),
        };
        assert!(select_resumable_key(&catalog, &table).is_err());
    }

    fn range_partition_catalog() -> VendorCatalog {
        let mut catalog = key_catalog(vec![CatalogObject {
            id: "constraint:root-key".into(),
            kind: CatalogObjectKind::PrimaryKey,
            name: Identifier::new("accounts_pkey").unwrap(),
            definition: b"PRIMARY KEY (id)".to_vec(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("relation:1")),
                ("validated".into(), serde_json::json!(true)),
                ("columns".into(), serde_json::json!(["id"])),
            ]),
        }]);
        let root = catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.id == "relation:1")
            .unwrap();
        root.attributes
            .insert("relkind".into(), serde_json::json!("p"));
        root.attributes
            .insert("is_partition".into(), serde_json::json!(false));
        root.attributes
            .insert("partition_strategy".into(), serde_json::json!("r"));
        root.attributes
            .insert("partition_key_column".into(), serde_json::json!("id"));
        root.attributes
            .insert("partition_key_type".into(), serde_json::json!("bigint"));
        root.attributes
            .insert("default_partition_oid".into(), serde_json::Value::Null);
        for (id, name, bound) in [
            (
                "relation:10",
                "accounts_negative",
                "FOR VALUES FROM (MINVALUE) TO (0)",
            ),
            (
                "relation:11",
                "accounts_positive",
                "FOR VALUES FROM (0) TO (MAXVALUE)",
            ),
        ] {
            catalog.namespaces[0].objects.push(CatalogObject {
                id: id.into(),
                kind: CatalogObjectKind::Partition,
                name: Identifier::new(name).unwrap(),
                definition: Vec::new(),
                attributes: BTreeMap::from([
                    ("relkind".into(), serde_json::json!("r")),
                    ("persistence".into(), serde_json::json!("p")),
                    ("is_partition".into(), serde_json::json!(true)),
                    (
                        "partition_parent_oid".into(),
                        serde_json::json!("relation:1"),
                    ),
                    ("partition_bound".into(), serde_json::json!(bound)),
                ]),
            });
        }
        catalog
    }

    #[test]
    fn typed_partition_bounds_parse_validate_and_render() {
        let catalog = range_partition_catalog();
        let topologies = postgres_partition_topologies(&catalog).unwrap();
        assert_eq!(topologies.len(), 1);
        assert_eq!(topologies[0].strategy, PostgresPartitionStrategy::Range);
        assert_eq!(
            partition_leaf_create_statement(&topologies[0], &topologies[0].leaves[0]),
            "CREATE TABLE \"public\".\"accounts_negative\" PARTITION OF \"public\".\"accounts\" FOR VALUES FROM (MINVALUE) TO (0)"
        );
        assert_eq!(
            parse_partition_bound(
                "FOR VALUES IN ('-2'::integer, 0, NULL)",
                PostgresPartitionStrategy::List
            )
            .unwrap(),
            PostgresPartitionBound::List {
                values: vec![Some(-2), Some(0), None]
            }
        );
        assert!(parse_partition_bound(
            "FOR VALUES WITH (modulus 2, remainder 2)",
            PostgresPartitionStrategy::Hash
        )
        .is_err());
    }

    #[test]
    fn partition_plan_copies_only_root_after_every_leaf_exists() {
        let source = CatalogSnapshot {
            endpoint_identity: "source".into(),
            server_version: "17".into(),
            server_version_num: 170000,
            catalog: range_partition_catalog(),
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "tls-source".into(),
        };
        let mut target = snapshot("target", false);
        target.catalog.format_version = CATALOG_FORMAT_VERSION;
        let reviewed = build_plan(&source, &target).unwrap();
        let copies = reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| operation.kind == OperationKind::CopyTable)
            .collect::<Vec<_>>();
        assert_eq!(copies.len(), 1);
        assert_eq!(copies[0].table.as_ref().unwrap().name.as_str(), "accounts");
        let leaf_creates = reviewed
            .plan
            .operations
            .iter()
            .filter(|operation| {
                operation.kind == OperationKind::Vendor("create_postgres_partition".into())
            })
            .collect::<Vec<_>>();
        assert_eq!(leaf_creates.len(), 2);
        assert!(leaf_creates
            .iter()
            .all(|leaf| copies[0].dependencies.contains(&leaf.id)));
        let statements = pre_data_statements(&source.catalog).unwrap();
        let root_position = statements
            .iter()
            .position(|statement| statement.contains("PARTITION BY RANGE"))
            .unwrap();
        assert!(
            statements
                .iter()
                .skip(root_position + 1)
                .filter(|statement| statement.contains("PARTITION OF"))
                .count()
                == 2
        );
    }

    #[test]
    fn nextval_text_without_sequence_dependency_is_not_classified_as_serial() {
        let mut catalog = key_catalog(Vec::new());
        let column = catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::Column)
            .unwrap();
        column.attributes.insert(
            "default".into(),
            serde_json::json!("'literal nextval('::text"),
        );
        column
            .attributes
            .insert("sequence_default_oid".into(), serde_json::Value::Null);

        assert!(postgres_sequences(&catalog).unwrap().is_empty());
        let statements = pre_data_statements(&catalog).unwrap();
        let table = statements
            .iter()
            .find(|statement| statement.starts_with("CREATE TABLE"))
            .unwrap();
        assert!(table.contains("DEFAULT 'literal nextval('::text"));
    }

    #[test]
    fn ordinary_index_is_typed_post_data_operation() {
        let source_catalog = key_catalog(vec![
            ordinary_index("index:ordinary"),
            CatalogObject {
                id: "constraint:key".into(),
                kind: CatalogObjectKind::PrimaryKey,
                name: Identifier::new("accounts_pkey").unwrap(),
                definition: b"PRIMARY KEY (id)".to_vec(),
                attributes: BTreeMap::from([
                    ("table_oid".into(), serde_json::json!("relation:1")),
                    ("validated".into(), serde_json::json!(true)),
                    ("columns".into(), serde_json::json!(["id"])),
                ]),
            },
        ]);
        let indexes = postgres_post_data_indexes(&source_catalog).unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].catalog_object_id, "index:ordinary");
        assert_eq!(
            ordinary_index_create_statement(&indexes[0]),
            "CREATE INDEX \"accounts_id_idx\" ON \"public\".\"accounts\" USING btree (\"id\")"
        );

        let source = CatalogSnapshot {
            endpoint_identity: "source".into(),
            server_version: "17".into(),
            server_version_num: 170000,
            catalog: source_catalog,
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "tls-source".into(),
        };
        let mut target = snapshot("target", false);
        target.catalog.format_version = CATALOG_FORMAT_VERSION;
        let reviewed = build_plan(&source, &target).unwrap();
        reviewed.validate().unwrap();
        let create_index = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.parameters.contains_key("postgres_index"))
            .unwrap();
        assert_eq!(create_index.kind, OperationKind::CreateIndex);
        let copy = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CopyTable)
            .unwrap();
        assert_eq!(create_index.dependencies, vec![copy.id.clone()]);
        let planned: PostgresIndex =
            serde_json::from_value(create_index.parameters["postgres_index"].clone()).unwrap();
        assert_eq!(planned, indexes[0]);
    }

    #[test]
    fn ordinary_index_rejects_non_default_forms() {
        for (attribute, value) in [
            ("unique", serde_json::json!(true)),
            ("access_method", serde_json::json!("hash")),
            ("predicate", serde_json::json!("id > 0")),
            ("has_expressions", serde_json::json!(true)),
            ("included_columns", serde_json::json!(["payload"])),
            ("options", serde_json::json!([1])),
            ("reloptions", serde_json::json!(["fillfactor=80"])),
            ("tablespace", serde_json::json!("fast")),
            ("clustered", serde_json::json!(true)),
            ("replica_identity", serde_json::json!(true)),
            ("nulls_not_distinct", serde_json::json!(true)),
            (
                "opclasses",
                serde_json::json!([{"schema":"public","name":"custom_ops","default":false}]),
            ),
            ("collations_default", serde_json::json!([false])),
        ] {
            let mut index = ordinary_index("index:bad");
            index.attributes.insert(attribute.into(), value);
            assert!(
                ordinary_index_columns(&index, Some(&key_catalog(Vec::new()).namespaces[0]))
                    .is_err()
            );
        }
    }

    #[test]
    fn standalone_unique_index_is_a_typed_pre_data_operation_and_persisted_key() {
        let source_catalog = key_catalog(vec![standalone_index("index:9")]);
        let source = CatalogSnapshot {
            endpoint_identity: "source".into(),
            server_version: "17".into(),
            server_version_num: 170000,
            catalog: source_catalog,
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "tls-source".into(),
        };
        let mut target = snapshot("target", false);
        target.catalog.format_version = CATALOG_FORMAT_VERSION;
        let reviewed = build_plan(&source, &target).unwrap();
        reviewed.validate().unwrap();
        let create_index = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CreateIndex)
            .unwrap();
        assert!(create_index.parameters.contains_key("catalog_object"));
        let copy = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CopyTable)
            .unwrap();
        assert!(copy.dependencies.contains(&create_index.id));
        let key: PostgresResumableKey =
            serde_json::from_value(copy.parameters["resumable_key"].clone()).unwrap();
        assert_eq!(key.catalog_object_id, "index:9");
        assert_eq!(key.kind, "standalone_unique_index");
        assert_eq!(key.columns, vec![Identifier::new("id").unwrap()]);
    }

    #[test]
    fn resumable_key_ranking_prefers_primary_key_and_rejects_index_variants() {
        let primary = CatalogObject {
            id: "constraint:20".into(),
            kind: CatalogObjectKind::PrimaryKey,
            name: Identifier::new("accounts_pkey").unwrap(),
            definition: b"PRIMARY KEY (id)".to_vec(),
            attributes: BTreeMap::from([
                ("table_oid".into(), serde_json::json!("relation:1")),
                ("validated".into(), serde_json::json!(true)),
                ("columns".into(), serde_json::json!(["id"])),
            ]),
        };
        let catalog = key_catalog(vec![standalone_index("index:1"), primary]);
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("accounts").unwrap(),
        };
        assert_eq!(
            select_resumable_key(&catalog, &table)
                .unwrap()
                .catalog_object_id,
            "constraint:20"
        );

        for (attribute, value) in [
            ("unique", serde_json::json!(false)),
            ("access_method", serde_json::json!("hash")),
            ("predicate", serde_json::json!("id > 0")),
            ("has_expressions", serde_json::json!(true)),
            ("included_columns", serde_json::json!(["payload"])),
            ("options", serde_json::json!([1])),
            (
                "opclasses",
                serde_json::json!([{"schema":"public","name":"custom_ops","default":false}]),
            ),
        ] {
            let mut index = standalone_index("index:bad");
            index.attributes.insert(attribute.into(), value);
            assert!(standalone_unique_index_columns(
                &index,
                Some(&key_catalog(Vec::new()).namespaces[0])
            )
            .is_err());
        }
    }

    #[test]
    fn programmable_catalog_requires_canonical_ast_and_sorted_authoritative_dependencies() {
        let ast = PostgresDurableAst::View(Box::new(
            parse_postgres_create_view(
                "CREATE VIEW public.active_accounts AS SELECT id FROM public.accounts",
            )
            .unwrap(),
        ));
        let mut catalog = snapshot("source", false).catalog;
        catalog.namespaces[0].objects.push(CatalogObject {
            id: "relation:20".into(),
            kind: CatalogObjectKind::View,
            name: Identifier::new("active_accounts").unwrap(),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                (
                    "postgres_durable_ast".into(),
                    serde_json::Value::String(ast.canonical_json().unwrap()),
                ),
                (
                    "postgres_authoritative_identity".into(),
                    serde_json::to_value(PostgresProgrammableIdentity::Relation {
                        namespace: Identifier::new("public").unwrap(),
                        name: Identifier::new("active_accounts").unwrap(),
                    })
                    .unwrap(),
                ),
                (
                    "postgres_authoritative_dependencies".into(),
                    serde_json::to_value([PostgresProgrammableIdentity::Relation {
                        namespace: Identifier::new("public").unwrap(),
                        name: Identifier::new("accounts").unwrap(),
                    }])
                    .unwrap(),
                ),
            ]),
        });

        let objects = postgres_programmable_objects(&catalog).unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].authoritative_dependencies,
            [PostgresProgrammableIdentity::Relation {
                namespace: Identifier::new("public").unwrap(),
                name: Identifier::new("accounts").unwrap(),
            }]
        );

        catalog.namespaces[0].objects[0].attributes.insert(
            "postgres_authoritative_dependencies".into(),
            serde_json::to_value([
                PostgresProgrammableIdentity::Type {
                    namespace: Identifier::new("pg_catalog").unwrap(),
                    name: Identifier::new("int8").unwrap(),
                },
                PostgresProgrammableIdentity::Relation {
                    namespace: Identifier::new("public").unwrap(),
                    name: Identifier::new("accounts").unwrap(),
                },
            ])
            .unwrap(),
        );
        assert!(postgres_programmable_objects(&catalog).is_err());
    }

    #[test]
    fn typed_programmable_identities_do_not_collide_on_dotted_identifiers() {
        let left = PostgresProgrammableIdentity::Relation {
            namespace: Identifier::new("a.b").unwrap(),
            name: Identifier::new("c").unwrap(),
        };
        let right = PostgresProgrammableIdentity::Relation {
            namespace: Identifier::new("a").unwrap(),
            name: Identifier::new("b.c").unwrap(),
        };
        assert_ne!(left, right);
        assert_ne!(
            serde_json::to_value(&left).unwrap(),
            serde_json::to_value(&right).unwrap()
        );
        assert_eq!(BTreeSet::from([left, right]).len(), 2);
    }

    #[test]
    fn view_column_acl_is_not_supported() {
        assert!(view_security_is_supported("p", true, true));
        assert!(!view_security_is_supported("p", true, false));
        let unsupported = unsupported_view_security("relation:7", "p", true, false).unwrap();
        assert_eq!(unsupported.object_kind, "view_column_acl");
        assert!(unsupported.required_semantics);
    }

    #[test]
    fn bulk_copy_sql_quotes_hostile_identifiers_and_uses_one_binary_stream() {
        let table = QualifiedTable {
            namespace: Identifier::new("tenant.schema").unwrap(),
            name: Identifier::new("order\"items").unwrap(),
        };
        let batch = RowBatch::new(
            vec![ColumnMeta {
                name: Identifier::new("value,column").unwrap(),
                ordinal: 1,
                vendor_type: "text".into(),
                nullable: false,
                collation: None,
                precision: None,
                scale: None,
                timezone_semantics: None,
            }],
            1,
            1,
        );
        assert_eq!(
            bulk_copy_sql(&table, &batch),
            "COPY \"tenant.schema\".\"order\"\"items\" (\"value,column\") FROM STDIN (FORMAT binary)"
        );
        assert_eq!(
            bulk_type_probe_sql(&table, &batch),
            "SELECT \"value,column\" FROM \"tenant.schema\".\"order\"\"items\" WHERE FALSE"
        );
    }

    #[test]
    fn bulk_copy_rejects_a_batch_without_columns_before_opening_a_stream() {
        let batch = RowBatch::new(Vec::new(), 1, 1);
        assert!(matches!(
            validate_bulk_batch(&batch),
            Err(ConnectionError::InvalidRequest(message))
                if message == "bulk-write batch must contain columns"
        ));
    }

    #[test]
    fn write_policy_is_exact_and_persisted_in_copy_operation() {
        let mut catalog = key_catalog(Vec::new());
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("accounts").unwrap(),
        };
        assert_eq!(
            postgres_write_policy(&catalog, &table).unwrap(),
            PostgresWritePolicy::BinaryCopyWithInsertFallbackV1
        );
        catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::Column)
            .unwrap()
            .attributes
            .insert("identity".into(), serde_json::json!("a"));
        assert_eq!(
            postgres_write_policy(&catalog, &table).unwrap(),
            PostgresWritePolicy::PlainInsertIdentityAlwaysV1
        );
        catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::Column)
            .unwrap()
            .attributes
            .insert("identity".into(), serde_json::json!(""));

        let source = CatalogSnapshot {
            endpoint_identity: "source".into(),
            server_version: "17".into(),
            server_version_num: 170000,
            catalog,
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "tls-source".into(),
        };
        let mut target = snapshot("target", false);
        target.catalog.format_version = CATALOG_FORMAT_VERSION;
        let reviewed = build_plan(&source, &target).unwrap();
        let copy = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CopyTable)
            .unwrap();
        assert_eq!(
            serde_json::from_value::<PostgresWritePolicy>(
                copy.parameters["postgres_write_policy"].clone()
            )
            .unwrap(),
            PostgresWritePolicy::BinaryCopyWithInsertFallbackV1
        );
    }

    #[test]
    fn concurrent_session_registry_deregisters_each_session_by_raii() {
        let registry = Arc::new(SessionRegistry::default());
        let first = registry.register("first").unwrap();
        let second = registry.register("second").unwrap();
        assert_eq!(registry.sessions().unwrap(), ["first", "second"]);
        drop(first);
        assert_eq!(registry.sessions().unwrap(), ["second"]);
        drop(second);
        assert!(registry.sessions().unwrap().is_empty());
    }

    #[test]
    fn imported_snapshot_statement_accepts_only_server_identifier_grammar() {
        assert_eq!(
            import_snapshot_statement("00000003-0000001B-1").unwrap(),
            "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"
        );
        for invalid in ["", "snapshot' ; SELECT 1; --", "path/segment", "space id"] {
            assert!(import_snapshot_statement(invalid).is_err());
        }
    }

    #[test]
    fn imported_snapshot_attestation_requires_exact_current_snapshot_text() {
        let expected = SnapshotToken {
            endpoint_identity: "postgres://127.0.0.1:5432/app?user=reader".into(),
            database_identity: "app".into(),
            snapshot_id: "10:20:11,12".into(),
            consistency_mode: "postgres_repeatable_read_read_only".into(),
            server_version: "17.5".into(),
            lifecycle_id: "primary".into(),
        };
        assert!(imported_snapshot_is_exact(
            &expected,
            &expected.endpoint_identity,
            &expected.database_identity,
            &expected.server_version,
            &expected.snapshot_id,
            true,
        ));
        assert!(!imported_snapshot_is_exact(
            &expected,
            &expected.endpoint_identity,
            &expected.database_identity,
            &expected.server_version,
            "10:21:11,12",
            true,
        ));
        assert!(!imported_snapshot_is_exact(
            &expected,
            &expected.endpoint_identity,
            &expected.database_identity,
            &expected.server_version,
            &expected.snapshot_id,
            false,
        ));
    }

    #[test]
    fn outage_byte_inventory_excludes_partition_roots_and_deduplicates_relations() {
        let relation =
            |id: &str, name: &str, kind: CatalogObjectKind, relkind: &str| CatalogObject {
                id: id.into(),
                kind,
                name: Identifier::new(name).unwrap(),
                definition: Vec::new(),
                attributes: BTreeMap::from([("relkind".into(), serde_json::json!(relkind))]),
            };
        let catalog = VendorCatalog {
            format_version: CATALOG_FORMAT_VERSION,
            dialect: "postgresql".into(),
            server_version: "17.0".into(),
            database: Identifier::new("app").unwrap(),
            namespaces: vec![CatalogNamespace {
                id: "namespace:1".into(),
                name: Identifier::new("public").unwrap(),
                owner: None,
                charset: None,
                collation: None,
                objects: vec![
                    relation("relation:10", "standalone", CatalogObjectKind::Table, "r"),
                    relation("relation:20", "root", CatalogObjectKind::Table, "p"),
                    relation("relation:21", "leaf", CatalogObjectKind::Partition, "r"),
                ],
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };

        assert_eq!(physical_business_relation_oids(&catalog).unwrap(), [10, 21]);
    }
}
