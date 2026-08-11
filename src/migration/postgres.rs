//! Read-only PostgreSQL plan adapter for the enterprise migration spike.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use postgres::{CancelToken, Client, Config, IsolationLevel};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::write_json_new;
use super::canonical::CANONICAL_ENCODING_VERSION;
use super::connection::{
    CancellationToken, Capability, CapabilitySet, ConnectionError, ConnectionResult,
    ControlSession, KeysetPage, ReadOnlyEvidence, ReadSession, SnapshotToken,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession, WriteSession,
};
use super::model::{
    CatalogDependency, CatalogNamespace, CatalogObject, CatalogObjectKind, ColumnMeta, DbValue,
    Identifier, QualifiedTable, RowBatch, ValueFormat, VendorCatalog,
};
use super::plan::{
    MigrationPlan, OperationKind, PlanOperation, ReviewedPlan, UnsupportedObject,
    UnsupportedObjectReport, PLAN_SCHEMA_VERSION,
};

const CATALOG_FORMAT_VERSION: u32 = 1;
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_BATCH_BYTES: usize = 64 * 1024 * 1024;
static SNAPSHOT_LIFECYCLE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PostgresConsistencyMode {
    ConsistentSnapshot,
    WriteFence,
}

impl PostgresConsistencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConsistentSnapshot => "consistent-snapshot",
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
    #[error("credential environment variable {0} is not set or is not Unicode")]
    MissingCredential(String),
    #[error("cannot read configured CA certificate")]
    ReadCa(#[source] std::io::Error),
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

    fn validate(&self) -> Result<(), PostgresPlanError> {
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
        Ok(())
    }

    fn connect(&self) -> Result<Client, PostgresPlanError> {
        let password = std::env::var(&self.credential_env)
            .map_err(|_| PostgresPlanError::MissingCredential(self.credential_env.clone()))?;
        let mut config = Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(password)
            .application_name("sql-splitter-migration-plan")
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
        if self.tls.insecure {
            tls.danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }
        Ok(MakeTlsConnector::new(tls.build()?))
    }
}

struct PendingSnapshot {
    client: Client,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    catalog: VendorCatalog,
    unsupported: UnsupportedObjectReport,
}

/// PostgreSQL source factory that transfers ownership of one live snapshot session.
pub struct PostgresSourceFactory {
    config: PostgresEndpointConfig,
    pending: Mutex<Option<PendingSnapshot>>,
    active_cancel: Mutex<Option<CancelToken>>,
}

impl PostgresSourceFactory {
    pub fn new(config: PostgresEndpointConfig) -> Self {
        Self {
            config,
            pending: Mutex::new(None),
            active_cancel: Mutex::new(None),
        }
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
}

impl SourceConnectionFactory for PostgresSourceFactory {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::from_entries([
            ("consistent_snapshot", Capability::Supported),
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
        let mut client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
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
        let (catalog, unsupported) =
            extract_catalog(&mut client, &token.database_identity, &token.server_version)
                .map_err(|error| ConnectionError::Database(error.to_string()))?;
        let cancel_token = client.cancel_token();
        *self
            .active_cancel
            .lock()
            .map_err(|_| ConnectionError::Database("control token lock is poisoned".into()))? =
            Some(cancel_token);
        *pending = Some(PendingSnapshot {
            client,
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
            token: pending_snapshot.token,
            evidence: pending_snapshot.evidence,
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            metadata_cache: HashMap::new(),
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        let token = self
            .active_cancel
            .lock()
            .map_err(|_| ConnectionError::Database("control token lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                ConnectionError::InvalidRequest(
                    "capture a PostgreSQL snapshot before opening its control session".into(),
                )
            })?;
        Ok(Box::new(PostgresControlSession {
            token,
            config: self.config.clone(),
        }))
    }
}

struct PostgresControlSession {
    token: CancelToken,
    config: PostgresEndpointConfig,
}

impl ControlSession for PostgresControlSession {
    fn cancel_active_statement(&mut self) -> ConnectionResult<()> {
        let connector = self
            .config
            .tls_connector()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        self.token.cancel_query(connector).map_err(database_error)
    }
}

/// PostgreSQL target factory for transactional same-dialect chunk writes.
pub struct PostgresTargetFactory {
    config: PostgresEndpointConfig,
    active_cancel: Mutex<Option<CancelToken>>,
}

impl PostgresTargetFactory {
    pub fn new(config: PostgresEndpointConfig) -> Self {
        Self {
            config,
            active_cancel: Mutex::new(None),
        }
    }

    fn remember_cancel_token(&self, client: &Client) -> ConnectionResult<()> {
        *self
            .active_cancel
            .lock()
            .map_err(|_| ConnectionError::Database("control token lock is poisoned".into()))? =
            Some(client.cancel_token());
        Ok(())
    }

    /// Recheck that the target is empty and owned by the configured role.
    pub fn assert_empty_and_owned(&self) -> ConnectionResult<()> {
        let mut client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        assert_target_empty_and_owned(&mut client)
    }

    /// Create supported namespaces and tables in one atomic PostgreSQL transaction.
    pub fn create_pre_data_schema(&self, catalog: &VendorCatalog) -> ConnectionResult<()> {
        if catalog.dialect != "postgresql" {
            return Err(ConnectionError::InvalidRequest(
                "PostgreSQL target requires a PostgreSQL vendor catalog".into(),
            ));
        }
        let statements = pre_data_statements(catalog)?;
        let mut client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        let mut transaction = client.transaction().map_err(database_error)?;
        assert_target_empty_and_owned(&mut transaction)?;
        for statement in statements {
            transaction
                .batch_execute(&statement)
                .map_err(database_error)?;
        }
        transaction.commit().map_err(database_error)
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
    if catalog.namespaces.iter().any(|namespace| {
        namespace
            .objects
            .iter()
            .any(|object| object.kind == CatalogObjectKind::Sequence)
    }) {
        return Err(ConnectionError::InvalidRequest(
            "sequence creation, ownership, and state restoration are not implemented".into(),
        ));
    }
    let mut statements = Vec::new();
    for namespace in &catalog.namespaces {
        if namespace.name.as_str() != "public" {
            statements.push(format!(
                "CREATE SCHEMA {}",
                quote_identifier(&namespace.name)
            ));
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
            if table
                .attributes
                .get("relkind")
                .and_then(serde_json::Value::as_str)
                != Some("r")
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "table {}.{} is not an ordinary PostgreSQL table",
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
            let mut definitions = table_column_definitions(namespace, table)?;
            definitions.extend(table_constraint_definitions(namespace, table)?);
            if definitions.is_empty() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "table {}.{} has no catalog columns",
                    namespace.name, table.name
                )));
            }
            statements.push(format!(
                "{create} {}.{} ({})",
                quote_identifier(&namespace.name),
                quote_identifier(&table.name),
                definitions.join(", ")
            ));
        }
    }
    Ok(statements)
}

fn table_column_definitions(
    namespace: &CatalogNamespace,
    table: &CatalogObject,
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
            if column
                .attributes
                .get("generated")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| !value.is_empty())
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "column {} is generated and cannot be created by the current executor",
                    column.name
                )));
            }
            if let Some(collation) = column
                .attributes
                .get("collation")
                .and_then(serde_json::Value::as_str)
            {
                definition.push_str(" COLLATE ");
                definition
                    .push_str(&quote_identifier(&Identifier::new(collation).map_err(
                        |error| ConnectionError::InvalidRequest(error.to_string()),
                    )?));
            }
            let identity = column
                .attributes
                .get("identity")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match identity {
                "a" => definition.push_str(" GENERATED ALWAYS AS IDENTITY"),
                "d" => definition.push_str(" GENERATED BY DEFAULT AS IDENTITY"),
                "" => {
                    if let Some(default) = column
                        .attributes
                        .get("default")
                        .and_then(serde_json::Value::as_str)
                    {
                        if default.contains("nextval(") {
                            return Err(ConnectionError::InvalidRequest(format!(
                                "column {} uses an unsupported serial sequence default",
                                column.name
                            )));
                        }
                        definition.push_str(" DEFAULT ");
                        definition.push_str(default);
                    }
                }
                value => {
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
        ])
    }

    fn open_writer(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn WriteSession>> {
        let client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        self.remember_cancel_token(&client)?;
        Ok(Box::new(PostgresWriter {
            client,
            cancellation,
            transaction_open: false,
        }))
    }

    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>> {
        let mut client = self
            .config
            .connect()
            .map_err(|error| ConnectionError::Database(error.to_string()))?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(database_error)?;
        self.remember_cancel_token(&client)?;
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
                token,
                evidence: ReadOnlyEvidence {
                    server_enforced: true,
                    description: "target verification transaction is read-only".into(),
                },
                cancellation,
                max_batch_rows: self.config.max_batch_rows,
                max_batch_bytes: self.config.max_batch_bytes,
                metadata_cache: HashMap::new(),
            },
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        let token = self
            .active_cancel
            .lock()
            .map_err(|_| ConnectionError::Database("control token lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                ConnectionError::InvalidRequest(
                    "open a PostgreSQL target session before its control session".into(),
                )
            })?;
        Ok(Box::new(PostgresControlSession {
            token,
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
}

struct PostgresWriter {
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
            bytes: value.clone(),
        })),
        DbValue::Json(value) if *ty == Type::JSONB => {
            let mut bytes = Vec::with_capacity(value.len() + 1);
            bytes.push(1);
            bytes.extend_from_slice(value);
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
    client: Client,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    cancellation: CancellationToken,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    metadata_cache: HashMap<(QualifiedTable, Vec<Identifier>), Vec<ColumnMeta>>,
}

impl ReadSession for PostgresSnapshotReader {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence {
        &self.evidence
    }

    fn snapshot(&self) -> &SnapshotToken {
        &self.token
    }

    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        validate_page(request, self.max_batch_rows)?;
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
            "{}.{}",
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
            batch
                .try_push(values, encoded_bytes)
                .map_err(|error| ConnectionError::BatchLimit(error.to_string()))?;
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
        (DbValue::Text(value), "pg_catalog.text" | "pg_catalog.varchar") => {
            Ok(Box::new(value.clone()))
        }
        (DbValue::Bytes(value), "pg_catalog.bytea") => Ok(Box::new(value.clone())),
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
        Type::JSON => Ok(DbValue::Json(bytes)),
        Type::JSONB if bytes.first() == Some(&1) => Ok(DbValue::Json(bytes[1..].to_vec())),
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
    pub tls_insecure: bool,
}

pub fn inspect_endpoint(
    config: &PostgresEndpointConfig,
) -> Result<CatalogSnapshot, PostgresPlanError> {
    let mut client = config.connect()?;
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
        tls_insecure: config.tls.insecure,
    })
}

fn extract_catalog(
    transaction: &mut impl postgres::GenericClient,
    database: &str,
    server_version: &str,
) -> Result<(VendorCatalog, UnsupportedObjectReport), PostgresPlanError> {
    let database_settings = transaction.query_one(
        "SELECT pg_encoding_to_char(encoding), datcollate, datctype FROM pg_database WHERE datname = current_database()",
        &[],
    )?;
    let server_encoding: String = database_settings.get(0);
    let database_collation: String = database_settings.get(1);
    let database_ctype: String = database_settings.get(2);
    let namespace_rows = transaction.query(
        "SELECT n.oid::text, n.nspname, pg_get_userbyid(n.nspowner) FROM pg_namespace n WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname",
        &[],
    )?;
    let mut namespaces = BTreeMap::new();
    for row in namespace_rows {
        let name: String = row.get(1);
        namespaces.insert(
            name.clone(),
            CatalogNamespace {
                name: Identifier::new(name)?,
                owner: Some(row.get(2)),
                charset: Some(server_encoding.clone()),
                collation: Some(database_collation.clone()),
                objects: Vec::new(),
            },
        );
    }

    let relation_rows = transaction.query(
        "SELECT c.oid::text, n.nspname, c.relname, c.relkind::text, pg_get_userbyid(c.relowner), c.relpersistence::text, c.relrowsecurity, CASE WHEN c.relkind IN ('v','m') THEN pg_get_viewdef(c.oid, true) ELSE NULL END, seq.seqstart::text, seq.seqincrement::text, seq.seqmax::text, seq.seqmin::text, seq.seqcache::text, seq.seqcycle, CASE WHEN seq.seqtypid IS NULL THEN NULL ELSE pg_catalog.format_type(seq.seqtypid, NULL) END FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_sequence seq ON seq.seqrelid = c.oid WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p','S','v','m') ORDER BY n.nspname, c.relname, c.relkind",
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
        let object_kind = match kind.as_str() {
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
        }
        if kind == "p" || kind == "m" || kind == "v" || relrowsecurity {
            unsupported.push(UnsupportedObject {
                object_id: id.clone(),
                object_kind: if relrowsecurity {
                    "row_security"
                } else if kind == "v" {
                    "view"
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

    let user_type_rows = transaction.query(
        "SELECT t.oid::text, n.nspname, t.typname, t.typtype::text, pg_catalog.format_type(t.oid, NULL) FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND t.typtype IN ('c','d','e','r') AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid) ORDER BY n.nspname, t.typname",
        &[],
    )?;
    for row in user_type_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let type_kind: String = row.get(3);
        let definition: String = row.get(4);
        unsupported.push(UnsupportedObject {
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
        "SELECT e.oid::text, n.nspname, e.extname, e.extversion FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname <> 'plpgsql' ORDER BY e.extname",
        &[],
    )?;
    for row in extension_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let version: String = row.get(3);
        unsupported.push(UnsupportedObject {
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
        "SELECT (a.attrelid::text || ':' || a.attnum::text), n.nspname, a.attname, 'column', pg_catalog.format_type(a.atttypid, a.atttypmod), jsonb_build_object('table_oid', c.oid::text, 'table', c.relname, 'ordinal', a.attnum, 'nullable', NOT a.attnotnull, 'default', pg_get_expr(ad.adbin, ad.adrelid), 'identity', a.attidentity::text, 'generated', a.attgenerated::text, 'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collname END, 'type_schema', typen.nspname, 'type_name', typ.typname)::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_type typ ON typ.oid = a.atttypid JOIN pg_namespace typen ON typen.oid = typ.typnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum LEFT JOIN pg_collation coll ON coll.oid = a.attcollation WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY n.nspname, c.relname, a.attnum",
        CatalogObjectKind::Column,
    )?;
    for object in namespaces
        .values()
        .flat_map(|namespace| namespace.objects.iter())
        .filter(|object| object.kind == CatalogObjectKind::Column)
    {
        if object
            .attributes
            .get("generated")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            unsupported.push(UnsupportedObject {
                object_id: object.id.clone(),
                object_kind: "generated_column".into(),
                reason: "generated-column DDL and value verification are not implemented".into(),
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
                object_id: object.id.clone(),
                object_kind: "user_defined_column_type".into(),
                reason: "user-defined PostgreSQL types are not reproduced by the executor".into(),
                required_semantics: true,
            });
        }
        let identity = object
            .attributes
            .get("identity")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if identity.is_empty()
            && object
                .attributes
                .get("default")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value.contains("nextval("))
        {
            unsupported.push(UnsupportedObject {
                object_id: object.id.clone(),
                object_kind: "serial_sequence_default".into(),
                reason: "serial sequence ownership and state are not implemented".into(),
                required_semantics: true,
            });
        }
    }
    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT con.oid::text, n.nspname, con.conname, 'constraint', pg_get_constraintdef(con.oid, true), jsonb_build_object('table_oid', con.conrelid::text, 'type', con.contype::text, 'validated', con.convalidated, 'deferrable', con.condeferrable, 'deferred', con.condeferred, 'referenced_table_oid', NULLIF(con.confrelid, 0)::text, 'columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(con.conkey) WITH ORDINALITY keys(attnum, ordinality) JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = keys.attnum), '[]'::jsonb), 'referenced_columns', COALESCE((SELECT jsonb_agg(att.attname ORDER BY keys.ordinality) FROM unnest(con.confkey) WITH ORDINALITY keys(attnum, ordinality) JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = keys.attnum), '[]'::jsonb), 'match_type', con.confmatchtype::text, 'update_action', con.confupdtype::text, 'delete_action', con.confdeltype::text)::text FROM pg_constraint con JOIN pg_namespace n ON n.oid = con.connamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, con.conname, con.oid",
        CatalogObjectKind::CheckConstraint,
    )?;
    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT i.indexrelid::text, n.nspname, ci.relname, 'index', pg_get_indexdef(i.indexrelid), jsonb_build_object('table_oid', i.indrelid::text, 'unique', i.indisunique, 'primary', i.indisprimary, 'valid', i.indisvalid, 'ready', i.indisready)::text FROM pg_index i JOIN pg_class ci ON ci.oid = i.indexrelid JOIN pg_class ct ON ct.oid = i.indrelid JOIN pg_namespace n ON n.oid = ct.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, ci.relname",
        CatalogObjectKind::Index,
    )?;

    let trigger_rows = transaction.query(
        "SELECT t.oid::text, n.nspname, t.tgname, pg_get_triggerdef(t.oid, true), c.oid::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE NOT t.tgisinternal AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, t.tgname, t.oid",
        &[],
    )?;
    for row in trigger_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let definition: String = row.get(3);
        let table_oid: String = row.get(4);
        unsupported.push(UnsupportedObject {
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
        "SELECT p.oid::text, n.nspname, p.proname, CASE WHEN p.prokind IN ('f','p') THEN pg_get_functiondef(p.oid) ELSE p.prokind::text || ' ' || pg_get_function_identity_arguments(p.oid) END FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, p.proname, p.oid",
        &[],
    )?;
    for row in routine_rows {
        let id: String = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let definition: String = row.get(3);
        unsupported.push(UnsupportedObject {
            object_id: id.clone(),
            object_kind: "routine".into(),
            reason: "routine execution semantics are not implemented".into(),
            required_semantics: true,
        });
        push_object(
            &mut namespaces,
            &namespace,
            CatalogObject {
                id,
                kind: CatalogObjectKind::Routine,
                name: Identifier::new(name)?,
                definition: definition.into_bytes(),
                attributes: BTreeMap::new(),
            },
        )?;
    }

    let policy_rows = transaction.query(
        "SELECT pol.oid::text, n.nspname, pol.polname FROM pg_policy pol JOIN pg_class c ON c.oid = pol.polrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, pol.polname, pol.oid",
        &[],
    )?;
    for row in policy_rows {
        unsupported.push(UnsupportedObject {
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
    Ok((
        VendorCatalog {
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
        },
        UnsupportedObjectReport {
            objects: unsupported,
        },
    ))
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
    let mut operations = Vec::new();
    let mut table_names = BTreeSet::new();
    let mut deferred_objects = Vec::new();
    for namespace in &source.catalog.namespaces {
        for object in &namespace.objects {
            if object.kind == CatalogObjectKind::Table {
                table_names.insert(QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: object.name.clone(),
                });
            } else if matches!(
                object.kind,
                CatalogObjectKind::Sequence | CatalogObjectKind::View
            ) {
                deferred_objects.push((namespace.name.clone(), object.clone()));
            }
        }
    }
    for table in table_names {
        let parameters = table_parameters(&source.catalog, &table)?;
        let create = PlanOperation::new(
            OperationKind::CreateTable,
            Some(table.clone()),
            Vec::new(),
            parameters,
        )?;
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            vec![create.id.clone()],
            BTreeMap::new(),
        )?;
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table),
            vec![copy.id.clone()],
            BTreeMap::new(),
        )?;
        operations.extend([create, copy, verify]);
    }
    for (namespace, object) in deferred_objects {
        let kind = match object.kind {
            CatalogObjectKind::Sequence => OperationKind::CreateSequence,
            CatalogObjectKind::View => OperationKind::CreateView,
            _ => continue,
        };
        operations.push(PlanOperation::new(
            kind,
            Some(QualifiedTable {
                namespace,
                name: object.name.clone(),
            }),
            Vec::new(),
            BTreeMap::from([("catalog_object".into(), serde_json::to_value(object)?)]),
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
    let target_object_count: usize = target
        .catalog
        .namespaces
        .iter()
        .map(|namespace| namespace.objects.len())
        .sum();
    if target_object_count > 0 {
        unsupported.objects.push(UnsupportedObject {
            object_id: "target-catalog-not-empty".into(),
            object_kind: "target_precondition".into(),
            reason: format!("target catalog contains {target_object_count} user objects"),
            required_semantics: true,
        });
    }
    if source.endpoint_identity == target.endpoint_identity {
        unsupported.objects.push(UnsupportedObject {
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
        migration_id: format!("pg-{}", &catalog_fingerprint(&source.catalog)?[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity.clone(),
        target_endpoint_identity: target.endpoint_identity.clone(),
        source_catalog_fingerprint: catalog_fingerprint(&source.catalog)?,
        target_catalog_fingerprint: catalog_fingerprint(&target.catalog)?,
        source_catalog: Some(source.catalog.clone()),
        target_catalog: Some(target.catalog.clone()),
        consistency_mode: consistency_mode.as_str().into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "postgresql_same_dialect_exact".into(),
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "repeatable_read_read_only".into(),
            ),
            (
                "source_tls".into(),
                if source.tls_insecure {
                    "insecure_explicit"
                } else {
                    "hostname_verified"
                }
                .into(),
            ),
            (
                "target_tls".into(),
                if target.tls_insecure {
                    "insecure_explicit"
                } else {
                    "hostname_verified"
                }
                .into(),
            ),
        ]),
        operations,
        unsupported_objects: unsupported,
    })
    .map_err(PostgresPlanError::from)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(endpoint: &str, with_table: bool) -> CatalogSnapshot {
        let mut objects = Vec::new();
        if with_table {
            objects.push(CatalogObject {
                id: "table-1".into(),
                kind: CatalogObjectKind::Table,
                name: Identifier::new("accounts").unwrap(),
                definition: Vec::new(),
                attributes: BTreeMap::new(),
            });
            objects.push(CatalogObject {
                id: "table-1:1".into(),
                kind: CatalogObjectKind::Column,
                name: Identifier::new("accounts.id").unwrap(),
                definition: b"bigint".to_vec(),
                attributes: BTreeMap::from([(
                    "table_oid".into(),
                    serde_json::Value::String("table-1".into()),
                )]),
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
            tls_insecure: false,
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
}
