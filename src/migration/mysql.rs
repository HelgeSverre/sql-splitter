//! MySQL adapter contracts for the enterprise migration spike.
//!
//! The adapter keeps one MySQL consistent-snapshot session alive from catalog
//! capture through source paging. It does not treat a GTID observation as a
//! snapshot identifier or as DDL exclusion evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Params, Row, SslOpts, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::{read_json, write_json_new, ArtifactError};
use super::canonical::{canonicalize_json, CANONICAL_ENCODING_VERSION};
use super::connection::{
    CancellationToken, Capability, CapabilitySet, ConnectionError, ConnectionResult,
    ControlSession, KeysetPage, ReadOnlyEvidence, ReadSession, SnapshotToken,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession, WriteSession,
};
use super::conversion::{
    cross_dialect_target_key_name, derive_mysql_to_postgres_column, ConversionDialect,
    CrossDialectKeyKind, CrossDialectResumableKey, CrossDialectTargetTableContract,
    CrossDialectTargetType, MySqlBinaryStorage, MySqlIntegerWidth, MySqlTargetEngine,
    MySqlTargetType, MySqlTextStorage, PostgresTargetPersistence, QualifiedIdentifier,
    RowConversionError, RowConversionPolicy, TableConversionPolicy, TargetCheckConstraint,
    TimestampBound, TimestampSemantics, MYSQL_DATETIME_RANGE, MYSQL_TIMESTAMP_RANGE,
    MYSQL_TIME_MAXIMUM_NANOS, MYSQL_TIME_MINIMUM_NANOS, ROW_TYPE_CONVERSION_SCHEMA_VERSION,
};
use super::model::{
    CatalogDependency, CatalogNamespace, CatalogObject, CatalogObjectKind, ColumnMeta, DbValue,
    Identifier, QualifiedTable, RowBatch, RowBatchError, VendorCatalog,
};
use super::mysql_profile::{
    MySqlDdlFreezeMechanism, MySqlDmlFreezeMechanism, MySqlExternalFreezeAssertion,
    MySqlExternalFreezeAttestation, MySqlFreezeAttestationStatus, MySqlFreezeProfileContract,
    MySqlFreezeProfileError, MySqlFreezeProfileKind, MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
};
use super::mysql_visibility::{
    MySqlAccountIdentity, MySqlAuthorizationContract, MySqlAuthorizationMapping,
    MySqlGrantInventory, MySqlGrantRecord, MySqlGrantTableColumn, MySqlMetadataVisibilityEvidence,
    MySqlOperationalAccountExclusion, MySqlOperationalAccountPurpose, MySqlProxyTarget,
    MySqlVisibilityError, MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
};
use super::plan::{
    AssessmentStatus, MigrationPlan, MySqlSnapshotEvidence, OperationKind, PlanError,
    PlanOperation, PlanPurpose, ReviewedPlan, UnsupportedObject, UnsupportedObjectCode,
    UnsupportedObjectReport, MYSQL_SAME_DIALECT_CONVERSION_POLICY, MYSQL_SESSION_CHARACTER_SET,
    MYSQL_SESSION_COLLATION, MYSQL_STRICT_SQL_MODE, PLAN_SCHEMA_VERSION,
};

pub const MYSQL_CATALOG_FORMAT_VERSION: u32 = 4;
pub const MYSQL_CONSISTENCY_SNAPSHOT: &str = "mysql-repeatable-read-consistent-snapshot";
const DEFAULT_PORT: u16 = 3306;
const DEFAULT_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_BATCH_BYTES: usize = 64 * 1024 * 1024;
static SNAPSHOT_LIFECYCLE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct MySqlLiveSessionIdentity {
    database: String,
    hostname: String,
    port: u16,
    server_version: String,
    server_uuid: String,
    authenticated_account: String,
    transaction_isolation: String,
    transaction_read_only: u8,
    session_time_zone: String,
    information_schema_stats_expiry: u64,
    gtid_executed_observation: String,
    connection_id: u32,
    lower_case_table_names: u8,
    session_sql_mode: String,
    character_set_client: String,
    character_set_connection: String,
    character_set_results: String,
    collation_connection: String,
}

type MySqlIndexRow = (
    String,
    String,
    u8,
    u64,
    Option<String>,
    Option<String>,
    Option<u64>,
    String,
    String,
    String,
    Option<String>,
    u8,
);
type MySqlTableRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u64>,
);
type MySqlConstraintKeyRow = (
    String,
    String,
    String,
    u64,
    Option<String>,
    Option<String>,
    Option<String>,
);
type MySqlReferenceRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);
type MySqlForeignKeyColumnRow = (u64, String, Option<String>, Option<String>, Option<String>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlEndpointConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub user: String,
    pub credential_env: String,
    /// Explicit server-administrator accounts excluded from business grant
    /// blockers when this configuration is used for metadata administration.
    #[serde(default)]
    pub operational_server_administrators: Vec<MySqlAccountIdentity>,
    #[serde(default)]
    pub tls: MySqlTlsConfig,
    #[serde(default = "default_timeout_seconds")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_batch_rows")]
    pub max_batch_rows: usize,
    #[serde(default = "default_batch_bytes")]
    pub max_batch_bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlTlsConfig {
    pub ca_certificate: Option<PathBuf>,
    pub client_identity_pkcs12: Option<PathBuf>,
    pub client_identity_password_env: Option<String>,
    #[serde(default)]
    pub insecure: bool,
}

#[derive(Debug, Error)]
pub enum MySqlPlanError {
    #[error("cannot read MySQL endpoint configuration")]
    ReadConfig(#[source] std::io::Error),
    #[error("invalid MySQL endpoint configuration")]
    ParseConfig(#[from] toml::de::Error),
    #[error("invalid MySQL endpoint configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("credential environment variable {0} is not set or is not Unicode")]
    MissingCredential(String),
    #[error("cannot inspect configured TLS file")]
    TlsFile(#[source] std::io::Error),
    #[error("MySQL operation failed: {0}")]
    Database(MySqlSafeError),
    #[error("invalid database identifier")]
    Identifier(#[from] super::model::IdentifierError),
    #[error("catalog serialization failed")]
    Serialize(#[from] serde_json::Error),
    #[error("plan construction failed")]
    Plan(#[from] PlanError),
    #[error("artifact publication failed")]
    Artifact(#[from] ArtifactError),
    #[error("invalid MySQL catalog: {0}")]
    InvalidCatalog(String),
    #[error("invalid MySQL freeze profile evidence")]
    FreezeProfile(#[from] MySqlFreezeProfileError),
    #[error("invalid MySQL metadata-visibility evidence")]
    MetadataVisibility(#[from] MySqlVisibilityError),
    #[error("cross-dialect conversion policy derivation failed")]
    Conversion(#[from] RowConversionError),
}

impl From<mysql::Error> for MySqlPlanError {
    fn from(error: mysql::Error) -> Self {
        Self::Database(MySqlSafeError::from(error))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MySqlSafeError {
    #[error("server error code {code}")]
    Server { code: u16 },
    #[error("transport error")]
    Transport,
    #[error("packet codec error")]
    Codec,
    #[error("driver error")]
    Driver,
    #[error("connection URL error")]
    Url,
    #[error("TLS error")]
    Tls,
    #[error("value decoding error")]
    ValueDecode,
    #[error("row decoding error")]
    RowDecode,
}

impl From<mysql::Error> for MySqlSafeError {
    fn from(error: mysql::Error) -> Self {
        match error {
            mysql::Error::MySqlError(error) => Self::Server { code: error.code },
            mysql::Error::IoError(_) => Self::Transport,
            mysql::Error::CodecError(_) => Self::Codec,
            mysql::Error::DriverError(_) => Self::Driver,
            mysql::Error::UrlError(_) => Self::Url,
            mysql::Error::TlsError(_) => Self::Tls,
            mysql::Error::FromValueError(_) => Self::ValueDecode,
            mysql::Error::FromRowError(_) => Self::RowDecode,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlCatalogBlocker {
    pub object_id: String,
    pub object_kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlCatalogSnapshot {
    pub endpoint_identity: String,
    pub database_identity: String,
    pub server_version: String,
    pub tls_binding: String,
    pub snapshot_evidence: MySqlSnapshotEvidence,
    pub catalog: VendorCatalog,
    pub blockers: Vec<MySqlCatalogBlocker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlMetadataVisibilityCapture {
    pub authoritative_catalog: VendorCatalog,
    pub authoritative_blockers: Vec<MySqlCatalogBlocker>,
    pub evidence: MySqlMetadataVisibilityEvidence,
}

pub fn mysql_catalog_visibility_is_complete(
    reader: &MySqlCatalogSnapshot,
    capture: &MySqlMetadataVisibilityCapture,
) -> Result<bool, MySqlPlanError> {
    let mut reader_catalog = reader.catalog.clone();
    let mut reader_blockers = reader.blockers.clone();
    remove_mysql_privilege_projection(&mut reader_catalog, &mut reader_blockers);
    Ok(reader_catalog == capture.authoritative_catalog
        && reader_blockers == capture.authoritative_blockers)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlResumableKey {
    pub index_name: Identifier,
    pub primary: bool,
    pub columns: Vec<Identifier>,
    pub column_types: Vec<String>,
    pub collations: Vec<Option<String>>,
    pub server_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlAutoIncrementState {
    pub table: QualifiedTable,
    pub column: Identifier,
    pub next_value: Option<u64>,
    pub stats_expiry: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlColumnType {
    Integer {
        name: String,
        unsigned: bool,
        display_width: Option<u32>,
    },
    Decimal {
        precision: u32,
        scale: u32,
        unsigned: bool,
    },
    Floating {
        name: String,
        precision: Option<u32>,
        scale: Option<u32>,
        unsigned: bool,
    },
    Bit {
        length: u32,
    },
    Temporal {
        name: String,
        fractional_precision: Option<u32>,
    },
    Year,
    Character {
        name: String,
        length: u32,
    },
    Binary {
        name: String,
        length: u32,
    },
    Text {
        name: String,
    },
    Blob {
        name: String,
    },
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlColumnDefinition {
    pub name: Identifier,
    pub ordinal: u64,
    pub data_type: MySqlColumnType,
    pub nullable: bool,
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlIndexDefinition {
    pub name: Identifier,
    pub primary: bool,
    pub unique: bool,
    pub constraint_backed: bool,
    pub columns: Vec<Identifier>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlTableDefinition {
    pub table: QualifiedTable,
    pub engine: String,
    pub character_set: String,
    pub collation: String,
    pub columns: Vec<MySqlColumnDefinition>,
    pub indexes: Vec<MySqlIndexDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlTableMapping {
    pub source: QualifiedTable,
    pub target: QualifiedTable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlForeignKeyAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlForeignKey {
    pub catalog_object_id: String,
    pub name: Identifier,
    pub table: QualifiedTable,
    pub columns: Vec<Identifier>,
    pub referenced_table: QualifiedTable,
    pub referenced_columns: Vec<Identifier>,
    pub referenced_constraint: Identifier,
    pub update_action: MySqlForeignKeyAction,
    pub delete_action: MySqlForeignKeyAction,
    pub enforced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlForeignKeyState {
    Absent,
    Exact,
    Different,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlTableState {
    Absent,
    Exact,
    Different,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlAutoIncrementTargetState {
    Exact,
    BeforeDesired,
    Different,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlAuthorizationTargetState {
    Absent,
    Subset,
    Exact,
    Different,
}

/// Inspect one MySQL endpoint through one retained consistent-snapshot session.
pub fn inspect_live_endpoint(
    config: MySqlEndpointConfig,
) -> Result<MySqlCatalogSnapshot, MySqlPlanError> {
    config.validate()?;
    let tls_binding = mysql_tls_binding(&config)?;
    let factory = MySqlSourceFactory::new(config);
    let token = factory
        .capture_snapshot()
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let (catalog, blockers, fingerprint) = factory
        .captured_catalog(&token)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let snapshot_evidence = factory
        .snapshot_evidence(&token)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    if snapshot_evidence.catalog_fingerprint != fingerprint {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL snapshot evidence does not bind the captured catalog".into(),
        ));
    }
    let reader = factory
        .open_reader(&token, CancellationToken::default())
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    drop(reader);
    Ok(MySqlCatalogSnapshot {
        endpoint_identity: token.endpoint_identity,
        database_identity: token.database_identity,
        server_version: token.server_version,
        tls_binding,
        snapshot_evidence,
        catalog,
        blockers,
    })
}

/// Capture the complete source catalog and authorization state through a
/// separately authenticated metadata administrator.
pub fn collect_mysql_metadata_visibility(
    source: &MySqlCatalogSnapshot,
    source_config: &MySqlEndpointConfig,
    metadata_admin_config: &MySqlEndpointConfig,
    freeze_admin_config: Option<&MySqlEndpointConfig>,
) -> Result<MySqlMetadataVisibilityCapture, MySqlPlanError> {
    validate_catalog_snapshot(source)?;
    source_config.validate()?;
    metadata_admin_config.validate()?;
    if source_config.database != source.database_identity
        || metadata_admin_config.database != source.database_identity
        || source_config.credential_env == metadata_admin_config.credential_env
        || freeze_admin_config.is_some_and(|config| {
            config.database != source.database_identity
                || config.credential_env == source_config.credential_env
                || config.credential_env == metadata_admin_config.credential_env
        })
    {
        return Err(MySqlPlanError::InvalidConfig(
            "source reader and metadata administrator must use separate credentials for the same database",
        ));
    }

    let mut conn = metadata_admin_config.connect()?;
    configure_mysql_session(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    conn.query_drop("SET ROLE ALL")?;
    conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")?;
    conn.query_drop("SET SESSION TRANSACTION READ ONLY")?;
    conn.query_drop("SET SESSION information_schema_stats_expiry = 0")?;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")?;
    let identity = mysql_live_session_identity(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let endpoint_identity = format!(
        "mysql://{}:{}/{}?server_uuid={}",
        identity.hostname, identity.port, identity.database, identity.server_uuid
    );
    if endpoint_identity != source.endpoint_identity
        || identity.database != source.database_identity
        || identity.server_uuid != source.snapshot_evidence.server_uuid
        || identity.server_version != source.server_version
        || identity.lower_case_table_names != source.snapshot_evidence.lower_case_table_names
        || identity.transaction_read_only != 1
        || !mysql_session_settings_are_exact(&identity)
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL metadata administrator differs from the reviewed source endpoint".into(),
        ));
    }

    let (mut authoritative_catalog, mut authoritative_blockers) = extract_catalog(
        &mut conn,
        &identity.database,
        &identity.server_version,
        identity.lower_case_table_names,
    )
    .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    remove_mysql_privilege_projection(&mut authoritative_catalog, &mut authoritative_blockers);
    let authoritative_catalog_fingerprint = mysql_catalog_fingerprint(&authoritative_catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let grant_inventory = collect_mysql_grant_inventory(&mut conn, &identity.database)?;
    let catalog_reader_account = resolve_mysql_account(
        &grant_inventory.accounts,
        &source.snapshot_evidence.authenticated_account,
    )?;
    let metadata_administrator_account =
        resolve_mysql_account(&grant_inventory.accounts, &identity.authenticated_account)?;
    if catalog_reader_account == metadata_administrator_account {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL metadata administrator must use a distinct authenticated account".into(),
        ));
    }
    let freeze_administrator_account = freeze_admin_config
        .map(|config| {
            attest_mysql_operational_account(source, config)
                .and_then(|account| resolve_mysql_account(&grant_inventory.accounts, &account))
        })
        .transpose()?;
    // ENABLED_ROLES is bound exactly, including inherited roles. The grant
    // inventory can under-count an unmodeled effective privilege only by
    // causing the later required-privilege proof to fail closed; it cannot
    // turn a restricted source account into an accepted unrestricted one.
    let active_administrator_roles = collect_enabled_mysql_roles(&mut conn)?;
    let operational_exclusions = derive_mysql_operational_exclusions(
        &grant_inventory,
        &catalog_reader_account,
        &metadata_administrator_account,
        freeze_administrator_account.as_ref(),
        &active_administrator_roles,
        &metadata_admin_config.operational_server_administrators,
    )?;
    let effective_administrator_privileges = mysql_effective_global_privileges(
        &grant_inventory,
        &metadata_administrator_account,
        &active_administrator_roles,
    )?;
    conn.query_drop("ROLLBACK")?;

    let grant_inventory_digest = grant_inventory.canonical_hash()?;
    let evidence = MySqlMetadataVisibilityEvidence {
        schema_version: MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
        endpoint_identity: source.endpoint_identity.clone(),
        database_identity: source.database_identity.clone(),
        server_uuid: source.snapshot_evidence.server_uuid.clone(),
        catalog_reader_tls_binding: source.tls_binding.clone(),
        metadata_administrator_tls_binding: mysql_tls_binding(metadata_admin_config)?,
        catalog_reader_account,
        metadata_administrator_account,
        active_administrator_roles,
        effective_administrator_privileges,
        operational_exclusions,
        catalog_reader_fingerprint: source.snapshot_evidence.catalog_fingerprint.clone(),
        authoritative_catalog_fingerprint,
        grant_inventory_digest,
        grant_inventory,
    };
    evidence.validate()?;
    Ok(MySqlMetadataVisibilityCapture {
        authoritative_catalog,
        authoritative_blockers,
        evidence,
    })
}

fn attest_mysql_operational_account(
    endpoint: &MySqlCatalogSnapshot,
    config: &MySqlEndpointConfig,
) -> Result<String, MySqlPlanError> {
    config.validate()?;
    let mut conn = config.connect()?;
    configure_mysql_session(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    conn.query_drop("SET SESSION information_schema_stats_expiry = 0")?;
    conn.query_drop("SET SESSION TRANSACTION READ ONLY")?;
    conn.query_drop("START TRANSACTION")?;
    let identity = mysql_live_session_identity(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let endpoint_identity = format!(
        "mysql://{}:{}/{}?server_uuid={}",
        identity.hostname, identity.port, identity.database, identity.server_uuid
    );
    conn.query_drop("ROLLBACK")?;
    if endpoint_identity != endpoint.endpoint_identity
        || identity.database != endpoint.database_identity
        || identity.server_uuid != endpoint.snapshot_evidence.server_uuid
        || identity.server_version != endpoint.server_version
        || identity.transaction_read_only != 1
        || !mysql_session_settings_are_exact(&identity)
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL operational administrator differs from the reviewed endpoint".into(),
        ));
    }
    Ok(identity.authenticated_account)
}

fn remove_mysql_privilege_projection(
    catalog: &mut VendorCatalog,
    blockers: &mut Vec<MySqlCatalogBlocker>,
) {
    let removed = catalog
        .namespaces
        .iter_mut()
        .flat_map(|namespace| {
            let removed = namespace
                .objects
                .iter()
                .filter(|object| {
                    matches!(
                        &object.kind,
                        CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege"
                    )
                })
                .map(|object| object.id.clone())
                .collect::<BTreeSet<_>>();
            namespace
                .objects
                .retain(|object| !removed.contains(&object.id));
            removed
        })
        .collect::<BTreeSet<_>>();
    catalog.dependencies.retain(|dependency| {
        !removed.contains(&dependency.from_object_id) && !removed.contains(&dependency.to_object_id)
    });
    blockers.retain(|blocker| {
        blocker.object_kind != "catalog_visibility" && blocker.object_kind != "privilege"
    });
}

fn resolve_mysql_account(
    accounts: &[MySqlAccountIdentity],
    authenticated_account: &str,
) -> Result<MySqlAccountIdentity, MySqlPlanError> {
    let mut matching = accounts
        .iter()
        .filter(|account| format!("{}@{}", account.user, account.host) == authenticated_account);
    let account = matching.next().cloned().ok_or_else(|| {
        MySqlPlanError::InvalidCatalog(
            "authenticated MySQL account is absent from mysql.user".into(),
        )
    })?;
    if matching.next().is_some() {
        return Err(MySqlPlanError::InvalidCatalog(
            "authenticated MySQL account text is ambiguous in mysql.user".into(),
        ));
    }
    Ok(account)
}

fn collect_enabled_mysql_roles(
    conn: &mut Conn,
) -> Result<Vec<MySqlAccountIdentity>, MySqlPlanError> {
    let rows: Vec<(String, String)> = conn.query(
        "SELECT ROLE_NAME, ROLE_HOST FROM information_schema.ENABLED_ROLES ORDER BY ROLE_NAME, ROLE_HOST",
    )?;
    let mut roles = rows
        .into_iter()
        .map(|(user, host)| MySqlAccountIdentity { user, host })
        .collect::<Vec<_>>();
    roles.sort();
    roles.dedup();
    Ok(roles)
}

fn derive_mysql_operational_exclusions(
    inventory: &MySqlGrantInventory,
    source_reader: &MySqlAccountIdentity,
    metadata_administrator: &MySqlAccountIdentity,
    freeze_administrator: Option<&MySqlAccountIdentity>,
    active_administrator_roles: &[MySqlAccountIdentity],
    declared_server_administrators: &[MySqlAccountIdentity],
) -> Result<Vec<MySqlOperationalAccountExclusion>, MySqlPlanError> {
    let mut purposes = BTreeMap::from([
        (
            source_reader.clone(),
            MySqlOperationalAccountPurpose::CatalogReader,
        ),
        (
            metadata_administrator.clone(),
            MySqlOperationalAccountPurpose::MetadataAdministrator,
        ),
    ]);
    if let Some(freeze_administrator) = freeze_administrator {
        purposes.insert(
            freeze_administrator.clone(),
            MySqlOperationalAccountPurpose::FreezeAdministrator,
        );
    }
    for role in active_administrator_roles {
        purposes
            .entry(role.clone())
            .or_insert(MySqlOperationalAccountPurpose::OperationalRole);
    }
    if !declared_server_administrators
        .windows(2)
        .all(|pair| pair[0] < pair[1])
    {
        return Err(MySqlPlanError::InvalidConfig(
            "operational server administrators must be sorted and unique",
        ));
    }
    for account in declared_server_administrators {
        account.validate()?;
        let is_server_administrator = inventory.records.iter().any(|record| {
            matches!(
                record,
                MySqlGrantRecord::StaticGlobal { account: holder, privilege }
                    if holder == account && privilege == "SUPER"
            ) || matches!(
                record,
                MySqlGrantRecord::DynamicGlobal { account: holder, privilege, .. }
                    if holder == account && privilege == "SYSTEM_USER"
            )
        });
        if inventory.accounts.binary_search(account).is_err() || !is_server_administrator {
            return Err(MySqlPlanError::InvalidCatalog(format!(
                "declared MySQL operational server administrator {}@{} is absent or lacks SUPER/SYSTEM_USER",
                account.user, account.host
            )));
        }
        purposes
            .entry(account.clone())
            .or_insert(MySqlOperationalAccountPurpose::ServerAdministrator);
    }
    loop {
        let roles = inventory
            .records
            .iter()
            .filter_map(|record| match record {
                MySqlGrantRecord::RoleEdge { role, grantee, .. }
                    if purposes.contains_key(grantee) && !purposes.contains_key(role) =>
                {
                    Some(role.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if roles.is_empty() {
            break;
        }
        for role in roles {
            purposes.insert(role, MySqlOperationalAccountPurpose::OperationalRole);
        }
    }
    let mut exclusions = purposes
        .into_iter()
        .map(|(account, purpose)| MySqlOperationalAccountExclusion { purpose, account })
        .collect::<Vec<_>>();
    exclusions.sort();
    Ok(exclusions)
}

fn mysql_effective_global_privileges(
    inventory: &MySqlGrantInventory,
    administrator: &MySqlAccountIdentity,
    active_roles: &[MySqlAccountIdentity],
) -> Result<Vec<String>, MySqlPlanError> {
    let principals = std::iter::once(administrator)
        .chain(active_roles.iter())
        .collect::<BTreeSet<_>>();
    if inventory.records.iter().any(|record| {
        matches!(record, MySqlGrantRecord::PartialRevoke { account, .. } if principals.contains(account))
    }) {
        return Err(MySqlPlanError::InvalidCatalog(
            "metadata administrator or active role has a partial privilege revoke".into(),
        ));
    }
    let mut privileges = inventory
        .records
        .iter()
        .filter_map(|record| match record {
            MySqlGrantRecord::StaticGlobal { account, privilege }
            | MySqlGrantRecord::DynamicGlobal {
                account, privilege, ..
            } if principals.contains(account) => Some(privilege.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    privileges.sort();
    privileges.dedup();
    Ok(privileges)
}

const MYSQL_USER_STATIC_PRIVILEGE_COLUMNS: &[&str] = &[
    "Select_priv",
    "Insert_priv",
    "Update_priv",
    "Delete_priv",
    "Create_priv",
    "Drop_priv",
    "Reload_priv",
    "Shutdown_priv",
    "Process_priv",
    "File_priv",
    "Grant_priv",
    "References_priv",
    "Index_priv",
    "Alter_priv",
    "Show_db_priv",
    "Super_priv",
    "Create_tmp_table_priv",
    "Lock_tables_priv",
    "Execute_priv",
    "Repl_slave_priv",
    "Repl_client_priv",
    "Create_view_priv",
    "Show_view_priv",
    "Create_routine_priv",
    "Alter_routine_priv",
    "Create_user_priv",
    "Event_priv",
    "Trigger_priv",
    "Create_tablespace_priv",
    "Create_role_priv",
    "Drop_role_priv",
];

const MYSQL_DATABASE_PRIVILEGE_COLUMNS: &[&str] = &[
    "Select_priv",
    "Insert_priv",
    "Update_priv",
    "Delete_priv",
    "Create_priv",
    "Drop_priv",
    "Grant_priv",
    "References_priv",
    "Index_priv",
    "Alter_priv",
    "Create_tmp_table_priv",
    "Lock_tables_priv",
    "Create_view_priv",
    "Show_view_priv",
    "Create_routine_priv",
    "Alter_routine_priv",
    "Execute_priv",
    "Event_priv",
    "Trigger_priv",
];

const MYSQL_GRANT_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    (
        "tables_priv",
        &[
            "Host",
            "Db",
            "User",
            "Table_name",
            "Grantor",
            "Timestamp",
            "Table_priv",
            "Column_priv",
        ],
    ),
    (
        "columns_priv",
        &[
            "Host",
            "Db",
            "User",
            "Table_name",
            "Column_name",
            "Timestamp",
            "Column_priv",
        ],
    ),
    (
        "procs_priv",
        &[
            "Host",
            "Db",
            "User",
            "Routine_name",
            "Routine_type",
            "Grantor",
            "Proc_priv",
            "Timestamp",
        ],
    ),
    (
        "proxies_priv",
        &[
            "Host",
            "User",
            "Proxied_host",
            "Proxied_user",
            "With_grant",
            "Grantor",
            "Timestamp",
        ],
    ),
    (
        "global_grants",
        &["USER", "HOST", "PRIV", "WITH_GRANT_OPTION"],
    ),
    (
        "default_roles",
        &["HOST", "USER", "DEFAULT_ROLE_HOST", "DEFAULT_ROLE_USER"],
    ),
    (
        "role_edges",
        &[
            "FROM_HOST",
            "FROM_USER",
            "TO_HOST",
            "TO_USER",
            "WITH_ADMIN_OPTION",
        ],
    ),
];

fn collect_mysql_grant_inventory(
    conn: &mut Conn,
    reviewed_database: &str,
) -> Result<MySqlGrantInventory, MySqlPlanError> {
    let partial_revokes_enabled: u8 = conn
        .query_first("SELECT @@global.partial_revokes")?
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog("MySQL partial_revokes query returned no row".into())
        })?;
    let partial_revokes_enabled =
        parse_mysql_zero_one("@@global.partial_revokes", partial_revokes_enabled)?;
    let mut grant_table_columns = conn
        .query::<(String, String, String), _>(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'mysql' AND TABLE_NAME IN ('user','db','tables_priv','columns_priv','procs_priv','proxies_priv','global_grants','default_roles','role_edges') ORDER BY TABLE_NAME, ORDINAL_POSITION",
        )?
        .into_iter()
        .map(|(table, column, data_type)| MySqlGrantTableColumn {
            table,
            column,
            data_type,
        })
        .collect::<Vec<_>>();
    grant_table_columns.sort();
    let unknown_privilege_classes = mysql_unknown_grant_schema(&grant_table_columns);

    let mut accounts = conn
        .query::<(String, String), _>("SELECT User, Host FROM mysql.user ORDER BY User, Host")?
        .into_iter()
        .map(|(user, host)| MySqlAccountIdentity { user, host })
        .collect::<Vec<_>>();
    accounts.sort();
    accounts.dedup();

    let mut records = collect_mysql_static_global_grants(conn)?;
    records.extend(collect_mysql_database_grants(conn)?);
    let dynamic: Vec<(String, String, String, String)> = conn.query(
        "SELECT USER, HOST, PRIV, WITH_GRANT_OPTION FROM mysql.global_grants ORDER BY USER, HOST, PRIV",
    )?;
    for (user, host, privilege, grantable) in dynamic {
        records.push(MySqlGrantRecord::DynamicGlobal {
            account: MySqlAccountIdentity { user, host },
            privilege: privilege.to_ascii_uppercase(),
            grantable: parse_mysql_yes_no("mysql.global_grants.WITH_GRANT_OPTION", &grantable)?,
        });
    }

    let table_rows: Vec<(String, String, String, String, String, String, String)> = conn.query(
        "SELECT Host, User, Db, Table_name, Grantor, CAST(Table_priv AS CHAR), CAST(Column_priv AS CHAR) FROM mysql.tables_priv ORDER BY Host, User, Db, Table_name",
    )?;
    for (host, user, database, table, grantor, table_privileges, column_privileges) in table_rows {
        let account = MySqlAccountIdentity { user, host };
        records.extend(
            split_mysql_set(&table_privileges)
                .into_iter()
                .map(|privilege| MySqlGrantRecord::Table {
                    account: account.clone(),
                    database: database.clone(),
                    table: table.clone(),
                    privilege,
                    grantor: grantor.clone(),
                }),
        );
        records.extend(
            split_mysql_set(&column_privileges)
                .into_iter()
                .map(|privilege| MySqlGrantRecord::Table {
                    account: account.clone(),
                    database: database.clone(),
                    table: table.clone(),
                    privilege: format!("COLUMN::{privilege}"),
                    grantor: grantor.clone(),
                }),
        );
    }

    let column_rows: Vec<(String, String, String, String, String, String)> = conn.query(
        "SELECT Host, User, Db, Table_name, Column_name, CAST(Column_priv AS CHAR) FROM mysql.columns_priv ORDER BY Host, User, Db, Table_name, Column_name",
    )?;
    for (host, user, database, table, column, privileges) in column_rows {
        let account = MySqlAccountIdentity { user, host };
        records.extend(split_mysql_set(&privileges).into_iter().map(|privilege| {
            MySqlGrantRecord::Column {
                account: account.clone(),
                database: database.clone(),
                table: table.clone(),
                column: column.clone(),
                privilege,
            }
        }));
    }

    let routine_rows: Vec<(String, String, String, String, String, String, String)> = conn.query(
        "SELECT Host, User, Db, Routine_name, Routine_type, Grantor, CAST(Proc_priv AS CHAR) FROM mysql.procs_priv ORDER BY Host, User, Db, Routine_name, Routine_type",
    )?;
    for (host, user, database, routine, routine_type, grantor, privileges) in routine_rows {
        let account = MySqlAccountIdentity { user, host };
        records.extend(split_mysql_set(&privileges).into_iter().map(|privilege| {
            MySqlGrantRecord::Routine {
                account: account.clone(),
                database: database.clone(),
                routine: routine.clone(),
                routine_type: routine_type.to_ascii_uppercase(),
                privilege,
                grantor: grantor.clone(),
            }
        }));
    }

    let proxy_rows: Vec<(String, String, String, String, u8)> = conn.query(
        "SELECT Host, User, Proxied_host, Proxied_user, With_grant FROM mysql.proxies_priv ORDER BY Host, User, Proxied_host, Proxied_user",
    )?;
    for (host, user, proxied_host, proxied_user, grantable) in proxy_rows {
        let target = if proxied_user.is_empty() && proxied_host.is_empty() {
            MySqlProxyTarget::AnyAccount
        } else {
            MySqlProxyTarget::Account {
                account: MySqlAccountIdentity {
                    user: proxied_user,
                    host: proxied_host,
                },
            }
        };
        records.push(MySqlGrantRecord::Proxy {
            account: MySqlAccountIdentity { user, host },
            target,
            grantable: parse_mysql_zero_one("mysql.proxies_priv.With_grant", grantable)?,
        });
    }

    let role_rows: Vec<(String, String, String, String, String)> = conn.query(
        "SELECT FROM_HOST, FROM_USER, TO_HOST, TO_USER, WITH_ADMIN_OPTION FROM mysql.role_edges ORDER BY FROM_HOST, FROM_USER, TO_HOST, TO_USER",
    )?;
    for (role_host, role_user, grantee_host, grantee_user, admin_option) in role_rows {
        records.push(MySqlGrantRecord::RoleEdge {
            role: MySqlAccountIdentity {
                user: role_user,
                host: role_host,
            },
            grantee: MySqlAccountIdentity {
                user: grantee_user,
                host: grantee_host,
            },
            admin_option: parse_mysql_yes_no("mysql.role_edges.WITH_ADMIN_OPTION", &admin_option)?,
        });
    }
    let default_role_rows: Vec<(String, String, String, String)> = conn.query(
        "SELECT HOST, USER, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER FROM mysql.default_roles ORDER BY HOST, USER, DEFAULT_ROLE_HOST, DEFAULT_ROLE_USER",
    )?;
    records.extend(
        default_role_rows
            .into_iter()
            .map(
                |(host, user, role_host, role_user)| MySqlGrantRecord::DefaultRole {
                    account: MySqlAccountIdentity { user, host },
                    role: MySqlAccountIdentity {
                        user: role_user,
                        host: role_host,
                    },
                },
            ),
    );
    records.extend(collect_mysql_partial_revokes(conn)?);
    records.retain(|record| match record {
        MySqlGrantRecord::Database { database, .. }
        | MySqlGrantRecord::Table { database, .. }
        | MySqlGrantRecord::Column { database, .. }
        | MySqlGrantRecord::Routine { database, .. }
        | MySqlGrantRecord::PartialRevoke { database, .. } => {
            mysql_grant_database_pattern_matches(database, reviewed_database)
        }
        _ => true,
    });
    records.sort();
    records.dedup();

    let inventory = MySqlGrantInventory {
        partial_revokes_enabled,
        grant_table_columns,
        accounts,
        records,
        unknown_privilege_classes,
    };
    inventory.validate()?;
    Ok(inventory)
}

fn mysql_grant_database_pattern_matches(pattern: &str, database: &str) -> bool {
    #[derive(Clone, Copy)]
    enum Token {
        Literal(char),
        One,
        Many,
    }
    let mut tokens = Vec::new();
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next() {
                Some(escaped) => tokens.push(Token::Literal(escaped)),
                None => tokens.push(Token::Literal('\\')),
            },
            '_' => tokens.push(Token::One),
            '%' => tokens.push(Token::Many),
            literal => tokens.push(Token::Literal(literal)),
        }
    }
    let database = database.chars().collect::<Vec<_>>();
    let mut reachable = vec![false; database.len() + 1];
    reachable[0] = true;
    for token in tokens {
        let mut next = vec![false; database.len() + 1];
        match token {
            Token::Literal(expected) => {
                for index in 0..database.len() {
                    if reachable[index] && database[index] == expected {
                        next[index + 1] = true;
                    }
                }
            }
            Token::One => {
                for index in 0..database.len() {
                    if reachable[index] {
                        next[index + 1] = true;
                    }
                }
            }
            Token::Many => {
                let mut matched = false;
                for index in 0..=database.len() {
                    matched |= reachable[index];
                    next[index] = matched;
                }
            }
        }
        reachable = next;
    }
    reachable[database.len()]
}

fn collect_mysql_static_global_grants(
    conn: &mut Conn,
) -> Result<Vec<MySqlGrantRecord>, MySqlPlanError> {
    let query = MYSQL_USER_STATIC_PRIVILEGE_COLUMNS
        .iter()
        .map(|column| {
            format!(
                "SELECT User, Host, '{}' AS privilege FROM mysql.user WHERE {column} = 'Y'",
                mysql_privilege_name(column)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let rows: Vec<(String, String, String)> = conn.query(query)?;
    Ok(rows
        .into_iter()
        .map(|(user, host, privilege)| MySqlGrantRecord::StaticGlobal {
            account: MySqlAccountIdentity { user, host },
            privilege,
        })
        .collect())
}

fn collect_mysql_database_grants(conn: &mut Conn) -> Result<Vec<MySqlGrantRecord>, MySqlPlanError> {
    let query = MYSQL_DATABASE_PRIVILEGE_COLUMNS
        .iter()
        .map(|column| {
            format!(
                "SELECT User, Host, Db, '{}' AS privilege FROM mysql.db WHERE {column} = 'Y'",
                mysql_privilege_name(column)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let rows: Vec<(String, String, String, String)> = conn.query(query)?;
    Ok(rows
        .into_iter()
        .map(
            |(user, host, database, privilege)| MySqlGrantRecord::Database {
                account: MySqlAccountIdentity { user, host },
                database,
                privilege,
            },
        )
        .collect())
}

#[derive(Debug, Deserialize)]
struct MySqlPartialRevokeRow {
    #[serde(rename = "Database")]
    database: String,
    #[serde(rename = "Privileges", alias = "Restrictions")]
    privileges: Vec<String>,
}

fn collect_mysql_partial_revokes(conn: &mut Conn) -> Result<Vec<MySqlGrantRecord>, MySqlPlanError> {
    let rows: Vec<(String, String, String)> = conn.query(
        "SELECT User, Host, JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.Restrictions')) FROM mysql.user WHERE JSON_CONTAINS_PATH(User_attributes, 'one', '$.Restrictions') ORDER BY User, Host",
    )?;
    let mut records = Vec::new();
    for (user, host, restrictions) in rows {
        let restrictions: Vec<MySqlPartialRevokeRow> = serde_json::from_str(&restrictions)?;
        for restriction in restrictions {
            let mut privileges = restriction
                .privileges
                .into_iter()
                .map(|privilege| privilege.to_ascii_uppercase())
                .collect::<Vec<_>>();
            privileges.sort();
            privileges.dedup();
            records.push(MySqlGrantRecord::PartialRevoke {
                account: MySqlAccountIdentity {
                    user: user.clone(),
                    host: host.clone(),
                },
                database: restriction.database,
                privileges,
            });
        }
    }
    Ok(records)
}

fn mysql_unknown_grant_schema(columns: &[MySqlGrantTableColumn]) -> Vec<String> {
    let by_table = columns.iter().fold(
        BTreeMap::<&str, BTreeSet<&str>>::new(),
        |mut by_table, column| {
            by_table
                .entry(column.table.as_str())
                .or_default()
                .insert(column.column.as_str());
            by_table
        },
    );
    let mut unknown = Vec::new();
    let user_columns = by_table.get("user").cloned().unwrap_or_default();
    for required in ["Host", "User", "User_attributes"]
        .into_iter()
        .chain(MYSQL_USER_STATIC_PRIVILEGE_COLUMNS.iter().copied())
    {
        if !user_columns.contains(required) {
            unknown.push(format!("missing:mysql.user.{required}"));
        }
    }
    for column in user_columns {
        if column.ends_with("_priv") && !MYSQL_USER_STATIC_PRIVILEGE_COLUMNS.contains(&column) {
            unknown.push(format!("unknown:mysql.user.{column}"));
        }
    }
    let db_columns = by_table.get("db").cloned().unwrap_or_default();
    for required in ["Host", "Db", "User"]
        .into_iter()
        .chain(MYSQL_DATABASE_PRIVILEGE_COLUMNS.iter().copied())
    {
        if !db_columns.contains(required) {
            unknown.push(format!("missing:mysql.db.{required}"));
        }
    }
    for column in db_columns {
        if column.ends_with("_priv") && !MYSQL_DATABASE_PRIVILEGE_COLUMNS.contains(&column) {
            unknown.push(format!("unknown:mysql.db.{column}"));
        }
    }
    for (table, expected) in MYSQL_GRANT_TABLE_COLUMNS {
        let observed = by_table.get(table).cloned().unwrap_or_default();
        for required in *expected {
            if !observed.contains(required) {
                unknown.push(format!("missing:mysql.{table}.{required}"));
            }
        }
        for column in observed {
            if !expected.contains(&column) {
                unknown.push(format!("unknown:mysql.{table}.{column}"));
            }
        }
    }
    unknown.sort();
    unknown.dedup();
    unknown
}

fn mysql_privilege_name(column: &str) -> String {
    column
        .strip_suffix("_priv")
        .unwrap_or(column)
        .replace('_', " ")
        .to_ascii_uppercase()
}

fn split_mysql_set(value: &str) -> Vec<String> {
    let mut values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn parse_mysql_yes_no(field: &str, value: &str) -> Result<bool, MySqlPlanError> {
    match value {
        "Y" => Ok(true),
        "N" => Ok(false),
        _ => Err(MySqlPlanError::InvalidCatalog(format!(
            "MySQL {field} has unsupported value {value:?}"
        ))),
    }
}

fn parse_mysql_zero_one(field: &str, value: u8) -> Result<bool, MySqlPlanError> {
    match value {
        1 => Ok(true),
        0 => Ok(false),
        _ => Err(MySqlPlanError::InvalidCatalog(format!(
            "MySQL {field} has unsupported value {value}"
        ))),
    }
}

/// Build a reviewed MySQL plan. Live execution separately requires the
/// reviewed external-freeze attestation stored in its journal genesis.
pub fn build_plan(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_catalog_snapshot(source)?;
    validate_catalog_snapshot(target)?;
    build_plan_from_execution_source(source, target, None, None, None)
}

/// Build a reviewed MySQL plan with separately authenticated authoritative
/// source catalog and grant evidence.
pub fn build_plan_with_visibility(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: &MySqlMetadataVisibilityCapture,
    target_visibility: &MySqlMetadataVisibilityCapture,
) -> Result<ReviewedPlan, MySqlPlanError> {
    build_plan_with_visibility_internal(source, target, source_visibility, target_visibility, None)
}

/// Build a reviewed MySQL plan with an explicit, complete business-principal
/// mapping. Target accounts must already exist; authentication data is never
/// copied or represented in the plan.
pub fn build_plan_with_visibility_and_authorization(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: &MySqlMetadataVisibilityCapture,
    target_visibility: &MySqlMetadataVisibilityCapture,
    mapping: MySqlAuthorizationMapping,
) -> Result<ReviewedPlan, MySqlPlanError> {
    build_plan_with_visibility_internal(
        source,
        target,
        source_visibility,
        target_visibility,
        Some(mapping),
    )
}

fn build_plan_with_visibility_internal(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: &MySqlMetadataVisibilityCapture,
    target_visibility: &MySqlMetadataVisibilityCapture,
    mapping: Option<MySqlAuthorizationMapping>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_metadata_visibility_capture(source, source_visibility)?;
    validate_metadata_visibility_capture(target, target_visibility)?;
    let mut execution_source = source.clone();
    execution_source.catalog = source_visibility.authoritative_catalog.clone();
    execution_source.blockers = source_visibility.authoritative_blockers.clone();
    let authorization = mapping
        .map(|mapping| {
            MySqlAuthorizationContract::build(
                mapping,
                &source_visibility.evidence,
                &target_visibility.evidence,
            )
        })
        .transpose()?;
    if authorization.is_none() {
        for record in source_visibility.evidence.non_operational_records() {
            execution_source.blockers.push(MySqlCatalogBlocker {
                object_id: mysql_grant_record_id(record)?,
                object_kind: "privilege".into(),
                reason: "MySQL business authorization state is inventoried but target role mapping and exact restoration are not yet implemented".into(),
            });
        }
    }
    execution_source.blockers.sort_by(|left, right| {
        (&left.object_id, &left.object_kind).cmp(&(&right.object_id, &right.object_kind))
    });
    execution_source.blockers.dedup_by(|left, right| {
        left.object_id == right.object_id && left.object_kind == right.object_kind
    });
    let mut execution_target = target.clone();
    execution_target.catalog = target_visibility.authoritative_catalog.clone();
    execution_target.blockers = target_visibility.authoritative_blockers.clone();
    build_plan_from_execution_source(
        &execution_source,
        &execution_target,
        Some(source_visibility.evidence.clone()),
        Some(target_visibility.evidence.clone()),
        authorization,
    )
}

fn build_plan_from_execution_source(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: Option<MySqlMetadataVisibilityEvidence>,
    target_visibility: Option<MySqlMetadataVisibilityEvidence>,
    authorization: Option<MySqlAuthorizationContract>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_supported_server_version(&source.server_version)?;
    validate_supported_server_version(&target.server_version)?;
    let auto_increment = mysql_auto_increment_states(&source.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let auto_increment_by_table = auto_increment
        .iter()
        .map(|state| (state.table.clone(), state))
        .collect::<BTreeMap<_, _>>();
    let foreign_keys = mysql_supported_foreign_keys(&source.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let mut operations = Vec::new();
    let mut table_verifications = Vec::new();
    let mut copy_operation_ids = BTreeMap::new();
    let mut tables = source
        .catalog
        .namespaces
        .iter()
        .flat_map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| object.kind == CatalogObjectKind::Table)
                .map(|object| {
                    (
                        QualifiedTable {
                            namespace: namespace.name.clone(),
                            name: object.name.clone(),
                        },
                        object,
                    )
                })
        })
        .collect::<Vec<_>>();
    tables.sort_by(|left, right| left.0.cmp(&right.0));
    for (table, object) in tables {
        let namespace = source
            .catalog
            .namespaces
            .iter()
            .find(|namespace| namespace.name == table.namespace)
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(format!(
                    "MySQL table {} has no namespace",
                    table.name
                ))
            })?;
        let columns = namespace
            .objects
            .iter()
            .filter(|candidate| {
                candidate.kind == CatalogObjectKind::Column
                    && candidate
                        .attributes
                        .get("table_name")
                        .and_then(serde_json::Value::as_str)
                        == Some(table.name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let indexes = namespace
            .objects
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.kind,
                    CatalogObjectKind::PrimaryKey
                        | CatalogObjectKind::UniqueConstraint
                        | CatalogObjectKind::Index
                ) && candidate
                    .attributes
                    .get("table_name")
                    .and_then(serde_json::Value::as_str)
                    == Some(table.name.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut create_parameters = BTreeMap::from([
            ("mysql_table".into(), serde_json::to_value(object)?),
            ("mysql_columns".into(), serde_json::to_value(columns)?),
            ("mysql_indexes".into(), serde_json::to_value(indexes)?),
        ]);
        let mut table_mapping = None;
        match mysql_table_definition(namespace, object) {
            Ok(mut definition) => {
                let source_table = definition.table.clone();
                definition.table.namespace = target.catalog.database.clone();
                let mapping = MySqlTableMapping {
                    source: source_table,
                    target: definition.table.clone(),
                };
                create_parameters.insert(
                    "mysql_table_definition".into(),
                    serde_json::to_value(definition)?,
                );
                create_parameters.insert(
                    "mysql_table_mapping".into(),
                    serde_json::to_value(&mapping)?,
                );
                table_mapping = Some(mapping);
            }
            Err(_) if table_has_ddl_blocker(source, namespace, object) => {}
            Err(error) => return Err(error),
        }
        let create = PlanOperation::new(
            OperationKind::CreateTable,
            Some(table.clone()),
            Vec::new(),
            create_parameters,
        )?;
        let key: Option<MySqlResumableKey> = object
            .attributes
            .get("resumable_key")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        let mut copy_parameters = BTreeMap::from([(
            "mysql_write_policy".into(),
            serde_json::json!("plain_insert_transaction_v1"),
        )]);
        if let Some(key) = key {
            copy_parameters.insert("resumable_key".into(), serde_json::to_value(key)?);
        }
        if let Some(mapping) = &table_mapping {
            copy_parameters.insert("mysql_table_mapping".into(), serde_json::to_value(mapping)?);
        }
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            vec![create.id.clone()],
            copy_parameters,
        )?;
        copy_operation_ids.insert(table.clone(), copy.id.clone());
        let mut verify_dependencies = vec![copy.id.clone()];
        let restore = auto_increment_by_table
            .get(&table)
            .filter(|state| state.next_value.is_some())
            .map(|state| {
                let mapping = table_mapping.as_ref().ok_or_else(|| {
                    MySqlPlanError::InvalidCatalog(format!(
                        "MySQL AUTO_INCREMENT table {} has no target mapping",
                        table.name
                    ))
                })?;
                PlanOperation::new(
                    OperationKind::Vendor("restore_mysql_auto_increment".into()),
                    Some(table.clone()),
                    vec![copy.id.clone()],
                    BTreeMap::from([
                        (
                            "mysql_auto_increment_state".into(),
                            serde_json::to_value(*state)?,
                        ),
                        ("mysql_table_mapping".into(), serde_json::to_value(mapping)?),
                    ]),
                )
                .map_err(MySqlPlanError::from)
            })
            .transpose()?;
        if let Some(restore) = &restore {
            verify_dependencies.push(restore.id.clone());
        }
        operations.extend([create, copy]);
        if let Some(restore) = restore {
            operations.push(restore);
        }
        table_verifications.push((table, verify_dependencies));
    }
    let all_copy_dependencies = copy_operation_ids.values().cloned().collect::<Vec<_>>();
    let mut foreign_key_add_operations = Vec::new();
    for foreign_key in foreign_keys {
        let check = PlanOperation::new(
            OperationKind::CheckForeignKey,
            Some(foreign_key.table.clone()),
            all_copy_dependencies.clone(),
            BTreeMap::from([(
                "mysql_foreign_key".into(),
                serde_json::to_value(&foreign_key)?,
            )]),
        )?;
        let add = PlanOperation::new(
            OperationKind::AddForeignKey,
            Some(foreign_key.table.clone()),
            vec![check.id.clone()],
            BTreeMap::from([(
                "mysql_foreign_key".into(),
                serde_json::to_value(&foreign_key)?,
            )]),
        )?;
        foreign_key_add_operations.push(add.id.clone());
        operations.extend([check, add]);
    }
    for (table, mut dependencies) in table_verifications {
        dependencies.extend(foreign_key_add_operations.iter().cloned());
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table),
            dependencies,
            BTreeMap::new(),
        )?;
        operations.push(verify);
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
    if let Some(contract) = &authorization {
        let verify_schema_id = operations
            .last()
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog("MySQL plan has no schema verifier".into())
            })?
            .id
            .clone();
        operations.push(PlanOperation::new(
            OperationKind::Vendor("restore_mysql_authorization".into()),
            None,
            vec![verify_schema_id],
            BTreeMap::from([(
                "mysql_authorization_contract".into(),
                serde_json::to_value(contract)?,
            )]),
        )?);
    }

    let mut unsupported = source
        .blockers
        .iter()
        .map(mysql_blocker)
        .collect::<Vec<_>>();
    if target_visibility.is_none() {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::MySqlCatalogSemantics,
            object_id: "mysql-target-catalog-visibility".into(),
            object_kind: "target_catalog_visibility".into(),
            reason: "MySQL target metadata visibility is account-dependent; exhaustive target catalog and privilege evidence is not yet modeled"
                .into(),
            required_semantics: true,
        });
    }
    let missing_auto_increment_state = auto_increment
        .iter()
        .filter(|state| state.next_value.is_none())
        .count();
    if missing_auto_increment_state != 0 {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::MySqlAutoIncrementConsistency,
            object_id: "mysql-auto-increment-consistency".into(),
            object_kind: "auto_increment_consistency".into(),
            reason: format!(
                "{missing_auto_increment_state} MySQL AUTO_INCREMENT counters have no exact next-value evidence"
            ),
            required_semantics: true,
        });
    }
    let target_object_count = target
        .catalog
        .namespaces
        .iter()
        .map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| {
                    !matches!(
                        &object.kind,
                        CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege"
                    )
                })
                .count()
        })
        .sum::<usize>();
    if target_object_count != 0 {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::TargetNotEmpty,
            object_id: "target-catalog-not-empty".into(),
            object_kind: "target_precondition".into(),
            reason: format!("target catalog contains {target_object_count} user objects"),
            required_semantics: true,
        });
    }
    if source.snapshot_evidence.server_uuid == target.snapshot_evidence.server_uuid {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::SameEndpoint,
            object_id: "source-target-endpoint-collision".into(),
            object_kind: "endpoint_precondition".into(),
            reason: "source and target resolve to the same MySQL server UUID; freezing the source would also freeze target writes"
                .into(),
            required_semantics: true,
        });
    }
    unsupported
        .sort_by(|left, right| (&left.object_id, left.code).cmp(&(&right.object_id, right.code)));
    let source_fingerprint = mysql_catalog_fingerprint(&source.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let target_fingerprint = mysql_catalog_fingerprint(&target.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Execution,
        migration_id: format!("mysql-{}", &source_fingerprint[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity.clone(),
        target_endpoint_identity: AssessmentStatus::Assessed(target.endpoint_identity.clone()),
        source_catalog_fingerprint: source_fingerprint,
        target_catalog_fingerprint: AssessmentStatus::Assessed(target_fingerprint),
        source_catalog: Some(source.catalog.clone()),
        target_catalog: AssessmentStatus::Assessed(target.catalog.clone()),
        source_tls_binding: source.tls_binding.clone(),
        target_tls_binding: AssessmentStatus::Assessed(target.tls_binding.clone()),
        target_mode: Some(AssessmentStatus::Assessed(
            super::plan::TargetModeContract::EmptyOwned,
        )),
        target_writer_identity: None,
        consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: MYSQL_SAME_DIALECT_CONVERSION_POLICY,
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: Some(MySqlFreezeProfileContract::external_continuous_freeze()),
        mysql_snapshot_evidence: Some(source.snapshot_evidence.clone()),
        mysql_target_snapshot_evidence: Some(target.snapshot_evidence.clone()),
        mysql_metadata_visibility: source_visibility,
        mysql_target_metadata_visibility: target_visibility,
        mysql_authorization: authorization,
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "mysql_repeatable_read_consistent_snapshot".into(),
            ),
            ("source_tls".into(), source.tls_binding.clone()),
            ("target_tls".into(), target.tls_binding.clone()),
            ("mysql.ddl_freeze".into(), "required_not_assessed".into()),
            ("mysql.catalog_snapshot_protected".into(), "false".into()),
            ("mysql.information_schema_stats_expiry".into(), "0".into()),
        ]),
        operations,
        unsupported_objects: UnsupportedObjectReport {
            objects: unsupported,
        },
    })
    .map_err(MySqlPlanError::from)
}

fn mysql_grant_record_id(record: &MySqlGrantRecord) -> Result<String, MySqlPlanError> {
    Ok(record.canonical_id()?)
}

fn table_has_ddl_blocker(
    snapshot: &MySqlCatalogSnapshot,
    namespace: &CatalogNamespace,
    table: &CatalogObject,
) -> bool {
    let object_ids = namespace
        .objects
        .iter()
        .filter(|object| {
            object.id == table.id
                || optional_text(object, "table_name") == Some(table.name.as_str())
        })
        .map(|object| object.id.as_str())
        .collect::<BTreeSet<_>>();
    snapshot.blockers.iter().any(|blocker| {
        object_ids.contains(blocker.object_id.as_str())
            && matches!(
                blocker.object_kind.as_str(),
                "table" | "table_ddl" | "column_ddl" | "generated_column" | "index"
            )
    })
}

pub(crate) fn validate_catalog_snapshot(
    snapshot: &MySqlCatalogSnapshot,
) -> Result<(), MySqlPlanError> {
    let fingerprint = mysql_catalog_fingerprint(&snapshot.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let evidence = &snapshot.snapshot_evidence;
    if snapshot.catalog.database.as_str() != snapshot.database_identity
        || snapshot.catalog.server_version != snapshot.server_version
        || evidence.endpoint_identity != snapshot.endpoint_identity
        || evidence.database_identity != snapshot.database_identity
        || evidence.server_version != snapshot.server_version
        || evidence.authenticated_account.is_empty()
        || evidence.authenticated_account.contains('\0')
        || evidence.server_uuid.is_empty()
        || evidence.lifecycle_id.is_empty()
        || evidence.connection_id == 0
        || evidence.catalog_fingerprint != fingerprint
        || !evidence
            .transaction_isolation
            .eq_ignore_ascii_case("REPEATABLE-READ")
        || !evidence.transaction_read_only
        || evidence.session_time_zone != "+00:00"
        || evidence.catalog_snapshot_protected
        || evidence.information_schema_stats_expiry != 0
        || evidence.lower_case_table_names > 2
        || evidence.session_sql_mode != MYSQL_STRICT_SQL_MODE
        || evidence.character_set_client != MYSQL_SESSION_CHARACTER_SET
        || evidence.character_set_connection != MYSQL_SESSION_CHARACTER_SET
        || evidence.character_set_results != MYSQL_SESSION_CHARACTER_SET
        || evidence.collation_connection != MYSQL_SESSION_COLLATION
        || snapshot
            .catalog
            .vendor_metadata
            .get("lower_case_table_names")
            .is_none_or(|value| value != &evidence.lower_case_table_names.to_string())
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL catalog snapshot is not exactly bound to its live snapshot evidence".into(),
        ));
    }
    let blocker_keys = snapshot
        .blockers
        .iter()
        .map(|blocker| (blocker.object_id.as_str(), blocker.object_kind.as_str()))
        .collect::<BTreeSet<_>>();
    if blocker_keys.len() != snapshot.blockers.len() {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL catalog contains duplicate blockers".into(),
        ));
    }
    for (object_id, object_kind) in required_blocker_keys(&snapshot.catalog)? {
        if !blocker_keys.contains(&(object_id.as_str(), object_kind)) {
            return Err(MySqlPlanError::InvalidCatalog(format!(
                "MySQL catalog object {object_id} lacks required {object_kind} blocker"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_metadata_visibility_capture(
    snapshot: &MySqlCatalogSnapshot,
    capture: &MySqlMetadataVisibilityCapture,
) -> Result<(), MySqlPlanError> {
    validate_catalog_snapshot(snapshot)?;
    capture.evidence.validate()?;
    let authoritative_fingerprint = mysql_catalog_fingerprint(&capture.authoritative_catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    if !mysql_catalog_visibility_is_complete(snapshot, capture)?
        || capture.evidence.endpoint_identity != snapshot.endpoint_identity
        || capture.evidence.database_identity != snapshot.database_identity
        || capture.evidence.server_uuid != snapshot.snapshot_evidence.server_uuid
        || capture.evidence.catalog_reader_tls_binding != snapshot.tls_binding
        || capture.evidence.catalog_reader_fingerprint
            != snapshot.snapshot_evidence.catalog_fingerprint
        || capture.evidence.authoritative_catalog_fingerprint != authoritative_fingerprint
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL metadata-visibility capture differs from its reader snapshot".into(),
        ));
    }
    Ok(())
}

fn required_blocker_keys(
    catalog: &VendorCatalog,
) -> Result<Vec<(String, &'static str)>, MySqlPlanError> {
    let supported_foreign_keys = mysql_supported_foreign_keys(catalog)
        .unwrap_or_default()
        .into_iter()
        .map(|foreign_key| foreign_key.catalog_object_id)
        .collect::<BTreeSet<_>>();
    let mut required = vec![(
        catalog_id(
            "catalog_visibility",
            catalog.database.as_str(),
            catalog.database.as_str(),
            "",
        ),
        "catalog_visibility",
    )];
    for object in catalog
        .namespaces
        .iter()
        .flat_map(|namespace| namespace.objects.iter())
    {
        match &object.kind {
            CatalogObjectKind::Table => {
                if optional_text(object, "engine") != Some("InnoDB") {
                    required.push((object.id.clone(), "table"));
                }
                if optional_text(object, "character_set").is_none()
                    || optional_text(object, "collation").is_none()
                    || optional_text(object, "create_options")
                        .is_some_and(|value| !value.is_empty())
                {
                    required.push((object.id.clone(), "table_ddl"));
                }
                if object
                    .attributes
                    .get("resumable_key")
                    .is_none_or(serde_json::Value::is_null)
                {
                    required.push((object.id.clone(), "resumable_key"));
                }
            }
            CatalogObjectKind::Column => {
                if object
                    .attributes
                    .get("mysql_ddl_type")
                    .is_none_or(serde_json::Value::is_null)
                    || !supported_value_type(required_text(object, "data_type")?)
                    || object
                        .attributes
                        .get("default")
                        .is_some_and(|value| !value.is_null())
                    || !supported_column_extra(required_text(object, "extra")?)
                {
                    required.push((object.id.clone(), "column_ddl"));
                }
                if optional_text(object, "generation_expression")
                    .is_some_and(|value| !value.is_empty())
                {
                    required.push((object.id.clone(), "generated_column"));
                }
            }
            CatalogObjectKind::PrimaryKey
            | CatalogObjectKind::UniqueConstraint
            | CatalogObjectKind::Index => {
                if catalog_index_requires_blocker(object)? {
                    required.push((object.id.clone(), "index"));
                }
            }
            CatalogObjectKind::View => required.push((object.id.clone(), "view")),
            CatalogObjectKind::ForeignKey => {
                if !supported_foreign_keys.contains(&object.id) {
                    required.push((object.id.clone(), "foreign_key"));
                }
            }
            CatalogObjectKind::CheckConstraint => {
                required.push((object.id.clone(), "check_constraint"));
            }
            CatalogObjectKind::Trigger => required.push((object.id.clone(), "trigger")),
            CatalogObjectKind::Routine => required.push((object.id.clone(), "routine")),
            CatalogObjectKind::Event => required.push((object.id.clone(), "event")),
            CatalogObjectKind::Partition => required.push((object.id.clone(), "partition")),
            CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege" => {
                required.push((object.id.clone(), "privilege"));
            }
            _ => {}
        }
    }
    required.sort();
    required.dedup();
    Ok(required)
}

fn catalog_index_requires_blocker(object: &CatalogObject) -> Result<bool, MySqlPlanError> {
    let columns: Vec<MySqlIndexColumn> =
        serde_json::from_value(object.attributes.get("columns").cloned().ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(format!("{} lacks index column attributes", object.id))
        })?)?;
    Ok(required_text(object, "index_type")? != "BTREE"
        || !required_bool(object, "visible")?
        || columns.is_empty()
        || columns.iter().any(|column| {
            column.name.is_none()
                || !column.ascending
                || column.prefix_length.is_some()
                || column.expression.is_some()
        }))
}

/// Inspect two live MySQL endpoints and publish one protected reviewed plan.
pub fn write_live_plan(
    source_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    let source_config = MySqlEndpointConfig::read(source_config.as_ref())?;
    let target_config = MySqlEndpointConfig::read(target_config.as_ref())?;
    if source_config.credential_env == target_config.credential_env {
        return Err(MySqlPlanError::InvalidConfig(
            "source and target must use separate credential references",
        ));
    }
    let source = inspect_live_endpoint(source_config)?;
    let target = inspect_live_endpoint(target_config)?;
    let reviewed = build_plan(&source, &target)?;
    write_json_new(output.as_ref(), &reviewed)?;
    Ok(reviewed)
}

/// Inspect the source reader, distinct metadata administrator, and target,
/// then publish one protected reviewed MySQL plan.
pub fn write_live_plan_with_visibility(
    source_config: impl AsRef<Path>,
    source_metadata_admin_config: impl AsRef<Path>,
    freeze_admin_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    target_metadata_admin_config: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    let source_config = MySqlEndpointConfig::read(source_config.as_ref())?;
    let source_metadata_admin_config =
        MySqlEndpointConfig::read(source_metadata_admin_config.as_ref())?;
    let freeze_admin_config = MySqlEndpointConfig::read(freeze_admin_config.as_ref())?;
    let target_config = MySqlEndpointConfig::read(target_config.as_ref())?;
    let target_metadata_admin_config =
        MySqlEndpointConfig::read(target_metadata_admin_config.as_ref())?;
    let credential_references = [
        source_config.credential_env.as_str(),
        source_metadata_admin_config.credential_env.as_str(),
        freeze_admin_config.credential_env.as_str(),
        target_config.credential_env.as_str(),
        target_metadata_admin_config.credential_env.as_str(),
    ];
    if credential_references
        .iter()
        .enumerate()
        .any(|(index, value)| credential_references[..index].contains(value))
    {
        return Err(MySqlPlanError::InvalidConfig(
            "source reader, source metadata administrator, freeze administrator, target writer, and target metadata administrator must use separate credential references",
        ));
    }
    let source = inspect_live_endpoint(source_config.clone())?;
    let source_visibility = collect_mysql_metadata_visibility(
        &source,
        &source_config,
        &source_metadata_admin_config,
        Some(&freeze_admin_config),
    )?;
    let target = inspect_live_endpoint(target_config.clone())?;
    let target_visibility = collect_mysql_metadata_visibility(
        &target,
        &target_config,
        &target_metadata_admin_config,
        None,
    )?;
    let reviewed =
        build_plan_with_visibility(&source, &target, &source_visibility, &target_visibility)?;
    write_json_new(output.as_ref(), &reviewed)?;
    Ok(reviewed)
}

/// Inspect both endpoints and publish a reviewed plan that includes an
/// explicit protected business-account mapping artifact.
#[allow(clippy::too_many_arguments)]
pub fn write_live_plan_with_visibility_and_authorization(
    source_config: impl AsRef<Path>,
    source_metadata_admin_config: impl AsRef<Path>,
    freeze_admin_config: impl AsRef<Path>,
    target_config: impl AsRef<Path>,
    target_metadata_admin_config: impl AsRef<Path>,
    authorization_mapping: impl AsRef<Path>,
    output: impl AsRef<Path>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    let source_config = MySqlEndpointConfig::read(source_config.as_ref())?;
    let source_metadata_admin_config =
        MySqlEndpointConfig::read(source_metadata_admin_config.as_ref())?;
    let freeze_admin_config = MySqlEndpointConfig::read(freeze_admin_config.as_ref())?;
    let target_config = MySqlEndpointConfig::read(target_config.as_ref())?;
    let target_metadata_admin_config =
        MySqlEndpointConfig::read(target_metadata_admin_config.as_ref())?;
    let mapping: MySqlAuthorizationMapping = read_json(authorization_mapping.as_ref())?;
    mapping.validate()?;
    let credential_references = [
        source_config.credential_env.as_str(),
        source_metadata_admin_config.credential_env.as_str(),
        freeze_admin_config.credential_env.as_str(),
        target_config.credential_env.as_str(),
        target_metadata_admin_config.credential_env.as_str(),
    ];
    if credential_references
        .iter()
        .enumerate()
        .any(|(index, value)| credential_references[..index].contains(value))
    {
        return Err(MySqlPlanError::InvalidConfig(
            "source reader, source metadata administrator, freeze administrator, target writer, and target metadata administrator must use separate credential references",
        ));
    }
    let source = inspect_live_endpoint(source_config.clone())?;
    let source_visibility = collect_mysql_metadata_visibility(
        &source,
        &source_config,
        &source_metadata_admin_config,
        Some(&freeze_admin_config),
    )?;
    let target = inspect_live_endpoint(target_config.clone())?;
    let target_visibility = collect_mysql_metadata_visibility(
        &target,
        &target_config,
        &target_metadata_admin_config,
        None,
    )?;
    let reviewed = build_plan_with_visibility_and_authorization(
        &source,
        &target,
        &source_visibility,
        &target_visibility,
        mapping,
    )?;
    write_json_new(output.as_ref(), &reviewed)?;
    Ok(reviewed)
}

/// Attest the externally owned MySQL freeze without changing or releasing it.
pub fn attest_mysql_external_freeze(
    admin_config: &MySqlEndpointConfig,
    reviewed: &ReviewedPlan,
    assertion: &MySqlExternalFreezeAssertion,
) -> Result<MySqlExternalFreezeAttestation, MySqlPlanError> {
    admin_config.validate()?;
    reviewed.validate()?;
    assertion.validate()?;
    reviewed
        .plan
        .mysql_source_profile
        .as_ref()
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "reviewed MySQL plan has no external freeze profile".into(),
            )
        })?
        .validate()?;
    let source_evidence = reviewed
        .plan
        .mysql_snapshot_evidence
        .as_ref()
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "reviewed MySQL plan has no source snapshot evidence".into(),
            )
        })?;
    let mut conn = admin_config.connect()?;
    configure_mysql_session(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    conn.query_drop("SET SESSION information_schema_stats_expiry = 0")?;
    let identity = mysql_live_session_identity(&mut conn)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let endpoint_identity = format!(
        "mysql://{}:{}/{}?server_uuid={}",
        identity.hostname, identity.port, identity.database, identity.server_uuid
    );
    if endpoint_identity != reviewed.plan.source_endpoint_identity
        || identity.database != source_evidence.database_identity
        || identity.server_uuid != source_evidence.server_uuid
        || identity.server_version != source_evidence.server_version
        || identity.lower_case_table_names != source_evidence.lower_case_table_names
        || !mysql_session_settings_are_exact(&identity)
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL freeze administrator endpoint differs from the reviewed source".into(),
        ));
    }
    if identity.connection_id == assertion.backup_lock_connection_id {
        return Err(MySqlPlanError::InvalidCatalog(
            "the migration process must not own the external MySQL backup lock".into(),
        ));
    }
    let (read_only, super_read_only): (u8, u8) = conn
        .query_first("SELECT @@global.read_only, @@global.super_read_only")?
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog("MySQL global read-only query returned no row".into())
        })?;
    let persisted_super_read_only: Option<String> = conn.exec_first(
        "SELECT VARIABLE_VALUE FROM performance_schema.persisted_variables WHERE VARIABLE_NAME = 'super_read_only'",
        (),
    )?;
    let (server_now_unix_seconds, server_uptime_seconds): (u64, u64) = conn
        .query_first(
            "SELECT CAST(UNIX_TIMESTAMP() AS UNSIGNED), CAST(VARIABLE_VALUE AS UNSIGNED) FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'",
        )?
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "MySQL server-start evidence query returned no row".into(),
            )
        })?;
    let server_start_estimate = server_now_unix_seconds
        .checked_sub(server_uptime_seconds)
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "MySQL server uptime exceeds the observed server clock".into(),
            )
        })?;
    let server_start_lower_bound_unix_seconds = server_start_estimate.saturating_sub(1);
    let server_start_upper_bound_unix_seconds = server_start_estimate
        .checked_add(1)
        .ok_or_else(|| MySqlPlanError::InvalidCatalog("MySQL server clock overflow".into()))?;
    let lock_owner: Option<(u64, Option<String>, Option<String>)> = conn.exec_first(
        "SELECT ml.OWNER_THREAD_ID, t.PROCESSLIST_USER, t.PROCESSLIST_HOST FROM performance_schema.metadata_locks ml JOIN performance_schema.threads t ON t.THREAD_ID = ml.OWNER_THREAD_ID WHERE ml.OBJECT_TYPE = 'BACKUP LOCK' AND ml.LOCK_STATUS = 'GRANTED' AND t.PROCESSLIST_ID = ?",
        (assertion.backup_lock_connection_id,),
    )?;
    let (backup_lock_owner_thread_id, lock_owner_user, lock_owner_host) = lock_owner
        .and_then(|(thread_id, user, host)| Some((thread_id, user?, host?)))
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "the asserted external MySQL backup-lock owner is absent".into(),
            )
        })?;
    let mut active_replication_channels: Vec<String> = conn.query(
        "SELECT CHANNEL_NAME FROM performance_schema.replication_connection_status WHERE SERVICE_STATE = 'ON' UNION SELECT CHANNEL_NAME FROM performance_schema.replication_applier_status WHERE SERVICE_STATE = 'ON' ORDER BY CHANNEL_NAME",
    )?;
    active_replication_channels.sort();
    active_replication_channels.dedup();
    let attested_at_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MySqlPlanError::InvalidCatalog("system clock precedes Unix epoch".into()))?
        .as_secs();
    let attestation = MySqlExternalFreezeAttestation {
        schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
        profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
        status: MySqlFreezeAttestationStatus::Active,
        source_endpoint_identity: endpoint_identity,
        source_database_identity: identity.database,
        source_catalog_fingerprint: reviewed.plan.source_catalog_fingerprint.clone(),
        server_uuid: identity.server_uuid,
        server_start_lower_bound_unix_seconds,
        server_start_upper_bound_unix_seconds,
        administrator_tls_binding: mysql_tls_binding(admin_config)?,
        profile_generation: assertion.profile_generation.clone(),
        provider_reference: assertion.provider_reference.clone(),
        activated_at_unix_seconds: assertion.activated_at_unix_seconds,
        expires_at_unix_seconds: assertion.expires_at_unix_seconds,
        continuity_token_hash: assertion.continuity_token_hash.clone(),
        backup_lock_connection_id: assertion.backup_lock_connection_id,
        backup_lock_owner_thread_id,
        backup_lock_owner_user: lock_owner_user,
        backup_lock_owner_host: lock_owner_host,
        read_only: read_only == 1,
        super_read_only: super_read_only == 1,
        super_read_only_persisted: persisted_super_read_only
            .is_some_and(|value| value.eq_ignore_ascii_case("ON") || value == "1"),
        active_replication_channels,
        dml_mechanism: MySqlDmlFreezeMechanism::PersistedSuperReadOnly,
        ddl_mechanism: MySqlDdlFreezeMechanism::ExternalBackupLock,
        freeze_enforced_by_tool: false,
        attested_at_unix_seconds,
    };
    attestation.validate()?;
    if !attestation.matches_assertion(assertion) {
        return Err(MySqlPlanError::InvalidCatalog(
            "live MySQL freeze evidence differs from the external assertion".into(),
        ));
    }
    Ok(attestation)
}

pub fn validate_mysql_external_freeze_continuity(
    accepted: &MySqlExternalFreezeAttestation,
    current: &MySqlExternalFreezeAttestation,
) -> Result<(), MySqlPlanError> {
    accepted.validate()?;
    current.validate()?;
    if !current.same_continuity_as(accepted) {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL freeze continuity, server, or backup-lock owner changed".into(),
        ));
    }
    Ok(())
}

fn validate_supported_server_version(version: &str) -> Result<(), MySqlPlanError> {
    if version.starts_with("8.0.") || version.starts_with("8.4.") {
        Ok(())
    } else {
        Err(MySqlPlanError::InvalidCatalog(format!(
            "MySQL server version {version} is outside the reviewed 8.0/8.4 matrix"
        )))
    }
}

fn mysql_blocker(blocker: &MySqlCatalogBlocker) -> UnsupportedObject {
    let code = match blocker.object_kind.as_str() {
        "table" => UnsupportedObjectCode::MySqlStorageEngine,
        "resumable_key" => UnsupportedObjectCode::ResumableKey,
        _ => UnsupportedObjectCode::MySqlCatalogSemantics,
    };
    UnsupportedObject {
        code,
        object_id: blocker.object_id.clone(),
        object_kind: blocker.object_kind.clone(),
        reason: blocker.reason.clone(),
        required_semantics: true,
    }
}

fn default_port() -> u16 {
    DEFAULT_PORT
}

fn default_timeout_seconds() -> u64 {
    DEFAULT_TIMEOUT_SECONDS
}

fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}

fn default_batch_bytes() -> usize {
    DEFAULT_BATCH_BYTES
}

impl MySqlEndpointConfig {
    pub fn read(path: &Path) -> Result<Self, MySqlPlanError> {
        let contents = fs::read_to_string(path).map_err(MySqlPlanError::ReadConfig)?;
        let config: Self = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), MySqlPlanError> {
        if self.host.is_empty() || self.host.contains('\0') {
            return Err(MySqlPlanError::InvalidConfig(
                "host must be non-empty and must not contain NUL",
            ));
        }
        if self.port == 0 {
            return Err(MySqlPlanError::InvalidConfig("port must be nonzero"));
        }
        for value in [&self.database, &self.user, &self.credential_env] {
            if value.is_empty() || value.contains('\0') {
                return Err(MySqlPlanError::InvalidConfig(
                    "database, user, and credential reference must be non-empty and must not contain NUL",
                ));
            }
        }
        if !self
            .operational_server_administrators
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(MySqlPlanError::InvalidConfig(
                "operational server administrators must be sorted and unique",
            ));
        }
        for account in &self.operational_server_administrators {
            account.validate()?;
        }
        if self.connect_timeout_seconds == 0
            || self.max_batch_rows == 0
            || self.max_batch_bytes == 0
        {
            return Err(MySqlPlanError::InvalidConfig(
                "timeouts and batch limits must be nonzero",
            ));
        }
        match (
            &self.tls.client_identity_pkcs12,
            &self.tls.client_identity_password_env,
        ) {
            (Some(path), Some(reference)) => {
                validate_sensitive_file(path)?;
                if reference.is_empty() || reference.contains('\0') {
                    return Err(MySqlPlanError::InvalidConfig(
                        "TLS identity password reference is invalid",
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(MySqlPlanError::InvalidConfig(
                    "TLS client identity and password reference must be configured together",
                ));
            }
        }
        if let Some(path) = &self.tls.ca_certificate {
            validate_public_tls_file(path)?;
        }
        Ok(())
    }

    fn opts(&self) -> Result<OptsBuilder, MySqlPlanError> {
        self.validate()?;
        let password = std::env::var(&self.credential_env)
            .map_err(|_| MySqlPlanError::MissingCredential(self.credential_env.clone()))?;
        let mut ssl = SslOpts::default()
            .with_root_cert_path(self.tls.ca_certificate.clone())
            .with_danger_skip_domain_validation(self.tls.insecure)
            .with_danger_accept_invalid_certs(self.tls.insecure);
        if let (Some(path), Some(reference)) = (
            &self.tls.client_identity_pkcs12,
            &self.tls.client_identity_password_env,
        ) {
            let identity_password = std::env::var(reference)
                .map_err(|_| MySqlPlanError::MissingCredential(reference.clone()))?;
            let identity =
                mysql::ClientIdentity::new(path.clone()).with_password(identity_password);
            ssl = ssl.with_client_identity(Some(identity));
        }
        let timeout = Some(Duration::from_secs(self.connect_timeout_seconds));
        Ok(OptsBuilder::new()
            .ip_or_hostname(Some(self.host.clone()))
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(password))
            .db_name(Some(self.database.clone()))
            .prefer_socket(false)
            .tcp_connect_timeout(timeout)
            .read_timeout(timeout)
            .write_timeout(timeout)
            .ssl_opts(Some(ssl)))
    }

    fn connect(&self) -> Result<Conn, MySqlPlanError> {
        Ok(Conn::new(self.opts()?)?)
    }
}

fn validate_public_tls_file(path: &Path) -> Result<(), MySqlPlanError> {
    let metadata = fs::symlink_metadata(path).map_err(MySqlPlanError::TlsFile)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MySqlPlanError::InvalidConfig(
            "TLS CA certificate must be a regular file and not a symlink",
        ));
    }
    Ok(())
}

fn validate_sensitive_file(path: &Path) -> Result<(), MySqlPlanError> {
    let metadata = fs::symlink_metadata(path).map_err(MySqlPlanError::TlsFile)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MySqlPlanError::InvalidConfig(
            "TLS client identity must be a regular file and not a symlink",
        ));
    }
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no arguments and only reads the process credentials.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid || metadata.permissions().mode() & 0o077 != 0 {
            return Err(MySqlPlanError::InvalidConfig(
                "TLS client identity must be owned by the current user with mode 0600",
            ));
        }
    }
    Ok(())
}

pub fn mysql_tls_binding(config: &MySqlEndpointConfig) -> Result<String, MySqlPlanError> {
    config.validate()?;
    let policy = match (
        config.tls.insecure,
        config.tls.client_identity_pkcs12.is_some(),
    ) {
        (false, false) => "hostname_verified",
        (false, true) => "hostname_verified+mtls",
        (true, false) => "insecure_explicit",
        (true, true) => "insecure_explicit+mtls",
    };
    let roots = hash_optional_file(config.tls.ca_certificate.as_deref(), "platform")?;
    let client = hash_optional_file(config.tls.client_identity_pkcs12.as_deref(), "none")?;
    Ok(format!("{policy};roots={roots};client={client}"))
}

fn hash_optional_file(path: Option<&Path>, absent: &str) -> Result<String, MySqlPlanError> {
    match path {
        Some(path) => Ok(format!(
            "sha256:{}",
            hex::encode(Sha256::digest(
                fs::read(path).map_err(MySqlPlanError::TlsFile)?
            ))
        )),
        None => Ok(absent.into()),
    }
}

#[derive(Debug)]
struct PendingSnapshot {
    conn: Conn,
    registration: SessionRegistration,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    snapshot_evidence: MySqlSnapshotEvidence,
    catalog: VendorCatalog,
    blockers: Vec<MySqlCatalogBlocker>,
}

#[derive(Debug, Default)]
struct SessionRegistry {
    connection_ids: Mutex<BTreeSet<u32>>,
}

impl SessionRegistry {
    fn register(self: &Arc<Self>, connection_id: u32) -> ConnectionResult<SessionRegistration> {
        let inserted = self
            .connection_ids
            .lock()
            .map_err(|_| ConnectionError::Database("MySQL session registry is poisoned".into()))?
            .insert(connection_id);
        if !inserted {
            return Err(ConnectionError::Database(
                "MySQL connection ID is already registered".into(),
            ));
        }
        Ok(SessionRegistration {
            registry: Arc::clone(self),
            connection_id,
        })
    }

    fn connection_ids(&self) -> ConnectionResult<Vec<u32>> {
        Ok(self
            .connection_ids
            .lock()
            .map_err(|_| ConnectionError::Database("MySQL session registry is poisoned".into()))?
            .iter()
            .copied()
            .collect())
    }
}

#[derive(Debug)]
struct SessionRegistration {
    registry: Arc<SessionRegistry>,
    connection_id: u32,
}

impl Drop for SessionRegistration {
    fn drop(&mut self) {
        if let Ok(mut connection_ids) = self.registry.connection_ids.lock() {
            connection_ids.remove(&self.connection_id);
        }
    }
}

/// MySQL source factory that transfers one live consistent-snapshot session.
#[derive(Debug)]
pub struct MySqlSourceFactory {
    config: MySqlEndpointConfig,
    pending: Mutex<Option<PendingSnapshot>>,
    registry: Arc<SessionRegistry>,
    cancellation: CancellationToken,
}

impl MySqlSourceFactory {
    pub fn new(config: MySqlEndpointConfig) -> Self {
        Self::new_with_cancellation(config, CancellationToken::default())
    }

    pub fn new_with_cancellation(
        config: MySqlEndpointConfig,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            config,
            pending: Mutex::new(None),
            registry: Arc::default(),
            cancellation,
        }
    }

    pub(crate) fn endpoint_config(&self) -> &MySqlEndpointConfig {
        &self.config
    }

    pub(crate) fn validate_reviewed_binding(
        &self,
        reviewed: &ReviewedPlan,
    ) -> Result<(), MySqlPlanError> {
        let evidence = reviewed
            .plan
            .mysql_snapshot_evidence
            .as_ref()
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "reviewed MySQL plan has no source snapshot evidence".into(),
                )
            })?;
        if self.config.database != evidence.database_identity
            || reviewed.plan.source_endpoint_identity != evidence.endpoint_identity
            || reviewed.plan.source_tls_binding != mysql_tls_binding(&self.config)?
        {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL source factory differs from the reviewed endpoint binding".into(),
            ));
        }
        Ok(())
    }

    fn controlled_connect(&self) -> ConnectionResult<(Conn, SessionRegistration)> {
        self.cancellation.check()?;
        let mut conn = self.config.connect().map_err(plan_connection_error)?;
        let registration = self.registry.register(conn.connection_id())?;
        configure_mysql_session(&mut conn)?;
        self.cancellation.check()?;
        Ok((conn, registration))
    }

    pub fn captured_catalog(
        &self,
        snapshot: &SnapshotToken,
    ) -> ConnectionResult<(VendorCatalog, Vec<MySqlCatalogBlocker>, String)> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("MySQL snapshot lock is poisoned".into()))?;
        let pending = pending.as_ref().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending.token != snapshot {
            return Err(ConnectionError::SnapshotMismatch);
        }
        Ok((
            pending.catalog.clone(),
            pending.blockers.clone(),
            pending.snapshot_evidence.catalog_fingerprint.clone(),
        ))
    }

    pub fn snapshot_evidence(
        &self,
        snapshot: &SnapshotToken,
    ) -> ConnectionResult<MySqlSnapshotEvidence> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("MySQL snapshot lock is poisoned".into()))?;
        let pending = pending.as_ref().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending.token != snapshot {
            return Err(ConnectionError::SnapshotMismatch);
        }
        Ok(pending.snapshot_evidence.clone())
    }
}

impl SourceConnectionFactory for MySqlSourceFactory {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::from_entries([
            ("consistent_snapshot", Capability::Supported),
            ("server_read_only", Capability::Supported),
            ("transactions", Capability::Supported),
            ("typed_identifiers", Capability::Supported),
            ("bound_parameters", Capability::Supported),
            ("cancellation", Capability::Supported),
            (
                "snapshot_sharing",
                Capability::Unsupported {
                    reason: "Phase 6 uses one dedicated MySQL snapshot session".into(),
                },
            ),
            (
                "ddl_snapshot_consistency",
                Capability::Unsupported {
                    reason: "MySQL catalog consistency requires separate continuous DDL exclusion evidence"
                        .into(),
                },
            ),
        ])
    }

    fn capture_snapshot(&self) -> ConnectionResult<SnapshotToken> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| ConnectionError::Database("MySQL snapshot lock is poisoned".into()))?;
        if pending.is_some() {
            return Err(ConnectionError::InvalidRequest(
                "a MySQL snapshot is already waiting for a reader".into(),
            ));
        }
        let (mut conn, registration) = self.controlled_connect()?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .map_err(database_error)?;
        conn.query_drop("SET SESSION TRANSACTION READ ONLY")
            .map_err(database_error)?;
        conn.query_drop("SET SESSION information_schema_stats_expiry = 0")
            .map_err(database_error)?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .map_err(database_error)?;
        let identity = mysql_live_session_identity(&mut conn)?;
        if identity.database != self.config.database
            || !identity
                .transaction_isolation
                .eq_ignore_ascii_case("REPEATABLE-READ")
            || identity.transaction_read_only != 1
            || !mysql_session_settings_are_exact(&identity)
            || identity.connection_id != registration.connection_id
        {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let endpoint_identity = format!(
            "mysql://{}:{}/{}?server_uuid={}",
            identity.hostname, identity.port, identity.database, identity.server_uuid
        );
        let lifecycle_id = format!(
            "mysql-session-{}",
            SNAPSHOT_LIFECYCLE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (catalog, blockers) = extract_catalog(
            &mut conn,
            &identity.database,
            &identity.server_version,
            identity.lower_case_table_names,
        )?;
        let catalog_fingerprint = mysql_catalog_fingerprint(&catalog)?;
        let snapshot_id = mysql_snapshot_id(
            &endpoint_identity,
            &identity.server_uuid,
            &identity.gtid_executed_observation,
            identity.connection_id,
            &lifecycle_id,
        );
        let token = SnapshotToken {
            endpoint_identity: endpoint_identity.clone(),
            database_identity: identity.database.clone(),
            snapshot_id,
            consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
            server_version: identity.server_version.clone(),
            lifecycle_id: lifecycle_id.clone(),
        };
        let snapshot_evidence = MySqlSnapshotEvidence {
            endpoint_identity,
            database_identity: identity.database,
            server_uuid: identity.server_uuid,
            server_version: identity.server_version,
            authenticated_account: identity.authenticated_account,
            lifecycle_id,
            connection_id: identity.connection_id,
            transaction_isolation: identity.transaction_isolation,
            transaction_read_only: true,
            session_time_zone: identity.session_time_zone,
            catalog_snapshot_protected: false,
            information_schema_stats_expiry: identity.information_schema_stats_expiry,
            lower_case_table_names: identity.lower_case_table_names,
            session_sql_mode: identity.session_sql_mode,
            character_set_client: identity.character_set_client,
            character_set_connection: identity.character_set_connection,
            character_set_results: identity.character_set_results,
            collation_connection: identity.collation_connection,
            gtid_executed_observation: identity.gtid_executed_observation,
            catalog_fingerprint,
        };
        self.cancellation.check()?;
        *pending = Some(PendingSnapshot {
            conn,
            registration,
            token: token.clone(),
            evidence: ReadOnlyEvidence {
                server_enforced: true,
                description: "MySQL REPEATABLE READ transaction is server-side READ ONLY; DDL exclusion requires separate continuous profile evidence".into(),
            },
            snapshot_evidence,
            catalog,
            blockers,
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
            .map_err(|_| ConnectionError::Database("MySQL snapshot lock is poisoned".into()))?;
        let pending_snapshot = pending.take().ok_or(ConnectionError::SnapshotMismatch)?;
        if &pending_snapshot.token != snapshot {
            *pending = Some(pending_snapshot);
            return Err(ConnectionError::SnapshotMismatch);
        }
        let table_contracts = table_contracts(&pending_snapshot.catalog)?;
        Ok(Box::new(MySqlSnapshotReader {
            conn: pending_snapshot.conn,
            registration: Some(pending_snapshot.registration),
            token: pending_snapshot.token,
            evidence: pending_snapshot.evidence,
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            table_contracts,
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        if self.registry.connection_ids()?.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "capture a MySQL snapshot before opening its control session".into(),
            ));
        }
        Ok(Box::new(MySqlControlSession {
            config: self.config.clone(),
            registry: Arc::clone(&self.registry),
        }))
    }
}

struct MySqlControlSession {
    config: MySqlEndpointConfig,
    registry: Arc<SessionRegistry>,
}

impl ControlSession for MySqlControlSession {
    fn cancel_active_statement(&mut self) -> ConnectionResult<()> {
        let connection_ids = self.registry.connection_ids()?;
        if connection_ids.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "no active MySQL statement can be cancelled".into(),
            ));
        }
        let mut control = self.config.connect().map_err(plan_connection_error)?;
        for connection_id in connection_ids {
            control
                .query_drop(format!("KILL QUERY {connection_id}"))
                .map_err(database_error)?;
        }
        Ok(())
    }
}

/// MySQL target factory bound to the reviewed source table contracts.
#[derive(Debug)]
pub struct MySqlTargetFactory {
    config: MySqlEndpointConfig,
    reviewed_catalog: VendorCatalog,
    reviewed_tables: BTreeMap<QualifiedTable, MySqlTableDefinition>,
    reviewed_table_contracts: BTreeMap<QualifiedTable, TableContract>,
    cross_conversion_tables: BTreeMap<QualifiedTable, TableConversionPolicy>,
    reviewed_foreign_keys: BTreeMap<String, MySqlForeignKey>,
    target_evidence: MySqlSnapshotEvidence,
    registry: Arc<SessionRegistry>,
    cancellation: CancellationToken,
}

fn validate_cross_mysql_target_evidence(
    config: &MySqlEndpointConfig,
    evidence: &MySqlSnapshotEvidence,
) -> Result<(), MySqlPlanError> {
    if evidence.database_identity != config.database
        || evidence.lower_case_table_names > 2
        || evidence.session_sql_mode != MYSQL_STRICT_SQL_MODE
        || evidence.character_set_client != MYSQL_SESSION_CHARACTER_SET
        || evidence.character_set_connection != MYSQL_SESSION_CHARACTER_SET
        || evidence.character_set_results != MYSQL_SESSION_CHARACTER_SET
        || evidence.collation_connection != MYSQL_SESSION_COLLATION
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL target evidence does not bind the required session contract".into(),
        ));
    }
    Ok(())
}

impl MySqlTargetFactory {
    pub fn new(
        config: MySqlEndpointConfig,
        reviewed_catalog: VendorCatalog,
        target_evidence: MySqlSnapshotEvidence,
    ) -> Result<Self, MySqlPlanError> {
        Self::new_with_cancellation(
            config,
            reviewed_catalog,
            target_evidence,
            CancellationToken::default(),
        )
    }

    pub fn new_with_cancellation(
        config: MySqlEndpointConfig,
        reviewed_catalog: VendorCatalog,
        target_evidence: MySqlSnapshotEvidence,
        cancellation: CancellationToken,
    ) -> Result<Self, MySqlPlanError> {
        config.validate()?;
        validate_cross_mysql_target_evidence(&config, &target_evidence)?;
        let target_namespace = Identifier::new(config.database.clone())?;
        let reviewed_tables = mysql_table_definitions(&reviewed_catalog)?
            .into_iter()
            .map(|mut definition| {
                definition.table.namespace = target_namespace.clone();
                (definition.table.clone(), definition)
            })
            .collect();
        let reviewed_table_contracts = table_contracts(&reviewed_catalog)
            .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?
            .into_iter()
            .map(|(mut table, contract)| {
                table.namespace = target_namespace.clone();
                (table, contract)
            })
            .collect();
        let reviewed_foreign_keys = mysql_foreign_keys(&reviewed_catalog)
            .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?
            .into_iter()
            .map(|foreign_key| (foreign_key.catalog_object_id.clone(), foreign_key))
            .collect();
        Ok(Self {
            config,
            reviewed_catalog,
            reviewed_tables,
            reviewed_table_contracts,
            cross_conversion_tables: BTreeMap::new(),
            reviewed_foreign_keys,
            target_evidence,
            registry: Arc::default(),
            cancellation,
        })
    }

    /// Construct a MySQL target bound to reviewed PostgreSQL-to-MySQL table
    /// conversion policies. The empty target catalog remains independent from
    /// the source catalog; writer and verifier metadata comes only from the
    /// reviewed typed policies.
    pub fn new_cross_dialect_with_cancellation(
        config: MySqlEndpointConfig,
        reviewed_target_catalog: VendorCatalog,
        policies: Vec<TableConversionPolicy>,
        target_evidence: MySqlSnapshotEvidence,
        cancellation: CancellationToken,
    ) -> Result<Self, MySqlPlanError> {
        config.validate()?;
        validate_cross_mysql_target_evidence(&config, &target_evidence)?;
        if reviewed_target_catalog.dialect != "mysql"
            || reviewed_target_catalog.database.as_str() != config.database
        {
            return Err(MySqlPlanError::InvalidCatalog(
                "cross-dialect MySQL target catalog has the wrong dialect or database".into(),
            ));
        }
        let mut reviewed_tables = BTreeMap::new();
        let mut reviewed_table_contracts = BTreeMap::new();
        let mut cross_conversion_tables = BTreeMap::new();
        for policy in policies {
            policy.validate()?;
            if policy.row_policy.target_dialect != ConversionDialect::MySql
                || policy.target_table.namespace.as_str() != config.database
            {
                return Err(MySqlPlanError::InvalidCatalog(
                    "cross-dialect policy does not target the configured MySQL database".into(),
                ));
            }
            let definition = mysql_conversion_table_definition(&policy)?;
            let contract = mysql_conversion_table_contract(&policy, &target_evidence)?;
            if reviewed_tables
                .insert(policy.target_table.clone(), definition)
                .is_some()
                || reviewed_table_contracts
                    .insert(policy.target_table.clone(), contract)
                    .is_some()
                || cross_conversion_tables
                    .insert(policy.target_table.clone(), policy)
                    .is_some()
            {
                return Err(MySqlPlanError::InvalidCatalog(
                    "cross-dialect target contains a duplicate table policy".into(),
                ));
            }
        }
        if reviewed_tables.is_empty() {
            return Err(MySqlPlanError::InvalidCatalog(
                "cross-dialect MySQL target has no reviewed table policies".into(),
            ));
        }
        Ok(Self {
            config,
            reviewed_catalog: reviewed_target_catalog,
            reviewed_tables,
            reviewed_table_contracts,
            cross_conversion_tables,
            reviewed_foreign_keys: BTreeMap::new(),
            target_evidence,
            registry: Arc::default(),
            cancellation,
        })
    }

    pub(crate) fn endpoint_config(&self) -> &MySqlEndpointConfig {
        &self.config
    }

    #[cfg(feature = "migration-fault-injection")]
    pub(crate) fn install_write_mutation_trigger(
        &self,
        table: &QualifiedTable,
        column: &Identifier,
    ) -> ConnectionResult<()> {
        const TRIGGER_NAME: &str = "sql_splitter_fault_mutate_write";
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        let exists: Option<u64> = conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = ?",
                (&self.config.database, TRIGGER_NAME),
            )
            .map_err(database_error)?;
        if exists != Some(0) {
            return Ok(());
        }
        let statement = format!(
            "CREATE TRIGGER {}.{} BEFORE INSERT ON {}.{} FOR EACH ROW SET NEW.{} = CONCAT(NEW.{}, '-mutated')",
            quote_identifier(&table.namespace),
            quote_identifier(&Identifier::new(TRIGGER_NAME).map_err(|error| {
                ConnectionError::InvalidRequest(error.to_string())
            })?),
            quote_identifier(&table.namespace),
            quote_identifier(&table.name),
            quote_identifier(column),
            quote_identifier(column),
        );
        conn.query_drop(statement).map_err(database_error)?;
        self.cancellation.check()
    }

    pub(crate) fn validate_reviewed_binding(
        &self,
        reviewed: &ReviewedPlan,
    ) -> Result<(), MySqlPlanError> {
        let reviewed_source_catalog = reviewed.plan.source_catalog.as_ref().ok_or_else(|| {
            MySqlPlanError::InvalidCatalog("reviewed MySQL plan has no source catalog".into())
        })?;
        let reviewed_target_evidence = reviewed
            .plan
            .mysql_target_snapshot_evidence
            .as_ref()
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "reviewed MySQL plan has no target snapshot evidence".into(),
                )
            })?;
        let reviewed_target_endpoint = reviewed
            .plan
            .target_endpoint_identity
            .as_assessed()
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "reviewed MySQL plan has no target endpoint identity".into(),
                )
            })?;
        let reviewed_target_tls =
            reviewed
                .plan
                .target_tls_binding
                .as_assessed()
                .ok_or_else(|| {
                    MySqlPlanError::InvalidCatalog(
                        "reviewed MySQL plan has no target TLS binding".into(),
                    )
                })?;
        if &self.reviewed_catalog != reviewed_source_catalog
            || &self.target_evidence != reviewed_target_evidence
            || reviewed_target_endpoint != &self.target_evidence.endpoint_identity
            || self.config.database != self.target_evidence.database_identity
            || reviewed_target_tls != &mysql_tls_binding(&self.config)?
        {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL target factory differs from the reviewed endpoint or catalog binding".into(),
            ));
        }
        Ok(())
    }

    fn controlled_connect(&self) -> ConnectionResult<(Conn, SessionRegistration)> {
        self.cancellation.check()?;
        let mut conn = self.config.connect().map_err(plan_connection_error)?;
        let registration = self.registry.register(conn.connection_id())?;
        configure_mysql_session(&mut conn)?;
        conn.query_drop("SET SESSION information_schema_stats_expiry = 0")
            .map_err(database_error)?;
        let identity = mysql_live_session_identity(&mut conn)?;
        let endpoint_identity = format!(
            "mysql://{}:{}/{}?server_uuid={}",
            identity.hostname, identity.port, identity.database, identity.server_uuid
        );
        if identity.database != self.config.database
            || endpoint_identity != self.target_evidence.endpoint_identity
            || identity.server_uuid != self.target_evidence.server_uuid
            || identity.server_version != self.target_evidence.server_version
            || identity.lower_case_table_names != self.target_evidence.lower_case_table_names
            || !mysql_session_settings_are_exact(&identity)
            || identity.connection_id != registration.connection_id
        {
            return Err(ConnectionError::SnapshotMismatch);
        }
        self.cancellation.check()?;
        Ok((conn, registration))
    }

    fn current_catalog(&self) -> ConnectionResult<VendorCatalog> {
        let (mut conn, _registration) = self.controlled_connect()?;
        let (catalog, _) = extract_catalog(
            &mut conn,
            &self.config.database,
            &self.target_evidence.server_version,
            self.target_evidence.lower_case_table_names,
        )?;
        self.cancellation.check()?;
        Ok(catalog)
    }

    pub fn assert_empty(&self) -> ConnectionResult<()> {
        let catalog = self.current_catalog()?;
        let user_objects = catalog
            .namespaces
            .iter()
            .flat_map(|namespace| namespace.objects.iter())
            .filter(|object| {
                !matches!(
                    &object.kind,
                    CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege"
                )
            })
            .count();
        if user_objects == 0 {
            Ok(())
        } else {
            Err(ConnectionError::InvalidRequest(format!(
                "MySQL target contains {user_objects} user catalog objects"
            )))
        }
    }

    pub fn inspect_table(
        &self,
        expected: &MySqlTableDefinition,
    ) -> ConnectionResult<MySqlTableState> {
        validate_mysql_table_definition(expected)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        if self.reviewed_tables.get(&expected.table) != Some(expected) {
            return Err(ConnectionError::InvalidRequest(
                "MySQL table definition differs from the reviewed target contract".into(),
            ));
        }
        if expected.table.namespace.as_str() != self.config.database {
            return Err(ConnectionError::InvalidRequest(
                "MySQL table definition targets a different database".into(),
            ));
        }
        let catalog = self.current_catalog()?;
        let namespace = catalog
            .namespaces
            .iter()
            .find(|namespace| namespace.name == expected.table.namespace)
            .ok_or_else(|| ConnectionError::InvalidRequest("target database is absent".into()))?;
        let occupancies = namespace
            .objects
            .iter()
            .filter(|object| {
                matches!(
                    object.kind,
                    CatalogObjectKind::Table | CatalogObjectKind::View
                ) && object.name == expected.table.name
            })
            .collect::<Vec<_>>();
        if occupancies.is_empty() {
            return Ok(MySqlTableState::Absent);
        }
        if occupancies.len() != 1 || occupancies[0].kind != CatalogObjectKind::Table {
            return Ok(MySqlTableState::Different);
        }
        let observed = mysql_table_definition(namespace, occupancies[0])
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        Ok(if observed == *expected {
            MySqlTableState::Exact
        } else {
            MySqlTableState::Different
        })
    }

    pub fn create_table(&self, expected: &MySqlTableDefinition) -> ConnectionResult<()> {
        if self.inspect_table(expected)? != MySqlTableState::Absent {
            return Err(ConnectionError::InvalidRequest(
                "MySQL create-only table target is not absent".into(),
            ));
        }
        let ddl = render_mysql_create_table(expected)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        conn.query_drop(ddl).map_err(database_error)?;
        self.cancellation.check()?;
        if self.inspect_table(expected)? != MySqlTableState::Exact {
            return Err(ConnectionError::Database(
                "MySQL CREATE TABLE committed without the exact reviewed effect".into(),
            ));
        }
        Ok(())
    }

    /// Inspect one PostgreSQL-to-MySQL target table against its complete
    /// reviewed typed conversion policy.
    pub fn inspect_conversion_table(
        &self,
        policy: &TableConversionPolicy,
    ) -> ConnectionResult<MySqlTableState> {
        if self.cross_conversion_tables.get(&policy.target_table) != Some(policy) {
            return Err(ConnectionError::InvalidRequest(
                "MySQL conversion table policy differs from the reviewed target contract".into(),
            ));
        }
        let expected = mysql_conversion_table_definition(policy)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let catalog = self.current_catalog()?;
        let Some(namespace) = catalog
            .namespaces
            .iter()
            .find(|namespace| namespace.name == policy.target_table.namespace)
        else {
            return Ok(MySqlTableState::Absent);
        };
        let occupancies = namespace
            .objects
            .iter()
            .filter(|object| {
                matches!(
                    object.kind,
                    CatalogObjectKind::Table | CatalogObjectKind::View
                ) && object.name == policy.target_table.name
            })
            .collect::<Vec<_>>();
        if occupancies.is_empty() {
            return Ok(MySqlTableState::Absent);
        }
        if occupancies.len() != 1 || occupancies[0].kind != CatalogObjectKind::Table {
            return Ok(MySqlTableState::Different);
        }
        let observed = mysql_table_definition(namespace, occupancies[0])
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        if observed != expected || !mysql_conversion_checks_are_exact(namespace, policy)? {
            return Ok(MySqlTableState::Different);
        }
        Ok(MySqlTableState::Exact)
    }

    /// Apply one create-only cross-dialect target-table effect. MySQL DDL can
    /// commit implicitly, so the caller must persist Prepared first.
    pub fn create_conversion_table(&self, policy: &TableConversionPolicy) -> ConnectionResult<()> {
        match self.inspect_conversion_table(policy)? {
            MySqlTableState::Absent => {}
            MySqlTableState::Exact | MySqlTableState::Different => {
                return Err(ConnectionError::InvalidRequest(
                    "MySQL cross-dialect create-only target is not absent".into(),
                ));
            }
        }
        let ddl = render_mysql_conversion_create_table(policy)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        conn.query_drop(ddl).map_err(database_error)?;
        self.cancellation.check()?;
        if self.inspect_conversion_table(policy)? != MySqlTableState::Exact {
            return Err(ConnectionError::Database(
                "MySQL CREATE TABLE committed without the exact reviewed conversion effect".into(),
            ));
        }
        Ok(())
    }

    /// Verify the complete reviewed cross-dialect target inventory and return
    /// its stable policy fingerprint.
    pub fn verify_conversion_schema(
        &self,
        policies: &[TableConversionPolicy],
    ) -> ConnectionResult<String> {
        let expected = policies
            .iter()
            .map(|policy| (policy.target_table.clone(), policy))
            .collect::<BTreeMap<_, _>>();
        if expected.len() != policies.len()
            || expected.len() != self.cross_conversion_tables.len()
            || expected
                .iter()
                .any(|(table, policy)| self.cross_conversion_tables.get(table) != Some(*policy))
        {
            return Err(ConnectionError::InvalidRequest(
                "MySQL conversion schema policies differ from the reviewed target contract".into(),
            ));
        }
        let catalog = self.current_catalog()?;
        for namespace in &catalog.namespaces {
            for object in &namespace.objects {
                if matches!(&object.kind, CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege")
                {
                    continue;
                }
                let table_name = if object.kind == CatalogObjectKind::Table {
                    Some(object.name.clone())
                } else {
                    object
                        .attributes
                        .get("table_name")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|name| Identifier::new(name).ok())
                };
                let supported_kind = matches!(
                    object.kind,
                    CatalogObjectKind::Table
                        | CatalogObjectKind::Column
                        | CatalogObjectKind::PrimaryKey
                        | CatalogObjectKind::UniqueConstraint
                        | CatalogObjectKind::Index
                        | CatalogObjectKind::CheckConstraint
                );
                if !supported_kind
                    || table_name.is_none_or(|name| {
                        !expected.contains_key(&QualifiedTable {
                            namespace: namespace.name.clone(),
                            name,
                        })
                    })
                {
                    return Err(ConnectionError::InvalidRequest(format!(
                        "MySQL target contains unexpected catalog object {}",
                        object.id
                    )));
                }
            }
        }
        for policy in policies {
            if self.inspect_conversion_table(policy)? != MySqlTableState::Exact {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL conversion target table {}.{} is not exact",
                    policy.target_table.namespace, policy.target_table.name
                )));
            }
        }
        serde_json::to_vec(policies)
            .map(|bytes| hex::encode(Sha256::digest(bytes)))
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))
    }

    pub fn inspect_foreign_key(
        &self,
        expected: &MySqlForeignKey,
    ) -> ConnectionResult<MySqlForeignKeyState> {
        let expected = self.target_foreign_key(expected)?;
        let catalog = self.current_catalog()?;
        let observed = match mysql_foreign_keys(&catalog) {
            Ok(observed) => observed,
            Err(_) => return Ok(MySqlForeignKeyState::Different),
        };
        let same_name = observed
            .iter()
            .filter(|foreign_key| foreign_key.name == expected.name)
            .collect::<Vec<_>>();
        if same_name.is_empty() {
            return Ok(MySqlForeignKeyState::Absent);
        }
        Ok(
            if same_name.len() == 1 && mysql_foreign_key_semantics_eq(same_name[0], &expected) {
                MySqlForeignKeyState::Exact
            } else {
                MySqlForeignKeyState::Different
            },
        )
    }

    pub fn check_foreign_key(&self, expected: &MySqlForeignKey) -> ConnectionResult<bool> {
        let expected = self.target_foreign_key(expected)?;
        let statement = mysql_foreign_key_violation_query(&expected)?;
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        let result = match conn.query_first::<u8, _>(statement) {
            Ok(result) => result,
            Err(error) => {
                self.cancellation.check()?;
                return Err(database_error(error));
            }
        };
        let has_violation = result.ok_or_else(|| {
            ConnectionError::Database("MySQL foreign-key anti-join returned no result".into())
        })? != 0;
        self.cancellation.check()?;
        Ok(has_violation)
    }

    pub fn reconcile_foreign_key(
        &self,
        expected: &MySqlForeignKey,
    ) -> ConnectionResult<MySqlForeignKeyState> {
        match self.inspect_foreign_key(expected)? {
            MySqlForeignKeyState::Exact => return Ok(MySqlForeignKeyState::Exact),
            MySqlForeignKeyState::Different => {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL target foreign key {} has different semantics",
                    expected.name
                )));
            }
            MySqlForeignKeyState::Absent => {}
        }
        let target = self.target_foreign_key(expected)?;
        let statement = render_mysql_add_foreign_key(&target)?;
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        if let Err(error) = conn.query_drop(statement) {
            self.cancellation.check()?;
            return Err(mysql_foreign_key_ddl_error(error));
        }
        self.cancellation.check()?;
        self.inspect_foreign_key(expected)
    }

    /// Inspect effective business authorization without treating historical
    /// table/routine grantor provenance as an access-control semantic.
    pub fn inspect_authorization(
        &self,
        expected: &MySqlAuthorizationContract,
        initial: &MySqlMetadataVisibilityEvidence,
    ) -> ConnectionResult<MySqlAuthorizationTargetState> {
        expected
            .mapping
            .validate()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        expected
            .translated_inventory
            .validate()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        initial
            .validate()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let (mut conn, _registration) = self.controlled_connect()?;
        let observed = collect_mysql_grant_inventory(&mut conn, &self.config.database)
            .map_err(plan_connection_error)?;
        self.cancellation.check()?;
        classify_mysql_authorization_inventory(&observed, initial, &expected.translated_inventory)
    }

    /// Apply only missing reviewed grants. Every statement is create-only or
    /// idempotent. MySQL grant statements can commit implicitly, so callers
    /// must persist Prepared before this method and re-inspect on resume.
    pub fn reconcile_authorization(
        &self,
        expected: &MySqlAuthorizationContract,
        initial: &MySqlMetadataVisibilityEvidence,
    ) -> ConnectionResult<MySqlAuthorizationTargetState> {
        self.apply_authorization_statements(expected, initial, None)
    }

    /// Apply a deterministic prefix of the reviewed statements for the live
    /// implicit-commit recovery matrix.
    #[cfg(feature = "migration-fault-injection")]
    pub(crate) fn reconcile_authorization_prefix(
        &self,
        expected: &MySqlAuthorizationContract,
        initial: &MySqlMetadataVisibilityEvidence,
        statement_limit: usize,
    ) -> ConnectionResult<MySqlAuthorizationTargetState> {
        if statement_limit == 0 {
            return Err(ConnectionError::InvalidRequest(
                "MySQL authorization fault prefix must contain at least one statement".into(),
            ));
        }
        self.apply_authorization_statements(expected, initial, Some(statement_limit))
    }

    fn apply_authorization_statements(
        &self,
        expected: &MySqlAuthorizationContract,
        initial: &MySqlMetadataVisibilityEvidence,
        statement_limit: Option<usize>,
    ) -> ConnectionResult<MySqlAuthorizationTargetState> {
        match self.inspect_authorization(expected, initial)? {
            MySqlAuthorizationTargetState::Exact => {
                return Ok(MySqlAuthorizationTargetState::Exact)
            }
            MySqlAuthorizationTargetState::Different => {
                return Err(ConnectionError::InvalidRequest(
                    "MySQL target business authorization differs from the reviewed mapping".into(),
                ))
            }
            MySqlAuthorizationTargetState::Absent | MySqlAuthorizationTargetState::Subset => {}
        }
        let statements = render_mysql_authorization_statements(&expected.translated_inventory)?;
        let (mut conn, _registration) = self.controlled_connect()?;
        for statement in statements
            .into_iter()
            .take(statement_limit.unwrap_or(usize::MAX))
        {
            self.cancellation.check()?;
            if let Err(error) = conn.query_drop(statement) {
                self.cancellation.check()?;
                return Err(database_error(error));
            }
        }
        self.cancellation.check()?;
        self.inspect_authorization(expected, initial)
    }

    fn target_foreign_key(&self, expected: &MySqlForeignKey) -> ConnectionResult<MySqlForeignKey> {
        if self.reviewed_foreign_keys.get(&expected.catalog_object_id) != Some(expected) {
            return Err(ConnectionError::InvalidRequest(
                "MySQL foreign key differs from the reviewed source catalog".into(),
            ));
        }
        let mut target = expected.clone();
        target.table.namespace = Identifier::new(self.config.database.clone())
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        target.referenced_table.namespace = target.table.namespace.clone();
        Ok(target)
    }

    pub fn reviewed_table(
        &self,
        table: &QualifiedTable,
    ) -> ConnectionResult<&MySqlTableDefinition> {
        self.reviewed_tables
            .get(table)
            .ok_or_else(|| ConnectionError::TableNotFound(table.clone()))
    }

    /// Require the complete supported target schema to match the reviewed
    /// typed table contracts. Privilege rows are checked by the separate
    /// catalog-visibility admission contract.
    pub fn assert_exact_schema(&self) -> ConnectionResult<()> {
        let catalog = self.current_catalog()?;
        let has_unmodeled_object = catalog
            .namespaces
            .iter()
            .flat_map(|namespace| namespace.objects.iter())
            .any(|object| {
                !matches!(
                    &object.kind,
                    CatalogObjectKind::Table
                        | CatalogObjectKind::Column
                        | CatalogObjectKind::PrimaryKey
                        | CatalogObjectKind::UniqueConstraint
                        | CatalogObjectKind::Index
                        | CatalogObjectKind::ForeignKey
                ) && !matches!(
                    &object.kind,
                    CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege"
                )
            });
        if has_unmodeled_object {
            return Err(ConnectionError::InvalidRequest(
                "MySQL target contains an unreviewed schema object".into(),
            ));
        }
        let observed = mysql_table_definitions(&catalog)
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?
            .into_iter()
            .map(|definition| (definition.table.clone(), definition))
            .collect::<BTreeMap<_, _>>();
        if observed != self.reviewed_tables {
            return Err(ConnectionError::InvalidRequest(
                "MySQL target schema differs from the reviewed typed schema".into(),
            ));
        }
        let mut expected_foreign_keys = self
            .reviewed_foreign_keys
            .values()
            .map(|foreign_key| self.target_foreign_key(foreign_key))
            .collect::<ConnectionResult<Vec<_>>>()?;
        expected_foreign_keys.sort_by(|left, right| {
            (&left.table, &left.name, &left.catalog_object_id).cmp(&(
                &right.table,
                &right.name,
                &right.catalog_object_id,
            ))
        });
        let observed_foreign_keys = mysql_foreign_keys(&catalog)?;
        if observed_foreign_keys.len() != expected_foreign_keys.len()
            || expected_foreign_keys.iter().any(|expected| {
                observed_foreign_keys
                    .iter()
                    .filter(|observed| mysql_foreign_key_semantics_eq(observed, expected))
                    .count()
                    != 1
            })
        {
            return Err(ConnectionError::InvalidRequest(
                "MySQL target foreign keys differ from the reviewed schema".into(),
            ));
        }
        Ok(())
    }

    pub fn inspect_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<MySqlAutoIncrementTargetState> {
        if mapping.source != expected.table
            || mapping.target.namespace.as_str() != self.config.database
        {
            return Err(ConnectionError::InvalidRequest(
                "MySQL AUTO_INCREMENT state and table mapping disagree".into(),
            ));
        }
        let reviewed = self.reviewed_table(&mapping.target)?;
        if !reviewed.columns.iter().any(|column| {
            column.name == expected.column && column.auto_increment && !column.nullable
        }) {
            return Err(ConnectionError::InvalidRequest(
                "MySQL AUTO_INCREMENT column differs from the reviewed target contract".into(),
            ));
        }
        let catalog = self.current_catalog()?;
        let observed = mysql_auto_increment_states(&catalog)?
            .into_iter()
            .find(|state| state.table == mapping.target)
            .ok_or_else(|| {
                ConnectionError::InvalidRequest(
                    "MySQL target lacks the reviewed AUTO_INCREMENT state".into(),
                )
            })?;
        if observed.column != expected.column || observed.stats_expiry != 0 {
            return Ok(MySqlAutoIncrementTargetState::Different);
        }
        Ok(match (observed.next_value, expected.next_value) {
            (current, desired) if current == desired => MySqlAutoIncrementTargetState::Exact,
            (Some(current), Some(desired)) if current < desired => {
                MySqlAutoIncrementTargetState::BeforeDesired
            }
            (None, Some(_)) => MySqlAutoIncrementTargetState::BeforeDesired,
            _ => MySqlAutoIncrementTargetState::Different,
        })
    }

    pub fn restore_auto_increment(
        &self,
        expected: &MySqlAutoIncrementState,
        mapping: &MySqlTableMapping,
    ) -> ConnectionResult<()> {
        if self.inspect_auto_increment(expected, mapping)?
            != MySqlAutoIncrementTargetState::BeforeDesired
        {
            return Err(ConnectionError::InvalidRequest(
                "MySQL AUTO_INCREMENT target is not at an admissible pre-restore state".into(),
            ));
        }
        let desired = expected.next_value.ok_or_else(|| {
            ConnectionError::InvalidRequest(
                "MySQL AUTO_INCREMENT restoration has no desired next value".into(),
            )
        })?;
        let statement = format!(
            "ALTER TABLE {}.{} AUTO_INCREMENT = {desired}",
            quote_identifier(&mapping.target.namespace),
            quote_identifier(&mapping.target.name)
        );
        let (mut conn, _registration) = self.controlled_connect()?;
        self.cancellation.check()?;
        conn.query_drop(statement).map_err(database_error)?;
        self.cancellation.check()?;
        if self.inspect_auto_increment(expected, mapping)? != MySqlAutoIncrementTargetState::Exact {
            return Err(ConnectionError::Database(
                "MySQL AUTO_INCREMENT restoration did not produce the reviewed state".into(),
            ));
        }
        Ok(())
    }
}

impl TargetConnectionFactory for MySqlTargetFactory {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::from_entries([
            ("transactions", Capability::Supported),
            ("cancellation", Capability::Supported),
            ("typed_identifiers", Capability::Supported),
            ("bound_parameters", Capability::Supported),
            ("plain_insert", Capability::Supported),
            ("foreign_keys", Capability::Supported),
            (
                "bulk_write",
                Capability::Unsupported {
                    reason: "the first MySQL target increment uses reviewed bound INSERTs".into(),
                },
            ),
            (
                "transactional_ddl",
                Capability::Unsupported {
                    reason:
                        "MySQL DDL implicitly commits and uses one journal boundary per statement"
                            .into(),
                },
            ),
        ])
    }

    fn open_writer(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn WriteSession>> {
        let (conn, registration) = self.controlled_connect()?;
        let reviewed_projections = self
            .reviewed_tables
            .iter()
            .map(|(table, definition)| {
                (
                    table.clone(),
                    definition
                        .columns
                        .iter()
                        .map(|column| (column.name.clone(), column.data_type.clone()))
                        .collect(),
                )
            })
            .collect();
        Ok(Box::new(MySqlWriter {
            conn,
            registration: Some(registration),
            cancellation,
            transaction_open: false,
            reviewed_projections,
        }))
    }

    fn open_verifier(
        &self,
        cancellation: CancellationToken,
    ) -> ConnectionResult<Box<dyn VerificationSession>> {
        let (mut conn, registration) = self.controlled_connect()?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .map_err(database_error)?;
        conn.query_drop("SET SESSION TRANSACTION READ ONLY")
            .map_err(database_error)?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .map_err(database_error)?;
        Ok(Box::new(MySqlVerifier {
            conn,
            registration: Some(registration),
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            table_contracts: self.reviewed_table_contracts.clone(),
        }))
    }

    fn open_control(&self) -> ConnectionResult<Box<dyn ControlSession>> {
        if self.registry.connection_ids()?.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "open a MySQL target session before its control session".into(),
            ));
        }
        Ok(Box::new(MySqlControlSession {
            config: self.config.clone(),
            registry: Arc::clone(&self.registry),
        }))
    }
}

struct MySqlWriter {
    conn: Conn,
    registration: Option<SessionRegistration>,
    cancellation: CancellationToken,
    transaction_open: bool,
    reviewed_projections: BTreeMap<QualifiedTable, Vec<(Identifier, MySqlColumnType)>>,
}

impl Drop for MySqlWriter {
    fn drop(&mut self) {
        if self.transaction_open {
            let _ = self.conn.query_drop("ROLLBACK");
        }
        self.registration.take();
    }
}

impl WriteSession for MySqlWriter {
    fn begin(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if self.transaction_open {
            return Err(ConnectionError::TransactionAlreadyOpen);
        }
        self.conn
            .query_drop("START TRANSACTION")
            .map_err(database_error)?;
        self.transaction_open = true;
        Ok(())
    }

    fn insert(&mut self, table: &QualifiedTable, batch: &RowBatch) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        if batch.columns().is_empty() || batch.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL insert requires a nonempty batch and projection".into(),
            ));
        }
        validate_mysql_write_contract(&self.reviewed_projections, table, batch)?;
        let reviewed_columns = self
            .reviewed_projections
            .get(table)
            .ok_or_else(|| ConnectionError::TableNotFound(table.clone()))?;
        let columns = batch
            .columns()
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = reviewed_columns
            .iter()
            .map(|column| {
                if column.1 == MySqlColumnType::Json {
                    // MySQL rejects a binary-character-set parameter during
                    // implicit JSON conversion. Convert the source JSON bytes
                    // to reviewed UTF-8 text before parsing them as JSON. The
                    // target therefore parses the exact server-read number
                    // text instead of the digest-canonical serialization.
                    "CAST(CAST(? AS CHAR CHARACTER SET utf8mb4) AS JSON)"
                } else {
                    "?"
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!(
            "INSERT INTO {}.{} ({columns}) VALUES ({placeholders})",
            quote_identifier(&table.namespace),
            quote_identifier(&table.name)
        );
        let parameters = batch
            .rows()
            .iter()
            .map(|row| {
                row.iter()
                    .zip(batch.columns())
                    .zip(reviewed_columns)
                    .map(|((value, column), (_, data_type))| {
                        mysql_write_value(value, column, data_type)
                    })
                    .collect::<ConnectionResult<Vec<_>>>()
            })
            .collect::<ConnectionResult<Vec<_>>>()?;
        let statement = self.conn.prep(statement).map_err(database_error)?;
        for parameters in parameters {
            if let Err(error) = self.conn.exec_drop(&statement, parameters) {
                return match self.cancellation.check() {
                    Err(cancellation) => Err(cancellation),
                    Ok(()) => Err(database_error(error)),
                };
            }
            require_mysql_insert_affected_rows(self.conn.affected_rows())?;
            self.cancellation.check()?;
        }
        Ok(())
    }

    fn commit(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        self.transaction_open = false;
        self.conn.query_drop("COMMIT").map_err(|error| {
            ConnectionError::CommitOutcomeUnknown(MySqlSafeError::from(error).to_string())
        })
    }

    fn rollback(&mut self) -> ConnectionResult<()> {
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        self.transaction_open = false;
        self.conn.query_drop("ROLLBACK").map_err(database_error)
    }
}

fn validate_mysql_write_contract(
    reviewed_projections: &BTreeMap<QualifiedTable, Vec<(Identifier, MySqlColumnType)>>,
    table: &QualifiedTable,
    batch: &RowBatch,
) -> ConnectionResult<()> {
    let expected = reviewed_projections
        .get(table)
        .ok_or_else(|| ConnectionError::TableNotFound(table.clone()))?;
    if !batch
        .columns()
        .iter()
        .map(|column| &column.name)
        .eq(expected.iter().map(|column| &column.0))
    {
        return Err(ConnectionError::InvalidRequest(
            "MySQL write projection differs from the reviewed table contract".into(),
        ));
    }
    Ok(())
}

fn require_mysql_insert_affected_rows(affected_rows: u64) -> ConnectionResult<()> {
    if affected_rows != 1 {
        return Err(ConnectionError::InvalidRequest(format!(
            "MySQL INSERT affected {affected_rows} rows; exact execution requires one"
        )));
    }
    Ok(())
}

struct MySqlVerifier {
    conn: Conn,
    registration: Option<SessionRegistration>,
    cancellation: CancellationToken,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    table_contracts: BTreeMap<QualifiedTable, TableContract>,
}

impl Drop for MySqlVerifier {
    fn drop(&mut self) {
        let _ = self.conn.query_drop("ROLLBACK");
        self.registration.take();
    }
}

impl VerificationSession for MySqlVerifier {
    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        let batch = match mysql_select_page(
            &mut self.conn,
            &self.table_contracts,
            self.max_batch_rows,
            self.max_batch_bytes,
            request,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                return match self.cancellation.check() {
                    Err(cancellation) => Err(cancellation),
                    Ok(()) => Err(error),
                };
            }
        };
        self.cancellation.check()?;
        Ok(batch)
    }
}

#[derive(Debug, Clone)]
struct TableContract {
    columns: BTreeMap<String, ColumnMeta>,
    key: MySqlResumableKey,
}

struct MySqlSnapshotReader {
    conn: Conn,
    registration: Option<SessionRegistration>,
    token: SnapshotToken,
    evidence: ReadOnlyEvidence,
    cancellation: CancellationToken,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    table_contracts: BTreeMap<QualifiedTable, TableContract>,
}

impl Drop for MySqlSnapshotReader {
    fn drop(&mut self) {
        let _ = self.conn.query_drop("ROLLBACK");
        self.registration.take();
    }
}

impl ReadSession for MySqlSnapshotReader {
    fn read_only_evidence(&self) -> &ReadOnlyEvidence {
        &self.evidence
    }

    fn snapshot(&self) -> &SnapshotToken {
        &self.token
    }

    fn select_page(&mut self, request: &KeysetPage) -> ConnectionResult<RowBatch> {
        self.cancellation.check()?;
        let batch = match mysql_select_page(
            &mut self.conn,
            &self.table_contracts,
            self.max_batch_rows,
            self.max_batch_bytes,
            request,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                return match self.cancellation.check() {
                    Err(cancellation) => Err(cancellation),
                    Ok(()) => Err(error),
                };
            }
        };
        self.cancellation.check()?;
        Ok(batch)
    }
}

fn mysql_select_page(
    conn: &mut Conn,
    table_contracts: &BTreeMap<QualifiedTable, TableContract>,
    max_batch_rows: usize,
    max_batch_bytes: usize,
    request: &KeysetPage,
) -> ConnectionResult<RowBatch> {
    if request.limit == 0 || request.projection.is_empty() || request.key.is_empty() {
        return Err(ConnectionError::InvalidRequest(
            "MySQL page requires a nonzero limit, projection, and complete key".into(),
        ));
    }
    let contract = table_contracts
        .get(&request.table)
        .ok_or_else(|| ConnectionError::TableNotFound(request.table.clone()))?;
    if request.key != contract.key.columns {
        return Err(ConnectionError::InvalidRequest(
            "MySQL page key differs from the reviewed resumable-key contract".into(),
        ));
    }
    if request
        .key
        .iter()
        .any(|key_column| !request.projection.contains(key_column))
    {
        return Err(ConnectionError::InvalidRequest(
            "MySQL page projection must contain the complete resumable key".into(),
        ));
    }
    if let Some(after) = &request.after {
        if after.as_slice().len() != request.key.len() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL page cursor does not contain the complete key".into(),
            ));
        }
    }
    let columns = request
        .projection
        .iter()
        .map(|column| {
            contract
                .columns
                .get(column.as_str())
                .cloned()
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL projection contains unknown column {column}"
                    ))
                })
        })
        .collect::<ConnectionResult<Vec<_>>>()?;
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
    let mut sql = format!(
        "SELECT {projection} FROM {}.{}",
        quote_identifier(&request.table.namespace),
        quote_identifier(&request.table.name)
    );
    let params = if let Some(after) = &request.after {
        sql.push_str(&format!(
            " WHERE ({key}) > ({})",
            std::iter::repeat_n("?", request.key.len())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        Params::Positional(
            after
                .as_slice()
                .iter()
                .map(mysql_parameter)
                .collect::<ConnectionResult<Vec<_>>>()?,
        )
    } else {
        Params::Empty
    };
    let limit = usize::min(request.limit as usize, max_batch_rows);
    sql.push_str(&format!(" ORDER BY {key} LIMIT {limit}"));
    let mut batch = RowBatch::new(columns.clone(), limit, max_batch_bytes);
    let mut rows = conn.exec_iter(sql, params).map_err(database_error)?;
    for row in rows.by_ref() {
        let values = row.map_err(database_error)?.unwrap();
        let converted = values
            .into_iter()
            .zip(&columns)
            .map(|(value, column)| mysql_value(value, column))
            .collect::<ConnectionResult<Vec<_>>>()?;
        let encoded_bytes = serde_json::to_vec(&converted)
            .map_err(|error| ConnectionError::Database(error.to_string()))?
            .len();
        match batch.try_push(converted, encoded_bytes) {
            Ok(()) => {}
            Err(RowBatchError::ByteLimit { .. }) if !batch.is_empty() => break,
            Err(error) => return Err(ConnectionError::BatchLimit(error.to_string())),
        }
    }
    drop(rows);
    Ok(batch)
}

fn mysql_parameter(value: &DbValue) -> ConnectionResult<Value> {
    match value {
        DbValue::Null => Ok(Value::NULL),
        DbValue::Bool(value) => Ok(Value::Int(i64::from(*value))),
        DbValue::Signed(value) => i64::try_from(*value)
            .map(Value::Int)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        DbValue::Unsigned(value) => u64::try_from(*value)
            .map(Value::UInt)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        DbValue::Text(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
        DbValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        _ => Err(ConnectionError::UnsupportedKeyValue),
    }
}

fn mysql_write_value(
    value: &DbValue,
    column: &ColumnMeta,
    data_type: &MySqlColumnType,
) -> ConnectionResult<Value> {
    validate_mysql_write_value_type(value, column, data_type)?;
    match value {
        DbValue::Null => Ok(Value::NULL),
        DbValue::Bool(value) => Ok(Value::Int(i64::from(*value))),
        DbValue::Signed(value) => i64::try_from(*value)
            .map(Value::Int)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        DbValue::Unsigned(value) => u64::try_from(*value)
            .map(Value::UInt)
            .map_err(|_| ConnectionError::UnsupportedKeyValue),
        DbValue::Decimal { coefficient, scale } => {
            Ok(Value::Bytes(render_mysql_decimal(coefficient, *scale)?))
        }
        DbValue::Float32(bits) => Ok(Value::Float(f32::from_bits(*bits))),
        DbValue::Float64(bits) => Ok(Value::Double(f64::from_bits(*bits))),
        DbValue::Text(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
        DbValue::Bytes(value) if column.vendor_type == "bit" => {
            if value.len() > std::mem::size_of::<u64>() {
                return Err(ConnectionError::UnsupportedKeyValue);
            }
            let decoded = value
                .iter()
                .fold(0_u64, |decoded, byte| (decoded << 8) | u64::from(*byte));
            let width = column.precision.ok_or_else(|| {
                ConnectionError::InvalidRequest("MySQL BIT column has no reviewed width".into())
            })?;
            if width == 0 || width > 64 || (width < 64 && decoded >= 1_u64 << width) {
                return Err(ConnectionError::UnsupportedKeyValue);
            }
            Ok(Value::UInt(decoded))
        }
        DbValue::Bytes(value) => Ok(Value::Bytes(value.clone())),
        // The source server's JSON text is the wire value. Canonical JSON is
        // used only for comparison digests; writing it would change MySQL's
        // observable INTEGER/DOUBLE representation for some numbers.
        DbValue::Json(value) => {
            canonicalize_json(value)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            Ok(Value::Bytes(value.clone()))
        }
        DbValue::Date { year, month, day } => Ok(Value::Date(
            u16::try_from(*year).map_err(|_| ConnectionError::UnsupportedKeyValue)?,
            *month,
            *day,
            0,
            0,
            0,
            0,
        )),
        DbValue::Time { nanos } => {
            let negative = *nanos < 0;
            let magnitude = nanos
                .checked_abs()
                .ok_or(ConnectionError::UnsupportedKeyValue)?;
            if magnitude % 1_000 != 0 {
                return Err(ConnectionError::UnsupportedKeyValue);
            }
            let total_seconds = magnitude / 1_000_000_000;
            let days = u32::try_from(total_seconds / 86_400)
                .map_err(|_| ConnectionError::UnsupportedKeyValue)?;
            let remaining = total_seconds % 86_400;
            let hours = u8::try_from(remaining / 3_600)
                .map_err(|_| ConnectionError::UnsupportedKeyValue)?;
            let minutes = u8::try_from((remaining % 3_600) / 60)
                .map_err(|_| ConnectionError::UnsupportedKeyValue)?;
            let seconds =
                u8::try_from(remaining % 60).map_err(|_| ConnectionError::UnsupportedKeyValue)?;
            let micros = u32::try_from((magnitude % 1_000_000_000) / 1_000)
                .map_err(|_| ConnectionError::UnsupportedKeyValue)?;
            Ok(Value::Time(negative, days, hours, minutes, seconds, micros))
        }
        DbValue::Timestamp { local, .. } => Ok(Value::Bytes(local.as_bytes().to_vec())),
        DbValue::Vendor { .. } => Err(ConnectionError::UnsupportedKeyValue),
    }
}

fn validate_mysql_write_value_type(
    value: &DbValue,
    column: &ColumnMeta,
    data_type: &MySqlColumnType,
) -> ConnectionResult<()> {
    if matches!(value, DbValue::Null) {
        return if column.nullable {
            Ok(())
        } else {
            Err(ConnectionError::InvalidRequest(format!(
                "MySQL non-null column {} received NULL",
                column.name
            )))
        };
    }
    let exact = match data_type {
        MySqlColumnType::Integer { unsigned, .. } => {
            if *unsigned {
                matches!(value, DbValue::Unsigned(_))
                    || matches!(value, DbValue::Signed(value) if *value >= 0)
            } else {
                matches!(value, DbValue::Signed(_))
                    || matches!(value, DbValue::Unsigned(value) if *value <= i64::MAX as u128)
            }
        }
        MySqlColumnType::Decimal {
            precision,
            scale,
            unsigned,
        } => match value {
            DbValue::Decimal {
                coefficient,
                scale: value_scale,
            } => {
                let negative = coefficient.first() == Some(&b'-');
                let digits = coefficient.strip_prefix(b"-").unwrap_or(coefficient);
                *value_scale == *scale as i32
                    && (!*unsigned || !negative)
                    && !digits.is_empty()
                    && digits.iter().all(u8::is_ascii_digit)
                    && digits.len() <= *precision as usize
            }
            _ => false,
        },
        MySqlColumnType::Floating { name, .. } if name == "float" => {
            matches!(value, DbValue::Float32(_))
        }
        MySqlColumnType::Floating { name, .. } if name == "double" => {
            matches!(value, DbValue::Float64(_))
        }
        MySqlColumnType::Floating { .. } => false,
        MySqlColumnType::Bit { .. } => matches!(value, DbValue::Bytes(_)),
        MySqlColumnType::Temporal { name, .. } if name == "date" => {
            matches!(value, DbValue::Date { .. })
        }
        MySqlColumnType::Temporal {
            name,
            fractional_precision,
        } if name == "time" => match value {
            DbValue::Time { nanos } => {
                mysql_temporal_value_has_exact_precision(*nanos, *fractional_precision)
            }
            _ => false,
        },
        MySqlColumnType::Temporal {
            name,
            fractional_precision,
        } if matches!(name.as_str(), "datetime" | "timestamp") => {
            matches!(
                value,
                DbValue::Timestamp { precision, .. }
                    if u32::from(*precision) == fractional_precision.unwrap_or(0)
            )
        }
        MySqlColumnType::Temporal { .. } => false,
        MySqlColumnType::Year => matches!(value, DbValue::Signed(_)),
        MySqlColumnType::Character { .. } | MySqlColumnType::Text { .. } => {
            matches!(value, DbValue::Text(_))
        }
        MySqlColumnType::Binary { .. } | MySqlColumnType::Blob { .. } => {
            matches!(value, DbValue::Bytes(_))
        }
        MySqlColumnType::Json => matches!(value, DbValue::Json(_)),
    };
    if !exact {
        return Err(ConnectionError::InvalidRequest(format!(
            "MySQL value type for column {} differs from the reviewed catalog type",
            column.name
        )));
    }
    Ok(())
}

fn mysql_temporal_value_has_exact_precision(
    nanos: i128,
    fractional_precision: Option<u32>,
) -> bool {
    let precision = fractional_precision.unwrap_or(0);
    let Some(exponent) = 9_u32.checked_sub(precision) else {
        return false;
    };
    let quantum = 10_i128.pow(exponent);
    nanos % quantum == 0
}

fn render_mysql_decimal(coefficient: &[u8], scale: i32) -> ConnectionResult<Vec<u8>> {
    let scale = usize::try_from(scale).map_err(|_| ConnectionError::UnsupportedKeyValue)?;
    let coefficient =
        std::str::from_utf8(coefficient).map_err(|_| ConnectionError::UnsupportedKeyValue)?;
    let (negative, digits) = coefficient
        .strip_prefix('-')
        .map_or((false, coefficient), |digits| (true, digits));
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ConnectionError::UnsupportedKeyValue);
    }
    let mut rendered = String::new();
    if negative && digits.bytes().any(|digit| digit != b'0') {
        rendered.push('-');
    }
    if scale == 0 {
        rendered.push_str(digits);
    } else if digits.len() <= scale {
        rendered.push_str("0.");
        rendered.extend(std::iter::repeat_n('0', scale - digits.len()));
        rendered.push_str(digits);
    } else {
        let split = digits.len() - scale;
        rendered.push_str(&digits[..split]);
        rendered.push('.');
        rendered.push_str(&digits[split..]);
    }
    Ok(rendered.into_bytes())
}

fn mysql_value(value: Value, column: &ColumnMeta) -> ConnectionResult<DbValue> {
    match value {
        Value::NULL => Ok(DbValue::Null),
        Value::Int(value) => Ok(DbValue::Signed(i128::from(value))),
        Value::UInt(value) => Ok(DbValue::Unsigned(u128::from(value))),
        Value::Float(value) => Ok(DbValue::Float32(value.to_bits())),
        Value::Double(value) => Ok(DbValue::Float64(value.to_bits())),
        Value::Bytes(value) => match column.vendor_type.as_str() {
            "json" => {
                canonicalize_json(&value)
                    .map_err(|error| ConnectionError::Database(error.to_string()))?;
                Ok(DbValue::Json(value))
            }
            "bit" | "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
                Ok(DbValue::Bytes(value))
            }
            "decimal" | "numeric" => decimal_value(&value, column.scale.unwrap_or(0)),
            _ => String::from_utf8(value)
                .map(DbValue::Text)
                .map_err(|_| ConnectionError::Database("MySQL text value is not UTF-8".into())),
        },
        Value::Date(year, month, day, hour, minute, second, micros) => {
            if hour == 0
                && minute == 0
                && second == 0
                && micros == 0
                && column.vendor_type == "date"
            {
                Ok(DbValue::Date {
                    year: i32::from(year),
                    month,
                    day,
                })
            } else {
                let precision = column.precision.unwrap_or(0).min(6) as u8;
                Ok(DbValue::Timestamp {
                    local: format!(
                        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
                    ),
                    offset_minutes: None,
                    precision,
                })
            }
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let total_seconds = i128::from(days) * 86_400
                + i128::from(hours) * 3_600
                + i128::from(minutes) * 60
                + i128::from(seconds);
            let nanos = total_seconds * 1_000_000_000 + i128::from(micros) * 1_000;
            Ok(DbValue::Time {
                nanos: if negative { -nanos } else { nanos },
            })
        }
    }
}

fn decimal_value(bytes: &[u8], scale: i32) -> ConnectionResult<DbValue> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ConnectionError::Database("MySQL decimal is not ASCII".into()))?;
    let negative = text.starts_with('-');
    let digits = text
        .trim_start_matches(['-', '+'])
        .replace('.', "")
        .trim_start_matches('0')
        .to_string();
    let mut coefficient = if digits.is_empty() {
        vec![0]
    } else {
        digits.into_bytes()
    };
    if negative && coefficient != [0] {
        coefficient.insert(0, b'-');
    }
    Ok(DbValue::Decimal { coefficient, scale })
}

fn table_contracts(
    catalog: &VendorCatalog,
) -> ConnectionResult<BTreeMap<QualifiedTable, TableContract>> {
    let mut contracts = BTreeMap::new();
    for namespace in &catalog.namespaces {
        let mut columns_by_table = BTreeMap::<String, BTreeMap<String, ColumnMeta>>::new();
        for object in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Column)
        {
            let table_name = object
                .attributes
                .get("table_name")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL column {} has no table identity",
                        object.id
                    ))
                })?;
            let meta = column_meta(object)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let previous = columns_by_table
                .entry(table_name.into())
                .or_default()
                .insert(object.name.as_str().into(), meta);
            if previous.is_some() {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL table {table_name} has duplicate column {}",
                    object.name
                )));
            }
        }
        for table in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Table)
        {
            let Some(key_value) = table.attributes.get("resumable_key") else {
                continue;
            };
            if key_value.is_null() {
                continue;
            }
            let key: MySqlResumableKey = serde_json::from_value(key_value.clone())
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let columns = columns_by_table
                .get(table.name.as_str())
                .cloned()
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL table {} has no column contract",
                        table.name
                    ))
                })?;
            contracts.insert(
                QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: table.name.clone(),
                },
                TableContract { columns, key },
            );
        }
    }
    Ok(contracts)
}

pub fn mysql_table_definitions(
    catalog: &VendorCatalog,
) -> Result<Vec<MySqlTableDefinition>, MySqlPlanError> {
    if catalog.dialect != "mysql" || catalog.format_version != MYSQL_CATALOG_FORMAT_VERSION {
        return Err(MySqlPlanError::InvalidCatalog(
            "table definitions require the current MySQL catalog format".into(),
        ));
    }
    let mut definitions = Vec::new();
    for namespace in &catalog.namespaces {
        for table in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Table)
        {
            definitions.push(mysql_table_definition(namespace, table)?);
        }
    }
    definitions.sort_by(|left, right| left.table.cmp(&right.table));
    Ok(definitions)
}

/// Derive one MySQL-to-PostgreSQL table policy from the exact source catalog.
pub fn mysql_to_postgres_table_conversion_policy(
    catalog: &VendorCatalog,
    source_table: &QualifiedTable,
    target_table: QualifiedTable,
    postgres_text_collation: QualifiedIdentifier,
) -> Result<TableConversionPolicy, MySqlPlanError> {
    if catalog.dialect != "mysql" || catalog.format_version != MYSQL_CATALOG_FORMAT_VERSION {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL conversion requires the current catalog format".into(),
        ));
    }
    let source_namespace = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == source_table.namespace)
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "MySQL conversion source namespace is absent from the reviewed catalog".into(),
            )
        })?;
    let source_table_object = source_namespace
        .objects
        .iter()
        .find(|object| object.kind == CatalogObjectKind::Table && object.name == source_table.name)
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "MySQL conversion source table is absent from the reviewed catalog".into(),
            )
        })?;
    let source_key: MySqlResumableKey = serde_json::from_value(
        source_table_object
            .attributes
            .get("resumable_key")
            .filter(|value| !value.is_null())
            .cloned()
            .ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL conversion source table has no reviewed resumable key".into(),
                )
            })?,
    )?;
    let definition = mysql_table_definitions(catalog)?
        .into_iter()
        .find(|definition| &definition.table == source_table)
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(
                "MySQL conversion source table is absent from the reviewed catalog".into(),
            )
        })?;
    if definition
        .columns
        .iter()
        .any(|column| column.auto_increment)
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL AUTO_INCREMENT requires a separate cross-dialect mapping".into(),
        ));
    }
    let columns = definition
        .columns
        .iter()
        .map(|column| {
            let source_type = mysql_conversion_source_type(column)?;
            let source = super::conversion::CrossDialectTargetType::MySql(source_type.clone())
                .column_meta(
                    column.name.clone(),
                    u32::try_from(column.ordinal).map_err(|_| {
                        MySqlPlanError::InvalidCatalog(
                            "MySQL conversion column ordinal is too large".into(),
                        )
                    })?,
                    column.nullable,
                )?;
            derive_mysql_to_postgres_column(
                source,
                source_type,
                column.name.clone(),
                u32::try_from(column.ordinal).map_err(|_| {
                    MySqlPlanError::InvalidCatalog(
                        "MySQL conversion column ordinal is too large".into(),
                    )
                })?,
                postgres_text_collation.clone(),
            )
            .map_err(MySqlPlanError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_policy = RowConversionPolicy {
        schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
        source_dialect: ConversionDialect::MySql,
        target_dialect: ConversionDialect::PostgreSql,
        columns,
    };
    row_policy.validate()?;
    let source_kind = if source_key.primary {
        CrossDialectKeyKind::PrimaryKey
    } else {
        CrossDialectKeyKind::Unique
    };
    let target_name = matches!(source_kind, CrossDialectKeyKind::Unique)
        .then(|| cross_dialect_target_key_name(&target_table, &source_key.columns))
        .transpose()?;
    let policy = TableConversionPolicy {
        source_table: source_table.clone(),
        target_table,
        target_contract: CrossDialectTargetTableContract::PostgreSql {
            persistence: PostgresTargetPersistence::Permanent,
        },
        resumable_key: CrossDialectResumableKey {
            source_name: source_key.index_name,
            source_kind,
            source_columns: source_key.columns.clone(),
            target_name,
            target_kind: source_kind,
            target_columns: source_key.columns,
        },
        row_policy,
    };
    policy.validate()?;
    Ok(policy)
}

/// Render a create-only MySQL table from a reviewed typed conversion policy.
pub fn render_mysql_conversion_create_table(
    policy: &TableConversionPolicy,
) -> Result<String, MySqlPlanError> {
    policy.validate()?;
    let CrossDialectTargetTableContract::MySql {
        engine: MySqlTargetEngine::InnoDb,
        character_set,
        collation,
    } = &policy.target_contract
    else {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL conversion DDL requires a reviewed MySQL target contract".into(),
        ));
    };
    if policy.row_policy.target_dialect != ConversionDialect::MySql {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL conversion DDL has the wrong target dialect".into(),
        ));
    }
    let mut columns = Vec::with_capacity(policy.row_policy.columns.len());
    for column in &policy.row_policy.columns {
        let CrossDialectTargetType::MySql(target_type) = &column.target_type else {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL conversion DDL contains a non-MySQL column".into(),
            ));
        };
        let quoted_column = quote_identifier(&column.target.name);
        let mut sql = format!("{quoted_column} {}", target_type.ddl_type()?);
        if let Some((column_character_set, column_collation)) =
            target_type.character_set_and_collation()
        {
            sql.push_str(" CHARACTER SET ");
            sql.push_str(&quote_identifier(column_character_set));
            sql.push_str(" COLLATE ");
            sql.push_str(&quote_identifier(column_collation));
        }
        if !column.target.nullable {
            sql.push_str(" NOT NULL");
        }
        for check in &column.target_checks {
            sql.push_str(" CHECK (");
            sql.push_str(&render_mysql_conversion_check(&quoted_column, check)?);
            sql.push(')');
        }
        columns.push(sql);
    }
    let key_columns = policy
        .resumable_key
        .target_columns
        .iter()
        .map(quote_identifier)
        .collect::<Vec<_>>()
        .join(", ");
    columns.push(match policy.resumable_key.target_kind {
        CrossDialectKeyKind::PrimaryKey => format!("PRIMARY KEY ({key_columns})"),
        CrossDialectKeyKind::Unique => format!(
            "CONSTRAINT {} UNIQUE ({key_columns})",
            quote_identifier(policy.resumable_key.target_name.as_ref().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL conversion unique key has no target name".into(),
                )
            })?,)
        ),
    });
    Ok(format!(
        "CREATE TABLE {}.{} ({}) ENGINE=InnoDB DEFAULT CHARACTER SET {} COLLATE {}",
        quote_identifier(&policy.target_table.namespace),
        quote_identifier(&policy.target_table.name),
        columns.join(", "),
        quote_identifier(character_set),
        quote_identifier(collation)
    ))
}

fn mysql_conversion_table_definition(
    policy: &TableConversionPolicy,
) -> Result<MySqlTableDefinition, MySqlPlanError> {
    policy.validate()?;
    let CrossDialectTargetTableContract::MySql {
        engine: MySqlTargetEngine::InnoDb,
        character_set,
        collation,
    } = &policy.target_contract
    else {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL conversion definition has a non-MySQL target contract".into(),
        ));
    };
    let columns = policy
        .row_policy
        .columns
        .iter()
        .map(|column| {
            let CrossDialectTargetType::MySql(target_type) = &column.target_type else {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL conversion definition contains a non-MySQL column".into(),
                ));
            };
            let data_type = parse_mysql_column_type(&target_type.ddl_type()?)
                .map_err(MySqlPlanError::InvalidCatalog)?;
            let (column_character_set, column_collation) = target_type
                .character_set_and_collation()
                .map_or((None, None), |(character_set, collation)| {
                    (
                        Some(character_set.as_str().to_owned()),
                        Some(collation.as_str().to_owned()),
                    )
                });
            Ok(MySqlColumnDefinition {
                name: column.target.name.clone(),
                ordinal: u64::from(column.target.ordinal),
                data_type,
                nullable: column.target.nullable,
                character_set: column_character_set,
                collation: column_collation,
                auto_increment: false,
            })
        })
        .collect::<Result<Vec<_>, MySqlPlanError>>()?;
    let primary = policy.resumable_key.target_kind == CrossDialectKeyKind::PrimaryKey;
    let index_name = if primary {
        Identifier::new("PRIMARY")?
    } else {
        policy.resumable_key.target_name.clone().ok_or_else(|| {
            MySqlPlanError::InvalidCatalog("MySQL conversion unique key has no target name".into())
        })?
    };
    Ok(MySqlTableDefinition {
        table: policy.target_table.clone(),
        engine: "InnoDB".into(),
        character_set: character_set.as_str().into(),
        collation: collation.as_str().into(),
        columns,
        indexes: vec![MySqlIndexDefinition {
            name: index_name,
            primary,
            unique: true,
            constraint_backed: true,
            columns: policy.resumable_key.target_columns.clone(),
        }],
    })
}

fn mysql_conversion_table_contract(
    policy: &TableConversionPolicy,
    evidence: &MySqlSnapshotEvidence,
) -> Result<TableContract, MySqlPlanError> {
    let mut columns = BTreeMap::new();
    let mut key_metadata = BTreeMap::new();
    for column in &policy.row_policy.columns {
        let CrossDialectTargetType::MySql(target_type) = &column.target_type else {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL conversion contract contains a non-MySQL column".into(),
            ));
        };
        if columns
            .insert(column.target.name.as_str().into(), column.target.clone())
            .is_some()
        {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL conversion contract has duplicate target columns".into(),
            ));
        }
        if policy
            .resumable_key
            .target_columns
            .contains(&column.target.name)
        {
            key_metadata.insert(
                column.target.name.clone(),
                (
                    target_type.ddl_type()?,
                    target_type
                        .character_set_and_collation()
                        .map(|(_, collation)| collation.as_str().to_owned()),
                ),
            );
        }
    }
    let key_metadata = policy
        .resumable_key
        .target_columns
        .iter()
        .map(|column| {
            key_metadata.get(column).cloned().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL conversion key does not resolve to every target column".into(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (column_types, collations) = key_metadata.into_iter().unzip();
    Ok(TableContract {
        columns,
        key: MySqlResumableKey {
            index_name: policy
                .resumable_key
                .target_name
                .clone()
                .unwrap_or(Identifier::new("PRIMARY")?),
            primary: policy.resumable_key.target_kind == CrossDialectKeyKind::PrimaryKey,
            columns: policy.resumable_key.target_columns.clone(),
            column_types,
            collations,
            server_version: evidence.server_version.clone(),
        },
    })
}

fn mysql_conversion_checks_are_exact(
    namespace: &CatalogNamespace,
    policy: &TableConversionPolicy,
) -> ConnectionResult<bool> {
    let mut expected = policy
        .row_policy
        .columns
        .iter()
        .flat_map(|column| {
            let quoted = quote_identifier(&column.target.name);
            column.target_checks.iter().map(move |check| {
                render_mysql_conversion_check(&quoted, check)
                    .map(|clause| canonical_mysql_check_clause(&clause))
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let mut observed = namespace
        .objects
        .iter()
        .filter(|object| {
            object.kind == CatalogObjectKind::CheckConstraint
                && object
                    .attributes
                    .get("table_name")
                    .and_then(serde_json::Value::as_str)
                    == Some(policy.target_table.name.as_str())
        })
        .map(|object| {
            if object
                .attributes
                .get("enforced")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL target CHECK {} is not enforced",
                    object.name
                )));
            }
            object
                .attributes
                .get("check_clause")
                .and_then(serde_json::Value::as_str)
                .map(canonical_mysql_check_clause)
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL target CHECK {} has no clause",
                        object.name
                    ))
                })
        })
        .collect::<ConnectionResult<Vec<_>>>()?;
    expected.sort();
    observed.sort();
    Ok(expected == observed)
}

fn canonical_mysql_check_clause(clause: &str) -> String {
    let mut clause = clause.trim();
    while let Some(inner) = clause
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    {
        if !parentheses_wrap_entire_mysql_clause(clause) {
            break;
        }
        clause = inner.trim();
    }
    let mut canonical = String::with_capacity(clause.len());
    let mut bytes = clause.bytes().peekable();
    let mut quote = None;
    while let Some(byte) = bytes.next() {
        if let Some(quoted) = quote {
            canonical.push(char::from(byte));
            if byte == quoted {
                if bytes.peek() == Some(&quoted) {
                    if let Some(escaped) = bytes.next() {
                        canonical.push(char::from(escaped));
                    }
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match byte {
            b'\'' | b'`' => {
                quote = Some(byte);
                canonical.push(char::from(byte));
            }
            byte if byte.is_ascii_whitespace() => {}
            byte => canonical.push(char::from(byte.to_ascii_lowercase())),
        }
    }
    canonical
}

fn parentheses_wrap_entire_mysql_clause(clause: &str) -> bool {
    let mut depth = 0_u32;
    let mut quote = None;
    let bytes = clause.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if let Some(quoted) = quote {
            if byte == quoted {
                if bytes.get(index + 1) == Some(&quoted) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' | b'`' => quote = Some(byte),
            b'(' => depth += 1,
            b')' => {
                depth = match depth.checked_sub(1) {
                    Some(depth) => depth,
                    None => return false,
                };
                if depth == 0 && index + 1 != bytes.len() {
                    return false;
                }
            }
            _ => {}
        }
        index += 1;
    }
    depth == 0 && quote.is_none()
}

fn render_mysql_conversion_check(
    column: &str,
    check: &TargetCheckConstraint,
) -> Result<String, MySqlPlanError> {
    Ok(match check {
        TargetCheckConstraint::NonNegative => format!("{column} >= 0"),
        TargetCheckConstraint::SignedIntegerRange { minimum, maximum } => {
            format!("{column} BETWEEN {minimum} AND {maximum}")
        }
        TargetCheckConstraint::UnsignedIntegerRange { maximum } => {
            format!("{column} BETWEEN 0 AND {maximum}")
        }
        TargetCheckConstraint::DateYearRange {
            minimum_year,
            maximum_year,
        } => format!("EXTRACT(YEAR FROM {column}) BETWEEN {minimum_year} AND {maximum_year}"),
        TargetCheckConstraint::TimestampRange { range } => format!(
            "{column} BETWEEN {} AND {}",
            mysql_timestamp_literal(range.minimum)?,
            mysql_timestamp_literal(range.maximum)?
        ),
        TargetCheckConstraint::CharacterLengthMaximum { maximum } => {
            format!("CHAR_LENGTH({column}) <= {maximum}")
        }
        TargetCheckConstraint::OctetLengthMaximum { maximum } => {
            format!("OCTET_LENGTH({column}) <= {maximum}")
        }
    })
}

fn mysql_timestamp_literal(bound: TimestampBound) -> Result<String, MySqlPlanError> {
    if bound.year <= 0 || !bound.nanos.is_multiple_of(1_000) {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL conversion timestamp CHECK bound is unsupported".into(),
        ));
    }
    Ok(format!(
        "TIMESTAMP '{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
        bound.year,
        bound.month,
        bound.day,
        bound.hour,
        bound.minute,
        bound.second,
        bound.nanos / 1_000
    ))
}

fn mysql_conversion_source_type(
    column: &MySqlColumnDefinition,
) -> Result<MySqlTargetType, MySqlPlanError> {
    let data_type = match &column.data_type {
        MySqlColumnType::Integer { name, unsigned, .. } => MySqlTargetType::Integer {
            width: match name.as_str() {
                "tinyint" => MySqlIntegerWidth::Tiny,
                "smallint" => MySqlIntegerWidth::Small,
                "mediumint" => MySqlIntegerWidth::Medium,
                "int" | "integer" => MySqlIntegerWidth::Integer,
                "bigint" => MySqlIntegerWidth::Big,
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL integer has no cross-dialect mapping".into(),
                    ));
                }
            },
            unsigned: *unsigned,
        },
        MySqlColumnType::Decimal {
            precision,
            scale,
            unsigned,
        } => MySqlTargetType::Decimal {
            precision: *precision,
            scale: *scale,
            unsigned: *unsigned,
        },
        MySqlColumnType::Floating {
            name,
            precision: None,
            scale: None,
            unsigned: false,
        } if name == "float" => MySqlTargetType::Float,
        MySqlColumnType::Floating {
            name,
            precision: None,
            scale: None,
            unsigned: false,
        } if name == "double" => MySqlTargetType::Double,
        MySqlColumnType::Temporal {
            name,
            fractional_precision,
        } => {
            let precision = u8::try_from(fractional_precision.unwrap_or(0)).map_err(|_| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL temporal precision has no cross-dialect mapping".into(),
                )
            })?;
            match name.as_str() {
                "date" if precision == 0 => MySqlTargetType::Date {
                    minimum_year: 1_000,
                    maximum_year: 9_999,
                },
                "time" => MySqlTargetType::Time {
                    precision,
                    minimum_nanos: MYSQL_TIME_MINIMUM_NANOS,
                    maximum_nanos: MYSQL_TIME_MAXIMUM_NANOS,
                },
                "datetime" => MySqlTargetType::DateTime {
                    precision,
                    semantics: TimestampSemantics::WallClock,
                    range: MYSQL_DATETIME_RANGE.at_precision(precision)?,
                },
                "timestamp" => MySqlTargetType::Timestamp {
                    precision,
                    semantics: TimestampSemantics::UtcNormalized,
                    range: MYSQL_TIMESTAMP_RANGE.at_precision(precision)?,
                },
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL temporal type has no cross-dialect mapping".into(),
                    ));
                }
            }
        }
        MySqlColumnType::Character { name, length } => MySqlTargetType::Text {
            // The target CHECK constraints use UTF-8 octet lengths. Other source
            // character sets can expand during decoding and need a separate policy.
            storage: match name.as_str() {
                "char" => MySqlTextStorage::Char { length: *length },
                "varchar" => MySqlTextStorage::VarChar { length: *length },
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL character type has no cross-dialect mapping".into(),
                    ));
                }
            },
            character_set: Identifier::new(column.character_set.clone().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL text conversion requires a character set".into(),
                )
            })?)
            .map_err(|_| {
                MySqlPlanError::InvalidCatalog("MySQL character set identity is invalid".into())
            })?,
            collation: Identifier::new(column.collation.clone().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog("MySQL text conversion requires a collation".into())
            })?)
            .map_err(|_| {
                MySqlPlanError::InvalidCatalog("MySQL collation identity is invalid".into())
            })?,
        },
        MySqlColumnType::Text { name } => MySqlTargetType::Text {
            storage: match name.as_str() {
                "tinytext" => MySqlTextStorage::TinyText,
                "text" => MySqlTextStorage::Text,
                "mediumtext" => MySqlTextStorage::MediumText,
                "longtext" => MySqlTextStorage::LongText,
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL text type has no cross-dialect mapping".into(),
                    ));
                }
            },
            character_set: Identifier::new(column.character_set.clone().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog(
                    "MySQL text conversion requires a character set".into(),
                )
            })?)
            .map_err(|_| {
                MySqlPlanError::InvalidCatalog("MySQL character set identity is invalid".into())
            })?,
            collation: Identifier::new(column.collation.clone().ok_or_else(|| {
                MySqlPlanError::InvalidCatalog("MySQL text conversion requires a collation".into())
            })?)
            .map_err(|_| {
                MySqlPlanError::InvalidCatalog("MySQL collation identity is invalid".into())
            })?,
        },
        MySqlColumnType::Binary { name, length } => MySqlTargetType::Binary {
            storage: match name.as_str() {
                "binary" => MySqlBinaryStorage::Binary { length: *length },
                "varbinary" => MySqlBinaryStorage::VarBinary { length: *length },
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL binary type has no cross-dialect mapping".into(),
                    ));
                }
            },
        },
        MySqlColumnType::Blob { name } => MySqlTargetType::Binary {
            storage: match name.as_str() {
                "tinyblob" => MySqlBinaryStorage::TinyBlob,
                "blob" => MySqlBinaryStorage::Blob,
                "mediumblob" => MySqlBinaryStorage::MediumBlob,
                "longblob" => MySqlBinaryStorage::LongBlob,
                _ => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL blob type has no cross-dialect mapping".into(),
                    ));
                }
            },
        },
        MySqlColumnType::Json => MySqlTargetType::Json,
        MySqlColumnType::Bit { .. } | MySqlColumnType::Year | MySqlColumnType::Floating { .. } => {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL source type has no exact cross-dialect mapping".into(),
            ));
        }
    };
    if matches!(data_type, MySqlTargetType::Text { ref character_set, .. } if character_set.as_str() != "utf8mb4")
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL text conversion currently requires utf8mb4 source columns".into(),
        ));
    }
    Ok(data_type)
}

fn mysql_table_definition(
    namespace: &CatalogNamespace,
    table: &CatalogObject,
) -> Result<MySqlTableDefinition, MySqlPlanError> {
    let engine = required_text(table, "engine")?.to_owned();
    let character_set = required_text(table, "character_set")?.to_owned();
    let collation = required_text(table, "collation")?.to_owned();
    if engine != "InnoDB"
        || !required_text(table, "create_options")?.is_empty()
        || character_set.is_empty()
        || collation.is_empty()
    {
        return Err(MySqlPlanError::InvalidCatalog(format!(
            "MySQL table {} has no supported typed DDL contract",
            table.id
        )));
    }
    let mut columns = namespace
        .objects
        .iter()
        .filter(|object| {
            object.kind == CatalogObjectKind::Column
                && optional_text(object, "table_name") == Some(table.name.as_str())
        })
        .map(|column| {
            if column
                .attributes
                .get("default")
                .is_some_and(|value| !value.is_null())
                || !supported_column_extra(required_text(column, "extra")?)
                || optional_text(column, "generation_expression")
                    .is_some_and(|value| !value.is_empty())
            {
                return Err(MySqlPlanError::InvalidCatalog(format!(
                    "MySQL column {} has unsupported typed DDL semantics",
                    column.id
                )));
            }
            let data_type: MySqlColumnType = serde_json::from_value(
                column
                    .attributes
                    .get("mysql_ddl_type")
                    .cloned()
                    .ok_or_else(|| {
                        MySqlPlanError::InvalidCatalog(format!(
                            "MySQL column {} lacks a typed DDL type",
                            column.id
                        ))
                    })?,
            )?;
            Ok(MySqlColumnDefinition {
                name: column.name.clone(),
                ordinal: required_u64(column, "ordinal")?,
                data_type,
                nullable: required_bool(column, "nullable")?,
                character_set: optional_text(column, "character_set").map(str::to_owned),
                collation: optional_text(column, "collation").map(str::to_owned),
                auto_increment: required_text(column, "extra")?
                    .eq_ignore_ascii_case("auto_increment"),
            })
        })
        .collect::<Result<Vec<_>, MySqlPlanError>>()?;
    columns.sort_by_key(|column| column.ordinal);
    if columns.is_empty()
        || columns
            .iter()
            .enumerate()
            .any(|(index, column)| column.ordinal != index as u64 + 1)
    {
        return Err(MySqlPlanError::InvalidCatalog(format!(
            "MySQL table {} has invalid column ordinals",
            table.id
        )));
    }
    let mut indexes = namespace
        .objects
        .iter()
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::PrimaryKey
                    | CatalogObjectKind::UniqueConstraint
                    | CatalogObjectKind::Index
            ) && optional_text(object, "table_name") == Some(table.name.as_str())
        })
        .map(|index| {
            if catalog_index_requires_blocker(index)? {
                return Err(MySqlPlanError::InvalidCatalog(format!(
                    "MySQL index {} has unsupported typed DDL semantics",
                    index.id
                )));
            }
            let columns: Vec<MySqlIndexColumn> = serde_json::from_value(
                index.attributes.get("columns").cloned().ok_or_else(|| {
                    MySqlPlanError::InvalidCatalog(format!(
                        "MySQL index {} lacks columns",
                        index.id
                    ))
                })?,
            )?;
            Ok(MySqlIndexDefinition {
                name: index.name.clone(),
                primary: required_bool(index, "primary")?,
                unique: !required_bool(index, "non_unique")?,
                constraint_backed: required_bool(index, "constraint_backed")?,
                columns: columns
                    .into_iter()
                    .map(|column| {
                        column.name.ok_or_else(|| {
                            MySqlPlanError::InvalidCatalog(format!(
                                "MySQL index {} contains an expression",
                                index.id
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect::<Result<Vec<_>, MySqlPlanError>>()?;
    indexes.sort_by(|left, right| {
        (!left.primary, left.name.as_str()).cmp(&(!right.primary, right.name.as_str()))
    });
    let definition = MySqlTableDefinition {
        table: QualifiedTable {
            namespace: namespace.name.clone(),
            name: table.name.clone(),
        },
        engine,
        character_set,
        collation,
        columns,
        indexes,
    };
    validate_mysql_table_definition(&definition)?;
    Ok(definition)
}

fn validate_mysql_table_definition(
    definition: &MySqlTableDefinition,
) -> Result<(), MySqlPlanError> {
    if definition.engine != "InnoDB"
        || definition.columns.is_empty()
        || definition.character_set.is_empty()
        || definition.collation.is_empty()
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL table definition is incomplete or not InnoDB".into(),
        ));
    }
    let column_names = definition
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if column_names.len() != definition.columns.len()
        || definition
            .columns
            .iter()
            .enumerate()
            .any(|(index, column)| column.ordinal != index as u64 + 1)
        || definition.indexes.iter().any(|index| {
            index.columns.is_empty()
                || index
                    .columns
                    .iter()
                    .any(|name| !column_names.contains(name.as_str()))
        })
        || definition
            .indexes
            .iter()
            .filter(|index| index.primary)
            .count()
            > 1
        || definition
            .indexes
            .iter()
            .any(|index| index.primary && (!index.unique || !index.constraint_backed))
        || definition
            .columns
            .iter()
            .filter(|column| column.auto_increment)
            .count()
            > 1
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL table definition has inconsistent columns or indexes".into(),
        ));
    }
    for column in &definition.columns {
        let rendered = render_mysql_column_type(&column.data_type)?;
        let reparsed =
            parse_mysql_column_type(&rendered).map_err(MySqlPlanError::InvalidCatalog)?;
        if reparsed != column.data_type {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL column type does not round-trip canonically".into(),
            ));
        }
        if column.auto_increment && !matches!(column.data_type, MySqlColumnType::Integer { .. }) {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL AUTO_INCREMENT requires a reviewed integer type".into(),
            ));
        }
    }
    Ok(())
}

fn render_mysql_create_table(definition: &MySqlTableDefinition) -> Result<String, MySqlPlanError> {
    validate_mysql_table_definition(definition)?;
    let mut clauses = definition
        .columns
        .iter()
        .map(|column| {
            let mut clause = format!(
                "{} {}",
                quote_identifier(&column.name),
                render_mysql_column_type(&column.data_type)?
            );
            if let Some(character_set) = &column.character_set {
                clause.push_str(" CHARACTER SET ");
                clause.push_str(&quote_identifier_text(character_set));
            }
            if let Some(collation) = &column.collation {
                clause.push_str(" COLLATE ");
                clause.push_str(&quote_identifier_text(collation));
            }
            clause.push_str(if column.nullable {
                " NULL"
            } else {
                " NOT NULL"
            });
            if column.auto_increment {
                clause.push_str(" AUTO_INCREMENT");
            }
            Ok(clause)
        })
        .collect::<Result<Vec<_>, MySqlPlanError>>()?;
    for index in &definition.indexes {
        let columns = index
            .columns
            .iter()
            .map(quote_identifier)
            .collect::<Vec<_>>()
            .join(", ");
        let clause = if index.primary {
            format!("PRIMARY KEY ({columns})")
        } else if index.constraint_backed {
            format!(
                "CONSTRAINT {} UNIQUE ({columns})",
                quote_identifier(&index.name)
            )
        } else if index.unique {
            format!("UNIQUE INDEX {} ({columns})", quote_identifier(&index.name))
        } else {
            format!("INDEX {} ({columns})", quote_identifier(&index.name))
        };
        clauses.push(clause);
    }
    Ok(format!(
        "CREATE TABLE {}.{} ({}) ENGINE=InnoDB DEFAULT CHARACTER SET {} COLLATE {}",
        quote_identifier(&definition.table.namespace),
        quote_identifier(&definition.table.name),
        clauses.join(", "),
        quote_identifier_text(&definition.character_set),
        quote_identifier_text(&definition.collation)
    ))
}

fn mysql_foreign_key_violation_query(foreign_key: &MySqlForeignKey) -> ConnectionResult<String> {
    validate_mysql_foreign_key_shape(foreign_key)?;
    let all_non_null = foreign_key
        .columns
        .iter()
        .map(|column| format!("child.{} IS NOT NULL", quote_identifier(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
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
    Ok(format!(
        "SELECT EXISTS (SELECT 1 FROM {}.{} AS child WHERE ({all_non_null}) AND NOT EXISTS (SELECT 1 FROM {}.{} AS parent WHERE {equality}))",
        quote_identifier(&foreign_key.table.namespace),
        quote_identifier(&foreign_key.table.name),
        quote_identifier(&foreign_key.referenced_table.namespace),
        quote_identifier(&foreign_key.referenced_table.name),
    ))
}

fn render_mysql_add_foreign_key(foreign_key: &MySqlForeignKey) -> ConnectionResult<String> {
    validate_mysql_foreign_key_shape(foreign_key)?;
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
    Ok(format!(
        "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({columns}) REFERENCES {}.{} ({referenced_columns}) ON UPDATE {} ON DELETE {}",
        quote_identifier(&foreign_key.table.namespace),
        quote_identifier(&foreign_key.table.name),
        quote_identifier(&foreign_key.name),
        quote_identifier(&foreign_key.referenced_table.namespace),
        quote_identifier(&foreign_key.referenced_table.name),
        mysql_foreign_key_action_sql(foreign_key.update_action),
        mysql_foreign_key_action_sql(foreign_key.delete_action),
    ))
}

fn classify_mysql_authorization_inventory(
    observed: &MySqlGrantInventory,
    initial: &MySqlMetadataVisibilityEvidence,
    expected_business: &MySqlGrantInventory,
) -> ConnectionResult<MySqlAuthorizationTargetState> {
    observed
        .validate()
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let initial_inventory = &initial.grant_inventory;
    if observed.partial_revokes_enabled != initial_inventory.partial_revokes_enabled
        || observed.grant_table_columns != initial_inventory.grant_table_columns
        || observed.accounts != initial_inventory.accounts
        || observed.unknown_privilege_classes != initial_inventory.unknown_privilege_classes
    {
        return Ok(MySqlAuthorizationTargetState::Different);
    }
    let mapped_accounts = expected_business.accounts.iter().collect::<BTreeSet<_>>();
    let partition = |records: &[MySqlGrantRecord]| {
        let mut base = Vec::new();
        let mut mapped = Vec::new();
        let mut mixed = false;
        for record in records {
            let involved = record.involved_accounts();
            let has_mapped = involved
                .iter()
                .any(|account| mapped_accounts.contains(account));
            let has_unmapped = involved
                .iter()
                .any(|account| !mapped_accounts.contains(account));
            if has_mapped && has_unmapped {
                mixed = true;
            } else if has_mapped {
                mapped.push(record.clone());
            } else {
                base.push(record.clone());
            }
        }
        (base, mapped, mixed)
    };
    let (initial_base, initial_mapped, initial_mixed) = partition(&initial_inventory.records);
    let (observed_base, observed_mapped, observed_mixed) = partition(&observed.records);
    if initial_mixed
        || observed_mixed
        || !initial_mapped.is_empty()
        || initial_base != observed_base
    {
        return Ok(MySqlAuthorizationTargetState::Different);
    }
    let effective = |records: &[MySqlGrantRecord]| -> ConnectionResult<BTreeSet<Vec<u8>>> {
        let keys = records
            .iter()
            .map(MySqlGrantRecord::effective_authorization_key)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let set = keys.iter().cloned().collect::<BTreeSet<_>>();
        if set.len() != keys.len() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL authorization contains duplicate effective records".into(),
            ));
        }
        Ok(set)
    };
    let observed = effective(&observed_mapped)?;
    let expected = effective(&expected_business.records)?;
    if observed == expected {
        Ok(MySqlAuthorizationTargetState::Exact)
    } else if observed.is_subset(&expected) {
        Ok(if observed.is_empty() {
            MySqlAuthorizationTargetState::Absent
        } else {
            MySqlAuthorizationTargetState::Subset
        })
    } else {
        Ok(MySqlAuthorizationTargetState::Different)
    }
}

#[derive(Default)]
struct MySqlScopedGrantGroup {
    privileges: BTreeSet<String>,
    column_privileges: BTreeMap<String, BTreeSet<String>>,
    grant_option: bool,
}

/// Render the closed typed authorization inventory. No password, plugin, raw
/// account clause, or unvalidated privilege token enters these statements.
fn render_mysql_authorization_statements(
    inventory: &MySqlGrantInventory,
) -> ConnectionResult<Vec<String>> {
    inventory
        .validate()
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let mut global = BTreeMap::<MySqlAccountIdentity, MySqlScopedGrantGroup>::new();
    let mut database = BTreeMap::<(MySqlAccountIdentity, String), MySqlScopedGrantGroup>::new();
    let mut table =
        BTreeMap::<(MySqlAccountIdentity, String, String), MySqlScopedGrantGroup>::new();
    let mut routine =
        BTreeMap::<(MySqlAccountIdentity, String, String, String), MySqlScopedGrantGroup>::new();
    let mut dynamic = Vec::new();
    let mut proxy = Vec::new();
    let mut role_edges = Vec::new();
    let mut default_roles = Vec::new();
    let mut partial_revokes = Vec::new();
    for record in &inventory.records {
        match record {
            MySqlGrantRecord::StaticGlobal { account, privilege } => {
                let group = global.entry(account.clone()).or_default();
                if privilege == "GRANT" {
                    group.grant_option = true;
                } else {
                    require_mysql_static_privilege(privilege)?;
                    group.privileges.insert(privilege.clone());
                }
            }
            MySqlGrantRecord::DynamicGlobal { .. } => dynamic.push(record),
            MySqlGrantRecord::Database {
                account,
                database: name,
                privilege,
            } => {
                let group = database.entry((account.clone(), name.clone())).or_default();
                if privilege == "GRANT" {
                    group.grant_option = true;
                } else {
                    require_mysql_static_privilege(privilege)?;
                    group.privileges.insert(privilege.clone());
                }
            }
            MySqlGrantRecord::Table {
                account,
                database: name,
                table: table_name,
                privilege,
                ..
            } => {
                let group = table
                    .entry((account.clone(), name.clone(), table_name.clone()))
                    .or_default();
                if privilege == "GRANT" {
                    group.grant_option = true;
                } else if let Some(column_privilege) = privilege.strip_prefix("COLUMN::") {
                    require_mysql_table_privilege(column_privilege)?;
                } else {
                    require_mysql_table_privilege(privilege)?;
                    group.privileges.insert(privilege.clone());
                }
            }
            MySqlGrantRecord::Column {
                account,
                database: name,
                table: table_name,
                column,
                privilege,
            } => {
                require_mysql_column_privilege(privilege)?;
                table
                    .entry((account.clone(), name.clone(), table_name.clone()))
                    .or_default()
                    .column_privileges
                    .entry(privilege.clone())
                    .or_default()
                    .insert(column.clone());
            }
            MySqlGrantRecord::Routine {
                account,
                database: name,
                routine: routine_name,
                routine_type,
                privilege,
                ..
            } => {
                if !matches!(routine_type.as_str(), "FUNCTION" | "PROCEDURE") {
                    return Err(ConnectionError::InvalidRequest(
                        "MySQL routine grant has an unsupported routine type".into(),
                    ));
                }
                let group = routine
                    .entry((
                        account.clone(),
                        name.clone(),
                        routine_name.clone(),
                        routine_type.clone(),
                    ))
                    .or_default();
                if privilege == "GRANT" {
                    group.grant_option = true;
                } else {
                    require_mysql_routine_privilege(privilege)?;
                    group.privileges.insert(privilege.clone());
                }
            }
            MySqlGrantRecord::Proxy { .. } => proxy.push(record),
            MySqlGrantRecord::RoleEdge { .. } => role_edges.push(record),
            MySqlGrantRecord::DefaultRole { .. } => default_roles.push(record),
            MySqlGrantRecord::PartialRevoke { .. } => partial_revokes.push(record),
        }
    }
    let mut statements = Vec::new();
    for (account, group) in global {
        if group.privileges.is_empty() && !group.grant_option {
            continue;
        }
        let privileges = if group.privileges.is_empty() {
            "USAGE".into()
        } else {
            group.privileges.into_iter().collect::<Vec<_>>().join(", ")
        };
        statements.push(format!(
            "GRANT {privileges} ON *.* TO {}{}",
            quote_mysql_account(&account),
            if group.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            }
        ));
    }
    for ((account, name), group) in database {
        statements.push(render_mysql_scoped_grant(
            group,
            format!("{}.*", quote_identifier_text(&name)),
            &account,
        )?);
    }
    for ((account, name, table_name), group) in table {
        let mut privileges = group.privileges.into_iter().collect::<Vec<_>>();
        for (privilege, columns) in group.column_privileges {
            privileges.push(format!(
                "{privilege} ({})",
                columns
                    .into_iter()
                    .map(|column| quote_identifier_text(&column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if privileges.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL table grant option has no underlying privilege".into(),
            ));
        }
        statements.push(format!(
            "GRANT {} ON {}.{} TO {}{}",
            privileges.join(", "),
            quote_identifier_text(&name),
            quote_identifier_text(&table_name),
            quote_mysql_account(&account),
            if group.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            }
        ));
    }
    for ((account, name, routine_name, routine_type), group) in routine {
        if group.privileges.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL routine grant option has no underlying privilege".into(),
            ));
        }
        statements.push(format!(
            "GRANT {} ON {routine_type} {}.{} TO {}{}",
            group.privileges.into_iter().collect::<Vec<_>>().join(", "),
            quote_identifier_text(&name),
            quote_identifier_text(&routine_name),
            quote_mysql_account(&account),
            if group.grant_option {
                " WITH GRANT OPTION"
            } else {
                ""
            }
        ));
    }
    for record in dynamic {
        let MySqlGrantRecord::DynamicGlobal {
            account,
            privilege,
            grantable,
        } = record
        else {
            unreachable!()
        };
        require_mysql_dynamic_privilege(privilege)?;
        statements.push(format!(
            "GRANT {privilege} ON *.* TO {}{}",
            quote_mysql_account(account),
            if *grantable { " WITH GRANT OPTION" } else { "" }
        ));
    }
    for record in proxy {
        let MySqlGrantRecord::Proxy {
            account,
            target,
            grantable,
        } = record
        else {
            unreachable!()
        };
        let target = match target {
            MySqlProxyTarget::Account { account } => quote_mysql_account(account),
            MySqlProxyTarget::AnyAccount => "''@''".into(),
        };
        statements.push(format!(
            "GRANT PROXY ON {target} TO {}{}",
            quote_mysql_account(account),
            if *grantable { " WITH GRANT OPTION" } else { "" }
        ));
    }
    for record in role_edges {
        let MySqlGrantRecord::RoleEdge {
            role,
            grantee,
            admin_option,
        } = record
        else {
            unreachable!()
        };
        statements.push(format!(
            "GRANT {} TO {}{}",
            quote_mysql_account(role),
            quote_mysql_account(grantee),
            if *admin_option {
                " WITH ADMIN OPTION"
            } else {
                ""
            }
        ));
    }
    for record in default_roles {
        let MySqlGrantRecord::DefaultRole { account, role } = record else {
            unreachable!()
        };
        statements.push(format!(
            "SET DEFAULT ROLE {} TO {}",
            quote_mysql_account(role),
            quote_mysql_account(account)
        ));
    }
    for record in partial_revokes {
        let MySqlGrantRecord::PartialRevoke {
            account,
            database,
            privileges,
        } = record
        else {
            unreachable!()
        };
        for privilege in privileges {
            require_mysql_static_privilege(privilege)?;
        }
        statements.push(format!(
            "REVOKE {} ON {}.* FROM {}",
            privileges.join(", "),
            quote_identifier_text(database),
            quote_mysql_account(account)
        ));
    }
    Ok(statements)
}

fn render_mysql_scoped_grant(
    group: MySqlScopedGrantGroup,
    scope: String,
    account: &MySqlAccountIdentity,
) -> ConnectionResult<String> {
    if group.privileges.is_empty() {
        return Err(ConnectionError::InvalidRequest(
            "MySQL scoped grant option has no underlying privilege".into(),
        ));
    }
    Ok(format!(
        "GRANT {} ON {scope} TO {}{}",
        group.privileges.into_iter().collect::<Vec<_>>().join(", "),
        quote_mysql_account(account),
        if group.grant_option {
            " WITH GRANT OPTION"
        } else {
            ""
        }
    ))
}

fn quote_mysql_account(account: &MySqlAccountIdentity) -> String {
    format!(
        "{}@{}",
        quote_mysql_string_literal(&account.user),
        quote_mysql_string_literal(&account.host)
    )
}

fn quote_mysql_string_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for character in value.chars() {
        match character {
            '\0' => quoted.push_str("\\0"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\u{1a}' => quoted.push_str("\\Z"),
            '\\' => quoted.push_str("\\\\"),
            '\'' => quoted.push_str("\\'"),
            value => quoted.push(value),
        }
    }
    quoted.push('\'');
    quoted
}

fn require_mysql_static_privilege(privilege: &str) -> ConnectionResult<()> {
    const VALUES: &[&str] = &[
        "ALTER",
        "ALTER ROUTINE",
        "CREATE",
        "CREATE ROLE",
        "CREATE ROUTINE",
        "CREATE TABLESPACE",
        "CREATE TEMPORARY TABLES",
        "CREATE USER",
        "CREATE VIEW",
        "DELETE",
        "DROP",
        "DROP ROLE",
        "EVENT",
        "EXECUTE",
        "FILE",
        "INDEX",
        "INSERT",
        "LOCK TABLES",
        "PROCESS",
        "REFERENCES",
        "RELOAD",
        "REPLICATION CLIENT",
        "REPLICATION SLAVE",
        "SELECT",
        "SHOW DATABASES",
        "SHOW VIEW",
        "SHUTDOWN",
        "SUPER",
        "TRIGGER",
        "UPDATE",
    ];
    require_mysql_privilege_in(privilege, VALUES)
}

fn require_mysql_table_privilege(privilege: &str) -> ConnectionResult<()> {
    require_mysql_privilege_in(
        privilege,
        &[
            "ALTER",
            "CREATE",
            "CREATE VIEW",
            "DELETE",
            "DROP",
            "INDEX",
            "INSERT",
            "REFERENCES",
            "SELECT",
            "SHOW VIEW",
            "TRIGGER",
            "UPDATE",
        ],
    )
}

fn require_mysql_column_privilege(privilege: &str) -> ConnectionResult<()> {
    require_mysql_privilege_in(privilege, &["INSERT", "REFERENCES", "SELECT", "UPDATE"])
}

fn require_mysql_routine_privilege(privilege: &str) -> ConnectionResult<()> {
    require_mysql_privilege_in(privilege, &["ALTER ROUTINE", "EXECUTE"])
}

fn require_mysql_privilege_in(privilege: &str, values: &[&str]) -> ConnectionResult<()> {
    if values.contains(&privilege) {
        Ok(())
    } else {
        Err(ConnectionError::InvalidRequest(
            "MySQL authorization contains an unsupported privilege token".into(),
        ))
    }
}

fn require_mysql_dynamic_privilege(privilege: &str) -> ConnectionResult<()> {
    if !privilege.is_empty()
        && privilege
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Ok(())
    } else {
        Err(ConnectionError::InvalidRequest(
            "MySQL authorization contains an unsafe dynamic privilege token".into(),
        ))
    }
}

fn validate_mysql_foreign_key_shape(foreign_key: &MySqlForeignKey) -> ConnectionResult<()> {
    if !foreign_key.enforced
        || foreign_key.columns.is_empty()
        || foreign_key.columns.len() != foreign_key.referenced_columns.len()
        || foreign_key.table.namespace != foreign_key.referenced_table.namespace
    {
        return Err(ConnectionError::InvalidRequest(format!(
            "MySQL foreign key {} has an invalid typed shape",
            foreign_key.name
        )));
    }
    Ok(())
}

fn mysql_foreign_key_semantics_eq(left: &MySqlForeignKey, right: &MySqlForeignKey) -> bool {
    left.name == right.name
        && left.table == right.table
        && left.columns == right.columns
        && left.referenced_table == right.referenced_table
        && left.referenced_columns == right.referenced_columns
        && left.referenced_constraint == right.referenced_constraint
        && left.update_action == right.update_action
        && left.delete_action == right.delete_action
        && left.enforced == right.enforced
}

fn mysql_foreign_key_action_sql(action: MySqlForeignKeyAction) -> &'static str {
    match action {
        MySqlForeignKeyAction::NoAction => "NO ACTION",
        MySqlForeignKeyAction::Restrict => "RESTRICT",
        MySqlForeignKeyAction::Cascade => "CASCADE",
        MySqlForeignKeyAction::SetNull => "SET NULL",
    }
}

fn column_meta(object: &CatalogObject) -> Result<ColumnMeta, MySqlPlanError> {
    let ordinal = required_u64(object, "ordinal")?;
    let data_type = required_text(object, "data_type")?;
    let precision = match data_type {
        "bit" | "decimal" | "numeric" => optional_u64(object, "numeric_precision"),
        "datetime" | "timestamp" | "time" => optional_u64(object, "datetime_precision"),
        _ => None,
    };
    Ok(ColumnMeta {
        name: object.name.clone(),
        ordinal: u32::try_from(ordinal)
            .map_err(|_| MySqlPlanError::InvalidCatalog("column ordinal is too large".into()))?,
        vendor_type: required_text(object, "data_type")?.into(),
        nullable: required_bool(object, "nullable")?,
        collation: optional_text(object, "collation").map(str::to_owned),
        precision: precision
            .map(u32::try_from)
            .transpose()
            .map_err(|_| MySqlPlanError::InvalidCatalog("column precision is too large".into()))?,
        scale: if matches!(data_type, "decimal" | "numeric") {
            optional_i64(object, "numeric_scale")
        } else {
            None
        }
        .map(i32::try_from)
        .transpose()
        .map_err(|_| MySqlPlanError::InvalidCatalog("column scale is too large".into()))?,
        timezone_semantics: match data_type {
            "timestamp" => Some("mysql_session_time_zone".into()),
            "datetime" => Some("local_without_offset".into()),
            _ => None,
        },
    })
}

fn extract_catalog(
    conn: &mut Conn,
    database: &str,
    server_version: &str,
    lower_case_table_names: u8,
) -> ConnectionResult<(VendorCatalog, Vec<MySqlCatalogBlocker>)> {
    let mut objects = Vec::new();
    let mut blockers = vec![MySqlCatalogBlocker {
        object_id: catalog_id("catalog_visibility", database, database, ""),
        object_kind: "catalog_visibility".into(),
        reason: "MySQL information_schema visibility for grants, routines, triggers, and events is account-dependent; exhaustive metadata-administrator evidence is not yet modeled"
            .into(),
    }];
    let table_rows: Vec<Row> = conn
        .exec(
            "SELECT t.TABLE_NAME, t.TABLE_TYPE, t.ENGINE, t.TABLE_COLLATION, c.CHARACTER_SET_NAME, t.CREATE_OPTIONS, t.AUTO_INCREMENT FROM information_schema.TABLES t LEFT JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY c ON c.COLLATION_NAME = t.TABLE_COLLATION WHERE BINARY t.TABLE_SCHEMA = BINARY ? ORDER BY t.TABLE_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let column_rows: Vec<Row> = conn
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, GENERATION_EXPRESSION, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA = BINARY ? ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (database,),
        )
        .map_err(database_error)?;
    let index_rows: Vec<Row> = conn
        .exec(
            "SELECT s.TABLE_NAME, s.INDEX_NAME, s.NON_UNIQUE, s.SEQ_IN_INDEX, s.COLUMN_NAME, s.COLLATION, s.SUB_PART, s.NULLABLE, s.INDEX_TYPE, s.IS_VISIBLE, s.EXPRESSION, EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS tc WHERE BINARY tc.CONSTRAINT_SCHEMA = BINARY s.TABLE_SCHEMA AND BINARY tc.TABLE_NAME = BINARY s.TABLE_NAME AND BINARY tc.CONSTRAINT_NAME = BINARY s.INDEX_NAME AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')) AS CONSTRAINT_BACKED FROM information_schema.STATISTICS s WHERE BINARY s.TABLE_SCHEMA = BINARY ? ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX",
            (database,),
        )
        .map_err(database_error)?;

    let mut columns = BTreeMap::<String, Vec<MySqlColumnContract>>::new();
    for mut row in column_rows {
        let table_name: String = take_cell(&mut row, 0, "TABLE_NAME")?;
        let column_name: String = take_cell(&mut row, 1, "COLUMN_NAME")?;
        let ordinal: u64 = take_cell(&mut row, 2, "ORDINAL_POSITION")?;
        let default: Option<Vec<u8>> = take_cell(&mut row, 3, "COLUMN_DEFAULT")?;
        let nullable: String = take_cell(&mut row, 4, "IS_NULLABLE")?;
        let data_type: String = take_cell(&mut row, 5, "DATA_TYPE")?;
        let column_type: String = take_cell(&mut row, 6, "COLUMN_TYPE")?;
        let charset: Option<String> = take_cell(&mut row, 7, "CHARACTER_SET_NAME")?;
        let collation: Option<String> = take_cell(&mut row, 8, "COLLATION_NAME")?;
        let extra: String = take_cell(&mut row, 9, "EXTRA")?;
        let generated_expression: String = take_cell(&mut row, 10, "GENERATION_EXPRESSION")?;
        let numeric_precision: Option<u64> = take_cell(&mut row, 11, "NUMERIC_PRECISION")?;
        let numeric_scale: Option<i64> = take_cell(&mut row, 12, "NUMERIC_SCALE")?;
        let datetime_precision: Option<i64> = take_cell(&mut row, 13, "DATETIME_PRECISION")?;
        let id = catalog_id("column", database, &table_name, &column_name);
        let identifier = Identifier::new(column_name.clone())
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let contract = MySqlColumnContract {
            name: identifier.clone(),
            ordinal,
            nullable: nullable == "YES",
            data_type: data_type.clone(),
            collation: collation.clone(),
            extra: extra.clone(),
        };
        let ddl_type = parse_mysql_column_type(&column_type).ok();
        let mut ddl_reasons = Vec::new();
        if ddl_type.is_none() || !supported_value_type(&data_type) {
            ddl_reasons.push(format!(
                "column type {column_type} has no reviewed typed DDL and lossless canonical value contract"
            ));
        }
        if default.is_some() || !supported_column_extra(&extra) {
            ddl_reasons.push(
                "column defaults and extra attributes outside AUTO_INCREMENT are not yet modeled as typed DDL"
                    .into(),
            );
        }
        if !ddl_reasons.is_empty() {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "column_ddl".into(),
                reason: format!("MySQL {}", ddl_reasons.join("; ")),
            });
        }
        if !generated_expression.is_empty()
            || extra
                .split_whitespace()
                .any(|part| matches!(part, "VIRTUAL" | "STORED" | "GENERATED"))
        {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "generated_column".into(),
                reason: "MySQL generated-column expression and dependency semantics are not yet modeled exactly"
                    .into(),
            });
        }
        columns
            .entry(table_name.clone())
            .or_default()
            .push(contract);
        let mut attributes = BTreeMap::new();
        attributes.insert("table_name".into(), serde_json::json!(table_name));
        attributes.insert("ordinal".into(), serde_json::json!(ordinal));
        attributes.insert("default".into(), serde_json::json!(default));
        attributes.insert("nullable".into(), serde_json::json!(nullable == "YES"));
        attributes.insert("data_type".into(), serde_json::json!(data_type));
        attributes.insert("column_type".into(), serde_json::json!(column_type));
        attributes.insert(
            "mysql_ddl_type".into(),
            serde_json::to_value(ddl_type)
                .map_err(|error| ConnectionError::Database(error.to_string()))?,
        );
        attributes.insert("character_set".into(), serde_json::json!(charset));
        attributes.insert("collation".into(), serde_json::json!(collation));
        attributes.insert("extra".into(), serde_json::json!(extra));
        attributes.insert(
            "generation_expression".into(),
            serde_json::json!(generated_expression),
        );
        attributes.insert(
            "numeric_precision".into(),
            serde_json::json!(numeric_precision),
        );
        attributes.insert("numeric_scale".into(), serde_json::json!(numeric_scale));
        attributes.insert(
            "datetime_precision".into(),
            serde_json::json!(datetime_precision),
        );
        objects.push(CatalogObject {
            id,
            kind: CatalogObjectKind::Column,
            name: identifier,
            definition: Vec::new(),
            attributes,
        });
    }

    let indexes = parse_indexes(database, index_rows)?;
    let index_by_table = indexes.iter().fold(
        BTreeMap::<String, Vec<MySqlIndexContract>>::new(),
        |mut map, index| {
            map.entry(index.table_name.clone())
                .or_default()
                .push(index.clone());
            map
        },
    );

    for row in table_rows {
        let (
            table_name,
            table_type,
            engine,
            table_collation,
            table_character_set,
            create_options,
            auto_increment,
        ): MySqlTableRow = mysql::from_row_opt(row).map_err(|error| {
            ConnectionError::Database(format!("invalid MySQL table catalog row: {error}"))
        })?;
        let id = catalog_id("table", database, &table_name, "");
        let table_identifier = Identifier::new(table_name.clone())
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        if table_type != "BASE TABLE" {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "view".into(),
                reason:
                    "MySQL views are inventoried but are not yet supported by Phase 6 execution"
                        .into(),
            });
        }
        if table_type == "BASE TABLE" && engine.as_deref() != Some("InnoDB") {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "table".into(),
                reason: format!(
                    "MySQL table uses unsupported non-MVCC or unknown engine {}",
                    engine.as_deref().unwrap_or("NULL")
                ),
            });
        }
        if table_type == "BASE TABLE"
            && (table_character_set.is_none()
                || table_collation.is_none()
                || create_options
                    .as_deref()
                    .is_none_or(|options| !options.is_empty()))
        {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "table_ddl".into(),
                reason: "MySQL table character-set/collation identity or nondefault create options are not yet modeled as typed DDL"
                    .into(),
            });
        }
        let table_columns = columns.get(&table_name).cloned().unwrap_or_default();
        let table_indexes = index_by_table.get(&table_name).cloned().unwrap_or_default();
        let resumable_key = select_resumable_key(&table_columns, &table_indexes, server_version);
        if table_type == "BASE TABLE"
            && engine.as_deref() == Some("InnoDB")
            && resumable_key.is_none()
        {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "resumable_key".into(),
                reason: "MySQL table has no complete non-null unique key with supported ordering semantics"
                    .into(),
            });
        }
        let create_statement = show_create_table(conn, database, &table_name)?;
        let auto_increment_column = table_columns
            .iter()
            .find(|column| {
                column
                    .extra
                    .split_whitespace()
                    .any(|part| part == "auto_increment")
            })
            .map(|column| column.name.clone());
        let mut attributes = BTreeMap::new();
        attributes.insert("table_type".into(), serde_json::json!(table_type));
        attributes.insert("engine".into(), serde_json::json!(engine));
        attributes.insert("collation".into(), serde_json::json!(table_collation));
        attributes.insert(
            "character_set".into(),
            serde_json::json!(table_character_set),
        );
        attributes.insert("create_options".into(), serde_json::json!(create_options));
        attributes.insert("auto_increment".into(), serde_json::json!(auto_increment));
        attributes.insert(
            "auto_increment_column".into(),
            serde_json::to_value(auto_increment_column)
                .map_err(|error| ConnectionError::Database(error.to_string()))?,
        );
        attributes.insert(
            "resumable_key".into(),
            serde_json::to_value(resumable_key)
                .map_err(|error| ConnectionError::Database(error.to_string()))?,
        );
        objects.push(CatalogObject {
            id,
            kind: if table_type == "BASE TABLE" {
                CatalogObjectKind::Table
            } else {
                CatalogObjectKind::View
            },
            name: table_identifier,
            definition: create_statement.into_bytes(),
            attributes,
        });
    }

    for index in indexes {
        let kind = if index.primary {
            CatalogObjectKind::PrimaryKey
        } else if !index.non_unique && index.constraint_backed {
            CatalogObjectKind::UniqueConstraint
        } else {
            CatalogObjectKind::Index
        };
        let id = catalog_id("index", database, &index.table_name, index.name.as_str());
        if index.index_type != "BTREE"
            || !index.visible
            || index.columns.is_empty()
            || index.columns.iter().any(|column| {
                column.name.is_none()
                    || !column.ascending
                    || column.prefix_length.is_some()
                    || column.expression.is_some()
            })
        {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "index".into(),
                reason: "MySQL index uses an expression, prefix, order, visibility, or access method that is not yet modeled exactly"
                    .into(),
            });
        }
        let mut attributes = BTreeMap::new();
        attributes.insert("table_name".into(), serde_json::json!(index.table_name));
        attributes.insert("non_unique".into(), serde_json::json!(index.non_unique));
        attributes.insert("primary".into(), serde_json::json!(index.primary));
        attributes.insert(
            "constraint_backed".into(),
            serde_json::json!(index.constraint_backed),
        );
        attributes.insert("index_type".into(), serde_json::json!(index.index_type));
        attributes.insert("visible".into(), serde_json::json!(index.visible));
        attributes.insert("columns".into(), serde_json::json!(index.columns));
        objects.push(CatalogObject {
            id,
            kind,
            name: index.name,
            definition: Vec::new(),
            attributes,
        });
    }

    inventory_constraints(conn, database, &mut objects, &mut blockers)?;
    inventory_privileges(conn, database, &mut objects, &mut blockers)?;
    inventory_unsupported_programmable_objects(conn, database, &mut objects, &mut blockers)?;
    objects.sort_by(|left, right| left.id.cmp(&right.id));
    blockers.sort_by(|left, right| {
        (&left.object_kind, &left.object_id, &left.reason).cmp(&(
            &right.object_kind,
            &right.object_id,
            &right.reason,
        ))
    });
    let database_identifier = Identifier::new(database)
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
    let dependencies = catalog_dependencies(database, &objects);
    let namespace = CatalogNamespace {
        id: catalog_id("schema", database, database, ""),
        name: database_identifier.clone(),
        owner: None,
        charset: query_database_default(conn, database, "DEFAULT_CHARACTER_SET_NAME")?,
        collation: query_database_default(conn, database, "DEFAULT_COLLATION_NAME")?,
        objects,
    };
    let mut vendor_metadata = BTreeMap::new();
    vendor_metadata.insert("information_schema_stats_expiry".into(), "0".into());
    vendor_metadata.insert(
        "lower_case_table_names".into(),
        lower_case_table_names.to_string(),
    );
    Ok((
        VendorCatalog {
            format_version: MYSQL_CATALOG_FORMAT_VERSION,
            dialect: "mysql".into(),
            server_version: server_version.into(),
            database: database_identifier,
            namespaces: vec![namespace],
            dependencies,
            vendor_metadata,
        },
        blockers,
    ))
}

#[derive(Debug, Clone)]
struct MySqlColumnContract {
    name: Identifier,
    ordinal: u64,
    nullable: bool,
    data_type: String,
    collation: Option<String>,
    extra: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MySqlIndexColumn {
    name: Option<Identifier>,
    ordinal: u64,
    ascending: bool,
    prefix_length: Option<u64>,
    nullable: bool,
    expression: Option<String>,
}

#[derive(Debug, Clone)]
struct MySqlIndexContract {
    table_name: String,
    name: Identifier,
    non_unique: bool,
    primary: bool,
    constraint_backed: bool,
    index_type: String,
    visible: bool,
    columns: Vec<MySqlIndexColumn>,
}

#[derive(Debug)]
struct MySqlIndexRowContract {
    ordinal: u64,
    column: MySqlIndexColumn,
    non_unique: bool,
    index_type: String,
    visible: bool,
    constraint_backed: bool,
}

fn parse_indexes(database: &str, rows: Vec<Row>) -> ConnectionResult<Vec<MySqlIndexContract>> {
    let mut grouped = BTreeMap::<(String, String), Vec<MySqlIndexRowContract>>::new();
    for row in rows {
        let (
            table_name,
            index_name,
            non_unique,
            ordinal,
            column_name,
            collation,
            prefix_length,
            nullable,
            index_type,
            visible,
            expression,
            constraint_backed,
        ): MySqlIndexRow = mysql::from_row(row);
        if column_name.is_none() && expression.is_none() {
            return Err(ConnectionError::InvalidRequest(format!(
                "MySQL index {database}.{table_name}.{index_name} has neither a column nor an expression"
            )));
        }
        let name = column_name
            .map(Identifier::new)
            .transpose()
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        grouped
            .entry((table_name, index_name))
            .or_default()
            .push(MySqlIndexRowContract {
                ordinal,
                column: MySqlIndexColumn {
                    name,
                    ordinal,
                    ascending: collation.as_deref() == Some("A"),
                    prefix_length,
                    nullable: nullable == "YES",
                    expression,
                },
                non_unique: non_unique != 0,
                index_type,
                visible: visible == "YES",
                constraint_backed: constraint_backed != 0,
            });
    }
    grouped
        .into_iter()
        .map(|((table_name, index_name), mut rows)| {
            rows.sort_by_key(|row| row.ordinal);
            if rows
                .iter()
                .enumerate()
                .any(|(index, row)| row.ordinal != index as u64 + 1)
                || rows.iter().any(|row| {
                    row.non_unique != rows[0].non_unique
                        || row.index_type != rows[0].index_type
                        || row.visible != rows[0].visible
                        || row.constraint_backed != rows[0].constraint_backed
                })
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL index {database}.{table_name}.{index_name} has inconsistent metadata"
                )));
            }
            let name = Identifier::new(index_name.clone())
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            Ok(MySqlIndexContract {
                table_name,
                name,
                non_unique: rows[0].non_unique,
                primary: index_name == "PRIMARY",
                constraint_backed: rows[0].constraint_backed,
                index_type: rows[0].index_type.clone(),
                visible: rows[0].visible,
                columns: rows.into_iter().map(|row| row.column).collect(),
            })
        })
        .collect()
}

fn select_resumable_key(
    columns: &[MySqlColumnContract],
    indexes: &[MySqlIndexContract],
    server_version: &str,
) -> Option<MySqlResumableKey> {
    if columns
        .iter()
        .enumerate()
        .any(|(index, column)| column.ordinal != index as u64 + 1)
    {
        return None;
    }
    let by_name = columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = indexes
        .iter()
        .filter(|index| {
            !index.non_unique
                && index.visible
                && index.index_type == "BTREE"
                && !index.columns.is_empty()
                && index.columns.iter().all(|index_column| {
                    index_column.ascending
                        && index_column.prefix_length.is_none()
                        && index_column.expression.is_none()
                        && !index_column.nullable
                        && index_column.name.as_ref().is_some_and(|name| {
                            by_name.get(name.as_str()).is_some_and(|column| {
                                !column.nullable
                                    && supported_key_type(
                                        &column.data_type,
                                        column.collation.as_deref(),
                                    )
                            })
                        })
                })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (
            !left.primary,
            left.columns.len(),
            left.columns
                .iter()
                .map(|column| column.name.as_ref().map(Identifier::as_str))
                .collect::<Vec<_>>(),
            left.name.as_str(),
        )
            .cmp(&(
                !right.primary,
                right.columns.len(),
                right
                    .columns
                    .iter()
                    .map(|column| column.name.as_ref().map(Identifier::as_str))
                    .collect::<Vec<_>>(),
                right.name.as_str(),
            ))
    });
    let selected = candidates.first()?;
    let selected_columns = selected
        .columns
        .iter()
        .filter_map(|index_column| {
            index_column
                .name
                .as_ref()
                .and_then(|name| by_name.get(name.as_str()).copied())
        })
        .collect::<Vec<_>>();
    Some(MySqlResumableKey {
        index_name: selected.name.clone(),
        primary: selected.primary,
        columns: selected_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        column_types: selected_columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect(),
        collations: selected_columns
            .iter()
            .map(|column| column.collation.clone())
            .collect(),
        server_version: server_version.into(),
    })
}

fn supported_key_type(data_type: &str, collation: Option<&str>) -> bool {
    match data_type {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "binary" | "varbinary" => true,
        "char" | "varchar" => collation.is_some_and(is_proven_binary_collation),
        _ => false,
    }
}

fn supported_value_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "bigint"
            | "decimal"
            | "numeric"
            | "float"
            | "double"
            | "bit"
            | "date"
            | "datetime"
            | "timestamp"
            | "time"
            | "year"
            | "char"
            | "varchar"
            | "binary"
            | "varbinary"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "enum"
            | "set"
            | "json"
    )
}

fn supported_column_extra(extra: &str) -> bool {
    extra.is_empty() || extra.eq_ignore_ascii_case("auto_increment")
}

fn parse_mysql_column_type(column_type: &str) -> Result<MySqlColumnType, String> {
    let normalized = column_type.trim().to_ascii_lowercase();
    if normalized.contains(" zerofill") {
        return Err("ZEROFILL column semantics are not modeled".into());
    }
    let (base, unsigned) = normalized
        .strip_suffix(" unsigned")
        .map_or((normalized.as_str(), false), |base| (base, true));
    let (name, arguments) = parse_type_name_and_arguments(base)?;
    let argument = |index: usize| -> Result<u32, String> {
        arguments
            .get(index)
            .copied()
            .ok_or_else(|| format!("MySQL type {name} lacks argument {}", index + 1))
    };
    match name {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            if arguments.len() > 1 {
                return Err(format!("MySQL integer type {name} has too many arguments"));
            }
            Ok(MySqlColumnType::Integer {
                name: name.into(),
                unsigned,
                display_width: arguments.first().copied(),
            })
        }
        "decimal" | "numeric" => {
            if arguments.len() != 2 {
                return Err(format!(
                    "MySQL decimal type {name} must bind precision and scale"
                ));
            }
            Ok(MySqlColumnType::Decimal {
                precision: argument(0)?,
                scale: argument(1)?,
                unsigned,
            })
        }
        "float" | "double" => {
            if arguments.len() > 2 {
                return Err(format!("MySQL floating type {name} has too many arguments"));
            }
            Ok(MySqlColumnType::Floating {
                name: name.into(),
                precision: arguments.first().copied(),
                scale: arguments.get(1).copied(),
                unsigned,
            })
        }
        "bit" => {
            if unsigned || arguments.len() != 1 {
                return Err("MySQL BIT must have one length and cannot be unsigned".into());
            }
            Ok(MySqlColumnType::Bit {
                length: argument(0)?,
            })
        }
        "date" => {
            require_no_type_modifiers(name, &arguments, unsigned)?;
            Ok(MySqlColumnType::Temporal {
                name: name.into(),
                fractional_precision: None,
            })
        }
        "datetime" | "timestamp" | "time" => {
            if unsigned || arguments.len() > 1 {
                return Err(format!("MySQL temporal type {name} has invalid modifiers"));
            }
            let fractional_precision = arguments.first().copied();
            if fractional_precision.is_some_and(|precision| precision > 6) {
                return Err(format!("MySQL temporal type {name} precision exceeds 6"));
            }
            Ok(MySqlColumnType::Temporal {
                name: name.into(),
                fractional_precision,
            })
        }
        "year" => {
            if unsigned || !(arguments.is_empty() || arguments.as_slice() == [4]) {
                return Err("MySQL YEAR has unsupported modifiers".into());
            }
            Ok(MySqlColumnType::Year)
        }
        "char" | "varchar" => {
            if unsigned || arguments.len() != 1 || argument(0)? == 0 {
                return Err(format!("MySQL character type {name} has invalid length"));
            }
            Ok(MySqlColumnType::Character {
                name: name.into(),
                length: argument(0)?,
            })
        }
        "binary" | "varbinary" => {
            if unsigned || arguments.len() != 1 || argument(0)? == 0 {
                return Err(format!("MySQL binary type {name} has invalid length"));
            }
            Ok(MySqlColumnType::Binary {
                name: name.into(),
                length: argument(0)?,
            })
        }
        "tinytext" | "text" | "mediumtext" | "longtext" => {
            require_no_type_modifiers(name, &arguments, unsigned)?;
            Ok(MySqlColumnType::Text { name: name.into() })
        }
        "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            require_no_type_modifiers(name, &arguments, unsigned)?;
            Ok(MySqlColumnType::Blob { name: name.into() })
        }
        "json" => {
            require_no_type_modifiers(name, &arguments, unsigned)?;
            Ok(MySqlColumnType::Json)
        }
        _ => Err(format!(
            "MySQL column type {column_type} has no reviewed typed DDL contract"
        )),
    }
}

fn parse_type_name_and_arguments(value: &str) -> Result<(&str, Vec<u32>), String> {
    let Some(open) = value.find('(') else {
        if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphabetic()) {
            return Err("MySQL column type name is invalid".into());
        }
        return Ok((value, Vec::new()));
    };
    if !value.ends_with(')')
        || value[open + 1..value.len() - 1].contains('(')
        || value[open + 1..value.len() - 1].contains(')')
    {
        return Err("MySQL column type arguments are malformed".into());
    }
    let name = &value[..open];
    if name.is_empty() || name.bytes().any(|byte| !byte.is_ascii_alphabetic()) {
        return Err("MySQL column type name is invalid".into());
    }
    let arguments = value[open + 1..value.len() - 1]
        .split(',')
        .map(|argument| {
            argument
                .trim()
                .parse::<u32>()
                .map_err(|_| "MySQL column type argument is not an unsigned integer".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if arguments.is_empty() {
        return Err("MySQL column type has an empty argument list".into());
    }
    Ok((name, arguments))
}

fn require_no_type_modifiers(name: &str, arguments: &[u32], unsigned: bool) -> Result<(), String> {
    if unsigned || !arguments.is_empty() {
        Err(format!("MySQL type {name} has unsupported modifiers"))
    } else {
        Ok(())
    }
}

fn render_mysql_column_type(data_type: &MySqlColumnType) -> Result<String, MySqlPlanError> {
    let rendered = match data_type {
        MySqlColumnType::Integer {
            name,
            unsigned,
            display_width,
        } => {
            if !matches!(
                name.as_str(),
                "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
            ) {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL integer type name is not supported".into(),
                ));
            }
            format!(
                "{name}{}{}",
                display_width.map_or_else(String::new, |width| format!("({width})")),
                if *unsigned { " unsigned" } else { "" }
            )
        }
        MySqlColumnType::Decimal {
            precision,
            scale,
            unsigned,
        } => format!(
            "decimal({precision},{scale}){}",
            if *unsigned { " unsigned" } else { "" }
        ),
        MySqlColumnType::Floating {
            name,
            precision,
            scale,
            unsigned,
        } => {
            if !matches!(name.as_str(), "float" | "double") {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL floating type name is not supported".into(),
                ));
            }
            let arguments = match (precision, scale) {
                (None, None) => String::new(),
                (Some(precision), None) => format!("({precision})"),
                (Some(precision), Some(scale)) => format!("({precision},{scale})"),
                (None, Some(_)) => {
                    return Err(MySqlPlanError::InvalidCatalog(
                        "MySQL floating scale requires precision".into(),
                    ));
                }
            };
            format!(
                "{name}{arguments}{}",
                if *unsigned { " unsigned" } else { "" }
            )
        }
        MySqlColumnType::Bit { length } => format!("bit({length})"),
        MySqlColumnType::Temporal {
            name,
            fractional_precision,
        } => {
            if !matches!(name.as_str(), "date" | "datetime" | "timestamp" | "time") {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL temporal type name is not supported".into(),
                ));
            }
            fractional_precision
                .map_or_else(|| name.clone(), |precision| format!("{name}({precision})"))
        }
        MySqlColumnType::Year => "year".into(),
        MySqlColumnType::Character { name, length } => {
            if !matches!(name.as_str(), "char" | "varchar") {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL character type name is not supported".into(),
                ));
            }
            format!("{name}({length})")
        }
        MySqlColumnType::Binary { name, length } => {
            if !matches!(name.as_str(), "binary" | "varbinary") {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL binary type name is not supported".into(),
                ));
            }
            format!("{name}({length})")
        }
        MySqlColumnType::Text { name } => {
            if !matches!(
                name.as_str(),
                "tinytext" | "text" | "mediumtext" | "longtext"
            ) {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL text type name is not supported".into(),
                ));
            }
            name.clone()
        }
        MySqlColumnType::Blob { name } => {
            if !matches!(
                name.as_str(),
                "tinyblob" | "blob" | "mediumblob" | "longblob"
            ) {
                return Err(MySqlPlanError::InvalidCatalog(
                    "MySQL blob type name is not supported".into(),
                ));
            }
            name.clone()
        }
        MySqlColumnType::Json => "json".into(),
    };
    Ok(rendered)
}

fn is_proven_binary_collation(collation: &str) -> bool {
    collation == "binary" || collation.ends_with("_bin")
}

fn show_create_table(conn: &mut Conn, database: &str, table: &str) -> ConnectionResult<String> {
    let sql = format!(
        "SHOW CREATE TABLE {}.{}",
        quote_identifier_text(database),
        quote_identifier_text(table)
    );
    let row: Option<Row> = conn.query_first(sql).map_err(database_error)?;
    let mut values = row
        .ok_or_else(|| ConnectionError::Database("SHOW CREATE TABLE returned no row".into()))?
        .unwrap();
    if values.len() < 2 {
        return Err(ConnectionError::Database(
            "SHOW CREATE TABLE returned an incomplete row".into(),
        ));
    }
    match values.swap_remove(1) {
        Value::Bytes(bytes) => String::from_utf8(bytes)
            .map_err(|_| ConnectionError::Database("SHOW CREATE TABLE is not UTF-8".into())),
        _ => Err(ConnectionError::Database(
            "SHOW CREATE TABLE returned a non-text definition".into(),
        )),
    }
}

fn inventory_constraints(
    conn: &mut Conn,
    database: &str,
    objects: &mut Vec<CatalogObject>,
    blockers: &mut Vec<MySqlCatalogBlocker>,
) -> ConnectionResult<()> {
    let constraints: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, ENFORCED FROM information_schema.TABLE_CONSTRAINTS WHERE BINARY CONSTRAINT_SCHEMA = BINARY ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let key_columns: Vec<MySqlConstraintKeyRow> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE BINARY CONSTRAINT_SCHEMA = BINARY ? ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
            (database,),
        )
        .map_err(database_error)?;
    let references: Vec<MySqlReferenceRow> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME, MATCH_OPTION, UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE BINARY CONSTRAINT_SCHEMA = BINARY ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let checks: Vec<(String, String)> = conn
        .exec(
            "SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE BINARY CONSTRAINT_SCHEMA = BINARY ? ORDER BY CONSTRAINT_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let columns_by_constraint = key_columns.into_iter().fold(
        BTreeMap::<(String, String), Vec<MySqlForeignKeyColumnRow>>::new(),
        |mut map, (table, constraint, column, ordinal, ref_schema, ref_table, ref_column)| {
            map.entry((table, constraint))
                .or_default()
                .push((ordinal, column, ref_schema, ref_table, ref_column));
            map
        },
    );
    let references_by_constraint = references
        .into_iter()
        .map(
            |(
                table,
                constraint,
                unique_schema,
                unique_name,
                match_option,
                update_rule,
                delete_rule,
            )| {
                (
                    (table, constraint),
                    (
                        unique_schema,
                        unique_name,
                        match_option,
                        update_rule,
                        delete_rule,
                    ),
                )
            },
        )
        .collect::<BTreeMap<_, _>>();
    let checks_by_name = checks.into_iter().collect::<BTreeMap<_, _>>();
    for (table, name, constraint_type, enforced) in constraints {
        if matches!(constraint_type.as_str(), "PRIMARY KEY" | "UNIQUE") {
            continue;
        }
        let id = catalog_id("constraint", database, &table, &name);
        let kind = match constraint_type.as_str() {
            "FOREIGN KEY" => CatalogObjectKind::ForeignKey,
            "CHECK" => CatalogObjectKind::CheckConstraint,
            _ => CatalogObjectKind::Vendor("mysql_constraint".into()),
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("table_name".into(), serde_json::json!(table));
        attributes.insert("constraint_type".into(), serde_json::json!(constraint_type));
        attributes.insert("enforced".into(), serde_json::json!(enforced == "YES"));
        if let Some(columns) = columns_by_constraint.get(&(table.clone(), name.clone())) {
            attributes.insert("columns".into(), serde_json::json!(columns));
        }
        if let Some(reference) = references_by_constraint.get(&(table.clone(), name.clone())) {
            attributes.insert("reference".into(), serde_json::json!(reference));
        }
        if let Some(check_clause) = checks_by_name.get(&name) {
            attributes.insert("check_clause".into(), serde_json::json!(check_clause));
        }
        if kind != CatalogObjectKind::ForeignKey
            || enforced != "YES"
            || references_by_constraint
                .get(&(table.clone(), name.clone()))
                .is_none_or(
                    |(schema, unique_name, match_option, update_rule, delete_rule)| {
                        schema.as_deref() != Some(database)
                            || unique_name.as_deref().is_none_or(str::is_empty)
                            || match_option != "NONE"
                            || parse_mysql_foreign_key_action(update_rule).is_err()
                            || parse_mysql_foreign_key_action(delete_rule).is_err()
                    },
                )
            || columns_by_constraint
                .get(&(table.clone(), name.clone()))
                .is_none_or(|columns| {
                    columns.is_empty()
                        || columns.iter().any(
                            |(_, _, schema, referenced_table, referenced_column)| {
                                schema.as_deref() != Some(database)
                                    || referenced_table.as_deref().is_none_or(str::is_empty)
                                    || referenced_column.as_deref().is_none_or(str::is_empty)
                            },
                        )
                })
        {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: match kind {
                    CatalogObjectKind::ForeignKey => "foreign_key",
                    CatalogObjectKind::CheckConstraint => "check_constraint",
                    _ => "constraint",
                }
                .into(),
                reason: "MySQL constraint semantics are outside the typed foreign-key subset or are malformed"
                    .into(),
            });
        }
        objects.push(CatalogObject {
            id,
            kind,
            name: Identifier::new(name)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
            definition: Vec::new(),
            attributes,
        });
    }
    Ok(())
}

fn inventory_privileges(
    conn: &mut Conn,
    database: &str,
    objects: &mut Vec<CatalogObject>,
    blockers: &mut Vec<MySqlCatalogBlocker>,
) -> ConnectionResult<()> {
    let schema_privileges: Vec<(String, String, String)> = conn
        .exec(
            "SELECT GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.SCHEMA_PRIVILEGES WHERE BINARY TABLE_SCHEMA = BINARY ? ORDER BY GRANTEE, PRIVILEGE_TYPE",
            (database,),
        )
        .map_err(database_error)?;
    for (grantee, privilege, grantable) in schema_privileges {
        add_privilege_object(
            database,
            PrivilegeRecord {
                scope: "schema",
                object: database,
                column: None,
                grantee,
                privilege,
                grantable,
            },
            objects,
            blockers,
        )?;
    }
    let table_privileges: Vec<(String, String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.TABLE_PRIVILEGES WHERE BINARY TABLE_SCHEMA = BINARY ? ORDER BY TABLE_NAME, GRANTEE, PRIVILEGE_TYPE",
            (database,),
        )
        .map_err(database_error)?;
    for (table, grantee, privilege, grantable) in table_privileges {
        add_privilege_object(
            database,
            PrivilegeRecord {
                scope: "table",
                object: &table,
                column: None,
                grantee,
                privilege,
                grantable,
            },
            objects,
            blockers,
        )?;
    }
    let column_privileges: Vec<(String, String, String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME, GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.COLUMN_PRIVILEGES WHERE BINARY TABLE_SCHEMA = BINARY ? ORDER BY TABLE_NAME, COLUMN_NAME, GRANTEE, PRIVILEGE_TYPE",
            (database,),
        )
        .map_err(database_error)?;
    for (table, column, grantee, privilege, grantable) in column_privileges {
        add_privilege_object(
            database,
            PrivilegeRecord {
                scope: "column",
                object: &table,
                column: Some(&column),
                grantee,
                privilege,
                grantable,
            },
            objects,
            blockers,
        )?;
    }
    Ok(())
}

struct PrivilegeRecord<'a> {
    scope: &'a str,
    object: &'a str,
    column: Option<&'a str>,
    grantee: String,
    privilege: String,
    grantable: String,
}

fn add_privilege_object(
    database: &str,
    record: PrivilegeRecord<'_>,
    objects: &mut Vec<CatalogObject>,
    blockers: &mut Vec<MySqlCatalogBlocker>,
) -> ConnectionResult<()> {
    let member = serde_json::to_string(&(
        record.column,
        &record.grantee,
        &record.privilege,
        &record.grantable,
    ))
    .map_err(|error| ConnectionError::Database(error.to_string()))?;
    let id = catalog_id("privilege", database, record.object, &member);
    let mut attributes = BTreeMap::new();
    attributes.insert("scope".into(), serde_json::json!(record.scope));
    attributes.insert("object".into(), serde_json::json!(record.object));
    attributes.insert("column".into(), serde_json::json!(record.column));
    attributes.insert("grantee".into(), serde_json::json!(&record.grantee));
    attributes.insert("privilege".into(), serde_json::json!(&record.privilege));
    attributes.insert(
        "grantable".into(),
        serde_json::json!(record.grantable == "YES"),
    );
    blockers.push(MySqlCatalogBlocker {
        object_id: id.clone(),
        object_kind: "privilege".into(),
        reason: "MySQL object privileges are inventoried but target role mapping and exact restoration are not yet implemented"
            .into(),
    });
    objects.push(CatalogObject {
        id,
        kind: CatalogObjectKind::Vendor("mysql_privilege".into()),
        name: Identifier::new(format!(
            "{}:{}:{}",
            record.scope, record.object, record.privilege
        ))
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
        definition: Vec::new(),
        attributes,
    });
    Ok(())
}

fn inventory_unsupported_programmable_objects(
    conn: &mut Conn,
    database: &str,
    objects: &mut Vec<CatalogObject>,
    blockers: &mut Vec<MySqlCatalogBlocker>,
) -> ConnectionResult<()> {
    for (query, kind, object_kind) in [
        (
            "SELECT TRIGGER_NAME, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE BINARY TRIGGER_SCHEMA = BINARY ? ORDER BY TRIGGER_NAME",
            CatalogObjectKind::Trigger,
            "trigger",
        ),
        (
            "SELECT ROUTINE_NAME, COALESCE(ROUTINE_DEFINITION, '') FROM information_schema.ROUTINES WHERE BINARY ROUTINE_SCHEMA = BINARY ? ORDER BY ROUTINE_NAME, SPECIFIC_NAME",
            CatalogObjectKind::Routine,
            "routine",
        ),
        (
            "SELECT EVENT_NAME, COALESCE(EVENT_DEFINITION, '') FROM information_schema.EVENTS WHERE BINARY EVENT_SCHEMA = BINARY ? ORDER BY EVENT_NAME",
            CatalogObjectKind::Event,
            "event",
        ),
    ] {
        let rows: Vec<(String, String)> = conn.exec(query, (database,)).map_err(database_error)?;
        for (name, definition) in rows {
            let id = catalog_id(object_kind, database, &name, "");
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: object_kind.into(),
                reason: format!("MySQL {object_kind} execution is not yet modeled exactly"),
            });
            objects.push(CatalogObject {
                id,
                kind: kind.clone(),
                name: Identifier::new(name)
                    .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
                definition: definition.into_bytes(),
                attributes: BTreeMap::new(),
            });
        }
    }
    let partitions: Vec<(String, String, Option<String>, Option<String>)> = conn
        .exec(
            "SELECT TABLE_NAME, PARTITION_NAME, PARTITION_METHOD, PARTITION_EXPRESSION FROM information_schema.PARTITIONS WHERE BINARY TABLE_SCHEMA = BINARY ? AND PARTITION_NAME IS NOT NULL ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION",
            (database,),
        )
        .map_err(database_error)?;
    for (table, name, method, expression) in partitions {
        let id = catalog_id("partition", database, &table, &name);
        blockers.push(MySqlCatalogBlocker {
            object_id: id.clone(),
            object_kind: "partition".into(),
            reason: "MySQL partition topology is not yet modeled exactly".into(),
        });
        let mut attributes = BTreeMap::new();
        attributes.insert("table_name".into(), serde_json::json!(table));
        attributes.insert("method".into(), serde_json::json!(method));
        attributes.insert("expression".into(), serde_json::json!(expression));
        objects.push(CatalogObject {
            id,
            kind: CatalogObjectKind::Partition,
            name: Identifier::new(name)
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
            definition: Vec::new(),
            attributes,
        });
    }
    Ok(())
}

fn query_database_default(
    conn: &mut Conn,
    database: &str,
    column: &str,
) -> ConnectionResult<Option<String>> {
    if !matches!(
        column,
        "DEFAULT_CHARACTER_SET_NAME" | "DEFAULT_COLLATION_NAME"
    ) {
        return Err(ConnectionError::InvalidRequest(
            "invalid information_schema schema attribute".into(),
        ));
    }
    conn.exec_first(
        format!(
            "SELECT {column} FROM information_schema.SCHEMATA WHERE BINARY SCHEMA_NAME = BINARY ?"
        ),
        (database,),
    )
    .map_err(database_error)
}

fn catalog_dependencies(database: &str, objects: &[CatalogObject]) -> Vec<CatalogDependency> {
    let mut dependencies = Vec::new();
    for object in objects {
        if let Some(table) = object
            .attributes
            .get("table_name")
            .and_then(serde_json::Value::as_str)
        {
            dependencies.push(CatalogDependency {
                from_object_id: object.id.clone(),
                to_object_id: catalog_id("table", database, table, ""),
                dependency_type: "belongs_to_table".into(),
            });
        }
        if object.kind == CatalogObjectKind::ForeignKey {
            let referenced_table = object
                .attributes
                .get("columns")
                .and_then(serde_json::Value::as_array)
                .and_then(|columns| columns.first())
                .and_then(serde_json::Value::as_array)
                .and_then(|column| column.get(3))
                .and_then(serde_json::Value::as_str);
            if let Some(referenced_table) = referenced_table {
                dependencies.push(CatalogDependency {
                    from_object_id: object.id.clone(),
                    to_object_id: catalog_id("table", database, referenced_table, ""),
                    dependency_type: "references_table".into(),
                });
            }
        }
    }
    dependencies.sort_by(|left, right| {
        (
            &left.from_object_id,
            &left.to_object_id,
            &left.dependency_type,
        )
            .cmp(&(
                &right.from_object_id,
                &right.to_object_id,
                &right.dependency_type,
            ))
    });
    dependencies
}

pub fn mysql_foreign_keys(catalog: &VendorCatalog) -> ConnectionResult<Vec<MySqlForeignKey>> {
    mysql_foreign_keys_with_policy(catalog, false)
}

fn mysql_supported_foreign_keys(catalog: &VendorCatalog) -> ConnectionResult<Vec<MySqlForeignKey>> {
    mysql_foreign_keys_with_policy(catalog, true)
}

fn mysql_foreign_keys_with_policy(
    catalog: &VendorCatalog,
    skip_unsupported: bool,
) -> ConnectionResult<Vec<MySqlForeignKey>> {
    if catalog.dialect != "mysql" || catalog.format_version != MYSQL_CATALOG_FORMAT_VERSION {
        return Err(ConnectionError::InvalidRequest(
            "foreign keys require the current MySQL catalog format".into(),
        ));
    }
    let namespace = catalog
        .namespaces
        .iter()
        .find(|namespace| namespace.name == catalog.database)
        .ok_or_else(|| {
            ConnectionError::InvalidRequest("MySQL catalog database namespace is absent".into())
        })?;
    let table_columns = namespace
        .objects
        .iter()
        .filter(|object| object.kind == CatalogObjectKind::Column)
        .filter_map(|object| {
            optional_text(object, "table_name").map(|table| ((table, object.name.as_str()), object))
        })
        .collect::<BTreeMap<_, _>>();
    let indexes = namespace
        .objects
        .iter()
        .filter(|object| {
            matches!(
                object.kind,
                CatalogObjectKind::PrimaryKey
                    | CatalogObjectKind::UniqueConstraint
                    | CatalogObjectKind::Index
            )
        })
        .collect::<Vec<_>>();
    let mut foreign_keys = Vec::new();
    for object in namespace
        .objects
        .iter()
        .filter(|object| object.kind == CatalogObjectKind::ForeignKey)
    {
        let parsed = (|| -> ConnectionResult<MySqlForeignKey> {
            if required_text(object, "constraint_type")
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?
                != "FOREIGN KEY"
                || !required_bool(object, "enforced")
                    .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} is not enforced",
                    object.id
                )));
            }
            let table_name = required_text(object, "table_name")
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let mut columns: Vec<MySqlForeignKeyColumnRow> = serde_json::from_value(
                object.attributes.get("columns").cloned().ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL foreign key {} lacks columns",
                        object.id
                    ))
                })?,
            )
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let (referenced_schema, referenced_constraint, match_option, update_rule, delete_rule): (
            Option<String>,
            Option<String>,
            String,
            String,
            String,
        ) = serde_json::from_value(object.attributes.get("reference").cloned().ok_or_else(
            || {
                ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} lacks reference metadata",
                    object.id
                ))
            },
        )?)
        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            columns.sort_by_key(|column| column.0);
            if columns.is_empty()
                || columns
                    .iter()
                    .enumerate()
                    .any(|(index, column)| column.0 != index as u64 + 1)
                || match_option != "NONE"
                || referenced_schema.as_deref() != Some(catalog.database.as_str())
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} has unsupported match or column metadata",
                    object.id
                )));
            }
            let referenced_table_name = columns
                .first()
                .and_then(|column| column.3.as_deref())
                .ok_or_else(|| {
                    ConnectionError::InvalidRequest(format!(
                        "MySQL foreign key {} lacks a referenced table",
                        object.id
                    ))
                })?;
            if columns.iter().any(|column| {
                column.2.as_deref() != Some(catalog.database.as_str())
                    || column.3.as_deref() != Some(referenced_table_name)
                    || column.4.is_none()
                    || !table_columns.contains_key(&(table_name, column.1.as_str()))
                    || !table_columns.contains_key(&(
                        referenced_table_name,
                        column.4.as_deref().unwrap_or_default(),
                    ))
            }) {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} has a dangling or cross-database column",
                    object.id
                )));
            }
            let child_columns = columns
                .iter()
                .map(|column| Identifier::new(column.1.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let referenced_columns = columns
                .iter()
                .map(|column| {
                    let name = column.4.clone().ok_or_else(|| {
                        ConnectionError::InvalidRequest(format!(
                            "MySQL foreign key {} lacks a referenced column",
                            object.id
                        ))
                    })?;
                    Identifier::new(name)
                        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))
                })
                .collect::<ConnectionResult<Vec<_>>>()?;
            let referenced_constraint = Identifier::new(
                referenced_constraint
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        ConnectionError::InvalidRequest(format!(
                            "MySQL foreign key {} lacks a referenced key identity",
                            object.id
                        ))
                    })?,
            )
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let referenced_index = indexes.iter().find(|index| {
                optional_text(index, "table_name") == Some(referenced_table_name)
                    && index.name == referenced_constraint
            });
            let referenced_index_columns = referenced_index
                .and_then(|index| index.attributes.get("columns"))
                .cloned()
                .map(serde_json::from_value::<Vec<MySqlIndexColumn>>)
                .transpose()
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            if referenced_index.is_none_or(|index| {
                required_bool(index, "non_unique").unwrap_or(true)
                    || required_text(index, "index_type").ok() != Some("BTREE")
                    || !required_bool(index, "visible").unwrap_or(false)
            }) || referenced_index_columns.is_none_or(|index_columns| {
                index_columns.len() != referenced_columns.len()
                    || index_columns.iter().zip(&referenced_columns).any(
                        |(index_column, referenced_column)| {
                            index_column.name.as_ref() != Some(referenced_column)
                                || !index_column.ascending
                                || index_column.prefix_length.is_some()
                                || index_column.expression.is_some()
                        },
                    )
            }) {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} does not reference an exact supported unique key",
                    object.id
                )));
            }
            let delete_action = parse_mysql_foreign_key_action(&delete_rule)?;
            if delete_action == MySqlForeignKeyAction::SetNull
                && columns.iter().any(|column| {
                    table_columns
                        .get(&(table_name, column.1.as_str()))
                        .is_none_or(|object| required_bool(object, "nullable").ok() != Some(true))
                })
            {
                return Err(ConnectionError::InvalidRequest(format!(
                    "MySQL foreign key {} uses SET NULL on a non-null column",
                    object.id
                )));
            }
            Ok(MySqlForeignKey {
                catalog_object_id: object.id.clone(),
                name: object.name.clone(),
                table: QualifiedTable {
                    namespace: catalog.database.clone(),
                    name: Identifier::new(table_name)
                        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
                },
                columns: child_columns,
                referenced_table: QualifiedTable {
                    namespace: catalog.database.clone(),
                    name: Identifier::new(referenced_table_name)
                        .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?,
                },
                referenced_columns,
                referenced_constraint,
                update_action: parse_mysql_foreign_key_action(&update_rule)?,
                delete_action,
                enforced: true,
            })
        })();
        match parsed {
            Ok(foreign_key) => foreign_keys.push(foreign_key),
            Err(_) if skip_unsupported => {}
            Err(error) => return Err(error),
        }
    }
    foreign_keys.sort_by(|left, right| {
        (&left.table, &left.name, &left.catalog_object_id).cmp(&(
            &right.table,
            &right.name,
            &right.catalog_object_id,
        ))
    });
    if foreign_keys.windows(2).any(|pair| {
        pair[0].catalog_object_id == pair[1].catalog_object_id
            || (pair[0].table == pair[1].table && pair[0].name == pair[1].name)
    }) {
        return Err(ConnectionError::InvalidRequest(
            "MySQL catalog contains duplicate foreign-key identities".into(),
        ));
    }
    Ok(foreign_keys)
}

fn parse_mysql_foreign_key_action(value: &str) -> ConnectionResult<MySqlForeignKeyAction> {
    match value {
        "NO ACTION" => Ok(MySqlForeignKeyAction::NoAction),
        "RESTRICT" => Ok(MySqlForeignKeyAction::Restrict),
        "CASCADE" => Ok(MySqlForeignKeyAction::Cascade),
        "SET NULL" => Ok(MySqlForeignKeyAction::SetNull),
        _ => Err(ConnectionError::InvalidRequest(format!(
            "unsupported MySQL foreign-key action {value:?}"
        ))),
    }
}

pub fn mysql_auto_increment_states(
    catalog: &VendorCatalog,
) -> ConnectionResult<Vec<MySqlAutoIncrementState>> {
    if catalog.dialect != "mysql" || catalog.format_version != MYSQL_CATALOG_FORMAT_VERSION {
        return Err(ConnectionError::InvalidRequest(
            "AUTO_INCREMENT state requires the current MySQL catalog format".into(),
        ));
    }
    let stats_expiry = catalog
        .vendor_metadata
        .get("information_schema_stats_expiry")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            ConnectionError::InvalidRequest(
                "MySQL catalog lacks information_schema_stats_expiry evidence".into(),
            )
        })?;
    if stats_expiry != 0 {
        return Err(ConnectionError::InvalidRequest(
            "MySQL AUTO_INCREMENT evidence was captured with cached statistics enabled".into(),
        ));
    }
    let mut states = Vec::new();
    for namespace in &catalog.namespaces {
        for table in namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Table)
        {
            let Some(column_value) = table.attributes.get("auto_increment_column") else {
                continue;
            };
            if column_value.is_null() {
                continue;
            }
            let column: Identifier = serde_json::from_value(column_value.clone())
                .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
            let next_value = table
                .attributes
                .get("auto_increment")
                .and_then(|value| value.as_u64());
            states.push(MySqlAutoIncrementState {
                table: QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: table.name.clone(),
                },
                column,
                next_value,
                stats_expiry,
            });
        }
    }
    states.sort_by(|left, right| left.table.cmp(&right.table));
    Ok(states)
}

pub fn mysql_catalog_fingerprint(catalog: &VendorCatalog) -> ConnectionResult<String> {
    if catalog.dialect != "mysql" || catalog.format_version != MYSQL_CATALOG_FORMAT_VERSION {
        return Err(ConnectionError::InvalidRequest(
            "catalog fingerprint requires the current MySQL catalog format".into(),
        ));
    }
    let bytes = serde_json::to_vec(catalog)
        .map_err(|error| ConnectionError::Database(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn mysql_snapshot_id(
    endpoint_identity: &str,
    server_uuid: &str,
    gtid: &str,
    connection_id: u32,
    lifecycle_id: &str,
) -> String {
    let bytes = serde_json::to_vec(&(
        "mysql_snapshot_session_v1",
        endpoint_identity,
        server_uuid,
        gtid,
        connection_id,
        lifecycle_id,
    ))
    .expect("tuple serialization cannot fail");
    format!(
        "mysql-session-sha256:{}",
        hex::encode(Sha256::digest(bytes))
    )
}

fn catalog_id(kind: &str, database: &str, object: &str, member: &str) -> String {
    let bytes = serde_json::to_vec(&(kind, database, object, member))
        .expect("string tuple serialization cannot fail");
    format!("mysql:{kind}:sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn quote_identifier(identifier: &Identifier) -> String {
    quote_identifier_text(identifier.as_str())
}

fn quote_identifier_text(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn required_text<'a>(object: &'a CatalogObject, key: &str) -> Result<&'a str, MySqlPlanError> {
    object
        .attributes
        .get(key)
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(format!("{} lacks text attribute {key}", object.id))
        })
}

fn optional_text<'a>(object: &'a CatalogObject, key: &str) -> Option<&'a str> {
    object.attributes.get(key).and_then(|value| value.as_str())
}

fn required_bool(object: &CatalogObject, key: &str) -> Result<bool, MySqlPlanError> {
    object
        .attributes
        .get(key)
        .and_then(|value| value.as_bool())
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(format!("{} lacks bool attribute {key}", object.id))
        })
}

fn required_u64(object: &CatalogObject, key: &str) -> Result<u64, MySqlPlanError> {
    object
        .attributes
        .get(key)
        .and_then(|value| value.as_u64())
        .ok_or_else(|| {
            MySqlPlanError::InvalidCatalog(format!("{} lacks u64 attribute {key}", object.id))
        })
}

fn optional_u64(object: &CatalogObject, key: &str) -> Option<u64> {
    object.attributes.get(key).and_then(|value| value.as_u64())
}

fn optional_i64(object: &CatalogObject, key: &str) -> Option<i64> {
    object.attributes.get(key).and_then(|value| value.as_i64())
}

fn take_cell<T>(row: &mut Row, index: usize, name: &str) -> ConnectionResult<T>
where
    T: mysql::prelude::FromValue,
{
    row.take_opt(index)
        .ok_or_else(|| ConnectionError::Database(format!("MySQL catalog row lacks column {name}")))?
        .map_err(|error| {
            ConnectionError::Database(format!(
                "MySQL catalog column {name} has an invalid value: {error}"
            ))
        })
}

fn configure_mysql_session(conn: &mut Conn) -> ConnectionResult<()> {
    conn.query_drop("SET SESSION time_zone = '+00:00'")
        .map_err(database_error)?;
    conn.query_drop(format!("SET SESSION sql_mode = '{MYSQL_STRICT_SQL_MODE}'"))
        .map_err(database_error)?;
    conn.query_drop(format!(
        "SET NAMES {MYSQL_SESSION_CHARACTER_SET} COLLATE {MYSQL_SESSION_COLLATION}"
    ))
    .map_err(database_error)?;
    Ok(())
}

fn mysql_live_session_identity(conn: &mut Conn) -> ConnectionResult<MySqlLiveSessionIdentity> {
    let mut row: Row = conn
        .query_first(
            "SELECT DATABASE(), @@hostname, @@port, VERSION(), @@server_uuid, CURRENT_USER(), @@transaction_isolation, @@transaction_read_only, @@session.time_zone, @@session.information_schema_stats_expiry, @@global.gtid_executed, CONNECTION_ID(), @@lower_case_table_names, @@session.sql_mode, @@session.character_set_client, @@session.character_set_connection, @@session.character_set_results, @@session.collation_connection",
        )
        .map_err(database_error)?
        .ok_or_else(|| ConnectionError::Database("MySQL identity query returned no row".into()))?;
    Ok(MySqlLiveSessionIdentity {
        database: take_cell(&mut row, 0, "DATABASE()")?,
        hostname: take_cell(&mut row, 1, "@@hostname")?,
        port: take_cell(&mut row, 2, "@@port")?,
        server_version: take_cell(&mut row, 3, "VERSION()")?,
        server_uuid: take_cell(&mut row, 4, "@@server_uuid")?,
        authenticated_account: take_cell(&mut row, 5, "CURRENT_USER()")?,
        transaction_isolation: take_cell(&mut row, 6, "@@transaction_isolation")?,
        transaction_read_only: take_cell(&mut row, 7, "@@transaction_read_only")?,
        session_time_zone: take_cell(&mut row, 8, "@@session.time_zone")?,
        information_schema_stats_expiry: take_cell(
            &mut row,
            9,
            "@@session.information_schema_stats_expiry",
        )?,
        gtid_executed_observation: take_cell(&mut row, 10, "@@global.gtid_executed")?,
        connection_id: take_cell(&mut row, 11, "CONNECTION_ID()")?,
        lower_case_table_names: take_cell(&mut row, 12, "@@lower_case_table_names")?,
        session_sql_mode: take_cell(&mut row, 13, "@@session.sql_mode")?,
        character_set_client: take_cell(&mut row, 14, "@@session.character_set_client")?,
        character_set_connection: take_cell(&mut row, 15, "@@session.character_set_connection")?,
        character_set_results: take_cell(&mut row, 16, "@@session.character_set_results")?,
        collation_connection: take_cell(&mut row, 17, "@@session.collation_connection")?,
    })
}

fn mysql_session_settings_are_exact(identity: &MySqlLiveSessionIdentity) -> bool {
    identity.session_time_zone == "+00:00"
        && identity.information_schema_stats_expiry == 0
        && identity.session_sql_mode == MYSQL_STRICT_SQL_MODE
        && identity.character_set_client == MYSQL_SESSION_CHARACTER_SET
        && identity.character_set_connection == MYSQL_SESSION_CHARACTER_SET
        && identity.character_set_results == MYSQL_SESSION_CHARACTER_SET
        && identity.collation_connection == MYSQL_SESSION_COLLATION
}

fn database_error(error: mysql::Error) -> ConnectionError {
    ConnectionError::Database(MySqlSafeError::from(error).to_string())
}

fn plan_connection_error(error: MySqlPlanError) -> ConnectionError {
    ConnectionError::Database(error.to_string())
}

fn mysql_foreign_key_ddl_error(error: mysql::Error) -> ConnectionError {
    match error {
        mysql::Error::MySqlError(error)
            if mysql_foreign_key_ddl_error_is_deterministic(error.code, &error.state) =>
        {
            ConnectionError::InvalidRequest(format!(
                "MySQL server rejected ADD FOREIGN KEY with deterministic code {}",
                error.code
            ))
        }
        error => database_error(error),
    }
}

fn mysql_foreign_key_ddl_error_is_deterministic(code: u16, sql_state: &str) -> bool {
    if sql_state.starts_with("23") {
        return true;
    }
    matches!(
        code,
        1005 | 1022 | 1215 | 1451 | 1452 | 1553 | 1821..=1833 | 3780
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).unwrap()
    }

    fn column(
        name: &str,
        data_type: &str,
        nullable: bool,
        collation: Option<&str>,
    ) -> MySqlColumnContract {
        MySqlColumnContract {
            name: identifier(name),
            ordinal: 1,
            nullable,
            data_type: data_type.into(),
            collation: collation.map(str::to_owned),
            extra: String::new(),
        }
    }

    fn index(name: &str, columns: &[(&str, bool, Option<u64>)]) -> MySqlIndexContract {
        MySqlIndexContract {
            table_name: "t".into(),
            name: identifier(name),
            non_unique: false,
            primary: name == "PRIMARY",
            constraint_backed: name == "PRIMARY",
            index_type: "BTREE".into(),
            visible: true,
            columns: columns
                .iter()
                .enumerate()
                .map(
                    |(index, (name, ascending, prefix_length))| MySqlIndexColumn {
                        name: Some(identifier(name)),
                        ordinal: index as u64 + 1,
                        ascending: *ascending,
                        prefix_length: *prefix_length,
                        nullable: false,
                        expression: None,
                    },
                )
                .collect(),
        }
    }

    fn catalog(with_table: bool) -> VendorCatalog {
        let database = identifier("app");
        let mut objects = Vec::new();
        if with_table {
            let key = MySqlResumableKey {
                index_name: identifier("PRIMARY"),
                primary: true,
                columns: vec![identifier("id")],
                column_types: vec!["bigint".into()],
                collations: vec![None],
                server_version: "8.4.0".into(),
            };
            objects.push(CatalogObject {
                id: catalog_id("column", "app", "items", "id"),
                kind: CatalogObjectKind::Column,
                name: identifier("id"),
                definition: Vec::new(),
                attributes: BTreeMap::from([
                    ("table_name".into(), serde_json::json!("items")),
                    ("ordinal".into(), serde_json::json!(1)),
                    ("default".into(), serde_json::Value::Null),
                    ("nullable".into(), serde_json::json!(false)),
                    ("data_type".into(), serde_json::json!("bigint")),
                    ("column_type".into(), serde_json::json!("bigint")),
                    (
                        "mysql_ddl_type".into(),
                        serde_json::to_value(MySqlColumnType::Integer {
                            name: "bigint".into(),
                            unsigned: false,
                            display_width: None,
                        })
                        .unwrap(),
                    ),
                    ("character_set".into(), serde_json::Value::Null),
                    ("collation".into(), serde_json::Value::Null),
                    ("extra".into(), serde_json::json!("")),
                    ("generation_expression".into(), serde_json::json!("")),
                    ("numeric_precision".into(), serde_json::json!(64)),
                    ("numeric_scale".into(), serde_json::json!(0)),
                ]),
            });
            objects.push(CatalogObject {
                id: catalog_id("table", "app", "items", ""),
                kind: CatalogObjectKind::Table,
                name: identifier("items"),
                definition:
                    b"CREATE TABLE `items` (`id` bigint NOT NULL PRIMARY KEY) ENGINE=InnoDB"
                        .to_vec(),
                attributes: BTreeMap::from([
                    ("engine".into(), serde_json::json!("InnoDB")),
                    ("character_set".into(), serde_json::json!("utf8mb4")),
                    ("collation".into(), serde_json::json!("utf8mb4_0900_bin")),
                    ("create_options".into(), serde_json::json!("")),
                    ("auto_increment".into(), serde_json::Value::Null),
                    ("auto_increment_column".into(), serde_json::Value::Null),
                    ("resumable_key".into(), serde_json::to_value(key).unwrap()),
                ]),
            });
        }
        VendorCatalog {
            format_version: MYSQL_CATALOG_FORMAT_VERSION,
            dialect: "mysql".into(),
            server_version: "8.4.0".into(),
            database: database.clone(),
            namespaces: vec![CatalogNamespace {
                id: catalog_id("schema", "app", "app", ""),
                name: database,
                owner: None,
                charset: Some("utf8mb4".into()),
                collation: Some("utf8mb4_0900_bin".into()),
                objects,
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::from([
                ("information_schema_stats_expiry".into(), "0".into()),
                ("lower_case_table_names".into(), "0".into()),
            ]),
        }
    }

    fn postgres_to_mysql_policy() -> TableConversionPolicy {
        let source = ColumnMeta {
            name: identifier("id"),
            ordinal: 1,
            vendor_type: "pg_catalog.int8".into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        };
        let target_type = CrossDialectTargetType::MySql(MySqlTargetType::Integer {
            width: MySqlIntegerWidth::Big,
            unsigned: false,
        });
        TableConversionPolicy {
            source_table: QualifiedTable {
                namespace: identifier("public"),
                name: identifier("items"),
            },
            target_table: QualifiedTable {
                namespace: identifier("app"),
                name: identifier("items"),
            },
            target_contract: CrossDialectTargetTableContract::MySql {
                engine: MySqlTargetEngine::InnoDb,
                character_set: identifier("utf8mb4"),
                collation: identifier("utf8mb4_0900_bin"),
            },
            resumable_key: CrossDialectResumableKey {
                source_name: identifier("items_pkey"),
                source_kind: CrossDialectKeyKind::PrimaryKey,
                source_columns: vec![identifier("id")],
                target_name: None,
                target_kind: CrossDialectKeyKind::PrimaryKey,
                target_columns: vec![identifier("id")],
            },
            row_policy: RowConversionPolicy {
                schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
                source_dialect: ConversionDialect::PostgreSql,
                target_dialect: ConversionDialect::MySql,
                columns: vec![super::super::conversion::ColumnConversion {
                    source,
                    source_type: super::super::conversion::CrossDialectSourceType::PostgreSql(
                        super::super::conversion::PostgresTargetType::BigInt,
                    ),
                    target: target_type.column_meta(identifier("id"), 1, false).unwrap(),
                    target_type,
                    target_checks: Vec::new(),
                    rule: super::super::conversion::ValueConversionRule::SignedInteger {
                        minimum: i64::MIN,
                        maximum: i64::MAX,
                    },
                }],
            },
        }
    }

    #[test]
    fn cross_dialect_target_policy_drives_writer_verifier_and_ddl_contracts() {
        let policy = postgres_to_mysql_policy();
        policy.validate().unwrap();
        let evidence = snapshot("mysql://target/app", false).snapshot_evidence;
        let definition = mysql_conversion_table_definition(&policy).unwrap();
        let contract = mysql_conversion_table_contract(&policy, &evidence).unwrap();

        assert_eq!(definition.table, policy.target_table);
        assert_eq!(
            definition.columns[0].data_type,
            MySqlColumnType::Integer {
                name: "bigint".into(),
                unsigned: false,
                display_width: None,
            }
        );
        assert_eq!(definition.indexes[0].name, identifier("PRIMARY"));
        assert_eq!(contract.columns["id"], policy.row_policy.columns[0].target);
        assert_eq!(contract.key.columns, vec![identifier("id")]);
        assert_eq!(contract.key.column_types, vec!["bigint"]);
        assert!(mysql_conversion_checks_are_exact(
            &CatalogNamespace {
                id: "namespace:app".into(),
                name: identifier("app"),
                owner: None,
                charset: Some("utf8mb4".into()),
                collation: Some("utf8mb4_0900_bin".into()),
                objects: Vec::new(),
            },
            &policy,
        )
        .unwrap());
        assert_eq!(
            canonical_mysql_check_clause(" (((`a``(b` BETWEEN 0 AND 7))) "),
            "`a``(b`between0and7"
        );
    }

    fn snapshot(endpoint: &str, with_table: bool) -> MySqlCatalogSnapshot {
        let catalog = catalog(with_table);
        let fingerprint = mysql_catalog_fingerprint(&catalog).unwrap();
        let visibility_blocker = MySqlCatalogBlocker {
            object_id: catalog_id("catalog_visibility", "app", "app", ""),
            object_kind: "catalog_visibility".into(),
            reason: "account-dependent metadata visibility is not yet proven".into(),
        };
        MySqlCatalogSnapshot {
            endpoint_identity: endpoint.into(),
            database_identity: "app".into(),
            server_version: "8.4.0".into(),
            tls_binding: "hostname_verified;roots=platform;client=none".into(),
            snapshot_evidence: MySqlSnapshotEvidence {
                endpoint_identity: endpoint.into(),
                database_identity: "app".into(),
                server_uuid: format!("uuid-{endpoint}"),
                server_version: "8.4.0".into(),
                authenticated_account: "reader@%".into(),
                lifecycle_id: format!("life-{endpoint}"),
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
                catalog_fingerprint: fingerprint,
            },
            catalog,
            blockers: vec![visibility_blocker],
        }
    }

    #[test]
    fn mysql_catalog_derives_a_typed_postgres_table_policy() {
        let source_table = QualifiedTable {
            namespace: identifier("app"),
            name: identifier("items"),
        };
        let policy = mysql_to_postgres_table_conversion_policy(
            &catalog(true),
            &source_table,
            QualifiedTable {
                namespace: identifier("public"),
                name: identifier("items"),
            },
            QualifiedIdentifier {
                namespace: identifier("pg_catalog"),
                name: identifier("C"),
            },
        )
        .unwrap();

        assert_eq!(policy.row_policy.columns.len(), 1);
        assert_eq!(
            policy.row_policy.columns[0].source_type,
            super::super::conversion::CrossDialectSourceType::MySql(MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: false,
            })
        );
        assert_eq!(
            policy.row_policy.columns[0].target_type,
            super::super::conversion::CrossDialectTargetType::PostgreSql(
                super::super::conversion::PostgresTargetType::BigInt,
            )
        );
        let mut hostile = policy;
        hostile.target_table = QualifiedTable {
            namespace: identifier("a\"b"),
            name: identifier("t\"x"),
        };
        assert_eq!(
            super::super::postgres::render_postgres_conversion_create_table(&hostile).unwrap(),
            "CREATE TABLE \"a\"\"b\".\"t\"\"x\" (\"id\" bigint NOT NULL CHECK (\"id\" BETWEEN -9223372036854775808 AND 9223372036854775807), PRIMARY KEY (\"id\"))"
        );
    }

    fn foreign_key_snapshot(endpoint: &str) -> MySqlCatalogSnapshot {
        let mut snapshot = snapshot(endpoint, true);
        let namespace = snapshot.catalog.namespaces.first_mut().unwrap();
        let mut parent_id = namespace
            .objects
            .iter()
            .find(|object| object.kind == CatalogObjectKind::Column && object.name.as_str() == "id")
            .cloned()
            .unwrap();
        parent_id.id = catalog_id("column", "app", "items", "parent_id");
        parent_id.name = identifier("parent_id");
        parent_id
            .attributes
            .insert("ordinal".into(), serde_json::json!(2));
        parent_id
            .attributes
            .insert("nullable".into(), serde_json::json!(true));
        namespace.objects.push(parent_id);
        namespace.objects.push(CatalogObject {
            id: catalog_id("index", "app", "items", "PRIMARY"),
            kind: CatalogObjectKind::PrimaryKey,
            name: identifier("PRIMARY"),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("table_name".into(), serde_json::json!("items")),
                ("non_unique".into(), serde_json::json!(false)),
                ("primary".into(), serde_json::json!(true)),
                ("constraint_backed".into(), serde_json::json!(true)),
                ("index_type".into(), serde_json::json!("BTREE")),
                ("visible".into(), serde_json::json!(true)),
                (
                    "columns".into(),
                    serde_json::to_value(vec![MySqlIndexColumn {
                        name: Some(identifier("id")),
                        ordinal: 1,
                        ascending: true,
                        prefix_length: None,
                        nullable: false,
                        expression: None,
                    }])
                    .unwrap(),
                ),
            ]),
        });
        namespace.objects.push(CatalogObject {
            id: catalog_id("constraint", "app", "items", "fk_items_parent"),
            kind: CatalogObjectKind::ForeignKey,
            name: identifier("fk_items_parent"),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("table_name".into(), serde_json::json!("items")),
                ("constraint_type".into(), serde_json::json!("FOREIGN KEY")),
                ("enforced".into(), serde_json::json!(true)),
                (
                    "columns".into(),
                    serde_json::json!([[1, "parent_id", "app", "items", "id"]]),
                ),
                (
                    "reference".into(),
                    serde_json::json!(["app", "PRIMARY", "NONE", "CASCADE", "SET NULL"]),
                ),
            ]),
        });
        namespace
            .objects
            .sort_by(|left, right| left.id.cmp(&right.id));
        snapshot.catalog.dependencies =
            catalog_dependencies("app", &snapshot.catalog.namespaces[0].objects);
        snapshot.snapshot_evidence.catalog_fingerprint =
            mysql_catalog_fingerprint(&snapshot.catalog).unwrap();
        snapshot
    }

    fn visibility_capture(
        snapshot: &MySqlCatalogSnapshot,
        administrator_user: &str,
    ) -> MySqlMetadataVisibilityCapture {
        let reader = MySqlAccountIdentity {
            user: "reader".into(),
            host: "%".into(),
        };
        let administrator = MySqlAccountIdentity {
            user: administrator_user.into(),
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
        let mut authoritative_catalog = snapshot.catalog.clone();
        let mut authoritative_blockers = snapshot.blockers.clone();
        remove_mysql_privilege_projection(&mut authoritative_catalog, &mut authoritative_blockers);
        let authoritative_catalog_fingerprint =
            mysql_catalog_fingerprint(&authoritative_catalog).unwrap();
        let grant_inventory_digest = grant_inventory.canonical_hash().unwrap();
        MySqlMetadataVisibilityCapture {
            authoritative_catalog,
            authoritative_blockers,
            evidence: MySqlMetadataVisibilityEvidence {
                schema_version: MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
                endpoint_identity: snapshot.endpoint_identity.clone(),
                database_identity: snapshot.database_identity.clone(),
                server_uuid: snapshot.snapshot_evidence.server_uuid.clone(),
                catalog_reader_tls_binding: snapshot.tls_binding.clone(),
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
                catalog_reader_fingerprint: snapshot.snapshot_evidence.catalog_fingerprint.clone(),
                authoritative_catalog_fingerprint,
                grant_inventory_digest,
                grant_inventory,
            },
        }
    }

    #[test]
    fn resumable_key_ranking_and_collation_are_fail_closed() {
        let mut columns = vec![
            column("tenant", "bigint", false, None),
            column("name", "varchar", false, Some("utf8mb4_0900_bin")),
        ];
        columns[1].ordinal = 2;
        let indexes = vec![
            index("z_unique", &[("name", true, None)]),
            index("PRIMARY", &[("tenant", true, None), ("name", true, None)]),
        ];
        let key = select_resumable_key(&columns, &indexes, "8.4.0").unwrap();
        assert!(key.primary);
        assert_eq!(key.columns, vec![identifier("tenant"), identifier("name")]);

        let case_insensitive = vec![column("name", "varchar", false, Some("utf8mb4_0900_ai_ci"))];
        assert!(select_resumable_key(
            &case_insensitive,
            &[index("PRIMARY", &[("name", true, None)])],
            "8.4.0"
        )
        .is_none());
    }

    #[test]
    fn unsuitable_index_forms_are_rejected() {
        let columns = vec![column("id", "bigint", false, None)];
        let mut descending = index("PRIMARY", &[("id", false, None)]);
        assert!(select_resumable_key(&columns, &[descending.clone()], "8.0.40").is_none());
        descending.columns[0].ascending = true;
        descending.columns[0].prefix_length = Some(4);
        assert!(select_resumable_key(&columns, &[descending], "8.0.40").is_none());
    }

    #[test]
    fn typed_foreign_key_contract_drives_checks_ddl_and_plan_order() {
        let source = foreign_key_snapshot("mysql://source/app");
        let foreign_keys = mysql_foreign_keys(&source.catalog).unwrap();
        assert_eq!(foreign_keys.len(), 1);
        let foreign_key = &foreign_keys[0];
        assert_eq!(foreign_key.columns, vec![identifier("parent_id")]);
        assert_eq!(foreign_key.referenced_columns, vec![identifier("id")]);
        assert_eq!(foreign_key.update_action, MySqlForeignKeyAction::Cascade);
        assert_eq!(foreign_key.delete_action, MySqlForeignKeyAction::SetNull);
        assert_eq!(
            mysql_foreign_key_violation_query(foreign_key).unwrap(),
            "SELECT EXISTS (SELECT 1 FROM `app`.`items` AS child WHERE (child.`parent_id` IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM `app`.`items` AS parent WHERE parent.`id` = child.`parent_id`))"
        );
        assert_eq!(
            render_mysql_add_foreign_key(foreign_key).unwrap(),
            "ALTER TABLE `app`.`items` ADD CONSTRAINT `fk_items_parent` FOREIGN KEY (`parent_id`) REFERENCES `app`.`items` (`id`) ON UPDATE CASCADE ON DELETE SET NULL"
        );

        let reviewed = build_plan(&source, &snapshot("mysql://target/app", false)).unwrap();
        let check = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CheckForeignKey)
            .unwrap();
        let add = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::AddForeignKey)
            .unwrap();
        assert_eq!(
            check
                .dependencies
                .iter()
                .filter(
                    |dependency| reviewed.plan.operations.iter().any(|operation| {
                        operation.id == **dependency && operation.kind == OperationKind::CopyTable
                    })
                )
                .count(),
            1
        );
        assert_eq!(add.dependencies.as_slice(), std::slice::from_ref(&check.id));
        let verify = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::VerifyTable)
            .unwrap();
        assert!(verify.dependencies.contains(&add.id));
    }

    #[test]
    fn unsupported_foreign_key_remains_an_explicit_plan_blocker() {
        let mut source = foreign_key_snapshot("mysql://source/app");
        let foreign_key = source.catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::ForeignKey)
            .unwrap();
        foreign_key.attributes.insert(
            "reference".into(),
            serde_json::json!(["app", "PRIMARY", "NONE", "CASCADE", "SET DEFAULT"]),
        );
        let object_id = foreign_key.id.clone();
        source.blockers.push(MySqlCatalogBlocker {
            object_id: object_id.clone(),
            object_kind: "foreign_key".into(),
            reason: "unsupported test foreign-key action".into(),
        });
        source.snapshot_evidence.catalog_fingerprint =
            mysql_catalog_fingerprint(&source.catalog).unwrap();

        assert!(mysql_foreign_keys(&source.catalog).is_err());
        let reviewed = build_plan(&source, &snapshot("mysql://target/app", false)).unwrap();
        assert!(reviewed.plan.unsupported_objects.blocks_execution());
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| object.object_id == object_id && object.object_kind == "foreign_key"));
        assert!(!reviewed.plan.operations.iter().any(|operation| matches!(
            operation.kind,
            OperationKind::CheckForeignKey | OperationKind::AddForeignKey
        )));
    }

    #[test]
    fn foreign_key_ddl_error_classification_keeps_transient_failures_retryable() {
        assert!(mysql_foreign_key_ddl_error_is_deterministic(1452, "23000"));
        assert!(mysql_foreign_key_ddl_error_is_deterministic(1826, "HY000"));
        assert!(mysql_foreign_key_ddl_error_is_deterministic(3780, "HY000"));
        assert!(!mysql_foreign_key_ddl_error_is_deterministic(1021, "HY000"));
        assert!(!mysql_foreign_key_ddl_error_is_deterministic(1205, "HY000"));
        assert!(!mysql_foreign_key_ddl_error_is_deterministic(1213, "40001"));
    }

    #[test]
    fn catalog_ids_are_injective_for_dotted_identifiers() {
        assert_ne!(
            catalog_id("table", "a.b", "c", ""),
            catalog_id("table", "a", "b.c", "")
        );
    }

    #[test]
    fn snapshot_id_is_lifecycle_bound_and_gtid_is_only_input_evidence() {
        let first = mysql_snapshot_id("endpoint", "uuid", "gtid", 7, "lifecycle-1");
        let second = mysql_snapshot_id("endpoint", "uuid", "gtid", 7, "lifecycle-2");
        assert_ne!(first, second);
        assert!(first.starts_with("mysql-session-sha256:"));
    }

    #[test]
    fn identifier_quoting_treats_hostile_input_as_data() {
        assert_eq!(
            quote_identifier_text("a`b; DROP TABLE x"),
            "`a``b; DROP TABLE x`"
        );
    }

    #[test]
    fn writer_rejects_unreviewed_tables_and_column_projections() {
        let table = QualifiedTable {
            namespace: identifier("app"),
            name: identifier("items"),
        };
        let columns = |names: &[&str]| {
            names
                .iter()
                .enumerate()
                .map(|(index, name)| ColumnMeta {
                    name: identifier(name),
                    ordinal: u32::try_from(index + 1).unwrap(),
                    vendor_type: "bigint".into(),
                    nullable: false,
                    collation: None,
                    precision: None,
                    scale: None,
                    timezone_semantics: None,
                })
                .collect()
        };
        let reviewed = BTreeMap::from([(
            table.clone(),
            vec![
                (
                    identifier("id"),
                    MySqlColumnType::Integer {
                        name: "bigint".into(),
                        unsigned: false,
                        display_width: None,
                    },
                ),
                (
                    identifier("payload"),
                    MySqlColumnType::Integer {
                        name: "bigint".into(),
                        unsigned: false,
                        display_width: None,
                    },
                ),
            ],
        )]);
        let exact = RowBatch::new(columns(&["id", "payload"]), 1, 1024);
        validate_mysql_write_contract(&reviewed, &table, &exact).unwrap();

        let reordered = RowBatch::new(columns(&["payload", "id"]), 1, 1024);
        assert!(matches!(
            validate_mysql_write_contract(&reviewed, &table, &reordered),
            Err(ConnectionError::InvalidRequest(_))
        ));
        let unreviewed = QualifiedTable {
            namespace: identifier("app"),
            name: identifier("other"),
        };
        assert!(matches!(
            validate_mysql_write_contract(&reviewed, &unreviewed, &exact),
            Err(ConnectionError::TableNotFound(_))
        ));
    }

    #[test]
    fn writer_requires_exactly_one_affected_row_per_source_row() {
        require_mysql_insert_affected_rows(1).unwrap();
        for affected_rows in [0, 2, u64::MAX] {
            assert!(matches!(
                require_mysql_insert_affected_rows(affected_rows),
                Err(ConnectionError::InvalidRequest(_))
            ));
        }
    }

    #[test]
    fn driver_errors_do_not_expose_server_messages_or_values() {
        let raw = mysql::Error::MySqlError(mysql::MySqlError {
            state: "23000".into(),
            message: "Duplicate entry 'row-secret-needle' for key 'secret-key'".into(),
            code: 1062,
        });
        let safe = MySqlSafeError::from(raw);
        assert_eq!(safe, MySqlSafeError::Server { code: 1062 });
        assert_eq!(safe.to_string(), "server error code 1062");
        let connection = database_error(mysql::Error::MySqlError(mysql::MySqlError {
            state: "23000".into(),
            message: "Duplicate entry 'row-secret-needle' for key 'secret-key'".into(),
            code: 1062,
        }));
        let display = connection.to_string();
        assert!(!display.contains("row-secret-needle"));
        assert!(!display.contains("secret-key"));
        assert!(display.contains("1062"));
    }

    #[test]
    fn writer_rejects_values_that_require_server_type_coercion() {
        let column = ColumnMeta {
            name: identifier("value"),
            ordinal: 1,
            vendor_type: "bigint".into(),
            nullable: false,
            collation: None,
            precision: Some(64),
            scale: Some(0),
            timezone_semantics: None,
        };
        let signed = MySqlColumnType::Integer {
            name: "bigint".into(),
            unsigned: false,
            display_width: None,
        };
        validate_mysql_write_value_type(&DbValue::Signed(7), &column, &signed).unwrap();
        validate_mysql_write_value_type(&DbValue::Unsigned(7), &column, &signed).unwrap();
        assert!(matches!(
            validate_mysql_write_value_type(
                &DbValue::Unsigned(i64::MAX as u128 + 1),
                &column,
                &signed
            ),
            Err(ConnectionError::InvalidRequest(_))
        ));
        let unsigned = MySqlColumnType::Integer {
            name: "bigint".into(),
            unsigned: true,
            display_width: None,
        };
        validate_mysql_write_value_type(&DbValue::Signed(0), &column, &unsigned).unwrap();
        assert!(matches!(
            validate_mysql_write_value_type(&DbValue::Signed(-1), &column, &unsigned),
            Err(ConnectionError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_mysql_write_value_type(&DbValue::Text("7".into()), &column, &signed),
            Err(ConnectionError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_mysql_write_value_type(&DbValue::Null, &column, &signed),
            Err(ConnectionError::InvalidRequest(_))
        ));

        let decimal = MySqlColumnType::Decimal {
            precision: 5,
            scale: 2,
            unsigned: true,
        };
        validate_mysql_write_value_type(
            &DbValue::Decimal {
                coefficient: b"12345".to_vec(),
                scale: 2,
            },
            &column,
            &decimal,
        )
        .unwrap();
        for value in [
            DbValue::Decimal {
                coefficient: b"12345".to_vec(),
                scale: 3,
            },
            DbValue::Decimal {
                coefficient: b"123456".to_vec(),
                scale: 2,
            },
            DbValue::Decimal {
                coefficient: b"-1".to_vec(),
                scale: 2,
            },
        ] {
            assert!(matches!(
                validate_mysql_write_value_type(&value, &column, &decimal),
                Err(ConnectionError::InvalidRequest(_))
            ));
        }

        let time = MySqlColumnType::Temporal {
            name: "time".into(),
            fractional_precision: Some(3),
        };
        validate_mysql_write_value_type(&DbValue::Time { nanos: 1_000_000 }, &column, &time)
            .unwrap();
        assert!(matches!(
            validate_mysql_write_value_type(&DbValue::Time { nanos: 1_001_000 }, &column, &time,),
            Err(ConnectionError::InvalidRequest(_))
        ));

        let timestamp = MySqlColumnType::Temporal {
            name: "timestamp".into(),
            fractional_precision: Some(6),
        };
        validate_mysql_write_value_type(
            &DbValue::Timestamp {
                local: "2026-08-12 12:34:56.123456".into(),
                offset_minutes: None,
                precision: 6,
            },
            &column,
            &timestamp,
        )
        .unwrap();
        assert!(matches!(
            validate_mysql_write_value_type(
                &DbValue::Timestamp {
                    local: "2026-08-12 12:34:56.123000".into(),
                    offset_minutes: None,
                    precision: 3,
                },
                &column,
                &timestamp,
            ),
            Err(ConnectionError::InvalidRequest(_))
        ));
    }

    #[test]
    fn reviewed_mysql_plan_is_typed_and_binds_the_external_freeze_profile() {
        let reviewed = build_plan(
            &snapshot("mysql://source/app", true),
            &snapshot("mysql://target/app", false),
        )
        .unwrap();
        assert_eq!(reviewed.plan.consistency_mode, MYSQL_CONSISTENCY_SNAPSHOT);
        assert!(reviewed.plan.operations.iter().any(|operation| {
            operation.kind == OperationKind::CopyTable
                && operation.parameters.contains_key("resumable_key")
                && operation.parameters.contains_key("mysql_write_policy")
        }));
        assert!(reviewed.plan.operations.iter().any(|operation| {
            operation.kind == OperationKind::CreateTable
                && operation.parameters.contains_key("mysql_table_definition")
        }));
        assert_eq!(
            reviewed.plan.mysql_source_profile,
            Some(MySqlFreezeProfileContract::external_continuous_freeze())
        );
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.code == UnsupportedObjectCode::MySqlCatalogSemantics
                    && object.object_kind == "target_catalog_visibility"
                    && object.required_semantics
            }));
        assert!(reviewed.plan.unsupported_objects.blocks_execution());
    }

    #[test]
    fn visibility_plan_requires_reader_and_administrator_catalog_equality() {
        let source = snapshot("mysql://source/app", true);
        let target = snapshot("mysql://target/app", false);
        let mut source_visibility = visibility_capture(&source, "source_metadata_admin");
        let target_visibility = visibility_capture(&target, "target_metadata_admin");
        source_visibility.authoritative_catalog = catalog(false);
        source_visibility.evidence.authoritative_catalog_fingerprint =
            mysql_catalog_fingerprint(&source_visibility.authoritative_catalog).unwrap();

        let error =
            build_plan_with_visibility(&source, &target, &source_visibility, &target_visibility)
                .unwrap_err();
        assert!(
            matches!(error, MySqlPlanError::InvalidCatalog(message) if message.contains("reader snapshot"))
        );
    }

    #[test]
    fn visibility_plan_binds_both_endpoints_and_blocks_business_grants() {
        let source = snapshot("mysql://source/app", true);
        let target = snapshot("mysql://target/app", false);
        let source_visibility = visibility_capture(&source, "source_metadata_admin");
        let target_visibility = visibility_capture(&target, "target_metadata_admin");
        let reviewed =
            build_plan_with_visibility(&source, &target, &source_visibility, &target_visibility)
                .unwrap();
        assert_eq!(
            reviewed.plan.mysql_metadata_visibility,
            Some(source_visibility.evidence.clone())
        );
        assert_eq!(
            reviewed.plan.mysql_target_metadata_visibility,
            Some(target_visibility.evidence)
        );
        assert!(!reviewed.plan.unsupported_objects.blocks_execution());

        let mut business_visibility = source_visibility;
        let business = MySqlAccountIdentity {
            user: "application".into(),
            host: "%".into(),
        };
        business_visibility
            .evidence
            .grant_inventory
            .accounts
            .push(business.clone());
        business_visibility.evidence.grant_inventory.accounts.sort();
        business_visibility
            .evidence
            .grant_inventory
            .records
            .push(MySqlGrantRecord::Database {
                account: business,
                database: "app".into(),
                privilege: "SELECT".into(),
            });
        business_visibility.evidence.grant_inventory.records.sort();
        business_visibility.evidence.grant_inventory_digest = business_visibility
            .evidence
            .grant_inventory
            .canonical_hash()
            .unwrap();
        let reviewed = build_plan_with_visibility(
            &source,
            &target,
            &business_visibility,
            &visibility_capture(&target, "target_metadata_admin"),
        )
        .unwrap();
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| object.object_kind == "privilege" && object.required_semantics));

        let original = reviewed.plan;
        let mut stripped = original.clone();
        stripped
            .unsupported_objects
            .objects
            .retain(|object| object.object_kind != "privilege");
        assert!(matches!(
            ReviewedPlan::new(stripped),
            Err(PlanError::InvalidMySqlMetadataVisibility)
        ));

        let mut reclassified = original.clone();
        reclassified
            .unsupported_objects
            .objects
            .iter_mut()
            .find(|object| object.object_kind == "privilege")
            .unwrap()
            .code = UnsupportedObjectCode::MySqlFreezeEvidence;
        assert!(matches!(
            ReviewedPlan::new(reclassified),
            Err(PlanError::InvalidMySqlMetadataVisibility)
        ));

        let mut added = original;
        let mut extra = added
            .unsupported_objects
            .objects
            .iter()
            .find(|object| object.object_kind == "privilege")
            .unwrap()
            .clone();
        extra.object_id = format!("mysql-grant:{}", "0".repeat(64));
        added.unsupported_objects.objects.push(extra);
        assert!(matches!(
            ReviewedPlan::new(added),
            Err(PlanError::InvalidMySqlMetadataVisibility)
        ));
    }

    #[test]
    fn server_administrators_are_excluded_only_when_explicitly_declared() {
        let reader = MySqlAccountIdentity {
            user: "reader".into(),
            host: "%".into(),
        };
        let metadata = MySqlAccountIdentity {
            user: "metadata".into(),
            host: "%".into(),
        };
        let legacy_application = MySqlAccountIdentity {
            user: "legacy_application".into(),
            host: "%".into(),
        };
        let mut inventory = MySqlGrantInventory {
            partial_revokes_enabled: false,
            grant_table_columns: Vec::new(),
            accounts: vec![reader.clone(), metadata.clone(), legacy_application.clone()],
            records: vec![
                MySqlGrantRecord::StaticGlobal {
                    account: legacy_application.clone(),
                    privilege: "SUPER".into(),
                },
                MySqlGrantRecord::Database {
                    account: legacy_application.clone(),
                    database: "app".into(),
                    privilege: "SELECT".into(),
                },
            ],
            unknown_privilege_classes: Vec::new(),
        };
        inventory.accounts.sort();
        inventory.records.sort();

        let implicit =
            derive_mysql_operational_exclusions(&inventory, &reader, &metadata, None, &[], &[])
                .unwrap();
        assert!(!implicit
            .iter()
            .any(|exclusion| exclusion.account == legacy_application));

        let explicit = derive_mysql_operational_exclusions(
            &inventory,
            &reader,
            &metadata,
            None,
            &[],
            std::slice::from_ref(&legacy_application),
        )
        .unwrap();
        assert!(explicit.iter().any(|exclusion| {
            exclusion.account == legacy_application
                && exclusion.purpose == MySqlOperationalAccountPurpose::ServerAdministrator
        }));
        assert!(derive_mysql_operational_exclusions(
            &inventory,
            &reader,
            &metadata,
            None,
            &[],
            std::slice::from_ref(&metadata),
        )
        .is_err());
    }

    #[test]
    fn grant_table_schema_rejects_unknown_privilege_columns() {
        let mut columns = ["Host", "User", "User_attributes"]
            .into_iter()
            .chain(MYSQL_USER_STATIC_PRIVILEGE_COLUMNS.iter().copied())
            .map(|column| MySqlGrantTableColumn {
                table: "user".into(),
                column: column.into(),
                data_type: "char".into(),
            })
            .chain(
                ["Host", "Db", "User"]
                    .into_iter()
                    .chain(MYSQL_DATABASE_PRIVILEGE_COLUMNS.iter().copied())
                    .map(|column| MySqlGrantTableColumn {
                        table: "db".into(),
                        column: column.into(),
                        data_type: "char".into(),
                    }),
            )
            .chain(
                MYSQL_GRANT_TABLE_COLUMNS
                    .iter()
                    .flat_map(|(table, expected)| {
                        expected.iter().map(|column| MySqlGrantTableColumn {
                            table: (*table).into(),
                            column: (*column).into(),
                            data_type: "char".into(),
                        })
                    }),
            )
            .collect::<Vec<_>>();
        assert!(mysql_unknown_grant_schema(&columns).is_empty());
        columns.push(MySqlGrantTableColumn {
            table: "user".into(),
            column: "Future_priv".into(),
            data_type: "enum".into(),
        });
        assert_eq!(
            mysql_unknown_grant_schema(&columns),
            vec!["unknown:mysql.user.Future_priv"]
        );
    }

    #[test]
    fn active_administrator_roles_are_explicit_operational_exclusions() {
        let reader = MySqlAccountIdentity {
            user: "reader".into(),
            host: "%".into(),
        };
        let administrator = MySqlAccountIdentity {
            user: "metadata_admin".into(),
            host: "%".into(),
        };
        let mandatory_role = MySqlAccountIdentity {
            user: "mandatory_visibility".into(),
            host: "%".into(),
        };
        let inventory = MySqlGrantInventory {
            partial_revokes_enabled: false,
            grant_table_columns: Vec::new(),
            accounts: vec![
                administrator.clone(),
                mandatory_role.clone(),
                reader.clone(),
            ],
            records: Vec::new(),
            unknown_privilege_classes: Vec::new(),
        };
        let exclusions = derive_mysql_operational_exclusions(
            &inventory,
            &reader,
            &administrator,
            None,
            std::slice::from_ref(&mandatory_role),
            &[],
        )
        .unwrap();
        assert!(exclusions.iter().any(|exclusion| {
            exclusion.purpose == MySqlOperationalAccountPurpose::OperationalRole
                && exclusion.account == mandatory_role
        }));
    }

    #[test]
    fn database_grant_patterns_are_matched_without_cross_database_leakage() {
        assert!(mysql_grant_database_pattern_matches(
            r"migration\_source",
            "migration_source"
        ));
        assert!(!mysql_grant_database_pattern_matches(
            r"migration\_target",
            "migration_source"
        ));
        assert!(mysql_grant_database_pattern_matches("tenant%", "tenant_eu"));
        assert!(mysql_grant_database_pattern_matches("app_", "app1"));
        assert!(!mysql_grant_database_pattern_matches("app_", "app12"));
        assert!(mysql_grant_database_pattern_matches(
            r"literal\%db",
            "literal%db"
        ));
    }

    #[test]
    fn typed_mysql_column_types_round_trip_without_raw_sql() {
        for source in [
            "bigint unsigned",
            "decimal(38,9)",
            "double",
            "bit(7)",
            "datetime(6)",
            "year",
            "varchar(255)",
            "varbinary(32)",
            "longtext",
            "mediumblob",
            "json",
        ] {
            let parsed = parse_mysql_column_type(source).unwrap();
            let rendered = render_mysql_column_type(&parsed).unwrap();
            assert_eq!(parse_mysql_column_type(&rendered).unwrap(), parsed);
        }
        assert!(parse_mysql_column_type("enum('a','b')").is_err());
        assert!(parse_mysql_column_type("int zerofill").is_err());
    }

    #[test]
    fn typed_create_table_quotes_every_identifier() {
        let definition = MySqlTableDefinition {
            table: QualifiedTable {
                namespace: identifier("a`b"),
                name: identifier("t`x"),
            },
            engine: "InnoDB".into(),
            character_set: "utf8mb4".into(),
            collation: "utf8mb4_0900_bin".into(),
            columns: vec![MySqlColumnDefinition {
                name: identifier("c`1"),
                ordinal: 1,
                data_type: MySqlColumnType::Integer {
                    name: "bigint".into(),
                    unsigned: false,
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
                columns: vec![identifier("c`1")],
            }],
        };
        let ddl = render_mysql_create_table(&definition).unwrap();
        assert!(ddl.contains("CREATE TABLE `a``b`.`t``x`"));
        assert!(ddl.contains("`c``1` bigint NOT NULL"));
        assert!(!ddl.contains(';'));
    }

    #[test]
    fn mysql_decimal_write_encoding_is_scale_exact() {
        assert_eq!(render_mysql_decimal(b"12345", 2).unwrap(), b"123.45");
        assert_eq!(render_mysql_decimal(b"-7", 3).unwrap(), b"-0.007");
        assert_eq!(render_mysql_decimal(b"0", 4).unwrap(), b"0.0000");
        assert!(render_mysql_decimal(b"1.2", 1).is_err());
        assert!(render_mysql_decimal(b"1", -1).is_err());
    }

    #[test]
    fn mysql_json_values_use_the_shared_canonical_contract() {
        let column = ColumnMeta {
            name: identifier("payload"),
            ordinal: 1,
            vendor_type: "json".into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        };
        assert_eq!(
            mysql_value(
                Value::Bytes(br#"{"b":12,"a":9007199254740993}"#.to_vec()),
                &column,
            )
            .unwrap(),
            DbValue::Json(br#"{"b":12,"a":9007199254740993}"#.to_vec())
        );
        assert!(matches!(
            mysql_value(Value::Bytes(br#"{"a":1,"a":2}"#.to_vec()), &column),
            Err(ConnectionError::Database(_))
        ));
        assert_eq!(
            mysql_write_value(
                &DbValue::Json(br#"{"n":12,"wide":9007199254740993}"#.to_vec()),
                &column,
                &MySqlColumnType::Json,
            )
            .unwrap(),
            Value::Bytes(br#"{"n":12,"wide":9007199254740993}"#.to_vec())
        );
        assert!(matches!(
            mysql_write_value(
                &DbValue::Json(br#"{"a":1,"a":2}"#.to_vec()),
                &column,
                &MySqlColumnType::Json,
            ),
            Err(ConnectionError::InvalidRequest(_))
        ));
    }

    #[test]
    fn mysql_bit_values_bind_as_exact_unsigned_integers() {
        let column = ColumnMeta {
            name: identifier("flags"),
            ordinal: 1,
            vendor_type: "bit".into(),
            nullable: false,
            collation: None,
            precision: Some(9),
            scale: None,
            timezone_semantics: None,
        };
        assert_eq!(
            mysql_write_value(
                &DbValue::Bytes(vec![1, 255]),
                &column,
                &MySqlColumnType::Bit { length: 9 },
            )
            .unwrap(),
            Value::UInt(511)
        );
        assert!(matches!(
            mysql_write_value(
                &DbValue::Bytes(vec![2, 0]),
                &column,
                &MySqlColumnType::Bit { length: 9 },
            ),
            Err(ConnectionError::UnsupportedKeyValue)
        ));
    }

    #[test]
    fn temporal_column_meta_uses_datetime_precision() {
        let object = CatalogObject {
            id: "column:app:items:created_at".into(),
            kind: CatalogObjectKind::Column,
            name: identifier("created_at"),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("ordinal".into(), serde_json::json!(1)),
                ("data_type".into(), serde_json::json!("timestamp")),
                ("nullable".into(), serde_json::json!(false)),
                ("collation".into(), serde_json::Value::Null),
                ("numeric_precision".into(), serde_json::Value::Null),
                ("numeric_scale".into(), serde_json::Value::Null),
                ("datetime_precision".into(), serde_json::json!(6)),
            ]),
        };
        let meta = column_meta(&object).unwrap();
        assert_eq!(meta.precision, Some(6));
        assert_eq!(meta.scale, None);
        assert_eq!(
            meta.timezone_semantics.as_deref(),
            Some("mysql_session_time_zone")
        );
    }

    #[test]
    fn bit_column_meta_preserves_the_reviewed_width() {
        let object = CatalogObject {
            id: "column:app:items:flags".into(),
            kind: CatalogObjectKind::Column,
            name: identifier("flags"),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("ordinal".into(), serde_json::json!(1)),
                ("data_type".into(), serde_json::json!("bit")),
                ("nullable".into(), serde_json::json!(false)),
                ("collation".into(), serde_json::Value::Null),
                ("numeric_precision".into(), serde_json::json!(9)),
                ("numeric_scale".into(), serde_json::Value::Null),
                ("datetime_precision".into(), serde_json::Value::Null),
            ]),
        };

        let meta = column_meta(&object).unwrap();
        assert_eq!(meta.precision, Some(9));
        assert_eq!(meta.scale, None);
    }

    #[test]
    fn integer_column_meta_does_not_treat_display_precision_as_value_metadata() {
        let object = CatalogObject {
            id: "column:app:items:id".into(),
            kind: CatalogObjectKind::Column,
            name: identifier("id"),
            definition: Vec::new(),
            attributes: BTreeMap::from([
                ("ordinal".into(), serde_json::json!(1)),
                ("data_type".into(), serde_json::json!("bigint")),
                ("nullable".into(), serde_json::json!(false)),
                ("collation".into(), serde_json::Value::Null),
                ("numeric_precision".into(), serde_json::json!(19)),
                ("numeric_scale".into(), serde_json::json!(0)),
                ("datetime_precision".into(), serde_json::Value::Null),
            ]),
        };

        let meta = column_meta(&object).unwrap();
        assert_eq!(meta.precision, None);
        assert_eq!(meta.scale, None);
    }

    #[test]
    fn mysql_yes_no_grant_fields_are_decoded_exactly() {
        assert!(parse_mysql_yes_no("field", "Y").unwrap());
        assert!(!parse_mysql_yes_no("field", "N").unwrap());
        assert!(parse_mysql_yes_no("field", "1").is_err());
        assert!(parse_mysql_yes_no("field", "").is_err());
        assert!(parse_mysql_zero_one("field", 1).unwrap());
        assert!(!parse_mysql_zero_one("field", 0).unwrap());
        assert!(parse_mysql_zero_one("field", 2).is_err());
    }

    #[test]
    fn authorization_renderer_is_typed_ordered_and_quotes_hostile_accounts() {
        let account = MySqlAccountIdentity {
            user: "reader'; DROP USER root; --".into(),
            host: "host\\name".into(),
        };
        let role = MySqlAccountIdentity {
            user: "reporting_role".into(),
            host: "%".into(),
        };
        let mut records = vec![
            MySqlGrantRecord::Database {
                account: account.clone(),
                database: "target-db".into(),
                privilege: "SELECT".into(),
            },
            MySqlGrantRecord::Database {
                account: account.clone(),
                database: "target-db".into(),
                privilege: "GRANT".into(),
            },
            MySqlGrantRecord::Table {
                account: account.clone(),
                database: "target-db".into(),
                table: "line items".into(),
                privilege: "COLUMN::UPDATE".into(),
                grantor: "admin@%".into(),
            },
            MySqlGrantRecord::Column {
                account: account.clone(),
                database: "target-db".into(),
                table: "line items".into(),
                column: "display name".into(),
                privilege: "UPDATE".into(),
            },
            MySqlGrantRecord::RoleEdge {
                role: role.clone(),
                grantee: account.clone(),
                admin_option: true,
            },
            MySqlGrantRecord::DefaultRole {
                account: account.clone(),
                role: role.clone(),
            },
            MySqlGrantRecord::PartialRevoke {
                account: account.clone(),
                database: "target-db".into(),
                privileges: vec!["SELECT".into()],
            },
        ];
        records.sort();
        let mut accounts = vec![account, role];
        accounts.sort();
        let inventory = MySqlGrantInventory {
            partial_revokes_enabled: true,
            grant_table_columns: Vec::new(),
            accounts,
            records,
            unknown_privilege_classes: Vec::new(),
        };
        let statements = render_mysql_authorization_statements(&inventory).unwrap();
        assert_eq!(statements.len(), 5);
        assert!(statements[0].starts_with(
            "GRANT SELECT ON `target-db`.* TO 'reader\\'; DROP USER root; --'@'host\\\\name' WITH GRANT OPTION"
        ));
        assert!(statements[1].contains(
            "GRANT UPDATE (`display name`) ON `target-db`.`line items` TO 'reader\\'; DROP USER root; --'@'host\\\\name'"
        ));
        assert!(statements[2].starts_with("GRANT 'reporting_role'@'%' TO "));
        assert!(statements[3].starts_with("SET DEFAULT ROLE 'reporting_role'@'%' TO "));
        assert!(statements[4].starts_with("REVOKE SELECT ON `target-db`.* FROM "));
    }

    #[test]
    fn tampered_snapshot_evidence_is_rejected_before_plan_construction() {
        let mut source = snapshot("mysql://source/app", true);
        source.snapshot_evidence.information_schema_stats_expiry = 86_400;
        let error = build_plan(&source, &snapshot("mysql://target/app", false)).unwrap_err();
        assert!(
            matches!(error, MySqlPlanError::InvalidCatalog(message) if message.contains("snapshot evidence"))
        );
    }

    #[test]
    fn incomplete_catalog_visibility_cannot_be_unblocked_by_a_caller() {
        let mut source = snapshot("mysql://source/app", true);
        source.blockers.clear();
        let error = build_plan(&source, &snapshot("mysql://target/app", false)).unwrap_err();
        assert!(
            matches!(error, MySqlPlanError::InvalidCatalog(message) if message.contains("catalog_visibility"))
        );
    }

    #[test]
    fn unsupported_column_type_requires_an_explicit_blocker() {
        let mut source = snapshot("mysql://source/app", true);
        let column = source.catalog.namespaces[0]
            .objects
            .iter_mut()
            .find(|object| object.kind == CatalogObjectKind::Column)
            .unwrap();
        column
            .attributes
            .insert("data_type".into(), serde_json::json!("geometry"));
        let fingerprint = mysql_catalog_fingerprint(&source.catalog).unwrap();
        source.snapshot_evidence.catalog_fingerprint = fingerprint;
        let error = build_plan(&source, &snapshot("mysql://target/app", false)).unwrap_err();
        assert!(
            matches!(error, MySqlPlanError::InvalidCatalog(message) if message.contains("column_ddl"))
        );
    }
}
