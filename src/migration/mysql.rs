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

use super::artifact::{write_json_new, ArtifactError};
use super::canonical::CANONICAL_ENCODING_VERSION;
use super::connection::{
    CancellationToken, Capability, CapabilitySet, ConnectionError, ConnectionResult,
    ControlSession, KeysetPage, ReadOnlyEvidence, ReadSession, SnapshotToken,
    SourceConnectionFactory, TargetConnectionFactory, VerificationSession, WriteSession,
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
    MySqlAccountIdentity, MySqlGrantInventory, MySqlGrantRecord, MySqlGrantTableColumn,
    MySqlMetadataVisibilityEvidence, MySqlOperationalAccountExclusion,
    MySqlOperationalAccountPurpose, MySqlProxyTarget, MySqlVisibilityError,
    MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
};
use super::plan::{
    AssessmentStatus, MigrationPlan, MySqlSnapshotEvidence, OperationKind, PlanError,
    PlanOperation, PlanPurpose, ReviewedPlan, UnsupportedObject, UnsupportedObjectCode,
    UnsupportedObjectReport, MYSQL_SESSION_CHARACTER_SET, MYSQL_SESSION_COLLATION,
    MYSQL_STRICT_SQL_MODE, PLAN_SCHEMA_VERSION,
};

pub const MYSQL_CATALOG_FORMAT_VERSION: u32 = 1;
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
    Database(#[from] mysql::Error),
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
    build_plan_from_execution_source(source, target, None, None)
}

/// Build a reviewed MySQL plan with separately authenticated authoritative
/// source catalog and grant evidence.
pub fn build_plan_with_visibility(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: &MySqlMetadataVisibilityCapture,
    target_visibility: &MySqlMetadataVisibilityCapture,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_catalog_snapshot(source)?;
    validate_catalog_snapshot(target)?;
    source_visibility.evidence.validate()?;
    target_visibility.evidence.validate()?;
    if !mysql_catalog_visibility_is_complete(source, source_visibility)?
        || !mysql_catalog_visibility_is_complete(target, target_visibility)?
        || source_visibility.evidence.endpoint_identity != source.endpoint_identity
        || source_visibility.evidence.database_identity != source.database_identity
        || source_visibility.evidence.server_uuid != source.snapshot_evidence.server_uuid
        || source_visibility.evidence.catalog_reader_tls_binding != source.tls_binding
        || source_visibility.evidence.catalog_reader_fingerprint
            != source.snapshot_evidence.catalog_fingerprint
        || source_visibility.evidence.authoritative_catalog_fingerprint
            != mysql_catalog_fingerprint(&source_visibility.authoritative_catalog)
                .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?
        || target_visibility.evidence.endpoint_identity != target.endpoint_identity
        || target_visibility.evidence.database_identity != target.database_identity
        || target_visibility.evidence.server_uuid != target.snapshot_evidence.server_uuid
        || target_visibility.evidence.catalog_reader_tls_binding != target.tls_binding
        || target_visibility.evidence.catalog_reader_fingerprint
            != target.snapshot_evidence.catalog_fingerprint
        || target_visibility.evidence.authoritative_catalog_fingerprint
            != mysql_catalog_fingerprint(&target_visibility.authoritative_catalog)
                .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?
    {
        return Err(MySqlPlanError::InvalidCatalog(
            "MySQL metadata-visibility capture differs from its reader snapshot".into(),
        ));
    }
    let mut execution_source = source.clone();
    execution_source.catalog = source_visibility.authoritative_catalog.clone();
    execution_source.blockers = source_visibility.authoritative_blockers.clone();
    for record in source_visibility.evidence.non_operational_records() {
        execution_source.blockers.push(MySqlCatalogBlocker {
            object_id: mysql_grant_record_id(record)?,
            object_kind: "privilege".into(),
            reason: "MySQL business authorization state is inventoried but target role mapping and exact restoration are not yet implemented".into(),
        });
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
    )
}

fn build_plan_from_execution_source(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    source_visibility: Option<MySqlMetadataVisibilityEvidence>,
    target_visibility: Option<MySqlMetadataVisibilityEvidence>,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_supported_server_version(&source.server_version)?;
    validate_supported_server_version(&target.server_version)?;
    let auto_increment = mysql_auto_increment_states(&source.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let auto_increment_by_table = auto_increment
        .iter()
        .map(|state| (state.table.clone(), state))
        .collect::<BTreeMap<_, _>>();
    let mut operations = Vec::new();
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
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table.clone()),
            verify_dependencies,
            BTreeMap::new(),
        )?;
        operations.extend([create, copy]);
        if let Some(restore) = restore {
            operations.push(restore);
        }
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
        consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy: "mysql_same_dialect_exact".into(),
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: Some(MySqlFreezeProfileContract::external_continuous_freeze()),
        mysql_snapshot_evidence: Some(source.snapshot_evidence.clone()),
        mysql_target_snapshot_evidence: Some(target.snapshot_evidence.clone()),
        mysql_metadata_visibility: source_visibility,
        mysql_target_metadata_visibility: target_visibility,
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

fn validate_catalog_snapshot(snapshot: &MySqlCatalogSnapshot) -> Result<(), MySqlPlanError> {
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

fn required_blocker_keys(
    catalog: &VendorCatalog,
) -> Result<Vec<(String, &'static str)>, MySqlPlanError> {
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
                required.push((object.id.clone(), "foreign_key"));
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
        let mut conn = self.config.connect().map_err(database_error)?;
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
        let mut control = self.config.connect().map_err(database_error)?;
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
    target_evidence: MySqlSnapshotEvidence,
    registry: Arc<SessionRegistry>,
    cancellation: CancellationToken,
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
        if target_evidence.database_identity != config.database
            || target_evidence.lower_case_table_names > 2
            || target_evidence.session_sql_mode != MYSQL_STRICT_SQL_MODE
            || target_evidence.character_set_client != MYSQL_SESSION_CHARACTER_SET
            || target_evidence.character_set_connection != MYSQL_SESSION_CHARACTER_SET
            || target_evidence.character_set_results != MYSQL_SESSION_CHARACTER_SET
            || target_evidence.collation_connection != MYSQL_SESSION_COLLATION
        {
            return Err(MySqlPlanError::InvalidCatalog(
                "MySQL target evidence does not bind the required session contract".into(),
            ));
        }
        let target_namespace = Identifier::new(config.database.clone())?;
        let reviewed_tables = mysql_table_definitions(&reviewed_catalog)?
            .into_iter()
            .map(|mut definition| {
                definition.table.namespace = target_namespace.clone();
                (definition.table.clone(), definition)
            })
            .collect();
        Ok(Self {
            config,
            reviewed_catalog,
            reviewed_tables,
            target_evidence,
            registry: Arc::default(),
            cancellation,
        })
    }

    pub(crate) fn endpoint_config(&self) -> &MySqlEndpointConfig {
        &self.config
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
        let mut conn = self.config.connect().map_err(database_error)?;
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
                        .map(|column| column.name.clone())
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
        let target_namespace = Identifier::new(self.config.database.clone())
            .map_err(|error| ConnectionError::InvalidRequest(error.to_string()))?;
        let table_contracts = table_contracts(&self.reviewed_catalog)?
            .into_iter()
            .map(|(mut table, contract)| {
                table.namespace = target_namespace.clone();
                (table, contract)
            })
            .collect();
        Ok(Box::new(MySqlVerifier {
            conn,
            registration: Some(registration),
            cancellation,
            max_batch_rows: self.config.max_batch_rows,
            max_batch_bytes: self.config.max_batch_bytes,
            table_contracts,
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
    reviewed_projections: BTreeMap<QualifiedTable, Vec<Identifier>>,
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
        let columns = batch
            .columns()
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n("?", batch.columns().len())
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
                    .map(mysql_write_value)
                    .collect::<ConnectionResult<Vec<_>>>()
            })
            .collect::<ConnectionResult<Vec<_>>>()?;
        self.conn
            .exec_batch(statement, parameters)
            .map_err(database_error)?;
        self.cancellation.check()?;
        Ok(())
    }

    fn commit(&mut self) -> ConnectionResult<()> {
        self.cancellation.check()?;
        if !self.transaction_open {
            return Err(ConnectionError::TransactionRequired);
        }
        self.transaction_open = false;
        self.conn
            .query_drop("COMMIT")
            .map_err(|error| ConnectionError::CommitOutcomeUnknown(error.to_string()))
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
    reviewed_projections: &BTreeMap<QualifiedTable, Vec<Identifier>>,
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
        .eq(expected.iter())
    {
        return Err(ConnectionError::InvalidRequest(
            "MySQL write projection differs from the reviewed table contract".into(),
        ));
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
        let batch = mysql_select_page(
            &mut self.conn,
            &self.table_contracts,
            self.max_batch_rows,
            self.max_batch_bytes,
            request,
        )?;
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
        let batch = mysql_select_page(
            &mut self.conn,
            &self.table_contracts,
            self.max_batch_rows,
            self.max_batch_bytes,
            request,
        )?;
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

fn mysql_write_value(value: &DbValue) -> ConnectionResult<Value> {
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
        DbValue::Bytes(value) | DbValue::Json(value) => Ok(Value::Bytes(value.clone())),
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
            "json" => Ok(DbValue::Json(value)),
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
                let precision = column.scale.unwrap_or(0).clamp(0, 6) as u8;
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

fn column_meta(object: &CatalogObject) -> Result<ColumnMeta, MySqlPlanError> {
    let ordinal = required_u64(object, "ordinal")?;
    Ok(ColumnMeta {
        name: object.name.clone(),
        ordinal: u32::try_from(ordinal)
            .map_err(|_| MySqlPlanError::InvalidCatalog("column ordinal is too large".into()))?,
        vendor_type: required_text(object, "data_type")?.into(),
        nullable: required_bool(object, "nullable")?,
        collation: optional_text(object, "collation").map(str::to_owned),
        precision: optional_u64(object, "numeric_precision")
            .map(u32::try_from)
            .transpose()
            .map_err(|_| MySqlPlanError::InvalidCatalog("column precision is too large".into()))?,
        scale: optional_i64(object, "numeric_scale")
            .map(i32::try_from)
            .transpose()
            .map_err(|_| MySqlPlanError::InvalidCatalog("column scale is too large".into()))?,
        timezone_semantics: match required_text(object, "data_type")? {
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
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, GENERATION_EXPRESSION, NUMERIC_PRECISION, NUMERIC_SCALE FROM information_schema.COLUMNS WHERE BINARY TABLE_SCHEMA = BINARY ? ORDER BY TABLE_NAME, ORDINAL_POSITION",
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
    let references: Vec<(String, String, String, String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE BINARY CONSTRAINT_SCHEMA = BINARY ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
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
        BTreeMap::<
            (String, String),
            Vec<(u64, String, Option<String>, Option<String>, Option<String>)>,
        >::new(),
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
            |(table, constraint, unique_schema, unique_name, update_rule, delete_rule)| {
                (
                    (table, constraint),
                    (unique_schema, unique_name, update_rule, delete_rule),
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
        blockers.push(MySqlCatalogBlocker {
            object_id: id.clone(),
            object_kind: match kind {
                CatalogObjectKind::ForeignKey => "foreign_key",
                CatalogObjectKind::CheckConstraint => "check_constraint",
                _ => "constraint",
            }
            .into(),
            reason: "MySQL constraint creation, anti-join checking, and implicit-commit recovery are not yet modeled exactly"
                .into(),
        });
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

fn database_error(error: impl std::fmt::Display) -> ConnectionError {
    ConnectionError::Database(error.to_string())
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
        let reviewed =
            BTreeMap::from([(table.clone(), vec![identifier("id"), identifier("payload")])]);
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
