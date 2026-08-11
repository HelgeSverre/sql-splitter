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
use std::time::Duration;

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
    SourceConnectionFactory,
};
use super::model::{
    CatalogDependency, CatalogNamespace, CatalogObject, CatalogObjectKind, ColumnMeta, DbValue,
    Identifier, QualifiedTable, RowBatch, RowBatchError, VendorCatalog,
};
use super::plan::{
    AssessmentStatus, MigrationPlan, MySqlSnapshotEvidence, OperationKind, PlanError,
    PlanOperation, PlanPurpose, ReviewedPlan, UnsupportedObject, UnsupportedObjectCode,
    UnsupportedObjectReport, PLAN_SCHEMA_VERSION,
};

pub const MYSQL_CATALOG_FORMAT_VERSION: u32 = 1;
pub const MYSQL_CONSISTENCY_SNAPSHOT: &str = "mysql-repeatable-read-consistent-snapshot";
const DEFAULT_PORT: u16 = 3306;
const DEFAULT_TIMEOUT_SECONDS: u64 = 10;
const DEFAULT_BATCH_ROWS: usize = 10_000;
const DEFAULT_BATCH_BYTES: usize = 64 * 1024 * 1024;
static SNAPSHOT_LIFECYCLE_COUNTER: AtomicU64 = AtomicU64::new(1);

type MySqlIdentityRow = (
    String,
    String,
    u16,
    String,
    String,
    String,
    u8,
    String,
    u64,
    String,
    u32,
);
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

/// Build a reviewed MySQL plan that remains blocked until freeze and
/// AUTO_INCREMENT consistency evidence are admitted at execution design time.
pub fn build_plan(
    source: &MySqlCatalogSnapshot,
    target: &MySqlCatalogSnapshot,
) -> Result<ReviewedPlan, MySqlPlanError> {
    validate_catalog_snapshot(source)?;
    validate_catalog_snapshot(target)?;
    validate_supported_server_version(&source.server_version)?;
    validate_supported_server_version(&target.server_version)?;
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
        let create = PlanOperation::new(
            OperationKind::CreateTable,
            Some(table.clone()),
            Vec::new(),
            BTreeMap::from([
                ("mysql_table".into(), serde_json::to_value(object)?),
                ("mysql_columns".into(), serde_json::to_value(columns)?),
                ("mysql_indexes".into(), serde_json::to_value(indexes)?),
            ]),
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
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            vec![create.id.clone()],
            copy_parameters,
        )?;
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(table.clone()),
            vec![copy.id.clone()],
            BTreeMap::new(),
        )?;
        operations.extend([create, copy, verify]);
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
    unsupported.push(UnsupportedObject {
        code: UnsupportedObjectCode::MySqlCatalogSemantics,
        object_id: "mysql-target-catalog-visibility".into(),
        object_kind: "target_catalog_visibility".into(),
        reason: "MySQL target metadata visibility is account-dependent; exhaustive target catalog and privilege evidence is not yet modeled"
            .into(),
        required_semantics: true,
    });
    unsupported.push(UnsupportedObject {
        code: UnsupportedObjectCode::MySqlFreezeEvidence,
        object_id: "mysql-continuous-dml-ddl-freeze".into(),
        object_kind: "consistency_precondition".into(),
        reason: "MySQL execution requires a continuously proven DML and DDL freeze; a consistent snapshot alone does not protect catalog reads or AUTO_INCREMENT state".into(),
        required_semantics: true,
    });
    let auto_increment = mysql_auto_increment_states(&source.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    if !auto_increment.is_empty() {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::MySqlAutoIncrementConsistency,
            object_id: "mysql-auto-increment-consistency".into(),
            object_kind: "auto_increment_consistency".into(),
            reason: format!(
                "{} MySQL AUTO_INCREMENT counters require a proven freeze or exact equality re-read and post-load restoration",
                auto_increment.len()
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
    if source.endpoint_identity == target.endpoint_identity {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::SameEndpoint,
            object_id: "source-target-endpoint-collision".into(),
            object_kind: "endpoint_precondition".into(),
            reason: "source and target resolve to the same MySQL endpoint identity".into(),
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
        mysql_snapshot_evidence: Some(source.snapshot_evidence.clone()),
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

fn validate_catalog_snapshot(snapshot: &MySqlCatalogSnapshot) -> Result<(), MySqlPlanError> {
    let fingerprint = mysql_catalog_fingerprint(&snapshot.catalog)
        .map_err(|error| MySqlPlanError::InvalidCatalog(error.to_string()))?;
    let evidence = &snapshot.snapshot_evidence;
    if snapshot.catalog.database.as_str() != snapshot.database_identity
        || snapshot.catalog.server_version != snapshot.server_version
        || evidence.endpoint_identity != snapshot.endpoint_identity
        || evidence.database_identity != snapshot.database_identity
        || evidence.server_version != snapshot.server_version
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
                if object
                    .attributes
                    .get("resumable_key")
                    .is_none_or(serde_json::Value::is_null)
                {
                    required.push((object.id.clone(), "resumable_key"));
                }
            }
            CatalogObjectKind::Column => {
                if !supported_value_type(required_text(object, "data_type")?) {
                    required.push((object.id.clone(), "column_type"));
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

    fn controlled_connect(&self) -> ConnectionResult<(Conn, SessionRegistration)> {
        self.cancellation.check()?;
        let conn = self.config.connect().map_err(database_error)?;
        let registration = self.registry.register(conn.connection_id())?;
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
        conn.query_drop("SET SESSION time_zone = '+00:00'")
            .map_err(database_error)?;
        conn.query_drop("SET SESSION information_schema_stats_expiry = 0")
            .map_err(database_error)?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .map_err(database_error)?;
        let identity: Option<MySqlIdentityRow> = conn
            .query_first(
                "SELECT DATABASE(), @@hostname, @@port, VERSION(), @@server_uuid, @@transaction_isolation, @@transaction_read_only, @@session.time_zone, @@session.information_schema_stats_expiry, @@global.gtid_executed, CONNECTION_ID()",
            )
            .map_err(database_error)?;
        let (
            database,
            hostname,
            port,
            server_version,
            server_uuid,
            transaction_isolation,
            transaction_read_only,
            session_time_zone,
            information_schema_stats_expiry,
            gtid_executed_observation,
            connection_id,
        ) = identity.ok_or_else(|| {
            ConnectionError::Database("MySQL identity query returned no row".into())
        })?;
        if database != self.config.database
            || !transaction_isolation.eq_ignore_ascii_case("REPEATABLE-READ")
            || transaction_read_only != 1
            || session_time_zone != "+00:00"
            || information_schema_stats_expiry != 0
            || connection_id != registration.connection_id
        {
            return Err(ConnectionError::SnapshotMismatch);
        }
        let endpoint_identity =
            format!("mysql://{hostname}:{port}/{database}?server_uuid={server_uuid}");
        let lifecycle_id = format!(
            "mysql-session-{}",
            SNAPSHOT_LIFECYCLE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let (catalog, blockers) = extract_catalog(&mut conn, &database, &server_version)?;
        let catalog_fingerprint = mysql_catalog_fingerprint(&catalog)?;
        let snapshot_id = mysql_snapshot_id(
            &endpoint_identity,
            &server_uuid,
            &gtid_executed_observation,
            connection_id,
            &lifecycle_id,
        );
        let token = SnapshotToken {
            endpoint_identity: endpoint_identity.clone(),
            database_identity: database.clone(),
            snapshot_id,
            consistency_mode: MYSQL_CONSISTENCY_SNAPSHOT.into(),
            server_version: server_version.clone(),
            lifecycle_id: lifecycle_id.clone(),
        };
        let snapshot_evidence = MySqlSnapshotEvidence {
            endpoint_identity,
            database_identity: database,
            server_uuid,
            server_version,
            lifecycle_id,
            connection_id,
            transaction_isolation,
            transaction_read_only: true,
            session_time_zone,
            catalog_snapshot_protected: false,
            information_schema_stats_expiry,
            gtid_executed_observation,
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
        if request.limit == 0 || request.projection.is_empty() || request.key.is_empty() {
            return Err(ConnectionError::InvalidRequest(
                "MySQL page requires a nonzero limit, projection, and complete key".into(),
            ));
        }
        let contract = self
            .table_contracts
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
        let limit = usize::min(request.limit as usize, self.max_batch_rows);
        sql.push_str(&format!(" ORDER BY {key} LIMIT {limit}"));
        let mut batch = RowBatch::new(columns.clone(), limit, self.max_batch_bytes);
        let mut rows = self.conn.exec_iter(sql, params).map_err(database_error)?;
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
        self.cancellation.check()?;
        Ok(batch)
    }
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
            "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION, CREATE_OPTIONS, AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let column_rows: Vec<Row> = conn
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME, EXTRA, GENERATION_EXPRESSION, NUMERIC_PRECISION, NUMERIC_SCALE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (database,),
        )
        .map_err(database_error)?;
    let index_rows: Vec<Row> = conn
        .exec(
            "SELECT s.TABLE_NAME, s.INDEX_NAME, s.NON_UNIQUE, s.SEQ_IN_INDEX, s.COLUMN_NAME, s.COLLATION, s.SUB_PART, s.NULLABLE, s.INDEX_TYPE, s.IS_VISIBLE, s.EXPRESSION, EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS tc WHERE tc.CONSTRAINT_SCHEMA = s.TABLE_SCHEMA AND tc.TABLE_NAME = s.TABLE_NAME AND tc.CONSTRAINT_NAME = s.INDEX_NAME AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')) AS CONSTRAINT_BACKED FROM information_schema.STATISTICS s WHERE s.TABLE_SCHEMA = ? ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX",
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
        if !supported_value_type(&data_type) {
            blockers.push(MySqlCatalogBlocker {
                object_id: id.clone(),
                object_kind: "column_type".into(),
                reason: format!(
                    "MySQL column type {data_type} has no reviewed lossless canonical value mapping"
                ),
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
        let (table_name, table_type, engine, table_collation, create_options, auto_increment): (
            String,
            String,
            Option<String>,
            Option<String>,
            String,
            Option<u64>,
        ) = mysql::from_row(row);
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
            "SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE, ENFORCED FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let key_columns: Vec<MySqlConstraintKeyRow> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA = ? ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
            (database,),
        )
        .map_err(database_error)?;
    let references: Vec<(String, String, String, String, String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? ORDER BY TABLE_NAME, CONSTRAINT_NAME",
            (database,),
        )
        .map_err(database_error)?;
    let checks: Vec<(String, String)> = conn
        .exec(
            "SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? ORDER BY CONSTRAINT_NAME",
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
            "SELECT GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.SCHEMA_PRIVILEGES WHERE TABLE_SCHEMA = ? ORDER BY GRANTEE, PRIVILEGE_TYPE",
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
            "SELECT TABLE_NAME, GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.TABLE_PRIVILEGES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, GRANTEE, PRIVILEGE_TYPE",
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
            "SELECT TABLE_NAME, COLUMN_NAME, GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.COLUMN_PRIVILEGES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, COLUMN_NAME, GRANTEE, PRIVILEGE_TYPE",
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
            "SELECT TRIGGER_NAME, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME",
            CatalogObjectKind::Trigger,
            "trigger",
        ),
        (
            "SELECT ROUTINE_NAME, COALESCE(ROUTINE_DEFINITION, '') FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME, SPECIFIC_NAME",
            CatalogObjectKind::Routine,
            "routine",
        ),
        (
            "SELECT EVENT_NAME, COALESCE(EVENT_DEFINITION, '') FROM information_schema.EVENTS WHERE EVENT_SCHEMA = ? ORDER BY EVENT_NAME",
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
            "SELECT TABLE_NAME, PARTITION_NAME, PARTITION_METHOD, PARTITION_EXPRESSION FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION",
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
        format!("SELECT {column} FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?"),
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
                    ("nullable".into(), serde_json::json!(false)),
                    ("data_type".into(), serde_json::json!("bigint")),
                    ("collation".into(), serde_json::Value::Null),
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
            vendor_metadata: BTreeMap::from([(
                "information_schema_stats_expiry".into(),
                "0".into(),
            )]),
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
                lifecycle_id: format!("life-{endpoint}"),
                connection_id: 7,
                transaction_isolation: "REPEATABLE-READ".into(),
                transaction_read_only: true,
                session_time_zone: "+00:00".into(),
                catalog_snapshot_protected: false,
                information_schema_stats_expiry: 0,
                gtid_executed_observation: String::new(),
                catalog_fingerprint: fingerprint,
            },
            catalog,
            blockers: vec![visibility_blocker],
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
    fn reviewed_mysql_plan_is_typed_and_remains_freeze_blocked() {
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
        assert!(reviewed
            .plan
            .unsupported_objects
            .objects
            .iter()
            .any(|object| {
                object.code == UnsupportedObjectCode::MySqlFreezeEvidence
                    && object.required_semantics
            }));
        assert!(reviewed.plan.unsupported_objects.blocks_execution());
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
            matches!(error, MySqlPlanError::InvalidCatalog(message) if message.contains("column_type"))
        );
    }
}
