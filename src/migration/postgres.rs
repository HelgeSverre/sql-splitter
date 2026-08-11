//! Read-only PostgreSQL plan adapter for the enterprise migration spike.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config, IsolationLevel, Transaction};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::write_json_new;
use super::canonical::CANONICAL_ENCODING_VERSION;
use super::model::{
    CatalogDependency, CatalogNamespace, CatalogObject, CatalogObjectKind, Identifier,
    QualifiedTable, VendorCatalog,
};
use super::plan::{
    MigrationPlan, OperationKind, PlanOperation, ReviewedPlan, UnsupportedObject,
    UnsupportedObjectReport, PLAN_SCHEMA_VERSION,
};

const CATALOG_FORMAT_VERSION: u32 = 1;

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

        let mut tls = TlsConnector::builder();
        if let Some(path) = &self.tls.ca_certificate {
            let pem = fs::read(path).map_err(PostgresPlanError::ReadCa)?;
            tls.add_root_certificate(Certificate::from_pem(&pem)?);
        }
        if self.tls.insecure {
            tls.danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }
        Ok(config.connect(MakeTlsConnector::new(tls.build()?))?)
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
    transaction: &mut Transaction<'_>,
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

    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT (a.attrelid::text || ':' || a.attnum::text), n.nspname, (c.relname || '.' || a.attname), 'column', pg_catalog.format_type(a.atttypid, a.atttypmod), jsonb_build_object('table_oid', c.oid::text, 'table', c.relname, 'ordinal', a.attnum, 'nullable', NOT a.attnotnull, 'default', pg_get_expr(ad.adbin, ad.adrelid), 'identity', a.attidentity::text, 'generated', a.attgenerated::text, 'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE coll.collname END)::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum LEFT JOIN pg_collation coll ON coll.oid = a.attcollation WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY n.nspname, c.relname, a.attnum",
        CatalogObjectKind::Column,
    )?;
    append_query_objects(
        transaction,
        &mut namespaces,
        "SELECT con.oid::text, n.nspname, con.conname, 'constraint', pg_get_constraintdef(con.oid, true), jsonb_build_object('table_oid', con.conrelid::text, 'type', con.contype::text, 'validated', con.convalidated, 'deferrable', con.condeferrable, 'deferred', con.condeferred, 'referenced_table_oid', NULLIF(con.confrelid, 0)::text)::text FROM pg_constraint con JOIN pg_namespace n ON n.oid = con.connamespace WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY n.nspname, con.conname, con.oid",
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
    transaction: &mut Transaction<'_>,
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
        consistency_mode: "repeatable_read_read_only_catalog_snapshot".into(),
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
    let source_config = PostgresEndpointConfig::read(source_config)?;
    let target_config = PostgresEndpointConfig::read(target_config)?;
    if source_config.credential_env == target_config.credential_env {
        return Err(PostgresPlanError::InvalidConfig(
            "source and target must use separate credential references",
        ));
    }
    let source = inspect_endpoint(&source_config)?;
    let target = inspect_endpoint(&target_config)?;
    let plan = build_plan(&source, &target)?;
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
