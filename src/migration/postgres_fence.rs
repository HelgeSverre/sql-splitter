//! Durable PostgreSQL source write fencing.
//!
//! A fence closes the crash-consistency gap that a process-owned snapshot
//! cannot close. Installation holds deterministic `ACCESS EXCLUSIVE` locks
//! until all DML and DDL guards, registry rows, and audit rows are committed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use native_tls::{Certificate, TlsConnector};
use postgres::config::SslMode;
use postgres::{Client, Config, Transaction};
use postgres_native_tls::MakeTlsConnector;
use rand::fill;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::write_json_new;
use super::journal::ConsistencyEvidence;
use super::model::{CatalogObjectKind, QualifiedTable, VendorCatalog};
use super::plan::{OperationKind, ReviewedPlan, UnsupportedObjectReport};
use super::postgres::{catalog_fingerprint, inspect_endpoint, PostgresEndpointConfig};

const FENCE_SCHEMA: &str = "sql_splitter_migration_fence";
const REGISTRY_TABLE: &str = "registry";
const HISTORY_TABLE: &str = "history";
const DML_FUNCTION: &str = "reject_source_writes";
const DDL_FUNCTION: &str = "reject_source_ddl";
const DDL_TRIGGER: &str = "sql_splitter_migration_fence_ddl";
const FENCE_FORMAT_VERSION: i32 = 1;
const DML_FUNCTION_BODY: &str = "BEGIN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source is protected by sql-splitter migration write fence'; END";
const DDL_FUNCTION_BODY: &str = "BEGIN IF EXISTS (SELECT 1 FROM sql_splitter_migration_fence.registry WHERE singleton AND state = 'Released' AND admin_role = session_user) THEN RETURN; END IF; RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source DDL is protected by sql-splitter migration write fence'; END";

/// The secret required to release one installed fence.
///
/// Callers must protect this value as a credential. Only its SHA-256 digest is
/// stored in PostgreSQL or returned as consistency evidence.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FenceToken(String);

impl std::fmt::Debug for FenceToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FenceToken([REDACTED])")
    }
}

impl FenceToken {
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    fn generate() -> Self {
        let mut bytes = [0_u8; 32];
        fill(&mut bytes);
        Self(hex::encode(bytes))
    }

    fn hash(&self) -> String {
        hex::encode(Sha256::digest(self.0.as_bytes()))
    }
}

/// Result of atomically installing a source write fence.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledPostgresFence {
    pub token: FenceToken,
    pub evidence: ConsistencyEvidence,
}

impl std::fmt::Debug for InstalledPostgresFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledPostgresFence")
            .field("token", &"[REDACTED]")
            .field("evidence", &self.evidence)
            .finish()
    }
}

/// Exact inventory used for installation and later attestation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceInventory {
    pub generation: String,
    pub admin_role: String,
    pub schema_oid: u32,
    pub registry_oid: u32,
    pub history_oid: u32,
    pub history_sequence_oid: u32,
    pub dml_function_oid: u32,
    pub ddl_function_oid: u32,
    pub event_trigger_oid: u32,
    pub tables: Vec<FencedTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedTable {
    pub namespace: String,
    pub table: String,
    pub relation_oid: u32,
    pub trigger_oid: u32,
    pub trigger_name: String,
}

impl FenceInventory {
    pub fn fingerprint(&self) -> Result<String, PostgresFenceError> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[derive(Debug, Error)]
pub enum PostgresFenceError {
    #[error("reviewed plan is not valid")]
    InvalidPlan(#[from] super::plan::PlanError),
    #[error("cannot recapture the fenced PostgreSQL catalog")]
    Catalog(#[source] super::postgres::PostgresPlanError),
    #[error("protected fence artifact operation failed")]
    Artifact(#[from] super::artifact::ArtifactError),
    #[error("write-fence installation requires consistency_mode write-fence")]
    WrongConsistencyMode,
    #[error("reviewed plan contains no copied ordinary tables")]
    NoTables,
    #[error("credential environment variable {0} is not set or is not Unicode")]
    MissingCredential(String),
    #[error("cannot read configured CA certificate")]
    ReadCa(#[source] std::io::Error),
    #[error("invalid TLS configuration")]
    Tls(#[from] native_tls::Error),
    #[error("PostgreSQL fence operation failed")]
    Database(#[from] postgres::Error),
    #[error("fence metadata serialization failed")]
    Serialization(#[from] serde_json::Error),
    #[error("a prepared transaction exists in the source database")]
    PreparedTransaction,
    #[error("source table {namespace}.{table} is absent or is not an ordinary table")]
    InvalidSourceTable { namespace: String, table: String },
    #[error("a source fence is already installed")]
    AlreadyInstalled,
    #[error("the source fence is absent")]
    NotInstalled,
    #[error("the fence generation does not match")]
    GenerationMismatch,
    #[error("the fence token does not match")]
    TokenMismatch,
    #[error("fence attestation failed: {0}")]
    Attestation(&'static str),
}

/// Install a durable fence using separate privileged administration credentials.
pub fn install_postgres_write_fence(
    admin: &PostgresEndpointConfig,
    reviewed: &ReviewedPlan,
    artifact_path: impl AsRef<Path>,
) -> Result<InstalledPostgresFence, PostgresFenceError> {
    reviewed.validate()?;
    if reviewed.plan.consistency_mode != "write-fence" {
        return Err(PostgresFenceError::WrongConsistencyMode);
    }
    let tables = planned_tables(reviewed)?;
    let generation = random_hex(16);
    let token = FenceToken::generate();
    let token_hash = token.hash();
    let mut client = connect_admin(admin)?;
    let mut transaction = client.transaction()?;

    reject_prepared_transactions(&mut transaction)?;
    reject_existing_fence(&mut transaction)?;
    let admin_role: String = transaction.query_one("SELECT current_user", &[])?.get(0);
    let mut inventory =
        lock_and_resolve_tables(&mut transaction, &tables, &generation, &admin_role)?;
    let live_identity = transaction.query_one(
        "SELECT current_database(), COALESCE(inet_server_addr()::text, 'local'), COALESCE(inet_server_port(), 0)",
        &[],
    )?;
    let live_endpoint = format!(
        "postgres://{}:{}/{}",
        live_identity.get::<_, String>(1),
        live_identity.get::<_, i32>(2),
        live_identity.get::<_, String>(0)
    );
    let reviewed_endpoint = reviewed
        .plan
        .source_endpoint_identity
        .split_once("?user=")
        .map_or(
            reviewed.plan.source_endpoint_identity.as_str(),
            |(base, _)| base,
        );
    if live_endpoint != reviewed_endpoint {
        return Err(PostgresFenceError::Attestation(
            "fence administrator is connected to a different source endpoint",
        ));
    }
    let endpoint_identity = reviewed.plan.source_endpoint_identity.clone();
    let business_catalog_fingerprint = reviewed.plan.source_catalog_fingerprint.clone();

    install_protected_registry(&mut transaction)?;
    install_guard_functions(&mut transaction)?;
    install_table_guards(&mut transaction, &inventory)?;
    install_ddl_guard(&mut transaction)?;
    resolve_protected_inventory(&mut transaction, &mut inventory)?;
    let inventory_json = serde_json::to_string(&inventory)?;
    let inventory_fingerprint = inventory.fingerprint()?;

    let row = transaction.query_one(
        "SELECT oid::bigint, (pg_control_system()).system_identifier::text FROM pg_database WHERE datname = current_database()",
        &[],
    )?;
    let database_oid = u32::try_from(row.get::<_, i64>(0))
        .map_err(|_| PostgresFenceError::Attestation("database OID is out of range"))?;
    let system_identifier: String = row.get(1);
    let activation = transaction.query_one(
        "SELECT txid_current()::bigint, clock_timestamp()::text",
        &[],
    )?;
    let activation_xid = u64::try_from(activation.get::<_, i64>(0))
        .map_err(|_| PostgresFenceError::Attestation("activation transaction ID is negative"))?;
    let activation_xid_db = i64::try_from(activation_xid)
        .map_err(|_| PostgresFenceError::Attestation("activation transaction ID exceeds bigint"))?;
    let activated_at: String = activation.get(1);

    transaction.execute(
        &format!(
            "INSERT INTO {schema}.{registry} (singleton, format_version, generation, token_hash, admin_role, endpoint_identity, database_oid, system_identifier, business_catalog_fingerprint, inventory_fingerprint, inventory_json, activation_xid, activated_at, state) VALUES (true, $1, $2, $3, $4, $5, $6::oid, $7, $8, $9, $10::text::jsonb, $11, $12::text::timestamptz, 'Draining')",
            schema = quote_ident(FENCE_SCHEMA),
            registry = quote_ident(REGISTRY_TABLE),
        ),
        &[&FENCE_FORMAT_VERSION, &generation, &token_hash, &admin_role, &endpoint_identity, &database_oid, &system_identifier, &business_catalog_fingerprint, &inventory_fingerprint, &inventory_json, &activation_xid_db, &activated_at],
    )?;
    append_history(&mut transaction, &generation, "Draining")?;
    let installed = InstalledPostgresFence {
        token,
        evidence: ConsistencyEvidence::WriteFence {
            generation: generation.clone(),
            token_hash: token_hash.clone(),
            endpoint_identity: endpoint_identity.clone(),
            database_oid,
            system_identifier: system_identifier.clone(),
            business_catalog_fingerprint: business_catalog_fingerprint.clone(),
            fence_inventory_fingerprint: inventory_fingerprint.clone(),
            activation_xid,
            activated_at: activated_at.clone(),
        },
    };
    write_json_new(artifact_path, &installed)?;
    transaction.commit()?;

    // The committed Draining fence now prevents all planned-table writes and
    // all new database DDL. End transactions that began before guard commit so
    // no statement that passed the DDL hook earlier can commit afterward.
    drain_preexisting_transactions(&mut client, &activated_at)?;
    // Recapture the business catalog after that drain. A failure leaves the
    // durable Draining fence in place and never claims crash-safe consistency.
    let mut snapshot = inspect_endpoint(admin).map_err(PostgresFenceError::Catalog)?;
    remove_attested_fence_objects(&mut snapshot.catalog, &mut snapshot.unsupported, &inventory)?;
    let post_drain_fingerprint =
        catalog_fingerprint(&snapshot.catalog).map_err(PostgresFenceError::Catalog)?;
    if post_drain_fingerprint != business_catalog_fingerprint {
        return Err(PostgresFenceError::Attestation(
            "business catalog changed while the fence was installed",
        ));
    }

    let mut active = client.transaction()?;
    lock_inventory_tables(&mut active, &inventory)?;
    active.execute(
        &format!(
            "UPDATE {}.{} SET state = 'Active' WHERE singleton AND state = 'Draining'",
            quote_ident(FENCE_SCHEMA),
            quote_ident(REGISTRY_TABLE)
        ),
        &[],
    )?;
    append_history(&mut active, &generation, "Active")?;
    attest_transaction(&mut active, &inventory, &token_hash, "Active")?;
    active.commit()?;

    Ok(installed)
}

fn drain_preexisting_transactions(
    client: &mut Client,
    guard_committed_at: &str,
) -> Result<(), PostgresFenceError> {
    let rows = client.query(
        "SELECT pid FROM pg_stat_activity WHERE datid = (SELECT oid FROM pg_database WHERE datname=current_database()) AND pid <> pg_backend_pid() AND backend_type = 'client backend' AND xact_start IS NOT NULL AND xact_start <= $1::text::timestamptz",
        &[&guard_committed_at],
    )?;
    for row in rows {
        let pid: i32 = row.get(0);
        let terminated: bool = client
            .query_one("SELECT pg_terminate_backend($1)", &[&pid])?
            .get(0);
        if !terminated {
            return Err(PostgresFenceError::Attestation(
                "a pre-fence source transaction could not be terminated",
            ));
        }
    }
    let remaining: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE datid = (SELECT oid FROM pg_database WHERE datname=current_database()) AND pid <> pg_backend_pid() AND backend_type = 'client backend' AND xact_start IS NOT NULL AND xact_start <= $1::text::timestamptz",
            &[&guard_committed_at],
        )?
        .get(0);
    if remaining != 0 {
        return Err(PostgresFenceError::Attestation(
            "a pre-fence source transaction remains active",
        ));
    }
    Ok(())
}

/// Remove only the exact, attested internal fence objects from a fresh catalog.
pub fn remove_attested_fence_objects(
    catalog: &mut VendorCatalog,
    unsupported: &mut UnsupportedObjectReport,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    let namespace_position = catalog
        .namespaces
        .iter()
        .position(|namespace| {
            namespace.name.as_str() == FENCE_SCHEMA
                && namespace.id == inventory.schema_oid.to_string()
        })
        .ok_or(PostgresFenceError::Attestation(
            "attested reserved namespace is absent from the catalog",
        ))?;
    let mut removed_ids = catalog.namespaces[namespace_position]
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect::<BTreeSet<_>>();
    removed_ids.insert(inventory.schema_oid.to_string());
    catalog.namespaces.remove(namespace_position);

    let trigger_ids = inventory
        .tables
        .iter()
        .map(|table| table.trigger_oid.to_string())
        .collect::<BTreeSet<_>>();
    for namespace in &mut catalog.namespaces {
        namespace.objects.retain(|object| {
            let is_attested_trigger =
                object.kind == CatalogObjectKind::Trigger && trigger_ids.contains(&object.id);
            if is_attested_trigger {
                removed_ids.insert(object.id.clone());
            }
            !is_attested_trigger
        });
    }
    catalog.dependencies.retain(|dependency| {
        !removed_ids.contains(&dependency.from_object_id)
            && !removed_ids.contains(&dependency.to_object_id)
    });

    let exact_oid_suffixes = [
        inventory.schema_oid,
        inventory.registry_oid,
        inventory.history_oid,
        inventory.history_sequence_oid,
        inventory.dml_function_oid,
        inventory.ddl_function_oid,
        inventory.event_trigger_oid,
    ]
    .into_iter()
    .chain(inventory.tables.iter().map(|table| table.trigger_oid))
    .map(|oid| oid.to_string())
    .collect::<BTreeSet<_>>();
    unsupported.objects.retain(|object| {
        if removed_ids.contains(&object.object_id) {
            return false;
        }
        let suffix = object.object_id.rsplit(':').next().unwrap_or_default();
        !matches!(
            object.object_kind.as_str(),
            "namespace_acl"
                | "relation_acl"
                | "routine_acl"
                | "event_trigger"
                | "trigger"
                | "routine"
                | "sequence"
                | "serial_sequence_default"
        ) || !exact_oid_suffixes.contains(suffix)
    });
    Ok(())
}

/// Attest that the database still has exactly the guards recorded in evidence.
pub fn attest_postgres_write_fence(
    admin: &PostgresEndpointConfig,
    evidence: &ConsistencyEvidence,
) -> Result<FenceInventory, PostgresFenceError> {
    let ConsistencyEvidence::WriteFence {
        generation,
        token_hash,
        endpoint_identity,
        database_oid,
        system_identifier,
        business_catalog_fingerprint,
        fence_inventory_fingerprint,
        activation_xid,
        activated_at,
    } = evidence
    else {
        return Err(PostgresFenceError::Attestation(
            "evidence is not a write fence",
        ));
    };
    let mut client = connect_admin(admin)?;
    let mut transaction = client.transaction()?;
    let registry = load_registry(&mut transaction)?;
    if registry.state != "Active" {
        return Err(PostgresFenceError::Attestation("registry is not Active"));
    }
    if &registry.generation != generation
        || &registry.token_hash != token_hash
        || &registry.endpoint_identity != endpoint_identity
        || &registry.database_oid != database_oid
        || &registry.system_identifier != system_identifier
        || &registry.business_catalog_fingerprint != business_catalog_fingerprint
        || &registry.inventory_fingerprint != fence_inventory_fingerprint
        || &registry.activation_xid != activation_xid
        || &registry.activated_at != activated_at
    {
        return Err(PostgresFenceError::Attestation(
            "registry differs from consistency evidence",
        ));
    }
    attest_transaction(&mut transaction, &registry.inventory, token_hash, "Active")?;
    transaction.commit()?;
    Ok(registry.inventory)
}

/// Release an installed fence after exact generation and secret validation.
///
/// The protected registry and append-only history remain as audit evidence.
pub fn release_postgres_write_fence(
    admin: &PostgresEndpointConfig,
    generation: &str,
    token: &FenceToken,
) -> Result<(), PostgresFenceError> {
    let mut client = connect_admin(admin)?;
    let mut transaction = client.transaction()?;
    let registry = load_registry_for_update(&mut transaction)?;
    if registry.generation != generation {
        return Err(PostgresFenceError::GenerationMismatch);
    }
    if !constant_time_eq(registry.token_hash.as_bytes(), token.hash().as_bytes()) {
        return Err(PostgresFenceError::TokenMismatch);
    }
    if registry.state == "Released" {
        let history: Vec<String> = transaction
            .query(
                &format!(
                    "SELECT state FROM {}.{} WHERE generation=$1 ORDER BY sequence",
                    quote_ident(FENCE_SCHEMA),
                    quote_ident(HISTORY_TABLE)
                ),
                &[&registry.generation],
            )?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        if history == ["Draining", "Active", "Released"] || history == ["Draining", "Released"] {
            transaction.commit()?;
            return Ok(());
        }
        return Err(PostgresFenceError::Attestation(
            "released fence history is invalid",
        ));
    }
    if !matches!(registry.state.as_str(), "Draining" | "Active") {
        return Err(PostgresFenceError::Attestation(
            "registry cannot be released from its current state",
        ));
    }
    attest_transaction(
        &mut transaction,
        &registry.inventory,
        &registry.token_hash,
        &registry.state,
    )?;
    append_history(&mut transaction, generation, "Released")?;
    transaction.execute(
        &format!(
            "UPDATE {}.{} SET state = 'Released' WHERE singleton",
            quote_ident(FENCE_SCHEMA),
            quote_ident(REGISTRY_TABLE)
        ),
        &[],
    )?;
    transaction.batch_execute(&format!(
        "ALTER EVENT TRIGGER {} DISABLE",
        quote_ident(DDL_TRIGGER)
    ))?;
    for table in &registry.inventory.tables {
        transaction.batch_execute(&format!(
            "DROP TRIGGER {} ON {}.{}",
            quote_ident(&table.trigger_name),
            quote_ident(&table.namespace),
            quote_ident(&table.table),
        ))?;
    }
    transaction.batch_execute(&format!(
        "DROP EVENT TRIGGER {}; DROP FUNCTION {}.{}(); DROP FUNCTION {}.{}();",
        quote_ident(DDL_TRIGGER),
        quote_ident(FENCE_SCHEMA),
        quote_ident(DDL_FUNCTION),
        quote_ident(FENCE_SCHEMA),
        quote_ident(DML_FUNCTION)
    ))?;
    transaction.commit()?;
    Ok(())
}

#[derive(Debug)]
struct Registry {
    generation: String,
    token_hash: String,
    endpoint_identity: String,
    database_oid: u32,
    system_identifier: String,
    business_catalog_fingerprint: String,
    inventory_fingerprint: String,
    activation_xid: u64,
    activated_at: String,
    inventory: FenceInventory,
    state: String,
}

fn planned_tables(reviewed: &ReviewedPlan) -> Result<Vec<QualifiedTable>, PostgresFenceError> {
    let tables: BTreeSet<_> = reviewed
        .plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::CopyTable)
        .filter_map(|operation| operation.table.clone())
        .collect();
    if tables.is_empty() {
        return Err(PostgresFenceError::NoTables);
    }
    Ok(tables.into_iter().collect())
}

fn connect_admin(config: &PostgresEndpointConfig) -> Result<Client, PostgresFenceError> {
    let password = std::env::var(&config.credential_env)
        .map_err(|_| PostgresFenceError::MissingCredential(config.credential_env.clone()))?;
    let mut postgres = Config::new();
    postgres
        .host(&config.host)
        .port(config.port)
        .dbname(&config.database)
        .user(&config.user)
        .password(password)
        .application_name("sql-splitter-migration-fence-admin")
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(config.connect_timeout_seconds));
    Ok(postgres.connect(tls_connector(config)?)?)
}

fn tls_connector(config: &PostgresEndpointConfig) -> Result<MakeTlsConnector, PostgresFenceError> {
    let mut tls = TlsConnector::builder();
    if let Some(path) = &config.tls.ca_certificate {
        let pem = fs::read(path).map_err(PostgresFenceError::ReadCa)?;
        tls.add_root_certificate(Certificate::from_pem(&pem)?);
    }
    if config.tls.insecure {
        tls.danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }
    Ok(MakeTlsConnector::new(tls.build()?))
}

fn reject_prepared_transactions(
    transaction: &mut Transaction<'_>,
) -> Result<(), PostgresFenceError> {
    if transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_prepared_xacts WHERE database = current_database())",
            &[],
        )?
        .get(0)
    {
        return Err(PostgresFenceError::PreparedTransaction);
    }
    Ok(())
}

fn reject_existing_fence(transaction: &mut Transaction<'_>) -> Result<(), PostgresFenceError> {
    let exists: bool = transaction
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}")],
        )?
        .get(0);
    if exists {
        return Err(PostgresFenceError::AlreadyInstalled);
    }
    Ok(())
}

fn lock_and_resolve_tables(
    transaction: &mut Transaction<'_>,
    tables: &[QualifiedTable],
    generation: &str,
    admin_role: &str,
) -> Result<FenceInventory, PostgresFenceError> {
    let mut inventory = Vec::with_capacity(tables.len());
    for (index, table) in tables.iter().enumerate() {
        let qualified = format!(
            "{}.{}",
            quote_ident(table.namespace.as_str()),
            quote_ident(table.name.as_str())
        );
        transaction.batch_execute(&format!("LOCK TABLE {qualified} IN ACCESS EXCLUSIVE MODE"))?;
        let row = transaction.query_opt("SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'r'", &[&table.namespace.as_str(), &table.name.as_str()])?
            .ok_or_else(|| PostgresFenceError::InvalidSourceTable { namespace: table.namespace.to_string(), table: table.name.to_string() })?;
        let relation_oid = u32::try_from(row.get::<_, i64>(0))
            .map_err(|_| PostgresFenceError::Attestation("relation OID is out of range"))?;
        inventory.push(FencedTable {
            namespace: table.namespace.to_string(),
            table: table.name.to_string(),
            relation_oid,
            trigger_oid: 0,
            trigger_name: trigger_name(index, table),
        });
    }
    Ok(FenceInventory {
        generation: generation.to_owned(),
        admin_role: admin_role.to_owned(),
        schema_oid: 0,
        registry_oid: 0,
        history_oid: 0,
        history_sequence_oid: 0,
        dml_function_oid: 0,
        ddl_function_oid: 0,
        event_trigger_oid: 0,
        tables: inventory,
    })
}

fn resolve_protected_inventory(
    transaction: &mut Transaction<'_>,
    inventory: &mut FenceInventory,
) -> Result<(), PostgresFenceError> {
    let row = transaction.query_one(
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4)::oid::bigint, to_regprocedure($5)::oid::bigint, (SELECT oid::bigint FROM pg_event_trigger WHERE evtname = $6) FROM pg_namespace n WHERE n.nspname = $1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{DML_FUNCTION}()"),
            &format!("{FENCE_SCHEMA}.{DDL_FUNCTION}()"),
            &DDL_TRIGGER,
        ],
    )?;
    inventory.schema_oid = oid_from_i64(row.get(0))?;
    inventory.registry_oid = oid_from_i64(row.get(1))?;
    inventory.history_oid = oid_from_i64(row.get(2))?;
    inventory.history_sequence_oid = oid_from_i64(row.get(3))?;
    inventory.dml_function_oid = oid_from_i64(row.get(4))?;
    inventory.ddl_function_oid = oid_from_i64(row.get(5))?;
    inventory.event_trigger_oid = oid_from_i64(row.get(6))?;
    for table in &mut inventory.tables {
        let oid: i64 = transaction
            .query_one(
                "SELECT oid::bigint FROM pg_trigger WHERE tgrelid = $1::oid AND tgname = $2 AND NOT tgisinternal",
                &[&table.relation_oid, &table.trigger_name],
            )?
            .get(0);
        table.trigger_oid = oid_from_i64(oid)?;
    }
    Ok(())
}

fn oid_from_i64(value: i64) -> Result<u32, PostgresFenceError> {
    u32::try_from(value)
        .map_err(|_| PostgresFenceError::Attestation("PostgreSQL OID is out of range"))
}

fn lock_inventory_tables(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    for table in &inventory.tables {
        transaction.batch_execute(&format!(
            "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
            quote_ident(&table.namespace),
            quote_ident(&table.table)
        ))?;
        let oid: i64 = transaction
            .query_one(
                "SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='r'",
                &[&table.namespace, &table.table],
            )?
            .get(0);
        if u32::try_from(oid).ok() != Some(table.relation_oid) {
            return Err(PostgresFenceError::Attestation(
                "a fenced relation identity changed during drain",
            ));
        }
    }
    Ok(())
}

fn install_protected_registry(transaction: &mut Transaction<'_>) -> Result<(), PostgresFenceError> {
    transaction.batch_execute(&format!(
        "CREATE SCHEMA {schema}; REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; CREATE TABLE {schema}.{registry} (singleton boolean PRIMARY KEY CHECK (singleton), format_version integer NOT NULL, generation text NOT NULL UNIQUE, token_hash text NOT NULL CHECK (length(token_hash) = 64), admin_role name NOT NULL, endpoint_identity text NOT NULL, database_oid oid NOT NULL, system_identifier text NOT NULL, business_catalog_fingerprint text NOT NULL, inventory_fingerprint text NOT NULL, inventory_json jsonb NOT NULL, activation_xid bigint NOT NULL, activated_at timestamptz NOT NULL, state text NOT NULL CHECK (state IN ('Draining', 'Active', 'Released'))); CREATE TABLE {schema}.{history} (sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, generation text NOT NULL, state text NOT NULL CHECK (state IN ('Draining', 'Active', 'Released')), recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), recorded_by name NOT NULL DEFAULT current_user); REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC; REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC;",
        schema = quote_ident(FENCE_SCHEMA), registry = quote_ident(REGISTRY_TABLE), history = quote_ident(HISTORY_TABLE)
    ))?;
    Ok(())
}

fn install_guard_functions(transaction: &mut Transaction<'_>) -> Result<(), PostgresFenceError> {
    transaction.batch_execute(&format!(
        "CREATE FUNCTION {schema}.{dml}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $body${dml_body}$body$; REVOKE ALL ON FUNCTION {schema}.{dml}() FROM PUBLIC; CREATE FUNCTION {schema}.{ddl}() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $body${ddl_body}$body$; REVOKE ALL ON FUNCTION {schema}.{ddl}() FROM PUBLIC;",
        schema = quote_ident(FENCE_SCHEMA), dml = quote_ident(DML_FUNCTION), ddl = quote_ident(DDL_FUNCTION), dml_body = DML_FUNCTION_BODY, ddl_body = DDL_FUNCTION_BODY
    ))?;
    Ok(())
}

fn install_table_guards(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    for table in &inventory.tables {
        transaction.batch_execute(&format!(
            "CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {namespace}.{table} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.{function}(); ALTER TABLE {namespace}.{table} ENABLE ALWAYS TRIGGER {trigger};",
            trigger = quote_ident(&table.trigger_name), namespace = quote_ident(&table.namespace), table = quote_ident(&table.table), schema = quote_ident(FENCE_SCHEMA), function = quote_ident(DML_FUNCTION)
        ))?;
    }
    Ok(())
}

fn install_ddl_guard(transaction: &mut Transaction<'_>) -> Result<(), PostgresFenceError> {
    transaction.batch_execute(&format!(
        "CREATE EVENT TRIGGER {} ON ddl_command_start EXECUTE FUNCTION {}.{}()",
        quote_ident(DDL_TRIGGER),
        quote_ident(FENCE_SCHEMA),
        quote_ident(DDL_FUNCTION)
    ))?;
    Ok(())
}

fn append_history(
    transaction: &mut Transaction<'_>,
    generation: &str,
    state: &str,
) -> Result<(), PostgresFenceError> {
    transaction.execute(
        &format!(
            "INSERT INTO {}.{} (generation, state) VALUES ($1, $2)",
            quote_ident(FENCE_SCHEMA),
            quote_ident(HISTORY_TABLE)
        ),
        &[&generation, &state],
    )?;
    Ok(())
}

fn load_registry(transaction: &mut Transaction<'_>) -> Result<Registry, PostgresFenceError> {
    load_registry_with_suffix(transaction, "")
}

fn load_registry_for_update(
    transaction: &mut Transaction<'_>,
) -> Result<Registry, PostgresFenceError> {
    load_registry_with_suffix(transaction, " FOR UPDATE")
}

fn load_registry_with_suffix(
    transaction: &mut Transaction<'_>,
    suffix: &str,
) -> Result<Registry, PostgresFenceError> {
    let sql = format!("SELECT generation, token_hash, endpoint_identity, database_oid::bigint, system_identifier, business_catalog_fingerprint, inventory_fingerprint, activation_xid, activated_at::text, inventory_json::text, state FROM {}.{} WHERE singleton{suffix}", quote_ident(FENCE_SCHEMA), quote_ident(REGISTRY_TABLE));
    let row = transaction
        .query_opt(&sql, &[])?
        .ok_or(PostgresFenceError::NotInstalled)?;
    let state: String = row.get(10);
    let database_oid = u32::try_from(row.get::<_, i64>(3))
        .map_err(|_| PostgresFenceError::Attestation("database OID is out of range"))?;
    let activation_xid = u64::try_from(row.get::<_, i64>(7))
        .map_err(|_| PostgresFenceError::Attestation("activation transaction ID is negative"))?;
    Ok(Registry {
        generation: row.get(0),
        token_hash: row.get(1),
        endpoint_identity: row.get(2),
        database_oid,
        system_identifier: row.get(4),
        business_catalog_fingerprint: row.get(5),
        inventory_fingerprint: row.get(6),
        activation_xid,
        activated_at: row.get(8),
        inventory: serde_json::from_str(&row.get::<_, String>(9))?,
        state,
    })
}

fn attest_transaction(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
    token_hash: &str,
    expected_state: &str,
) -> Result<(), PostgresFenceError> {
    if inventory.fingerprint()? != load_inventory_fingerprint(transaction)? {
        return Err(PostgresFenceError::Attestation(
            "inventory fingerprint differs",
        ));
    }
    let stored_token: String = transaction
        .query_one(
            &format!(
                "SELECT token_hash FROM {}.{} WHERE singleton AND state = $1",
                quote_ident(FENCE_SCHEMA),
                quote_ident(REGISTRY_TABLE)
            ),
            &[&expected_state],
        )?
        .get(0);
    if !constant_time_eq(stored_token.as_bytes(), token_hash.as_bytes()) {
        return Err(PostgresFenceError::Attestation("token digest differs"));
    }
    attest_owners_and_acl(transaction, &inventory.admin_role)?;
    attest_protected_object_ids(transaction, inventory, expected_state)?;
    let event: Option<i64> = transaction.query_opt("SELECT e.oid::bigint FROM pg_event_trigger e JOIN pg_proc p ON p.oid=e.evtfoid JOIN pg_namespace n ON n.oid=p.pronamespace WHERE e.evtname=$1 AND e.evtevent='ddl_command_start' AND e.evtenabled='O' AND n.nspname=$2 AND p.proname=$3", &[&DDL_TRIGGER, &FENCE_SCHEMA, &DDL_FUNCTION])?.map(|row| row.get(0));
    if event.and_then(|oid| u32::try_from(oid).ok()) != Some(inventory.event_trigger_oid) {
        return Err(PostgresFenceError::Attestation("DDL event trigger differs"));
    }
    let expected: BTreeMap<_, _> = inventory
        .tables
        .iter()
        .map(|table| (table.relation_oid, &table.trigger_name))
        .collect();
    let rows = transaction.query("SELECT tgrelid::bigint, t.oid::bigint, tgname FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace WHERE NOT t.tgisinternal AND n.nspname=$1 AND p.proname=$2", &[&FENCE_SCHEMA, &DML_FUNCTION])?;
    let actual: BTreeMap<u32, (u32, String)> = rows
        .into_iter()
        .map(|row| {
            let oid = u32::try_from(row.get::<_, i64>(0)).map_err(|_| {
                PostgresFenceError::Attestation("trigger relation OID is out of range")
            })?;
            let trigger_oid = oid_from_i64(row.get(1))?;
            Ok((oid, (trigger_oid, row.get(2))))
        })
        .collect::<Result<_, PostgresFenceError>>()?;
    if actual.len() != expected.len()
        || expected.iter().any(|(oid, name)| {
            actual.get(oid).is_none_or(|(trigger_oid, actual_name)| {
                actual_name != name.as_str()
                    || inventory
                        .tables
                        .iter()
                        .find(|table| table.relation_oid == *oid)
                        .is_none_or(|table| table.trigger_oid != *trigger_oid)
            })
        })
    {
        return Err(PostgresFenceError::Attestation(
            "table trigger inventory differs",
        ));
    }
    let invalid: i64 = transaction.query_one("SELECT count(*) FROM pg_trigger WHERE tgname = ANY($1) AND (tgenabled <> 'A' OR (tgtype & 2) = 0 OR (tgtype & 4) = 0 OR (tgtype & 8) = 0 OR (tgtype & 16) = 0 OR (tgtype & 32) = 0 OR (tgtype & 1) <> 0)", &[&inventory.tables.iter().map(|table| table.trigger_name.as_str()).collect::<Vec<_>>()])?.get(0);
    if invalid != 0 {
        return Err(PostgresFenceError::Attestation(
            "a table trigger is not an ENABLE ALWAYS statement guard",
        ));
    }
    Ok(())
}

fn attest_protected_object_ids(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
    expected_state: &str,
) -> Result<(), PostgresFenceError> {
    let row = transaction.query_one(
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4)::oid::bigint, to_regprocedure($5)::oid::bigint FROM pg_namespace n WHERE n.nspname=$1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{DML_FUNCTION}()"),
            &format!("{FENCE_SCHEMA}.{DDL_FUNCTION}()"),
        ],
    )?;
    let observed = [
        oid_from_i64(row.get(0))?,
        oid_from_i64(row.get(1))?,
        oid_from_i64(row.get(2))?,
        oid_from_i64(row.get(3))?,
        oid_from_i64(row.get(4))?,
        oid_from_i64(row.get(5))?,
    ];
    let expected = [
        inventory.schema_oid,
        inventory.registry_oid,
        inventory.history_oid,
        inventory.history_sequence_oid,
        inventory.dml_function_oid,
        inventory.ddl_function_oid,
    ];
    if observed != expected {
        return Err(PostgresFenceError::Attestation(
            "protected object identity differs",
        ));
    }
    let function_oids = [inventory.dml_function_oid, inventory.ddl_function_oid];
    let valid_functions: i64 = transaction.query_one(
        "SELECT count(*) FROM pg_proc WHERE oid = ANY($1) AND prosecdef AND proconfig = ARRAY['search_path=pg_catalog']::text[]",
        &[&&function_oids[..]],
    )?.get(0);
    if valid_functions != 2 {
        return Err(PostgresFenceError::Attestation(
            "guard function security attributes differ",
        ));
    }
    let definitions = transaction.query(
        "SELECT p.oid::bigint, p.prosrc, l.lanname, p.prorettype::regtype::text, p.pronargs FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid = ANY($1)",
        &[&&function_oids[..]],
    )?;
    let expected_definitions = BTreeMap::from([
        (inventory.dml_function_oid, (DML_FUNCTION_BODY, "trigger")),
        (
            inventory.ddl_function_oid,
            (DDL_FUNCTION_BODY, "event_trigger"),
        ),
    ]);
    for row in definitions {
        let oid = oid_from_i64(row.get(0))?;
        let Some((body, return_type)) = expected_definitions.get(&oid) else {
            return Err(PostgresFenceError::Attestation("unexpected guard function"));
        };
        if row.get::<_, String>(1).trim() != *body
            || row.get::<_, String>(2) != "plpgsql"
            || row.get::<_, String>(3) != *return_type
            || row.get::<_, i16>(4) != 0
        {
            return Err(PostgresFenceError::Attestation(
                "guard function definition differs",
            ));
        }
    }
    let history: Vec<String> = transaction
        .query(
            &format!(
                "SELECT state FROM {}.{} WHERE generation=$1 ORDER BY sequence",
                quote_ident(FENCE_SCHEMA),
                quote_ident(HISTORY_TABLE)
            ),
            &[&inventory.generation],
        )?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let expected_history = if expected_state == "Draining" {
        &["Draining"][..]
    } else {
        &["Draining", "Active"][..]
    };
    if history.iter().map(String::as_str).collect::<Vec<_>>() != expected_history {
        return Err(PostgresFenceError::Attestation(
            "fence history is incomplete or contains an unexpected transition",
        ));
    }
    Ok(())
}

fn load_inventory_fingerprint(
    transaction: &mut Transaction<'_>,
) -> Result<String, PostgresFenceError> {
    Ok(transaction
        .query_one(
            &format!(
                "SELECT inventory_fingerprint FROM {}.{} WHERE singleton",
                quote_ident(FENCE_SCHEMA),
                quote_ident(REGISTRY_TABLE)
            ),
            &[],
        )?
        .get(0))
}

fn attest_owners_and_acl(
    transaction: &mut Transaction<'_>,
    admin_role: &str,
) -> Result<(), PostgresFenceError> {
    let row = transaction.query_one("SELECT n.nspowner::regrole::text, NOT EXISTS (SELECT 1 FROM aclexplode(coalesce(n.nspacl, acldefault('n'::\"char\", n.nspowner))) a WHERE a.grantee = 0) FROM pg_namespace n WHERE n.nspname=$1", &[&FENCE_SCHEMA])?;
    if row.get::<_, String>(0) != admin_role || !row.get::<_, bool>(1) {
        return Err(PostgresFenceError::Attestation(
            "reserved schema owner or ACL differs",
        ));
    }
    let invalid: i64 = transaction.query_one("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND (c.relowner::regrole::text <> $2 OR EXISTS (SELECT 1 FROM aclexplode(coalesce(c.relacl, acldefault(CASE WHEN c.relkind='S' THEN 'S'::\"char\" ELSE 'r'::\"char\" END, c.relowner))) a WHERE a.grantee <> c.relowner))", &[&FENCE_SCHEMA, &admin_role])?.get(0);
    if invalid != 0 {
        return Err(PostgresFenceError::Attestation(
            "reserved object owner or ACL differs",
        ));
    }
    Ok(())
}

fn trigger_name(index: usize, table: &QualifiedTable) -> String {
    let identity = format!("{}.{}", table.namespace, table.name);
    let digest = hex::encode(Sha256::digest(identity.as_bytes()));
    format!("sql_splitter_fence_{index:06}_{}", &digest[..16])
}

fn random_hex(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    fill(&mut value);
    hex::encode(value)
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::super::model::Identifier;
    use super::*;

    fn table(namespace: &str, name: &str) -> QualifiedTable {
        QualifiedTable {
            namespace: Identifier::new(namespace).unwrap(),
            name: Identifier::new(name).unwrap(),
        }
    }

    #[test]
    fn trigger_inventory_names_are_deterministic_and_identifier_safe() {
        let first = trigger_name(7, &table("odd schema", "account\"entry"));
        let second = trigger_name(7, &table("odd schema", "account\"entry"));
        assert_eq!(first, second);
        assert!(first.len() <= 63);
        assert!(first
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'));
    }

    #[test]
    fn tokens_are_random_and_validate_only_exactly() {
        let first = FenceToken::generate();
        let second = FenceToken::generate();
        assert_ne!(first, second);
        assert!(constant_time_eq(
            first.hash().as_bytes(),
            first.hash().as_bytes()
        ));
        assert!(!constant_time_eq(
            first.hash().as_bytes(),
            second.hash().as_bytes()
        ));
        assert!(!constant_time_eq(b"short", b"longer"));
        assert_eq!(format!("{first:?}"), "FenceToken([REDACTED])");
        assert!(!format!("{first:?}").contains(first.expose_secret()));
    }

    #[test]
    fn inventory_fingerprint_is_order_sensitive_and_stable() {
        let inventory = FenceInventory {
            generation: "g".into(),
            admin_role: "admin".into(),
            schema_oid: 1,
            registry_oid: 2,
            history_oid: 3,
            history_sequence_oid: 4,
            dml_function_oid: 5,
            ddl_function_oid: 6,
            event_trigger_oid: 7,
            tables: vec![FencedTable {
                namespace: "public".into(),
                table: "a".into(),
                relation_oid: 1,
                trigger_oid: 8,
                trigger_name: "t1".into(),
            }],
        };
        assert_eq!(
            inventory.fingerprint().unwrap(),
            inventory.fingerprint().unwrap()
        );
        let mut changed = inventory.clone();
        changed.tables[0].relation_oid = 2;
        assert_ne!(
            inventory.fingerprint().unwrap(),
            changed.fingerprint().unwrap()
        );
    }

    #[test]
    fn identifier_quoting_cannot_add_sql_tokens() {
        assert_eq!(
            quote_ident("a\"; DROP SCHEMA public; --"),
            "\"a\"\"; DROP SCHEMA public; --\""
        );
    }
}
