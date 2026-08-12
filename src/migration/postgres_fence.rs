//! Durable PostgreSQL source write fencing.
//!
//! A fence closes the crash-consistency gap that a process-owned snapshot
//! cannot close. Installation holds deterministic `ACCESS EXCLUSIVE` locks
//! until all DML and DDL guards, registry rows, and audit rows are committed.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use postgres::config::SslMode;
use postgres::{Client, Config, Transaction};
use rand::fill;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::artifact::write_json_new;
use super::journal::ConsistencyEvidence;
use super::model::{CatalogObjectKind, QualifiedTable, VendorCatalog};
use super::plan::{OperationKind, ReviewedPlan, UnsupportedObjectReport};
use super::postgres::{
    catalog_fingerprint, inspect_endpoint, postgres_tls_binding, postgres_tls_connector,
    PostgresEndpointConfig,
};
use super::postgres_profile::PostgresSourceProfileContract;

const FENCE_SCHEMA: &str = "sql_splitter_migration_fence";
const REGISTRY_TABLE: &str = "registry";
const HISTORY_TABLE: &str = "history";
const DML_FUNCTION: &str = "reject_source_writes";
const DDL_FUNCTION: &str = "reject_source_ddl";
const DDL_TRIGGER: &str = "sql_splitter_migration_fence_ddl";
const HISTORY_FUNCTION: &str = "reject_history_mutation";
const HISTORY_TRIGGER: &str = "sql_splitter_migration_fence_history_immutable";
const FENCE_FORMAT_VERSION: i32 = 3;
pub(crate) const POSTGRES_FENCE_ARTIFACT_VERSION: u32 = 4;
const DML_FUNCTION_BODY: &str = "BEGIN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source is protected by sql-splitter migration write fence'; END";
const DDL_FUNCTION_BODY: &str = "BEGIN IF EXISTS (SELECT 1 FROM sql_splitter_migration_fence.registry WHERE singleton AND state = 'Released' AND admin_role = session_user) THEN RETURN; END IF; RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'source DDL is protected by sql-splitter migration write fence'; END";
const HISTORY_FUNCTION_BODY: &str = "BEGIN RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'sql-splitter migration fence history is immutable'; END";

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
    #[serde(default)]
    pub format_version: u32,
    pub token: FenceToken,
    pub evidence: ConsistencyEvidence,
    #[serde(default)]
    pub admin_tls_binding: Option<String>,
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
    #[serde(default)]
    pub history_function_oid: u32,
    #[serde(default)]
    pub history_trigger_oid: u32,
    pub dml_function_oid: u32,
    pub ddl_function_oid: u32,
    pub event_trigger_oid: u32,
    pub tables: Vec<FencedTable>,
    #[serde(default)]
    pub sequences: Vec<FencedSequence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedTable {
    pub namespace: String,
    pub table: String,
    pub relation_oid: u32,
    pub trigger_oid: u32,
    pub trigger_name: String,
    /// PostgreSQL `pg_class.relkind` (`r` for a stored table, `p` for a
    /// partitioned table).
    #[serde(default)]
    pub relation_kind: String,
    #[serde(default)]
    pub is_partition: bool,
    #[serde(default)]
    pub parent_relation_oid: Option<u32>,
    /// Exact deparsed partition strategy (`r`, `l`, or `h`) for partitioned
    /// relations. The migration catalog separately owns the typed form.
    #[serde(default)]
    pub partition_strategy: Option<String>,
    #[serde(default)]
    pub partition_key_definition: Option<String>,
    #[serde(default)]
    pub partition_bound: Option<String>,
}

/// Exact source sequence contract protected by one fence generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedSequence {
    pub namespace: String,
    pub sequence: String,
    pub relation_oid: u32,
    pub original_owner_oid: u32,
    pub original_owner: String,
    pub original_acl_is_null: bool,
    pub original_acl: Vec<FencedSequenceGrant>,
    pub data_type: String,
    pub start_value: i64,
    pub increment: i64,
    pub minimum_value: i64,
    pub maximum_value: i64,
    pub cache_size: i64,
    pub cycle: bool,
    pub ownership: Option<FencedSequenceOwnership>,
    pub last_value: i64,
    pub is_called: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedSequenceGrant {
    /// `None` means PUBLIC.
    pub grantee: Option<String>,
    pub grantor: String,
    pub privilege: String,
    pub grantable: bool,
}

/// The table column dependency that makes a sequence owner follow its table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencedSequenceOwnership {
    pub table_namespace: String,
    pub table: String,
    pub table_oid: u32,
    pub column: String,
    pub column_number: i16,
    pub dependency_type: String,
    pub original_table_owner_oid: u32,
    pub original_table_owner: String,
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
    #[error("reviewed plan contains no copied PostgreSQL tables")]
    NoTables,
    #[error("credential environment variable {0} is not set or is not Unicode")]
    MissingCredential(String),
    #[error("PostgreSQL fence operation failed")]
    Database(#[from] postgres::Error),
    #[error("fence metadata serialization failed")]
    Serialization(#[from] serde_json::Error),
    #[error("a prepared transaction exists in the source database")]
    PreparedTransaction,
    #[error("source table {namespace}.{table} is absent or is not fenceable")]
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
    reviewed.plan.validate_for_execution()?;
    if admin.tls.insecure {
        return Err(PostgresFenceError::Attestation(
            "fence administration requires authenticated TLS",
        ));
    }
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
    let storage = inspect_install_storage(&mut transaction)?;
    let admin_role: String = transaction.query_one("SELECT current_user", &[])?.get(0);
    let require_superuser = match reviewed.plan.postgres_source_profile.as_ref() {
        Some(PostgresSourceProfileContract::SelfManagedAdministrator {
            probe_artifact, ..
        })
        | Some(PostgresSourceProfileContract::ManagedAdministrator { probe_artifact, .. }) => {
            if probe_artifact.administrator_role != admin_role {
                return Err(PostgresFenceError::Attestation(
                    "fence administrator role differs from reviewed probe evidence",
                ));
            }
            false
        }
        Some(PostgresSourceProfileContract::AttestedExternalQuiesce { .. }) => {
            return Err(PostgresFenceError::WrongConsistencyMode);
        }
        None => true,
    };
    let mut inventory =
        lock_and_resolve_tables(&mut transaction, &tables, &generation, &admin_role)?;
    inventory.sequences = inventory_business_sequences(&mut transaction)?;
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

    match storage {
        FenceInstallStorage::Fresh => install_protected_registry(&mut transaction)?,
        FenceInstallStorage::Rearm => prepare_rearm_registry(&mut transaction, &admin_role, false)?,
        FenceInstallStorage::RearmLegacy => {
            prepare_rearm_registry(&mut transaction, &admin_role, true)?
        }
    }
    install_guard_functions(&mut transaction)?;
    install_table_guards(&mut transaction, &inventory)?;
    protect_business_sequences(&mut transaction, &mut inventory, require_superuser)?;
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
        format_version: POSTGRES_FENCE_ARTIFACT_VERSION,
        token,
        admin_tls_binding: Some(postgres_tls_binding(admin).map_err(PostgresFenceError::Catalog)?),
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
    drain_non_migration_sessions(&mut client)?;
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

fn drain_non_migration_sessions(client: &mut Client) -> Result<(), PostgresFenceError> {
    let rows = client.query(
        "SELECT pid FROM pg_stat_activity WHERE datid = (SELECT oid FROM pg_database WHERE datname=current_database()) AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
        &[],
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
            "SELECT count(*) FROM pg_stat_activity WHERE datid = (SELECT oid FROM pg_database WHERE datname=current_database()) AND pid <> pg_backend_pid() AND backend_type = 'client backend'",
            &[],
        )?
        .get(0);
    if remaining != 0 {
        return Err(PostgresFenceError::Attestation(
            "a non-migration source session remains connected",
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
    normalize_sequence_fence_changes(catalog, unsupported, inventory)?;
    let namespace_position = catalog
        .namespaces
        .iter()
        .position(|namespace| {
            namespace.name.as_str() == FENCE_SCHEMA
                && namespace.id == format!("namespace:{}", inventory.schema_oid)
        })
        .ok_or(PostgresFenceError::Attestation(
            "attested reserved namespace is absent from the catalog",
        ))?;
    let mut removed_ids = catalog.namespaces[namespace_position]
        .objects
        .iter()
        .map(|object| object.id.clone())
        .collect::<BTreeSet<_>>();
    removed_ids.insert(format!("namespace:{}", inventory.schema_oid));
    catalog.namespaces.remove(namespace_position);

    let trigger_ids = inventory
        .tables
        .iter()
        .map(|table| format!("trigger:{}", table.trigger_oid))
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

    let exact_unsupported_ids = fence_unsupported_ids(inventory);
    unsupported.objects.retain(|object| {
        !removed_ids.contains(&object.object_id)
            && !exact_unsupported_ids.contains(&object.object_id)
    });
    Ok(())
}

fn normalize_sequence_fence_changes(
    catalog: &mut VendorCatalog,
    unsupported: &mut UnsupportedObjectReport,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    for sequence in &inventory.sequences {
        let sequence_id = format!("relation:{}", sequence.relation_oid);
        let object = catalog
            .namespaces
            .iter_mut()
            .flat_map(|namespace| namespace.objects.iter_mut())
            .find(|object| object.id == sequence_id && object.kind == CatalogObjectKind::Sequence)
            .ok_or(PostgresFenceError::Attestation(
                "an attested business sequence is absent from the catalog",
            ))?;
        object.attributes.insert(
            "owner".into(),
            serde_json::Value::String(sequence.original_owner.clone()),
        );
        if let Some(ownership) = &sequence.ownership {
            let table_id = format!("relation:{}", ownership.table_oid);
            let table = catalog
                .namespaces
                .iter_mut()
                .flat_map(|namespace| namespace.objects.iter_mut())
                .find(|object| object.id == table_id && object.kind == CatalogObjectKind::Table)
                .ok_or(PostgresFenceError::Attestation(
                    "an attested sequence owning table is absent from the catalog",
                ))?;
            table.attributes.insert(
                "owner".into(),
                serde_json::Value::String(ownership.original_table_owner.clone()),
            );
        }
        if sequence.original_acl_is_null {
            unsupported.objects.retain(|object| {
                object.object_id != format!("relation-acl:{}", sequence.relation_oid)
            });
        }
    }
    Ok(())
}

fn fence_unsupported_ids(inventory: &FenceInventory) -> BTreeSet<String> {
    let mut ids = [
        format!("namespace-acl:{}", inventory.schema_oid),
        format!("relation-acl:{}", inventory.registry_oid),
        format!("relation-acl:{}", inventory.history_oid),
        format!("relation-acl:{}", inventory.history_sequence_oid),
        format!("routine-acl:{}", inventory.dml_function_oid),
        format!("routine-acl:{}", inventory.ddl_function_oid),
        format!("routine-acl:{}", inventory.history_function_oid),
        format!("event-trigger:{}", inventory.event_trigger_oid),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    ids.extend(
        inventory
            .tables
            .iter()
            .map(|table| format!("partition-trigger:{}", table.trigger_oid)),
    );
    ids
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
    if registry.format_version != FENCE_FORMAT_VERSION {
        return Err(PostgresFenceError::Attestation(
            "active registry format does not prove current fence coverage",
        ));
    }
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

/// Return whether exact fence evidence has reached its atomic Released state.
pub fn postgres_write_fence_is_released(
    admin: &PostgresEndpointConfig,
    evidence: &ConsistencyEvidence,
) -> Result<bool, PostgresFenceError> {
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
    if registry.state != "Released" {
        transaction.commit()?;
        return Ok(false);
    }
    let history: Vec<String> = transaction
        .query(
            &format!(
                "SELECT state FROM {}.{} WHERE generation=$1 ORDER BY sequence",
                quote_ident(FENCE_SCHEMA),
                quote_ident(HISTORY_TABLE)
            ),
            &[generation],
        )?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if history != ["Draining", "Active", "Released"] && history != ["Draining", "Released"] {
        return Err(PostgresFenceError::Attestation(
            "released fence history is invalid",
        ));
    }
    validate_all_released_history(&mut transaction, &registry.generation)?;
    attest_released_storage(&mut transaction, &registry)?;
    let remaining_guards: i64 = transaction
        .query_one(
            "SELECT (SELECT count(*) FROM pg_event_trigger WHERE evtname=$1) + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$2 AND p.proname = ANY($3)) + (SELECT count(*) FROM pg_trigger WHERE oid = ANY($4))",
            &[&DDL_TRIGGER, &FENCE_SCHEMA, &&[DML_FUNCTION, DDL_FUNCTION][..], &&registry.inventory.tables.iter().map(|table| table.trigger_oid).collect::<Vec<_>>()[..]],
        )?
        .get(0);
    if remaining_guards != 0 {
        return Err(PostgresFenceError::Attestation(
            "released fence retains active guard objects",
        ));
    }
    transaction.commit()?;
    Ok(true)
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
            validate_all_released_history(&mut transaction, &registry.generation)?;
            attest_released_storage(&mut transaction, &registry)?;
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
    restore_business_sequences(&mut transaction, &registry.inventory)?;
    transaction.commit()?;
    Ok(())
}

#[derive(Debug)]
struct Registry {
    format_version: i32,
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
    if config.tls.insecure {
        return Err(PostgresFenceError::Attestation(
            "fence administration requires authenticated TLS",
        ));
    }
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
    Ok(postgres.connect(postgres_tls_connector(config).map_err(PostgresFenceError::Catalog)?)?)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FenceInstallStorage {
    Fresh,
    Rearm,
    RearmLegacy,
}

fn inspect_install_storage(
    transaction: &mut Transaction<'_>,
) -> Result<FenceInstallStorage, PostgresFenceError> {
    let row = transaction.query_one(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=$1), to_regclass($2) IS NOT NULL, to_regclass($3) IS NOT NULL",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
        ],
    )?;
    let schema_exists: bool = row.get(0);
    let registry_exists: bool = row.get(1);
    let history_exists: bool = row.get(2);
    if !schema_exists && !registry_exists && !history_exists {
        return Ok(FenceInstallStorage::Fresh);
    }
    if !schema_exists || !registry_exists || !history_exists {
        return Err(PostgresFenceError::Attestation(
            "prior fence storage is incomplete",
        ));
    }
    let registry = load_registry_for_update(transaction)?;
    if registry.state != "Released" {
        return Err(PostgresFenceError::AlreadyInstalled);
    }
    let history_storage = history_guard_storage(&registry.inventory)?;
    let expected_inventory_fingerprint = match (registry.format_version, history_storage) {
        (FENCE_FORMAT_VERSION, HistoryGuardStorage::Current) => registry.inventory.fingerprint()?,
        (2, HistoryGuardStorage::Current) => {
            pre_partition_inventory_fingerprint(&registry.inventory)?
        }
        (1, HistoryGuardStorage::Current) => {
            pre_sequence_inventory_fingerprint(&registry.inventory)?
        }
        (1, HistoryGuardStorage::Legacy) => legacy_inventory_fingerprint(&registry.inventory)?,
        _ => {
            return Err(PostgresFenceError::Attestation(
                "released registry format is unsupported",
            ));
        }
    };
    if registry.inventory.generation != registry.generation
        || expected_inventory_fingerprint != registry.inventory_fingerprint
    {
        return Err(PostgresFenceError::Attestation(
            "released registry inventory is malformed",
        ));
    }
    transaction.batch_execute(&format!(
        "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
        quote_ident(FENCE_SCHEMA),
        quote_ident(HISTORY_TABLE)
    ))?;
    validate_all_released_history(transaction, &registry.generation)?;
    match history_storage {
        HistoryGuardStorage::Current => {
            attest_released_storage(transaction, &registry)?;
            Ok(FenceInstallStorage::Rearm)
        }
        HistoryGuardStorage::Legacy => {
            attest_legacy_released_storage(transaction, &registry)?;
            Ok(FenceInstallStorage::RearmLegacy)
        }
    }
}

fn legacy_inventory_fingerprint(inventory: &FenceInventory) -> Result<String, PostgresFenceError> {
    let tables = prior_fenced_tables(&inventory.tables);
    #[derive(Serialize)]
    struct LegacyInventory<'a> {
        generation: &'a str,
        admin_role: &'a str,
        schema_oid: u32,
        registry_oid: u32,
        history_oid: u32,
        history_sequence_oid: u32,
        dml_function_oid: u32,
        ddl_function_oid: u32,
        event_trigger_oid: u32,
        tables: &'a [PriorFencedTable<'a>],
    }
    let legacy = LegacyInventory {
        generation: &inventory.generation,
        admin_role: &inventory.admin_role,
        schema_oid: inventory.schema_oid,
        registry_oid: inventory.registry_oid,
        history_oid: inventory.history_oid,
        history_sequence_oid: inventory.history_sequence_oid,
        dml_function_oid: inventory.dml_function_oid,
        ddl_function_oid: inventory.ddl_function_oid,
        event_trigger_oid: inventory.event_trigger_oid,
        tables: &tables,
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&legacy)?)))
}

fn pre_sequence_inventory_fingerprint(
    inventory: &FenceInventory,
) -> Result<String, PostgresFenceError> {
    let tables = prior_fenced_tables(&inventory.tables);
    #[derive(Serialize)]
    struct PreSequenceInventory<'a> {
        generation: &'a str,
        admin_role: &'a str,
        schema_oid: u32,
        registry_oid: u32,
        history_oid: u32,
        history_sequence_oid: u32,
        history_function_oid: u32,
        history_trigger_oid: u32,
        dml_function_oid: u32,
        ddl_function_oid: u32,
        event_trigger_oid: u32,
        tables: &'a [PriorFencedTable<'a>],
    }
    let prior = PreSequenceInventory {
        generation: &inventory.generation,
        admin_role: &inventory.admin_role,
        schema_oid: inventory.schema_oid,
        registry_oid: inventory.registry_oid,
        history_oid: inventory.history_oid,
        history_sequence_oid: inventory.history_sequence_oid,
        history_function_oid: inventory.history_function_oid,
        history_trigger_oid: inventory.history_trigger_oid,
        dml_function_oid: inventory.dml_function_oid,
        ddl_function_oid: inventory.ddl_function_oid,
        event_trigger_oid: inventory.event_trigger_oid,
        tables: &tables,
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&prior)?)))
}

fn pre_partition_inventory_fingerprint(
    inventory: &FenceInventory,
) -> Result<String, PostgresFenceError> {
    let tables = prior_fenced_tables(&inventory.tables);
    #[derive(Serialize)]
    struct PrePartitionInventory<'a> {
        generation: &'a str,
        admin_role: &'a str,
        schema_oid: u32,
        registry_oid: u32,
        history_oid: u32,
        history_sequence_oid: u32,
        history_function_oid: u32,
        history_trigger_oid: u32,
        dml_function_oid: u32,
        ddl_function_oid: u32,
        event_trigger_oid: u32,
        tables: &'a [PriorFencedTable<'a>],
        sequences: &'a [FencedSequence],
    }
    let prior = PrePartitionInventory {
        generation: &inventory.generation,
        admin_role: &inventory.admin_role,
        schema_oid: inventory.schema_oid,
        registry_oid: inventory.registry_oid,
        history_oid: inventory.history_oid,
        history_sequence_oid: inventory.history_sequence_oid,
        history_function_oid: inventory.history_function_oid,
        history_trigger_oid: inventory.history_trigger_oid,
        dml_function_oid: inventory.dml_function_oid,
        ddl_function_oid: inventory.ddl_function_oid,
        event_trigger_oid: inventory.event_trigger_oid,
        tables: &tables,
        sequences: &inventory.sequences,
    };
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&prior)?)))
}

#[derive(Serialize)]
struct PriorFencedTable<'a> {
    namespace: &'a str,
    table: &'a str,
    relation_oid: u32,
    trigger_oid: u32,
    trigger_name: &'a str,
}

fn prior_fenced_tables(tables: &[FencedTable]) -> Vec<PriorFencedTable<'_>> {
    tables
        .iter()
        .map(|table| PriorFencedTable {
            namespace: &table.namespace,
            table: &table.table,
            relation_oid: table.relation_oid,
            trigger_oid: table.trigger_oid,
            trigger_name: &table.trigger_name,
        })
        .collect()
}

fn prepare_rearm_registry(
    transaction: &mut Transaction<'_>,
    admin_role: &str,
    install_legacy_history_guard: bool,
) -> Result<(), PostgresFenceError> {
    let registry = load_registry_for_update(transaction)?;
    if registry.state != "Released" || registry.inventory.admin_role != admin_role {
        return Err(PostgresFenceError::Attestation(
            "released fence owner differs from the new fence administrator",
        ));
    }
    if install_legacy_history_guard {
        install_immutable_history_guard(transaction)?;
    }
    let deleted = transaction.execute(
        &format!(
            "DELETE FROM {}.{} WHERE singleton AND generation=$1 AND state='Released'",
            quote_ident(FENCE_SCHEMA),
            quote_ident(REGISTRY_TABLE)
        ),
        &[&registry.generation],
    )?;
    if deleted != 1 {
        return Err(PostgresFenceError::Attestation(
            "released registry changed during rearm",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryGuardStorage {
    Legacy,
    Current,
}

fn history_guard_storage(
    inventory: &FenceInventory,
) -> Result<HistoryGuardStorage, PostgresFenceError> {
    match (
        inventory.history_function_oid,
        inventory.history_trigger_oid,
    ) {
        (0, 0) => Ok(HistoryGuardStorage::Legacy),
        (function, trigger) if function != 0 && trigger != 0 => Ok(HistoryGuardStorage::Current),
        _ => Err(PostgresFenceError::Attestation(
            "released history guard identity is incomplete",
        )),
    }
}

fn validate_all_released_history(
    transaction: &mut Transaction<'_>,
    current_generation: &str,
) -> Result<(), PostgresFenceError> {
    let rows = transaction.query(
        &format!(
            "SELECT generation, state FROM {}.{} ORDER BY sequence",
            quote_ident(FENCE_SCHEMA),
            quote_ident(HISTORY_TABLE)
        ),
        &[],
    )?;
    let transitions = rows
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    validate_released_transitions(&transitions, current_generation)
}

fn validate_released_transitions(
    transitions: &[(String, String)],
    current_generation: &str,
) -> Result<(), PostgresFenceError> {
    if transitions.is_empty() {
        return Err(PostgresFenceError::Attestation(
            "released fence has no history",
        ));
    }
    let mut generations = Vec::<(&str, Vec<&str>)>::new();
    let mut seen = BTreeSet::new();
    for (generation, state) in transitions {
        if generations
            .last()
            .is_none_or(|(previous, _)| *previous != generation)
        {
            if !seen.insert(generation.as_str()) {
                return Err(PostgresFenceError::Attestation(
                    "fence history generation is not contiguous",
                ));
            }
            generations.push((generation, Vec::new()));
        }
        if let Some((_, states)) = generations.last_mut() {
            states.push(state);
        }
    }
    if generations.last().map(|(generation, _)| *generation) != Some(current_generation) {
        return Err(PostgresFenceError::Attestation(
            "released registry is not the latest history generation",
        ));
    }
    if generations.iter().any(|(_, states)| {
        states.as_slice() != ["Draining", "Active", "Released"]
            && states.as_slice() != ["Draining", "Released"]
    }) {
        return Err(PostgresFenceError::Attestation(
            "prior fence history contains an invalid transition sequence",
        ));
    }
    Ok(())
}

fn attest_released_storage(
    transaction: &mut Transaction<'_>,
    registry: &Registry,
) -> Result<(), PostgresFenceError> {
    attest_owners_and_acl(transaction, &registry.inventory.admin_role)?;
    attest_restored_sequences(transaction, &registry.inventory)?;
    let row = transaction.query_one(
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4)::oid::bigint, (SELECT oid::bigint FROM pg_trigger WHERE tgrelid=to_regclass($3) AND tgname=$5 AND NOT tgisinternal) FROM pg_namespace n WHERE n.nspname=$1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_FUNCTION}()"),
            &HISTORY_TRIGGER,
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
        registry.inventory.schema_oid,
        registry.inventory.registry_oid,
        registry.inventory.history_oid,
        registry.inventory.history_sequence_oid,
        registry.inventory.history_function_oid,
        registry.inventory.history_trigger_oid,
    ];
    if observed != expected {
        return Err(PostgresFenceError::Attestation(
            "released audit storage identity differs",
        ));
    }
    let history_guard: i64 = transaction
        .query_one(
            "SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.oid=$1::oid AND t.tgrelid=$2::oid AND t.tgenabled='A' AND (t.tgtype & 2) <> 0 AND (t.tgtype & 4) = 0 AND (t.tgtype & 8) <> 0 AND (t.tgtype & 16) <> 0 AND (t.tgtype & 32) <> 0 AND (t.tgtype & 1) = 0 AND p.oid=$3::oid AND p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND p.prosrc=$4 AND p.proowner::regrole::text=$5 AND NOT EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f'::\"char\", p.proowner))) a WHERE a.grantee <> p.proowner)",
            &[
                &registry.inventory.history_trigger_oid,
                &registry.inventory.history_oid,
                &registry.inventory.history_function_oid,
                &HISTORY_FUNCTION_BODY,
                &registry.inventory.admin_role,
            ],
        )?
        .get(0);
    if history_guard != 1 {
        return Err(PostgresFenceError::Attestation(
            "immutable history guard differs",
        ));
    }
    let remaining_guards: i64 = transaction
        .query_one(
            "SELECT (SELECT count(*) FROM pg_event_trigger WHERE evtname=$1) + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$2 AND p.proname = ANY($3)) + (SELECT count(*) FROM pg_trigger WHERE oid = ANY($4))",
            &[
                &DDL_TRIGGER,
                &FENCE_SCHEMA,
                &&[DML_FUNCTION, DDL_FUNCTION][..],
                &&registry
                    .inventory
                    .tables
                    .iter()
                    .map(|table| table.trigger_oid)
                    .collect::<Vec<_>>()[..],
            ],
        )?
        .get(0);
    if remaining_guards != 0 {
        return Err(PostgresFenceError::Attestation(
            "released fence retains guard objects",
        ));
    }
    Ok(())
}

fn attest_legacy_released_storage(
    transaction: &mut Transaction<'_>,
    registry: &Registry,
) -> Result<(), PostgresFenceError> {
    attest_owners_and_acl(transaction, &registry.inventory.admin_role)?;
    let row = transaction.query_one(
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4) IS NULL, NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass($3) AND tgname=$5 AND NOT tgisinternal) FROM pg_namespace n WHERE n.nspname=$1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_FUNCTION}()"),
            &HISTORY_TRIGGER,
        ],
    )?;
    let observed = [
        oid_from_i64(row.get(0))?,
        oid_from_i64(row.get(1))?,
        oid_from_i64(row.get(2))?,
        oid_from_i64(row.get(3))?,
    ];
    let expected = [
        registry.inventory.schema_oid,
        registry.inventory.registry_oid,
        registry.inventory.history_oid,
        registry.inventory.history_sequence_oid,
    ];
    if observed != expected || !row.get::<_, bool>(4) || !row.get::<_, bool>(5) {
        return Err(PostgresFenceError::Attestation(
            "legacy released audit storage differs",
        ));
    }
    attest_no_released_migration_guards(transaction, registry)
}

fn attest_no_released_migration_guards(
    transaction: &mut Transaction<'_>,
    registry: &Registry,
) -> Result<(), PostgresFenceError> {
    let remaining_guards: i64 = transaction
        .query_one(
            "SELECT (SELECT count(*) FROM pg_event_trigger WHERE evtname=$1) + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$2 AND p.proname = ANY($3)) + (SELECT count(*) FROM pg_trigger WHERE oid = ANY($4))",
            &[
                &DDL_TRIGGER,
                &FENCE_SCHEMA,
                &&[DML_FUNCTION, DDL_FUNCTION][..],
                &&registry
                    .inventory
                    .tables
                    .iter()
                    .map(|table| table.trigger_oid)
                    .collect::<Vec<_>>()[..],
            ],
        )?
        .get(0);
    if remaining_guards != 0 {
        return Err(PostgresFenceError::Attestation(
            "released fence retains guard objects",
        ));
    }
    Ok(())
}

fn lock_and_resolve_tables(
    transaction: &mut Transaction<'_>,
    tables: &[QualifiedTable],
    generation: &str,
    admin_role: &str,
) -> Result<FenceInventory, PostgresFenceError> {
    let mut roots = BTreeMap::new();
    for table in tables {
        let row = transaction
            .query_opt(
                "SELECT root.oid::bigint, rn.nspname, root.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL (SELECT CASE WHEN c.relispartition THEN pg_partition_root(c.oid) ELSE c.oid END AS oid) selected JOIN pg_class root ON root.oid=selected.oid JOIN pg_namespace rn ON rn.oid=root.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p') AND root.relkind IN ('r','p')",
                &[&table.namespace.as_str(), &table.name.as_str()],
            )?
            .ok_or_else(|| PostgresFenceError::InvalidSourceTable {
                namespace: table.namespace.to_string(),
                table: table.name.to_string(),
            })?;
        roots.insert(
            oid_from_i64(row.get(0))?,
            (row.get::<_, String>(1), row.get::<_, String>(2)),
        );
    }

    let mut by_oid = BTreeMap::new();
    for (root_oid, (namespace, table)) in roots {
        transaction.batch_execute(&format!(
            "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
            quote_ident(&namespace),
            quote_ident(&table)
        ))?;
        let kind: String = transaction
            .query_one(
                "SELECT relkind::text FROM pg_class WHERE oid=$1::oid",
                &[&root_oid],
            )?
            .get(0);
        let rows = if kind == "p" {
            transaction.query(
                "SELECT tree.relid::bigint, tree.parentrelid::bigint, n.nspname, c.relname, c.relkind::text, c.relispartition, pt.partstrat::text, CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END, CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid, true) END FROM pg_partition_tree($1::oid) tree JOIN pg_class c ON c.oid=tree.relid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_partitioned_table pt ON pt.partrelid=c.oid ORDER BY tree.relid",
                &[&root_oid],
            )?
        } else {
            transaction.query(
                "SELECT c.oid::bigint, NULL::bigint, n.nspname, c.relname, c.relkind::text, c.relispartition, NULL::text, NULL::text, NULL::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1::oid",
                &[&root_oid],
            )?
        };
        for row in rows {
            let relation_oid = oid_from_i64(row.get(0))?;
            let parent_relation_oid = row.get::<_, Option<i64>>(1).map(oid_from_i64).transpose()?;
            let observed = FencedTable {
                namespace: row.get(2),
                table: row.get(3),
                relation_oid,
                trigger_oid: 0,
                trigger_name: String::new(),
                relation_kind: row.get(4),
                is_partition: row.get(5),
                parent_relation_oid,
                partition_strategy: row.get(6),
                partition_key_definition: row.get(7),
                partition_bound: row.get(8),
            };
            if by_oid.insert(relation_oid, observed).is_some() {
                return Err(PostgresFenceError::Attestation(
                    "planned partition trees overlap",
                ));
            }
        }
    }
    let mut inventory = by_oid.into_values().collect::<Vec<_>>();
    for (index, table) in inventory.iter_mut().enumerate() {
        let qualified = QualifiedTable {
            namespace: super::model::Identifier::new(table.namespace.clone())
                .map_err(|_| PostgresFenceError::Attestation("invalid live namespace"))?,
            name: super::model::Identifier::new(table.table.clone())
                .map_err(|_| PostgresFenceError::Attestation("invalid live table"))?,
        };
        table.trigger_name = trigger_name(index, &qualified);
    }
    let result = FenceInventory {
        generation: generation.to_owned(),
        admin_role: admin_role.to_owned(),
        schema_oid: 0,
        registry_oid: 0,
        history_oid: 0,
        history_sequence_oid: 0,
        history_function_oid: 0,
        history_trigger_oid: 0,
        dml_function_oid: 0,
        ddl_function_oid: 0,
        event_trigger_oid: 0,
        tables: inventory,
        sequences: Vec::new(),
    };
    validate_table_topology_inventory(&result)?;
    Ok(result)
}

fn inventory_business_sequences(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<FencedSequence>, PostgresFenceError> {
    let rows = transaction.query(
        "SELECT s.oid::bigint, sn.nspname, s.relname, s.relowner::bigint, pg_get_userbyid(s.relowner), s.relacl IS NULL, pg_catalog.format_type(q.seqtypid, NULL), q.seqstart, q.seqincrement, q.seqmin, q.seqmax, q.seqcache, q.seqcycle FROM pg_class s JOIN pg_namespace sn ON sn.oid=s.relnamespace JOIN pg_sequence q ON q.seqrelid=s.oid WHERE s.relkind='S' AND sn.nspname <> 'information_schema' AND sn.nspname !~ '^pg_' AND sn.nspname <> $1 ORDER BY sn.nspname, s.relname, s.oid",
        &[&FENCE_SCHEMA],
    )?;
    let mut sequences = Vec::with_capacity(rows.len());
    for row in rows {
        let relation_oid = oid_from_i64(row.get(0))?;
        let ownership_rows = transaction.query(
            "SELECT tn.nspname, t.relname, t.oid::bigint, a.attname, a.attnum, d.deptype::text, t.relowner::bigint, pg_get_userbyid(t.relowner) FROM pg_depend d JOIN pg_class t ON t.oid=d.refobjid JOIN pg_namespace tn ON tn.oid=t.relnamespace JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid WHERE d.classid='pg_class'::regclass AND d.objid=$1::oid AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') ORDER BY t.oid, a.attnum",
            &[&relation_oid],
        )?;
        let ownership = match ownership_rows.as_slice() {
            [] => None,
            [ownership] => Some(FencedSequenceOwnership {
                table_namespace: ownership.get(0),
                table: ownership.get(1),
                table_oid: oid_from_i64(ownership.get(2))?,
                column: ownership.get(3),
                column_number: ownership.get(4),
                dependency_type: ownership.get(5),
                original_table_owner_oid: oid_from_i64(ownership.get(6))?,
                original_table_owner: ownership.get(7),
            }),
            _ => {
                return Err(PostgresFenceError::Attestation(
                    "a business sequence has multiple ownership links",
                ));
            }
        };
        let grants: Vec<FencedSequenceGrant> = transaction
            .query(
                "SELECT CASE WHEN a.grantee=0 THEN NULL ELSE pg_get_userbyid(a.grantee) END, pg_get_userbyid(a.grantor), a.privilege_type, a.is_grantable FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('S'::\"char\", c.relowner))) a WHERE c.oid=$1::oid ORDER BY a.grantee, a.grantor, a.privilege_type, a.is_grantable",
                &[&relation_oid],
            )?
            .into_iter()
            .map(|grant| FencedSequenceGrant {
                grantee: grant.get(0),
                grantor: grant.get(1),
                privilege: grant.get(2),
                grantable: grant.get(3),
            })
            .collect();
        if grants.iter().any(|grant: &FencedSequenceGrant| {
            grant.grantor != row.get::<_, String>(4)
                || !matches!(grant.privilege.as_str(), "SELECT" | "USAGE" | "UPDATE")
        }) {
            return Err(PostgresFenceError::Attestation(
                "a business sequence ACL has an unsupported delegated grantor or privilege",
            ));
        }
        sequences.push(FencedSequence {
            namespace: row.get(1),
            sequence: row.get(2),
            relation_oid,
            original_owner_oid: oid_from_i64(row.get(3))?,
            original_owner: row.get(4),
            original_acl_is_null: row.get(5),
            original_acl: grants,
            data_type: row.get(6),
            start_value: row.get(7),
            increment: row.get(8),
            minimum_value: row.get(9),
            maximum_value: row.get(10),
            cache_size: row.get(11),
            cycle: row.get(12),
            ownership,
            last_value: 0,
            is_called: false,
        });
    }
    Ok(sequences)
}

fn protect_business_sequences(
    transaction: &mut Transaction<'_>,
    inventory: &mut FenceInventory,
    require_superuser: bool,
) -> Result<(), PostgresFenceError> {
    let admin_is_superuser: bool = transaction
        .query_one(
            "SELECT rolsuper FROM pg_roles WHERE rolname=current_user",
            &[],
        )?
        .get(0);
    if require_superuser && !admin_is_superuser {
        return Err(PostgresFenceError::Attestation(
            "sequence fencing requires an explicit superuser administrator",
        ));
    }
    let inherited_admin: bool = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolcanlogin AND NOT r.rolsuper AND r.rolname <> current_user AND pg_has_role(r.oid, current_user, 'MEMBER'))",
            &[],
        )?
        .get(0);
    if inherited_admin {
        return Err(PostgresFenceError::Attestation(
            "a nontrusted login can assume the fence administrator role",
        ));
    }

    let mut transferred_tables = BTreeSet::new();
    for sequence in &inventory.sequences {
        if let Some(ownership) = &sequence.ownership {
            if transferred_tables.insert(ownership.table_oid) {
                transaction.batch_execute(&format!(
                    "ALTER TABLE {}.{} OWNER TO {}",
                    quote_ident(&ownership.table_namespace),
                    quote_ident(&ownership.table),
                    quote_ident(&inventory.admin_role),
                ))?;
            }
        } else {
            transaction.batch_execute(&format!(
                "ALTER SEQUENCE {}.{} OWNER TO {}",
                quote_ident(&sequence.namespace),
                quote_ident(&sequence.sequence),
                quote_ident(&inventory.admin_role),
            ))?;
        }
        revoke_all_sequence_grants(transaction, sequence)?;
    }
    attest_sequence_exclusion(transaction, inventory)?;
    for sequence in &mut inventory.sequences {
        let state = transaction.query_one(
            &format!(
                "SELECT last_value, is_called FROM {}.{}",
                quote_ident(&sequence.namespace),
                quote_ident(&sequence.sequence),
            ),
            &[],
        )?;
        sequence.last_value = state.get(0);
        sequence.is_called = state.get(1);
    }
    Ok(())
}

fn revoke_all_sequence_grants(
    transaction: &mut Transaction<'_>,
    sequence: &FencedSequence,
) -> Result<(), PostgresFenceError> {
    let qualified = format!(
        "{}.{}",
        quote_ident(&sequence.namespace),
        quote_ident(&sequence.sequence)
    );
    transaction.batch_execute(&format!(
        "REVOKE USAGE, UPDATE ON SEQUENCE {qualified} FROM PUBLIC"
    ))?;
    let grantees = sequence
        .original_acl
        .iter()
        .filter_map(|grant| grant.grantee.as_deref())
        .collect::<BTreeSet<_>>();
    for grantee in grantees {
        transaction.batch_execute(&format!(
            "REVOKE USAGE, UPDATE ON SEQUENCE {qualified} FROM {}",
            quote_ident(grantee)
        ))?;
    }
    Ok(())
}

fn attest_sequence_exclusion(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    let sequence_oids = inventory
        .sequences
        .iter()
        .map(|sequence| sequence.relation_oid)
        .collect::<Vec<_>>();
    if sequence_oids.is_empty() {
        return Ok(());
    }
    let bypass: bool = transaction
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_roles r CROSS JOIN unnest($1::oid[]) sequence_oid WHERE r.rolcanlogin AND NOT r.rolsuper AND r.rolname <> current_user AND has_sequence_privilege(r.oid, sequence_oid, 'USAGE,UPDATE'))",
            &[&&sequence_oids[..]],
        )?
        .get(0);
    if bypass {
        return Err(PostgresFenceError::Attestation(
            "a nontrusted login retains effective sequence write privilege",
        ));
    }
    Ok(())
}

fn restore_business_sequences(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    let mut restored_tables = BTreeSet::new();
    for sequence in &inventory.sequences {
        if let Some(ownership) = &sequence.ownership {
            if sequence.original_owner_oid != ownership.original_table_owner_oid
                || sequence.original_owner != ownership.original_table_owner
            {
                return Err(PostgresFenceError::Attestation(
                    "an owned sequence and its table had different original owners",
                ));
            }
            if restored_tables.insert(ownership.table_oid) {
                transaction.batch_execute(&format!(
                    "ALTER TABLE {}.{} OWNER TO {}",
                    quote_ident(&ownership.table_namespace),
                    quote_ident(&ownership.table),
                    quote_ident(&ownership.original_table_owner),
                ))?;
            }
        } else {
            transaction.batch_execute(&format!(
                "ALTER SEQUENCE {}.{} OWNER TO {}",
                quote_ident(&sequence.namespace),
                quote_ident(&sequence.sequence),
                quote_ident(&sequence.original_owner),
            ))?;
        }
    }
    for sequence in &inventory.sequences {
        restore_sequence_acl(transaction, sequence)?;
    }
    attest_restored_sequences(transaction, inventory)
}

fn restore_sequence_acl(
    transaction: &mut Transaction<'_>,
    sequence: &FencedSequence,
) -> Result<(), PostgresFenceError> {
    let qualified = format!(
        "{}.{}",
        quote_ident(&sequence.namespace),
        quote_ident(&sequence.sequence)
    );
    transaction.batch_execute(&format!(
        "SET LOCAL ROLE {}; REVOKE ALL PRIVILEGES ON SEQUENCE {qualified} FROM PUBLIC",
        quote_ident(&sequence.original_owner)
    ))?;
    let grantees = sequence
        .original_acl
        .iter()
        .filter_map(|grant| grant.grantee.as_deref())
        .filter(|grantee| *grantee != sequence.original_owner)
        .collect::<BTreeSet<_>>();
    for grantee in grantees {
        transaction.batch_execute(&format!(
            "REVOKE ALL PRIVILEGES ON SEQUENCE {qualified} FROM {}",
            quote_ident(grantee)
        ))?;
    }
    for grant in sequence.original_acl.iter().filter(|grant| {
        grant
            .grantee
            .as_deref()
            .is_none_or(|grantee| grantee != sequence.original_owner)
    }) {
        let grantee = grant
            .grantee
            .as_deref()
            .map_or_else(|| "PUBLIC".to_owned(), quote_ident);
        transaction.batch_execute(&format!(
            "GRANT {} ON SEQUENCE {qualified} TO {grantee}{}",
            grant.privilege,
            if grant.grantable {
                " WITH GRANT OPTION"
            } else {
                ""
            }
        ))?;
    }
    transaction.batch_execute("RESET ROLE")?;
    Ok(())
}

fn attest_restored_sequences(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    for sequence in &inventory.sequences {
        let row = transaction.query_one(
            "SELECT c.oid::bigint, c.relowner::bigint, pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='S'",
            &[&sequence.namespace, &sequence.sequence],
        )?;
        if oid_from_i64(row.get(0))? != sequence.relation_oid
            || oid_from_i64(row.get(1))? != sequence.original_owner_oid
            || row.get::<_, String>(2) != sequence.original_owner
        {
            return Err(PostgresFenceError::Attestation(
                "released sequence owner differs",
            ));
        }
        let actual = transaction
            .query(
                "SELECT CASE WHEN a.grantee=0 THEN NULL ELSE pg_get_userbyid(a.grantee) END, pg_get_userbyid(a.grantor), a.privilege_type, a.is_grantable FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('S'::\"char\", c.relowner))) a WHERE c.oid=$1::oid ORDER BY a.grantee, a.grantor, a.privilege_type, a.is_grantable",
                &[&sequence.relation_oid],
            )?
            .into_iter()
            .map(|grant| FencedSequenceGrant {
                grantee: grant.get(0),
                grantor: grant.get(1),
                privilege: grant.get(2),
                grantable: grant.get(3),
            })
            .collect::<Vec<_>>();
        if actual != sequence.original_acl {
            return Err(PostgresFenceError::Attestation(
                "released sequence ACL differs",
            ));
        }
        if let Some(ownership) = &sequence.ownership {
            let owner = transaction.query_one(
                "SELECT relowner::bigint, pg_get_userbyid(relowner) FROM pg_class WHERE oid=$1::oid",
                &[&ownership.table_oid],
            )?;
            if oid_from_i64(owner.get(0))? != ownership.original_table_owner_oid
                || owner.get::<_, String>(1) != ownership.original_table_owner
            {
                return Err(PostgresFenceError::Attestation(
                    "released sequence owning table owner differs",
                ));
            }
        }
    }
    Ok(())
}

fn resolve_protected_inventory(
    transaction: &mut Transaction<'_>,
    inventory: &mut FenceInventory,
) -> Result<(), PostgresFenceError> {
    let row = transaction.query_one(
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4)::oid::bigint, to_regprocedure($5)::oid::bigint, (SELECT oid::bigint FROM pg_event_trigger WHERE evtname = $6), to_regprocedure($7)::oid::bigint, (SELECT oid::bigint FROM pg_trigger WHERE tgrelid=to_regclass($3) AND tgname=$8 AND NOT tgisinternal) FROM pg_namespace n WHERE n.nspname = $1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{DML_FUNCTION}()"),
            &format!("{FENCE_SCHEMA}.{DDL_FUNCTION}()"),
            &DDL_TRIGGER,
            &format!("{FENCE_SCHEMA}.{HISTORY_FUNCTION}()"),
            &HISTORY_TRIGGER,
        ],
    )?;
    inventory.schema_oid = oid_from_i64(row.get(0))?;
    inventory.registry_oid = oid_from_i64(row.get(1))?;
    inventory.history_oid = oid_from_i64(row.get(2))?;
    inventory.history_sequence_oid = oid_from_i64(row.get(3))?;
    inventory.dml_function_oid = oid_from_i64(row.get(4))?;
    inventory.ddl_function_oid = oid_from_i64(row.get(5))?;
    inventory.event_trigger_oid = oid_from_i64(row.get(6))?;
    inventory.history_function_oid = oid_from_i64(row.get(7))?;
    inventory.history_trigger_oid = oid_from_i64(row.get(8))?;
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
    }
    attest_table_topology(transaction, inventory)?;
    Ok(())
}

fn attest_table_topology(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    validate_table_topology_inventory(inventory)?;
    let expected_oids = inventory
        .tables
        .iter()
        .map(|table| table.relation_oid)
        .collect::<BTreeSet<_>>();
    if expected_oids.len() != inventory.tables.len() {
        return Err(PostgresFenceError::Attestation(
            "fenced relation identities are duplicated",
        ));
    }
    for table in &inventory.tables {
        let row = transaction
            .query_opt(
                "SELECT n.nspname, c.relname, c.relkind::text, c.relispartition, parent.inhparent::bigint, pt.partstrat::text, CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END, CASE WHEN c.relispartition THEN pg_get_expr(c.relpartbound, c.oid, true) END FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN LATERAL (SELECT i.inhparent FROM pg_inherits i WHERE i.inhrelid=c.oid ORDER BY i.inhseqno LIMIT 1) parent ON true LEFT JOIN pg_partitioned_table pt ON pt.partrelid=c.oid WHERE c.oid=$1::oid AND c.relkind IN ('r','p')",
                &[&table.relation_oid],
            )?
            .ok_or(PostgresFenceError::Attestation(
                "a fenced relation identity is absent",
            ))?;
        let parent_relation_oid = row.get::<_, Option<i64>>(4).map(oid_from_i64).transpose()?;
        if row.get::<_, String>(0) != table.namespace
            || row.get::<_, String>(1) != table.table
            || row.get::<_, String>(2) != table.relation_kind
            || row.get::<_, bool>(3) != table.is_partition
            || parent_relation_oid != table.parent_relation_oid
            || row.get::<_, Option<String>>(5) != table.partition_strategy
            || row.get::<_, Option<String>>(6) != table.partition_key_definition
            || row.get::<_, Option<String>>(7) != table.partition_bound
        {
            return Err(PostgresFenceError::Attestation(
                "a fenced table or partition topology changed",
            ));
        }
        if table.is_partition != table.parent_relation_oid.is_some() {
            return Err(PostgresFenceError::Attestation(
                "partition parent evidence is inconsistent",
            ));
        }
        if table
            .parent_relation_oid
            .is_some_and(|parent| !expected_oids.contains(&parent))
        {
            return Err(PostgresFenceError::Attestation(
                "partition parent is outside the fence inventory",
            ));
        }
    }

    let roots = inventory
        .tables
        .iter()
        .filter(|table| table.relation_kind == "p" && table.parent_relation_oid.is_none());
    for root in roots {
        let observed = transaction
            .query(
                "SELECT relid::bigint FROM pg_partition_tree($1::oid) ORDER BY relid",
                &[&root.relation_oid],
            )?
            .into_iter()
            .map(|row| oid_from_i64(row.get(0)))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut expected = BTreeSet::from([root.relation_oid]);
        loop {
            let previous = expected.len();
            let additions = inventory
                .tables
                .iter()
                .filter_map(|table| {
                    table
                        .parent_relation_oid
                        .filter(|parent| expected.contains(parent))
                        .map(|_| table.relation_oid)
                })
                .collect::<Vec<_>>();
            expected.extend(additions);
            if expected.len() == previous {
                break;
            }
        }
        if observed != expected {
            return Err(PostgresFenceError::Attestation(
                "partition tree membership changed",
            ));
        }
    }
    Ok(())
}

fn validate_table_topology_inventory(inventory: &FenceInventory) -> Result<(), PostgresFenceError> {
    let by_oid = inventory
        .tables
        .iter()
        .map(|table| (table.relation_oid, table))
        .collect::<BTreeMap<_, _>>();
    let names = inventory
        .tables
        .iter()
        .map(|table| (&table.namespace, &table.table))
        .collect::<BTreeSet<_>>();
    if by_oid.len() != inventory.tables.len() || names.len() != inventory.tables.len() {
        return Err(PostgresFenceError::Attestation(
            "fenced relation identities are duplicated",
        ));
    }
    for table in &inventory.tables {
        if !matches!(table.relation_kind.as_str(), "r" | "p") {
            return Err(PostgresFenceError::Attestation(
                "fenced relation kind is unsupported",
            ));
        }
        if table.relation_kind == "p"
            && (table.partition_strategy.is_none() || table.partition_key_definition.is_none())
        {
            return Err(PostgresFenceError::Attestation(
                "partitioned table key evidence is incomplete",
            ));
        }
        if table.relation_kind == "r"
            && (table.partition_strategy.is_some() || table.partition_key_definition.is_some())
        {
            return Err(PostgresFenceError::Attestation(
                "stored table has partitioned-table key evidence",
            ));
        }
        if table.is_partition
            != (table.parent_relation_oid.is_some() && table.partition_bound.is_some())
        {
            return Err(PostgresFenceError::Attestation(
                "partition parent or bound evidence is inconsistent",
            ));
        }
        let mut current = table;
        let mut visited = BTreeSet::new();
        while let Some(parent_oid) = current.parent_relation_oid {
            if !visited.insert(current.relation_oid) {
                return Err(PostgresFenceError::Attestation(
                    "partition topology contains a cycle",
                ));
            }
            current = by_oid
                .get(&parent_oid)
                .copied()
                .ok_or(PostgresFenceError::Attestation(
                    "partition parent is outside the fence inventory",
                ))?;
        }
        if table.is_partition && current.relation_kind != "p" {
            return Err(PostgresFenceError::Attestation(
                "partition topology does not terminate at a partitioned root",
            ));
        }
    }
    Ok(())
}

fn install_protected_registry(transaction: &mut Transaction<'_>) -> Result<(), PostgresFenceError> {
    transaction.batch_execute(&format!(
        "CREATE SCHEMA {schema}; REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; CREATE TABLE {schema}.{registry} (singleton boolean PRIMARY KEY CHECK (singleton), format_version integer NOT NULL, generation text NOT NULL UNIQUE, token_hash text NOT NULL CHECK (length(token_hash) = 64), admin_role name NOT NULL, endpoint_identity text NOT NULL, database_oid oid NOT NULL, system_identifier text NOT NULL, business_catalog_fingerprint text NOT NULL, inventory_fingerprint text NOT NULL, inventory_json jsonb NOT NULL, activation_xid bigint NOT NULL, activated_at timestamptz NOT NULL, state text NOT NULL CHECK (state IN ('Draining', 'Active', 'Released'))); CREATE TABLE {schema}.{history} (sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, generation text NOT NULL, state text NOT NULL CHECK (state IN ('Draining', 'Active', 'Released')), recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(), recorded_by name NOT NULL DEFAULT current_user); CREATE FUNCTION {schema}.{history_function}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $body${history_body}$body$; REVOKE ALL ON FUNCTION {schema}.{history_function}() FROM PUBLIC; CREATE TRIGGER {history_trigger} BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.{history} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.{history_function}(); ALTER TABLE {schema}.{history} ENABLE ALWAYS TRIGGER {history_trigger}; REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM PUBLIC; REVOKE ALL ON ALL SEQUENCES IN SCHEMA {schema} FROM PUBLIC;",
        schema = quote_ident(FENCE_SCHEMA), registry = quote_ident(REGISTRY_TABLE), history = quote_ident(HISTORY_TABLE), history_function = quote_ident(HISTORY_FUNCTION), history_trigger = quote_ident(HISTORY_TRIGGER), history_body = HISTORY_FUNCTION_BODY
    ))?;
    Ok(())
}

fn install_immutable_history_guard(
    transaction: &mut Transaction<'_>,
) -> Result<(), PostgresFenceError> {
    transaction.batch_execute(&format!(
        "CREATE FUNCTION {schema}.{function}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $body${body}$body$; REVOKE ALL ON FUNCTION {schema}.{function}() FROM PUBLIC; CREATE TRIGGER {trigger} BEFORE UPDATE OR DELETE OR TRUNCATE ON {schema}.{history} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.{function}(); ALTER TABLE {schema}.{history} ENABLE ALWAYS TRIGGER {trigger};",
        schema = quote_ident(FENCE_SCHEMA), function = quote_ident(HISTORY_FUNCTION), trigger = quote_ident(HISTORY_TRIGGER), history = quote_ident(HISTORY_TABLE), body = HISTORY_FUNCTION_BODY
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
    let sql = format!("SELECT format_version, generation, token_hash, endpoint_identity, database_oid::bigint, system_identifier, business_catalog_fingerprint, inventory_fingerprint, activation_xid, activated_at::text, inventory_json::text, state FROM {}.{} WHERE singleton{suffix}", quote_ident(FENCE_SCHEMA), quote_ident(REGISTRY_TABLE));
    let row = transaction
        .query_opt(&sql, &[])?
        .ok_or(PostgresFenceError::NotInstalled)?;
    let state: String = row.get(11);
    let database_oid = u32::try_from(row.get::<_, i64>(4))
        .map_err(|_| PostgresFenceError::Attestation("database OID is out of range"))?;
    let activation_xid = u64::try_from(row.get::<_, i64>(8))
        .map_err(|_| PostgresFenceError::Attestation("activation transaction ID is negative"))?;
    Ok(Registry {
        format_version: row.get(0),
        generation: row.get(1),
        token_hash: row.get(2),
        endpoint_identity: row.get(3),
        database_oid,
        system_identifier: row.get(5),
        business_catalog_fingerprint: row.get(6),
        inventory_fingerprint: row.get(7),
        activation_xid,
        activated_at: row.get(9),
        inventory: serde_json::from_str(&row.get::<_, String>(10))?,
        state,
    })
}

fn attest_transaction(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
    token_hash: &str,
    expected_state: &str,
) -> Result<(), PostgresFenceError> {
    let (format_version, stored_fingerprint) = load_inventory_fingerprint(transaction)?;
    let observed_fingerprint = match format_version {
        FENCE_FORMAT_VERSION => inventory.fingerprint()?,
        2 => pre_partition_inventory_fingerprint(inventory)?,
        1 => match history_guard_storage(inventory)? {
            HistoryGuardStorage::Current => pre_sequence_inventory_fingerprint(inventory)?,
            HistoryGuardStorage::Legacy => legacy_inventory_fingerprint(inventory)?,
        },
        _ => {
            return Err(PostgresFenceError::Attestation(
                "registry format is unsupported",
            ));
        }
    };
    if observed_fingerprint != stored_fingerprint {
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
    if format_version >= 2 {
        attest_protected_sequences(transaction, inventory)?;
        attest_sequence_exclusion(transaction, inventory)?;
    }
    if format_version == FENCE_FORMAT_VERSION {
        attest_table_topology(transaction, inventory)?;
    }
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
        "SELECT n.oid::bigint, to_regclass($2)::oid::bigint, to_regclass($3)::oid::bigint, pg_get_serial_sequence($3, 'sequence')::regclass::oid::bigint, to_regprocedure($4)::oid::bigint, to_regprocedure($5)::oid::bigint, to_regprocedure($6)::oid::bigint, (SELECT oid::bigint FROM pg_trigger WHERE tgrelid=to_regclass($3) AND tgname=$7 AND NOT tgisinternal) FROM pg_namespace n WHERE n.nspname=$1",
        &[
            &FENCE_SCHEMA,
            &format!("{FENCE_SCHEMA}.{REGISTRY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{HISTORY_TABLE}"),
            &format!("{FENCE_SCHEMA}.{DML_FUNCTION}()"),
            &format!("{FENCE_SCHEMA}.{DDL_FUNCTION}()"),
            &format!("{FENCE_SCHEMA}.{HISTORY_FUNCTION}()"),
            &HISTORY_TRIGGER,
        ],
    )?;
    let observed = [
        oid_from_i64(row.get(0))?,
        oid_from_i64(row.get(1))?,
        oid_from_i64(row.get(2))?,
        oid_from_i64(row.get(3))?,
        oid_from_i64(row.get(4))?,
        oid_from_i64(row.get(5))?,
        oid_from_i64(row.get(6))?,
        oid_from_i64(row.get(7))?,
    ];
    let expected = [
        inventory.schema_oid,
        inventory.registry_oid,
        inventory.history_oid,
        inventory.history_sequence_oid,
        inventory.dml_function_oid,
        inventory.ddl_function_oid,
        inventory.history_function_oid,
        inventory.history_trigger_oid,
    ];
    if observed != expected {
        return Err(PostgresFenceError::Attestation(
            "protected object identity differs",
        ));
    }
    let history_guard: i64 = transaction.query_one(
        "SELECT count(*) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.oid=$1::oid AND t.tgrelid=$2::oid AND t.tgenabled='A' AND (t.tgtype & 2) <> 0 AND (t.tgtype & 4) = 0 AND (t.tgtype & 8) <> 0 AND (t.tgtype & 16) <> 0 AND (t.tgtype & 32) <> 0 AND (t.tgtype & 1) = 0 AND p.oid=$3::oid AND p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND p.prosrc=$4 AND p.proowner::regrole::text=$5 AND NOT EXISTS (SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f'::\"char\", p.proowner))) a WHERE a.grantee <> p.proowner)",
        &[&inventory.history_trigger_oid, &inventory.history_oid, &inventory.history_function_oid, &HISTORY_FUNCTION_BODY, &inventory.admin_role],
    )?.get(0);
    if history_guard != 1 {
        return Err(PostgresFenceError::Attestation(
            "immutable history guard differs",
        ));
    }
    let function_oids = [
        inventory.dml_function_oid,
        inventory.ddl_function_oid,
        inventory.history_function_oid,
    ];
    let valid_functions: i64 = transaction.query_one(
        "SELECT count(*) FROM pg_proc WHERE oid = ANY($1) AND prosecdef AND proconfig = ARRAY['search_path=pg_catalog']::text[]",
        &[&&function_oids[..]],
    )?.get(0);
    if valid_functions != 3 {
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
        (
            inventory.history_function_oid,
            (HISTORY_FUNCTION_BODY, "trigger"),
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

fn attest_protected_sequences(
    transaction: &mut Transaction<'_>,
    inventory: &FenceInventory,
) -> Result<(), PostgresFenceError> {
    let actual_count: i64 = transaction
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S' AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' AND n.nspname <> $1",
            &[&FENCE_SCHEMA],
        )?
        .get(0);
    if usize::try_from(actual_count).ok() != Some(inventory.sequences.len()) {
        return Err(PostgresFenceError::Attestation(
            "business sequence inventory count differs",
        ));
    }
    for sequence in &inventory.sequences {
        let row = transaction
            .query_opt(
                "SELECT c.oid::bigint, pg_get_userbyid(c.relowner), pg_catalog.format_type(q.seqtypid, NULL), q.seqstart, q.seqincrement, q.seqmin, q.seqmax, q.seqcache, q.seqcycle FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_sequence q ON q.seqrelid=c.oid WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind='S'",
                &[&sequence.namespace, &sequence.sequence],
            )?
            .ok_or(PostgresFenceError::Attestation(
                "an attested business sequence is absent",
            ))?;
        if oid_from_i64(row.get(0))? != sequence.relation_oid
            || row.get::<_, String>(1) != inventory.admin_role
            || row.get::<_, String>(2) != sequence.data_type
            || row.get::<_, i64>(3) != sequence.start_value
            || row.get::<_, i64>(4) != sequence.increment
            || row.get::<_, i64>(5) != sequence.minimum_value
            || row.get::<_, i64>(6) != sequence.maximum_value
            || row.get::<_, i64>(7) != sequence.cache_size
            || row.get::<_, bool>(8) != sequence.cycle
        {
            return Err(PostgresFenceError::Attestation(
                "business sequence identity or static configuration differs",
            ));
        }
        let state = transaction.query_one(
            &format!(
                "SELECT last_value, is_called FROM {}.{}",
                quote_ident(&sequence.namespace),
                quote_ident(&sequence.sequence),
            ),
            &[],
        )?;
        if state.get::<_, i64>(0) != sequence.last_value
            || state.get::<_, bool>(1) != sequence.is_called
        {
            return Err(PostgresFenceError::Attestation(
                "business sequence state changed after drain",
            ));
        }
        let links = transaction.query(
            "SELECT tn.nspname, t.relname, t.oid::bigint, a.attname, a.attnum, d.deptype::text, pg_get_userbyid(t.relowner) FROM pg_depend d JOIN pg_class t ON t.oid=d.refobjid JOIN pg_namespace tn ON tn.oid=t.relnamespace JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid WHERE d.classid='pg_class'::regclass AND d.objid=$1::oid AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i') ORDER BY t.oid, a.attnum",
            &[&sequence.relation_oid],
        )?;
        match (&sequence.ownership, links.as_slice()) {
            (None, []) => {}
            (Some(expected), [actual])
                if actual.get::<_, String>(0) == expected.table_namespace
                    && actual.get::<_, String>(1) == expected.table
                    && oid_from_i64(actual.get(2))? == expected.table_oid
                    && actual.get::<_, String>(3) == expected.column
                    && actual.get::<_, i16>(4) == expected.column_number
                    && actual.get::<_, String>(5) == expected.dependency_type
                    && actual.get::<_, String>(6) == inventory.admin_role => {}
            _ => {
                return Err(PostgresFenceError::Attestation(
                    "business sequence ownership link differs",
                ));
            }
        }
    }
    Ok(())
}

fn load_inventory_fingerprint(
    transaction: &mut Transaction<'_>,
) -> Result<(i32, String), PostgresFenceError> {
    let row = transaction.query_one(
        &format!(
            "SELECT format_version, inventory_fingerprint FROM {}.{} WHERE singleton",
            quote_ident(FENCE_SCHEMA),
            quote_ident(REGISTRY_TABLE)
        ),
        &[],
    )?;
    Ok((row.get(0), row.get(1)))
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
            history_function_oid: 9,
            history_trigger_oid: 10,
            dml_function_oid: 5,
            ddl_function_oid: 6,
            event_trigger_oid: 7,
            tables: vec![FencedTable {
                namespace: "public".into(),
                table: "a".into(),
                relation_oid: 1,
                trigger_oid: 8,
                trigger_name: "t1".into(),
                relation_kind: "r".into(),
                is_partition: false,
                parent_relation_oid: None,
                partition_strategy: None,
                partition_key_definition: None,
                partition_bound: None,
            }],
            sequences: Vec::new(),
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

        let mut sequence_changed = inventory.clone();
        sequence_changed.sequences.push(FencedSequence {
            namespace: "public".into(),
            sequence: "account_id_seq".into(),
            relation_oid: 11,
            original_owner_oid: 12,
            original_owner: "app".into(),
            original_acl_is_null: true,
            original_acl: vec![FencedSequenceGrant {
                grantee: Some("app".into()),
                grantor: "app".into(),
                privilege: "USAGE".into(),
                grantable: false,
            }],
            data_type: "bigint".into(),
            start_value: 1,
            increment: 1,
            minimum_value: 1,
            maximum_value: i64::MAX,
            cache_size: 32,
            cycle: false,
            ownership: None,
            last_value: 33,
            is_called: true,
        });
        assert_ne!(
            inventory.fingerprint().unwrap(),
            sequence_changed.fingerprint().unwrap()
        );
        sequence_changed.sequences[0].last_value = 65;
        assert_ne!(
            inventory.fingerprint().unwrap(),
            sequence_changed.fingerprint().unwrap()
        );
    }

    #[test]
    fn fence_unsupported_ids_are_catalog_class_qualified() {
        let inventory = FenceInventory {
            generation: "g".into(),
            admin_role: "admin".into(),
            schema_oid: 1,
            registry_oid: 2,
            history_oid: 3,
            history_sequence_oid: 4,
            history_function_oid: 8,
            history_trigger_oid: 9,
            dml_function_oid: 5,
            ddl_function_oid: 6,
            event_trigger_oid: 7,
            tables: Vec::new(),
            sequences: Vec::new(),
        };
        let ids = fence_unsupported_ids(&inventory);
        assert!(ids.contains("routine-acl:5"));
        assert!(!ids.contains("relation-acl:5"));
        assert!(!ids.contains("event-trigger:5"));
    }

    #[test]
    fn legacy_fence_artifact_remains_readable_for_explicit_recovery() {
        let artifact = InstalledPostgresFence {
            format_version: POSTGRES_FENCE_ARTIFACT_VERSION,
            token: FenceToken::generate(),
            evidence: ConsistencyEvidence::NativeSnapshot {
                endpoint_identity: "source".into(),
                database_identity: "database".into(),
                lifecycle_id: "lifecycle".into(),
                snapshot_id: "snapshot".into(),
                server_version: "17".into(),
            },
            admin_tls_binding: Some("binding".into()),
        };
        let mut value = serde_json::to_value(artifact).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("format_version");
        object.remove("admin_tls_binding");

        let legacy: InstalledPostgresFence = serde_json::from_value(value).unwrap();

        assert_eq!(legacy.format_version, 0);
        assert!(legacy.admin_tls_binding.is_none());
        assert!(!legacy.token.expose_secret().is_empty());
    }

    #[test]
    fn fence_admin_rejects_insecure_tls_before_reading_credentials() {
        let mut config: PostgresEndpointConfig = toml::from_str(
            r#"
host = "127.0.0.1"
database = "source"
user = "admin"
credential_env = "SQL_SPLITTER_TEST_MISSING_ADMIN_PASSWORD"

[tls]
insecure = true
"#,
        )
        .unwrap();
        config.connect_timeout_seconds = 1;
        assert!(matches!(
            connect_admin(&config),
            Err(PostgresFenceError::Attestation(
                "fence administration requires authenticated TLS"
            ))
        ));
    }

    #[test]
    fn identifier_quoting_cannot_add_sql_tokens() {
        assert_eq!(
            quote_ident("a\"; DROP SCHEMA public; --"),
            "\"a\"\"; DROP SCHEMA public; --\""
        );
    }

    #[test]
    fn released_generations_must_be_complete_contiguous_and_latest() {
        let valid = vec![
            ("g1".into(), "Draining".into()),
            ("g1".into(), "Active".into()),
            ("g1".into(), "Released".into()),
            ("g2".into(), "Draining".into()),
            ("g2".into(), "Active".into()),
            ("g2".into(), "Released".into()),
        ];
        assert!(validate_released_transitions(&valid, "g2").is_ok());
        assert!(validate_released_transitions(&valid, "g1").is_err());

        let interleaved = vec![
            ("g1".into(), "Draining".into()),
            ("g2".into(), "Draining".into()),
            ("g1".into(), "Released".into()),
        ];
        assert!(validate_released_transitions(&interleaved, "g1").is_err());
    }

    #[test]
    fn active_draining_breached_and_malformed_history_are_not_rearmable() {
        for transitions in [
            vec![("g".into(), "Draining".into())],
            vec![
                ("g".into(), "Draining".into()),
                ("g".into(), "Active".into()),
            ],
            vec![
                ("g".into(), "Draining".into()),
                ("g".into(), "Breached".into()),
            ],
            vec![
                ("g".into(), "Draining".into()),
                ("g".into(), "Released".into()),
                ("g".into(), "Active".into()),
            ],
        ] {
            assert!(validate_released_transitions(&transitions, "g").is_err());
        }
    }

    #[test]
    fn independently_generated_fence_secrets_do_not_cross_validate() {
        let old = FenceToken::generate();
        let new = FenceToken::generate();
        assert_ne!(old, new);
        assert!(!constant_time_eq(
            old.hash().as_bytes(),
            new.hash().as_bytes()
        ));
    }

    #[test]
    fn released_legacy_inventory_is_authenticated_with_its_original_fingerprint() {
        let inventory = FenceInventory {
            generation: "legacy-generation".into(),
            admin_role: "admin".into(),
            schema_oid: 1,
            registry_oid: 2,
            history_oid: 3,
            history_sequence_oid: 4,
            history_function_oid: 0,
            history_trigger_oid: 0,
            dml_function_oid: 5,
            ddl_function_oid: 6,
            event_trigger_oid: 7,
            tables: vec![FencedTable {
                namespace: "public".into(),
                table: "accounts".into(),
                relation_oid: 8,
                trigger_oid: 9,
                trigger_name: "legacy-trigger".into(),
                relation_kind: String::new(),
                is_partition: false,
                parent_relation_oid: None,
                partition_strategy: None,
                partition_key_definition: None,
                partition_bound: None,
            }],
            sequences: Vec::new(),
        };
        assert_eq!(
            history_guard_storage(&inventory).unwrap(),
            HistoryGuardStorage::Legacy
        );
        let fingerprint = legacy_inventory_fingerprint(&inventory).unwrap();
        assert_eq!(fingerprint.len(), 64);
        assert_ne!(fingerprint, inventory.fingerprint().unwrap());
        assert_ne!(
            pre_sequence_inventory_fingerprint(&inventory).unwrap(),
            inventory.fingerprint().unwrap()
        );

        let mut malformed = inventory;
        malformed.history_function_oid = 10;
        assert!(history_guard_storage(&malformed).is_err());
    }

    #[test]
    fn partition_topology_is_part_of_the_current_inventory_fingerprint() {
        let root = FencedTable {
            namespace: "ledger".into(),
            table: "entries".into(),
            relation_oid: 20,
            trigger_oid: 30,
            trigger_name: "root_guard".into(),
            relation_kind: "p".into(),
            is_partition: false,
            parent_relation_oid: None,
            partition_strategy: Some("r".into()),
            partition_key_definition: Some("RANGE (account_id)".into()),
            partition_bound: None,
        };
        let leaf = FencedTable {
            namespace: "ledger".into(),
            table: "entries_low".into(),
            relation_oid: 21,
            trigger_oid: 31,
            trigger_name: "leaf_guard".into(),
            relation_kind: "r".into(),
            is_partition: true,
            parent_relation_oid: Some(20),
            partition_strategy: None,
            partition_key_definition: None,
            partition_bound: Some("FOR VALUES FROM (MINVALUE) TO (1000)".into()),
        };
        let inventory = FenceInventory {
            generation: "partition-generation".into(),
            admin_role: "admin".into(),
            schema_oid: 1,
            registry_oid: 2,
            history_oid: 3,
            history_sequence_oid: 4,
            history_function_oid: 5,
            history_trigger_oid: 6,
            dml_function_oid: 7,
            ddl_function_oid: 8,
            event_trigger_oid: 9,
            tables: vec![root, leaf],
            sequences: Vec::new(),
        };
        assert!(validate_table_topology_inventory(&inventory).is_ok());

        let prior_fingerprint = pre_partition_inventory_fingerprint(&inventory).unwrap();
        let mut changed = inventory.clone();
        changed.tables[1].partition_bound = Some("FOR VALUES FROM (MINVALUE) TO (999)".into());
        assert_ne!(
            inventory.fingerprint().unwrap(),
            changed.fingerprint().unwrap()
        );
        assert_eq!(
            prior_fingerprint,
            pre_partition_inventory_fingerprint(&changed).unwrap()
        );
    }

    #[test]
    fn malformed_partition_topology_fails_closed() {
        let root = FencedTable {
            namespace: "public".into(),
            table: "root".into(),
            relation_oid: 1,
            trigger_oid: 3,
            trigger_name: "root_guard".into(),
            relation_kind: "p".into(),
            is_partition: false,
            parent_relation_oid: None,
            partition_strategy: Some("l".into()),
            partition_key_definition: Some("LIST (tenant_id)".into()),
            partition_bound: None,
        };
        let leaf = FencedTable {
            namespace: "public".into(),
            table: "leaf".into(),
            relation_oid: 2,
            trigger_oid: 4,
            trigger_name: "leaf_guard".into(),
            relation_kind: "r".into(),
            is_partition: true,
            parent_relation_oid: Some(1),
            partition_strategy: None,
            partition_key_definition: None,
            partition_bound: Some("FOR VALUES IN (1)".into()),
        };
        let inventory = FenceInventory {
            generation: "g".into(),
            admin_role: "admin".into(),
            schema_oid: 5,
            registry_oid: 6,
            history_oid: 7,
            history_sequence_oid: 8,
            history_function_oid: 9,
            history_trigger_oid: 10,
            dml_function_oid: 11,
            ddl_function_oid: 12,
            event_trigger_oid: 13,
            tables: vec![root, leaf],
            sequences: Vec::new(),
        };
        assert!(validate_table_topology_inventory(&inventory).is_ok());

        let mut missing_parent = inventory.clone();
        missing_parent.tables.remove(0);
        assert!(validate_table_topology_inventory(&missing_parent).is_err());

        let mut missing_bound = inventory.clone();
        missing_bound.tables[1].partition_bound = None;
        assert!(validate_table_topology_inventory(&missing_bound).is_err());

        let mut cycle = inventory;
        cycle.tables[0].is_partition = true;
        cycle.tables[0].parent_relation_oid = Some(2);
        cycle.tables[0].partition_bound = Some("FOR VALUES IN (2)".into());
        assert!(validate_table_topology_inventory(&cycle).is_err());
    }
}
