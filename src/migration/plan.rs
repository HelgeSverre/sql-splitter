use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::canonical::CANONICAL_ENCODING_VERSION;
use super::conversion::{
    ConversionDialect, MigrationConversionMode, MigrationConversionPolicy, RowConversionError,
};
use super::model::{QualifiedTable, VendorCatalog};
use super::mysql_profile::{MySqlFreezeProfileContract, MySqlFreezeProfileError};
use super::mysql_visibility::{
    MySqlAuthorizationContract, MySqlGrantRecord, MySqlMetadataVisibilityEvidence,
    MySqlVisibilityError,
};
use super::outage_projection::{OutageProjectionError, ReviewedOutagePolicy};
use super::postgres_profile::{PostgresSourceProfileContract, PostgresSourceProfileError};

pub const PLAN_SCHEMA_VERSION: u16 = 16;
pub const MYSQL_SAME_DIALECT_CONVERSION_POLICY: MigrationConversionPolicy =
    MigrationConversionPolicy::same_dialect_exact(ConversionDialect::MySql);
pub const POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY: MigrationConversionPolicy =
    MigrationConversionPolicy::same_dialect_exact(ConversionDialect::PostgreSql);
pub const POSTGRESQL_ASSESSMENT_CONVERSION_POLICY: MigrationConversionPolicy =
    MigrationConversionPolicy::assessment_source_only(ConversionDialect::PostgreSql);
pub const MYSQL_STRICT_SQL_MODE: &str =
    "NO_AUTO_VALUE_ON_ZERO,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION";
pub const MYSQL_SESSION_CHARACTER_SET: &str = "utf8mb4";
pub const MYSQL_SESSION_COLLATION: &str = "utf8mb4_0900_bin";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlSnapshotEvidence {
    pub endpoint_identity: String,
    pub database_identity: String,
    pub server_uuid: String,
    pub server_version: String,
    pub authenticated_account: String,
    pub lifecycle_id: String,
    pub connection_id: u32,
    pub transaction_isolation: String,
    pub transaction_read_only: bool,
    pub session_time_zone: String,
    /// MySQL data-dictionary reads are not protected by the InnoDB row snapshot.
    pub catalog_snapshot_protected: bool,
    pub information_schema_stats_expiry: u64,
    pub lower_case_table_names: u8,
    pub session_sql_mode: String,
    pub character_set_client: String,
    pub character_set_connection: String,
    pub character_set_results: String,
    pub collation_connection: String,
    pub gtid_executed_observation: String,
    pub catalog_fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanPurpose {
    Assessment,
    Execution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum AssessmentStatus<T> {
    Assessed(T),
    NotAssessed,
}

impl<T> AssessmentStatus<T> {
    pub fn as_assessed(&self) -> Option<&T> {
        match self {
            Self::Assessed(value) => Some(value),
            Self::NotAssessed => None,
        }
    }

    pub fn is_assessed(&self) -> bool {
        matches!(self, Self::Assessed(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PlanHash(String);

impl PlanHash {
    pub fn new(value: impl Into<String>) -> Result<Self, PlanError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(PlanError::InvalidHash);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for PlanHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for PlanHash {
    type Error = PlanError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<PlanHash> for String {
    fn from(value: PlanHash) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId(String);
impl OperationId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    CreateNamespace,
    CreateTable,
    CreateSequence,
    CopyTable,
    CreateIndex,
    CheckForeignKey,
    AddForeignKey,
    CreateView,
    CreateRoutine,
    CreateTrigger,
    VerifyTable,
    VerifySchema,
    Vendor(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub table: Option<QualifiedTable>,
    pub dependencies: Vec<OperationId>,
    /// Typed adapter payload. Raw SQL must not be placed here.
    pub parameters: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct OperationIdentity<'a> {
    kind: &'a OperationKind,
    table: &'a Option<QualifiedTable>,
    dependencies: &'a [OperationId],
    parameters: &'a BTreeMap<String, serde_json::Value>,
}

impl PlanOperation {
    pub fn new(
        kind: OperationKind,
        table: Option<QualifiedTable>,
        mut dependencies: Vec<OperationId>,
        parameters: BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, PlanError> {
        dependencies.sort();
        dependencies.dedup();
        let identity = OperationIdentity {
            kind: &kind,
            table: &table,
            dependencies: &dependencies,
            parameters: &parameters,
        };
        let bytes = serde_json::to_vec(&identity)?;
        let id = OperationId(hex::encode(Sha256::digest(bytes)));
        Ok(Self {
            id,
            kind,
            table,
            dependencies,
            parameters,
        })
    }
    fn expected_id(&self) -> Result<OperationId, PlanError> {
        let mut dependencies = self.dependencies.clone();
        dependencies.sort();
        dependencies.dedup();
        let identity = OperationIdentity {
            kind: &self.kind,
            table: &self.table,
            dependencies: &dependencies,
            parameters: &self.parameters,
        };
        Ok(OperationId(hex::encode(Sha256::digest(
            serde_json::to_vec(&identity)?,
        ))))
    }
}

macro_rules! unsupported_object_codes {
    ($( $variant:ident => $name:literal ),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum UnsupportedObjectCode {
            $( $variant, )+
        }

        impl UnsupportedObjectCode {
            pub const ALL: &'static [Self] = &[$( Self::$variant, )+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $name, )+
                }
            }
        }
    };
}

unsupported_object_codes! {
    SequencePersistence => "sequence_persistence",
    ViewSecurity => "view_security",
    ViewColumnAcl => "view_column_acl",
    ViewAst => "view_ast",
    MaterializedView => "materialized_view",
    RowSecurity => "row_security",
    PartitionTopology => "partition_topology",
    PartitionLocalIndex => "partition_local_index",
    PartitionChildIndexStorage => "partition_child_index_storage",
    PartitionChildIndexName => "partition_child_index_name",
    PartitionLocalConstraint => "partition_local_constraint",
    PartitionStorage => "partition_storage",
    PartitionTrigger => "partition_trigger",
    TraditionalInheritance => "traditional_inheritance",
    SequenceOwnership => "sequence_ownership",
    UserTypeDdl => "user_type_ddl",
    Extension => "extension",
    GeneratedDependency => "generated_dependency",
    GeneratedMode => "generated_mode",
    UserDefinedColumnType => "user_defined_column_type",
    CollationVersion => "collation_version",
    StandaloneIndex => "standalone_index",
    Trigger => "trigger",
    Routine => "routine",
    RowSecurityPolicy => "row_security_policy",
    NamespaceAcl => "namespace_acl",
    RelationAcl => "relation_acl",
    RoutineAcl => "routine_acl",
    DefaultPrivileges => "default_privileges",
    EventTrigger => "event_trigger",
    RewriteRule => "rewrite_rule",
    Publication => "publication",
    ForeignServer => "foreign_server",
    ForeignTable => "foreign_table",
    ExtendedStatistics => "extended_statistics",
    UserCollation => "user_collation",
    ResumableKey => "resumable_key",
    SequenceConsistency => "sequence_consistency",
    GeneratedCrossMajor => "generated_cross_major",
    TargetNotEmpty => "target_not_empty",
    SameEndpoint => "same_endpoint",
    MySqlStorageEngine => "mysql_storage_engine",
    MySqlCatalogSemantics => "mysql_catalog_semantics",
    MySqlFreezeEvidence => "mysql_freeze_evidence",
    MySqlAutoIncrementConsistency => "mysql_auto_increment_consistency",
}

impl UnsupportedObjectCode {
    pub const fn requires_execution_block(self) -> bool {
        !matches!(
            self,
            Self::NamespaceAcl | Self::RelationAcl | Self::RoutineAcl | Self::DefaultPrivileges
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedObject {
    pub code: UnsupportedObjectCode,
    pub object_id: String,
    pub object_kind: String,
    pub reason: String,
    pub required_semantics: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UnsupportedObjectReport {
    pub objects: Vec<UnsupportedObject>,
}
impl UnsupportedObjectReport {
    pub fn blocks_execution(&self) -> bool {
        self.objects.iter().any(|o| o.required_semantics)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationPlan {
    pub schema_version: u16,
    pub purpose: PlanPurpose,
    pub migration_id: String,
    pub tool_version: String,
    pub source_endpoint_identity: String,
    pub target_endpoint_identity: AssessmentStatus<String>,
    pub source_catalog_fingerprint: String,
    pub target_catalog_fingerprint: AssessmentStatus<String>,
    pub source_catalog: Option<VendorCatalog>,
    pub target_catalog: AssessmentStatus<VendorCatalog>,
    pub source_tls_binding: String,
    pub target_tls_binding: AssessmentStatus<String>,
    pub consistency_mode: String,
    pub canonical_encoding_version: u16,
    pub conversion_policy: MigrationConversionPolicy,
    #[serde(default)]
    pub outage_policy: Option<ReviewedOutagePolicy>,
    #[serde(default)]
    pub postgres_source_profile: Option<PostgresSourceProfileContract>,
    #[serde(default)]
    pub mysql_source_profile: Option<MySqlFreezeProfileContract>,
    #[serde(default)]
    pub mysql_snapshot_evidence: Option<MySqlSnapshotEvidence>,
    #[serde(default)]
    pub mysql_target_snapshot_evidence: Option<MySqlSnapshotEvidence>,
    #[serde(default)]
    pub mysql_metadata_visibility: Option<MySqlMetadataVisibilityEvidence>,
    #[serde(default)]
    pub mysql_target_metadata_visibility: Option<MySqlMetadataVisibilityEvidence>,
    #[serde(default)]
    pub mysql_authorization: Option<MySqlAuthorizationContract>,
    pub capabilities: BTreeMap<String, String>,
    pub operations: Vec<PlanOperation>,
    pub unsupported_objects: UnsupportedObjectReport,
}

impl MigrationPlan {
    pub fn validate(&self) -> Result<(), PlanError> {
        if self.schema_version != PLAN_SCHEMA_VERSION {
            return Err(PlanError::UnsupportedVersion {
                found: self.schema_version,
            });
        }
        if self.canonical_encoding_version != CANONICAL_ENCODING_VERSION {
            return Err(PlanError::UnsupportedCanonicalEncodingVersion {
                found: self.canonical_encoding_version,
            });
        }
        for (field, value) in [
            ("migration_id", &self.migration_id),
            ("tool_version", &self.tool_version),
            ("source_endpoint_identity", &self.source_endpoint_identity),
            (
                "source_catalog_fingerprint",
                &self.source_catalog_fingerprint,
            ),
            ("source_tls_binding", &self.source_tls_binding),
        ] {
            if value.is_empty() {
                return Err(PlanError::EmptyField { field });
            }
        }
        let source_catalog = self
            .source_catalog
            .as_ref()
            .ok_or(PlanError::MissingEvidence {
                field: "source_catalog",
            })?;
        self.conversion_policy.validate()?;
        if self.conversion_policy.source_dialect().catalog_name() != source_catalog.dialect {
            return Err(PlanError::InvalidConversionPolicy);
        }
        validate_catalog_fingerprint(
            "source_catalog_fingerprint",
            source_catalog,
            &self.source_catalog_fingerprint,
        )?;

        let target_assessment = [
            self.target_endpoint_identity.is_assessed(),
            self.target_catalog_fingerprint.is_assessed(),
            self.target_catalog.is_assessed(),
            self.target_tls_binding.is_assessed(),
        ];
        if !target_assessment
            .iter()
            .all(|assessed| *assessed == target_assessment[0])
        {
            return Err(PlanError::InconsistentTargetAssessment);
        }
        if let (Some(endpoint), Some(fingerprint), Some(catalog), Some(tls_binding)) = (
            self.target_endpoint_identity.as_assessed(),
            self.target_catalog_fingerprint.as_assessed(),
            self.target_catalog.as_assessed(),
            self.target_tls_binding.as_assessed(),
        ) {
            for (field, value) in [
                ("target_endpoint_identity", endpoint),
                ("target_catalog_fingerprint", fingerprint),
                ("target_tls_binding", tls_binding),
            ] {
                if value.is_empty() {
                    return Err(PlanError::EmptyField { field });
                }
            }
            validate_catalog_fingerprint("target_catalog_fingerprint", catalog, fingerprint)?;
        }
        if self.purpose == PlanPurpose::Execution {
            require_execution_target(self)?;
            if matches!(
                &self.conversion_policy.mode,
                MigrationConversionMode::AssessmentSourceOnly { .. }
            ) {
                return Err(PlanError::InvalidConversionPolicy);
            }
            let target_catalog = self
                .target_catalog
                .as_assessed()
                .ok_or(PlanError::InvalidConversionPolicy)?;
            if self
                .conversion_policy
                .target_dialect()
                .is_none_or(|dialect| dialect.catalog_name() != target_catalog.dialect)
            {
                return Err(PlanError::InvalidConversionPolicy);
            }
            if self.consistency_mode.is_empty() {
                return Err(PlanError::EmptyField {
                    field: "consistency_mode",
                });
            }
        } else {
            if !matches!(
                &self.conversion_policy.mode,
                MigrationConversionMode::AssessmentSourceOnly { .. }
            ) {
                return Err(PlanError::InvalidConversionPolicy);
            }
            if self.outage_policy.is_some() {
                return Err(PlanError::AssessmentContainsOutagePolicy);
            }
            if self.postgres_source_profile.is_some() {
                return Err(PlanError::AssessmentContainsSourceProfile);
            }
            if self.mysql_source_profile.is_some() {
                return Err(PlanError::AssessmentContainsSourceProfile);
            }
        }
        if let Some(outage_policy) = &self.outage_policy {
            outage_policy.validate()?;
            if outage_policy.source_catalog_fingerprint != self.source_catalog_fingerprint {
                return Err(PlanError::OutagePolicySourceMismatch);
            }
        }
        if let Some(source_profile) = &self.postgres_source_profile {
            source_profile.validate_for_plan(
                &self.source_endpoint_identity,
                &self.source_catalog_fingerprint,
                &self.consistency_mode,
            )?;
        }
        validate_mysql_source_contract(self, source_catalog)?;
        validate_mysql_target_contract(self)?;
        let mut finding_keys = BTreeSet::new();
        for finding in &self.unsupported_objects.objects {
            if finding.object_id.is_empty()
                || finding.object_kind.is_empty()
                || finding.reason.is_empty()
            {
                return Err(PlanError::InvalidUnsupportedFinding);
            }
            if finding.required_semantics != finding.code.requires_execution_block() {
                return Err(PlanError::UnsupportedFindingSeverityMismatch { code: finding.code });
            }
            if !finding_keys.insert((finding.code, finding.object_id.as_str())) {
                return Err(PlanError::DuplicateUnsupportedFinding);
            }
        }
        let ids: BTreeSet<_> = self.operations.iter().map(|op| &op.id).collect();
        if ids.len() != self.operations.len() {
            return Err(PlanError::DuplicateOperationId);
        }
        for op in &self.operations {
            if op.id != op.expected_id()? {
                return Err(PlanError::OperationIdMismatch { id: op.id.clone() });
            }
            if op
                .dependencies
                .iter()
                .any(|dependency| dependency == &op.id)
            {
                return Err(PlanError::SelfDependency { id: op.id.clone() });
            }
            for dependency in &op.dependencies {
                if !ids.contains(dependency) {
                    return Err(PlanError::UnknownDependency {
                        operation: op.id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        validate_operation_conversion_bindings(self)?;
        detect_cycle(&self.operations)?;
        Ok(())
    }
    pub fn canonical_json(&self) -> Result<Vec<u8>, PlanError> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }
    pub fn hash(&self) -> Result<PlanHash, PlanError> {
        Ok(PlanHash(hex::encode(Sha256::digest(
            self.canonical_json()?,
        ))))
    }

    pub fn validate_for_execution(&self) -> Result<(), PlanError> {
        self.validate()?;
        if self.purpose != PlanPurpose::Execution {
            return Err(PlanError::AssessmentCannotExecute);
        }
        require_execution_target(self)?;
        if self.unsupported_objects.blocks_execution() {
            return Err(PlanError::UnsupportedRequiredSemantics);
        }
        Ok(())
    }

    pub fn execution_target_endpoint_identity(&self) -> Result<&str, PlanError> {
        self.validate_for_execution()?;
        self.target_endpoint_identity
            .as_assessed()
            .map(String::as_str)
            .ok_or(PlanError::MissingEvidence {
                field: "target_endpoint_identity",
            })
    }

    pub fn execution_target_catalog_fingerprint(&self) -> Result<&str, PlanError> {
        self.validate_for_execution()?;
        self.target_catalog_fingerprint
            .as_assessed()
            .map(String::as_str)
            .ok_or(PlanError::MissingEvidence {
                field: "target_catalog_fingerprint",
            })
    }
}

fn validate_operation_conversion_bindings(plan: &MigrationPlan) -> Result<(), PlanError> {
    const COPY_POLICY: &str = "table_conversion_policy";
    const CREATE_POLICY: &str = "cross_dialect_target_table_policy";
    const VERIFY_POLICY: &str = "cross_dialect_verification_policy";
    let MigrationConversionMode::CrossDialect { tables, .. } = &plan.conversion_policy.mode else {
        if plan.operations.iter().any(|operation| {
            [COPY_POLICY, CREATE_POLICY, VERIFY_POLICY]
                .iter()
                .any(|parameter| operation.parameters.contains_key(*parameter))
        }) {
            return Err(PlanError::InvalidConversionPolicy);
        }
        return Ok(());
    };
    let expected_source = tables
        .iter()
        .map(|table| (&table.source_table, table))
        .collect::<BTreeMap<_, _>>();
    let expected_target = tables
        .iter()
        .map(|table| (&table.target_table, table))
        .collect::<BTreeMap<_, _>>();
    if tables.is_empty() {
        return if plan.operations.is_empty() {
            Ok(())
        } else {
            Err(PlanError::InvalidConversionPolicy)
        };
    }
    let mut observed_copy = BTreeMap::new();
    let mut observed_create = BTreeMap::new();
    let mut observed_verify = BTreeMap::new();
    for operation in &plan.operations {
        let (parameter, expected, observed) = match operation.kind {
            OperationKind::CopyTable => (COPY_POLICY, &expected_source, &mut observed_copy),
            OperationKind::CreateTable => (CREATE_POLICY, &expected_target, &mut observed_create),
            OperationKind::VerifyTable => (VERIFY_POLICY, &expected_source, &mut observed_verify),
            _ => {
                if [COPY_POLICY, CREATE_POLICY, VERIFY_POLICY]
                    .iter()
                    .any(|parameter| operation.parameters.contains_key(*parameter))
                {
                    return Err(PlanError::InvalidConversionPolicy);
                }
                continue;
            }
        };
        if [COPY_POLICY, CREATE_POLICY, VERIFY_POLICY]
            .iter()
            .any(|candidate| {
                *candidate != parameter && operation.parameters.contains_key(*candidate)
            })
        {
            return Err(PlanError::InvalidConversionPolicy);
        }
        if operation.parameters.len() != 1 {
            return Err(PlanError::InvalidConversionPolicy);
        }
        let table = operation
            .table
            .as_ref()
            .ok_or(PlanError::InvalidConversionPolicy)?;
        let reviewed = expected
            .get(table)
            .ok_or(PlanError::InvalidConversionPolicy)?;
        let embedded = operation
            .parameters
            .get(parameter)
            .cloned()
            .map(serde_json::from_value::<super::conversion::TableConversionPolicy>)
            .transpose()
            .map_err(|_| PlanError::InvalidConversionPolicy)?
            .ok_or(PlanError::InvalidConversionPolicy)?;
        if &embedded != *reviewed || observed.insert(table, operation).is_some() {
            return Err(PlanError::InvalidConversionPolicy);
        }
    }
    if observed_copy.len() != expected_source.len()
        || observed_create.len() != expected_target.len()
        || observed_verify.len() != expected_source.len()
    {
        return Err(PlanError::InvalidConversionPolicy);
    }
    for policy in tables {
        let create = observed_create
            .get(&policy.target_table)
            .ok_or(PlanError::InvalidConversionPolicy)?;
        let copy = observed_copy
            .get(&policy.source_table)
            .ok_or(PlanError::InvalidConversionPolicy)?;
        let verify = observed_verify
            .get(&policy.source_table)
            .ok_or(PlanError::InvalidConversionPolicy)?;
        if !create.dependencies.is_empty()
            || copy.dependencies.as_slice() != std::slice::from_ref(&create.id)
            || verify.dependencies.as_slice() != std::slice::from_ref(&copy.id)
        {
            return Err(PlanError::InvalidConversionPolicy);
        }
    }
    let mut schema_verifiers = plan
        .operations
        .iter()
        .filter(|operation| operation.kind == OperationKind::VerifySchema);
    let schema_verifier = schema_verifiers
        .next()
        .ok_or(PlanError::InvalidConversionPolicy)?;
    let mut expected_schema_dependencies = plan
        .operations
        .iter()
        .filter(|operation| operation.kind != OperationKind::VerifySchema)
        .map(|operation| operation.id.clone())
        .collect::<Vec<_>>();
    expected_schema_dependencies.sort();
    if schema_verifiers.next().is_some()
        || schema_verifier.table.is_some()
        || !schema_verifier.parameters.is_empty()
        || schema_verifier.dependencies != expected_schema_dependencies
        || plan.operations.iter().any(|operation| {
            !matches!(
                operation.kind,
                OperationKind::CreateTable
                    | OperationKind::CopyTable
                    | OperationKind::VerifyTable
                    | OperationKind::VerifySchema
            )
        })
    {
        return Err(PlanError::InvalidConversionPolicy);
    }
    Ok(())
}

fn validate_mysql_source_contract(
    plan: &MigrationPlan,
    source_catalog: &VendorCatalog,
) -> Result<(), PlanError> {
    if source_catalog.dialect != "mysql" {
        if plan.mysql_snapshot_evidence.is_some()
            || plan.mysql_source_profile.is_some()
            || plan.mysql_metadata_visibility.is_some()
            || plan.mysql_authorization.is_some()
        {
            return Err(PlanError::UnexpectedMySqlSnapshotEvidence);
        }
        return Ok(());
    }

    let evidence = plan
        .mysql_snapshot_evidence
        .as_ref()
        .ok_or(PlanError::MissingEvidence {
            field: "mysql_snapshot_evidence",
        })?;
    plan.mysql_source_profile
        .as_ref()
        .ok_or(PlanError::MissingEvidence {
            field: "mysql_source_profile",
        })?
        .validate()?;
    if plan.postgres_source_profile.is_some()
        || plan.consistency_mode != "mysql-repeatable-read-consistent-snapshot"
    {
        return Err(PlanError::InvalidMySqlSnapshotEvidence);
    }
    let catalog_reader_fingerprint = plan
        .mysql_metadata_visibility
        .as_ref()
        .map_or(plan.source_catalog_fingerprint.as_str(), |visibility| {
            visibility.catalog_reader_fingerprint.as_str()
        });
    validate_mysql_snapshot_evidence(
        evidence,
        &plan.source_endpoint_identity,
        source_catalog,
        catalog_reader_fingerprint,
    )?;
    if let Some(visibility) = &plan.mysql_metadata_visibility {
        visibility.validate()?;
        if visibility.endpoint_identity != plan.source_endpoint_identity
            || visibility.database_identity != evidence.database_identity
            || visibility.server_uuid != evidence.server_uuid
            || visibility.catalog_reader_tls_binding != plan.source_tls_binding
            || format!(
                "{}@{}",
                visibility.catalog_reader_account.user, visibility.catalog_reader_account.host
            ) != evidence.authenticated_account
            || visibility.catalog_reader_fingerprint != evidence.catalog_fingerprint
            || visibility.authoritative_catalog_fingerprint != plan.source_catalog_fingerprint
        {
            return Err(PlanError::InvalidMySqlMetadataVisibility);
        }
        validate_mysql_authorization_contract(
            visibility,
            plan.mysql_target_metadata_visibility.as_ref(),
            plan.mysql_authorization.as_ref(),
            &plan.unsupported_objects,
        )?;
    } else if !plan
        .unsupported_objects
        .objects
        .iter()
        .any(|finding| finding.object_kind == "catalog_visibility")
    {
        return Err(PlanError::MissingEvidence {
            field: "mysql_metadata_visibility",
        });
    } else if plan.mysql_authorization.is_some() {
        return Err(PlanError::InvalidMySqlMetadataVisibility);
    }
    Ok(())
}

fn validate_mysql_target_contract(plan: &MigrationPlan) -> Result<(), PlanError> {
    let target_catalog = plan.target_catalog.as_assessed();
    if target_catalog.is_none_or(|catalog| catalog.dialect != "mysql") {
        if plan.mysql_target_snapshot_evidence.is_some()
            || plan.mysql_target_metadata_visibility.is_some()
        {
            return Err(PlanError::UnexpectedMySqlSnapshotEvidence);
        }
        if plan
            .source_catalog
            .as_ref()
            .is_some_and(|catalog| catalog.dialect == "mysql")
            && plan.mysql_authorization.is_some()
        {
            return Err(PlanError::InvalidMySqlMetadataVisibility);
        }
        return Ok(());
    }

    let target_catalog = target_catalog.ok_or(PlanError::InvalidMySqlSnapshotEvidence)?;
    let target_evidence =
        plan.mysql_target_snapshot_evidence
            .as_ref()
            .ok_or(PlanError::MissingEvidence {
                field: "mysql_target_snapshot_evidence",
            })?;
    let target_reader_catalog_fingerprint =
        plan.mysql_target_metadata_visibility.as_ref().map_or_else(
            || {
                plan.target_catalog_fingerprint
                    .as_assessed()
                    .map(String::as_str)
                    .unwrap_or_default()
            },
            |visibility| visibility.catalog_reader_fingerprint.as_str(),
        );
    validate_mysql_snapshot_evidence(
        target_evidence,
        plan.target_endpoint_identity
            .as_assessed()
            .ok_or(PlanError::InvalidMySqlSnapshotEvidence)?,
        target_catalog,
        target_reader_catalog_fingerprint,
    )?;
    if let Some(visibility) = &plan.mysql_target_metadata_visibility {
        visibility.validate()?;
        if visibility.endpoint_identity
            != *plan
                .target_endpoint_identity
                .as_assessed()
                .ok_or(PlanError::InvalidMySqlMetadataVisibility)?
            || visibility.database_identity != target_evidence.database_identity
            || visibility.server_uuid != target_evidence.server_uuid
            || visibility.catalog_reader_tls_binding
                != *plan
                    .target_tls_binding
                    .as_assessed()
                    .ok_or(PlanError::InvalidMySqlMetadataVisibility)?
            || format!(
                "{}@{}",
                visibility.catalog_reader_account.user, visibility.catalog_reader_account.host
            ) != target_evidence.authenticated_account
            || visibility.catalog_reader_fingerprint != target_evidence.catalog_fingerprint
            || visibility.authoritative_catalog_fingerprint
                != *plan
                    .target_catalog_fingerprint
                    .as_assessed()
                    .ok_or(PlanError::InvalidMySqlMetadataVisibility)?
        {
            return Err(PlanError::InvalidMySqlMetadataVisibility);
        }
    } else if !plan
        .unsupported_objects
        .objects
        .iter()
        .any(|finding| finding.object_kind == "target_catalog_visibility")
    {
        return Err(PlanError::MissingEvidence {
            field: "mysql_target_metadata_visibility",
        });
    }

    if let Some(source_evidence) = &plan.mysql_snapshot_evidence {
        if source_evidence.lower_case_table_names != target_evidence.lower_case_table_names
            || plan.conversion_policy != MYSQL_SAME_DIALECT_CONVERSION_POLICY
        {
            return Err(PlanError::InvalidMySqlSnapshotEvidence);
        }
    } else if !plan.conversion_policy.has_approved_transformations()
        || plan.mysql_authorization.is_some()
    {
        return Err(PlanError::InvalidMySqlSnapshotEvidence);
    }
    Ok(())
}

fn validate_mysql_authorization_contract(
    source: &MySqlMetadataVisibilityEvidence,
    target: Option<&MySqlMetadataVisibilityEvidence>,
    authorization: Option<&MySqlAuthorizationContract>,
    unsupported: &UnsupportedObjectReport,
) -> Result<(), PlanError> {
    let expected = source
        .non_operational_records()
        .into_iter()
        .map(MySqlGrantRecord::canonical_id)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let actual = unsupported
        .objects
        .iter()
        .filter(|finding| finding.object_kind == "privilege")
        .map(|finding| {
            if finding.code != UnsupportedObjectCode::MySqlCatalogSemantics
                || !finding.required_semantics
            {
                return Err(PlanError::InvalidMySqlMetadataVisibility);
            }
            Ok(finding.object_id.clone())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    match authorization {
        Some(contract) => {
            let target = target.ok_or(PlanError::InvalidMySqlMetadataVisibility)?;
            contract.validate_against(source, target)?;
            if !actual.is_empty() {
                return Err(PlanError::InvalidMySqlMetadataVisibility);
            }
        }
        None if expected == actual => {}
        None => return Err(PlanError::InvalidMySqlMetadataVisibility),
    }
    Ok(())
}

fn validate_catalog_fingerprint(
    field: &'static str,
    catalog: &VendorCatalog,
    expected: &str,
) -> Result<(), PlanError> {
    let actual = hex::encode(Sha256::digest(serde_json::to_vec(catalog)?));
    if actual != expected {
        return Err(PlanError::CatalogFingerprintMismatch { field, actual });
    }
    Ok(())
}

fn require_execution_target(plan: &MigrationPlan) -> Result<(), PlanError> {
    for (field, assessed) in [
        (
            "target_endpoint_identity",
            plan.target_endpoint_identity.is_assessed(),
        ),
        (
            "target_catalog_fingerprint",
            plan.target_catalog_fingerprint.is_assessed(),
        ),
        ("target_catalog", plan.target_catalog.is_assessed()),
        ("target_tls_binding", plan.target_tls_binding.is_assessed()),
    ] {
        if !assessed {
            return Err(PlanError::MissingEvidence { field });
        }
    }
    Ok(())
}

fn validate_mysql_snapshot_evidence(
    evidence: &MySqlSnapshotEvidence,
    endpoint_identity: &str,
    catalog: &VendorCatalog,
    catalog_fingerprint: &str,
) -> Result<(), PlanError> {
    if evidence.endpoint_identity != endpoint_identity
        || evidence.database_identity != catalog.database.as_str()
        || evidence.server_version != catalog.server_version
        || evidence.authenticated_account.is_empty()
        || evidence.authenticated_account.contains('\0')
        || evidence.catalog_fingerprint != catalog_fingerprint
        || evidence.server_uuid.is_empty()
        || evidence.lifecycle_id.is_empty()
        || evidence.connection_id == 0
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
        || catalog
            .vendor_metadata
            .get("lower_case_table_names")
            .is_none_or(|value| value != &evidence.lower_case_table_names.to_string())
    {
        return Err(PlanError::InvalidMySqlSnapshotEvidence);
    }
    Ok(())
}

fn detect_cycle(operations: &[PlanOperation]) -> Result<(), PlanError> {
    fn visit<'a>(
        id: &'a OperationId,
        by_id: &BTreeMap<&'a OperationId, &'a PlanOperation>,
        visiting: &mut BTreeSet<&'a OperationId>,
        visited: &mut BTreeSet<&'a OperationId>,
    ) -> Result<(), PlanError> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(PlanError::DependencyCycle { id: id.clone() });
        }
        for dependency in &by_id[id].dependencies {
            visit(dependency, by_id, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id);
        Ok(())
    }
    let by_id: BTreeMap<_, _> = operations.iter().map(|op| (&op.id, op)).collect();
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in by_id.keys() {
        visit(id, &by_id, &mut visiting, &mut visited)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewedPlan {
    pub plan: MigrationPlan,
    pub plan_hash: PlanHash,
}
impl ReviewedPlan {
    pub fn new(plan: MigrationPlan) -> Result<Self, PlanError> {
        let plan_hash = plan.hash()?;
        Ok(Self { plan, plan_hash })
    }
    pub fn validate(&self) -> Result<(), PlanError> {
        let actual = self.plan.hash()?;
        if actual != self.plan_hash {
            return Err(PlanError::HashMismatch {
                expected: self.plan_hash.clone(),
                actual,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("plan contains invalid SHA-256 text")]
    InvalidHash,
    #[error("unsupported plan schema version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("unsupported canonical encoding version {found}")]
    UnsupportedCanonicalEncodingVersion { found: u16 },
    #[error("reviewed conversion policy is invalid")]
    ConversionPolicy(#[from] RowConversionError),
    #[error("reviewed conversion policy does not match the plan purpose or catalogs")]
    InvalidConversionPolicy,
    #[error("required plan field {field} is empty")]
    EmptyField { field: &'static str },
    #[error("plan contains duplicate operation IDs")]
    DuplicateOperationId,
    #[error("plan contains an unsupported-object finding with an empty field")]
    InvalidUnsupportedFinding,
    #[error("plan contains a duplicate unsupported-object code and identity")]
    DuplicateUnsupportedFinding,
    #[error("unsupported-object code {code:?} has the wrong blocking severity")]
    UnsupportedFindingSeverityMismatch { code: UnsupportedObjectCode },
    #[error("operation ID {id} does not match its content")]
    OperationIdMismatch { id: OperationId },
    #[error("operation {id} depends on itself")]
    SelfDependency { id: OperationId },
    #[error("operation {operation} has unknown dependency {dependency}")]
    UnknownDependency {
        operation: OperationId,
        dependency: OperationId,
    },
    #[error("operation dependency cycle includes {id}")]
    DependencyCycle { id: OperationId },
    #[error("reviewed plan hash mismatch: expected {expected}, actual {actual}")]
    HashMismatch {
        expected: PlanHash,
        actual: PlanHash,
    },
    #[error("cannot serialize canonical plan JSON")]
    Serialization(#[from] serde_json::Error),
    #[error("plan contains unsupported objects with required semantics")]
    UnsupportedRequiredSemantics,
    #[error("assessment plans cannot be executed")]
    AssessmentCannotExecute,
    #[error("assessment plan must not contain an execution outage policy")]
    AssessmentContainsOutagePolicy,
    #[error("assessment plan must not contain a PostgreSQL execution source profile")]
    AssessmentContainsSourceProfile,
    #[error("outage policy source fingerprint differs from the plan")]
    OutagePolicySourceMismatch,
    #[error("reviewed outage policy is invalid")]
    OutagePolicy(#[from] OutageProjectionError),
    #[error("reviewed PostgreSQL source profile is invalid")]
    PostgresSourceProfile(#[from] PostgresSourceProfileError),
    #[error("reviewed MySQL snapshot evidence is invalid or inconsistent")]
    InvalidMySqlSnapshotEvidence,
    #[error("reviewed MySQL metadata-visibility evidence is invalid or inconsistent")]
    InvalidMySqlMetadataVisibility,
    #[error("reviewed MySQL metadata-visibility evidence is invalid")]
    MySqlMetadataVisibility(#[from] MySqlVisibilityError),
    #[error("reviewed MySQL source profile is invalid")]
    MySqlSourceProfile(#[from] MySqlFreezeProfileError),
    #[error("non-MySQL plan contains MySQL snapshot evidence")]
    UnexpectedMySqlSnapshotEvidence,
    #[error("required plan evidence {field} is absent")]
    MissingEvidence { field: &'static str },
    #[error("target assessment fields must all be assessed or all be not assessed")]
    InconsistentTargetAssessment,
    #[error("{field} does not match the embedded catalog; actual fingerprint is {actual}")]
    CatalogFingerprintMismatch { field: &'static str, actual: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::conversion::{
        CrossDialectKeyKind, CrossDialectResumableKey, CrossDialectSourceType,
        CrossDialectTargetType, MySqlIntegerWidth, MySqlTargetType, PostgresTargetType,
        RowConversionPolicy, TableConversionPolicy, ValueConversionRule,
        ROW_TYPE_CONVERSION_SCHEMA_VERSION,
    };
    use crate::migration::model::{ColumnMeta, Identifier};
    use crate::migration::outage_projection::{
        ByteBasis, ThroughputProfile, OUTAGE_PROJECTION_SCHEMA_VERSION,
        THROUGHPUT_PROFILE_SCHEMA_VERSION,
    };

    fn catalog(database: &str) -> VendorCatalog {
        VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: super::super::model::Identifier::new(database).unwrap(),
            namespaces: Vec::new(),
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        }
    }

    fn fingerprint(catalog: &VendorCatalog) -> String {
        hex::encode(Sha256::digest(serde_json::to_vec(catalog).unwrap()))
    }

    fn plan() -> MigrationPlan {
        let source_catalog = catalog("source");
        let target_catalog = catalog("target");
        let source_fingerprint = fingerprint(&source_catalog);
        MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            purpose: PlanPurpose::Execution,
            migration_id: "m1".into(),
            tool_version: "test".into(),
            source_endpoint_identity: "s".into(),
            target_endpoint_identity: AssessmentStatus::Assessed("t".into()),
            source_catalog_fingerprint: source_fingerprint.clone(),
            target_catalog_fingerprint: AssessmentStatus::Assessed(fingerprint(&target_catalog)),
            source_catalog: Some(source_catalog),
            target_catalog: AssessmentStatus::Assessed(target_catalog),
            source_tls_binding: "source-tls".into(),
            target_tls_binding: AssessmentStatus::Assessed("target-tls".into()),
            consistency_mode: "consistent-snapshot".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
            conversion_policy: POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY,
            outage_policy: Some(ReviewedOutagePolicy {
                schema_version: OUTAGE_PROJECTION_SCHEMA_VERSION,
                assessment_digest: "a".repeat(64),
                source_catalog_fingerprint: source_fingerprint,
                byte_basis: ByteBasis::PostgresTotalRelationBytesV1,
                throughput_profile: ThroughputProfile {
                    schema_version: THROUGHPUT_PROFILE_SCHEMA_VERSION,
                    measurement_reference: "run-1".into(),
                    environment_reference: "pg17-local".into(),
                    postgres_major_version: 17,
                    measured_at_unix_seconds: 1_000,
                    valid_for_seconds: 1_000,
                    copy_bytes_per_second: 100,
                    verification_bytes_per_second: 100,
                },
                reviewed_at_unix_seconds: 1_100,
                reviewed_assessed_bytes: 0,
                reviewed_projected_seconds: 0,
                maximum_approved_seconds: 1,
            }),
            postgres_source_profile: None,
            mysql_source_profile: None,
            mysql_snapshot_evidence: None,
            mysql_target_snapshot_evidence: None,
            mysql_metadata_visibility: None,
            mysql_target_metadata_visibility: None,
            mysql_authorization: None,
            capabilities: BTreeMap::new(),
            operations: vec![PlanOperation::new(
                OperationKind::VerifySchema,
                None,
                Vec::new(),
                BTreeMap::new(),
            )
            .unwrap()],
            unsupported_objects: UnsupportedObjectReport::default(),
        }
    }

    fn bind_unproven_mysql_target(plan: &mut MigrationPlan) {
        let target_catalog = match &mut plan.target_catalog {
            AssessmentStatus::Assessed(target) => target,
            AssessmentStatus::NotAssessed => unreachable!(),
        };
        target_catalog.dialect = "mysql".into();
        target_catalog.server_version = "8.4.0".into();
        target_catalog
            .vendor_metadata
            .insert("lower_case_table_names".into(), "0".into());
        let target_fingerprint = fingerprint(target_catalog);
        plan.target_catalog_fingerprint = AssessmentStatus::Assessed(target_fingerprint.clone());
        plan.mysql_target_snapshot_evidence = Some(MySqlSnapshotEvidence {
            endpoint_identity: plan.target_endpoint_identity.as_assessed().unwrap().clone(),
            database_identity: target_catalog.database.to_string(),
            server_uuid: "target-server-uuid".into(),
            server_version: target_catalog.server_version.clone(),
            authenticated_account: "target@%".into(),
            lifecycle_id: "mysql-target-session-1".into(),
            connection_id: 8,
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
            catalog_fingerprint: target_fingerprint,
        });
        plan.unsupported_objects.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::MySqlCatalogSemantics,
            object_id: "mysql-target-catalog-visibility".into(),
            object_kind: "target_catalog_visibility".into(),
            reason: "target metadata visibility is not proven in this fixture".into(),
            required_semantics: true,
        });
    }
    #[test]
    fn hash_is_deterministic_and_tamper_evident() {
        let reviewed = ReviewedPlan::new(plan()).unwrap();
        assert_eq!(reviewed.plan.hash().unwrap(), reviewed.plan_hash);
        let mut changed = reviewed;
        changed.plan.tool_version = "other".into();
        assert!(matches!(
            changed.validate(),
            Err(PlanError::HashMismatch { .. })
        ));
    }

    #[test]
    fn prior_plan_schema_is_rejected_at_the_version_boundary() {
        let mut value = plan();
        value.schema_version = PLAN_SCHEMA_VERSION - 1;
        assert!(matches!(
            value.validate(),
            Err(PlanError::UnsupportedVersion { found })
                if found == PLAN_SCHEMA_VERSION - 1
        ));
    }

    #[test]
    fn cross_dialect_plan_binds_both_catalog_dialects() {
        let mut plan = plan();
        bind_unproven_mysql_target(&mut plan);
        plan.conversion_policy = MigrationConversionPolicy::cross_dialect(
            ConversionDialect::PostgreSql,
            ConversionDialect::MySql,
            Vec::new(),
        )
        .unwrap();
        assert!(plan.validate().is_ok());

        plan.conversion_policy = POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::InvalidConversionPolicy)
        ));
    }

    #[test]
    fn cross_dialect_copy_operation_binds_its_exact_table_policy() {
        let mut plan = plan();
        bind_unproven_mysql_target(&mut plan);
        let source_table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("items").unwrap(),
        };
        let target_table = QualifiedTable {
            namespace: Identifier::new("app").unwrap(),
            name: Identifier::new("items").unwrap(),
        };
        let source_column = ColumnMeta {
            name: Identifier::new("id").unwrap(),
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
        let target_column = target_type
            .column_meta(Identifier::new("id").unwrap(), 1, false)
            .unwrap();
        let table_policy = TableConversionPolicy {
            source_table: source_table.clone(),
            target_table: target_table.clone(),
            target_contract: super::super::conversion::CrossDialectTargetTableContract::MySql {
                engine: super::super::conversion::MySqlTargetEngine::InnoDb,
                character_set: Identifier::new("utf8mb4").unwrap(),
                collation: Identifier::new("utf8mb4_0900_bin").unwrap(),
            },
            resumable_key: CrossDialectResumableKey {
                source_name: Identifier::new("source_pkey").unwrap(),
                source_kind: CrossDialectKeyKind::PrimaryKey,
                source_columns: vec![Identifier::new("id").unwrap()],
                target_name: None,
                target_kind: CrossDialectKeyKind::PrimaryKey,
                target_columns: vec![Identifier::new("id").unwrap()],
            },
            row_policy: RowConversionPolicy {
                schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
                source_dialect: ConversionDialect::PostgreSql,
                target_dialect: ConversionDialect::MySql,
                columns: vec![super::super::conversion::ColumnConversion {
                    source: source_column,
                    source_type: CrossDialectSourceType::PostgreSql(PostgresTargetType::BigInt),
                    target: target_column,
                    target_type,
                    target_checks: Vec::new(),
                    rule: ValueConversionRule::SignedInteger {
                        minimum: i64::MIN,
                        maximum: i64::MAX,
                    },
                }],
            },
        };
        plan.conversion_policy = MigrationConversionPolicy::cross_dialect(
            ConversionDialect::PostgreSql,
            ConversionDialect::MySql,
            vec![table_policy.clone()],
        )
        .unwrap();
        let create = PlanOperation::new(
            OperationKind::CreateTable,
            Some(target_table),
            Vec::new(),
            BTreeMap::from([(
                "cross_dialect_target_table_policy".into(),
                serde_json::to_value(&table_policy).unwrap(),
            )]),
        )
        .unwrap();
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(source_table.clone()),
            vec![create.id.clone()],
            BTreeMap::from([(
                "table_conversion_policy".into(),
                serde_json::to_value(&table_policy).unwrap(),
            )]),
        )
        .unwrap();
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(source_table.clone()),
            vec![copy.id.clone()],
            BTreeMap::from([(
                "cross_dialect_verification_policy".into(),
                serde_json::to_value(&table_policy).unwrap(),
            )]),
        )
        .unwrap();
        let verify_schema = PlanOperation::new(
            OperationKind::VerifySchema,
            None,
            vec![create.id.clone(), copy.id.clone(), verify.id.clone()],
            BTreeMap::new(),
        )
        .unwrap();
        plan.operations = vec![create, copy, verify, verify_schema];
        assert!(plan.validate().is_ok());

        plan.operations = vec![PlanOperation::new(
            OperationKind::CopyTable,
            Some(source_table),
            Vec::new(),
            BTreeMap::new(),
        )
        .unwrap()];
        assert!(matches!(
            plan.validate(),
            Err(PlanError::InvalidConversionPolicy)
        ));
    }

    #[test]
    fn mysql_plan_requires_exact_unprotected_catalog_snapshot_evidence() {
        let mut plan = plan();
        let source_catalog = plan.source_catalog.as_mut().unwrap();
        source_catalog.dialect = "mysql".into();
        source_catalog.server_version = "8.4.0".into();
        let source_fingerprint = fingerprint(source_catalog);
        plan.source_catalog_fingerprint = source_fingerprint.clone();
        let target_catalog = match &mut plan.target_catalog {
            AssessmentStatus::Assessed(catalog) => catalog,
            AssessmentStatus::NotAssessed => unreachable!(),
        };
        target_catalog.dialect = "mysql".into();
        target_catalog.server_version = "8.4.0".into();
        target_catalog
            .vendor_metadata
            .insert("lower_case_table_names".into(), "0".into());
        let target_fingerprint = fingerprint(target_catalog);
        plan.target_catalog_fingerprint = AssessmentStatus::Assessed(target_fingerprint.clone());
        source_catalog
            .vendor_metadata
            .insert("lower_case_table_names".into(), "0".into());
        let source_fingerprint = fingerprint(source_catalog);
        plan.source_catalog_fingerprint = source_fingerprint.clone();
        plan.consistency_mode = "mysql-repeatable-read-consistent-snapshot".into();
        plan.conversion_policy = MYSQL_SAME_DIALECT_CONVERSION_POLICY;
        plan.outage_policy = None;
        plan.mysql_source_profile = Some(
            crate::migration::mysql_profile::MySqlFreezeProfileContract::external_continuous_freeze(
            ),
        );
        plan.mysql_snapshot_evidence = Some(MySqlSnapshotEvidence {
            endpoint_identity: plan.source_endpoint_identity.clone(),
            database_identity: source_catalog.database.to_string(),
            server_uuid: "server-uuid".into(),
            server_version: "8.4.0".into(),
            authenticated_account: "source@%".into(),
            lifecycle_id: "mysql-session-1".into(),
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
            gtid_executed_observation: "uuid:1-7".into(),
            catalog_fingerprint: source_fingerprint,
        });
        plan.mysql_target_snapshot_evidence = Some(MySqlSnapshotEvidence {
            endpoint_identity: plan.target_endpoint_identity.as_assessed().unwrap().clone(),
            database_identity: target_catalog.database.to_string(),
            server_uuid: "target-server-uuid".into(),
            server_version: "8.4.0".into(),
            authenticated_account: "target@%".into(),
            lifecycle_id: "mysql-target-session-1".into(),
            connection_id: 8,
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
            catalog_fingerprint: target_fingerprint,
        });
        plan.unsupported_objects.objects.extend([
            UnsupportedObject {
                code: UnsupportedObjectCode::MySqlCatalogSemantics,
                object_id: "mysql-source-catalog-visibility".into(),
                object_kind: "catalog_visibility".into(),
                reason: "source metadata visibility is not proven in this fixture".into(),
                required_semantics: true,
            },
            UnsupportedObject {
                code: UnsupportedObjectCode::MySqlCatalogSemantics,
                object_id: "mysql-target-catalog-visibility".into(),
                object_kind: "target_catalog_visibility".into(),
                reason: "target metadata visibility is not proven in this fixture".into(),
                required_semantics: true,
            },
        ]);
        assert!(plan.validate().is_ok());

        plan.conversion_policy = POSTGRESQL_SAME_DIALECT_CONVERSION_POLICY;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::InvalidConversionPolicy)
        ));
        plan.conversion_policy = MYSQL_SAME_DIALECT_CONVERSION_POLICY;

        plan.mysql_target_snapshot_evidence
            .as_mut()
            .unwrap()
            .lower_case_table_names = 1;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::InvalidMySqlSnapshotEvidence)
        ));
        plan.mysql_target_snapshot_evidence
            .as_mut()
            .unwrap()
            .lower_case_table_names = 0;

        plan.mysql_snapshot_evidence
            .as_mut()
            .unwrap()
            .catalog_snapshot_protected = true;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::InvalidMySqlSnapshotEvidence)
        ));
        plan.mysql_snapshot_evidence = None;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::MissingEvidence {
                field: "mysql_snapshot_evidence"
            })
        ));
    }
    #[test]
    fn operation_id_changes_with_parameters() {
        let a = PlanOperation::new(OperationKind::CopyTable, None, Vec::new(), BTreeMap::new())
            .unwrap();
        let mut p = BTreeMap::new();
        p.insert("x".into(), serde_json::json!(1));
        let b = PlanOperation::new(OperationKind::CopyTable, None, Vec::new(), p).unwrap();
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn stale_canonical_encoding_is_rejected_at_plan_validation() {
        let mut plan = plan();
        plan.canonical_encoding_version = CANONICAL_ENCODING_VERSION - 1;
        assert!(matches!(
            plan.validate(),
            Err(PlanError::UnsupportedCanonicalEncodingVersion { found })
                if found == CANONICAL_ENCODING_VERSION - 1
        ));
    }

    #[test]
    fn unsupported_object_codes_have_unique_stable_names() {
        let names = UnsupportedObjectCode::ALL
            .iter()
            .map(|code| code.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), UnsupportedObjectCode::ALL.len());
    }

    #[test]
    fn required_unsupported_semantics_block_execution() {
        let mut plan = plan();
        plan.unsupported_objects.objects.push(UnsupportedObject {
            code: UnsupportedObjectCode::Trigger,
            object_id: "trigger-1".into(),
            object_kind: "trigger".into(),
            reason: "fixture adapter cannot reproduce it".into(),
            required_semantics: true,
        });
        assert!(matches!(
            plan.validate_for_execution(),
            Err(PlanError::UnsupportedRequiredSemantics)
        ));
        assert!(ReviewedPlan::new(plan).is_ok());
    }

    #[test]
    fn embedded_catalog_must_match_its_declared_fingerprint() {
        let mut plan = plan();
        plan.source_catalog = Some(catalog("changed"));
        assert!(matches!(
            plan.validate(),
            Err(PlanError::CatalogFingerprintMismatch {
                field: "source_catalog_fingerprint",
                ..
            })
        ));
    }

    #[test]
    fn source_only_assessment_is_structurally_valid_but_not_executable() {
        let mut plan = plan();
        plan.purpose = PlanPurpose::Assessment;
        plan.outage_policy = None;
        plan.target_endpoint_identity = AssessmentStatus::NotAssessed;
        plan.target_catalog_fingerprint = AssessmentStatus::NotAssessed;
        plan.target_catalog = AssessmentStatus::NotAssessed;
        plan.target_tls_binding = AssessmentStatus::NotAssessed;
        plan.consistency_mode.clear();
        plan.conversion_policy = POSTGRESQL_ASSESSMENT_CONVERSION_POLICY;

        assert!(plan.validate().is_ok());
        assert!(matches!(
            plan.validate_for_execution(),
            Err(PlanError::AssessmentCannotExecute)
        ));
        assert!(ReviewedPlan::new(plan).is_ok());
    }

    #[test]
    fn execution_allows_no_outage_policy_but_assessment_rejects_one() {
        let mut execution_plan = plan();
        execution_plan.outage_policy = None;
        assert!(execution_plan.validate().is_ok());
        execution_plan.validate_for_execution().unwrap();

        let mut assessment = execution_plan;
        assessment.purpose = PlanPurpose::Assessment;
        assessment.conversion_policy = POSTGRESQL_ASSESSMENT_CONVERSION_POLICY;
        assessment.outage_policy = Some(plan().outage_policy.unwrap());
        assert!(matches!(
            assessment.validate(),
            Err(PlanError::AssessmentContainsOutagePolicy)
        ));
    }

    #[test]
    fn execution_binds_a_typed_postgres_source_profile() {
        let mut external = plan();
        external.postgres_source_profile =
            Some(PostgresSourceProfileContract::AttestedExternalQuiesce {
                verified_rescan: true,
                freeze_enforced_by_tool: false,
            });
        external.validate_for_execution().unwrap();

        external.consistency_mode = "write-fence".into();
        assert!(matches!(
            external.validate_for_execution(),
            Err(PlanError::PostgresSourceProfile(
                PostgresSourceProfileError::ProfileConsistencyMismatch
            ))
        ));
    }

    #[test]
    fn assessment_rejects_a_postgres_source_profile() {
        let mut assessment = plan();
        assessment.purpose = PlanPurpose::Assessment;
        assessment.outage_policy = None;
        assessment.postgres_source_profile =
            Some(PostgresSourceProfileContract::AttestedExternalQuiesce {
                verified_rescan: true,
                freeze_enforced_by_tool: false,
            });
        assessment.target_endpoint_identity = AssessmentStatus::NotAssessed;
        assessment.target_catalog_fingerprint = AssessmentStatus::NotAssessed;
        assessment.target_catalog = AssessmentStatus::NotAssessed;
        assessment.target_tls_binding = AssessmentStatus::NotAssessed;
        assessment.consistency_mode.clear();
        assessment.conversion_policy = POSTGRESQL_ASSESSMENT_CONVERSION_POLICY;

        assert!(matches!(
            assessment.validate(),
            Err(PlanError::AssessmentContainsSourceProfile)
        ));
    }

    #[test]
    fn execution_requires_complete_target_evidence() {
        let mut plan = plan();
        plan.target_catalog = AssessmentStatus::NotAssessed;

        assert!(matches!(
            plan.validate_for_execution(),
            Err(PlanError::InconsistentTargetAssessment)
        ));

        plan.target_endpoint_identity = AssessmentStatus::NotAssessed;
        plan.target_catalog_fingerprint = AssessmentStatus::NotAssessed;
        plan.target_tls_binding = AssessmentStatus::NotAssessed;
        assert!(matches!(
            plan.validate_for_execution(),
            Err(PlanError::MissingEvidence {
                field: "target_endpoint_identity"
            })
        ));
    }

    #[test]
    fn source_catalog_and_tls_evidence_are_always_required() {
        let mut missing_catalog = plan();
        missing_catalog.source_catalog = None;
        assert!(matches!(
            missing_catalog.validate(),
            Err(PlanError::MissingEvidence {
                field: "source_catalog"
            })
        ));

        let mut plan = plan();
        plan.source_tls_binding.clear();
        assert!(matches!(
            plan.validate(),
            Err(PlanError::EmptyField {
                field: "source_tls_binding"
            })
        ));
    }

    #[test]
    fn reviewed_hash_covers_purpose_and_assessment_status() {
        let reviewed = ReviewedPlan::new(plan()).unwrap();
        let mut changed = reviewed.clone();
        changed.plan.purpose = PlanPurpose::Assessment;
        changed.plan.outage_policy = None;
        changed.plan.conversion_policy = POSTGRESQL_ASSESSMENT_CONVERSION_POLICY;
        assert!(matches!(
            changed.validate(),
            Err(PlanError::HashMismatch { .. })
        ));

        let mut changed = reviewed;
        changed.plan.target_tls_binding = AssessmentStatus::NotAssessed;
        assert!(changed.validate().is_err());
    }
}
