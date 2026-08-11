use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::model::{QualifiedTable, VendorCatalog};
use super::outage_projection::{OutageProjectionError, ReviewedOutagePolicy};
use super::postgres_profile::{PostgresSourceProfileContract, PostgresSourceProfileError};

pub const PLAN_SCHEMA_VERSION: u16 = 8;

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
    pub conversion_policy: String,
    #[serde(default)]
    pub outage_policy: Option<ReviewedOutagePolicy>,
    #[serde(default)]
    pub postgres_source_profile: Option<PostgresSourceProfileContract>,
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
        for (field, value) in [
            ("migration_id", &self.migration_id),
            ("tool_version", &self.tool_version),
            ("source_endpoint_identity", &self.source_endpoint_identity),
            (
                "source_catalog_fingerprint",
                &self.source_catalog_fingerprint,
            ),
            ("source_tls_binding", &self.source_tls_binding),
            ("conversion_policy", &self.conversion_policy),
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
            if self.consistency_mode.is_empty() {
                return Err(PlanError::EmptyField {
                    field: "consistency_mode",
                });
            }
        } else {
            if self.outage_policy.is_some() {
                return Err(PlanError::AssessmentContainsOutagePolicy);
            }
            if self.postgres_source_profile.is_some() {
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
            canonical_encoding_version: 1,
            conversion_policy: "exact".into(),
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
    fn operation_id_changes_with_parameters() {
        let a = PlanOperation::new(OperationKind::CopyTable, None, Vec::new(), BTreeMap::new())
            .unwrap();
        let mut p = BTreeMap::new();
        p.insert("x".into(), serde_json::json!(1));
        let b = PlanOperation::new(OperationKind::CopyTable, None, Vec::new(), p).unwrap();
        assert_ne!(a.id, b.id);
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
        assert!(matches!(
            changed.validate(),
            Err(PlanError::HashMismatch { .. })
        ));

        let mut changed = reviewed;
        changed.plan.target_tls_binding = AssessmentStatus::NotAssessed;
        assert!(changed.validate().is_err());
    }
}
