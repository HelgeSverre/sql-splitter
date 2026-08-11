use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::model::{QualifiedTable, VendorCatalog};

pub const PLAN_SCHEMA_VERSION: u16 = 3;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedObject {
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
    pub migration_id: String,
    pub tool_version: String,
    pub source_endpoint_identity: String,
    pub target_endpoint_identity: String,
    pub source_catalog_fingerprint: String,
    pub target_catalog_fingerprint: String,
    pub source_catalog: Option<VendorCatalog>,
    pub target_catalog: Option<VendorCatalog>,
    pub consistency_mode: String,
    pub canonical_encoding_version: u16,
    pub conversion_policy: String,
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
            ("target_endpoint_identity", &self.target_endpoint_identity),
            (
                "source_catalog_fingerprint",
                &self.source_catalog_fingerprint,
            ),
            (
                "target_catalog_fingerprint",
                &self.target_catalog_fingerprint,
            ),
            ("consistency_mode", &self.consistency_mode),
            ("conversion_policy", &self.conversion_policy),
        ] {
            if value.is_empty() {
                return Err(PlanError::EmptyField { field });
            }
        }
        for (field, catalog, expected) in [
            (
                "source_catalog_fingerprint",
                self.source_catalog.as_ref(),
                &self.source_catalog_fingerprint,
            ),
            (
                "target_catalog_fingerprint",
                self.target_catalog.as_ref(),
                &self.target_catalog_fingerprint,
            ),
        ] {
            if let Some(catalog) = catalog {
                let actual = hex::encode(Sha256::digest(serde_json::to_vec(catalog)?));
                if &actual != expected {
                    return Err(PlanError::CatalogFingerprintMismatch { field, actual });
                }
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
        if self.unsupported_objects.blocks_execution() {
            return Err(PlanError::UnsupportedRequiredSemantics);
        }
        Ok(())
    }
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
    #[error("{field} does not match the embedded catalog; actual fingerprint is {actual}")]
    CatalogFingerprintMismatch { field: &'static str, actual: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan() -> MigrationPlan {
        MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            migration_id: "m1".into(),
            tool_version: "test".into(),
            source_endpoint_identity: "s".into(),
            target_endpoint_identity: "t".into(),
            source_catalog_fingerprint: "sf".into(),
            target_catalog_fingerprint: "tf".into(),
            source_catalog: None,
            target_catalog: None,
            consistency_mode: "consistent_snapshot".into(),
            canonical_encoding_version: 1,
            conversion_policy: "exact".into(),
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
    fn required_unsupported_semantics_block_execution() {
        let mut plan = plan();
        plan.unsupported_objects.objects.push(UnsupportedObject {
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
        plan.source_catalog = Some(VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: super::super::model::Identifier::new("app").unwrap(),
            namespaces: Vec::new(),
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        });
        assert!(matches!(
            plan.validate(),
            Err(PlanError::CatalogFingerprintMismatch {
                field: "source_catalog_fingerprint",
                ..
            })
        ));
    }
}
