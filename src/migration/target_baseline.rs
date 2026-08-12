//! Accepted target-row evidence for non-empty migration modes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::model::QualifiedTable;
use super::plan::{
    AssessmentStatus, OperationKind, ReviewedPlan, TargetModeContract, WarmMergeContract,
};

pub const WARM_TARGET_BASELINE_SCHEMA_VERSION: u16 = 1;

/// One target-only table baseline accepted before warm-merge writes begin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedWarmTableBaseline {
    pub table: QualifiedTable,
    pub target_table_object_id: String,
    pub copy_operation_id: String,
    pub baseline_row_count: u64,
    pub baseline_rows_hash: String,
}

/// Execute-time row evidence bound to a reviewed warm-merge plan and target
/// protection boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedWarmTargetBaseline {
    pub schema_version: u16,
    pub migration_id: String,
    pub plan_hash: String,
    pub target_endpoint_identity: String,
    pub target_catalog_fingerprint: String,
    pub target_mode_hash: String,
    pub target_protection_evidence_digest: String,
    pub canonical_encoding_version: u16,
    pub tables: Vec<AcceptedWarmTableBaseline>,
}

impl AcceptedWarmTargetBaseline {
    pub fn new(
        reviewed: &ReviewedPlan,
        target_protection_evidence_digest: String,
        tables: Vec<AcceptedWarmTableBaseline>,
    ) -> Result<Self, TargetBaselineError> {
        reviewed
            .validate()
            .map_err(|_| TargetBaselineError::ReviewedPlanMismatch)?;
        let plan = &reviewed.plan;
        let target_endpoint_identity = plan
            .target_endpoint_identity
            .as_assessed()
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        let target_catalog_fingerprint = plan
            .target_catalog_fingerprint
            .as_assessed()
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        let mode = plan
            .target_mode
            .as_ref()
            .and_then(AssessmentStatus::as_assessed)
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        if !matches!(mode, TargetModeContract::WarmMerge(_)) {
            return Err(TargetBaselineError::WrongTargetMode);
        }
        let accepted = Self {
            schema_version: WARM_TARGET_BASELINE_SCHEMA_VERSION,
            migration_id: plan.migration_id.clone(),
            plan_hash: reviewed.plan_hash.to_string(),
            target_endpoint_identity: target_endpoint_identity.clone(),
            target_catalog_fingerprint: target_catalog_fingerprint.clone(),
            target_mode_hash: canonical_hash(mode)?,
            target_protection_evidence_digest,
            canonical_encoding_version: plan.canonical_encoding_version,
            tables,
        };
        accepted.validate_against(reviewed, &accepted.target_protection_evidence_digest)?;
        Ok(accepted)
    }

    pub fn validate_against(
        &self,
        reviewed: &ReviewedPlan,
        target_protection_evidence_digest: &str,
    ) -> Result<(), TargetBaselineError> {
        reviewed
            .validate()
            .map_err(|_| TargetBaselineError::ReviewedPlanMismatch)?;
        if self.schema_version != WARM_TARGET_BASELINE_SCHEMA_VERSION {
            return Err(TargetBaselineError::UnsupportedVersion {
                found: self.schema_version,
            });
        }
        let plan = &reviewed.plan;
        let target_endpoint = plan
            .target_endpoint_identity
            .as_assessed()
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        let target_fingerprint = plan
            .target_catalog_fingerprint
            .as_assessed()
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        let mode = plan
            .target_mode
            .as_ref()
            .and_then(AssessmentStatus::as_assessed)
            .ok_or(TargetBaselineError::ReviewedPlanMismatch)?;
        let TargetModeContract::WarmMerge(contract) = mode else {
            return Err(TargetBaselineError::WrongTargetMode);
        };
        if self.migration_id != plan.migration_id
            || self.plan_hash != reviewed.plan_hash.as_str()
            || self.target_endpoint_identity != *target_endpoint
            || self.target_catalog_fingerprint != *target_fingerprint
            || self.canonical_encoding_version != plan.canonical_encoding_version
            || self.target_mode_hash != canonical_hash(mode)?
        {
            return Err(TargetBaselineError::ReviewedPlanMismatch);
        }
        require_sha256(target_protection_evidence_digest)?;
        if self.target_protection_evidence_digest != target_protection_evidence_digest {
            return Err(TargetBaselineError::ProtectionEvidenceMismatch);
        }
        self.validate_tables(contract, reviewed)
    }

    pub fn canonical_hash(
        &self,
        reviewed: &ReviewedPlan,
        target_protection_evidence_digest: &str,
    ) -> Result<String, TargetBaselineError> {
        self.validate_against(reviewed, target_protection_evidence_digest)?;
        canonical_hash(self)
    }

    fn validate_tables(
        &self,
        contract: &WarmMergeContract,
        reviewed: &ReviewedPlan,
    ) -> Result<(), TargetBaselineError> {
        if self.tables.len() != contract.tables.len()
            || self
                .tables
                .windows(2)
                .any(|pair| pair[0].table >= pair[1].table)
        {
            return Err(TargetBaselineError::InvalidTableManifest);
        }
        for (accepted, expected) in self.tables.iter().zip(&contract.tables) {
            let operation = reviewed
                .plan
                .operations
                .iter()
                .find(|operation| {
                    operation.kind == OperationKind::CopyTable
                        && operation.table.as_ref() == Some(&expected.table)
                })
                .ok_or(TargetBaselineError::InvalidTableManifest)?;
            if accepted.table != expected.table
                || accepted.target_table_object_id != expected.target_table_object_id
                || accepted.copy_operation_id != operation.id.as_str()
            {
                return Err(TargetBaselineError::InvalidTableManifest);
            }
            require_sha256(&accepted.baseline_rows_hash)?;
        }
        Ok(())
    }
}

fn canonical_hash(value: &impl Serialize) -> Result<String, TargetBaselineError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn require_sha256(value: &str) -> Result<(), TargetBaselineError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(TargetBaselineError::InvalidDigest)
    }
}

#[derive(Debug, Error)]
pub enum TargetBaselineError {
    #[error("unsupported accepted warm-target baseline version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("accepted target baseline does not match the reviewed plan")]
    ReviewedPlanMismatch,
    #[error("accepted target baseline requires warm-merge target mode")]
    WrongTargetMode,
    #[error("accepted target baseline does not match the target-protection evidence")]
    ProtectionEvidenceMismatch,
    #[error("accepted target baseline contains an invalid SHA-256 digest")]
    InvalidDigest,
    #[error("accepted target baseline table manifest is incomplete or inconsistent")]
    InvalidTableManifest,
    #[error("cannot serialize accepted target baseline evidence")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::migration::canonical::CANONICAL_ENCODING_VERSION;
    use crate::migration::conversion::ConversionDialect;
    use crate::migration::model::{
        CatalogNamespace, CatalogObject, CatalogObjectKind, Identifier, VendorCatalog,
    };
    use crate::migration::plan::{
        MigrationPlan, PlanOperation, PlanPurpose, TargetCatalogItem, TargetObjectDisposition,
        TargetOwnershipClaim, TargetOwnershipManifest, TargetProtectionContract,
        UnsupportedObjectReport, WarmMergeConflictPolicy, WarmMergeTable, PLAN_SCHEMA_VERSION,
    };

    fn fingerprint(catalog: &VendorCatalog) -> String {
        hex::encode(Sha256::digest(serde_json::to_vec(catalog).unwrap()))
    }

    fn reviewed() -> ReviewedPlan {
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("items").unwrap(),
        };
        let source = VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new("source").unwrap(),
            namespaces: Vec::new(),
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };
        let target = VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new("target").unwrap(),
            namespaces: vec![CatalogNamespace {
                id: "namespace:public".into(),
                name: table.namespace.clone(),
                owner: None,
                charset: None,
                collation: None,
                objects: vec![CatalogObject {
                    id: "relation:items".into(),
                    kind: CatalogObjectKind::Table,
                    name: table.name.clone(),
                    definition: Vec::new(),
                    attributes: BTreeMap::new(),
                }],
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(table.clone()),
            Vec::new(),
            BTreeMap::new(),
        )
        .unwrap();
        let source_fingerprint = fingerprint(&source);
        let target_fingerprint = fingerprint(&target);
        ReviewedPlan::new(MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            purpose: PlanPurpose::Execution,
            migration_id: "migration-1".into(),
            tool_version: "test".into(),
            source_endpoint_identity: "source-endpoint".into(),
            target_endpoint_identity: AssessmentStatus::Assessed("target-endpoint".into()),
            source_catalog_fingerprint: source_fingerprint,
            target_catalog_fingerprint: AssessmentStatus::Assessed(target_fingerprint.clone()),
            source_catalog: Some(source),
            target_catalog: AssessmentStatus::Assessed(target),
            source_tls_binding: "source-tls".into(),
            target_tls_binding: AssessmentStatus::Assessed("target-tls".into()),
            target_mode: Some(AssessmentStatus::Assessed(TargetModeContract::WarmMerge(
                WarmMergeContract {
                    conflict_policy: WarmMergeConflictPolicy::RejectAnyKeyCollision,
                    target_protection: TargetProtectionContract::PostgreSqlMigrationTokenFenceV1,
                    ownership: TargetOwnershipManifest {
                        target_catalog_fingerprint: target_fingerprint,
                        claims: vec![
                            TargetOwnershipClaim {
                                item: TargetCatalogItem::Namespace("namespace:public".into()),
                                disposition: TargetObjectDisposition::Preserve,
                            },
                            TargetOwnershipClaim {
                                item: TargetCatalogItem::Object("relation:items".into()),
                                disposition: TargetObjectDisposition::MergeRows,
                            },
                        ],
                    },
                    tables: vec![WarmMergeTable {
                        table,
                        target_table_object_id: "relation:items".into(),
                    }],
                },
            ))),
            consistency_mode: "write-fence".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
            conversion_policy:
                super::super::conversion::MigrationConversionPolicy::same_dialect_exact(
                    ConversionDialect::PostgreSql,
                ),
            outage_policy: None,
            postgres_source_profile: None,
            mysql_source_profile: None,
            mysql_snapshot_evidence: None,
            mysql_target_snapshot_evidence: None,
            mysql_metadata_visibility: None,
            mysql_target_metadata_visibility: None,
            mysql_authorization: None,
            capabilities: BTreeMap::new(),
            operations: vec![copy],
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap()
    }

    fn accepted(reviewed: &ReviewedPlan) -> AcceptedWarmTargetBaseline {
        let TargetModeContract::WarmMerge(contract) = reviewed
            .plan
            .target_mode
            .as_ref()
            .and_then(AssessmentStatus::as_assessed)
            .unwrap()
        else {
            unreachable!()
        };
        let copy = reviewed
            .plan
            .operations
            .iter()
            .find(|operation| operation.kind == OperationKind::CopyTable)
            .unwrap();
        AcceptedWarmTargetBaseline::new(
            reviewed,
            "a".repeat(64),
            vec![AcceptedWarmTableBaseline {
                table: contract.tables[0].table.clone(),
                target_table_object_id: contract.tables[0].target_table_object_id.clone(),
                copy_operation_id: copy.id.to_string(),
                baseline_row_count: 3,
                baseline_rows_hash: "b".repeat(64),
            }],
        )
        .unwrap()
    }

    #[test]
    fn accepted_baseline_is_plan_bound_and_tamper_evident() {
        let reviewed = reviewed();
        let baseline = accepted(&reviewed);
        let protection_digest = "a".repeat(64);
        assert!(baseline
            .validate_against(&reviewed, &protection_digest)
            .is_ok());
        assert_eq!(
            baseline
                .canonical_hash(&reviewed, &protection_digest)
                .unwrap()
                .len(),
            64
        );

        let mut changed = baseline;
        changed.tables[0].baseline_row_count += 1;
        assert_ne!(
            changed
                .canonical_hash(&reviewed, &protection_digest)
                .unwrap(),
            accepted(&reviewed)
                .canonical_hash(&reviewed, &protection_digest)
                .unwrap()
        );
    }

    #[test]
    fn accepted_baseline_rejects_wrong_operation_and_invalid_digest() {
        let reviewed = reviewed();
        let protection_digest = "a".repeat(64);
        let mut baseline = accepted(&reviewed);
        baseline.tables[0].copy_operation_id = "0".repeat(64);
        assert!(matches!(
            baseline.validate_against(&reviewed, &protection_digest),
            Err(TargetBaselineError::InvalidTableManifest)
        ));

        let mut baseline = accepted(&reviewed);
        baseline.target_protection_evidence_digest = "not-a-digest".into();
        assert!(matches!(
            baseline.validate_against(&reviewed, &protection_digest),
            Err(TargetBaselineError::ProtectionEvidenceMismatch)
        ));

        let baseline = accepted(&reviewed);
        assert!(matches!(
            baseline.validate_against(&reviewed, &"c".repeat(64)),
            Err(TargetBaselineError::ProtectionEvidenceMismatch)
        ));
        assert!(matches!(
            baseline.validate_against(&reviewed, "not-a-digest"),
            Err(TargetBaselineError::InvalidDigest)
        ));
    }
}
