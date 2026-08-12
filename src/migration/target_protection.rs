//! Accepted target-protection evidence for non-empty migration modes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::model::Identifier;
use super::mysql_visibility::MySqlAccountIdentity;
use super::plan::{AssessmentStatus, ReviewedPlan, TargetModeContract, TargetProtectionContract};

pub const TARGET_PROTECTION_EVIDENCE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetProtectionBinding {
    pub schema_version: u16,
    pub migration_id: String,
    pub plan_hash: String,
    pub target_endpoint_identity: String,
    pub target_catalog_fingerprint: String,
    pub target_mode_hash: String,
    pub activated_at_unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mechanism", rename_all = "snake_case", deny_unknown_fields)]
pub enum AcceptedTargetProtectionEvidence {
    PostgreSqlMigrationTokenFenceV1 {
        binding: TargetProtectionBinding,
        generation: String,
        token_hash: String,
        guard_inventory_fingerprint: String,
        writer_role: Identifier,
        administrator_tls_binding: String,
    },
    MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
        binding: TargetProtectionBinding,
        generation: String,
        token_hash: String,
        guard_inventory_fingerprint: String,
        writer_account: MySqlAccountIdentity,
        administrator_tls_binding: String,
        provider_reference: String,
        ddl_freeze_continuity_token_hash: String,
        backup_lock_connection_id: u32,
        expires_at_unix_seconds: u64,
    },
    ExternalContinuousQuiesceV1 {
        binding: TargetProtectionBinding,
        provider_reference: String,
        continuity_token_hash: String,
        expires_at_unix_seconds: u64,
    },
}

impl AcceptedTargetProtectionEvidence {
    pub fn validate_against(&self, reviewed: &ReviewedPlan) -> Result<(), TargetProtectionError> {
        reviewed
            .validate()
            .map_err(|_| TargetProtectionError::ReviewedPlanMismatch)?;
        let plan = &reviewed.plan;
        let target_endpoint = plan
            .target_endpoint_identity
            .as_assessed()
            .ok_or(TargetProtectionError::ReviewedPlanMismatch)?;
        let target_fingerprint = plan
            .target_catalog_fingerprint
            .as_assessed()
            .ok_or(TargetProtectionError::ReviewedPlanMismatch)?;
        let mode = plan
            .target_mode
            .as_ref()
            .and_then(AssessmentStatus::as_assessed)
            .ok_or(TargetProtectionError::ReviewedPlanMismatch)?;
        let contract = match mode {
            TargetModeContract::WarmMerge(contract) => &contract.target_protection,
            TargetModeContract::StagingSwap(contract) => &contract.target_protection,
            TargetModeContract::EmptyOwned => return Err(TargetProtectionError::WrongTargetMode),
        };
        let binding = self.binding();
        if binding.schema_version != TARGET_PROTECTION_EVIDENCE_SCHEMA_VERSION {
            return Err(TargetProtectionError::UnsupportedVersion {
                found: binding.schema_version,
            });
        }
        if binding.migration_id != plan.migration_id
            || binding.plan_hash != reviewed.plan_hash.as_str()
            || binding.target_endpoint_identity != *target_endpoint
            || binding.target_catalog_fingerprint != *target_fingerprint
            || binding.target_mode_hash != canonical_hash(mode)?
            || binding.activated_at_unix_seconds == 0
        {
            return Err(TargetProtectionError::ReviewedPlanMismatch);
        }
        self.validate_mechanism(contract)
    }

    pub fn canonical_hash(&self, reviewed: &ReviewedPlan) -> Result<String, TargetProtectionError> {
        self.validate_against(reviewed)?;
        canonical_hash(self)
    }

    pub fn binding(&self) -> &TargetProtectionBinding {
        match self {
            Self::PostgreSqlMigrationTokenFenceV1 { binding, .. }
            | Self::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 { binding, .. }
            | Self::ExternalContinuousQuiesceV1 { binding, .. } => binding,
        }
    }

    fn validate_mechanism(
        &self,
        contract: &TargetProtectionContract,
    ) -> Result<(), TargetProtectionError> {
        match (self, contract) {
            (
                Self::PostgreSqlMigrationTokenFenceV1 {
                    generation,
                    token_hash,
                    guard_inventory_fingerprint,
                    writer_role,
                    administrator_tls_binding,
                    ..
                },
                TargetProtectionContract::PostgreSqlMigrationTokenFenceV1,
            ) => {
                require_nonempty(generation)?;
                require_sha256(token_hash)?;
                require_sha256(guard_inventory_fingerprint)?;
                require_nonempty(writer_role.as_str())?;
                require_nonempty(administrator_tls_binding)
            }
            (
                Self::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
                    generation,
                    token_hash,
                    guard_inventory_fingerprint,
                    writer_account,
                    administrator_tls_binding,
                    provider_reference,
                    ddl_freeze_continuity_token_hash,
                    backup_lock_connection_id,
                    expires_at_unix_seconds,
                    ..
                },
                TargetProtectionContract::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
                    provider_reference: expected_provider,
                },
            ) => {
                require_nonempty(generation)?;
                require_sha256(token_hash)?;
                require_sha256(guard_inventory_fingerprint)?;
                writer_account
                    .validate()
                    .map_err(|_| TargetProtectionError::InvalidEvidence)?;
                require_nonempty(administrator_tls_binding)?;
                require_nonempty(provider_reference)?;
                require_sha256(ddl_freeze_continuity_token_hash)?;
                if provider_reference != expected_provider
                    || *backup_lock_connection_id == 0
                    || *expires_at_unix_seconds <= self.binding().activated_at_unix_seconds
                {
                    return Err(TargetProtectionError::MechanismMismatch);
                }
                Ok(())
            }
            (
                Self::ExternalContinuousQuiesceV1 {
                    provider_reference,
                    continuity_token_hash,
                    expires_at_unix_seconds,
                    ..
                },
                TargetProtectionContract::ExternalContinuousQuiesceV1 {
                    provider_reference: expected_provider,
                },
            ) => {
                require_nonempty(provider_reference)?;
                require_sha256(continuity_token_hash)?;
                if provider_reference != expected_provider
                    || *expires_at_unix_seconds <= self.binding().activated_at_unix_seconds
                {
                    return Err(TargetProtectionError::MechanismMismatch);
                }
                Ok(())
            }
            _ => Err(TargetProtectionError::MechanismMismatch),
        }
    }
}

fn canonical_hash(value: &impl Serialize) -> Result<String, TargetProtectionError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn require_nonempty(value: &str) -> Result<(), TargetProtectionError> {
    if value.trim().is_empty() || value.contains('\0') {
        Err(TargetProtectionError::InvalidEvidence)
    } else {
        Ok(())
    }
}

fn require_sha256(value: &str) -> Result<(), TargetProtectionError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(TargetProtectionError::InvalidEvidence)
    }
}

#[derive(Debug, Error)]
pub enum TargetProtectionError {
    #[error("unsupported target-protection evidence version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("target-protection evidence does not match the reviewed plan")]
    ReviewedPlanMismatch,
    #[error("target-protection evidence is not valid for empty-owned target mode")]
    WrongTargetMode,
    #[error("target-protection mechanism or provider differs from the reviewed contract")]
    MechanismMismatch,
    #[error("target-protection evidence is incomplete or malformed")]
    InvalidEvidence,
    #[error("cannot serialize target-protection evidence")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> TargetProtectionBinding {
        TargetProtectionBinding {
            schema_version: TARGET_PROTECTION_EVIDENCE_SCHEMA_VERSION,
            migration_id: "migration-1".into(),
            plan_hash: "a".repeat(64),
            target_endpoint_identity: "target".into(),
            target_catalog_fingerprint: "b".repeat(64),
            target_mode_hash: "c".repeat(64),
            activated_at_unix_seconds: 100,
        }
    }

    #[test]
    fn mysql_evidence_requires_the_reviewed_provider_and_live_continuity() {
        let evidence =
            AcceptedTargetProtectionEvidence::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
                binding: binding(),
                generation: "generation-1".into(),
                token_hash: "d".repeat(64),
                guard_inventory_fingerprint: "e".repeat(64),
                writer_account: MySqlAccountIdentity {
                    user: "writer".into(),
                    host: "host".into(),
                },
                administrator_tls_binding: "admin-tls".into(),
                provider_reference: "provider-1".into(),
                ddl_freeze_continuity_token_hash: "f".repeat(64),
                backup_lock_connection_id: 42,
                expires_at_unix_seconds: 200,
            };
        assert!(evidence
            .validate_mechanism(
                &TargetProtectionContract::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
                    provider_reference: "provider-1".into(),
                }
            )
            .is_ok());
        assert!(matches!(
            evidence.validate_mechanism(
                &TargetProtectionContract::MySqlMigrationTokenFenceWithExternalDdlFreezeV1 {
                    provider_reference: "other".into(),
                }
            ),
            Err(TargetProtectionError::MechanismMismatch)
        ));
    }

    #[test]
    fn evidence_variant_must_equal_the_reviewed_mechanism() {
        let evidence = AcceptedTargetProtectionEvidence::PostgreSqlMigrationTokenFenceV1 {
            binding: binding(),
            generation: "generation-1".into(),
            token_hash: "d".repeat(64),
            guard_inventory_fingerprint: "e".repeat(64),
            writer_role: Identifier::new("writer").unwrap(),
            administrator_tls_binding: "admin-tls".into(),
        };
        assert!(evidence
            .validate_mechanism(&TargetProtectionContract::PostgreSqlMigrationTokenFenceV1)
            .is_ok());
        assert!(matches!(
            evidence.validate_mechanism(&TargetProtectionContract::ExternalContinuousQuiesceV1 {
                provider_reference: "provider-1".into(),
            }),
            Err(TargetProtectionError::MechanismMismatch)
        ));
    }
}
