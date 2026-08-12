//! Typed evidence for the MySQL external continuous-freeze profile.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const MYSQL_FREEZE_PROFILE_SCHEMA_VERSION: u16 = 1;
pub const MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlFreezeProfileKind {
    ExternalContinuousFreezeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlFreezeProfileContract {
    pub schema_version: u16,
    pub profile: MySqlFreezeProfileKind,
    pub freeze_enforced_by_tool: bool,
    pub reject_active_replication_appliers: bool,
    pub require_persisted_super_read_only: bool,
}

impl MySqlFreezeProfileContract {
    pub fn external_continuous_freeze() -> Self {
        Self {
            schema_version: MYSQL_FREEZE_PROFILE_SCHEMA_VERSION,
            profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
            freeze_enforced_by_tool: false,
            reject_active_replication_appliers: true,
            require_persisted_super_read_only: true,
        }
    }

    pub fn validate(&self) -> Result<(), MySqlFreezeProfileError> {
        if self.schema_version != MYSQL_FREEZE_PROFILE_SCHEMA_VERSION {
            return Err(MySqlFreezeProfileError::UnsupportedVersion);
        }
        if self.profile != MySqlFreezeProfileKind::ExternalContinuousFreezeV1
            || self.freeze_enforced_by_tool
            || !self.reject_active_replication_appliers
            || !self.require_persisted_super_read_only
        {
            return Err(MySqlFreezeProfileError::InvalidProfile);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlFreezeAttestationStatus {
    Active,
    Withdrawn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlDmlFreezeMechanism {
    PersistedSuperReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlDdlFreezeMechanism {
    ExternalBackupLock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlExternalFreezeAssertion {
    pub schema_version: u16,
    pub profile_generation: String,
    pub provider_reference: String,
    pub activated_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    /// SHA-256 of the provider-owned continuity token. The raw token is not
    /// stored in the migration artifact.
    pub continuity_token_hash: String,
    pub backup_lock_connection_id: u32,
    pub backup_lock_owner_user: String,
    pub backup_lock_owner_host: String,
}

impl MySqlExternalFreezeAssertion {
    pub fn validate(&self) -> Result<(), MySqlFreezeProfileError> {
        if self.schema_version != MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION {
            return Err(MySqlFreezeProfileError::UnsupportedVersion);
        }
        require_nonempty(&self.profile_generation)?;
        require_nonempty(&self.provider_reference)?;
        require_sha256(&self.continuity_token_hash)?;
        require_nonempty(&self.backup_lock_owner_user)?;
        require_nonempty(&self.backup_lock_owner_host)?;
        if self.activated_at_unix_seconds == 0
            || self.expires_at_unix_seconds <= self.activated_at_unix_seconds
            || self.backup_lock_connection_id == 0
        {
            return Err(MySqlFreezeProfileError::InvalidAssertion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlExternalFreezeAttestation {
    pub schema_version: u16,
    pub profile: MySqlFreezeProfileKind,
    pub status: MySqlFreezeAttestationStatus,
    pub source_endpoint_identity: String,
    pub source_database_identity: String,
    pub source_catalog_fingerprint: String,
    pub server_uuid: String,
    pub administrator_tls_binding: String,
    pub profile_generation: String,
    pub provider_reference: String,
    pub activated_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    pub continuity_token_hash: String,
    pub backup_lock_connection_id: u32,
    pub backup_lock_owner_user: String,
    pub backup_lock_owner_host: String,
    pub read_only: bool,
    pub super_read_only: bool,
    pub super_read_only_persisted: bool,
    pub active_replication_channels: Vec<String>,
    pub dml_mechanism: MySqlDmlFreezeMechanism,
    pub ddl_mechanism: MySqlDdlFreezeMechanism,
    pub freeze_enforced_by_tool: bool,
    pub attested_at_unix_seconds: u64,
}

impl MySqlExternalFreezeAttestation {
    pub fn validate(&self) -> Result<(), MySqlFreezeProfileError> {
        if self.schema_version != MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION {
            return Err(MySqlFreezeProfileError::UnsupportedVersion);
        }
        for value in [
            &self.source_endpoint_identity,
            &self.source_database_identity,
            &self.server_uuid,
            &self.administrator_tls_binding,
            &self.profile_generation,
            &self.provider_reference,
            &self.backup_lock_owner_user,
            &self.backup_lock_owner_host,
        ] {
            require_nonempty(value)?;
        }
        require_sha256(&self.source_catalog_fingerprint)?;
        require_sha256(&self.continuity_token_hash)?;
        if self.profile != MySqlFreezeProfileKind::ExternalContinuousFreezeV1
            || self.status != MySqlFreezeAttestationStatus::Active
            || self.activated_at_unix_seconds == 0
            || self.attested_at_unix_seconds < self.activated_at_unix_seconds
            || self.expires_at_unix_seconds <= self.attested_at_unix_seconds
            || self.backup_lock_connection_id == 0
            || !self.read_only
            || !self.super_read_only
            || !self.super_read_only_persisted
            || !self.active_replication_channels.is_empty()
            || self.dml_mechanism != MySqlDmlFreezeMechanism::PersistedSuperReadOnly
            || self.ddl_mechanism != MySqlDdlFreezeMechanism::ExternalBackupLock
            || self.freeze_enforced_by_tool
        {
            return Err(MySqlFreezeProfileError::InvalidAttestation);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, MySqlFreezeProfileError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| MySqlFreezeProfileError::Serialization)?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }

    pub fn matches_assertion(&self, assertion: &MySqlExternalFreezeAssertion) -> bool {
        self.profile_generation == assertion.profile_generation
            && self.provider_reference == assertion.provider_reference
            && self.activated_at_unix_seconds == assertion.activated_at_unix_seconds
            && self.expires_at_unix_seconds == assertion.expires_at_unix_seconds
            && self.continuity_token_hash == assertion.continuity_token_hash
            && self.backup_lock_connection_id == assertion.backup_lock_connection_id
            && self.backup_lock_owner_user == assertion.backup_lock_owner_user
            && self.backup_lock_owner_host == assertion.backup_lock_owner_host
    }

    pub fn same_continuity_as(&self, accepted: &Self) -> bool {
        self.profile == accepted.profile
            && self.source_endpoint_identity == accepted.source_endpoint_identity
            && self.source_database_identity == accepted.source_database_identity
            && self.source_catalog_fingerprint == accepted.source_catalog_fingerprint
            && self.server_uuid == accepted.server_uuid
            && self.administrator_tls_binding == accepted.administrator_tls_binding
            && self.profile_generation == accepted.profile_generation
            && self.provider_reference == accepted.provider_reference
            && self.activated_at_unix_seconds == accepted.activated_at_unix_seconds
            && self.expires_at_unix_seconds == accepted.expires_at_unix_seconds
            && self.continuity_token_hash == accepted.continuity_token_hash
            && self.backup_lock_connection_id == accepted.backup_lock_connection_id
            && self.backup_lock_owner_user == accepted.backup_lock_owner_user
            && self.backup_lock_owner_host == accepted.backup_lock_owner_host
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MySqlFreezeProfileError {
    #[error("unsupported MySQL freeze profile schema version")]
    UnsupportedVersion,
    #[error("invalid MySQL freeze profile contract")]
    InvalidProfile,
    #[error("invalid MySQL external freeze assertion")]
    InvalidAssertion,
    #[error("invalid MySQL external freeze attestation")]
    InvalidAttestation,
    #[error("required MySQL freeze evidence field is empty")]
    EmptyField,
    #[error("required MySQL freeze evidence digest is not SHA-256")]
    InvalidDigest,
    #[error("cannot serialize MySQL freeze evidence")]
    Serialization,
}

fn require_nonempty(value: &str) -> Result<(), MySqlFreezeProfileError> {
    if value.trim().is_empty() {
        Err(MySqlFreezeProfileError::EmptyField)
    } else {
        Ok(())
    }
}

fn require_sha256(value: &str) -> Result<(), MySqlFreezeProfileError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(MySqlFreezeProfileError::InvalidDigest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attestation() -> MySqlExternalFreezeAttestation {
        MySqlExternalFreezeAttestation {
            schema_version: MYSQL_FREEZE_ATTESTATION_SCHEMA_VERSION,
            profile: MySqlFreezeProfileKind::ExternalContinuousFreezeV1,
            status: MySqlFreezeAttestationStatus::Active,
            source_endpoint_identity: "mysql://source:3306/app?server_uuid=uuid".into(),
            source_database_identity: "app".into(),
            source_catalog_fingerprint: "a".repeat(64),
            server_uuid: "uuid".into(),
            administrator_tls_binding: "tls".into(),
            profile_generation: "generation-1".into(),
            provider_reference: "change-1".into(),
            activated_at_unix_seconds: 10,
            expires_at_unix_seconds: 30,
            continuity_token_hash: "b".repeat(64),
            backup_lock_connection_id: 42,
            backup_lock_owner_user: "freeze_owner".into(),
            backup_lock_owner_host: "10.0.0.1".into(),
            read_only: true,
            super_read_only: true,
            super_read_only_persisted: true,
            active_replication_channels: Vec::new(),
            dml_mechanism: MySqlDmlFreezeMechanism::PersistedSuperReadOnly,
            ddl_mechanism: MySqlDdlFreezeMechanism::ExternalBackupLock,
            freeze_enforced_by_tool: false,
            attested_at_unix_seconds: 20,
        }
    }

    #[test]
    fn attestation_hash_binds_continuity_and_lock_owner() {
        let original = attestation();
        let original_hash = original.canonical_hash().unwrap();
        let mut changed = original.clone();
        changed.backup_lock_connection_id += 1;
        assert_ne!(changed.canonical_hash().unwrap(), original_hash);
        changed = original;
        changed.continuity_token_hash = "c".repeat(64);
        assert_ne!(changed.canonical_hash().unwrap(), original_hash);
    }

    #[test]
    fn withdrawn_or_replica_attestation_fails_closed() {
        let mut value = attestation();
        value.status = MySqlFreezeAttestationStatus::Withdrawn;
        assert_eq!(
            value.validate(),
            Err(MySqlFreezeProfileError::InvalidAttestation)
        );
        value = attestation();
        value.active_replication_channels.push("channel-1".into());
        assert_eq!(
            value.validate(),
            Err(MySqlFreezeProfileError::InvalidAttestation)
        );
    }
}
