//! Typed evidence contracts for PostgreSQL managed-source profiles.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::connection::SnapshotToken;
use super::postgres::{validate_sequence, PostgresSequence, POSTGRES_CONSISTENCY_SNAPSHOT};

pub const POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION: u16 = 1;
pub const POSTGRES_EXTERNAL_QUIESCE_ATTESTATION_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSourceProfileKind {
    SelfManagedAdministrator,
    ManagedAdministrator,
    AttestedExternalQuiesce,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresSourceProfileContract {
    SelfManagedAdministrator {
        probe_artifact: PostgresSourceProbeArtifact,
        probe_artifact_digest: String,
    },
    ManagedAdministrator {
        probe_artifact: PostgresSourceProbeArtifact,
        probe_artifact_digest: String,
    },
    AttestedExternalQuiesce {
        verified_rescan: bool,
        freeze_enforced_by_tool: bool,
    },
}

impl PostgresSourceProfileContract {
    pub fn kind(&self) -> PostgresSourceProfileKind {
        match self {
            Self::SelfManagedAdministrator { .. } => {
                PostgresSourceProfileKind::SelfManagedAdministrator
            }
            Self::ManagedAdministrator { .. } => PostgresSourceProfileKind::ManagedAdministrator,
            Self::AttestedExternalQuiesce { .. } => {
                PostgresSourceProfileKind::AttestedExternalQuiesce
            }
        }
    }

    pub fn validate_for_plan(
        &self,
        source_endpoint_identity: &str,
        source_catalog_fingerprint: &str,
        consistency_mode: &str,
    ) -> Result<(), PostgresSourceProfileError> {
        require_nonempty(source_endpoint_identity)?;
        require_sha256(source_catalog_fingerprint)?;
        match self {
            Self::SelfManagedAdministrator {
                probe_artifact,
                probe_artifact_digest,
            }
            | Self::ManagedAdministrator {
                probe_artifact,
                probe_artifact_digest,
            } => {
                if consistency_mode != "write-fence" {
                    return Err(PostgresSourceProfileError::ProfileConsistencyMismatch);
                }
                probe_artifact.require_all_proven()?;
                if probe_artifact.profile != self.kind()
                    || probe_artifact.source_endpoint_identity != source_endpoint_identity
                    || probe_artifact.source_catalog_fingerprint != source_catalog_fingerprint
                    || probe_artifact.canonical_hash()? != *probe_artifact_digest
                {
                    return Err(PostgresSourceProfileError::ProfileEvidenceMismatch);
                }
            }
            Self::AttestedExternalQuiesce {
                freeze_enforced_by_tool,
                ..
            } => {
                if consistency_mode != POSTGRES_CONSISTENCY_SNAPSHOT {
                    return Err(PostgresSourceProfileError::ProfileConsistencyMismatch);
                }
                if *freeze_enforced_by_tool {
                    return Err(PostgresSourceProfileError::ExternalFreezeMisrepresented);
                }
            }
        }
        Ok(())
    }
}

impl PostgresSourceProfileKind {
    pub fn requires_privilege_probe(self) -> bool {
        matches!(
            self,
            Self::SelfManagedAdministrator | Self::ManagedAdministrator
        )
    }

    pub fn requires_external_attestation(self) -> bool {
        self == Self::AttestedExternalQuiesce
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresSourceProbeRequirement {
    PlannedTableLockPrivileges,
    PlannedTableTriggerPrivileges,
    SequenceOwnershipAndPrivileges,
    TransactionalEventTriggerExercise,
    TransactionalRegistryWriteExercise,
    SacrificialBackendTermination,
}

impl PostgresSourceProbeRequirement {
    pub const ALL: [Self; 6] = [
        Self::PlannedTableLockPrivileges,
        Self::PlannedTableTriggerPrivileges,
        Self::SequenceOwnershipAndPrivileges,
        Self::TransactionalEventTriggerExercise,
        Self::TransactionalRegistryWriteExercise,
        Self::SacrificialBackendTermination,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresSourceProbeStatus {
    Proven,
    Unavailable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceProbeResult {
    pub requirement: PostgresSourceProbeRequirement,
    pub status: PostgresSourceProbeStatus,
    pub object_ids: Vec<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceProbeArtifact {
    pub schema_version: u16,
    pub profile: PostgresSourceProfileKind,
    pub source_endpoint_identity: String,
    pub source_catalog_fingerprint: String,
    pub administrator_role: String,
    pub probed_at_unix_seconds: u64,
    pub results: Vec<PostgresSourceProbeResult>,
}

impl PostgresSourceProbeArtifact {
    pub fn validate(&self) -> Result<(), PostgresSourceProfileError> {
        if self.schema_version != POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION {
            return Err(PostgresSourceProfileError::UnsupportedVersion);
        }
        if !self.profile.requires_privilege_probe() {
            return Err(PostgresSourceProfileError::ProfileEvidenceMismatch);
        }
        require_nonempty(&self.source_endpoint_identity)?;
        require_sha256(&self.source_catalog_fingerprint)?;
        require_nonempty(&self.administrator_role)?;
        if self.probed_at_unix_seconds == 0 {
            return Err(PostgresSourceProfileError::InvalidTimestamp);
        }
        if self.results.len() != PostgresSourceProbeRequirement::ALL.len() {
            return Err(PostgresSourceProfileError::IncompleteProbeSet);
        }
        if self
            .results
            .windows(2)
            .any(|pair| pair[0].requirement >= pair[1].requirement)
        {
            return Err(PostgresSourceProfileError::IncompleteProbeSet);
        }
        let mut requirements = BTreeSet::new();
        for result in &self.results {
            if !requirements.insert(result.requirement) {
                return Err(PostgresSourceProfileError::DuplicateProbeRequirement);
            }
            require_nonempty(&result.detail)?;
            if result.object_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(PostgresSourceProfileError::InvalidObjectManifest);
            }
            match &result.status {
                PostgresSourceProbeStatus::Proven => {}
                PostgresSourceProbeStatus::Unavailable { reason } => require_nonempty(reason)?,
            }
        }
        if requirements
            != PostgresSourceProbeRequirement::ALL
                .into_iter()
                .collect::<BTreeSet<_>>()
        {
            return Err(PostgresSourceProfileError::IncompleteProbeSet);
        }
        Ok(())
    }

    pub fn require_all_proven(&self) -> Result<(), PostgresSourceProfileError> {
        self.validate()?;
        if self
            .results
            .iter()
            .all(|result| result.status == PostgresSourceProbeStatus::Proven)
        {
            Ok(())
        } else {
            Err(PostgresSourceProfileError::ProbeNotProven)
        }
    }

    pub fn canonical_hash(&self) -> Result<String, PostgresSourceProfileError> {
        self.validate()?;
        canonical_hash(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresExternalQuiesceStatus {
    Active,
    Withdrawn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresExternalQuiesceAttestation {
    pub schema_version: u16,
    pub source_endpoint_identity: String,
    pub source_catalog_fingerprint: String,
    pub attestation_reference: String,
    #[serde(deserialize_with = "deserialize_required_nullable_string")]
    pub previous_attestation_digest: Option<String>,
    pub issued_at_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
    pub status: PostgresExternalQuiesceStatus,
}

fn deserialize_required_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

impl PostgresExternalQuiesceAttestation {
    pub fn validate(&self) -> Result<(), PostgresSourceProfileError> {
        if self.schema_version != POSTGRES_EXTERNAL_QUIESCE_ATTESTATION_SCHEMA_VERSION {
            return Err(PostgresSourceProfileError::UnsupportedVersion);
        }
        require_nonempty(&self.source_endpoint_identity)?;
        require_sha256(&self.source_catalog_fingerprint)?;
        require_nonempty(&self.attestation_reference)?;
        if let Some(digest) = &self.previous_attestation_digest {
            require_sha256(digest)?;
        }
        if self.issued_at_unix_seconds == 0
            || self.expires_at_unix_seconds <= self.issued_at_unix_seconds
        {
            return Err(PostgresSourceProfileError::InvalidTimestamp);
        }
        Ok(())
    }

    pub fn validate_initial(&self) -> Result<(), PostgresSourceProfileError> {
        self.validate()?;
        if self.previous_attestation_digest.is_some() {
            return Err(PostgresSourceProfileError::InvalidAttestationChain);
        }
        Ok(())
    }

    pub fn validate_renewal_from(&self, previous: &Self) -> Result<(), PostgresSourceProfileError> {
        self.validate()?;
        previous.validate()?;
        let previous_digest = previous.canonical_hash()?;
        if self.previous_attestation_digest.as_deref() != Some(previous_digest.as_str())
            || self.source_endpoint_identity != previous.source_endpoint_identity
            || self.source_catalog_fingerprint != previous.source_catalog_fingerprint
            || self.attestation_reference != previous.attestation_reference
            || self.status != PostgresExternalQuiesceStatus::Active
            || previous.status != PostgresExternalQuiesceStatus::Active
            || self.issued_at_unix_seconds < previous.issued_at_unix_seconds
            || self.issued_at_unix_seconds > previous.expires_at_unix_seconds
            || self.expires_at_unix_seconds <= previous.expires_at_unix_seconds
        {
            return Err(PostgresSourceProfileError::InvalidAttestationChain);
        }
        Ok(())
    }

    pub fn require_active_at(
        &self,
        observed_at_unix_seconds: u64,
    ) -> Result<(), PostgresSourceProfileError> {
        self.validate()?;
        if self.status != PostgresExternalQuiesceStatus::Active {
            return Err(PostgresSourceProfileError::AttestationWithdrawn);
        }
        if observed_at_unix_seconds < self.issued_at_unix_seconds
            || observed_at_unix_seconds > self.expires_at_unix_seconds
        {
            return Err(PostgresSourceProfileError::AttestationExpired);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, PostgresSourceProfileError> {
        self.validate()?;
        canonical_hash(self)
    }

    pub fn validate_for_plan(
        &self,
        source_endpoint_identity: &str,
        source_catalog_fingerprint: &str,
        observed_at_unix_seconds: u64,
    ) -> Result<(), PostgresSourceProfileError> {
        self.require_active_at(observed_at_unix_seconds)?;
        if self.source_endpoint_identity != source_endpoint_identity
            || self.source_catalog_fingerprint != source_catalog_fingerprint
        {
            return Err(PostgresSourceProfileError::ProfileEvidenceMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSequenceEqualityEvidence {
    pub schema_version: u16,
    pub source_catalog_fingerprint: String,
    pub initial: Vec<PostgresSequence>,
    pub final_observation: Vec<PostgresSequence>,
    pub verified_at_unix_seconds: u64,
}

impl PostgresSequenceEqualityEvidence {
    pub fn new(
        source_catalog_fingerprint: String,
        initial: Vec<PostgresSequence>,
        final_observation: Vec<PostgresSequence>,
        verified_at_unix_seconds: u64,
    ) -> Result<Self, PostgresSourceProfileError> {
        let evidence = Self {
            schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
            source_catalog_fingerprint,
            initial,
            final_observation,
            verified_at_unix_seconds,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<(), PostgresSourceProfileError> {
        if self.schema_version != POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION {
            return Err(PostgresSourceProfileError::UnsupportedVersion);
        }
        require_sha256(&self.source_catalog_fingerprint)?;
        if self.verified_at_unix_seconds == 0 {
            return Err(PostgresSourceProfileError::InvalidTimestamp);
        }
        validate_sequence_manifest(&self.initial)?;
        validate_sequence_manifest(&self.final_observation)?;
        if self.initial != self.final_observation {
            return Err(PostgresSourceProfileError::SequenceStateDrift);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, PostgresSourceProfileError> {
        self.validate()?;
        canonical_hash(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresExternalQuiesceRescanTableEvidence {
    pub operation_id: String,
    pub chunk_count: u64,
    pub row_count: u64,
    pub manifest_hash: String,
    pub fresh_source_hash: String,
    pub target_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresExternalQuiesceRescanEvidence {
    pub schema_version: u16,
    pub source_catalog_fingerprint: String,
    pub source_endpoint_identity: String,
    pub source_database_identity: String,
    pub source_snapshot_id: String,
    pub source_lifecycle_id: String,
    pub source_server_version: String,
    pub source_read_only_server_enforced: bool,
    pub tables: Vec<PostgresExternalQuiesceRescanTableEvidence>,
    pub verified_at_unix_seconds: u64,
}

impl PostgresExternalQuiesceRescanEvidence {
    pub fn new(
        source_catalog_fingerprint: String,
        snapshot: &SnapshotToken,
        source_read_only_server_enforced: bool,
        tables: Vec<PostgresExternalQuiesceRescanTableEvidence>,
        verified_at_unix_seconds: u64,
    ) -> Result<Self, PostgresSourceProfileError> {
        let evidence = Self {
            schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
            source_catalog_fingerprint,
            source_endpoint_identity: snapshot.endpoint_identity.clone(),
            source_database_identity: snapshot.database_identity.clone(),
            source_snapshot_id: snapshot.snapshot_id.clone(),
            source_lifecycle_id: snapshot.lifecycle_id.clone(),
            source_server_version: snapshot.server_version.clone(),
            source_read_only_server_enforced,
            tables,
            verified_at_unix_seconds,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<(), PostgresSourceProfileError> {
        if self.schema_version != POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION {
            return Err(PostgresSourceProfileError::UnsupportedVersion);
        }
        require_sha256(&self.source_catalog_fingerprint)?;
        for value in [
            &self.source_endpoint_identity,
            &self.source_database_identity,
            &self.source_snapshot_id,
            &self.source_lifecycle_id,
            &self.source_server_version,
        ] {
            require_nonempty(value)?;
        }
        if !self.source_read_only_server_enforced {
            return Err(PostgresSourceProfileError::ExternalRescanNotReadOnly);
        }
        if self.verified_at_unix_seconds == 0 {
            return Err(PostgresSourceProfileError::InvalidTimestamp);
        }
        let mut operation_ids = BTreeSet::new();
        for table in &self.tables {
            require_nonempty(&table.operation_id)?;
            for digest in [
                &table.manifest_hash,
                &table.fresh_source_hash,
                &table.target_hash,
            ] {
                require_sha256(digest)?;
            }
            if table.fresh_source_hash != table.target_hash {
                return Err(PostgresSourceProfileError::ExternalRescanDrift);
            }
            if !operation_ids.insert(table.operation_id.as_str()) {
                return Err(PostgresSourceProfileError::DuplicateRescanTable);
            }
        }
        Ok(())
    }
}

fn validate_sequence_manifest(
    observations: &[PostgresSequence],
) -> Result<(), PostgresSourceProfileError> {
    for observation in observations {
        validate_sequence(observation)
            .map_err(|_| PostgresSourceProfileError::InvalidSequenceContract)?;
        require_nonempty(&observation.catalog_object_id)?;
        if observation.cache_size != 1 {
            return Err(PostgresSourceProfileError::CachedSequenceUnsupported);
        }
    }
    if observations.windows(2).any(|pair| {
        (
            &pair[0].namespace,
            &pair[0].name,
            &pair[0].catalog_object_id,
        ) >= (
            &pair[1].namespace,
            &pair[1].name,
            &pair[1].catalog_object_id,
        )
    }) {
        return Err(PostgresSourceProfileError::InvalidSequenceManifest);
    }
    Ok(())
}

fn canonical_hash(value: &impl Serialize) -> Result<String, PostgresSourceProfileError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn require_sha256(value: &str) -> Result<(), PostgresSourceProfileError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(PostgresSourceProfileError::InvalidDigest)
    }
}

fn require_nonempty(value: &str) -> Result<(), PostgresSourceProfileError> {
    if value.trim().is_empty() {
        Err(PostgresSourceProfileError::EmptyField)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum PostgresSourceProfileError {
    #[error("unsupported PostgreSQL source-profile schema version")]
    UnsupportedVersion,
    #[error("source-profile evidence does not match the selected profile")]
    ProfileEvidenceMismatch,
    #[error("source profile is incompatible with the reviewed consistency mode")]
    ProfileConsistencyMismatch,
    #[error("external-quiesce plans must state that the tool does not enforce the source freeze")]
    ExternalFreezeMisrepresented,
    #[error("source-profile evidence contains an empty required field")]
    EmptyField,
    #[error("source-profile evidence contains an invalid SHA-256 digest")]
    InvalidDigest,
    #[error("source-profile evidence contains an invalid timestamp")]
    InvalidTimestamp,
    #[error("source-profile probe result set is incomplete")]
    IncompleteProbeSet,
    #[error("source-profile probe requirement is duplicated")]
    DuplicateProbeRequirement,
    #[error("source-profile probe object manifest is not sorted and unique")]
    InvalidObjectManifest,
    #[error("one or more required source-profile probes are not proven")]
    ProbeNotProven,
    #[error("external quiesce attestation is withdrawn")]
    AttestationWithdrawn,
    #[error("external quiesce attestation is not active at the observed time")]
    AttestationExpired,
    #[error("external quiesce attestation renewal is not a continuous exact chain")]
    InvalidAttestationChain,
    #[error("sequence equality cannot prove sequences with CACHE greater than one")]
    CachedSequenceUnsupported,
    #[error("sequence equality evidence contains an invalid sequence contract")]
    InvalidSequenceContract,
    #[error("sequence observation manifest is not sorted and unique")]
    InvalidSequenceManifest,
    #[error("sequence state changed between initial and final observation")]
    SequenceStateDrift,
    #[error("external-quiesce verified re-scan contains a duplicate table operation")]
    DuplicateRescanTable,
    #[error("external-quiesce verified re-scan differs from the copied target")]
    ExternalRescanDrift,
    #[error("external-quiesce verified re-scan lacks server-enforced read-only evidence")]
    ExternalRescanNotReadOnly,
    #[error("cannot serialize PostgreSQL source-profile evidence")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::model::Identifier;

    fn probe_artifact() -> PostgresSourceProbeArtifact {
        PostgresSourceProbeArtifact {
            schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
            profile: PostgresSourceProfileKind::ManagedAdministrator,
            source_endpoint_identity: "postgres://source/app?user=admin".into(),
            source_catalog_fingerprint: "a".repeat(64),
            administrator_role: "migration_admin".into(),
            probed_at_unix_seconds: 1_000,
            results: PostgresSourceProbeRequirement::ALL
                .into_iter()
                .map(|requirement| PostgresSourceProbeResult {
                    requirement,
                    status: PostgresSourceProbeStatus::Proven,
                    object_ids: Vec::new(),
                    detail: "exact probe passed".into(),
                })
                .collect(),
        }
    }

    fn sequence(id: &str, last_value: i64) -> PostgresSequence {
        PostgresSequence {
            catalog_object_id: id.into(),
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new(format!("sequence_{id}")).unwrap(),
            persistence: "p".into(),
            data_type: "bigint".into(),
            start_value: 1,
            increment: 1,
            minimum_value: 1,
            maximum_value: i64::MAX,
            cache_size: 1,
            cycle: false,
            last_value,
            is_called: true,
            ownership: None,
        }
    }

    #[test]
    fn probe_artifact_requires_every_exact_requirement() {
        let artifact = probe_artifact();
        artifact.require_all_proven().unwrap();
        artifact.canonical_hash().unwrap();

        let mut incomplete = artifact.clone();
        incomplete.results.pop();
        assert!(matches!(
            incomplete.validate(),
            Err(PostgresSourceProfileError::IncompleteProbeSet)
        ));

        let mut reordered = artifact.clone();
        reordered.results.swap(0, 1);
        assert!(matches!(
            reordered.validate(),
            Err(PostgresSourceProfileError::IncompleteProbeSet)
        ));

        let mut unavailable = artifact;
        unavailable.results[0].status = PostgresSourceProbeStatus::Unavailable {
            reason: "provider denies CREATE EVENT TRIGGER".into(),
        };
        unavailable.validate().unwrap();
        assert!(matches!(
            unavailable.require_all_proven(),
            Err(PostgresSourceProfileError::ProbeNotProven)
        ));
    }

    #[test]
    fn external_attestation_must_be_active_and_current() {
        let mut attestation = PostgresExternalQuiesceAttestation {
            schema_version: POSTGRES_EXTERNAL_QUIESCE_ATTESTATION_SCHEMA_VERSION,
            source_endpoint_identity: "postgres://source/app?user=reader".into(),
            source_catalog_fingerprint: "a".repeat(64),
            attestation_reference: "CHANGE-1234".into(),
            previous_attestation_digest: None,
            issued_at_unix_seconds: 1_000,
            expires_at_unix_seconds: 2_000,
            status: PostgresExternalQuiesceStatus::Active,
        };
        attestation.require_active_at(1_500).unwrap();
        assert!(matches!(
            attestation.require_active_at(2_001),
            Err(PostgresSourceProfileError::AttestationExpired)
        ));
        attestation.status = PostgresExternalQuiesceStatus::Withdrawn;
        assert!(matches!(
            attestation.require_active_at(1_500),
            Err(PostgresSourceProfileError::AttestationWithdrawn)
        ));
    }

    #[test]
    fn external_attestation_renewal_requires_an_overlapping_exact_chain() {
        let initial = PostgresExternalQuiesceAttestation {
            schema_version: POSTGRES_EXTERNAL_QUIESCE_ATTESTATION_SCHEMA_VERSION,
            source_endpoint_identity: "postgres://source/app?user=reader".into(),
            source_catalog_fingerprint: "a".repeat(64),
            attestation_reference: "CHANGE-1234".into(),
            previous_attestation_digest: None,
            issued_at_unix_seconds: 1_000,
            expires_at_unix_seconds: 2_000,
            status: PostgresExternalQuiesceStatus::Active,
        };
        initial.validate_initial().unwrap();
        let mut renewal = PostgresExternalQuiesceAttestation {
            schema_version: POSTGRES_EXTERNAL_QUIESCE_ATTESTATION_SCHEMA_VERSION,
            source_endpoint_identity: initial.source_endpoint_identity.clone(),
            source_catalog_fingerprint: initial.source_catalog_fingerprint.clone(),
            attestation_reference: initial.attestation_reference.clone(),
            previous_attestation_digest: Some(initial.canonical_hash().unwrap()),
            issued_at_unix_seconds: 1_900,
            expires_at_unix_seconds: 3_000,
            status: PostgresExternalQuiesceStatus::Active,
        };
        renewal.validate_renewal_from(&initial).unwrap();
        renewal.issued_at_unix_seconds = 2_001;
        assert!(matches!(
            renewal.validate_renewal_from(&initial),
            Err(PostgresSourceProfileError::InvalidAttestationChain)
        ));
    }

    #[test]
    fn external_attestation_rejects_a_pre_current_field_set() {
        let stale = r#"{"schema_version":1,"source_endpoint_identity":"postgres://source/app?user=reader","source_catalog_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","attestation_reference":"CHANGE-1234","issued_at_unix_seconds":1000,"expires_at_unix_seconds":2000,"status":"active"}"#;
        assert!(serde_json::from_str::<PostgresExternalQuiesceAttestation>(stale).is_err());
    }

    #[test]
    fn external_profile_cannot_claim_tool_enforced_freeze() {
        let profile = PostgresSourceProfileContract::AttestedExternalQuiesce {
            verified_rescan: true,
            freeze_enforced_by_tool: true,
        };
        assert!(matches!(
            profile.validate_for_plan(
                "postgres://source/app",
                &"a".repeat(64),
                "consistent-snapshot"
            ),
            Err(PostgresSourceProfileError::ExternalFreezeMisrepresented)
        ));
    }

    #[test]
    fn sequence_equality_rejects_cache_and_state_drift() {
        let initial = vec![sequence("relation:1", 10)];
        let evidence = PostgresSequenceEqualityEvidence {
            schema_version: POSTGRES_SOURCE_PROFILE_SCHEMA_VERSION,
            source_catalog_fingerprint: "a".repeat(64),
            initial: initial.clone(),
            final_observation: initial.clone(),
            verified_at_unix_seconds: 2_000,
        };
        evidence.validate().unwrap();

        let mut first_by_name = sequence("relation:9", 10);
        first_by_name.name = Identifier::new("a_sequence").unwrap();
        let mut second_by_name = sequence("relation:1", 10);
        second_by_name.name = Identifier::new("b_sequence").unwrap();
        PostgresSequenceEqualityEvidence::new(
            "a".repeat(64),
            vec![first_by_name.clone(), second_by_name.clone()],
            vec![first_by_name, second_by_name],
            2_000,
        )
        .unwrap();

        let mut drifted = evidence.clone();
        drifted.final_observation[0].last_value = 11;
        assert!(matches!(
            drifted.validate(),
            Err(PostgresSourceProfileError::SequenceStateDrift)
        ));

        let mut cached = evidence;
        cached.initial[0].cache_size = 2;
        cached.final_observation[0].cache_size = 2;
        assert!(matches!(
            cached.validate(),
            Err(PostgresSourceProfileError::CachedSequenceUnsupported)
        ));
    }
}
