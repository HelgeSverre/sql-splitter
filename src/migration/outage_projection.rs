use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const OUTAGE_PROJECTION_SCHEMA_VERSION: u16 = 1;
pub const THROUGHPUT_PROFILE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteBasis {
    /// Checked sum of `pg_total_relation_size(oid)` for each copied physical
    /// PostgreSQL relation with `relkind = 'r'`, exactly once. For a partitioned
    /// table, the copied partition leaves replace its `relkind = 'p'` root.
    /// Assessment and execute preflight must use the same physical relation set.
    PostgresTotalRelationBytesV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThroughputProfile {
    pub schema_version: u16,
    pub measurement_reference: String,
    pub environment_reference: String,
    pub postgres_major_version: u16,
    pub measured_at_unix_seconds: u64,
    pub valid_for_seconds: u64,
    pub copy_bytes_per_second: u64,
    pub verification_bytes_per_second: u64,
}

impl ThroughputProfile {
    pub fn validate_at(
        &self,
        postgres_major_version: u16,
        observed_at_unix_seconds: u64,
    ) -> Result<(), OutageProjectionError> {
        if self.schema_version != THROUGHPUT_PROFILE_SCHEMA_VERSION {
            return Err(OutageProjectionError::UnsupportedProfileVersion);
        }
        if self.measurement_reference.trim().is_empty()
            || self.environment_reference.trim().is_empty()
            || self.postgres_major_version == 0
            || self.valid_for_seconds == 0
            || self.copy_bytes_per_second == 0
            || self.verification_bytes_per_second == 0
        {
            return Err(OutageProjectionError::IncompleteProfile);
        }
        if self.postgres_major_version != postgres_major_version {
            return Err(OutageProjectionError::WrongPostgresMajor);
        }
        let expires_at = self
            .measured_at_unix_seconds
            .checked_add(self.valid_for_seconds)
            .ok_or(OutageProjectionError::InvalidValidityInterval)?;
        if self.measured_at_unix_seconds > observed_at_unix_seconds {
            return Err(OutageProjectionError::FutureMeasurement);
        }
        if observed_at_unix_seconds > expires_at {
            return Err(OutageProjectionError::StaleProfile);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewedOutagePolicy {
    pub schema_version: u16,
    pub assessment_digest: String,
    pub source_catalog_fingerprint: String,
    pub byte_basis: ByteBasis,
    pub throughput_profile: ThroughputProfile,
    pub reviewed_at_unix_seconds: u64,
    pub reviewed_assessed_bytes: u64,
    pub reviewed_projected_seconds: u64,
    pub maximum_approved_seconds: u64,
}

impl ReviewedOutagePolicy {
    pub fn validate(&self) -> Result<(), OutageProjectionError> {
        if self.schema_version != OUTAGE_PROJECTION_SCHEMA_VERSION {
            return Err(OutageProjectionError::UnsupportedPolicyVersion);
        }
        validate_sha256(&self.assessment_digest)?;
        validate_sha256(&self.source_catalog_fingerprint)?;
        if self.maximum_approved_seconds == 0 {
            return Err(OutageProjectionError::InvalidMaximum);
        }
        self.throughput_profile.validate_at(
            self.throughput_profile.postgres_major_version,
            self.reviewed_at_unix_seconds,
        )?;
        let expected = projected_seconds(self.reviewed_assessed_bytes, &self.throughput_profile)?;
        if expected != self.reviewed_projected_seconds {
            return Err(OutageProjectionError::ProjectionMismatch);
        }
        if expected > self.maximum_approved_seconds {
            return Err(OutageProjectionError::MaximumExceeded);
        }
        Ok(())
    }

    pub fn canonical_hash(&self) -> Result<String, OutageProjectionError> {
        self.validate()?;
        canonical_hash(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedOutageProjection {
    pub schema_version: u16,
    pub policy_hash: String,
    pub source_catalog_fingerprint: String,
    pub source_server_version_num: u32,
    pub byte_basis: ByteBasis,
    pub refreshed_at_unix_seconds: u64,
    pub refreshed_assessed_bytes: u64,
    pub projected_seconds: u64,
    pub throughput_profile: ThroughputProfile,
}

impl AcceptedOutageProjection {
    pub fn validate_against(
        &self,
        policy: &ReviewedOutagePolicy,
    ) -> Result<(), OutageProjectionError> {
        policy.validate()?;
        if self.schema_version != OUTAGE_PROJECTION_SCHEMA_VERSION {
            return Err(OutageProjectionError::UnsupportedAcceptedVersion);
        }
        if self.policy_hash != policy.canonical_hash()?
            || self.source_catalog_fingerprint != policy.source_catalog_fingerprint
            || self.byte_basis != policy.byte_basis
            || self.throughput_profile != policy.throughput_profile
        {
            return Err(OutageProjectionError::PolicyBindingMismatch);
        }
        let postgres_major_version = u16::try_from(self.source_server_version_num / 10_000)
            .map_err(|_| OutageProjectionError::WrongPostgresMajor)?;
        self.throughput_profile
            .validate_at(postgres_major_version, self.refreshed_at_unix_seconds)?;
        let expected = projected_seconds(self.refreshed_assessed_bytes, &self.throughput_profile)?;
        if expected != self.projected_seconds {
            return Err(OutageProjectionError::ProjectionMismatch);
        }
        if expected > policy.maximum_approved_seconds {
            return Err(OutageProjectionError::MaximumExceeded);
        }
        Ok(())
    }

    pub fn canonical_hash(
        &self,
        policy: &ReviewedOutagePolicy,
    ) -> Result<String, OutageProjectionError> {
        self.validate_against(policy)?;
        canonical_hash(self)
    }
}

pub fn projected_seconds(
    assessed_bytes: u64,
    profile: &ThroughputProfile,
) -> Result<u64, OutageProjectionError> {
    if profile.copy_bytes_per_second == 0 || profile.verification_bytes_per_second == 0 {
        return Err(OutageProjectionError::IncompleteProfile);
    }
    let copy = ceiling_division(assessed_bytes, profile.copy_bytes_per_second)?;
    let verification = ceiling_division(assessed_bytes, profile.verification_bytes_per_second)?;
    copy.checked_add(verification)
        .ok_or(OutageProjectionError::ArithmeticOverflow)
}

fn ceiling_division(dividend: u64, divisor: u64) -> Result<u64, OutageProjectionError> {
    let quotient = dividend / divisor;
    quotient
        .checked_add(u64::from(!dividend.is_multiple_of(divisor)))
        .ok_or(OutageProjectionError::ArithmeticOverflow)
}

fn canonical_hash(value: &impl Serialize) -> Result<String, OutageProjectionError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn validate_sha256(value: &str) -> Result<(), OutageProjectionError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(OutageProjectionError::InvalidDigest)
    }
}

#[derive(Debug, Error)]
pub enum OutageProjectionError {
    #[error("unsupported throughput profile schema version")]
    UnsupportedProfileVersion,
    #[error("unsupported reviewed outage policy schema version")]
    UnsupportedPolicyVersion,
    #[error("unsupported accepted outage projection schema version")]
    UnsupportedAcceptedVersion,
    #[error("throughput profile is incomplete")]
    IncompleteProfile,
    #[error("throughput profile PostgreSQL major version does not match")]
    WrongPostgresMajor,
    #[error("throughput profile validity interval is invalid")]
    InvalidValidityInterval,
    #[error("throughput profile measurement is in the future")]
    FutureMeasurement,
    #[error("throughput profile is stale")]
    StaleProfile,
    #[error("outage policy contains an invalid SHA-256 digest")]
    InvalidDigest,
    #[error("maximum approved outage must be positive")]
    InvalidMaximum,
    #[error("outage projection arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("recorded outage projection does not match its inputs")]
    ProjectionMismatch,
    #[error("projected outage exceeds the reviewed maximum")]
    MaximumExceeded,
    #[error("accepted outage projection differs from the reviewed policy")]
    PolicyBindingMismatch,
    #[error("cannot serialize canonical outage projection")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> ThroughputProfile {
        ThroughputProfile {
            schema_version: THROUGHPUT_PROFILE_SCHEMA_VERSION,
            measurement_reference: "run-1".into(),
            environment_reference: "pg17-local-ssd".into(),
            postgres_major_version: 17,
            measured_at_unix_seconds: 1_000,
            valid_for_seconds: 1_000,
            copy_bytes_per_second: 200,
            verification_bytes_per_second: 500,
        }
    }

    fn policy() -> ReviewedOutagePolicy {
        ReviewedOutagePolicy {
            schema_version: OUTAGE_PROJECTION_SCHEMA_VERSION,
            assessment_digest: "a".repeat(64),
            source_catalog_fingerprint: "b".repeat(64),
            byte_basis: ByteBasis::PostgresTotalRelationBytesV1,
            throughput_profile: profile(),
            reviewed_at_unix_seconds: 1_100,
            reviewed_assessed_bytes: 1_001,
            reviewed_projected_seconds: 9,
            maximum_approved_seconds: 10,
        }
    }

    #[test]
    fn reviewed_and_accepted_evidence_are_tamper_evident() {
        let policy = policy();
        policy.validate().unwrap();
        let mut accepted = AcceptedOutageProjection {
            schema_version: OUTAGE_PROJECTION_SCHEMA_VERSION,
            policy_hash: policy.canonical_hash().unwrap(),
            source_catalog_fingerprint: policy.source_catalog_fingerprint.clone(),
            source_server_version_num: 170_000,
            byte_basis: policy.byte_basis,
            refreshed_at_unix_seconds: 1_200,
            refreshed_assessed_bytes: 1_001,
            projected_seconds: 9,
            throughput_profile: policy.throughput_profile.clone(),
        };
        accepted.validate_against(&policy).unwrap();
        accepted.projected_seconds += 1;
        assert!(matches!(
            accepted.validate_against(&policy),
            Err(OutageProjectionError::ProjectionMismatch)
        ));
    }

    #[test]
    fn stale_wrong_major_and_budget_overrun_fail_closed() {
        let mut stale = profile();
        stale.valid_for_seconds = 10;
        assert!(matches!(
            stale.validate_at(17, 1_100),
            Err(OutageProjectionError::StaleProfile)
        ));
        assert!(matches!(
            profile().validate_at(16, 1_100),
            Err(OutageProjectionError::WrongPostgresMajor)
        ));
        let mut over = policy();
        over.maximum_approved_seconds = 8;
        assert!(matches!(
            over.validate(),
            Err(OutageProjectionError::MaximumExceeded)
        ));
    }
}
