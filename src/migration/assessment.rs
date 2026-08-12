use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::artifact::{read_json, ArtifactError};
use super::model::{CatalogObjectKind, QualifiedTable};
use super::outage_projection::projected_seconds;
pub use super::outage_projection::{ThroughputProfile, THROUGHPUT_PROFILE_SCHEMA_VERSION};
use super::plan::{
    AssessmentStatus, OperationKind, PlanError, PlanPurpose, ReviewedPlan, UnsupportedObject,
    UnsupportedObjectCode, UnsupportedObjectReport,
};

pub const ASSESSMENT_SCHEMA_VERSION: u16 = 3;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    Proven,
    NotAssessed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAssessmentEvidence {
    pub server_version: String,
    pub server_version_num: u32,
    pub tls_binding: String,
    pub transaction_read_only: bool,
    pub direct_write_privileges_absent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeEstimate {
    pub table: QualifiedTable,
    pub estimated_rows: i64,
    pub relation_bytes: u64,
    pub total_relation_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRequirement {
    pub name: String,
    pub status: EvidenceStatus,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectedWindow {
    NotAssessed {
        reason: String,
    },
    Estimated {
        seconds: u64,
        assessed_bytes: u64,
        included_relations: Vec<QualifiedTable>,
        assessed_at_unix_seconds: u64,
        throughput_profile: ThroughputProfile,
    },
}

/// Projects the outage for the supplied copied physical PostgreSQL relations.
///
/// The caller must supply each copied `relkind = 'r'` relation exactly once.
/// Partition leaves replace the corresponding `relkind = 'p'` root.
pub fn project_outage_window(
    scope_estimates: &[ScopeEstimate],
    profile: Option<&ThroughputProfile>,
    postgres_major_version: u16,
    assessed_at_unix_seconds: u64,
) -> ProjectedWindow {
    let Some(profile) = profile else {
        return not_assessed("no throughput profile was supplied");
    };
    if let Err(error) = profile.validate_at(postgres_major_version, assessed_at_unix_seconds) {
        return not_assessed(&error.to_string());
    }
    let mut included_relations = scope_estimates
        .iter()
        .map(|estimate| estimate.table.clone())
        .collect::<Vec<_>>();
    included_relations.sort();
    if included_relations.windows(2).any(|pair| pair[0] == pair[1]) {
        return not_assessed("projection relation set contains duplicate identities");
    }
    let Some(assessed_bytes) = scope_estimates.iter().try_fold(0_u64, |total, estimate| {
        total.checked_add(estimate.total_relation_bytes)
    }) else {
        return not_assessed("assessed relation byte total overflowed");
    };
    let Ok(seconds) = projected_seconds(assessed_bytes, profile) else {
        return not_assessed("projected window overflowed");
    };
    ProjectedWindow::Estimated {
        seconds,
        assessed_bytes,
        included_relations,
        assessed_at_unix_seconds,
        throughput_profile: profile.clone(),
    }
}

pub fn read_throughput_profile(
    path: impl AsRef<std::path::Path>,
) -> Result<ThroughputProfile, AssessmentError> {
    Ok(read_json(path)?)
}

fn not_assessed(reason: &str) -> ProjectedWindow {
    ProjectedWindow::NotAssessed {
        reason: reason.to_owned(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssessmentArtifact {
    pub schema_version: u16,
    pub reviewed_plan: ReviewedPlan,
    pub source_evidence: SourceAssessmentEvidence,
    pub execution_requirements: Vec<ExecutionRequirement>,
    pub scope_estimates: Vec<ScopeEstimate>,
    pub projected_window: ProjectedWindow,
}

impl AssessmentArtifact {
    pub fn validate(&self) -> Result<(), AssessmentError> {
        if self.schema_version != ASSESSMENT_SCHEMA_VERSION {
            return Err(AssessmentError::UnsupportedVersion {
                found: self.schema_version,
            });
        }
        self.reviewed_plan.validate()?;
        let plan = &self.reviewed_plan.plan;
        if plan.purpose != PlanPurpose::Assessment
            || plan.target_endpoint_identity != AssessmentStatus::NotAssessed
            || plan.target_catalog_fingerprint != AssessmentStatus::NotAssessed
            || plan.target_catalog != AssessmentStatus::NotAssessed
            || plan.target_tls_binding != AssessmentStatus::NotAssessed
            || plan.consistency_mode != "not_assessed"
        {
            return Err(AssessmentError::InvalidAssessmentPurpose);
        }
        if !self.source_evidence.transaction_read_only {
            return Err(AssessmentError::SourceTransactionNotReadOnly);
        }
        if !self.source_evidence.direct_write_privileges_absent {
            return Err(AssessmentError::SourceRoleHasDirectWritePrivilege);
        }
        if self.source_evidence.server_version.trim().is_empty()
            || self.source_evidence.server_version_num == 0
            || self.source_evidence.tls_binding.trim().is_empty()
            || self.source_evidence.tls_binding != plan.source_tls_binding
            || plan
                .source_catalog
                .as_ref()
                .is_none_or(|catalog| catalog.server_version != self.source_evidence.server_version)
        {
            return Err(AssessmentError::MissingSourceEvidence);
        }
        if plan.capabilities.get("acl.report_only").map(String::as_str) != Some("approval_required")
        {
            return Err(AssessmentError::MissingAclReviewCapability);
        }
        if self
            .scope_estimates
            .iter()
            .any(|estimate| estimate.estimated_rows < 0)
        {
            return Err(AssessmentError::InvalidScopeEstimate);
        }
        let estimated_tables = self
            .scope_estimates
            .iter()
            .map(|estimate| &estimate.table)
            .collect::<BTreeSet<_>>();
        if estimated_tables.len() != self.scope_estimates.len() {
            return Err(AssessmentError::InvalidScopeEstimate);
        }
        let catalog_tables = plan
            .source_catalog
            .as_ref()
            .into_iter()
            .flat_map(|catalog| &catalog.namespaces)
            .flat_map(|namespace| {
                namespace
                    .objects
                    .iter()
                    .filter(|object| {
                        matches!(
                            object.kind,
                            CatalogObjectKind::Table | CatalogObjectKind::Partition
                        )
                    })
                    .map(|object| QualifiedTable {
                        namespace: namespace.name.clone(),
                        name: object.name.clone(),
                    })
            })
            .collect::<BTreeSet<_>>();
        if estimated_tables != catalog_tables.iter().collect::<BTreeSet<_>>() {
            return Err(AssessmentError::InvalidScopeEstimate);
        }
        if let ProjectedWindow::Estimated {
            assessed_at_unix_seconds,
            throughput_profile,
            included_relations,
            ..
        } = &self.projected_window
        {
            if included_relations.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(AssessmentError::InvalidProjectedWindow);
            }
            let estimates_by_table = self
                .scope_estimates
                .iter()
                .map(|estimate| (&estimate.table, estimate))
                .collect::<BTreeMap<_, _>>();
            let included_estimates = included_relations
                .iter()
                .map(|table| {
                    estimates_by_table
                        .get(table)
                        .copied()
                        .ok_or(AssessmentError::InvalidProjectedWindow)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let postgres_major_version =
                u16::try_from(self.source_evidence.server_version_num / 10_000)
                    .map_err(|_| AssessmentError::InvalidProjectedWindow)?;
            let recomputed = project_outage_window(
                &included_estimates.into_iter().cloned().collect::<Vec<_>>(),
                Some(throughput_profile),
                postgres_major_version,
                *assessed_at_unix_seconds,
            );
            if recomputed != self.projected_window {
                return Err(AssessmentError::InvalidProjectedWindow);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AssessmentError {
    #[error("unsupported assessment schema version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("assessment plan is invalid")]
    Plan(#[from] PlanError),
    #[error("assessment artifact does not contain a source-only assessment plan")]
    InvalidAssessmentPurpose,
    #[error("assessment source transaction is not read-only")]
    SourceTransactionNotReadOnly,
    #[error("assessment source role has a direct database-object write privilege")]
    SourceRoleHasDirectWritePrivilege,
    #[error("assessment source evidence is incomplete")]
    MissingSourceEvidence,
    #[error("assessment does not carry the mandatory acl.report_only review capability")]
    MissingAclReviewCapability,
    #[error("assessment contains an invalid scope estimate")]
    InvalidScopeEstimate,
    #[error("assessment contains an invalid projected outage window")]
    InvalidProjectedWindow,
    #[error("assessment source catalog is missing")]
    MissingSourceCatalog,
    #[error("throughput profile artifact is invalid")]
    ThroughputProfileArtifact(#[from] ArtifactError),
    #[error("assessment catalog contains an unclassified vendor object kind {kind}")]
    UnclassifiedVendorObject { kind: String },
    #[error("assessment report formatting failed")]
    Format(#[from] std::fmt::Error),
}

/// Render a deterministic Markdown report. Volatile estimates are isolated
/// between explicit markers and are excluded from stable-body comparisons.
pub fn render_markdown(assessment: &AssessmentArtifact) -> Result<String, AssessmentError> {
    use std::fmt::Write as _;

    assessment.validate()?;
    let plan = &assessment.reviewed_plan.plan;
    let catalog = plan
        .source_catalog
        .as_ref()
        .ok_or(AssessmentError::MissingSourceCatalog)?;
    let inventory = classify_catalog_inventory(catalog, &plan.unsupported_objects)?;

    let mut output = String::new();
    writeln!(output, "# Enterprise migration assessment")?;
    writeln!(output)?;
    writeln!(output, "EXPERIMENTAL SPIKE — NOT FOR PRODUCTION")?;
    writeln!(output)?;
    writeln!(output, "## Source-freeze boundary")?;
    writeln!(output)?;
    writeln!(
        output,
        "This source-only assessment does not select or enforce a source freeze. If a later execution plan selects `attested_external_quiesce`, sql-splitter records operator evidence but does **not** enforce the external source freeze."
    )?;
    writeln!(output)?;
    writeln!(output, "## Identity and evidence")?;
    writeln!(output)?;
    writeln!(
        output,
        "- Source endpoint: `{}`",
        markdown_code(&plan.source_endpoint_identity)
    )?;
    writeln!(
        output,
        "- PostgreSQL version: `{}`",
        markdown_code(&assessment.source_evidence.server_version)
    )?;
    writeln!(
        output,
        "- TLS binding: `{}`",
        markdown_code(&assessment.source_evidence.tls_binding)
    )?;
    writeln!(output, "- Source transaction: `repeatable_read_read_only`")?;
    writeln!(
        output,
        "- Direct database-object write privileges: `absent`"
    )?;
    writeln!(
        output,
        "- Executable routine side effects: `not proven`; safety requires the audited assessment statement set plus read-only transaction restrictions"
    )?;
    writeln!(
        output,
        "- Source catalog fingerprint: `{}`",
        plan.source_catalog_fingerprint
    )?;
    writeln!(output, "- Target endpoint: **not assessed**")?;
    writeln!(
        output,
        "- Plan hash: `{}`",
        assessment.reviewed_plan.plan_hash
    )?;

    writeln!(output)?;
    writeln!(output, "## Supported inventory")?;
    writeln!(output)?;
    writeln!(output, "| Object class | Count |")?;
    writeln!(output, "| --- | ---: |")?;
    for (kind, count) in &inventory.supported {
        writeln!(output, "| {} | {count} |", markdown_cell(kind))?;
    }

    writeln!(output)?;
    writeln!(output, "## Unsupported catalog inventory")?;
    writeln!(output)?;
    writeln!(output, "| Blocking | Object class | Count |")?;
    writeln!(output, "| --- | --- | ---: |")?;
    for (kind, count) in &inventory.blocking {
        writeln!(output, "| yes | {} | {count} |", markdown_cell(kind))?;
    }
    for (kind, count) in &inventory.review_required {
        writeln!(output, "| no | {} | {count} |", markdown_cell(kind))?;
    }

    let mut planned_operations = BTreeMap::<String, usize>::new();
    for operation in &plan.operations {
        *planned_operations
            .entry(operation_kind_name(&operation.kind))
            .or_default() += 1;
    }
    writeln!(output)?;
    writeln!(output, "## Planned migration operations")?;
    writeln!(output)?;
    writeln!(
        output,
        "These operations describe the supported source projection. Blocking findings below still prevent execution."
    )?;
    writeln!(output)?;
    writeln!(output, "| Operation class | Count |")?;
    writeln!(output, "| --- | ---: |")?;
    for (kind, count) in planned_operations {
        writeln!(output, "| {} | {count} |", markdown_cell(&kind))?;
    }

    writeln!(output)?;
    writeln!(output, "## Blocking findings")?;
    writeln!(output)?;
    writeln!(output, "| Code | Class | Object | Reason |")?;
    writeln!(output, "| --- | --- | --- | --- |")?;
    let mut unsupported = plan.unsupported_objects.objects.iter().collect::<Vec<_>>();
    unsupported.sort_by(|left, right| unsupported_key(left).cmp(&unsupported_key(right)));
    for finding in unsupported
        .iter()
        .copied()
        .filter(|finding| finding.required_semantics)
    {
        writeln!(
            output,
            "| `{}` | {} | {} | {} |",
            finding.code.as_str(),
            markdown_cell(&finding.object_kind),
            markdown_cell(&finding.object_id),
            markdown_cell(&finding.reason)
        )?;
    }

    writeln!(output)?;
    writeln!(output, "## Review-required findings")?;
    writeln!(output)?;
    writeln!(output, "| Code | Class | Object | Reason |")?;
    writeln!(output, "| --- | --- | --- | --- |")?;
    for finding in unsupported
        .iter()
        .copied()
        .filter(|finding| !finding.required_semantics)
    {
        writeln!(
            output,
            "| `{}` | {} | {} | {} |",
            finding.code.as_str(),
            markdown_cell(&finding.object_kind),
            markdown_cell(&finding.object_id),
            markdown_cell(&finding.reason)
        )?;
    }

    writeln!(output)?;
    writeln!(output, "## Reviewed capabilities")?;
    writeln!(output)?;
    for (name, value) in &plan.capabilities {
        writeln!(
            output,
            "- `{}`: `{}`",
            markdown_code(name),
            markdown_code(value)
        )?;
    }

    writeln!(output)?;
    writeln!(output, "## Execution requirements")?;
    writeln!(output)?;
    for requirement in &assessment.execution_requirements {
        let status = match &requirement.status {
            EvidenceStatus::Proven => "proven".to_owned(),
            EvidenceStatus::NotAssessed { reason } => {
                format!("not assessed: {}", markdown_cell(reason))
            }
        };
        writeln!(
            output,
            "- `{}` — {} — {}",
            markdown_code(&requirement.name),
            status,
            markdown_cell(&requirement.detail)
        )?;
    }
    writeln!(
        output,
        "- Target compatibility and emptiness — **not assessed**"
    )?;

    writeln!(output)?;
    writeln!(output, "<!-- sql-splitter:volatile-begin -->")?;
    writeln!(output, "## Volatile estimates")?;
    writeln!(output)?;
    writeln!(
        output,
        "| Table | Estimated rows | Relation bytes | Total bytes |"
    )?;
    writeln!(output, "| --- | ---: | ---: | ---: |")?;
    let mut estimates = assessment.scope_estimates.iter().collect::<Vec<_>>();
    estimates.sort_by(|left, right| left.table.cmp(&right.table));
    for estimate in estimates {
        writeln!(
            output,
            "| {}.{} | {} | {} | {} |",
            markdown_cell(estimate.table.namespace.as_str()),
            markdown_cell(estimate.table.name.as_str()),
            estimate.estimated_rows,
            estimate.relation_bytes,
            estimate.total_relation_bytes
        )?;
    }
    match &assessment.projected_window {
        ProjectedWindow::NotAssessed { reason } => writeln!(
            output,
            "\nProjected migration window: **not assessed** ({})",
            markdown_cell(reason)
        )?,
        ProjectedWindow::Estimated {
            seconds,
            assessed_bytes,
            throughput_profile,
            ..
        } => writeln!(
            output,
            "\nProjected copy-plus-verification window: approximately {seconds} seconds for {assessed_bytes} assessed relation bytes using `{}` measured in environment `{}`; actual throughput can differ",
            markdown_code(&throughput_profile.measurement_reference),
            markdown_code(&throughput_profile.environment_reference)
        )?,
    }
    writeln!(output, "<!-- sql-splitter:volatile-end -->")?;
    Ok(output)
}

pub fn stable_report_body(report: &str) -> String {
    const BEGIN: &str = "\n<!-- sql-splitter:volatile-begin -->\n";
    const END: &str = "\n<!-- sql-splitter:volatile-end -->\n";
    match (
        report.match_indices(BEGIN).collect::<Vec<_>>().as_slice(),
        report.match_indices(END).collect::<Vec<_>>().as_slice(),
    ) {
        ([(begin, _)], [(end, _)]) if begin <= end => {
            let after = *end + END.len();
            format!("{}{}", &report[..*begin], &report[after..])
        }
        _ => report.to_owned(),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ClassifiedCatalogInventory {
    supported: BTreeMap<String, usize>,
    blocking: BTreeMap<String, usize>,
    review_required: BTreeMap<String, usize>,
}

fn classify_catalog_inventory(
    catalog: &super::model::VendorCatalog,
    unsupported: &UnsupportedObjectReport,
) -> Result<ClassifiedCatalogInventory, AssessmentError> {
    let direct_findings = unsupported.objects.iter().fold(
        BTreeMap::<&str, (bool, bool)>::new(),
        |mut findings, finding| {
            let entry = findings
                .entry(finding.object_id.as_str())
                .or_insert((false, false));
            if finding.required_semantics {
                entry.0 = true;
            } else {
                entry.1 = true;
            }
            findings
        },
    );
    let mut classified = ClassifiedCatalogInventory::default();
    for namespace in &catalog.namespaces {
        let namespace_severity = finding_severity(
            &direct_findings,
            &namespace.id,
            "namespace:",
            "namespace-acl:",
        );
        increment_inventory(&mut classified, "namespace".into(), namespace_severity);
        for object in &namespace.objects {
            let kind = object_kind_name(&object.kind)?;
            let severity = match object.kind {
                CatalogObjectKind::Routine => {
                    finding_severity(&direct_findings, &object.id, "routine:", "routine-acl:")
                }
                _ => finding_severity(&direct_findings, &object.id, "relation:", "relation-acl:"),
            };
            increment_inventory(&mut classified, kind, severity);
        }
    }
    Ok(classified)
}

fn finding_severity(
    findings: &BTreeMap<&str, (bool, bool)>,
    object_id: &str,
    object_prefix: &str,
    finding_prefix: &str,
) -> (bool, bool) {
    let direct = findings.get(object_id).copied().unwrap_or_default();
    let related = object_id
        .strip_prefix(object_prefix)
        .map(|suffix| format!("{finding_prefix}{suffix}"))
        .and_then(|id| findings.get(id.as_str()).copied())
        .unwrap_or_default();
    (direct.0 || related.0, direct.1 || related.1)
}

fn increment_inventory(
    inventory: &mut ClassifiedCatalogInventory,
    kind: String,
    severity: (bool, bool),
) {
    let destination = match severity {
        (true, _) => &mut inventory.blocking,
        (false, true) => &mut inventory.review_required,
        _ => &mut inventory.supported,
    };
    *destination.entry(kind).or_default() += 1;
}

fn unsupported_key(finding: &UnsupportedObject) -> (UnsupportedObjectCode, &str, &str, &str, bool) {
    (
        finding.code,
        &finding.object_kind,
        &finding.object_id,
        &finding.reason,
        finding.required_semantics,
    )
}

fn object_kind_name(kind: &CatalogObjectKind) -> Result<String, AssessmentError> {
    let name = match kind {
        CatalogObjectKind::Table => "table",
        CatalogObjectKind::Column => "column",
        CatalogObjectKind::Sequence => "sequence",
        CatalogObjectKind::PrimaryKey => "primary_key",
        CatalogObjectKind::UniqueConstraint => "unique_constraint",
        CatalogObjectKind::CheckConstraint => "check_constraint",
        CatalogObjectKind::ForeignKey => "foreign_key",
        CatalogObjectKind::Index => "index",
        CatalogObjectKind::View => "view",
        CatalogObjectKind::Routine => "routine",
        CatalogObjectKind::Trigger => "trigger",
        CatalogObjectKind::Event => "event",
        CatalogObjectKind::Partition => "partition",
        CatalogObjectKind::Tablespace => "tablespace",
        CatalogObjectKind::Vendor(value)
            if value == "extension" || value.starts_with("postgres_type:") =>
        {
            return Ok(format!("vendor:{value}"));
        }
        CatalogObjectKind::Vendor(value) => {
            return Err(AssessmentError::UnclassifiedVendorObject {
                kind: value.clone(),
            });
        }
    };
    Ok(name.into())
}

fn operation_kind_name(kind: &OperationKind) -> String {
    match kind {
        OperationKind::CreateNamespace => "create_namespace".into(),
        OperationKind::CreateTable => "create_table".into(),
        OperationKind::CreateSequence => "create_sequence".into(),
        OperationKind::CopyTable => "copy_table".into(),
        OperationKind::CreateIndex => "create_index".into(),
        OperationKind::CheckForeignKey => "check_foreign_key".into(),
        OperationKind::AddForeignKey => "add_foreign_key".into(),
        OperationKind::CreateView => "create_view".into(),
        OperationKind::CreateRoutine => "create_routine".into(),
        OperationKind::CreateTrigger => "create_trigger".into(),
        OperationKind::VerifyTable => "verify_table".into(),
        OperationKind::VerifySchema => "verify_schema".into(),
        OperationKind::Vendor(value) => format!("vendor:{value}"),
    }
}

fn markdown_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace(['\r', '\n'], " ")
}

fn markdown_code(value: &str) -> String {
    markdown_cell(value).replace('`', "\\`")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    use crate::migration::canonical::CANONICAL_ENCODING_VERSION;
    use crate::migration::model::{CatalogNamespace, CatalogObject, Identifier, VendorCatalog};
    use crate::migration::plan::{
        AssessmentStatus, MigrationPlan, PlanPurpose, ReviewedPlan, UnsupportedObjectReport,
        PLAN_SCHEMA_VERSION, POSTGRESQL_ASSESSMENT_CONVERSION_POLICY,
    };

    #[test]
    fn stable_body_excludes_estimates() {
        let first = "stable\n<!-- sql-splitter:volatile-begin -->\none\n<!-- sql-splitter:volatile-end -->\nend";
        let second = "stable\n<!-- sql-splitter:volatile-begin -->\ntwo\n<!-- sql-splitter:volatile-end -->\nend";
        assert_eq!(stable_report_body(first), stable_report_body(second));
    }

    #[test]
    fn throughput_profile_projects_copy_plus_verification_with_ceiling_rounding() {
        let estimates = vec![scope_estimate(501), scope_estimate(500)];
        let profile = throughput_profile();
        assert_eq!(
            project_outage_window(&estimates, Some(&profile), 17, 1_100),
            ProjectedWindow::Estimated {
                seconds: 10,
                assessed_bytes: 1_201,
                included_relations: vec![scope_estimate(500).table, scope_estimate(501).table,],
                assessed_at_unix_seconds: 1_100,
                throughput_profile: profile,
            }
        );
    }

    #[test]
    fn throughput_profile_fails_safe_when_missing_stale_or_incompatible() {
        assert!(matches!(
            project_outage_window(&[], None, 17, 1_100),
            ProjectedWindow::NotAssessed { .. }
        ));

        let mut stale = throughput_profile();
        stale.valid_for_seconds = 50;
        assert_eq!(
            project_outage_window(&[], Some(&stale), 17, 1_100),
            not_assessed("throughput profile is stale")
        );

        let mut incompatible = throughput_profile();
        incompatible.postgres_major_version = 16;
        assert_eq!(
            project_outage_window(&[], Some(&incompatible), 17, 1_100),
            not_assessed("throughput profile PostgreSQL major version does not match")
        );

        let mut copy_only = throughput_profile();
        copy_only.verification_bytes_per_second = 0;
        assert_eq!(
            project_outage_window(&[], Some(&copy_only), 17, 1_100),
            not_assessed("throughput profile is incomplete")
        );
    }

    #[test]
    fn throughput_profile_loader_uses_protected_artifact_reader() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("throughput.json");
        super::super::artifact::write_json_new(&path, &throughput_profile()).unwrap();
        assert_eq!(
            read_throughput_profile(&path).unwrap(),
            throughput_profile()
        );

        let link = directory.path().join("throughput-link.json");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(matches!(
                read_throughput_profile(&link),
                Err(AssessmentError::ThroughputProfileArtifact(_))
            ));
        }
    }

    #[test]
    fn assessment_validation_recomputes_embedded_projection() {
        let mut artifact = assessment(10);
        let profile = throughput_profile();
        artifact.projected_window =
            project_outage_window(&artifact.scope_estimates, Some(&profile), 17, 1_100);
        artifact.validate().unwrap();

        let ProjectedWindow::Estimated {
            seconds,
            throughput_profile,
            ..
        } = &mut artifact.projected_window
        else {
            panic!("valid profile did not produce an estimate");
        };
        *seconds += 1;
        throughput_profile.copy_bytes_per_second += 1;
        assert!(matches!(
            artifact.validate(),
            Err(AssessmentError::InvalidProjectedWindow)
        ));
    }

    #[test]
    fn assessment_validation_rejects_invalid_projection_relation_manifest() {
        let mut artifact = assessment(10);
        let profile = throughput_profile();
        artifact.projected_window =
            project_outage_window(&artifact.scope_estimates, Some(&profile), 17, 1_100);
        artifact.validate().unwrap();

        let ProjectedWindow::Estimated {
            included_relations, ..
        } = &mut artifact.projected_window
        else {
            panic!("valid profile did not produce an estimate");
        };
        included_relations.push(included_relations[0].clone());
        assert!(matches!(
            artifact.validate(),
            Err(AssessmentError::InvalidProjectedWindow)
        ));

        let mut artifact = assessment(10);
        artifact.projected_window =
            project_outage_window(&artifact.scope_estimates, Some(&profile), 17, 1_100);
        let ProjectedWindow::Estimated {
            included_relations, ..
        } = &mut artifact.projected_window
        else {
            panic!("valid profile did not produce an estimate");
        };
        included_relations.clear();
        assert!(matches!(
            artifact.validate(),
            Err(AssessmentError::InvalidProjectedWindow)
        ));

        let mut artifact = assessment(10);
        artifact.projected_window =
            project_outage_window(&artifact.scope_estimates, Some(&profile), 17, 1_100);
        let ProjectedWindow::Estimated {
            included_relations, ..
        } = &mut artifact.projected_window
        else {
            panic!("valid profile did not produce an estimate");
        };
        included_relations[0] = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("not_assessed").unwrap(),
        };
        assert!(matches!(
            artifact.validate(),
            Err(AssessmentError::InvalidProjectedWindow)
        ));
    }

    fn throughput_profile() -> ThroughputProfile {
        ThroughputProfile {
            schema_version: THROUGHPUT_PROFILE_SCHEMA_VERSION,
            measurement_reference: "benchmark-17-a".into(),
            environment_reference: "pg17-local-ssd".into(),
            postgres_major_version: 17,
            measured_at_unix_seconds: 1_000,
            valid_for_seconds: 200,
            copy_bytes_per_second: 200,
            verification_bytes_per_second: 500,
        }
    }

    fn scope_estimate(relation_bytes: u64) -> ScopeEstimate {
        ScopeEstimate {
            table: QualifiedTable {
                namespace: Identifier::new("public").unwrap(),
                name: Identifier::new(format!("table_{relation_bytes}")).unwrap(),
            },
            estimated_rows: 1,
            relation_bytes,
            total_relation_bytes: relation_bytes + 100,
        }
    }

    #[test]
    fn customer_marker_text_cannot_hide_stable_report_content() {
        let report = "stable <!-- sql-splitter:volatile-begin --> text\n<!-- sql-splitter:volatile-begin -->\nvolatile\n<!-- sql-splitter:volatile-end -->\nend";
        assert_eq!(
            stable_report_body(report),
            "stable <!-- sql-splitter:volatile-begin --> textend"
        );
    }

    #[test]
    fn every_catalog_kind_has_a_stable_name() {
        let kinds = [
            CatalogObjectKind::Table,
            CatalogObjectKind::Column,
            CatalogObjectKind::Sequence,
            CatalogObjectKind::PrimaryKey,
            CatalogObjectKind::UniqueConstraint,
            CatalogObjectKind::CheckConstraint,
            CatalogObjectKind::ForeignKey,
            CatalogObjectKind::Index,
            CatalogObjectKind::View,
            CatalogObjectKind::Routine,
            CatalogObjectKind::Trigger,
            CatalogObjectKind::Event,
            CatalogObjectKind::Partition,
            CatalogObjectKind::Tablespace,
            CatalogObjectKind::Vendor("extension".into()),
        ];
        let names = kinds
            .iter()
            .map(object_kind_name)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(names.len(), 15);
        assert!(names.iter().all(|name| !name.is_empty()));
    }

    #[test]
    fn unknown_vendor_object_fails_report_rendering() {
        let mut artifact = assessment(10);
        let mut unknown = artifact
            .reviewed_plan
            .plan
            .source_catalog
            .as_mut()
            .unwrap()
            .namespaces[0]
            .objects[0]
            .clone();
        unknown.id = "vendor:future".into();
        unknown.name = Identifier::new("future").unwrap();
        unknown.kind = CatalogObjectKind::Vendor("future".into());
        artifact
            .reviewed_plan
            .plan
            .source_catalog
            .as_mut()
            .unwrap()
            .namespaces[0]
            .objects
            .push(unknown);
        let catalog_bytes =
            serde_json::to_vec(artifact.reviewed_plan.plan.source_catalog.as_ref().unwrap())
                .unwrap();
        artifact.reviewed_plan.plan.source_catalog_fingerprint =
            hex::encode(Sha256::digest(catalog_bytes));
        artifact.reviewed_plan = ReviewedPlan::new(artifact.reviewed_plan.plan).unwrap();
        assert!(matches!(
            render_markdown(&artifact),
            Err(AssessmentError::UnclassifiedVendorObject { .. })
        ));
    }

    #[test]
    fn catalog_objects_are_classified_into_one_inventory_bucket() {
        let mut artifact = assessment(10);
        let object_id = artifact
            .reviewed_plan
            .plan
            .source_catalog
            .as_ref()
            .unwrap()
            .namespaces[0]
            .objects[0]
            .id
            .clone();
        artifact
            .reviewed_plan
            .plan
            .unsupported_objects
            .objects
            .push(UnsupportedObject {
                code: UnsupportedObjectCode::ResumableKey,
                object_id,
                object_kind: "resumable_key".into(),
                reason: "fixture table has no resumable key".into(),
                required_semantics: true,
            });
        artifact
            .reviewed_plan
            .plan
            .unsupported_objects
            .objects
            .push(UnsupportedObject {
                code: UnsupportedObjectCode::NamespaceAcl,
                object_id: "namespace-acl:1".into(),
                object_kind: "namespace_acl".into(),
                reason: "fixture review requirement".into(),
                required_semantics: false,
            });
        artifact.reviewed_plan = ReviewedPlan::new(artifact.reviewed_plan.plan).unwrap();
        let inventory = classify_catalog_inventory(
            artifact.reviewed_plan.plan.source_catalog.as_ref().unwrap(),
            &artifact.reviewed_plan.plan.unsupported_objects,
        )
        .unwrap();
        assert_eq!(inventory.blocking.get("table"), Some(&1));
        assert!(!inventory.supported.contains_key("table"));
        assert_eq!(inventory.review_required.get("namespace"), Some(&1));
        assert!(!inventory.supported.contains_key("namespace"));
    }

    #[test]
    fn report_is_deterministic_and_does_not_render_definitions() {
        let first = assessment(10);
        let second = assessment(20);
        let first_report = render_markdown(&first).unwrap();
        let second_report = render_markdown(&second).unwrap();
        assert_eq!(
            stable_report_body(&first_report),
            stable_report_body(&second_report)
        );
        assert!(!first_report.contains("private row literal"));
        assert!(first_report.contains("hostile\\|name"));
        assert!(first_report.contains("`acl.report_only`: `approval_required`"));
        assert!(first_report.contains("Target endpoint: **not assessed**"));
        assert!(first_report.contains(
            "sql-splitter records operator evidence but does **not** enforce the external source freeze"
        ));
    }

    fn assessment(estimated_rows: i64) -> AssessmentArtifact {
        let table = QualifiedTable {
            namespace: Identifier::new("public").unwrap(),
            name: Identifier::new("hostile|name").unwrap(),
        };
        let catalog = VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: Identifier::new("source").unwrap(),
            namespaces: vec![CatalogNamespace {
                id: "namespace:1".into(),
                name: table.namespace.clone(),
                owner: None,
                charset: None,
                collation: None,
                objects: vec![CatalogObject {
                    id: "relation:1".into(),
                    kind: CatalogObjectKind::Table,
                    name: table.name.clone(),
                    definition: b"private row literal".to_vec(),
                    attributes: BTreeMap::new(),
                }],
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&catalog).unwrap()));
        let reviewed_plan = ReviewedPlan::new(MigrationPlan {
            schema_version: PLAN_SCHEMA_VERSION,
            purpose: PlanPurpose::Assessment,
            migration_id: "assessment".into(),
            tool_version: "test".into(),
            source_endpoint_identity: "postgres://source/database".into(),
            target_endpoint_identity: AssessmentStatus::NotAssessed,
            source_catalog_fingerprint: fingerprint,
            target_catalog_fingerprint: AssessmentStatus::NotAssessed,
            source_catalog: Some(catalog),
            target_catalog: AssessmentStatus::NotAssessed,
            source_tls_binding: "hostname_verified".into(),
            target_tls_binding: AssessmentStatus::NotAssessed,
            consistency_mode: "not_assessed".into(),
            canonical_encoding_version: CANONICAL_ENCODING_VERSION,
            conversion_policy: POSTGRESQL_ASSESSMENT_CONVERSION_POLICY,
            outage_policy: None,
            postgres_source_profile: None,
            mysql_source_profile: None,
            mysql_snapshot_evidence: None,
            mysql_target_snapshot_evidence: None,
            mysql_metadata_visibility: None,
            mysql_target_metadata_visibility: None,
            mysql_authorization: None,
            capabilities: BTreeMap::from([("acl.report_only".into(), "approval_required".into())]),
            operations: Vec::new(),
            unsupported_objects: UnsupportedObjectReport::default(),
        })
        .unwrap();
        AssessmentArtifact {
            schema_version: ASSESSMENT_SCHEMA_VERSION,
            reviewed_plan,
            source_evidence: SourceAssessmentEvidence {
                server_version: "17".into(),
                server_version_num: 170_000,
                tls_binding: "hostname_verified".into(),
                transaction_read_only: true,
                direct_write_privileges_absent: true,
            },
            execution_requirements: vec![ExecutionRequirement {
                name: "write_fence".into(),
                status: EvidenceStatus::NotAssessed {
                    reason: "requires a separate preflight".into(),
                },
                detail: "probe evidence is not collected by assessment".into(),
            }],
            scope_estimates: vec![ScopeEstimate {
                table,
                estimated_rows,
                relation_bytes: 100,
                total_relation_bytes: 200,
            }],
            projected_window: ProjectedWindow::NotAssessed {
                reason: "no throughput measurement".into(),
            },
        }
    }
}
