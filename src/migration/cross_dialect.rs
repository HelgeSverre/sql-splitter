//! Reviewed cross-dialect plan construction.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::artifact::{read_json, write_json_new, ArtifactError};
use super::canonical::CANONICAL_ENCODING_VERSION;
use super::conversion::{
    ConversionDialect, MigrationConversionPolicy, QualifiedIdentifier, TableConversionPolicy,
};
use super::model::{CatalogObjectKind, Identifier, QualifiedTable, VendorCatalog};
use super::mysql::{
    mysql_catalog_fingerprint, mysql_catalog_visibility_is_complete,
    mysql_to_postgres_table_conversion_policy, validate_metadata_visibility_capture,
    MySqlCatalogSnapshot, MySqlEndpointConfig, MySqlMetadataVisibilityCapture, MySqlPlanError,
};
use super::mysql_profile::MySqlFreezeProfileContract;
use super::plan::{
    AssessmentStatus, MigrationPlan, OperationKind, PlanError, PlanOperation, PlanPurpose,
    ReviewedPlan, UnsupportedObject, UnsupportedObjectCode, UnsupportedObjectReport,
    PLAN_SCHEMA_VERSION,
};
use super::postgres::{
    catalog_fingerprint, postgres_to_mysql_table_conversion_policy, CatalogSnapshot,
    PostgresConsistencyMode, PostgresEndpointConfig, PostgresPlanError, CATALOG_FORMAT_VERSION,
};

pub const CROSS_DIALECT_MAPPING_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossDialectTableMapping {
    pub source: QualifiedTable,
    pub target: QualifiedTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "dialect", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrossDialectTargetDefaults {
    MySql {
        character_set: Identifier,
        collation: Identifier,
    },
    PostgreSql {
        text_collation: QualifiedIdentifier,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossDialectMapping {
    pub schema_version: u16,
    pub source_dialect: ConversionDialect,
    pub target_dialect: ConversionDialect,
    pub target_defaults: CrossDialectTargetDefaults,
    pub tables: Vec<CrossDialectTableMapping>,
}

impl CrossDialectMapping {
    pub fn validate(&self) -> Result<(), CrossDialectPlanError> {
        if self.schema_version != CROSS_DIALECT_MAPPING_SCHEMA_VERSION {
            return Err(CrossDialectPlanError::UnsupportedMappingVersion {
                found: self.schema_version,
            });
        }
        if self.source_dialect == self.target_dialect || self.tables.is_empty() {
            return Err(CrossDialectPlanError::InvalidMapping);
        }
        match (
            self.source_dialect,
            self.target_dialect,
            &self.target_defaults,
        ) {
            (
                ConversionDialect::PostgreSql,
                ConversionDialect::MySql,
                CrossDialectTargetDefaults::MySql {
                    character_set,
                    collation,
                },
            ) if character_set.as_str() == "utf8mb4"
                && matches!(collation.as_str(), "utf8mb4_bin" | "utf8mb4_0900_bin") => {}
            (
                ConversionDialect::MySql,
                ConversionDialect::PostgreSql,
                CrossDialectTargetDefaults::PostgreSql { .. },
            ) => {}
            _ => return Err(CrossDialectPlanError::InvalidMapping),
        }
        let mut source_tables = BTreeSet::new();
        let mut target_tables = BTreeSet::new();
        let mut previous = None;
        for mapping in &self.tables {
            if previous.is_some_and(|previous| previous >= &mapping.source)
                || !source_tables.insert(&mapping.source)
                || !target_tables.insert(&mapping.target)
            {
                return Err(CrossDialectPlanError::InvalidMapping);
            }
            previous = Some(&mapping.source);
        }
        Ok(())
    }
}

/// Inspect a PostgreSQL source and empty MySQL target, then publish one
/// reviewed PostgreSQL-to-MySQL plan.
pub fn write_live_postgres_to_mysql_plan(
    source_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    target_metadata_config_path: impl AsRef<Path>,
    mapping_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<ReviewedPlan, CrossDialectPlanError> {
    let source_config = PostgresEndpointConfig::read(source_config_path.as_ref())?;
    let target_config = MySqlEndpointConfig::read(target_config_path.as_ref())?;
    let target_metadata = MySqlEndpointConfig::read(target_metadata_config_path.as_ref())?;
    require_separate_credentials(&[
        &source_config.credential_env,
        &target_config.credential_env,
        &target_metadata.credential_env,
    ])?;
    let mapping: CrossDialectMapping = read_json(mapping_path)?;
    let source = super::postgres::inspect_endpoint(&source_config)?;
    let target = super::mysql::inspect_live_endpoint(target_config.clone())?;
    let visibility = super::mysql::collect_mysql_metadata_visibility(
        &target,
        &target_config,
        &target_metadata,
        None,
    )?;
    let reviewed = build_postgres_to_mysql_plan(&source, &target, &visibility, &mapping)?;
    write_json_new(output_path, &reviewed)?;
    Ok(reviewed)
}

/// Inspect a continuously frozen MySQL source and empty PostgreSQL target,
/// then publish one reviewed MySQL-to-PostgreSQL plan.
pub fn write_live_mysql_to_postgres_plan(
    source_config_path: impl AsRef<Path>,
    source_metadata_config_path: impl AsRef<Path>,
    freeze_config_path: impl AsRef<Path>,
    target_config_path: impl AsRef<Path>,
    mapping_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<ReviewedPlan, CrossDialectPlanError> {
    let source_config = MySqlEndpointConfig::read(source_config_path.as_ref())?;
    let source_metadata = MySqlEndpointConfig::read(source_metadata_config_path.as_ref())?;
    let freeze_config = MySqlEndpointConfig::read(freeze_config_path.as_ref())?;
    let target_config = PostgresEndpointConfig::read(target_config_path.as_ref())?;
    require_separate_credentials(&[
        &source_config.credential_env,
        &source_metadata.credential_env,
        &freeze_config.credential_env,
        &target_config.credential_env,
    ])?;
    let mapping: CrossDialectMapping = read_json(mapping_path)?;
    let source = super::mysql::inspect_live_endpoint(source_config.clone())?;
    let visibility = super::mysql::collect_mysql_metadata_visibility(
        &source,
        &source_config,
        &source_metadata,
        Some(&freeze_config),
    )?;
    let target = super::postgres::inspect_endpoint(&target_config)?;
    let reviewed = build_mysql_to_postgres_plan(&source, &visibility, &target, &mapping)?;
    write_json_new(output_path, &reviewed)?;
    Ok(reviewed)
}

fn require_separate_credentials(credentials: &[&String]) -> Result<(), CrossDialectPlanError> {
    if credentials
        .iter()
        .enumerate()
        .any(|(index, credential)| credentials[..index].contains(credential))
    {
        return Err(CrossDialectPlanError::SharedCredentials);
    }
    Ok(())
}

/// Build one PostgreSQL-to-MySQL reviewed plan from exact endpoint evidence.
pub fn build_postgres_to_mysql_plan(
    source: &CatalogSnapshot,
    target: &MySqlCatalogSnapshot,
    target_visibility: &MySqlMetadataVisibilityCapture,
    mapping: &CrossDialectMapping,
) -> Result<ReviewedPlan, CrossDialectPlanError> {
    mapping.validate()?;
    let CrossDialectTargetDefaults::MySql {
        character_set,
        collation,
    } = &mapping.target_defaults
    else {
        return Err(CrossDialectPlanError::InvalidMapping);
    };
    if mapping.source_dialect != ConversionDialect::PostgreSql
        || mapping.target_dialect != ConversionDialect::MySql
        || !mysql_catalog_visibility_is_complete(target, target_visibility)?
    {
        return Err(CrossDialectPlanError::EndpointEvidenceMismatch);
    }
    require_complete_source_table_mapping(&source.catalog, &mapping.tables)?;
    if mapping
        .tables
        .iter()
        .any(|table| table.target.namespace != target_visibility.authoritative_catalog.database)
    {
        return Err(CrossDialectPlanError::InvalidMapping);
    }

    let mut policies = mapping
        .tables
        .iter()
        .map(|table| {
            postgres_to_mysql_table_conversion_policy(
                &source.catalog,
                &table.source,
                table.target.clone(),
                character_set.as_str(),
                collation.as_str(),
            )
            .map_err(CrossDialectPlanError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    policies.sort_by(|left, right| left.source_table.cmp(&right.source_table));
    let conversion_policy = MigrationConversionPolicy::cross_dialect(
        ConversionDialect::PostgreSql,
        ConversionDialect::MySql,
        policies.clone(),
    )?;
    let operations = cross_dialect_operations(&policies)?;
    let source_fingerprint = catalog_fingerprint(&source.catalog)?;
    let target_fingerprint = mysql_catalog_fingerprint(&target_visibility.authoritative_catalog)
        .map_err(|error| {
            CrossDialectPlanError::MySql(MySqlPlanError::InvalidCatalog(error.to_string()))
        })?;
    let mut unsupported = source.unsupported.objects.clone();
    unsupported.extend(unmodeled_postgres_cross_dialect_objects(
        &source.catalog,
        &policies,
    ));
    unsupported.extend(
        target_visibility
            .authoritative_blockers
            .iter()
            .map(|blocker| UnsupportedObject {
                code: UnsupportedObjectCode::MySqlCatalogSemantics,
                object_id: blocker.object_id.clone(),
                object_kind: "cross_dialect_target_semantics".into(),
                reason: blocker.reason.clone(),
                required_semantics: true,
            }),
    );
    let target_object_count = target_visibility
        .authoritative_catalog
        .namespaces
        .iter()
        .flat_map(|namespace| &namespace.objects)
        .filter(|object| {
            !matches!(&object.kind, CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege")
        })
        .count();
    if target_object_count != 0 {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::TargetNotEmpty,
            object_id: "cross-dialect-target-catalog-not-empty".into(),
            object_kind: "target_precondition".into(),
            reason: format!("target catalog contains {target_object_count} user objects"),
            required_semantics: true,
        });
    }
    unsupported
        .sort_by(|left, right| (&left.object_id, left.code).cmp(&(&right.object_id, right.code)));
    unsupported
        .dedup_by(|left, right| left.code == right.code && left.object_id == right.object_id);
    ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Execution,
        migration_id: format!("pg-mysql-{}", &source_fingerprint[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity.clone(),
        target_endpoint_identity: AssessmentStatus::Assessed(target.endpoint_identity.clone()),
        source_catalog_fingerprint: source_fingerprint,
        target_catalog_fingerprint: AssessmentStatus::Assessed(target_fingerprint),
        source_catalog: Some(source.catalog.clone()),
        target_catalog: AssessmentStatus::Assessed(target_visibility.authoritative_catalog.clone()),
        source_tls_binding: source.tls_binding.clone(),
        target_tls_binding: AssessmentStatus::Assessed(target.tls_binding.clone()),
        target_mode: Some(AssessmentStatus::Assessed(
            super::plan::TargetModeContract::EmptyOwned,
        )),
        consistency_mode: PostgresConsistencyMode::WriteFence.as_str().into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy,
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: None,
        mysql_snapshot_evidence: None,
        mysql_target_snapshot_evidence: Some(target.snapshot_evidence.clone()),
        mysql_metadata_visibility: None,
        mysql_target_metadata_visibility: Some(target_visibility.evidence.clone()),
        mysql_authorization: None,
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "repeatable_read_read_only".into(),
            ),
            ("source_tls".into(), source.tls_binding.clone()),
            ("target_tls".into(), target.tls_binding.clone()),
            ("cross_dialect".into(), "postgresql_to_mysql_v1".into()),
        ]),
        operations,
        unsupported_objects: UnsupportedObjectReport {
            objects: unsupported,
        },
    })
    .map_err(CrossDialectPlanError::from)
}

/// Build one MySQL-to-PostgreSQL reviewed plan from exact endpoint evidence.
pub fn build_mysql_to_postgres_plan(
    source: &MySqlCatalogSnapshot,
    source_visibility: &MySqlMetadataVisibilityCapture,
    target: &CatalogSnapshot,
    mapping: &CrossDialectMapping,
) -> Result<ReviewedPlan, CrossDialectPlanError> {
    mapping.validate()?;
    let CrossDialectTargetDefaults::PostgreSql { text_collation } = &mapping.target_defaults else {
        return Err(CrossDialectPlanError::InvalidMapping);
    };
    if mapping.source_dialect != ConversionDialect::MySql
        || mapping.target_dialect != ConversionDialect::PostgreSql
        || target.catalog.dialect != "postgresql"
        || target.catalog.format_version != CATALOG_FORMAT_VERSION
    {
        return Err(CrossDialectPlanError::EndpointEvidenceMismatch);
    }
    validate_metadata_visibility_capture(source, source_visibility)?;
    require_complete_source_table_mapping(
        &source_visibility.authoritative_catalog,
        &mapping.tables,
    )?;
    let target_namespaces = target
        .catalog
        .namespaces
        .iter()
        .map(|namespace| &namespace.name)
        .collect::<BTreeSet<_>>();
    if mapping
        .tables
        .iter()
        .any(|table| !target_namespaces.contains(&table.target.namespace))
    {
        return Err(CrossDialectPlanError::InvalidMapping);
    }

    let mut policies = mapping
        .tables
        .iter()
        .map(|table| {
            mysql_to_postgres_table_conversion_policy(
                &source_visibility.authoritative_catalog,
                &table.source,
                table.target.clone(),
                text_collation.clone(),
            )
            .map_err(CrossDialectPlanError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    policies.sort_by(|left, right| left.source_table.cmp(&right.source_table));
    let conversion_policy = MigrationConversionPolicy::cross_dialect(
        ConversionDialect::MySql,
        ConversionDialect::PostgreSql,
        policies.clone(),
    )?;
    let operations = cross_dialect_operations(&policies)?;
    let source_fingerprint = mysql_catalog_fingerprint(&source_visibility.authoritative_catalog)
        .map_err(|error| {
            CrossDialectPlanError::MySql(MySqlPlanError::InvalidCatalog(error.to_string()))
        })?;
    let target_fingerprint = catalog_fingerprint(&target.catalog)?;
    let mut unsupported = source_visibility
        .authoritative_blockers
        .iter()
        .map(|blocker| UnsupportedObject {
            code: UnsupportedObjectCode::MySqlCatalogSemantics,
            object_id: blocker.object_id.clone(),
            object_kind: blocker.object_kind.clone(),
            reason: blocker.reason.clone(),
            required_semantics: true,
        })
        .collect::<Vec<_>>();
    let privilege_blockers = source_visibility
        .evidence
        .non_operational_records()
        .into_iter()
        .map(|record| {
            Ok(UnsupportedObject {
                code: UnsupportedObjectCode::MySqlCatalogSemantics,
                object_id: record.canonical_id()?,
                object_kind: "privilege".into(),
                reason:
                    "MySQL business authorization has no reviewed cross-dialect target role mapping"
                        .into(),
                required_semantics: true,
            })
        })
        .collect::<Result<Vec<_>, super::mysql_visibility::MySqlVisibilityError>>()
        .map_err(MySqlPlanError::from)?;
    unsupported.extend(privilege_blockers);
    unsupported.extend(unmodeled_mysql_cross_dialect_objects(
        &source_visibility.authoritative_catalog,
        &policies,
    ));
    unsupported.extend(target.unsupported.objects.iter().cloned());
    let target_object_count = target
        .catalog
        .namespaces
        .iter()
        .map(|namespace| namespace.objects.len())
        .sum::<usize>();
    if target_object_count != 0 {
        unsupported.push(UnsupportedObject {
            code: UnsupportedObjectCode::TargetNotEmpty,
            object_id: "cross-dialect-target-catalog-not-empty".into(),
            object_kind: "target_precondition".into(),
            reason: format!("target catalog contains {target_object_count} user objects"),
            required_semantics: true,
        });
    }
    unsupported
        .sort_by(|left, right| (&left.object_id, left.code).cmp(&(&right.object_id, right.code)));
    unsupported
        .dedup_by(|left, right| left.code == right.code && left.object_id == right.object_id);

    ReviewedPlan::new(MigrationPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        purpose: PlanPurpose::Execution,
        migration_id: format!("mysql-pg-{}", &source_fingerprint[..16]),
        tool_version: env!("CARGO_PKG_VERSION").into(),
        source_endpoint_identity: source.endpoint_identity.clone(),
        target_endpoint_identity: AssessmentStatus::Assessed(target.endpoint_identity.clone()),
        source_catalog_fingerprint: source_fingerprint,
        target_catalog_fingerprint: AssessmentStatus::Assessed(target_fingerprint),
        source_catalog: Some(source_visibility.authoritative_catalog.clone()),
        target_catalog: AssessmentStatus::Assessed(target.catalog.clone()),
        source_tls_binding: source.tls_binding.clone(),
        target_tls_binding: AssessmentStatus::Assessed(target.tls_binding.clone()),
        target_mode: Some(AssessmentStatus::Assessed(
            super::plan::TargetModeContract::EmptyOwned,
        )),
        consistency_mode: super::mysql::MYSQL_CONSISTENCY_SNAPSHOT.into(),
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        conversion_policy,
        outage_policy: None,
        postgres_source_profile: None,
        mysql_source_profile: Some(MySqlFreezeProfileContract::external_continuous_freeze()),
        mysql_snapshot_evidence: Some(source.snapshot_evidence.clone()),
        mysql_target_snapshot_evidence: None,
        mysql_metadata_visibility: Some(source_visibility.evidence.clone()),
        mysql_target_metadata_visibility: None,
        mysql_authorization: None,
        capabilities: BTreeMap::from([
            (
                "catalog_snapshot".into(),
                "mysql_repeatable_read_consistent_snapshot".into(),
            ),
            ("source_tls".into(), source.tls_binding.clone()),
            ("target_tls".into(), target.tls_binding.clone()),
            ("cross_dialect".into(), "mysql_to_postgresql_v1".into()),
            ("mysql.ddl_freeze".into(), "required_not_assessed".into()),
            ("mysql.catalog_snapshot_protected".into(), "false".into()),
        ]),
        operations,
        unsupported_objects: UnsupportedObjectReport {
            objects: unsupported,
        },
    })
    .map_err(CrossDialectPlanError::from)
}

fn cross_dialect_operations(
    policies: &[TableConversionPolicy],
) -> Result<Vec<PlanOperation>, CrossDialectPlanError> {
    let mut operations = Vec::new();
    for policy in policies {
        let create = PlanOperation::new(
            OperationKind::CreateTable,
            Some(policy.target_table.clone()),
            Vec::new(),
            BTreeMap::from([(
                "cross_dialect_target_table_policy".into(),
                serde_json::to_value(policy)?,
            )]),
        )?;
        let copy = PlanOperation::new(
            OperationKind::CopyTable,
            Some(policy.source_table.clone()),
            vec![create.id.clone()],
            BTreeMap::from([(
                "table_conversion_policy".into(),
                serde_json::to_value(policy)?,
            )]),
        )?;
        let verify = PlanOperation::new(
            OperationKind::VerifyTable,
            Some(policy.source_table.clone()),
            vec![copy.id.clone()],
            BTreeMap::from([(
                "cross_dialect_verification_policy".into(),
                serde_json::to_value(policy)?,
            )]),
        )?;
        operations.extend([create, copy, verify]);
    }
    let verify_schema = PlanOperation::new(
        OperationKind::VerifySchema,
        None,
        operations
            .iter()
            .map(|operation| operation.id.clone())
            .collect(),
        BTreeMap::new(),
    )?;
    operations.push(verify_schema);
    Ok(operations)
}

fn require_complete_source_table_mapping(
    catalog: &VendorCatalog,
    mappings: &[CrossDialectTableMapping],
) -> Result<(), CrossDialectPlanError> {
    let catalog_tables = catalog
        .namespaces
        .iter()
        .flat_map(|namespace| {
            namespace
                .objects
                .iter()
                .filter(|object| object.kind == CatalogObjectKind::Table)
                .map(|object| QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: object.name.clone(),
                })
        })
        .collect::<BTreeSet<_>>();
    let mapped_tables = mappings
        .iter()
        .map(|mapping| mapping.source.clone())
        .collect::<BTreeSet<_>>();
    if catalog_tables != mapped_tables {
        return Err(CrossDialectPlanError::IncompleteMapping);
    }
    Ok(())
}

fn unmodeled_postgres_cross_dialect_objects(
    catalog: &VendorCatalog,
    policies: &[TableConversionPolicy],
) -> Vec<UnsupportedObject> {
    let mapped_tables = policies
        .iter()
        .map(|policy| &policy.source_table)
        .collect::<BTreeSet<_>>();
    let selected_keys = policies
        .iter()
        .map(|policy| (&policy.source_table, &policy.resumable_key.source_name))
        .collect::<BTreeSet<_>>();
    let mut unsupported = Vec::new();
    for namespace in &catalog.namespaces {
        let table_ids = namespace
            .objects
            .iter()
            .filter(|object| object.kind == CatalogObjectKind::Table)
            .map(|object| {
                (
                    object.id.as_str(),
                    QualifiedTable {
                        namespace: namespace.name.clone(),
                        name: object.name.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for object in &namespace.objects {
            let table = object
                .attributes
                .get("table_oid")
                .and_then(serde_json::Value::as_str)
                .and_then(|table_id| table_ids.get(table_id));
            let supported = match object.kind {
                CatalogObjectKind::Table => mapped_tables.contains(&QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: object.name.clone(),
                }),
                CatalogObjectKind::Column => {
                    table.is_some_and(|table| mapped_tables.contains(table))
                }
                CatalogObjectKind::PrimaryKey | CatalogObjectKind::UniqueConstraint => {
                    table.is_some_and(|table| selected_keys.contains(&(table, &object.name)))
                }
                CatalogObjectKind::Index => object
                    .attributes
                    .get("constraint_oid")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|constraint_id| {
                        namespace.objects.iter().any(|constraint| {
                            constraint.id == constraint_id
                                && matches!(
                                    constraint.kind,
                                    CatalogObjectKind::PrimaryKey
                                        | CatalogObjectKind::UniqueConstraint
                                )
                                && table.is_some_and(|table| {
                                    selected_keys.contains(&(table, &constraint.name))
                                })
                        })
                    }),
                _ => false,
            };
            if !supported {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::MySqlCatalogSemantics,
                    object_id: object.id.clone(),
                    object_kind: "cross_dialect_source_semantics".into(),
                    reason: "source object is outside the approved cross-dialect table subset"
                        .into(),
                    required_semantics: true,
                });
            }
        }
    }
    unsupported
}

fn unmodeled_mysql_cross_dialect_objects(
    catalog: &VendorCatalog,
    policies: &[TableConversionPolicy],
) -> Vec<UnsupportedObject> {
    let mapped_tables = policies
        .iter()
        .map(|policy| &policy.source_table)
        .collect::<BTreeSet<_>>();
    let selected_keys = policies
        .iter()
        .map(|policy| (&policy.source_table, &policy.resumable_key.source_name))
        .collect::<BTreeSet<_>>();
    let mut unsupported = Vec::new();
    for namespace in &catalog.namespaces {
        for object in &namespace.objects {
            let table = object
                .attributes
                .get("table_name")
                .and_then(serde_json::Value::as_str)
                .and_then(|name| Identifier::new(name).ok())
                .map(|name| QualifiedTable {
                    namespace: namespace.name.clone(),
                    name,
                });
            let supported = match object.kind {
                CatalogObjectKind::Table => mapped_tables.contains(&QualifiedTable {
                    namespace: namespace.name.clone(),
                    name: object.name.clone(),
                }),
                CatalogObjectKind::Column => table
                    .as_ref()
                    .is_some_and(|table| mapped_tables.contains(table)),
                CatalogObjectKind::PrimaryKey | CatalogObjectKind::UniqueConstraint => table
                    .as_ref()
                    .is_some_and(|table| selected_keys.contains(&(table, &object.name))),
                _ => false,
            };
            if !supported
                && !matches!(&object.kind, CatalogObjectKind::Vendor(kind) if kind == "mysql_privilege")
            {
                unsupported.push(UnsupportedObject {
                    code: UnsupportedObjectCode::MySqlCatalogSemantics,
                    object_id: object.id.clone(),
                    object_kind: "cross_dialect_source_semantics".into(),
                    reason: "source object is outside the approved cross-dialect table subset"
                        .into(),
                    required_semantics: true,
                });
            }
        }
    }
    unsupported
}

#[derive(Debug, Error)]
pub enum CrossDialectPlanError {
    #[error("unsupported cross-dialect mapping schema version {found}")]
    UnsupportedMappingVersion { found: u16 },
    #[error("cross-dialect table mapping is invalid")]
    InvalidMapping,
    #[error("cross-dialect mapping does not cover every source table exactly once")]
    IncompleteMapping,
    #[error("cross-dialect endpoint evidence differs from the reviewed catalogs")]
    EndpointEvidenceMismatch,
    #[error("cross-dialect operational roles require separate credential references")]
    SharedCredentials,
    #[error("protected cross-dialect artifact operation failed")]
    Artifact(#[from] ArtifactError),
    #[error("PostgreSQL cross-dialect plan failed")]
    Postgres(#[from] PostgresPlanError),
    #[error("MySQL cross-dialect plan failed")]
    MySql(#[from] MySqlPlanError),
    #[error("row-conversion policy failed")]
    Conversion(#[from] super::conversion::RowConversionError),
    #[error("migration plan failed")]
    Plan(#[from] PlanError),
    #[error("cross-dialect plan serialization failed")]
    Serialize(#[from] serde_json::Error),
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::migration::conversion::{
        CrossDialectKeyKind, CrossDialectResumableKey, CrossDialectSourceType,
        CrossDialectTargetTableContract, CrossDialectTargetType, MySqlIntegerWidth,
        MySqlTargetEngine, MySqlTargetType, RowConversionPolicy, ValueConversionRule,
        ROW_TYPE_CONVERSION_SCHEMA_VERSION,
    };
    use crate::migration::model::{CatalogNamespace, CatalogObject, ColumnMeta};
    use crate::migration::mysql::{
        mysql_catalog_fingerprint, MySqlCatalogBlocker, MySqlColumnType, MySqlResumableKey,
        MYSQL_CATALOG_FORMAT_VERSION,
    };
    use crate::migration::mysql_visibility::{
        MySqlAccountIdentity, MySqlGrantInventory, MySqlGrantRecord, MySqlGrantTableColumn,
        MySqlMetadataVisibilityEvidence, MySqlOperationalAccountExclusion,
        MySqlOperationalAccountPurpose, MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
    };
    use crate::migration::plan::{
        MySqlSnapshotEvidence, MYSQL_SESSION_CHARACTER_SET, MYSQL_SESSION_COLLATION,
        MYSQL_STRICT_SQL_MODE,
    };

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).unwrap()
    }

    fn table(namespace: &str, name: &str) -> QualifiedTable {
        QualifiedTable {
            namespace: identifier(namespace),
            name: identifier(name),
        }
    }

    fn mapping(tables: Vec<CrossDialectTableMapping>) -> CrossDialectMapping {
        CrossDialectMapping {
            schema_version: CROSS_DIALECT_MAPPING_SCHEMA_VERSION,
            source_dialect: ConversionDialect::PostgreSql,
            target_dialect: ConversionDialect::MySql,
            target_defaults: CrossDialectTargetDefaults::MySql {
                character_set: identifier("utf8mb4"),
                collation: identifier("utf8mb4_0900_bin"),
            },
            tables,
        }
    }

    fn table_policy() -> TableConversionPolicy {
        let source_table = table("public", "items");
        let target_table = table("app", "items");
        let source = ColumnMeta {
            name: identifier("id"),
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
        TableConversionPolicy {
            source_table,
            target_table,
            target_contract: CrossDialectTargetTableContract::MySql {
                engine: MySqlTargetEngine::InnoDb,
                character_set: identifier("utf8mb4"),
                collation: identifier("utf8mb4_0900_bin"),
            },
            resumable_key: CrossDialectResumableKey {
                source_name: identifier("items_pkey"),
                source_kind: CrossDialectKeyKind::PrimaryKey,
                source_columns: vec![identifier("id")],
                target_name: None,
                target_kind: CrossDialectKeyKind::PrimaryKey,
                target_columns: vec![identifier("id")],
            },
            row_policy: RowConversionPolicy {
                schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
                source_dialect: ConversionDialect::PostgreSql,
                target_dialect: ConversionDialect::MySql,
                columns: vec![super::super::conversion::ColumnConversion {
                    source,
                    source_type: CrossDialectSourceType::PostgreSql(
                        super::super::conversion::PostgresTargetType::BigInt,
                    ),
                    target: target_type.column_meta(identifier("id"), 1, false).unwrap(),
                    target_type,
                    target_checks: Vec::new(),
                    rule: ValueConversionRule::SignedInteger {
                        minimum: i64::MIN,
                        maximum: i64::MAX,
                    },
                }],
            },
        }
    }

    fn mysql_catalog_id(kind: &str, database: &str, object: &str, member: &str) -> String {
        use sha2::{Digest, Sha256};

        let bytes = serde_json::to_vec(&(kind, database, object, member)).unwrap();
        format!("mysql:{kind}:sha256:{}", hex::encode(Sha256::digest(bytes)))
    }

    fn mysql_catalog(with_table: bool) -> VendorCatalog {
        let database = identifier("app");
        let mut objects = Vec::new();
        if with_table {
            let key = MySqlResumableKey {
                index_name: identifier("PRIMARY"),
                primary: true,
                columns: vec![identifier("id")],
                column_types: vec!["bigint".into()],
                collations: vec![None],
                server_version: "8.4.0".into(),
            };
            objects.extend([
                CatalogObject {
                    id: mysql_catalog_id("column", "app", "items", "id"),
                    kind: CatalogObjectKind::Column,
                    name: identifier("id"),
                    definition: b"bigint".to_vec(),
                    attributes: BTreeMap::from([
                        ("table_name".into(), serde_json::json!("items")),
                        ("ordinal".into(), serde_json::json!(1)),
                        ("default".into(), serde_json::Value::Null),
                        ("nullable".into(), serde_json::json!(false)),
                        ("data_type".into(), serde_json::json!("bigint")),
                        ("column_type".into(), serde_json::json!("bigint")),
                        (
                            "mysql_ddl_type".into(),
                            serde_json::to_value(MySqlColumnType::Integer {
                                name: "bigint".into(),
                                unsigned: false,
                                display_width: None,
                            })
                            .unwrap(),
                        ),
                        ("character_set".into(), serde_json::Value::Null),
                        ("collation".into(), serde_json::Value::Null),
                        ("extra".into(), serde_json::json!("")),
                        ("generation_expression".into(), serde_json::json!("")),
                        ("numeric_precision".into(), serde_json::json!(64)),
                        ("numeric_scale".into(), serde_json::json!(0)),
                    ]),
                },
                CatalogObject {
                    id: mysql_catalog_id("table", "app", "items", ""),
                    kind: CatalogObjectKind::Table,
                    name: identifier("items"),
                    definition:
                        b"CREATE TABLE `items` (`id` bigint NOT NULL PRIMARY KEY) ENGINE=InnoDB"
                            .to_vec(),
                    attributes: BTreeMap::from([
                        ("engine".into(), serde_json::json!("InnoDB")),
                        ("character_set".into(), serde_json::json!("utf8mb4")),
                        ("collation".into(), serde_json::json!("utf8mb4_0900_bin")),
                        ("create_options".into(), serde_json::json!("")),
                        ("auto_increment".into(), serde_json::Value::Null),
                        ("auto_increment_column".into(), serde_json::Value::Null),
                        ("resumable_key".into(), serde_json::to_value(key).unwrap()),
                    ]),
                },
                CatalogObject {
                    id: mysql_catalog_id("index", "app", "items", "PRIMARY"),
                    kind: CatalogObjectKind::PrimaryKey,
                    name: identifier("PRIMARY"),
                    definition: Vec::new(),
                    attributes: BTreeMap::from([
                        ("table_name".into(), serde_json::json!("items")),
                        ("non_unique".into(), serde_json::json!(false)),
                        ("primary".into(), serde_json::json!(true)),
                        ("constraint_backed".into(), serde_json::json!(true)),
                        ("index_type".into(), serde_json::json!("BTREE")),
                        ("visible".into(), serde_json::json!(true)),
                        (
                            "columns".into(),
                            serde_json::json!([{
                                "name": "id",
                                "ordinal": 1,
                                "ascending": true,
                                "prefix_length": null,
                                "nullable": false,
                                "expression": null
                            }]),
                        ),
                    ]),
                },
            ]);
            objects.sort_by(|left, right| left.id.cmp(&right.id));
        }
        VendorCatalog {
            format_version: MYSQL_CATALOG_FORMAT_VERSION,
            dialect: "mysql".into(),
            server_version: "8.4.0".into(),
            database: database.clone(),
            namespaces: vec![CatalogNamespace {
                id: mysql_catalog_id("schema", "app", "app", ""),
                name: database,
                owner: None,
                charset: Some("utf8mb4".into()),
                collation: Some("utf8mb4_0900_bin".into()),
                objects,
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::from([
                ("information_schema_stats_expiry".into(), "0".into()),
                ("lower_case_table_names".into(), "0".into()),
            ]),
        }
    }

    fn mysql_snapshot(endpoint: &str, with_table: bool) -> MySqlCatalogSnapshot {
        let catalog = mysql_catalog(with_table);
        let fingerprint = mysql_catalog_fingerprint(&catalog).unwrap();
        MySqlCatalogSnapshot {
            endpoint_identity: endpoint.into(),
            database_identity: "app".into(),
            server_version: "8.4.0".into(),
            tls_binding: "hostname_verified;roots=platform;client=none".into(),
            snapshot_evidence: MySqlSnapshotEvidence {
                endpoint_identity: endpoint.into(),
                database_identity: "app".into(),
                server_uuid: format!("uuid-{endpoint}"),
                server_version: "8.4.0".into(),
                authenticated_account: "reader@%".into(),
                lifecycle_id: format!("life-{endpoint}"),
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
                gtid_executed_observation: String::new(),
                catalog_fingerprint: fingerprint,
            },
            catalog,
            blockers: vec![MySqlCatalogBlocker {
                object_id: mysql_catalog_id("catalog_visibility", "app", "app", ""),
                object_kind: "catalog_visibility".into(),
                reason: "reader visibility requires authoritative capture".into(),
            }],
        }
    }

    fn mysql_visibility(snapshot: &MySqlCatalogSnapshot) -> MySqlMetadataVisibilityCapture {
        let reader = MySqlAccountIdentity {
            user: "reader".into(),
            host: "%".into(),
        };
        let administrator = MySqlAccountIdentity {
            user: "metadata_admin".into(),
            host: "%".into(),
        };
        let mut records = ["EVENT", "SELECT", "SHOW VIEW", "TRIGGER"]
            .into_iter()
            .map(|privilege| MySqlGrantRecord::StaticGlobal {
                account: administrator.clone(),
                privilege: privilege.into(),
            })
            .chain(std::iter::once(MySqlGrantRecord::DynamicGlobal {
                account: administrator.clone(),
                privilege: "SHOW_ROUTINE".into(),
                grantable: false,
            }))
            .collect::<Vec<_>>();
        records.sort();
        let mut accounts = vec![reader.clone(), administrator.clone()];
        accounts.sort();
        let inventory = MySqlGrantInventory {
            partial_revokes_enabled: true,
            grant_table_columns: vec![MySqlGrantTableColumn {
                table: "user".into(),
                column: "Host".into(),
                data_type: "char".into(),
            }],
            accounts,
            records,
            unknown_privilege_classes: Vec::new(),
        };
        let authoritative_catalog = snapshot.catalog.clone();
        let authoritative_fingerprint = mysql_catalog_fingerprint(&authoritative_catalog).unwrap();
        MySqlMetadataVisibilityCapture {
            authoritative_catalog,
            authoritative_blockers: Vec::new(),
            evidence: MySqlMetadataVisibilityEvidence {
                schema_version: MYSQL_METADATA_VISIBILITY_SCHEMA_VERSION,
                endpoint_identity: snapshot.endpoint_identity.clone(),
                database_identity: snapshot.database_identity.clone(),
                server_uuid: snapshot.snapshot_evidence.server_uuid.clone(),
                catalog_reader_tls_binding: snapshot.tls_binding.clone(),
                metadata_administrator_tls_binding: "metadata-admin-tls".into(),
                catalog_reader_account: reader.clone(),
                metadata_administrator_account: administrator.clone(),
                active_administrator_roles: Vec::new(),
                effective_administrator_privileges: vec![
                    "EVENT".into(),
                    "SELECT".into(),
                    "SHOW VIEW".into(),
                    "SHOW_ROUTINE".into(),
                    "TRIGGER".into(),
                ],
                operational_exclusions: vec![
                    MySqlOperationalAccountExclusion {
                        purpose: MySqlOperationalAccountPurpose::CatalogReader,
                        account: reader,
                    },
                    MySqlOperationalAccountExclusion {
                        purpose: MySqlOperationalAccountPurpose::MetadataAdministrator,
                        account: administrator,
                    },
                ],
                catalog_reader_fingerprint: snapshot.snapshot_evidence.catalog_fingerprint.clone(),
                authoritative_catalog_fingerprint: authoritative_fingerprint,
                grant_inventory_digest: inventory.canonical_hash().unwrap(),
                grant_inventory: inventory,
            },
        }
    }

    fn postgres_snapshot(endpoint: &str, with_table: bool) -> CatalogSnapshot {
        let mut objects = Vec::new();
        if with_table {
            objects.extend([
                CatalogObject {
                    id: "relation:1".into(),
                    kind: CatalogObjectKind::Table,
                    name: identifier("items"),
                    definition: Vec::new(),
                    attributes: BTreeMap::from([
                        ("relkind".into(), serde_json::json!("r")),
                        ("persistence".into(), serde_json::json!("p")),
                    ]),
                },
                CatalogObject {
                    id: "column:1:1".into(),
                    kind: CatalogObjectKind::Column,
                    name: identifier("id"),
                    definition: b"int8".to_vec(),
                    attributes: BTreeMap::from([
                        ("table_oid".into(), serde_json::json!("relation:1")),
                        ("ordinal".into(), serde_json::json!(1)),
                        ("nullable".into(), serde_json::json!(false)),
                        ("default".into(), serde_json::Value::Null),
                        ("generated_expression".into(), serde_json::Value::Null),
                        ("sequence_default_oid".into(), serde_json::Value::Null),
                        ("identity".into(), serde_json::json!("")),
                        ("type_schema".into(), serde_json::json!("pg_catalog")),
                        ("type_name".into(), serde_json::json!("int8")),
                        ("numeric_precision".into(), serde_json::Value::Null),
                        ("numeric_scale".into(), serde_json::Value::Null),
                        ("datetime_precision".into(), serde_json::Value::Null),
                        ("collation_schema".into(), serde_json::Value::Null),
                        ("collation".into(), serde_json::Value::Null),
                    ]),
                },
                CatalogObject {
                    id: "constraint:1".into(),
                    kind: CatalogObjectKind::PrimaryKey,
                    name: identifier("items_pkey"),
                    definition: Vec::new(),
                    attributes: BTreeMap::from([
                        ("table_oid".into(), serde_json::json!("relation:1")),
                        ("validated".into(), serde_json::json!(true)),
                        ("columns".into(), serde_json::json!(["id"])),
                    ]),
                },
            ]);
        }
        CatalogSnapshot {
            endpoint_identity: endpoint.into(),
            server_version: "17.0".into(),
            server_version_num: 170_000,
            catalog: VendorCatalog {
                format_version: CATALOG_FORMAT_VERSION,
                dialect: "postgresql".into(),
                server_version: "17.0".into(),
                database: identifier("app"),
                namespaces: vec![CatalogNamespace {
                    id: "namespace-public".into(),
                    name: identifier("public"),
                    owner: Some("owner".into()),
                    charset: Some("UTF8".into()),
                    collation: None,
                    objects,
                }],
                dependencies: Vec::new(),
                vendor_metadata: BTreeMap::new(),
            },
            unsupported: UnsupportedObjectReport::default(),
            tls_binding: "hostname_verified;roots=platform;client=none".into(),
        }
    }

    pub(crate) fn mysql_to_postgres_reviewed_plan() -> ReviewedPlan {
        let source = mysql_snapshot("mysql://source/app", true);
        let source_visibility = mysql_visibility(&source);
        let target = postgres_snapshot("postgres://target/app", false);
        let mapping = CrossDialectMapping {
            schema_version: CROSS_DIALECT_MAPPING_SCHEMA_VERSION,
            source_dialect: ConversionDialect::MySql,
            target_dialect: ConversionDialect::PostgreSql,
            target_defaults: CrossDialectTargetDefaults::PostgreSql {
                text_collation: QualifiedIdentifier {
                    namespace: identifier("pg_catalog"),
                    name: identifier("C"),
                },
            },
            tables: vec![CrossDialectTableMapping {
                source: table("app", "items"),
                target: table("public", "items"),
            }],
        };
        build_mysql_to_postgres_plan(&source, &source_visibility, &target, &mapping).unwrap()
    }

    pub(crate) fn postgres_to_mysql_reviewed_plan() -> ReviewedPlan {
        let source = postgres_snapshot("postgres://source/app?user=reader", true);
        let target = mysql_snapshot("mysql://target/app", false);
        let target_visibility = mysql_visibility(&target);
        let mapping = mapping(vec![CrossDialectTableMapping {
            source: table("public", "items"),
            target: table("app", "items"),
        }]);
        build_postgres_to_mysql_plan(&source, &target, &target_visibility, &mapping).unwrap()
    }

    #[test]
    fn mapping_requires_stable_bijective_table_order() {
        let first = CrossDialectTableMapping {
            source: table("public", "a"),
            target: table("app", "a"),
        };
        let second = CrossDialectTableMapping {
            source: table("public", "b"),
            target: table("app", "b"),
        };
        assert!(mapping(vec![first.clone(), second.clone()])
            .validate()
            .is_ok());

        assert!(matches!(
            mapping(vec![second.clone(), first.clone()]).validate(),
            Err(CrossDialectPlanError::InvalidMapping)
        ));
        let mut duplicate_target = second;
        duplicate_target.target = first.target.clone();
        assert!(matches!(
            mapping(vec![first, duplicate_target]).validate(),
            Err(CrossDialectPlanError::InvalidMapping)
        ));
        let mut stale = mapping(vec![CrossDialectTableMapping {
            source: table("public", "a"),
            target: table("app", "a"),
        }]);
        stale.schema_version -= 1;
        assert!(matches!(
            stale.validate(),
            Err(CrossDialectPlanError::UnsupportedMappingVersion { .. })
        ));
    }

    #[test]
    fn operations_bind_create_copy_verify_and_terminal_dependencies() {
        let policy = table_policy();
        policy.validate().unwrap();
        let operations = cross_dialect_operations(std::slice::from_ref(&policy)).unwrap();
        assert_eq!(operations.len(), 4);
        let [create, copy, verify, verify_schema] = operations.as_slice() else {
            panic!("cross-dialect operation graph changed")
        };
        assert_eq!(create.kind, OperationKind::CreateTable);
        assert_eq!(create.table.as_ref(), Some(&policy.target_table));
        assert!(create.dependencies.is_empty());
        assert_eq!(copy.kind, OperationKind::CopyTable);
        assert_eq!(copy.table.as_ref(), Some(&policy.source_table));
        assert_eq!(copy.dependencies, [create.id.clone()]);
        assert_eq!(verify.kind, OperationKind::VerifyTable);
        assert_eq!(verify.dependencies, [copy.id.clone()]);
        assert_eq!(verify_schema.kind, OperationKind::VerifySchema);
        assert_eq!(
            verify_schema.dependencies,
            vec![create.id.clone(), copy.id.clone(), verify.id.clone()]
        );
        for (operation, parameter) in [
            (create, "cross_dialect_target_table_policy"),
            (copy, "table_conversion_policy"),
            (verify, "cross_dialect_verification_policy"),
        ] {
            let embedded: TableConversionPolicy =
                serde_json::from_value(operation.parameters.get(parameter).unwrap().clone())
                    .unwrap();
            assert_eq!(embedded, policy);
        }
    }

    #[test]
    fn every_source_table_must_be_mapped_once() {
        let catalog = VendorCatalog {
            format_version: 1,
            dialect: "postgresql".into(),
            server_version: "17".into(),
            database: identifier("source"),
            namespaces: vec![CatalogNamespace {
                id: "namespace-public".into(),
                name: identifier("public"),
                owner: None,
                charset: Some("UTF8".into()),
                collation: None,
                objects: ["a", "b"]
                    .into_iter()
                    .map(|name| CatalogObject {
                        id: format!("table-{name}"),
                        kind: CatalogObjectKind::Table,
                        name: identifier(name),
                        definition: Vec::new(),
                        attributes: BTreeMap::new(),
                    })
                    .collect(),
            }],
            dependencies: Vec::new(),
            vendor_metadata: BTreeMap::new(),
        };
        assert!(matches!(
            require_complete_source_table_mapping(
                &catalog,
                &[CrossDialectTableMapping {
                    source: table("public", "a"),
                    target: table("app", "a"),
                }]
            ),
            Err(CrossDialectPlanError::IncompleteMapping)
        ));
    }

    #[test]
    fn postgres_to_mysql_builder_binds_authoritative_target_and_typed_graph() {
        let source = postgres_snapshot("postgres://source/app", true);
        let target = mysql_snapshot("mysql://target/app", false);
        let target_visibility = mysql_visibility(&target);
        let mapping = mapping(vec![CrossDialectTableMapping {
            source: table("public", "items"),
            target: table("app", "items"),
        }]);

        let reviewed =
            build_postgres_to_mysql_plan(&source, &target, &target_visibility, &mapping).unwrap();
        assert!(!reviewed.plan.unsupported_objects.blocks_execution());
        assert_eq!(reviewed.plan.operations.len(), 4);
        assert_eq!(reviewed.plan.consistency_mode, "write-fence");
        assert!(matches!(
            reviewed.plan.conversion_policy.mode,
            super::super::conversion::MigrationConversionMode::CrossDialect {
                source_dialect: ConversionDialect::PostgreSql,
                target_dialect: ConversionDialect::MySql,
                ..
            }
        ));
        assert_eq!(
            reviewed.plan.target_catalog.as_assessed(),
            Some(&target_visibility.authoritative_catalog)
        );
    }

    #[test]
    fn mysql_to_postgres_builder_binds_authoritative_source_and_typed_graph() {
        let source = mysql_snapshot("mysql://source/app", true);
        let source_visibility = mysql_visibility(&source);
        let target = postgres_snapshot("postgres://target/app", false);
        let mapping = CrossDialectMapping {
            schema_version: CROSS_DIALECT_MAPPING_SCHEMA_VERSION,
            source_dialect: ConversionDialect::MySql,
            target_dialect: ConversionDialect::PostgreSql,
            target_defaults: CrossDialectTargetDefaults::PostgreSql {
                text_collation: QualifiedIdentifier {
                    namespace: identifier("pg_catalog"),
                    name: identifier("C"),
                },
            },
            tables: vec![CrossDialectTableMapping {
                source: table("app", "items"),
                target: table("public", "items"),
            }],
        };

        let reviewed =
            build_mysql_to_postgres_plan(&source, &source_visibility, &target, &mapping).unwrap();
        assert!(!reviewed.plan.unsupported_objects.blocks_execution());
        assert_eq!(reviewed.plan.operations.len(), 4);
        assert!(matches!(
            reviewed.plan.conversion_policy.mode,
            super::super::conversion::MigrationConversionMode::CrossDialect {
                source_dialect: ConversionDialect::MySql,
                target_dialect: ConversionDialect::PostgreSql,
                ..
            }
        ));
        assert_eq!(
            reviewed.plan.source_catalog.as_ref(),
            Some(&source_visibility.authoritative_catalog)
        );
        assert!(reviewed.plan.mysql_target_snapshot_evidence.is_none());
    }
}
