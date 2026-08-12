//! Versioned cross-dialect row conversion over canonical database values.
//!
//! A conversion policy is reviewed plan data. Adapters derive each typed rule
//! from their exact source and target catalogs. Runtime never infers a rule
//! from a vendor type string and never coerces an unapproved value.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::canonical::canonicalize_json;
use super::model::{ColumnMeta, DbValue, Identifier, QualifiedTable};

pub const ROW_TYPE_CONVERSION_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversionDialect {
    PostgreSql,
    MySql,
}

impl ConversionDialect {
    pub const fn catalog_name(self) -> &'static str {
        match self {
            Self::PostgreSql => "postgresql",
            Self::MySql => "mysql",
        }
    }
}

/// The reviewed conversion contract for one migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationConversionPolicy {
    pub schema_version: u16,
    pub mode: MigrationConversionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum MigrationConversionMode {
    AssessmentSourceOnly {
        dialect: ConversionDialect,
    },
    SameDialectExact {
        dialect: ConversionDialect,
    },
    CrossDialect {
        source_dialect: ConversionDialect,
        target_dialect: ConversionDialect,
        tables: Vec<TableConversionPolicy>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConversionPolicy {
    pub source_table: QualifiedTable,
    pub target_table: QualifiedTable,
    pub row_policy: RowConversionPolicy,
}

impl MigrationConversionPolicy {
    pub const fn assessment_source_only(dialect: ConversionDialect) -> Self {
        Self {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            mode: MigrationConversionMode::AssessmentSourceOnly { dialect },
        }
    }

    pub const fn same_dialect_exact(dialect: ConversionDialect) -> Self {
        Self {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            mode: MigrationConversionMode::SameDialectExact { dialect },
        }
    }

    pub fn cross_dialect(
        source_dialect: ConversionDialect,
        target_dialect: ConversionDialect,
        tables: Vec<TableConversionPolicy>,
    ) -> Result<Self, RowConversionError> {
        let policy = Self {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            mode: MigrationConversionMode::CrossDialect {
                source_dialect,
                target_dialect,
                tables,
            },
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn source_dialect(&self) -> ConversionDialect {
        match &self.mode {
            MigrationConversionMode::AssessmentSourceOnly { dialect }
            | MigrationConversionMode::SameDialectExact { dialect } => *dialect,
            MigrationConversionMode::CrossDialect { source_dialect, .. } => *source_dialect,
        }
    }

    pub fn target_dialect(&self) -> Option<ConversionDialect> {
        match &self.mode {
            MigrationConversionMode::AssessmentSourceOnly { .. } => None,
            MigrationConversionMode::SameDialectExact { dialect } => Some(*dialect),
            MigrationConversionMode::CrossDialect { target_dialect, .. } => Some(*target_dialect),
        }
    }

    pub fn has_approved_transformations(&self) -> bool {
        matches!(self.mode, MigrationConversionMode::CrossDialect { .. })
    }

    pub fn validate(&self) -> Result<(), RowConversionError> {
        if self.schema_version != ROW_TYPE_CONVERSION_SCHEMA_VERSION {
            return Err(RowConversionError::UnsupportedVersion {
                found: self.schema_version,
            });
        }
        let MigrationConversionMode::CrossDialect {
            source_dialect,
            target_dialect,
            tables,
        } = &self.mode
        else {
            return Ok(());
        };
        if source_dialect == target_dialect {
            return Err(RowConversionError::SameDialectPolicy);
        }
        let mut source_tables = BTreeSet::new();
        let mut target_tables = BTreeSet::new();
        let mut previous_source_table = None;
        for table in tables {
            if !source_tables.insert(&table.source_table) {
                return Err(RowConversionError::DuplicateSourceTable {
                    table: table.source_table.clone(),
                });
            }
            if previous_source_table.is_some_and(|previous| previous > &table.source_table) {
                return Err(RowConversionError::InvalidTableOrder);
            }
            previous_source_table = Some(&table.source_table);
            if !target_tables.insert(&table.target_table) {
                return Err(RowConversionError::DuplicateTargetTable {
                    table: table.target_table.clone(),
                });
            }
            table.row_policy.validate()?;
            if table.row_policy.source_dialect != *source_dialect
                || table.row_policy.target_dialect != *target_dialect
            {
                return Err(RowConversionError::DialectMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case", deny_unknown_fields)]
pub enum ValueConversionRule {
    BooleanExact,
    BooleanToSigned,
    SignedZeroOneToBoolean,
    SignedInteger {
        minimum: i64,
        maximum: i64,
    },
    UnsignedInteger {
        maximum: u64,
    },
    SignedToUnsigned {
        maximum: u64,
    },
    UnsignedToSigned {
        maximum: i64,
    },
    DecimalExact {
        precision: u32,
        scale: i32,
    },
    Float32ExactBits,
    Float64ExactBits,
    TextUtf8Exact,
    BytesExact,
    JsonCanonicalV1,
    DateRange {
        minimum_year: i32,
        maximum_year: i32,
    },
    TimeRange {
        minimum_nanos: i64,
        maximum_nanos: i64,
    },
    TimestampExact {
        precision: u8,
        offset: TimestampOffsetPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampOffsetPolicy {
    RequireAbsent,
    RequirePresent,
    Preserve,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConversion {
    pub source: ColumnMeta,
    pub target: ColumnMeta,
    pub rule: ValueConversionRule,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowConversionPolicy {
    pub schema_version: u16,
    pub source_dialect: ConversionDialect,
    pub target_dialect: ConversionDialect,
    pub columns: Vec<ColumnConversion>,
}

impl RowConversionPolicy {
    pub fn validate(&self) -> Result<(), RowConversionError> {
        if self.schema_version != ROW_TYPE_CONVERSION_SCHEMA_VERSION {
            return Err(RowConversionError::UnsupportedVersion {
                found: self.schema_version,
            });
        }
        if self.source_dialect == self.target_dialect {
            return Err(RowConversionError::SameDialectPolicy);
        }
        if self.columns.is_empty() {
            return Err(RowConversionError::EmptyPolicy);
        }
        let mut source_names = BTreeSet::new();
        let mut target_names = BTreeSet::new();
        let mut previous_source_ordinal = 0;
        let mut previous_target_ordinal = 0;
        for column in &self.columns {
            if column.source.ordinal <= previous_source_ordinal
                || column.target.ordinal <= previous_target_ordinal
            {
                return Err(RowConversionError::InvalidColumnOrder);
            }
            previous_source_ordinal = column.source.ordinal;
            previous_target_ordinal = column.target.ordinal;
            if !source_names.insert(&column.source.name) {
                return Err(RowConversionError::DuplicateSourceColumn {
                    column: column.source.name.clone(),
                });
            }
            if !target_names.insert(&column.target.name) {
                return Err(RowConversionError::DuplicateTargetColumn {
                    column: column.target.name.clone(),
                });
            }
            if column.source.nullable && !column.target.nullable {
                return Err(RowConversionError::NullableSourceToRequiredTarget {
                    column: column.source.name.clone(),
                });
            }
            validate_rule_metadata(column)?;
        }
        Ok(())
    }
}

pub trait RowTypeConverter {
    fn policy(&self) -> &RowConversionPolicy;

    fn convert_row(
        &self,
        source_columns: &[ColumnMeta],
        source_values: &[DbValue],
    ) -> Result<ConvertedRow, RowConversionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewedRowTypeConverter {
    policy: RowConversionPolicy,
}

impl ReviewedRowTypeConverter {
    pub fn new(policy: RowConversionPolicy) -> Result<Self, RowConversionError> {
        policy.validate()?;
        Ok(Self { policy })
    }
}

impl RowTypeConverter for ReviewedRowTypeConverter {
    fn policy(&self) -> &RowConversionPolicy {
        &self.policy
    }

    fn convert_row(
        &self,
        source_columns: &[ColumnMeta],
        source_values: &[DbValue],
    ) -> Result<ConvertedRow, RowConversionError> {
        if !source_columns
            .iter()
            .eq(self.policy.columns.iter().map(|column| &column.source))
        {
            return Err(RowConversionError::SourceMetadataMismatch);
        }
        if source_values.len() != self.policy.columns.len() {
            return Err(RowConversionError::ColumnCount {
                expected: self.policy.columns.len(),
                actual: source_values.len(),
            });
        }
        let mut values = Vec::with_capacity(source_values.len());
        let mut transformations = Vec::with_capacity(source_values.len());
        for (column, value) in self.policy.columns.iter().zip(source_values) {
            let converted = convert_value(column, value)?;
            values.push(converted);
            transformations.push(AppliedTransformation {
                source_column: column.source.name.clone(),
                target_column: column.target.name.clone(),
                rule: column.rule.clone(),
            });
        }
        Ok(ConvertedRow {
            target_columns: self
                .policy
                .columns
                .iter()
                .map(|column| column.target.clone())
                .collect(),
            values,
            transformations,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertedRow {
    pub target_columns: Vec<ColumnMeta>,
    pub values: Vec<DbValue>,
    pub transformations: Vec<AppliedTransformation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedTransformation {
    pub source_column: Identifier,
    pub target_column: Identifier,
    pub rule: ValueConversionRule,
}

fn validate_rule_metadata(column: &ColumnConversion) -> Result<(), RowConversionError> {
    match &column.rule {
        ValueConversionRule::SignedInteger { minimum, maximum } if minimum > maximum => {
            Err(RowConversionError::InvalidRule)
        }
        ValueConversionRule::UnsignedToSigned { maximum } if *maximum < 0 => {
            Err(RowConversionError::InvalidRule)
        }
        ValueConversionRule::DecimalExact { precision, scale }
            if *precision == 0
                || *scale < 0
                || u32::try_from(*scale)
                    .ok()
                    .is_none_or(|scale| scale > *precision)
                || column.source.precision != Some(*precision)
                || column.target.precision != Some(*precision)
                || column.source.scale != Some(*scale)
                || column.target.scale != Some(*scale) =>
        {
            Err(RowConversionError::InvalidRule)
        }
        ValueConversionRule::DateRange {
            minimum_year,
            maximum_year,
        } if minimum_year > maximum_year => Err(RowConversionError::InvalidRule),
        ValueConversionRule::TimeRange {
            minimum_nanos,
            maximum_nanos,
        } if minimum_nanos > maximum_nanos => Err(RowConversionError::InvalidRule),
        ValueConversionRule::TimestampExact { precision, .. }
            if *precision > 6
                || column.source.precision != Some(u32::from(*precision))
                || column.target.precision != Some(u32::from(*precision)) =>
        {
            Err(RowConversionError::InvalidRule)
        }
        _ => Ok(()),
    }
}

fn convert_value(
    column: &ColumnConversion,
    value: &DbValue,
) -> Result<DbValue, RowConversionError> {
    if matches!(value, DbValue::Null) {
        if !column.source.nullable || !column.target.nullable {
            return Err(RowConversionError::UnexpectedNull {
                column: column.source.name.clone(),
            });
        }
        return Ok(DbValue::Null);
    }
    let failure = || RowConversionError::ValueOutsidePolicy {
        column: column.source.name.clone(),
    };
    let converted = match (&column.rule, value) {
        (ValueConversionRule::BooleanExact, DbValue::Bool(value)) => DbValue::Bool(*value),
        (ValueConversionRule::BooleanToSigned, DbValue::Bool(value)) => {
            DbValue::Signed(i128::from(*value))
        }
        (ValueConversionRule::SignedZeroOneToBoolean, DbValue::Signed(value))
            if matches!(value, 0 | 1) =>
        {
            DbValue::Bool(*value == 1)
        }
        (ValueConversionRule::SignedInteger { minimum, maximum }, DbValue::Signed(value))
            if *value >= i128::from(*minimum) && *value <= i128::from(*maximum) =>
        {
            DbValue::Signed(*value)
        }
        (ValueConversionRule::UnsignedInteger { maximum }, DbValue::Unsigned(value))
            if *value <= u128::from(*maximum) =>
        {
            DbValue::Unsigned(*value)
        }
        (ValueConversionRule::SignedToUnsigned { maximum }, DbValue::Signed(value))
            if *value >= 0
                && u128::try_from(*value).is_ok_and(|value| value <= u128::from(*maximum)) =>
        {
            DbValue::Unsigned(u128::try_from(*value).map_err(|_| failure())?)
        }
        (ValueConversionRule::UnsignedToSigned { maximum }, DbValue::Unsigned(value))
            if i128::try_from(*value).is_ok_and(|value| value <= i128::from(*maximum)) =>
        {
            DbValue::Signed(i128::try_from(*value).map_err(|_| failure())?)
        }
        (
            ValueConversionRule::DecimalExact { precision, scale },
            DbValue::Decimal {
                coefficient,
                scale: value_scale,
            },
        ) if value_scale == scale && decimal_fits(coefficient, *precision) => DbValue::Decimal {
            coefficient: coefficient.clone(),
            scale: *value_scale,
        },
        (ValueConversionRule::Float32ExactBits, DbValue::Float32(bits)) => DbValue::Float32(*bits),
        (ValueConversionRule::Float64ExactBits, DbValue::Float64(bits)) => DbValue::Float64(*bits),
        (ValueConversionRule::TextUtf8Exact, DbValue::Text(value)) => DbValue::Text(value.clone()),
        (ValueConversionRule::BytesExact, DbValue::Bytes(value)) => DbValue::Bytes(value.clone()),
        (ValueConversionRule::JsonCanonicalV1, DbValue::Json(value)) => {
            DbValue::Json(canonicalize_json(value)?)
        }
        (
            ValueConversionRule::DateRange {
                minimum_year,
                maximum_year,
            },
            DbValue::Date { year, month, day },
        ) if year >= minimum_year
            && year <= maximum_year
            && valid_gregorian_date(*year, *month, *day) =>
        {
            DbValue::Date {
                year: *year,
                month: *month,
                day: *day,
            }
        }
        (
            ValueConversionRule::TimeRange {
                minimum_nanos,
                maximum_nanos,
            },
            DbValue::Time { nanos },
        ) if *nanos >= i128::from(*minimum_nanos) && *nanos <= i128::from(*maximum_nanos) => {
            DbValue::Time { nanos: *nanos }
        }
        (
            ValueConversionRule::TimestampExact { precision, offset },
            DbValue::Timestamp {
                local,
                offset_minutes,
                precision: value_precision,
            },
        ) if value_precision == precision
            && !local.is_empty()
            && offset_minutes.is_none_or(|offset| (-14 * 60..=14 * 60).contains(&offset))
            && match offset {
                TimestampOffsetPolicy::RequireAbsent => offset_minutes.is_none(),
                TimestampOffsetPolicy::RequirePresent => offset_minutes.is_some(),
                TimestampOffsetPolicy::Preserve => true,
            } =>
        {
            DbValue::Timestamp {
                local: local.clone(),
                offset_minutes: *offset_minutes,
                precision: *value_precision,
            }
        }
        _ => return Err(failure()),
    };
    Ok(converted)
}

fn decimal_fits(coefficient: &[u8], precision: u32) -> bool {
    let digits = coefficient.strip_prefix(b"-").unwrap_or(coefficient);
    !digits.is_empty()
        && digits.iter().all(u8::is_ascii_digit)
        && (digits == b"0" || digits.first() != Some(&b'0'))
        && !(coefficient.starts_with(b"-") && digits == b"0")
        && u32::try_from(digits.len()).is_ok_and(|digits| digits <= precision)
}

fn valid_gregorian_date(year: i32, month: u8, day: u8) -> bool {
    let leap_year =
        year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    let maximum_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return false,
    };
    day != 0 && day <= maximum_day
}

#[derive(Debug, Error)]
pub enum RowConversionError {
    #[error("unsupported row-conversion schema version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("row-conversion policy must cross dialects")]
    SameDialectPolicy,
    #[error("row-conversion policy has no columns")]
    EmptyPolicy,
    #[error("row-conversion policy has invalid column order")]
    InvalidColumnOrder,
    #[error("migration conversion policy has invalid source table order")]
    InvalidTableOrder,
    #[error("migration conversion policy contains duplicate source table {table:?}")]
    DuplicateSourceTable { table: QualifiedTable },
    #[error("migration conversion policy contains duplicate target table {table:?}")]
    DuplicateTargetTable { table: QualifiedTable },
    #[error("table conversion policy dialects differ from the migration conversion policy")]
    DialectMismatch,
    #[error("row-conversion policy contains duplicate source column {column}")]
    DuplicateSourceColumn { column: Identifier },
    #[error("row-conversion policy contains duplicate target column {column}")]
    DuplicateTargetColumn { column: Identifier },
    #[error("nullable source column {column} maps to a required target column")]
    NullableSourceToRequiredTarget { column: Identifier },
    #[error("row-conversion policy has an invalid typed rule")]
    InvalidRule,
    #[error("source column metadata differs from the reviewed conversion policy")]
    SourceMetadataMismatch,
    #[error("source row has {actual} values but policy has {expected} columns")]
    ColumnCount { expected: usize, actual: usize },
    #[error("source column {column} contains an unexpected NULL")]
    UnexpectedNull { column: Identifier },
    #[error("source column {column} contains a value outside its reviewed conversion policy")]
    ValueOutsidePolicy { column: Identifier },
    #[error("canonical JSON conversion failed")]
    CanonicalJson(#[from] super::canonical::CanonicalJsonError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, ordinal: u32, nullable: bool) -> ColumnMeta {
        ColumnMeta {
            name: Identifier::new(name).unwrap(),
            ordinal,
            vendor_type: name.into(),
            nullable,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        }
    }

    fn policy(rules: Vec<ValueConversionRule>) -> RowConversionPolicy {
        RowConversionPolicy {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            source_dialect: ConversionDialect::MySql,
            target_dialect: ConversionDialect::PostgreSql,
            columns: rules
                .into_iter()
                .enumerate()
                .map(|(index, rule)| ColumnConversion {
                    source: column(
                        &format!("source_{index}"),
                        u32::try_from(index + 1).unwrap(),
                        false,
                    ),
                    target: column(
                        &format!("target_{index}"),
                        u32::try_from(index + 1).unwrap(),
                        false,
                    ),
                    rule,
                })
                .collect(),
        }
    }

    fn convert_one(rule: ValueConversionRule, value: DbValue) -> DbValue {
        let policy = policy(vec![rule]);
        let source_columns = vec![policy.columns[0].source.clone()];
        ReviewedRowTypeConverter::new(policy)
            .unwrap()
            .convert_row(&source_columns, &[value])
            .unwrap()
            .values
            .into_iter()
            .next()
            .unwrap()
    }

    fn table(namespace: &str, name: &str) -> QualifiedTable {
        QualifiedTable {
            namespace: Identifier::new(namespace).unwrap(),
            name: Identifier::new(name).unwrap(),
        }
    }

    #[test]
    fn migration_policy_is_typed_ordered_and_dialect_bound() {
        let first = TableConversionPolicy {
            source_table: table("source", "a"),
            target_table: table("target", "a"),
            row_policy: policy(vec![ValueConversionRule::SignedInteger {
                minimum: i64::MIN,
                maximum: i64::MAX,
            }]),
        };
        let second = TableConversionPolicy {
            source_table: table("source", "b"),
            target_table: table("target", "b"),
            row_policy: policy(vec![ValueConversionRule::TextUtf8Exact]),
        };
        let reviewed = MigrationConversionPolicy::cross_dialect(
            ConversionDialect::MySql,
            ConversionDialect::PostgreSql,
            vec![first.clone(), second.clone()],
        )
        .unwrap();
        assert_eq!(reviewed.source_dialect(), ConversionDialect::MySql);
        assert_eq!(
            reviewed.target_dialect(),
            Some(ConversionDialect::PostgreSql)
        );
        assert!(reviewed.has_approved_transformations());
        assert_eq!(
            serde_json::from_slice::<MigrationConversionPolicy>(
                &serde_json::to_vec(&reviewed).unwrap()
            )
            .unwrap(),
            reviewed
        );

        assert!(matches!(
            MigrationConversionPolicy::cross_dialect(
                ConversionDialect::MySql,
                ConversionDialect::PostgreSql,
                vec![second.clone(), first.clone()],
            ),
            Err(RowConversionError::InvalidTableOrder)
        ));
        let mut duplicate_target = second;
        duplicate_target.target_table = first.target_table.clone();
        assert!(matches!(
            MigrationConversionPolicy::cross_dialect(
                ConversionDialect::MySql,
                ConversionDialect::PostgreSql,
                vec![first, duplicate_target],
            ),
            Err(RowConversionError::DuplicateTargetTable { .. })
        ));
        assert!(matches!(
            MigrationConversionPolicy::cross_dialect(
                ConversionDialect::MySql,
                ConversionDialect::MySql,
                Vec::new(),
            ),
            Err(RowConversionError::SameDialectPolicy)
        ));
    }

    #[test]
    fn integer_and_boolean_boundaries_are_checked_without_coercion() {
        let policy = policy(vec![
            ValueConversionRule::SignedZeroOneToBoolean,
            ValueConversionRule::SignedToUnsigned { maximum: 255 },
            ValueConversionRule::UnsignedToSigned {
                maximum: i64::from(i16::MAX),
            },
        ]);
        let source_columns = policy
            .columns
            .iter()
            .map(|column| column.source.clone())
            .collect::<Vec<_>>();
        let converter = ReviewedRowTypeConverter::new(policy).unwrap();
        let converted = converter
            .convert_row(
                &source_columns,
                &[
                    DbValue::Signed(1),
                    DbValue::Signed(255),
                    DbValue::Unsigned(32767),
                ],
            )
            .unwrap();
        assert_eq!(
            converted.values,
            vec![
                DbValue::Bool(true),
                DbValue::Unsigned(255),
                DbValue::Signed(32767)
            ]
        );
        assert_eq!(converted.transformations.len(), 3);
        assert!(converter
            .convert_row(
                &source_columns,
                &[
                    DbValue::Signed(2),
                    DbValue::Signed(255),
                    DbValue::Unsigned(32767)
                ],
            )
            .is_err());
        assert!(converter
            .convert_row(
                &source_columns,
                &[
                    DbValue::Signed(1),
                    DbValue::Signed(-1),
                    DbValue::Unsigned(32767)
                ],
            )
            .is_err());
        assert!(converter
            .convert_row(
                &source_columns,
                &[
                    DbValue::Signed(1),
                    DbValue::Signed(255),
                    DbValue::Unsigned(32768)
                ],
            )
            .is_err());
    }

    #[test]
    fn json_is_canonicalized_and_duplicate_keys_fail() {
        let policy = policy(vec![ValueConversionRule::JsonCanonicalV1]);
        let source_columns = vec![policy.columns[0].source.clone()];
        let converter = ReviewedRowTypeConverter::new(policy).unwrap();
        let converted = converter
            .convert_row(
                &source_columns,
                &[DbValue::Json(br#"{"z":1.00,"a":1e0}"#.to_vec())],
            )
            .unwrap();
        assert_eq!(
            converted.values,
            vec![DbValue::Json(br#"{"a":1,"z":1}"#.to_vec())]
        );
        assert!(converter
            .convert_row(
                &source_columns,
                &[DbValue::Json(br#"{"a":1,"a":2}"#.to_vec())],
            )
            .is_err());
    }

    #[test]
    fn every_metadata_free_rule_has_exact_expected_value_vectors() {
        let vectors = [
            (
                ValueConversionRule::BooleanExact,
                DbValue::Bool(false),
                DbValue::Bool(false),
            ),
            (
                ValueConversionRule::BooleanToSigned,
                DbValue::Bool(true),
                DbValue::Signed(1),
            ),
            (
                ValueConversionRule::SignedZeroOneToBoolean,
                DbValue::Signed(0),
                DbValue::Bool(false),
            ),
            (
                ValueConversionRule::SignedInteger {
                    minimum: i64::MIN,
                    maximum: i64::MAX,
                },
                DbValue::Signed(i128::from(i64::MIN)),
                DbValue::Signed(i128::from(i64::MIN)),
            ),
            (
                ValueConversionRule::UnsignedInteger { maximum: u64::MAX },
                DbValue::Unsigned(u128::from(u64::MAX)),
                DbValue::Unsigned(u128::from(u64::MAX)),
            ),
            (
                ValueConversionRule::SignedToUnsigned { maximum: 255 },
                DbValue::Signed(255),
                DbValue::Unsigned(255),
            ),
            (
                ValueConversionRule::UnsignedToSigned { maximum: i64::MAX },
                DbValue::Unsigned(i64::MAX as u128),
                DbValue::Signed(i128::from(i64::MAX)),
            ),
            (
                ValueConversionRule::Float32ExactBits,
                DbValue::Float32(0x7fc0_0123),
                DbValue::Float32(0x7fc0_0123),
            ),
            (
                ValueConversionRule::Float64ExactBits,
                DbValue::Float64((-0.0_f64).to_bits()),
                DbValue::Float64((-0.0_f64).to_bits()),
            ),
            (
                ValueConversionRule::TextUtf8Exact,
                DbValue::Text("Grüße 👩🏽‍💻".into()),
                DbValue::Text("Grüße 👩🏽‍💻".into()),
            ),
            (
                ValueConversionRule::BytesExact,
                DbValue::Bytes(vec![0, 0x7f, 0x80, 0xff]),
                DbValue::Bytes(vec![0, 0x7f, 0x80, 0xff]),
            ),
            (
                ValueConversionRule::JsonCanonicalV1,
                DbValue::Json(br#"{"b":1e0,"a":[true,null]}"#.to_vec()),
                DbValue::Json(br#"{"a":[true,null],"b":1}"#.to_vec()),
            ),
            (
                ValueConversionRule::DateRange {
                    minimum_year: 1000,
                    maximum_year: 9999,
                },
                DbValue::Date {
                    year: 2000,
                    month: 2,
                    day: 29,
                },
                DbValue::Date {
                    year: 2000,
                    month: 2,
                    day: 29,
                },
            ),
            (
                ValueConversionRule::TimeRange {
                    minimum_nanos: 0,
                    maximum_nanos: 86_399_999_999_999,
                },
                DbValue::Time {
                    nanos: 86_399_999_999_999,
                },
                DbValue::Time {
                    nanos: 86_399_999_999_999,
                },
            ),
        ];
        for (rule, input, expected) in vectors {
            assert_eq!(convert_one(rule, input), expected);
        }
    }

    #[test]
    fn decimal_timestamp_and_null_vectors_are_exact_and_recorded() {
        let mut reviewed = policy(vec![
            ValueConversionRule::DecimalExact {
                precision: 5,
                scale: 2,
            },
            ValueConversionRule::TimestampExact {
                precision: 6,
                offset: TimestampOffsetPolicy::RequirePresent,
            },
            ValueConversionRule::TextUtf8Exact,
        ]);
        reviewed.columns[0].source.precision = Some(5);
        reviewed.columns[0].source.scale = Some(2);
        reviewed.columns[0].target.precision = Some(5);
        reviewed.columns[0].target.scale = Some(2);
        reviewed.columns[1].source.precision = Some(6);
        reviewed.columns[1].target.precision = Some(6);
        reviewed.columns[2].source.nullable = true;
        reviewed.columns[2].target.nullable = true;
        let source_columns = reviewed
            .columns
            .iter()
            .map(|column| column.source.clone())
            .collect::<Vec<_>>();
        let converted = ReviewedRowTypeConverter::new(reviewed)
            .unwrap()
            .convert_row(
                &source_columns,
                &[
                    DbValue::Decimal {
                        coefficient: b"-12345".to_vec(),
                        scale: 2,
                    },
                    DbValue::Timestamp {
                        local: "2026-08-12 14:15:16.123456".into(),
                        offset_minutes: Some(120),
                        precision: 6,
                    },
                    DbValue::Null,
                ],
            )
            .unwrap();
        assert_eq!(converted.transformations.len(), 3);
        assert_eq!(converted.values[2], DbValue::Null);
    }

    #[test]
    fn malformed_canonical_values_and_invalid_rule_metadata_fail_closed() {
        for coefficient in [b"".as_slice(), b"-", b"-0", b"00", b"+1", b"1.0"] {
            let mut reviewed = policy(vec![ValueConversionRule::DecimalExact {
                precision: 3,
                scale: 1,
            }]);
            reviewed.columns[0].source.precision = Some(3);
            reviewed.columns[0].source.scale = Some(1);
            reviewed.columns[0].target.precision = Some(3);
            reviewed.columns[0].target.scale = Some(1);
            let columns = vec![reviewed.columns[0].source.clone()];
            let converter = ReviewedRowTypeConverter::new(reviewed).unwrap();
            assert!(converter
                .convert_row(
                    &columns,
                    &[DbValue::Decimal {
                        coefficient: coefficient.to_vec(),
                        scale: 1,
                    }],
                )
                .is_err());
        }

        for invalid_date in [
            DbValue::Date {
                year: 1900,
                month: 2,
                day: 29,
            },
            DbValue::Date {
                year: 2024,
                month: 13,
                day: 1,
            },
            DbValue::Date {
                year: 2024,
                month: 1,
                day: 0,
            },
        ] {
            let reviewed = policy(vec![ValueConversionRule::DateRange {
                minimum_year: 1000,
                maximum_year: 9999,
            }]);
            let columns = vec![reviewed.columns[0].source.clone()];
            assert!(ReviewedRowTypeConverter::new(reviewed)
                .unwrap()
                .convert_row(&columns, &[invalid_date])
                .is_err());
        }

        assert!(matches!(
            policy(vec![ValueConversionRule::UnsignedToSigned { maximum: -1 }]).validate(),
            Err(RowConversionError::InvalidRule)
        ));
    }

    #[test]
    fn metadata_nullability_decimal_and_timestamp_contracts_fail_closed() {
        let mut nullable = policy(vec![ValueConversionRule::TextUtf8Exact]);
        nullable.columns[0].source.nullable = true;
        assert!(matches!(
            nullable.validate(),
            Err(RowConversionError::NullableSourceToRequiredTarget { .. })
        ));

        let mut decimal = policy(vec![ValueConversionRule::DecimalExact {
            precision: 5,
            scale: 2,
        }]);
        decimal.columns[0].source.precision = Some(5);
        decimal.columns[0].source.scale = Some(2);
        decimal.columns[0].target.precision = Some(5);
        decimal.columns[0].target.scale = Some(2);
        let source_columns = vec![decimal.columns[0].source.clone()];
        let converter = ReviewedRowTypeConverter::new(decimal).unwrap();
        assert!(converter
            .convert_row(
                &source_columns,
                &[DbValue::Decimal {
                    coefficient: b"12345".to_vec(),
                    scale: 2,
                }],
            )
            .is_ok());
        assert!(converter
            .convert_row(
                &source_columns,
                &[DbValue::Decimal {
                    coefficient: b"123456".to_vec(),
                    scale: 2,
                }],
            )
            .is_err());

        let timestamp = policy(vec![ValueConversionRule::TimestampExact {
            precision: 6,
            offset: TimestampOffsetPolicy::RequirePresent,
        }]);
        assert!(matches!(
            timestamp.validate(),
            Err(RowConversionError::InvalidRule)
        ));
    }

    #[test]
    fn policy_version_direction_order_and_metadata_are_bound() {
        let reviewed_policy = policy(vec![ValueConversionRule::BytesExact]);
        let converter = ReviewedRowTypeConverter::new(reviewed_policy.clone()).unwrap();
        let mut wrong_columns = vec![reviewed_policy.columns[0].source.clone()];
        wrong_columns[0].vendor_type = "changed".into();
        assert!(matches!(
            converter.convert_row(&wrong_columns, &[DbValue::Bytes(vec![0, 255])]),
            Err(RowConversionError::SourceMetadataMismatch)
        ));
        let mut wrong = reviewed_policy;
        wrong.source_dialect = wrong.target_dialect;
        assert!(matches!(
            wrong.validate(),
            Err(RowConversionError::SameDialectPolicy)
        ));

        let mut duplicate = policy(vec![
            ValueConversionRule::BytesExact,
            ValueConversionRule::BytesExact,
        ]);
        duplicate.columns[1].target.name = duplicate.columns[0].target.name.clone();
        assert!(matches!(
            duplicate.validate(),
            Err(RowConversionError::DuplicateTargetColumn { .. })
        ));

        let mut ordinal_gap = policy(vec![
            ValueConversionRule::BytesExact,
            ValueConversionRule::BytesExact,
        ]);
        ordinal_gap.columns[1].source.ordinal = 4;
        ordinal_gap.columns[1].target.ordinal = 7;
        assert!(ordinal_gap.validate().is_ok());
        ordinal_gap.columns[1].source.ordinal = 1;
        assert!(matches!(
            ordinal_gap.validate(),
            Err(RowConversionError::InvalidColumnOrder)
        ));
    }
}
