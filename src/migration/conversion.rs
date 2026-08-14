//! Versioned cross-dialect row conversion over canonical database values.
//!
//! A conversion policy is reviewed plan data. Adapters derive each typed rule
//! from their exact source and target catalogs. Runtime never infers a rule
//! from a vendor type string and never coerces an unapproved value.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::canonical::canonicalize_json;
use super::model::{ColumnMeta, DbValue, Identifier, QualifiedTable, RowBatch};

pub const ROW_TYPE_CONVERSION_SCHEMA_VERSION: u16 = 3;
pub const MYSQL_UTF8MB4_VARCHAR_MAX_CHARACTERS: u32 = 16_383;

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
    pub target_contract: CrossDialectTargetTableContract,
    pub resumable_key: CrossDialectResumableKey,
    pub row_policy: RowConversionPolicy,
}

impl TableConversionPolicy {
    pub fn validate(&self) -> Result<(), RowConversionError> {
        self.row_policy.validate()?;
        if self.target_contract.dialect() != self.row_policy.target_dialect {
            return Err(RowConversionError::DialectMismatch);
        }
        validate_table_contract(self)?;
        validate_resumable_key(self)
    }

    /// Convert one complete source batch through the reviewed row policy.
    pub fn convert_batch(&self, source: &RowBatch) -> Result<RowBatch, RowConversionError> {
        self.validate()?;
        let converter = ReviewedRowTypeConverter::new(self.row_policy.clone())?;
        let target_columns = self
            .row_policy
            .columns
            .iter()
            .map(|column| column.target.clone())
            .collect::<Vec<_>>();
        let mut target = RowBatch::new(target_columns, source.len().max(1), usize::MAX);
        for row in source.rows() {
            let converted = converter.convert_row(source.columns(), row)?;
            target
                .try_push(converted.values, 0)
                .map_err(|_| RowConversionError::InvalidConvertedBatch)?;
        }
        Ok(target)
    }

    /// Convert a source resumable-key tuple to the exact target key tuple.
    pub fn convert_source_key(
        &self,
        source_key: &[DbValue],
    ) -> Result<Vec<DbValue>, RowConversionError> {
        self.validate()?;
        if source_key.len() != self.resumable_key.source_columns.len() {
            return Err(RowConversionError::InvalidSourceKey);
        }
        self.resumable_key
            .source_columns
            .iter()
            .zip(source_key)
            .map(|(name, value)| {
                let column = self
                    .row_policy
                    .columns
                    .iter()
                    .find(|column| column.source.name == *name)
                    .ok_or(RowConversionError::InvalidSourceKey)?;
                convert_value(column, value)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossDialectKeyKind {
    PrimaryKey,
    Unique,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossDialectResumableKey {
    pub source_name: Identifier,
    pub source_kind: CrossDialectKeyKind,
    pub source_columns: Vec<Identifier>,
    pub target_name: Option<Identifier>,
    pub target_kind: CrossDialectKeyKind,
    pub target_columns: Vec<Identifier>,
}

pub fn cross_dialect_target_key_name(
    target_table: &QualifiedTable,
    target_columns: &[Identifier],
) -> Result<Identifier, RowConversionError> {
    let mut digest = Sha256::new();
    for value in std::iter::once(&target_table.namespace)
        .chain(std::iter::once(&target_table.name))
        .chain(target_columns)
    {
        let bytes = value.as_str().as_bytes();
        digest.update(
            u64::try_from(bytes.len())
                .map_err(|_| RowConversionError::InvalidTargetKey)?
                .to_be_bytes(),
        );
        digest.update(bytes);
    }
    let suffix = hex::encode(digest.finalize());
    Identifier::new(format!("sqlspl_resume_{}", &suffix[..32]))
        .map_err(|_| RowConversionError::InvalidTargetKey)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "dialect", rename_all = "snake_case", deny_unknown_fields)]
pub enum CrossDialectTargetTableContract {
    PostgreSql {
        persistence: PostgresTargetPersistence,
    },
    MySql {
        engine: MySqlTargetEngine,
        character_set: Identifier,
        collation: Identifier,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTargetPersistence {
    Permanent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlTargetEngine {
    InnoDb,
}

impl CrossDialectTargetTableContract {
    pub const fn dialect(&self) -> ConversionDialect {
        match self {
            Self::PostgreSql { .. } => ConversionDialect::PostgreSql,
            Self::MySql { .. } => ConversionDialect::MySql,
        }
    }
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
            table.validate()?;
            if table.row_policy.source_dialect != *source_dialect
                || table.row_policy.target_dialect != *target_dialect
            {
                return Err(RowConversionError::DialectMismatch);
            }
        }
        Ok(())
    }
}

fn validate_table_contract(table: &TableConversionPolicy) -> Result<(), RowConversionError> {
    match &table.target_contract {
        CrossDialectTargetTableContract::PostgreSql { .. } => Ok(()),
        CrossDialectTargetTableContract::MySql {
            character_set,
            collation,
            ..
        } => {
            if character_set.as_str() != "utf8mb4"
                || !matches!(collation.as_str(), "utf8mb4_bin" | "utf8mb4_0900_bin")
            {
                return Err(RowConversionError::IncoherentTableContract);
            }
            let exact = table.row_policy.columns.iter().all(|column| {
                !matches!(
                    &column.target_type,
                    CrossDialectTargetType::MySql(MySqlTargetType::Text {
                        character_set: column_character_set,
                        collation: column_collation,
                        ..
                    }) if column_character_set != character_set || column_collation != collation
                )
            });
            if exact {
                Ok(())
            } else {
                Err(RowConversionError::IncoherentTableContract)
            }
        }
    }
}

fn validate_resumable_key(table: &TableConversionPolicy) -> Result<(), RowConversionError> {
    let key = &table.resumable_key;
    if key.source_columns.is_empty()
        || key.source_columns.len() != key.target_columns.len()
        || key.source_columns.iter().collect::<BTreeSet<_>>().len() != key.source_columns.len()
        || key.target_columns.iter().collect::<BTreeSet<_>>().len() != key.target_columns.len()
        || matches!(key.target_kind, CrossDialectKeyKind::PrimaryKey) && key.target_name.is_some()
        || matches!(key.target_kind, CrossDialectKeyKind::Unique) && key.target_name.is_none()
    {
        return Err(RowConversionError::InvalidTargetKey);
    }
    for (source_name, target_name) in key.source_columns.iter().zip(&key.target_columns) {
        let column = table
            .row_policy
            .columns
            .iter()
            .find(|column| column.source.name == *source_name && column.target.name == *target_name)
            .ok_or(RowConversionError::InvalidTargetKey)?;
        if column.source.nullable
            || column.target.nullable
            || !conversion_rule_is_order_preserving(&column.rule)
            || !target_type_supports_resumable_key(&column.target_type)
        {
            return Err(RowConversionError::InvalidTargetKey);
        }
    }
    Ok(())
}

fn target_type_supports_resumable_key(target_type: &CrossDialectTargetType) -> bool {
    match target_type {
        CrossDialectTargetType::PostgreSql(target_type) => matches!(
            target_type,
            PostgresTargetType::Boolean
                | PostgresTargetType::SmallInt
                | PostgresTargetType::Integer
                | PostgresTargetType::BigInt
                | PostgresTargetType::Numeric { .. }
                | PostgresTargetType::Bytea
                | PostgresTargetType::Date { .. }
                | PostgresTargetType::Time { .. }
                | PostgresTargetType::Timestamp { .. }
        ),
        CrossDialectTargetType::MySql(target_type) => match target_type {
            MySqlTargetType::Boolean
            | MySqlTargetType::Integer { .. }
            | MySqlTargetType::Decimal { .. }
            | MySqlTargetType::Date { .. }
            | MySqlTargetType::Time { .. }
            | MySqlTargetType::DateTime { .. }
            | MySqlTargetType::Timestamp { .. } => true,
            MySqlTargetType::Binary {
                storage:
                    MySqlBinaryStorage::Binary { length } | MySqlBinaryStorage::VarBinary { length },
            } => *length <= 3_072,
            MySqlTargetType::Float
            | MySqlTargetType::Double
            | MySqlTargetType::Text { .. }
            | MySqlTargetType::Binary { .. }
            | MySqlTargetType::Json => false,
        },
    }
}

fn conversion_rule_is_order_preserving(rule: &ValueConversionRule) -> bool {
    matches!(
        rule,
        ValueConversionRule::BooleanExact
            | ValueConversionRule::BooleanToSigned
            | ValueConversionRule::SignedZeroOneToBoolean
            | ValueConversionRule::SignedInteger { .. }
            | ValueConversionRule::UnsignedInteger { .. }
            | ValueConversionRule::UnsignedIntegerToDecimal { .. }
            | ValueConversionRule::SignedToUnsigned { .. }
            | ValueConversionRule::UnsignedToSigned { .. }
            | ValueConversionRule::DecimalExact { .. }
            | ValueConversionRule::BytesExact
            | ValueConversionRule::DateRange { .. }
            | ValueConversionRule::TimeRange { .. }
            | ValueConversionRule::TimestampExact { .. }
    )
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
    UnsignedIntegerToDecimal {
        maximum: u64,
        precision: u32,
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
    JsonCanonicalV1 {
        policy: JsonConversionPolicy,
    },
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
        source_semantics: TimestampSemantics,
        target_semantics: TimestampSemantics,
        conversion: TimestampConversionPolicy,
        accepted_range: TimestampRange,
    },
}

/// A schema-qualified database object identity. It is never executable SQL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualifiedIdentifier {
    pub namespace: Identifier,
    pub name: Identifier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresIdentityDataType {
    SmallInt,
    Integer,
    BigInt,
}

impl PostgresIdentityDataType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SmallInt => "smallint",
            Self::Integer => "integer",
            Self::BigInt => "bigint",
        }
    }

    pub const fn maximum_value(self) -> i64 {
        match self {
            Self::SmallInt => i16::MAX as i64,
            Self::Integer => i32::MAX as i64,
            Self::BigInt => i64::MAX,
        }
    }
}

/// Exact MySQL `AUTO_INCREMENT` to PostgreSQL identity effect.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossDialectPostgresIdentity {
    pub source_table: QualifiedTable,
    pub source_column: Identifier,
    pub target_table: QualifiedTable,
    pub target_column: Identifier,
    pub sequence: QualifiedIdentifier,
    pub data_type: PostgresIdentityDataType,
    pub next_value: i64,
}

impl CrossDialectPostgresIdentity {
    pub fn validate(&self) -> Result<(), RowConversionError> {
        if self.next_value < 1 || self.next_value > self.data_type.maximum_value() {
            return Err(RowConversionError::InvalidTargetType);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonConversionPolicy {
    PostgreSqlJsonbToMySqlBinaryV1,
    MySqlBinaryToPostgreSqlJsonbV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampSemantics {
    WallClock,
    UtcNormalized,
    OffsetAware,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampConversionPolicy {
    PreserveWallClock,
    UtcNormalizedToOffsetAware,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampBound {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanos: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampRange {
    pub minimum: TimestampBound,
    pub maximum: TimestampBound,
}

impl TimestampBound {
    fn is_valid(self) -> bool {
        valid_gregorian_date(self.year, self.month, self.day)
            && self.hour <= 23
            && self.minute <= 59
            && self.second <= 59
            && self.nanos <= 999_999_999
    }

    fn ceil_fraction(mut self, quantum: u32) -> Result<Self, RowConversionError> {
        let remainder = self.nanos % quantum;
        if remainder == 0 {
            return Ok(self);
        }
        self.nanos += quantum - remainder;
        if self.nanos < 1_000_000_000 {
            return Ok(self);
        }
        self.nanos = 0;
        self.increment_second()?;
        Ok(self)
    }

    fn increment_second(&mut self) -> Result<(), RowConversionError> {
        self.second += 1;
        if self.second <= 59 {
            return Ok(());
        }
        self.second = 0;
        self.minute += 1;
        if self.minute <= 59 {
            return Ok(());
        }
        self.minute = 0;
        self.hour += 1;
        if self.hour <= 23 {
            return Ok(());
        }
        self.hour = 0;
        self.day += 1;
        let month_days =
            days_in_month(self.year, self.month).ok_or(RowConversionError::InvalidTargetType)?;
        if self.day <= month_days {
            return Ok(());
        }
        self.day = 1;
        self.month += 1;
        if self.month <= 12 {
            return Ok(());
        }
        self.month = 1;
        self.year = self
            .year
            .checked_add(1)
            .ok_or(RowConversionError::InvalidTargetType)?;
        Ok(())
    }
}

impl TimestampRange {
    fn is_valid(self) -> bool {
        self.minimum.is_valid() && self.maximum.is_valid() && self.minimum <= self.maximum
    }

    pub fn at_precision(mut self, precision: u8) -> Result<Self, RowConversionError> {
        if precision > 6 || !self.is_valid() {
            return Err(RowConversionError::InvalidTargetType);
        }
        let quantum = 10_u32.pow(9 - u32::from(precision));
        self.minimum = self.minimum.ceil_fraction(quantum)?;
        self.maximum.nanos -= self.maximum.nanos % quantum;
        if self.minimum > self.maximum {
            return Err(RowConversionError::InvalidTargetType);
        }
        Ok(self)
    }
}

pub const MYSQL_DATETIME_RANGE: TimestampRange = TimestampRange {
    minimum: TimestampBound {
        year: 1_000,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        nanos: 0,
    },
    maximum: TimestampBound {
        year: 9_999,
        month: 12,
        day: 31,
        hour: 23,
        minute: 59,
        second: 59,
        nanos: 999_999_000,
    },
};

pub const MYSQL_TIMESTAMP_RANGE: TimestampRange = TimestampRange {
    minimum: TimestampBound {
        year: 1_970,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 1,
        nanos: 0,
    },
    maximum: TimestampBound {
        year: 2_038,
        month: 1,
        day: 19,
        hour: 3,
        minute: 14,
        second: 7,
        nanos: 999_999_000,
    },
};

pub const POSTGRES_TIMESTAMP_RANGE: TimestampRange = TimestampRange {
    minimum: TimestampBound {
        // Canonical years use astronomical numbering: 4713 BC is -4712.
        year: -4_712,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        nanos: 0,
    },
    maximum: TimestampBound {
        year: 294_276,
        month: 12,
        day: 31,
        hour: 23,
        minute: 59,
        second: 59,
        nanos: 999_999_000,
    },
};

pub const POSTGRES_TIME_MINIMUM_NANOS: i64 = 0;
pub const POSTGRES_TIME_MAXIMUM_NANOS: i64 = 86_400_000_000_000;
pub const MYSQL_TIME_MINIMUM_NANOS: i64 = -3_020_399_999_999_000;
pub const MYSQL_TIME_MAXIMUM_NANOS: i64 = 3_020_399_999_999_000;

/// A typed target-column constraint emitted by the target adapter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TargetCheckConstraint {
    NonNegative,
    SignedIntegerRange {
        minimum: i64,
        maximum: i64,
    },
    UnsignedIntegerRange {
        maximum: u64,
    },
    DateYearRange {
        minimum_year: i32,
        maximum_year: i32,
    },
    TimestampRange {
        range: TimestampRange,
    },
    CharacterLengthMaximum {
        maximum: u64,
    },
    OctetLengthMaximum {
        maximum: u64,
    },
}

/// A closed target DDL type selected by the target adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "dialect",
    content = "type",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CrossDialectTargetType {
    PostgreSql(PostgresTargetType),
    MySql(MySqlTargetType),
}

/// The exact source storage type observed by the source adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "dialect",
    content = "type",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CrossDialectSourceType {
    PostgreSql(PostgresTargetType),
    MySql(MySqlTargetType),
}

impl CrossDialectSourceType {
    pub const fn dialect(&self) -> ConversionDialect {
        match self {
            Self::PostgreSql(_) => ConversionDialect::PostgreSql,
            Self::MySql(_) => ConversionDialect::MySql,
        }
    }

    fn validate_for(&self, source: &ColumnMeta) -> Result<(), RowConversionError> {
        let expected = self.column_meta()?;
        if expected.vendor_type == source.vendor_type
            && expected.collation == source.collation
            && expected.precision == source.precision
            && expected.scale == source.scale
            && expected.timezone_semantics == source.timezone_semantics
        {
            Ok(())
        } else {
            Err(RowConversionError::SourceTypeMetadataMismatch {
                column: source.name.clone(),
            })
        }
    }

    fn column_meta(&self) -> Result<ColumnMeta, RowConversionError> {
        Ok(match self {
            Self::PostgreSql(source_type) => source_type.column_meta()?,
            Self::MySql(source_type) => source_type.column_meta()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case", deny_unknown_fields)]
pub enum PostgresTargetType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Numeric {
        precision: u32,
        scale: i32,
    },
    Real,
    DoublePrecision,
    Character {
        length: u32,
        collation: QualifiedIdentifier,
    },
    CharacterVarying {
        length: u32,
        collation: QualifiedIdentifier,
    },
    Text {
        collation: QualifiedIdentifier,
    },
    Bytea,
    Json,
    Jsonb,
    Date {
        minimum_year: i32,
        maximum_year: i32,
    },
    Time {
        precision: u8,
        with_time_zone: bool,
        minimum_nanos: i64,
        maximum_nanos: i64,
    },
    Timestamp {
        precision: u8,
        semantics: TimestampSemantics,
        range: TimestampRange,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MySqlIntegerWidth {
    Tiny,
    Small,
    Medium,
    Integer,
    Big,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlTextStorage {
    Char { length: u32 },
    VarChar { length: u32 },
    TinyText,
    Text,
    MediumText,
    LongText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlBinaryStorage {
    Binary { length: u32 },
    VarBinary { length: u32 },
    TinyBlob,
    Blob,
    MediumBlob,
    LongBlob,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case", deny_unknown_fields)]
pub enum MySqlTargetType {
    Boolean,
    Integer {
        width: MySqlIntegerWidth,
        unsigned: bool,
    },
    Decimal {
        precision: u32,
        scale: u32,
        unsigned: bool,
    },
    Float,
    Double,
    Text {
        storage: MySqlTextStorage,
        character_set: Identifier,
        collation: Identifier,
    },
    Binary {
        storage: MySqlBinaryStorage,
    },
    Json,
    Date {
        minimum_year: i32,
        maximum_year: i32,
    },
    Time {
        precision: u8,
        minimum_nanos: i64,
        maximum_nanos: i64,
    },
    DateTime {
        precision: u8,
        semantics: TimestampSemantics,
        range: TimestampRange,
    },
    Timestamp {
        precision: u8,
        semantics: TimestampSemantics,
        range: TimestampRange,
    },
}

impl CrossDialectTargetType {
    pub const fn dialect(&self) -> ConversionDialect {
        match self {
            Self::PostgreSql(_) => ConversionDialect::PostgreSql,
            Self::MySql(_) => ConversionDialect::MySql,
        }
    }

    pub fn column_meta(
        &self,
        name: Identifier,
        ordinal: u32,
        nullable: bool,
    ) -> Result<ColumnMeta, RowConversionError> {
        let mut metadata = match self {
            Self::PostgreSql(target_type) => target_type.column_meta()?,
            Self::MySql(target_type) => target_type.column_meta()?,
        };
        metadata.name = name;
        metadata.ordinal = ordinal;
        metadata.nullable = nullable;
        Ok(metadata)
    }

    fn validate_for(&self, target: &ColumnMeta) -> Result<(), RowConversionError> {
        let expected = self.column_meta(target.name.clone(), target.ordinal, target.nullable)?;
        if target == &expected {
            Ok(())
        } else {
            Err(RowConversionError::TargetTypeMetadataMismatch {
                column: target.name.clone(),
            })
        }
    }
}

impl PostgresTargetType {
    fn validate_for_source(&self, source: &ColumnMeta) -> Result<(), RowConversionError> {
        CrossDialectSourceType::PostgreSql(self.clone()).validate_for(source)
    }

    pub fn ddl_type(&self) -> Result<String, RowConversionError> {
        self.column_meta()?;
        Ok(match self {
            Self::Boolean => "boolean".into(),
            Self::SmallInt => "smallint".into(),
            Self::Integer => "integer".into(),
            Self::BigInt => "bigint".into(),
            Self::Numeric { precision, scale } => format!("numeric({precision},{scale})"),
            Self::Real => "real".into(),
            Self::DoublePrecision => "double precision".into(),
            Self::Character { length, .. } => format!("character({length})"),
            Self::CharacterVarying { length, .. } => {
                format!("character varying({length})")
            }
            Self::Text { .. } => "text".into(),
            Self::Bytea => "bytea".into(),
            Self::Json => "json".into(),
            Self::Jsonb => "jsonb".into(),
            Self::Date { .. } => "date".into(),
            Self::Time {
                precision,
                with_time_zone,
                ..
            } => format!(
                "time({precision}) {} time zone",
                if *with_time_zone { "with" } else { "without" }
            ),
            Self::Timestamp {
                precision,
                semantics,
                ..
            } => format!(
                "timestamp({precision}) {} time zone",
                if *semantics == TimestampSemantics::OffsetAware {
                    "with"
                } else {
                    "without"
                }
            ),
        })
    }

    pub fn collation(&self) -> Option<&QualifiedIdentifier> {
        match self {
            Self::Character { collation, .. }
            | Self::CharacterVarying { collation, .. }
            | Self::Text { collation } => Some(collation),
            _ => None,
        }
    }

    fn column_meta(&self) -> Result<ColumnMeta, RowConversionError> {
        if matches!(self, Self::Numeric { precision, scale } if *precision == 0 || *precision > 1_000 || *scale < 0 || u32::try_from(*scale).ok().is_none_or(|scale| scale > *precision))
            || matches!(self, Self::Time { precision, .. } | Self::Timestamp { precision, .. } if *precision > 6)
            || matches!(self, Self::Date { minimum_year, maximum_year } if minimum_year > maximum_year)
            || matches!(self, Self::Character { length, .. } | Self::CharacterVarying { length, .. } if *length == 0)
            || matches!(
                self,
                Self::Timestamp {
                    semantics: TimestampSemantics::UtcNormalized,
                    ..
                }
            )
            || matches!(self, Self::Timestamp { precision, range, .. } if !range.is_valid() || range.at_precision(*precision).ok() != Some(*range))
            || matches!(self, Self::Time { minimum_nanos, maximum_nanos, .. } if minimum_nanos > maximum_nanos)
        {
            return Err(RowConversionError::InvalidTargetType);
        }
        let (vendor_type, precision, scale, timezone_semantics) = match self {
            Self::Boolean => ("pg_catalog.bool", None, None, None),
            Self::SmallInt => ("pg_catalog.int2", None, None, None),
            Self::Integer => ("pg_catalog.int4", None, None, None),
            Self::BigInt => ("pg_catalog.int8", None, None, None),
            Self::Numeric { precision, scale } => {
                ("pg_catalog.numeric", Some(*precision), Some(*scale), None)
            }
            Self::Real => ("pg_catalog.float4", None, None, None),
            Self::DoublePrecision => ("pg_catalog.float8", None, None, None),
            Self::Character { .. } => ("pg_catalog.bpchar", None, None, None),
            Self::CharacterVarying { .. } => ("pg_catalog.varchar", None, None, None),
            Self::Text { .. } => ("pg_catalog.text", None, None, None),
            Self::Bytea => ("pg_catalog.bytea", None, None, None),
            Self::Json => ("pg_catalog.json", None, None, None),
            Self::Jsonb => ("pg_catalog.jsonb", None, None, None),
            Self::Date { .. } => ("pg_catalog.date", None, None, None),
            Self::Time {
                precision,
                with_time_zone,
                ..
            } => (
                if *with_time_zone {
                    "pg_catalog.timetz"
                } else {
                    "pg_catalog.time"
                },
                Some(u32::from(*precision)),
                None,
                Some(if *with_time_zone {
                    "with_time_zone"
                } else {
                    "without_time_zone"
                }),
            ),
            Self::Timestamp {
                precision,
                semantics,
                ..
            } => (
                if *semantics == TimestampSemantics::OffsetAware {
                    "pg_catalog.timestamptz"
                } else {
                    "pg_catalog.timestamp"
                },
                Some(u32::from(*precision)),
                None,
                Some(if *semantics == TimestampSemantics::OffsetAware {
                    "with_time_zone"
                } else {
                    "without_time_zone"
                }),
            ),
        };
        Ok(ColumnMeta {
            name: Identifier::new("target").map_err(|_| RowConversionError::InvalidTargetType)?,
            ordinal: 1,
            vendor_type: vendor_type.into(),
            nullable: false,
            collation: match self {
                Self::Character { collation, .. }
                | Self::CharacterVarying { collation, .. }
                | Self::Text { collation } => Some(format!(
                    "{}.{}",
                    collation.namespace.as_str(),
                    collation.name.as_str()
                )),
                _ => None,
            },
            precision,
            scale,
            timezone_semantics: timezone_semantics.map(str::to_owned),
        })
    }
}

impl MySqlTargetType {
    pub fn ddl_type(&self) -> Result<String, RowConversionError> {
        self.column_meta()?;
        let sql = match self {
            Self::Boolean => "tinyint(1)".into(),
            Self::Integer { width, unsigned } => format!(
                "{}{}",
                match width {
                    MySqlIntegerWidth::Tiny => "tinyint",
                    MySqlIntegerWidth::Small => "smallint",
                    MySqlIntegerWidth::Medium => "mediumint",
                    MySqlIntegerWidth::Integer => "int",
                    MySqlIntegerWidth::Big => "bigint",
                },
                if *unsigned { " unsigned" } else { "" }
            ),
            Self::Decimal {
                precision,
                scale,
                unsigned,
            } => format!(
                "decimal({precision},{scale}){}",
                if *unsigned { " unsigned" } else { "" }
            ),
            Self::Float => "float".into(),
            Self::Double => "double".into(),
            Self::Text { storage, .. } => match storage {
                MySqlTextStorage::Char { length } => format!("char({length})"),
                MySqlTextStorage::VarChar { length } => format!("varchar({length})"),
                MySqlTextStorage::TinyText => "tinytext".into(),
                MySqlTextStorage::Text => "text".into(),
                MySqlTextStorage::MediumText => "mediumtext".into(),
                MySqlTextStorage::LongText => "longtext".into(),
            },
            Self::Binary { storage } => match storage {
                MySqlBinaryStorage::Binary { length } => format!("binary({length})"),
                MySqlBinaryStorage::VarBinary { length } => format!("varbinary({length})"),
                MySqlBinaryStorage::TinyBlob => "tinyblob".into(),
                MySqlBinaryStorage::Blob => "blob".into(),
                MySqlBinaryStorage::MediumBlob => "mediumblob".into(),
                MySqlBinaryStorage::LongBlob => "longblob".into(),
            },
            Self::Json => "json".into(),
            Self::Date { .. } => "date".into(),
            Self::Time { precision, .. } => format!("time({precision})"),
            Self::DateTime { precision, .. } => format!("datetime({precision})"),
            Self::Timestamp { precision, .. } => format!("timestamp({precision})"),
        };
        Ok(sql)
    }

    pub fn character_set_and_collation(&self) -> Option<(&Identifier, &Identifier)> {
        match self {
            Self::Text {
                character_set,
                collation,
                ..
            } => Some((character_set, collation)),
            _ => None,
        }
    }

    fn column_meta(&self) -> Result<ColumnMeta, RowConversionError> {
        let (vendor_type, precision, scale, timezone_semantics, collation) = match self {
            Self::Boolean => ("tinyint", None, None, None, None),
            Self::Integer { width, .. } => (
                match width {
                    MySqlIntegerWidth::Tiny => "tinyint",
                    MySqlIntegerWidth::Small => "smallint",
                    MySqlIntegerWidth::Medium => "mediumint",
                    MySqlIntegerWidth::Integer => "int",
                    MySqlIntegerWidth::Big => "bigint",
                },
                None,
                None,
                None,
                None,
            ),
            Self::Decimal {
                precision, scale, ..
            } => (
                "decimal",
                Some(*precision),
                i32::try_from(*scale).ok(),
                None,
                None,
            ),
            Self::Float => ("float", None, None, None, None),
            Self::Double => ("double", None, None, None, None),
            Self::Text {
                storage, collation, ..
            } => (
                match storage {
                    MySqlTextStorage::Char { .. } => "char",
                    MySqlTextStorage::VarChar { .. } => "varchar",
                    MySqlTextStorage::TinyText => "tinytext",
                    MySqlTextStorage::Text => "text",
                    MySqlTextStorage::MediumText => "mediumtext",
                    MySqlTextStorage::LongText => "longtext",
                },
                None,
                None,
                None,
                Some(collation.as_str()),
            ),
            Self::Binary { storage } => (
                match storage {
                    MySqlBinaryStorage::Binary { .. } => "binary",
                    MySqlBinaryStorage::VarBinary { .. } => "varbinary",
                    MySqlBinaryStorage::TinyBlob => "tinyblob",
                    MySqlBinaryStorage::Blob => "blob",
                    MySqlBinaryStorage::MediumBlob => "mediumblob",
                    MySqlBinaryStorage::LongBlob => "longblob",
                },
                None,
                None,
                None,
                None,
            ),
            Self::Json => ("json", None, None, None, None),
            Self::Date { .. } => ("date", None, None, None, None),
            Self::Time { precision, .. } => ("time", Some(u32::from(*precision)), None, None, None),
            Self::DateTime { precision, .. } => (
                "datetime",
                Some(u32::from(*precision)),
                None,
                Some("local_without_offset"),
                None,
            ),
            Self::Timestamp { precision, .. } => (
                "timestamp",
                Some(u32::from(*precision)),
                None,
                Some("mysql_session_time_zone"),
                None,
            ),
        };
        if matches!(self, Self::Decimal { precision, scale, .. } if *precision == 0 || *precision > 65 || *scale > 30 || *scale > *precision)
            || matches!(self, Self::Time { precision, .. } | Self::DateTime { precision, .. } | Self::Timestamp { precision, .. } if *precision > 6)
            || matches!(self, Self::Text { storage: MySqlTextStorage::Char { length }, .. } if *length == 0 || *length > 255)
            || matches!(self, Self::Text {
                storage: MySqlTextStorage::VarChar { length },
                character_set,
                ..
            } if *length == 0
                || *length > 65_535
                || (character_set.as_str() == "utf8mb4"
                    && *length > MYSQL_UTF8MB4_VARCHAR_MAX_CHARACTERS))
            || matches!(self, Self::Binary { storage: MySqlBinaryStorage::Binary { length } } if *length == 0 || *length > 255)
            || matches!(self, Self::Binary { storage: MySqlBinaryStorage::VarBinary { length } } if *length == 0 || *length > 65_535)
            || matches!(self, Self::Date { minimum_year, maximum_year } if minimum_year > maximum_year)
            || matches!(self, Self::DateTime { precision, semantics, range } if *semantics != TimestampSemantics::WallClock || !range.is_valid() || range.at_precision(*precision).ok() != Some(*range))
            || matches!(self, Self::Timestamp { precision, semantics, range } if *semantics != TimestampSemantics::UtcNormalized || !range.is_valid() || range.at_precision(*precision).ok() != Some(*range))
            || matches!(self, Self::Time { minimum_nanos, maximum_nanos, .. } if minimum_nanos > maximum_nanos)
        {
            return Err(RowConversionError::InvalidTargetType);
        }
        Ok(ColumnMeta {
            name: Identifier::new("target").map_err(|_| RowConversionError::InvalidTargetType)?,
            ordinal: 1,
            vendor_type: vendor_type.into(),
            nullable: false,
            collation: collation.map(ToString::to_string),
            precision,
            scale,
            timezone_semantics: timezone_semantics.map(str::to_owned),
        })
    }
}

/// Derive one reviewed PostgreSQL-to-MySQL column conversion from adapter metadata.
pub fn derive_postgres_to_mysql_column(
    source: ColumnMeta,
    postgres_source_type: PostgresTargetType,
    target_name: Identifier,
    target_ordinal: u32,
    mysql_character_set: Identifier,
    mysql_collation: Identifier,
) -> Result<ColumnConversion, RowConversionError> {
    postgres_source_type.validate_for_source(&source)?;
    let (target, target_checks, rule) = match &postgres_source_type {
        PostgresTargetType::Boolean => (
            MySqlTargetType::Boolean,
            vec![TargetCheckConstraint::SignedIntegerRange {
                minimum: 0,
                maximum: 1,
            }],
            ValueConversionRule::BooleanToSigned,
        ),
        PostgresTargetType::SmallInt => (
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Small,
                unsigned: false,
            },
            Vec::new(),
            ValueConversionRule::SignedInteger {
                minimum: i64::from(i16::MIN),
                maximum: i64::from(i16::MAX),
            },
        ),
        PostgresTargetType::Integer => (
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Integer,
                unsigned: false,
            },
            Vec::new(),
            ValueConversionRule::SignedInteger {
                minimum: i64::from(i32::MIN),
                maximum: i64::from(i32::MAX),
            },
        ),
        PostgresTargetType::BigInt => (
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: false,
            },
            Vec::new(),
            ValueConversionRule::SignedInteger {
                minimum: i64::MIN,
                maximum: i64::MAX,
            },
        ),
        PostgresTargetType::Numeric { .. } => {
            let (precision, scale) = exact_decimal_metadata(&source)?;
            if precision > 65 || scale > 30 {
                return Err(RowConversionError::UnsupportedSourceType {
                    vendor_type: source.vendor_type.clone(),
                });
            }
            (
                MySqlTargetType::Decimal {
                    precision,
                    scale,
                    unsigned: false,
                },
                Vec::new(),
                ValueConversionRule::DecimalExact {
                    precision,
                    scale: i32::try_from(scale).map_err(|_| RowConversionError::InvalidRule)?,
                },
            )
        }
        PostgresTargetType::Real => (
            MySqlTargetType::Float,
            Vec::new(),
            ValueConversionRule::Float32ExactBits,
        ),
        PostgresTargetType::DoublePrecision => (
            MySqlTargetType::Double,
            Vec::new(),
            ValueConversionRule::Float64ExactBits,
        ),
        PostgresTargetType::Character { length, .. }
        | PostgresTargetType::CharacterVarying { length, .. } => (
            MySqlTargetType::Text {
                storage: MySqlTextStorage::VarChar { length: *length },
                character_set: mysql_character_set.clone(),
                collation: mysql_collation.clone(),
            },
            Vec::new(),
            ValueConversionRule::TextUtf8Exact,
        ),
        PostgresTargetType::Text { .. } => (
            MySqlTargetType::Text {
                storage: MySqlTextStorage::LongText,
                character_set: mysql_character_set.clone(),
                collation: mysql_collation.clone(),
            },
            Vec::new(),
            ValueConversionRule::TextUtf8Exact,
        ),
        PostgresTargetType::Bytea => (
            MySqlTargetType::Binary {
                storage: MySqlBinaryStorage::LongBlob,
            },
            Vec::new(),
            ValueConversionRule::BytesExact,
        ),
        PostgresTargetType::Jsonb => (
            MySqlTargetType::Json,
            Vec::new(),
            ValueConversionRule::JsonCanonicalV1 {
                policy: JsonConversionPolicy::PostgreSqlJsonbToMySqlBinaryV1,
            },
        ),
        PostgresTargetType::Date { .. } => (
            MySqlTargetType::Date {
                minimum_year: 1_000,
                maximum_year: 9_999,
            },
            Vec::new(),
            ValueConversionRule::DateRange {
                minimum_year: 1_000,
                maximum_year: 9_999,
            },
        ),
        PostgresTargetType::Time {
            precision,
            with_time_zone: false,
            ..
        } => (
            MySqlTargetType::Time {
                precision: *precision,
                minimum_nanos: MYSQL_TIME_MINIMUM_NANOS,
                maximum_nanos: MYSQL_TIME_MAXIMUM_NANOS,
            },
            Vec::new(),
            ValueConversionRule::TimeRange {
                minimum_nanos: POSTGRES_TIME_MINIMUM_NANOS,
                maximum_nanos: POSTGRES_TIME_MAXIMUM_NANOS,
            },
        ),
        PostgresTargetType::Timestamp {
            semantics: TimestampSemantics::WallClock,
            ..
        } => {
            let precision = temporal_precision(&source)?;
            let accepted_range = MYSQL_DATETIME_RANGE.at_precision(precision)?;
            (
                MySqlTargetType::DateTime {
                    precision,
                    semantics: TimestampSemantics::WallClock,
                    range: accepted_range,
                },
                Vec::new(),
                ValueConversionRule::TimestampExact {
                    precision,
                    source_semantics: TimestampSemantics::WallClock,
                    target_semantics: TimestampSemantics::WallClock,
                    conversion: TimestampConversionPolicy::PreserveWallClock,
                    accepted_range,
                },
            )
        }
        PostgresTargetType::Json
        | PostgresTargetType::Time {
            with_time_zone: true,
            ..
        }
        | PostgresTargetType::Timestamp { .. } => {
            return Err(RowConversionError::UnsupportedSourceType {
                vendor_type: source.vendor_type.clone(),
            });
        }
    };
    build_column_conversion(
        source,
        CrossDialectSourceType::PostgreSql(postgres_source_type),
        target_name,
        target_ordinal,
        CrossDialectTargetType::MySql(target),
        target_checks,
        rule,
    )
}

/// Derive one reviewed MySQL-to-PostgreSQL column conversion from adapter metadata.
pub fn derive_mysql_to_postgres_column(
    source: ColumnMeta,
    mysql_source_type: MySqlTargetType,
    target_name: Identifier,
    target_ordinal: u32,
    postgres_text_collation: QualifiedIdentifier,
) -> Result<ColumnConversion, RowConversionError> {
    CrossDialectSourceType::MySql(mysql_source_type.clone()).validate_for(&source)?;
    let (target, target_checks, rule) = match &mysql_source_type {
        MySqlTargetType::Boolean => (
            PostgresTargetType::Boolean,
            Vec::new(),
            ValueConversionRule::SignedZeroOneToBoolean,
        ),
        MySqlTargetType::Integer { width, unsigned } => {
            let (target, rule) = mysql_integer_to_postgres(*width, *unsigned);
            let checks = vec![mysql_integer_check(*width, *unsigned)];
            (target, checks, rule)
        }
        MySqlTargetType::Decimal {
            precision,
            scale,
            unsigned: _,
        } => {
            if source.precision != Some(*precision) || source.scale != i32::try_from(*scale).ok() {
                return Err(RowConversionError::UnsupportedSourceType {
                    vendor_type: source.vendor_type.clone(),
                });
            }
            let (precision, scale) = exact_decimal_metadata(&source)?;
            (
                PostgresTargetType::Numeric {
                    precision,
                    scale: i32::try_from(scale).map_err(|_| RowConversionError::InvalidRule)?,
                },
                if mysql_source_type.is_unsigned_numeric() {
                    vec![TargetCheckConstraint::NonNegative]
                } else {
                    Vec::new()
                },
                ValueConversionRule::DecimalExact {
                    precision,
                    scale: i32::try_from(scale).map_err(|_| RowConversionError::InvalidRule)?,
                },
            )
        }
        MySqlTargetType::Float => (
            PostgresTargetType::Real,
            Vec::new(),
            ValueConversionRule::Float32ExactBits,
        ),
        MySqlTargetType::Double => (
            PostgresTargetType::DoublePrecision,
            Vec::new(),
            ValueConversionRule::Float64ExactBits,
        ),
        MySqlTargetType::Text { storage, .. } => {
            let target = match storage {
                MySqlTextStorage::Char { length } => PostgresTargetType::CharacterVarying {
                    length: *length,
                    collation: postgres_text_collation.clone(),
                },
                MySqlTextStorage::VarChar { length } => PostgresTargetType::CharacterVarying {
                    length: *length,
                    collation: postgres_text_collation.clone(),
                },
                MySqlTextStorage::TinyText
                | MySqlTextStorage::Text
                | MySqlTextStorage::MediumText
                | MySqlTextStorage::LongText => PostgresTargetType::Text {
                    collation: postgres_text_collation.clone(),
                },
            };
            (
                target,
                mysql_text_target_checks(&mysql_source_type),
                ValueConversionRule::TextUtf8Exact,
            )
        }
        MySqlTargetType::Binary { .. } => (
            PostgresTargetType::Bytea,
            mysql_binary_target_checks(&mysql_source_type),
            ValueConversionRule::BytesExact,
        ),
        MySqlTargetType::Json => (
            PostgresTargetType::Jsonb,
            Vec::new(),
            ValueConversionRule::JsonCanonicalV1 {
                policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
            },
        ),
        MySqlTargetType::Date {
            minimum_year,
            maximum_year,
        } => (
            PostgresTargetType::Date {
                minimum_year: -4_712,
                maximum_year: 5_874_897,
            },
            vec![TargetCheckConstraint::DateYearRange {
                minimum_year: *minimum_year,
                maximum_year: *maximum_year,
            }],
            ValueConversionRule::DateRange {
                minimum_year: *minimum_year,
                maximum_year: *maximum_year,
            },
        ),
        MySqlTargetType::Time { precision, .. } => (
            PostgresTargetType::Time {
                precision: *precision,
                with_time_zone: false,
                minimum_nanos: POSTGRES_TIME_MINIMUM_NANOS,
                maximum_nanos: POSTGRES_TIME_MAXIMUM_NANOS,
            },
            Vec::new(),
            ValueConversionRule::TimeRange {
                minimum_nanos: POSTGRES_TIME_MINIMUM_NANOS,
                maximum_nanos: POSTGRES_TIME_MAXIMUM_NANOS,
            },
        ),
        MySqlTargetType::DateTime {
            precision,
            semantics,
            range,
        } => (
            PostgresTargetType::Timestamp {
                precision: *precision,
                semantics: TimestampSemantics::WallClock,
                range: POSTGRES_TIMESTAMP_RANGE,
            },
            vec![TargetCheckConstraint::TimestampRange { range: *range }],
            ValueConversionRule::TimestampExact {
                precision: *precision,
                source_semantics: *semantics,
                target_semantics: TimestampSemantics::WallClock,
                conversion: TimestampConversionPolicy::PreserveWallClock,
                accepted_range: *range,
            },
        ),
        MySqlTargetType::Timestamp {
            precision,
            semantics,
            range,
        } => (
            PostgresTargetType::Timestamp {
                precision: *precision,
                semantics: TimestampSemantics::OffsetAware,
                range: POSTGRES_TIMESTAMP_RANGE,
            },
            vec![TargetCheckConstraint::TimestampRange { range: *range }],
            ValueConversionRule::TimestampExact {
                precision: *precision,
                source_semantics: *semantics,
                target_semantics: TimestampSemantics::OffsetAware,
                conversion: TimestampConversionPolicy::UtcNormalizedToOffsetAware,
                accepted_range: *range,
            },
        ),
    };
    build_column_conversion(
        source,
        CrossDialectSourceType::MySql(mysql_source_type),
        target_name,
        target_ordinal,
        CrossDialectTargetType::PostgreSql(target),
        target_checks,
        rule,
    )
}

fn build_column_conversion(
    source: ColumnMeta,
    source_type: CrossDialectSourceType,
    target_name: Identifier,
    target_ordinal: u32,
    target_type: CrossDialectTargetType,
    target_checks: Vec<TargetCheckConstraint>,
    rule: ValueConversionRule,
) -> Result<ColumnConversion, RowConversionError> {
    let target = target_type.column_meta(target_name, target_ordinal, source.nullable)?;
    let conversion = ColumnConversion {
        source,
        source_type,
        target,
        target_type,
        target_checks,
        rule,
    };
    conversion.source_type.validate_for(&conversion.source)?;
    conversion.target_type.validate_for(&conversion.target)?;
    validate_target_checks(&conversion.target_checks)?;
    validate_rule_metadata(&conversion)?;
    Ok(conversion)
}

fn exact_decimal_metadata(source: &ColumnMeta) -> Result<(u32, u32), RowConversionError> {
    let precision = source.precision.filter(|precision| *precision > 0).ok_or(
        RowConversionError::UnsupportedSourceType {
            vendor_type: source.vendor_type.clone(),
        },
    )?;
    let scale = source
        .scale
        .and_then(|scale| u32::try_from(scale).ok())
        .filter(|scale| *scale <= precision)
        .ok_or(RowConversionError::UnsupportedSourceType {
            vendor_type: source.vendor_type.clone(),
        })?;
    Ok((precision, scale))
}

fn temporal_precision(source: &ColumnMeta) -> Result<u8, RowConversionError> {
    source
        .precision
        .and_then(|precision| u8::try_from(precision).ok())
        .filter(|precision| *precision <= 6)
        .ok_or(RowConversionError::UnsupportedSourceType {
            vendor_type: source.vendor_type.clone(),
        })
}

fn mysql_integer_to_postgres(
    width: MySqlIntegerWidth,
    unsigned: bool,
) -> (PostgresTargetType, ValueConversionRule) {
    if unsigned {
        return match width {
            MySqlIntegerWidth::Tiny => (
                PostgresTargetType::SmallInt,
                ValueConversionRule::UnsignedToSigned { maximum: 255 },
            ),
            MySqlIntegerWidth::Small => (
                PostgresTargetType::Integer,
                ValueConversionRule::UnsignedToSigned { maximum: 65_535 },
            ),
            MySqlIntegerWidth::Medium => (
                PostgresTargetType::Integer,
                ValueConversionRule::UnsignedToSigned {
                    maximum: 16_777_215,
                },
            ),
            MySqlIntegerWidth::Integer => (
                PostgresTargetType::BigInt,
                ValueConversionRule::UnsignedToSigned {
                    maximum: 4_294_967_295,
                },
            ),
            MySqlIntegerWidth::Big => (
                PostgresTargetType::Numeric {
                    precision: 20,
                    scale: 0,
                },
                ValueConversionRule::UnsignedIntegerToDecimal {
                    maximum: u64::MAX,
                    precision: 20,
                },
            ),
        };
    }
    let target = match width {
        MySqlIntegerWidth::Tiny | MySqlIntegerWidth::Small => PostgresTargetType::SmallInt,
        MySqlIntegerWidth::Medium | MySqlIntegerWidth::Integer => PostgresTargetType::Integer,
        MySqlIntegerWidth::Big => PostgresTargetType::BigInt,
    };
    let (minimum, maximum) = mysql_signed_integer_bounds(width);
    (
        target,
        ValueConversionRule::SignedInteger { minimum, maximum },
    )
}

fn mysql_signed_integer_bounds(width: MySqlIntegerWidth) -> (i64, i64) {
    match width {
        MySqlIntegerWidth::Tiny => (i64::from(i8::MIN), i64::from(i8::MAX)),
        MySqlIntegerWidth::Small => (i64::from(i16::MIN), i64::from(i16::MAX)),
        MySqlIntegerWidth::Medium => (-8_388_608, 8_388_607),
        MySqlIntegerWidth::Integer => (i64::from(i32::MIN), i64::from(i32::MAX)),
        MySqlIntegerWidth::Big => (i64::MIN, i64::MAX),
    }
}

fn mysql_integer_check(width: MySqlIntegerWidth, unsigned: bool) -> TargetCheckConstraint {
    if unsigned {
        return TargetCheckConstraint::UnsignedIntegerRange {
            maximum: match width {
                MySqlIntegerWidth::Tiny => u64::from(u8::MAX),
                MySqlIntegerWidth::Small => u64::from(u16::MAX),
                MySqlIntegerWidth::Medium => 16_777_215,
                MySqlIntegerWidth::Integer => u64::from(u32::MAX),
                MySqlIntegerWidth::Big => u64::MAX,
            },
        };
    }
    let (minimum, maximum) = mysql_signed_integer_bounds(width);
    TargetCheckConstraint::SignedIntegerRange { minimum, maximum }
}

fn mysql_text_target_checks(source: &MySqlTargetType) -> Vec<TargetCheckConstraint> {
    let MySqlTargetType::Text { storage, .. } = source else {
        return Vec::new();
    };
    vec![match storage {
        MySqlTextStorage::Char { length } | MySqlTextStorage::VarChar { length } => {
            TargetCheckConstraint::CharacterLengthMaximum {
                maximum: u64::from(*length),
            }
        }
        MySqlTextStorage::TinyText => TargetCheckConstraint::OctetLengthMaximum { maximum: 255 },
        MySqlTextStorage::Text => TargetCheckConstraint::OctetLengthMaximum { maximum: 65_535 },
        MySqlTextStorage::MediumText => TargetCheckConstraint::OctetLengthMaximum {
            maximum: 16_777_215,
        },
        MySqlTextStorage::LongText => TargetCheckConstraint::OctetLengthMaximum {
            maximum: 4_294_967_295,
        },
    }]
}

fn mysql_binary_target_checks(source: &MySqlTargetType) -> Vec<TargetCheckConstraint> {
    let MySqlTargetType::Binary { storage } = source else {
        return Vec::new();
    };
    vec![TargetCheckConstraint::OctetLengthMaximum {
        maximum: match storage {
            MySqlBinaryStorage::Binary { length } | MySqlBinaryStorage::VarBinary { length } => {
                u64::from(*length)
            }
            MySqlBinaryStorage::TinyBlob => 255,
            MySqlBinaryStorage::Blob => 65_535,
            MySqlBinaryStorage::MediumBlob => 16_777_215,
            MySqlBinaryStorage::LongBlob => 4_294_967_295,
        },
    }]
}

impl MySqlTargetType {
    fn is_unsigned_numeric(&self) -> bool {
        matches!(self, Self::Decimal { unsigned: true, .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnConversion {
    pub source: ColumnMeta,
    pub source_type: CrossDialectSourceType,
    pub target: ColumnMeta,
    pub target_type: CrossDialectTargetType,
    pub target_checks: Vec<TargetCheckConstraint>,
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
        for (index, column) in self.columns.iter().enumerate() {
            let expected_ordinal =
                u32::try_from(index + 1).map_err(|_| RowConversionError::InvalidColumnOrder)?;
            if column.source.ordinal != expected_ordinal
                || column.target.ordinal != expected_ordinal
            {
                return Err(RowConversionError::InvalidColumnOrder);
            }
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
            if column.source.nullable != column.target.nullable {
                return Err(RowConversionError::NullabilityMismatch {
                    column: column.source.name.clone(),
                });
            }
            if column.source_type.dialect() != self.source_dialect {
                return Err(RowConversionError::DialectMismatch);
            }
            if column.target_type.dialect() != self.target_dialect {
                return Err(RowConversionError::DialectMismatch);
            }
            column.source_type.validate_for(&column.source)?;
            column.target_type.validate_for(&column.target)?;
            validate_target_checks(&column.target_checks)?;
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
    let result = match &column.rule {
        ValueConversionRule::SignedInteger { minimum, maximum } if minimum > maximum => {
            Err(RowConversionError::InvalidRule)
        }
        ValueConversionRule::UnsignedToSigned { maximum } if *maximum < 0 => {
            Err(RowConversionError::InvalidRule)
        }
        ValueConversionRule::UnsignedIntegerToDecimal { maximum, precision }
            if *precision == 0
                || *precision > 1_000
                || u32::try_from(maximum.to_string().len())
                    .ok()
                    .is_none_or(|digits| digits > *precision) =>
        {
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
        ValueConversionRule::TimestampExact {
            source_semantics,
            target_semantics,
            conversion,
            accepted_range,
            ..
        } if !accepted_range.is_valid()
            || !matches!(
                (source_semantics, target_semantics, conversion),
                (
                    TimestampSemantics::WallClock,
                    TimestampSemantics::WallClock,
                    TimestampConversionPolicy::PreserveWallClock
                ) | (
                    TimestampSemantics::UtcNormalized,
                    TimestampSemantics::OffsetAware,
                    TimestampConversionPolicy::UtcNormalizedToOffsetAware
                )
            ) =>
        {
            Err(RowConversionError::InvalidRule)
        }
        _ => Ok(()),
    };
    result?;
    validate_type_rule_coherence(column)
}

fn validate_target_checks(checks: &[TargetCheckConstraint]) -> Result<(), RowConversionError> {
    if checks.windows(2).any(|pair| pair[0] >= pair[1])
        || checks.iter().any(|check| match check {
            TargetCheckConstraint::NonNegative => false,
            TargetCheckConstraint::SignedIntegerRange { minimum, maximum } => minimum > maximum,
            TargetCheckConstraint::UnsignedIntegerRange { .. } => false,
            TargetCheckConstraint::DateYearRange {
                minimum_year,
                maximum_year,
            } => minimum_year > maximum_year,
            TargetCheckConstraint::TimestampRange { range } => !range.is_valid(),
            TargetCheckConstraint::CharacterLengthMaximum { maximum }
            | TargetCheckConstraint::OctetLengthMaximum { maximum } => *maximum == 0,
        })
    {
        return Err(RowConversionError::InvalidTargetChecks);
    }
    Ok(())
}

fn validate_type_rule_coherence(column: &ColumnConversion) -> Result<(), RowConversionError> {
    let coherent = match (&column.source_type, &column.target_type, &column.rule) {
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Boolean),
            CrossDialectTargetType::MySql(MySqlTargetType::Boolean),
            ValueConversionRule::BooleanToSigned,
        ) => {
            column.target_checks
                == [TargetCheckConstraint::SignedIntegerRange {
                    minimum: 0,
                    maximum: 1,
                }]
        }
        (
            CrossDialectSourceType::PostgreSql(source),
            CrossDialectTargetType::MySql(target),
            ValueConversionRule::SignedInteger { minimum, maximum },
        ) => {
            postgres_signed_integer_mapping(source, target, *minimum, *maximum)
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Numeric { precision, scale }),
            CrossDialectTargetType::MySql(MySqlTargetType::Decimal {
                precision: target_precision,
                scale: target_scale,
                unsigned: false,
            }),
            ValueConversionRule::DecimalExact {
                precision: rule_precision,
                scale: rule_scale,
            },
        ) => {
            precision == target_precision
                && Some(*scale) == i32::try_from(*target_scale).ok()
                && precision == rule_precision
                && scale == rule_scale
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Real),
            CrossDialectTargetType::MySql(MySqlTargetType::Float),
            ValueConversionRule::Float32ExactBits,
        )
        | (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::DoublePrecision),
            CrossDialectTargetType::MySql(MySqlTargetType::Double),
            ValueConversionRule::Float64ExactBits,
        )
        | (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Bytea),
            CrossDialectTargetType::MySql(MySqlTargetType::Binary { .. }),
            ValueConversionRule::BytesExact,
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::PostgreSql(
                PostgresTargetType::Character { length, .. }
                | PostgresTargetType::CharacterVarying { length, .. },
            ),
            CrossDialectTargetType::MySql(MySqlTargetType::Text {
                storage:
                    MySqlTextStorage::VarChar {
                        length: target_length,
                    },
                ..
            }),
            ValueConversionRule::TextUtf8Exact,
        ) => target_length == length && column.target_checks.is_empty(),
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Text { .. }),
            CrossDialectTargetType::MySql(MySqlTargetType::Text {
                storage: MySqlTextStorage::LongText,
                ..
            }),
            ValueConversionRule::TextUtf8Exact,
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Jsonb),
            CrossDialectTargetType::MySql(MySqlTargetType::Json),
            ValueConversionRule::JsonCanonicalV1 {
                policy: JsonConversionPolicy::PostgreSqlJsonbToMySqlBinaryV1,
            },
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Date { .. }),
            CrossDialectTargetType::MySql(MySqlTargetType::Date {
                minimum_year,
                maximum_year,
            }),
            ValueConversionRule::DateRange {
                minimum_year: rule_minimum,
                maximum_year: rule_maximum,
            },
        ) => {
            minimum_year == rule_minimum
                && maximum_year == rule_maximum
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Time {
                precision,
                with_time_zone: false,
                minimum_nanos,
                maximum_nanos,
            }),
            CrossDialectTargetType::MySql(MySqlTargetType::Time {
                precision: target_precision,
                minimum_nanos: target_minimum,
                maximum_nanos: target_maximum,
            }),
            ValueConversionRule::TimeRange {
                minimum_nanos: rule_minimum,
                maximum_nanos: rule_maximum,
            },
        ) => {
            precision == target_precision
                && minimum_nanos == rule_minimum
                && maximum_nanos == rule_maximum
                && *target_minimum == MYSQL_TIME_MINIMUM_NANOS
                && *target_maximum == MYSQL_TIME_MAXIMUM_NANOS
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::PostgreSql(PostgresTargetType::Timestamp {
                precision,
                semantics: TimestampSemantics::WallClock,
                ..
            }),
            CrossDialectTargetType::MySql(MySqlTargetType::DateTime {
                precision: target_precision,
                semantics: TimestampSemantics::WallClock,
                range,
            }),
            ValueConversionRule::TimestampExact {
                precision: rule_precision,
                source_semantics: TimestampSemantics::WallClock,
                target_semantics: TimestampSemantics::WallClock,
                conversion: TimestampConversionPolicy::PreserveWallClock,
                accepted_range,
            },
        ) => {
            precision == target_precision
                && precision == rule_precision
                && range == accepted_range
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Boolean),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Boolean),
            ValueConversionRule::SignedZeroOneToBoolean,
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Integer { width, unsigned }),
            CrossDialectTargetType::PostgreSql(target),
            ValueConversionRule::SignedInteger { .. }
            | ValueConversionRule::UnsignedIntegerToDecimal { .. }
            | ValueConversionRule::UnsignedToSigned { .. },
        ) => {
            let (expected_target, expected_rule) = mysql_integer_to_postgres(*width, *unsigned);
            target == &expected_target
                && column.rule == expected_rule
                && column.target_checks == [mysql_integer_check(*width, *unsigned)]
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Decimal {
                precision,
                scale,
                unsigned,
            }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Numeric {
                precision: target_precision,
                scale: target_scale,
            }),
            ValueConversionRule::DecimalExact {
                precision: rule_precision,
                scale: rule_scale,
            },
        ) => {
            precision == target_precision
                && i32::try_from(*scale).ok() == Some(*target_scale)
                && precision == rule_precision
                && target_scale == rule_scale
                && column.target_checks
                    == if *unsigned {
                        vec![TargetCheckConstraint::NonNegative]
                    } else {
                        Vec::new()
                    }
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Float),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Real),
            ValueConversionRule::Float32ExactBits,
        )
        | (
            CrossDialectSourceType::MySql(MySqlTargetType::Double),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::DoublePrecision),
            ValueConversionRule::Float64ExactBits,
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::MySql(source @ MySqlTargetType::Text { character_set, .. }),
            CrossDialectTargetType::PostgreSql(
                PostgresTargetType::Character { .. }
                | PostgresTargetType::CharacterVarying { .. }
                | PostgresTargetType::Text { .. },
            ),
            ValueConversionRule::TextUtf8Exact,
        ) => {
            character_set.as_str() == "utf8mb4"
                && column.target_checks == mysql_text_target_checks(source)
        }
        (
            CrossDialectSourceType::MySql(source @ MySqlTargetType::Binary { .. }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Bytea),
            ValueConversionRule::BytesExact,
        ) => column.target_checks == mysql_binary_target_checks(source),
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Json),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Jsonb),
            ValueConversionRule::JsonCanonicalV1 {
                policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
            },
        ) => column.target_checks.is_empty(),
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Date {
                minimum_year,
                maximum_year,
            }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Date {
                minimum_year: target_minimum,
                maximum_year: target_maximum,
            }),
            ValueConversionRule::DateRange {
                minimum_year: rule_minimum,
                maximum_year: rule_maximum,
            },
        ) => {
            *target_minimum == -4_712
                && *target_maximum == 5_874_897
                && minimum_year == rule_minimum
                && maximum_year == rule_maximum
                && column.target_checks
                    == [TargetCheckConstraint::DateYearRange {
                        minimum_year: *minimum_year,
                        maximum_year: *maximum_year,
                    }]
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Time { precision, .. }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Time {
                precision: target_precision,
                with_time_zone: false,
                minimum_nanos,
                maximum_nanos,
            }),
            ValueConversionRule::TimeRange {
                minimum_nanos: rule_minimum,
                maximum_nanos: rule_maximum,
            },
        ) => {
            precision == target_precision
                && *minimum_nanos == POSTGRES_TIME_MINIMUM_NANOS
                && *maximum_nanos == POSTGRES_TIME_MAXIMUM_NANOS
                && *rule_minimum == POSTGRES_TIME_MINIMUM_NANOS
                && *rule_maximum == POSTGRES_TIME_MAXIMUM_NANOS
                && column.target_checks.is_empty()
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::DateTime {
                precision,
                semantics,
                range,
            }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Timestamp {
                precision: target_precision,
                semantics: TimestampSemantics::WallClock,
                range: target_range,
            }),
            ValueConversionRule::TimestampExact {
                precision: rule_precision,
                source_semantics,
                target_semantics: TimestampSemantics::WallClock,
                conversion: TimestampConversionPolicy::PreserveWallClock,
                accepted_range,
            },
        ) => {
            precision == target_precision
                && precision == rule_precision
                && semantics == source_semantics
                && *target_range == POSTGRES_TIMESTAMP_RANGE
                && range == accepted_range
                && column.target_checks == [TargetCheckConstraint::TimestampRange { range: *range }]
        }
        (
            CrossDialectSourceType::MySql(MySqlTargetType::Timestamp {
                precision,
                semantics,
                range,
            }),
            CrossDialectTargetType::PostgreSql(PostgresTargetType::Timestamp {
                precision: target_precision,
                semantics: TimestampSemantics::OffsetAware,
                range: target_range,
            }),
            ValueConversionRule::TimestampExact {
                precision: rule_precision,
                source_semantics,
                target_semantics: TimestampSemantics::OffsetAware,
                conversion: TimestampConversionPolicy::UtcNormalizedToOffsetAware,
                accepted_range,
            },
        ) => {
            precision == target_precision
                && precision == rule_precision
                && semantics == source_semantics
                && *target_range == POSTGRES_TIMESTAMP_RANGE
                && range == accepted_range
                && column.target_checks == [TargetCheckConstraint::TimestampRange { range: *range }]
        }
        _ => false,
    };
    if coherent {
        Ok(())
    } else {
        Err(RowConversionError::IncoherentTypeRule)
    }
}

fn postgres_signed_integer_mapping(
    source: &PostgresTargetType,
    target: &MySqlTargetType,
    minimum: i64,
    maximum: i64,
) -> bool {
    match (source, target) {
        (
            PostgresTargetType::SmallInt,
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Small,
                unsigned: false,
            },
        ) => minimum == i64::from(i16::MIN) && maximum == i64::from(i16::MAX),
        (
            PostgresTargetType::Integer,
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Integer,
                unsigned: false,
            },
        ) => minimum == i64::from(i32::MIN) && maximum == i64::from(i32::MAX),
        (
            PostgresTargetType::BigInt,
            MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: false,
            },
        ) => minimum == i64::MIN && maximum == i64::MAX,
        _ => false,
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
        (
            ValueConversionRule::UnsignedIntegerToDecimal { maximum, precision },
            DbValue::Unsigned(value),
        ) if *value <= u128::from(*maximum) => {
            let coefficient = value.to_string().into_bytes();
            if !decimal_fits(&coefficient, *precision) {
                return Err(failure());
            }
            DbValue::Decimal {
                coefficient,
                scale: 0,
            }
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
        (ValueConversionRule::JsonCanonicalV1 { .. }, DbValue::Json(value)) => {
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
            ValueConversionRule::TimestampExact {
                precision,
                source_semantics,
                target_semantics,
                conversion,
                accepted_range,
            },
            DbValue::Timestamp {
                local,
                offset_minutes,
                precision: value_precision,
            },
        ) if value_precision == precision
            && timestamp_bound_from_local(local).is_some_and(|value| {
                value >= accepted_range.minimum && value <= accepted_range.maximum
            })
            && timestamp_source_offset_is_valid(*source_semantics, *offset_minutes) =>
        {
            let target_offset = match conversion {
                TimestampConversionPolicy::PreserveWallClock => None,
                TimestampConversionPolicy::UtcNormalizedToOffsetAware => Some(0),
            };
            if !timestamp_target_offset_is_valid(*target_semantics, target_offset) {
                return Err(failure());
            }
            DbValue::Timestamp {
                local: local.clone(),
                offset_minutes: target_offset,
                precision: *value_precision,
            }
        }
        _ => return Err(failure()),
    };
    validate_converted_checks(column, &converted).map_err(|_| failure())?;
    Ok(converted)
}

fn timestamp_source_offset_is_valid(
    semantics: TimestampSemantics,
    offset_minutes: Option<i16>,
) -> bool {
    match semantics {
        TimestampSemantics::WallClock | TimestampSemantics::UtcNormalized => {
            offset_minutes.is_none()
        }
        TimestampSemantics::OffsetAware => {
            offset_minutes.is_some_and(|offset| (-14 * 60..=14 * 60).contains(&offset))
        }
    }
}

fn timestamp_target_offset_is_valid(
    semantics: TimestampSemantics,
    offset_minutes: Option<i16>,
) -> bool {
    match semantics {
        TimestampSemantics::WallClock | TimestampSemantics::UtcNormalized => {
            offset_minutes.is_none()
        }
        TimestampSemantics::OffsetAware => {
            offset_minutes.is_some_and(|offset| (-14 * 60..=14 * 60).contains(&offset))
        }
    }
}

fn timestamp_bound_from_local(local: &str) -> Option<TimestampBound> {
    let (date, time) = local.split_once(' ')?;
    let (year, month_day) = if let Some(rest) = date.strip_prefix('-') {
        let (year, month_day) = rest.split_once('-')?;
        (-year.parse::<i32>().ok()?, month_day)
    } else {
        let (year, month_day) = date.split_once('-')?;
        (year.parse().ok()?, month_day)
    };
    let (month, day) = month_day.split_once('-')?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse().ok()?;
    let minute = time_parts.next()?.parse().ok()?;
    let second_and_fraction = time_parts.next()?;
    if time_parts.next().is_some() {
        return None;
    }
    let (second, nanos) = if let Some((second, fraction)) = second_and_fraction.split_once('.') {
        if fraction.is_empty()
            || fraction.len() > 9
            || !fraction.as_bytes().iter().all(u8::is_ascii_digit)
        {
            return None;
        }
        let mut nanos = fraction.parse::<u32>().ok()?;
        for _ in fraction.len()..9 {
            nanos = nanos.checked_mul(10)?;
        }
        (second.parse().ok()?, nanos)
    } else {
        (second_and_fraction.parse().ok()?, 0)
    };
    let value = TimestampBound {
        year,
        month: month.parse().ok()?,
        day: day.parse().ok()?,
        hour,
        minute,
        second,
        nanos,
    };
    value.is_valid().then_some(value)
}

fn validate_converted_checks(
    column: &ColumnConversion,
    value: &DbValue,
) -> Result<(), RowConversionError> {
    for check in &column.target_checks {
        let accepted = match (check, value) {
            (TargetCheckConstraint::NonNegative, DbValue::Signed(value)) => *value >= 0,
            (TargetCheckConstraint::NonNegative, DbValue::Unsigned(_)) => true,
            (TargetCheckConstraint::NonNegative, DbValue::Decimal { coefficient, .. }) => {
                !coefficient.starts_with(b"-")
            }
            (
                TargetCheckConstraint::SignedIntegerRange { minimum, maximum },
                DbValue::Signed(value),
            ) => *value >= i128::from(*minimum) && *value <= i128::from(*maximum),
            (
                TargetCheckConstraint::SignedIntegerRange { minimum, maximum },
                DbValue::Unsigned(value),
            ) => *minimum >= 0 && u128::try_from(*maximum).is_ok_and(|maximum| *value <= maximum),
            (TargetCheckConstraint::UnsignedIntegerRange { maximum }, DbValue::Unsigned(value)) => {
                *value <= u128::from(*maximum)
            }
            (TargetCheckConstraint::UnsignedIntegerRange { maximum }, DbValue::Signed(value)) => {
                *value >= 0
                    && u128::try_from(*value).is_ok_and(|value| value <= u128::from(*maximum))
            }
            (
                TargetCheckConstraint::UnsignedIntegerRange { maximum },
                DbValue::Decimal {
                    coefficient,
                    scale: 0,
                },
            ) => std::str::from_utf8(coefficient)
                .ok()
                .and_then(|value| value.parse::<u128>().ok())
                .is_some_and(|value| value <= u128::from(*maximum)),
            (
                TargetCheckConstraint::DateYearRange {
                    minimum_year,
                    maximum_year,
                },
                DbValue::Date { year, .. },
            ) => year >= minimum_year && year <= maximum_year,
            (TargetCheckConstraint::TimestampRange { range }, DbValue::Timestamp { local, .. }) => {
                timestamp_bound_from_local(local)
                    .is_some_and(|value| value >= range.minimum && value <= range.maximum)
            }
            (TargetCheckConstraint::CharacterLengthMaximum { maximum }, DbValue::Text(value)) => {
                u64::try_from(value.chars().count()).is_ok_and(|length| length <= *maximum)
            }
            (TargetCheckConstraint::OctetLengthMaximum { maximum }, DbValue::Text(value)) => {
                u64::try_from(value.len()).is_ok_and(|length| length <= *maximum)
            }
            (TargetCheckConstraint::OctetLengthMaximum { maximum }, DbValue::Bytes(value)) => {
                u64::try_from(value.len()).is_ok_and(|length| length <= *maximum)
            }
            _ => false,
        };
        if !accepted {
            return Err(RowConversionError::ValueOutsidePolicy {
                column: column.source.name.clone(),
            });
        }
    }
    Ok(())
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
    days_in_month(year, month).is_some_and(|maximum_day| day != 0 && day <= maximum_day)
}

fn days_in_month(year: i32, month: u8) -> Option<u8> {
    let leap_year =
        year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0);
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return None,
    })
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
    #[error("target type metadata differs for column {column}")]
    TargetTypeMetadataMismatch { column: Identifier },
    #[error("source type metadata differs for column {column}")]
    SourceTypeMetadataMismatch { column: Identifier },
    #[error("cross-dialect target type is invalid")]
    InvalidTargetType,
    #[error("source vendor type {vendor_type} has no reviewed cross-dialect mapping")]
    UnsupportedSourceType { vendor_type: String },
    #[error("row-conversion policy contains duplicate source column {column}")]
    DuplicateSourceColumn { column: Identifier },
    #[error("row-conversion policy contains duplicate target column {column}")]
    DuplicateTargetColumn { column: Identifier },
    #[error("source and target nullability differ for column {column}")]
    NullabilityMismatch { column: Identifier },
    #[error("row-conversion policy has an invalid typed rule")]
    InvalidRule,
    #[error("row-conversion policy has invalid or non-canonical target checks")]
    InvalidTargetChecks,
    #[error("source type, conversion rule, target type, and target checks are incoherent")]
    IncoherentTypeRule,
    #[error("target table contract is incoherent with its target column contracts")]
    IncoherentTableContract,
    #[error("cross-dialect resumable key is missing, malformed, or not order preserving")]
    InvalidTargetKey,
    #[error("source resumable-key tuple differs from the reviewed policy")]
    InvalidSourceKey,
    #[error("converted row batch violates its reviewed shape")]
    InvalidConvertedBatch,
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

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).unwrap()
    }

    fn postgres_collation() -> QualifiedIdentifier {
        QualifiedIdentifier {
            namespace: identifier("pg_catalog"),
            name: identifier("C"),
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
                .map(|(index, rule)| {
                    let ordinal = u32::try_from(index + 1).unwrap();
                    let source_type = mysql_source_type_for_rule(&rule);
                    let source = identified(
                        source_type.column_meta().unwrap(),
                        format!("source_{index}"),
                        ordinal,
                    );
                    let conversion = derive_mysql_to_postgres_column(
                        source,
                        match &source_type {
                            CrossDialectSourceType::MySql(source_type) => source_type.clone(),
                            CrossDialectSourceType::PostgreSql(_) => unreachable!(),
                        },
                        identifier(&format!("target_{index}")),
                        ordinal,
                        postgres_collation(),
                    )
                    .unwrap();
                    assert_eq!(conversion.rule, rule);
                    conversion
                })
                .collect(),
        }
    }

    fn mysql_source_type_for_rule(rule: &ValueConversionRule) -> CrossDialectSourceType {
        let source = match rule {
            ValueConversionRule::SignedZeroOneToBoolean => MySqlTargetType::Boolean,
            ValueConversionRule::SignedInteger { .. } => MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: false,
            },
            ValueConversionRule::UnsignedInteger { .. } => MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: true,
            },
            ValueConversionRule::UnsignedIntegerToDecimal { .. } => MySqlTargetType::Integer {
                width: MySqlIntegerWidth::Big,
                unsigned: true,
            },
            ValueConversionRule::DecimalExact { precision, scale } => MySqlTargetType::Decimal {
                precision: *precision,
                scale: u32::try_from(*scale).unwrap(),
                unsigned: false,
            },
            ValueConversionRule::Float32ExactBits => MySqlTargetType::Float,
            ValueConversionRule::Float64ExactBits => MySqlTargetType::Double,
            ValueConversionRule::TextUtf8Exact => MySqlTargetType::Text {
                storage: MySqlTextStorage::LongText,
                character_set: identifier("utf8mb4"),
                collation: identifier("utf8mb4_0900_bin"),
            },
            ValueConversionRule::BytesExact => MySqlTargetType::Binary {
                storage: MySqlBinaryStorage::LongBlob,
            },
            ValueConversionRule::JsonCanonicalV1 {
                policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
            } => MySqlTargetType::Json,
            ValueConversionRule::DateRange { .. } => MySqlTargetType::Date {
                minimum_year: 1_000,
                maximum_year: 9_999,
            },
            ValueConversionRule::TimeRange { .. } => MySqlTargetType::Time {
                precision: 6,
                minimum_nanos: MYSQL_TIME_MINIMUM_NANOS,
                maximum_nanos: MYSQL_TIME_MAXIMUM_NANOS,
            },
            ValueConversionRule::TimestampExact {
                precision,
                source_semantics: TimestampSemantics::WallClock,
                target_semantics: TimestampSemantics::WallClock,
                conversion: TimestampConversionPolicy::PreserveWallClock,
                accepted_range: MYSQL_DATETIME_RANGE,
            } => MySqlTargetType::DateTime {
                precision: *precision,
                semantics: TimestampSemantics::WallClock,
                range: MYSQL_DATETIME_RANGE,
            },
            ValueConversionRule::TimestampExact {
                precision,
                source_semantics: TimestampSemantics::UtcNormalized,
                target_semantics: TimestampSemantics::OffsetAware,
                conversion: TimestampConversionPolicy::UtcNormalizedToOffsetAware,
                accepted_range: MYSQL_TIMESTAMP_RANGE,
            } => MySqlTargetType::Timestamp {
                precision: *precision,
                semantics: TimestampSemantics::UtcNormalized,
                range: MYSQL_TIMESTAMP_RANGE,
            },
            _ => panic!("test rule has no reviewed MySQL-to-PostgreSQL mapping"),
        };
        CrossDialectSourceType::MySql(source)
    }

    fn convert_one_result(
        rule: ValueConversionRule,
        value: DbValue,
    ) -> Result<DbValue, RowConversionError> {
        let metadata = ColumnMeta {
            name: identifier("value"),
            ordinal: 1,
            vendor_type: "test".into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        };
        convert_value(
            &ColumnConversion {
                source: metadata.clone(),
                source_type: CrossDialectSourceType::MySql(MySqlTargetType::Boolean),
                target: metadata,
                target_type: CrossDialectTargetType::PostgreSql(PostgresTargetType::Boolean),
                target_checks: Vec::new(),
                rule,
            },
            &value,
        )
    }

    fn convert_one(rule: ValueConversionRule, value: DbValue) -> DbValue {
        convert_one_result(rule, value).unwrap()
    }

    fn table(namespace: &str, name: &str) -> QualifiedTable {
        QualifiedTable {
            namespace: Identifier::new(namespace).unwrap(),
            name: Identifier::new(name).unwrap(),
        }
    }

    fn primary_key(row_policy: &RowConversionPolicy) -> CrossDialectResumableKey {
        CrossDialectResumableKey {
            source_name: identifier("PRIMARY"),
            source_kind: CrossDialectKeyKind::PrimaryKey,
            source_columns: vec![row_policy.columns[0].source.name.clone()],
            target_name: None,
            target_kind: CrossDialectKeyKind::PrimaryKey,
            target_columns: vec![row_policy.columns[0].target.name.clone()],
        }
    }

    fn identified(mut metadata: ColumnMeta, name: String, ordinal: u32) -> ColumnMeta {
        metadata.name = Identifier::new(name).unwrap();
        metadata.ordinal = ordinal;
        metadata
    }

    #[test]
    fn postgres_to_mysql_derivation_binds_typed_source_target_and_rule() {
        let source_types = [
            PostgresTargetType::Boolean,
            PostgresTargetType::SmallInt,
            PostgresTargetType::Integer,
            PostgresTargetType::BigInt,
            PostgresTargetType::Numeric {
                precision: 20,
                scale: 4,
            },
            PostgresTargetType::Real,
            PostgresTargetType::DoublePrecision,
            PostgresTargetType::Text {
                collation: postgres_collation(),
            },
            PostgresTargetType::Bytea,
            PostgresTargetType::Jsonb,
            PostgresTargetType::Date {
                minimum_year: -4_712,
                maximum_year: 5_874_897,
            },
            PostgresTargetType::Time {
                precision: 6,
                with_time_zone: false,
                minimum_nanos: POSTGRES_TIME_MINIMUM_NANOS,
                maximum_nanos: POSTGRES_TIME_MAXIMUM_NANOS,
            },
            PostgresTargetType::Timestamp {
                precision: 6,
                semantics: TimestampSemantics::WallClock,
                range: POSTGRES_TIMESTAMP_RANGE,
            },
        ];
        for (index, source_type) in source_types.into_iter().enumerate() {
            let ordinal = u32::try_from(index + 1).unwrap();
            let source = identified(
                source_type.column_meta().unwrap(),
                format!("source_{index}"),
                ordinal,
            );
            let conversion = derive_postgres_to_mysql_column(
                source.clone(),
                source_type.clone(),
                Identifier::new(format!("target_{index}")).unwrap(),
                ordinal,
                identifier("utf8mb4"),
                identifier("utf8mb4_0900_bin"),
            )
            .unwrap();
            assert_eq!(
                conversion.source_type,
                CrossDialectSourceType::PostgreSql(source_type)
            );
            assert_eq!(conversion.source, source);
            assert_eq!(conversion.target_type.dialect(), ConversionDialect::MySql);
        }
    }

    #[test]
    fn mysql_unsigned_derivation_widens_without_wrapping() {
        let cases = [
            (
                MySqlIntegerWidth::Tiny,
                PostgresTargetType::SmallInt,
                ValueConversionRule::UnsignedToSigned { maximum: 255 },
            ),
            (
                MySqlIntegerWidth::Small,
                PostgresTargetType::Integer,
                ValueConversionRule::UnsignedToSigned { maximum: 65_535 },
            ),
            (
                MySqlIntegerWidth::Integer,
                PostgresTargetType::BigInt,
                ValueConversionRule::UnsignedToSigned {
                    maximum: 4_294_967_295,
                },
            ),
            (
                MySqlIntegerWidth::Big,
                PostgresTargetType::Numeric {
                    precision: 20,
                    scale: 0,
                },
                ValueConversionRule::UnsignedIntegerToDecimal {
                    maximum: u64::MAX,
                    precision: 20,
                },
            ),
        ];
        for (index, (width, expected_target, expected_rule)) in cases.into_iter().enumerate() {
            let source_type = MySqlTargetType::Integer {
                width,
                unsigned: true,
            };
            let source = identified(
                source_type.column_meta().unwrap(),
                format!("source_{index}"),
                u32::try_from(index + 1).unwrap(),
            );
            let conversion = derive_mysql_to_postgres_column(
                source,
                source_type.clone(),
                Identifier::new(format!("target_{index}")).unwrap(),
                u32::try_from(index + 1).unwrap(),
                postgres_collation(),
            )
            .unwrap();
            assert_eq!(
                conversion.source_type,
                CrossDialectSourceType::MySql(source_type)
            );
            assert_eq!(
                conversion.target_type,
                CrossDialectTargetType::PostgreSql(expected_target)
            );
            assert_eq!(conversion.rule, expected_rule);
            if width == MySqlIntegerWidth::Big {
                assert_eq!(
                    convert_value(&conversion, &DbValue::Unsigned(u128::from(u64::MAX))).unwrap(),
                    DbValue::Decimal {
                        coefficient: b"18446744073709551615".to_vec(),
                        scale: 0,
                    }
                );
            }
        }
    }

    #[test]
    fn fixed_width_character_sources_use_non_padding_targets() {
        let postgres_source = PostgresTargetType::Character {
            length: 8,
            collation: postgres_collation(),
        };
        let source = identified(postgres_source.column_meta().unwrap(), "code".into(), 1);
        let conversion = derive_postgres_to_mysql_column(
            source,
            postgres_source,
            identifier("code"),
            1,
            identifier("utf8mb4"),
            identifier("utf8mb4_0900_bin"),
        )
        .unwrap();
        assert!(matches!(
            conversion.target_type,
            CrossDialectTargetType::MySql(MySqlTargetType::Text {
                storage: MySqlTextStorage::VarChar { length: 8 },
                ..
            })
        ));

        let mysql_source = MySqlTargetType::Text {
            storage: MySqlTextStorage::Char { length: 8 },
            character_set: identifier("utf8mb4"),
            collation: identifier("utf8mb4_0900_bin"),
        };
        let source = identified(mysql_source.column_meta().unwrap(), "code".into(), 1);
        let conversion = derive_mysql_to_postgres_column(
            source,
            mysql_source,
            identifier("code"),
            1,
            postgres_collation(),
        )
        .unwrap();
        assert!(matches!(
            conversion.target_type,
            CrossDialectTargetType::PostgreSql(PostgresTargetType::CharacterVarying {
                length: 8,
                ..
            })
        ));
    }

    #[test]
    fn utf8mb4_varchar_rejects_a_declared_width_that_mysql_cannot_create() {
        let accepted = MySqlTargetType::Text {
            storage: MySqlTextStorage::VarChar {
                length: MYSQL_UTF8MB4_VARCHAR_MAX_CHARACTERS,
            },
            character_set: identifier("utf8mb4"),
            collation: identifier("utf8mb4_0900_bin"),
        };
        assert!(accepted.ddl_type().is_ok());

        let rejected = MySqlTargetType::Text {
            storage: MySqlTextStorage::VarChar {
                length: MYSQL_UTF8MB4_VARCHAR_MAX_CHARACTERS + 1,
            },
            character_set: identifier("utf8mb4"),
            collation: identifier("utf8mb4_0900_bin"),
        };
        assert!(matches!(
            rejected.ddl_type(),
            Err(RowConversionError::InvalidTargetType)
        ));
    }

    #[test]
    fn derivation_rejects_unbounded_or_unmodeled_source_types() {
        let source = ColumnMeta {
            name: Identifier::new("amount").unwrap(),
            ordinal: 1,
            vendor_type: "pg_catalog.numeric".into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        };
        assert!(matches!(
            derive_postgres_to_mysql_column(
                source,
                PostgresTargetType::Numeric {
                    precision: 20,
                    scale: 2,
                },
                Identifier::new("amount").unwrap(),
                1,
                identifier("utf8mb4"),
                identifier("utf8mb4_0900_bin"),
            ),
            Err(RowConversionError::SourceTypeMetadataMismatch { .. })
        ));
        let unsupported = ColumnMeta {
            name: Identifier::new("value").unwrap(),
            ordinal: 1,
            vendor_type: "pg_catalog.uuid".into(),
            nullable: false,
            collation: None,
            precision: None,
            scale: None,
            timezone_semantics: None,
        };
        assert!(matches!(
            derive_postgres_to_mysql_column(
                unsupported,
                PostgresTargetType::BigInt,
                Identifier::new("value").unwrap(),
                1,
                identifier("utf8mb4"),
                identifier("utf8mb4_0900_bin"),
            ),
            Err(RowConversionError::SourceTypeMetadataMismatch { .. })
        ));
    }

    #[test]
    fn postgres_json_text_is_not_approved_as_mysql_binary_json() {
        let source_type = PostgresTargetType::Json;
        let source = identified(source_type.column_meta().unwrap(), "payload".into(), 1);
        assert!(matches!(
            derive_postgres_to_mysql_column(
                source,
                source_type,
                identifier("payload"),
                1,
                identifier("utf8mb4"),
                identifier("utf8mb4_0900_bin"),
            ),
            Err(RowConversionError::UnsupportedSourceType { .. })
        ));
    }

    #[test]
    fn source_rule_target_and_checks_validate_as_one_contract() {
        let source_type = MySqlTargetType::Integer {
            width: MySqlIntegerWidth::Big,
            unsigned: true,
        };
        let source = identified(source_type.column_meta().unwrap(), "id".into(), 1);
        let conversion = derive_mysql_to_postgres_column(
            source,
            source_type,
            identifier("id"),
            1,
            postgres_collation(),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<ColumnConversion>(&serde_json::to_vec(&conversion).unwrap())
                .unwrap(),
            conversion
        );

        let mut missing_check = conversion.clone();
        missing_check.target_checks.clear();
        assert!(matches!(
            validate_rule_metadata(&missing_check),
            Err(RowConversionError::IncoherentTypeRule)
        ));

        let mut wrong_json = policy(vec![ValueConversionRule::JsonCanonicalV1 {
            policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
        }]);
        wrong_json.columns[0].rule = ValueConversionRule::JsonCanonicalV1 {
            policy: JsonConversionPolicy::PostgreSqlJsonbToMySqlBinaryV1,
        };
        assert!(matches!(
            wrong_json.validate(),
            Err(RowConversionError::IncoherentTypeRule)
        ));

        let invalid_mysql_type = MySqlTargetType::Decimal {
            precision: 65,
            scale: 31,
            unsigned: false,
        };
        assert!(matches!(
            invalid_mysql_type.column_meta(),
            Err(RowConversionError::InvalidTargetType)
        ));
    }

    #[test]
    fn timestamp_ranges_are_narrowed_without_expanding_the_minimum() {
        let narrowed = TimestampRange {
            minimum: TimestampBound {
                year: 2026,
                month: 12,
                day: 31,
                hour: 23,
                minute: 59,
                second: 59,
                nanos: 1,
            },
            maximum: TimestampBound {
                year: 2027,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 1,
                nanos: 999_999_999,
            },
        }
        .at_precision(0)
        .unwrap();
        assert_eq!(
            narrowed.minimum,
            TimestampBound {
                year: 2027,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
                nanos: 0,
            }
        );
        assert_eq!(narrowed.maximum.second, 1);
        assert_eq!(narrowed.maximum.nanos, 0);
    }

    #[test]
    fn mysql_timestamp_range_and_utc_semantics_are_enforced() {
        let source_type = MySqlTargetType::Timestamp {
            precision: 6,
            semantics: TimestampSemantics::UtcNormalized,
            range: MYSQL_TIMESTAMP_RANGE,
        };
        let source = identified(source_type.column_meta().unwrap(), "created_at".into(), 1);
        let conversion = derive_mysql_to_postgres_column(
            source.clone(),
            source_type,
            identifier("created_at"),
            1,
            postgres_collation(),
        )
        .unwrap();
        let converter = ReviewedRowTypeConverter::new(RowConversionPolicy {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            source_dialect: ConversionDialect::MySql,
            target_dialect: ConversionDialect::PostgreSql,
            columns: vec![conversion],
        })
        .unwrap();
        let maximum = converter
            .convert_row(
                &[source.clone()],
                &[DbValue::Timestamp {
                    local: "2038-01-19 03:14:07.999999".into(),
                    offset_minutes: None,
                    precision: 6,
                }],
            )
            .unwrap();
        assert_eq!(
            maximum.values,
            [DbValue::Timestamp {
                local: "2038-01-19 03:14:07.999999".into(),
                offset_minutes: Some(0),
                precision: 6,
            }]
        );
        assert!(converter
            .convert_row(
                &[source],
                &[DbValue::Timestamp {
                    local: "2038-01-19 03:14:08.000000".into(),
                    offset_minutes: None,
                    precision: 6,
                }],
            )
            .is_err());
    }

    #[test]
    fn migration_policy_is_typed_ordered_and_dialect_bound() {
        let first_row_policy = policy(vec![ValueConversionRule::SignedInteger {
            minimum: i64::MIN,
            maximum: i64::MAX,
        }]);
        let first = TableConversionPolicy {
            source_table: table("source", "a"),
            target_table: table("target", "a"),
            target_contract: CrossDialectTargetTableContract::PostgreSql {
                persistence: PostgresTargetPersistence::Permanent,
            },
            resumable_key: primary_key(&first_row_policy),
            row_policy: first_row_policy,
        };
        let second_row_policy = policy(vec![ValueConversionRule::BytesExact]);
        let second = TableConversionPolicy {
            source_table: table("source", "b"),
            target_table: table("target", "b"),
            target_contract: CrossDialectTargetTableContract::PostgreSql {
                persistence: PostgresTargetPersistence::Permanent,
            },
            resumable_key: primary_key(&second_row_policy),
            row_policy: second_row_policy,
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

        let bytes_row_policy = policy(vec![ValueConversionRule::BytesExact]);
        let mut invalid_key_table = TableConversionPolicy {
            source_table: table("source", "key"),
            target_table: table("target", "key"),
            target_contract: CrossDialectTargetTableContract::PostgreSql {
                persistence: PostgresTargetPersistence::Permanent,
            },
            resumable_key: primary_key(&bytes_row_policy),
            row_policy: bytes_row_policy,
        };
        invalid_key_table.resumable_key.target_columns[0] = identifier("missing");
        assert!(matches!(
            invalid_key_table.validate(),
            Err(RowConversionError::InvalidTargetKey)
        ));

        let text_row_policy = policy(vec![ValueConversionRule::TextUtf8Exact]);
        let text_key_table = TableConversionPolicy {
            source_table: table("source", "text_key"),
            target_table: table("target", "text_key"),
            target_contract: CrossDialectTargetTableContract::PostgreSql {
                persistence: PostgresTargetPersistence::Permanent,
            },
            resumable_key: primary_key(&text_row_policy),
            row_policy: text_row_policy,
        };
        assert!(matches!(
            text_key_table.validate(),
            Err(RowConversionError::InvalidTargetKey)
        ));
    }

    #[test]
    fn table_policy_converts_batches_and_source_cursors_through_one_rule() {
        let row_policy = policy(vec![ValueConversionRule::UnsignedIntegerToDecimal {
            maximum: u64::MAX,
            precision: 20,
        }]);
        let table_policy = TableConversionPolicy {
            source_table: table("source", "items"),
            target_table: table("target", "items"),
            target_contract: CrossDialectTargetTableContract::PostgreSql {
                persistence: PostgresTargetPersistence::Permanent,
            },
            resumable_key: primary_key(&row_policy),
            row_policy: row_policy.clone(),
        };
        let mut source = RowBatch::new(
            row_policy
                .columns
                .iter()
                .map(|column| column.source.clone())
                .collect(),
            1,
            1024,
        );
        source
            .try_push(vec![DbValue::Unsigned(u128::from(u64::MAX))], 16)
            .unwrap();

        let converted = table_policy.convert_batch(&source).unwrap();
        let expected = DbValue::Decimal {
            coefficient: b"18446744073709551615".to_vec(),
            scale: 0,
        };
        assert_eq!(converted.columns(), [row_policy.columns[0].target.clone()]);
        assert_eq!(converted.rows(), &[vec![expected.clone()]]);
        assert_eq!(
            table_policy
                .convert_source_key(&[DbValue::Unsigned(u128::from(u64::MAX))])
                .unwrap(),
            [expected]
        );
        assert!(matches!(
            table_policy.convert_source_key(&[]),
            Err(RowConversionError::InvalidSourceKey)
        ));
    }

    #[test]
    fn integer_and_boolean_boundaries_are_checked_without_coercion() {
        assert_eq!(
            convert_one(
                ValueConversionRule::SignedZeroOneToBoolean,
                DbValue::Signed(1)
            ),
            DbValue::Bool(true)
        );
        assert_eq!(
            convert_one(
                ValueConversionRule::SignedToUnsigned { maximum: 255 },
                DbValue::Signed(255)
            ),
            DbValue::Unsigned(255)
        );
        assert_eq!(
            convert_one(
                ValueConversionRule::UnsignedToSigned {
                    maximum: i64::from(i16::MAX),
                },
                DbValue::Unsigned(32_767)
            ),
            DbValue::Signed(32_767)
        );
        assert!(convert_one_result(
            ValueConversionRule::SignedZeroOneToBoolean,
            DbValue::Signed(2)
        )
        .is_err());
        assert!(convert_one_result(
            ValueConversionRule::SignedToUnsigned { maximum: 255 },
            DbValue::Signed(-1)
        )
        .is_err());
        assert!(convert_one_result(
            ValueConversionRule::UnsignedToSigned {
                maximum: i64::from(i16::MAX),
            },
            DbValue::Unsigned(32_768)
        )
        .is_err());
    }

    #[test]
    fn json_is_canonicalized_and_duplicate_keys_fail() {
        let policy = policy(vec![ValueConversionRule::JsonCanonicalV1 {
            policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
        }]);
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
                ValueConversionRule::JsonCanonicalV1 {
                    policy: JsonConversionPolicy::MySqlBinaryToPostgreSqlJsonbV1,
                },
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
                source_semantics: TimestampSemantics::UtcNormalized,
                target_semantics: TimestampSemantics::OffsetAware,
                conversion: TimestampConversionPolicy::UtcNormalizedToOffsetAware,
                accepted_range: MYSQL_TIMESTAMP_RANGE,
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
                        offset_minutes: None,
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

        let source_type = MySqlTargetType::Integer {
            width: MySqlIntegerWidth::Tiny,
            unsigned: true,
        };
        let source = identified(source_type.column_meta().unwrap(), "value".into(), 1);
        let mut invalid = RowConversionPolicy {
            schema_version: ROW_TYPE_CONVERSION_SCHEMA_VERSION,
            source_dialect: ConversionDialect::MySql,
            target_dialect: ConversionDialect::PostgreSql,
            columns: vec![derive_mysql_to_postgres_column(
                source,
                source_type,
                identifier("value"),
                1,
                postgres_collation(),
            )
            .unwrap()],
        };
        invalid.columns[0].rule = ValueConversionRule::UnsignedToSigned { maximum: -1 };
        assert!(matches!(
            invalid.validate(),
            Err(RowConversionError::InvalidRule)
        ));
    }

    #[test]
    fn metadata_nullability_decimal_and_timestamp_contracts_fail_closed() {
        let mut nullable = policy(vec![ValueConversionRule::TextUtf8Exact]);
        nullable.columns[0].source.nullable = true;
        assert!(matches!(
            nullable.validate(),
            Err(RowConversionError::NullabilityMismatch { .. })
        ));
        let mut relaxed = policy(vec![ValueConversionRule::TextUtf8Exact]);
        relaxed.columns[0].target.nullable = true;
        assert!(matches!(
            relaxed.validate(),
            Err(RowConversionError::NullabilityMismatch { .. })
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

        let mut timestamp = policy(vec![ValueConversionRule::TimestampExact {
            precision: 6,
            source_semantics: TimestampSemantics::UtcNormalized,
            target_semantics: TimestampSemantics::OffsetAware,
            conversion: TimestampConversionPolicy::UtcNormalizedToOffsetAware,
            accepted_range: MYSQL_TIMESTAMP_RANGE,
        }]);
        timestamp.columns[0].source.precision = None;
        assert!(matches!(
            timestamp.validate(),
            Err(RowConversionError::SourceTypeMetadataMismatch { .. })
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
        assert!(matches!(
            ordinal_gap.validate(),
            Err(RowConversionError::InvalidColumnOrder)
        ));
        ordinal_gap.columns[1].source.ordinal = 1;
        assert!(matches!(
            ordinal_gap.validate(),
            Err(RowConversionError::InvalidColumnOrder)
        ));
    }
}
