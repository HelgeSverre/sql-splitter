//! Dialect-agnostic, serializable schema types for synthetic data generation.

use crate::parser::SqlDialect;
use crate::schema::{Column, Schema, TableSchema};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Largest fixed-point scale supported by the built-in decimal generator.
pub(crate) const MAX_GENERATED_DECIMAL_SCALE: u32 = 18;

/// Return the declared character limit of a bounded SQL text type.
///
/// This recognizes the common MySQL, PostgreSQL, SQLite, and MSSQL spellings.
/// Length-less text types and non-text precision declarations return `None`.
pub(crate) fn declared_character_length(source_type: &str) -> Option<usize> {
    let open = source_type.find('(')?;
    let close = source_type[open + 1..].find(')')? + open + 1;
    let type_name = source_type[..open]
        .trim()
        .trim_matches(['[', ']'])
        .to_ascii_lowercase();
    if !matches!(
        type_name.as_str(),
        "char"
            | "varchar"
            | "nchar"
            | "nvarchar"
            | "character"
            | "character varying"
            | "national char"
            | "national character"
            | "national character varying"
            | "bpchar"
    ) {
        return None;
    }
    source_type[open + 1..close].trim().parse().ok()
}

/// Return the exact value range of a fixed-width SQL integer type.
///
/// Bare `tinyint` requires a dialect because MySQL defines it as signed while
/// MSSQL defines it as unsigned. Explicit `unsigned` declarations are
/// unambiguous.
pub(crate) fn declared_integer_bounds(
    source_type: &str,
    dialect: Option<SqlDialect>,
) -> Option<(i128, i128)> {
    let lower = source_type.trim().to_ascii_lowercase();
    let type_name = lower.split(['(', ' ', '[']).find(|part| !part.is_empty())?;
    let unsigned = lower.split_whitespace().any(|part| part == "unsigned");
    let signed = match type_name {
        "tinyint" if unsigned => (i8::MIN as i128, i8::MAX as i128, u8::MAX as i128),
        "tinyint" => match dialect {
            Some(SqlDialect::MySql) => (i8::MIN as i128, i8::MAX as i128, u8::MAX as i128),
            Some(SqlDialect::Mssql) => (0, u8::MAX as i128, u8::MAX as i128),
            Some(SqlDialect::Postgres | SqlDialect::Sqlite) | None => return None,
        },
        "smallint" | "int2" | "smallserial" => {
            (i16::MIN as i128, i16::MAX as i128, u16::MAX as i128)
        }
        "mediumint" => (-8_388_608, 8_388_607, 16_777_215),
        "int" | "integer" | "int4" | "serial" => {
            (i32::MIN as i128, i32::MAX as i128, u32::MAX as i128)
        }
        "bigint" | "int8" | "bigserial" => (i64::MIN as i128, i64::MAX as i128, u64::MAX as i128),
        _ => return None,
    };
    Some(if unsigned {
        (0, signed.2)
    } else {
        (signed.0, signed.1)
    })
}

/// Return a range that is safe to generate for every dialect that uses the
/// declared fixed-width integer spelling.
pub(crate) fn conservative_integer_generation_bounds(source_type: &str) -> Option<(i128, i128)> {
    declared_integer_bounds(source_type, None).or_else(|| {
        let lower = source_type.trim().to_ascii_lowercase();
        let type_name = lower.split(['(', ' ', '[']).find(|part| !part.is_empty())?;
        (type_name == "tinyint").then_some((0, i8::MAX as i128))
    })
}

/// Return `(precision, scale)` for a bounded fixed-point SQL decimal type.
pub(crate) fn declared_decimal_shape(source_type: &str) -> Option<(u32, u32)> {
    let open = source_type.find('(')?;
    let close = source_type[open + 1..].find(')')? + open + 1;
    let type_name = source_type[..open].trim().to_ascii_lowercase();
    if !matches!(type_name.as_str(), "decimal" | "numeric" | "dec" | "fixed") {
        return None;
    }
    let mut parts = source_type[open + 1..close].split(',').map(str::trim);
    let precision = parts.next()?.parse().ok()?;
    let scale = parts.next().unwrap_or("0").parse().ok()?;
    (scale <= precision).then_some((precision, scale))
}

/// Coarse-grained classification of a SQL column type, independent of the
/// source dialect's exact type name. Generation strategies key off this
/// instead of re-deriving it from `source_type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SqlTypeFamily {
    Integer,
    BigInteger,
    Decimal,
    Enum,
    Boolean,
    Text,
    Bytes,
    Uuid,
    DateTime,
    Json,
    Other,
}

impl SqlTypeFamily {
    /// Classify a runtime [`crate::schema::ColumnType`], falling back to
    /// sniffing `source_type` for the dialect-specific types (JSON, binary)
    /// that `ColumnType` collapses into `Other`.
    fn from_column(column: &Column) -> Self {
        use crate::schema::ColumnType;

        match &column.col_type {
            ColumnType::Int => SqlTypeFamily::Integer,
            ColumnType::BigInt => SqlTypeFamily::BigInteger,
            ColumnType::Enum(_) => SqlTypeFamily::Enum,
            ColumnType::Decimal => SqlTypeFamily::Decimal,
            ColumnType::Bool => SqlTypeFamily::Boolean,
            ColumnType::Text => SqlTypeFamily::Text,
            ColumnType::Uuid => SqlTypeFamily::Uuid,
            ColumnType::DateTime => SqlTypeFamily::DateTime,
            ColumnType::Other(_) => Self::from_unclassified_source_type(&column.source_type),
        }
    }

    /// Classify a raw SQL type name (e.g. `"bigint"`, `"varchar(255)"`)
    /// using the same coarse mapping as [`Self::from_column`], for callers
    /// that only have a type name and not a fully parsed [`Column`] — namely
    /// the hand-authored `type:` shorthand accepted by [`PortableColumn`].
    fn from_source_type(source_type: &str) -> Self {
        use crate::schema::ColumnType;

        match ColumnType::from_sql_type(source_type) {
            ColumnType::Int => SqlTypeFamily::Integer,
            ColumnType::BigInt => SqlTypeFamily::BigInteger,
            ColumnType::Enum(_) => SqlTypeFamily::Enum,
            ColumnType::Decimal => SqlTypeFamily::Decimal,
            ColumnType::Bool => SqlTypeFamily::Boolean,
            ColumnType::Text => SqlTypeFamily::Text,
            ColumnType::Uuid => SqlTypeFamily::Uuid,
            ColumnType::DateTime => SqlTypeFamily::DateTime,
            ColumnType::Other(_) => Self::from_unclassified_source_type(source_type),
        }
    }

    /// Shared JSON/blob sniffing for the `ColumnType::Other` fallback,
    /// used by both [`Self::from_column`] and [`Self::from_source_type`].
    fn from_unclassified_source_type(source_type: &str) -> Self {
        let lower = source_type.to_lowercase();
        if lower.contains("json") {
            SqlTypeFamily::Json
        } else if lower.contains("blob") || lower.contains("binary") || lower.contains("bytea") {
            SqlTypeFamily::Bytes
        } else {
            SqlTypeFamily::Other
        }
    }
}

/// Portable column: everything a generation strategy needs to know about a
/// single column, independent of the source SQL dialect.
///
/// Deserialization accepts a concise hand-authored shorthand: `type:` as an
/// alias for `source_type`, with `family` derived automatically when it is
/// absent (see [`PortableColumnInput`]). Serialization always emits the
/// canonical `source_type` + `family` fields; `--emit-config` never writes
/// the `type:` shorthand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PortableColumnInput")]
pub struct PortableColumn {
    pub name: String,
    pub source_type: String,
    pub family: SqlTypeFamily,
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_sql: Option<String>,
    #[serde(default)]
    pub generated: bool,
    #[serde(default)]
    pub identity: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

/// Manual schema: `#[serde(try_from = "PortableColumnInput")]` means the
/// derive macro would describe `PortableColumn`'s own fields (which always
/// require `source_type` + `family`), not what YAML actually accepts. This
/// impl instead mirrors [`PortableColumnInput`]: `family` is optional, and
/// either `source_type` or its `type` shorthand alias may supply the type
/// name (see D1 in the module tests below).
impl JsonSchema for PortableColumn {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PortableColumn".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let family_schema = generator.subschema_for::<SqlTypeFamily>();
        schemars::json_schema!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "source_type": { "type": "string" },
                "type": {
                    "type": "string",
                    "description": "Shorthand alias for `source_type`; `family` is derived automatically when omitted."
                },
                "family": family_schema,
                "nullable": { "type": "boolean" },
                "primary_key": { "type": "boolean" },
                "unique": { "type": "boolean" },
                "default_sql": { "type": ["string", "null"] },
                "generated": { "type": "boolean" },
                "identity": { "type": "boolean" },
                "collation": { "type": ["string", "null"] }
            },
            "required": ["name", "nullable"],
            "anyOf": [
                { "required": ["source_type"] },
                { "required": ["type"] }
            ],
            "additionalProperties": false
        })
    }
}

impl PortableColumn {
    fn from_runtime(column: &Column) -> Self {
        Self {
            name: column.name.clone(),
            source_type: column.source_type.clone(),
            family: SqlTypeFamily::from_column(column),
            nullable: column.is_nullable,
            primary_key: column.is_primary_key,
            unique: column.is_unique,
            default_sql: column.default_sql.clone(),
            generated: column.is_generated,
            identity: column.is_identity,
            collation: column.collation.clone(),
        }
    }
}

/// The input-only shape [`PortableColumn`] deserializes through: `type:` is
/// accepted as an alias for `source_type`, and `family` is optional, derived
/// from `source_type` via [`SqlTypeFamily::from_source_type`] when absent.
/// `#[serde(deny_unknown_fields)]` lives here, not on `PortableColumn`
/// itself, since this shadow struct is what actually parses YAML keys.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableColumnInput {
    name: String,
    #[serde(alias = "type")]
    source_type: String,
    #[serde(default)]
    family: Option<SqlTypeFamily>,
    nullable: bool,
    #[serde(default)]
    primary_key: bool,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    default_sql: Option<String>,
    #[serde(default)]
    generated: bool,
    #[serde(default)]
    identity: bool,
    #[serde(default)]
    collation: Option<String>,
}

impl From<PortableColumnInput> for PortableColumn {
    fn from(input: PortableColumnInput) -> Self {
        let family = input
            .family
            .unwrap_or_else(|| SqlTypeFamily::from_source_type(&input.source_type));
        Self {
            name: input.name,
            source_type: input.source_type,
            family,
            nullable: input.nullable,
            primary_key: input.primary_key,
            unique: input.unique,
            default_sql: input.default_sql,
            generated: input.generated,
            identity: input.identity,
            collation: input.collation,
        }
    }
}

/// A table-level UNIQUE constraint, covering one or more columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableUniqueConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub columns: Vec<String>,
}

/// A CHECK constraint, with its raw SQL expression preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableCheckConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub expression: String,
}

/// An index definition (not necessarily unique; see
/// [`PortableUniqueConstraint`] for UNIQUE constraints).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableIndex {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
}

/// A declared foreign-key relationship to another table, by name (not
/// resolved to an ID, since a `PortableSchema` stands alone from the
/// `Schema` it was built from).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableRelationship {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Portable table: ordered columns and constraints, plus the raw same-dialect
/// DDL for reference. Column and constraint order is preserved in `Vec`s;
/// only table name lookup uses a `BTreeMap` (see [`PortableSchema::tables`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableTable {
    pub name: String,
    pub columns: Vec<PortableColumn>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub unique_constraints: Vec<PortableUniqueConstraint>,
    #[serde(default)]
    pub check_constraints: Vec<PortableCheckConstraint>,
    #[serde(default)]
    pub indexes: Vec<PortableIndex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_statement: Option<String>,
    #[serde(default)]
    pub relationships: Vec<PortableRelationship>,
}

impl PortableTable {
    fn from_runtime(table: &TableSchema) -> Self {
        let columns: Vec<PortableColumn> = table
            .columns
            .iter()
            .map(PortableColumn::from_runtime)
            .collect();

        let primary_key = table
            .primary_key
            .iter()
            .filter_map(|&id| table.column(id))
            .map(|c| c.name.clone())
            .collect();

        let unique_constraints = table
            .unique_constraints
            .iter()
            .map(|uc| PortableUniqueConstraint {
                name: uc.name.clone(),
                columns: uc.columns.clone(),
            })
            .collect();

        let check_constraints = table
            .check_constraints
            .iter()
            .map(|cc| PortableCheckConstraint {
                name: cc.name.clone(),
                expression: cc.expression.clone(),
            })
            .collect();

        let indexes = table
            .indexes
            .iter()
            .map(|idx| PortableIndex {
                name: idx.name.clone(),
                columns: idx.columns.clone(),
                unique: idx.is_unique,
                index_type: idx.index_type.clone(),
            })
            .collect();

        let relationships = table
            .foreign_keys
            .iter()
            .map(|fk| PortableRelationship {
                name: fk.name.clone(),
                columns: fk.column_names.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_columns: fk.referenced_columns.clone(),
            })
            .collect();

        Self {
            name: table.name.clone(),
            columns,
            primary_key,
            unique_constraints,
            check_constraints,
            indexes,
            create_statement: table.create_statement.clone(),
            relationships,
        }
    }
}

/// Dialect-agnostic snapshot of a full database schema, suitable for
/// serialization and for driving synthetic data generation without
/// depending on the DDL parser internals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortableSchema {
    /// The dialect the source DDL was parsed with (e.g. `"mysql"`).
    pub dialect: String,
    /// Tables keyed by name for lookup; use each table's own `Vec` fields
    /// when order matters (declaration order is not implied by map order).
    pub tables: BTreeMap<String, PortableTable>,
}

impl PortableSchema {
    /// Build a portable snapshot from a parsed runtime [`Schema`].
    pub fn from_runtime(schema: &Schema, dialect: SqlDialect) -> Self {
        let tables = schema
            .iter()
            .map(|table| (table.name.clone(), PortableTable::from_runtime(table)))
            .collect();

        Self {
            dialect: dialect.to_string(),
            tables,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Design decision D1: hand-authored column entries may use a concise
    /// `type:` shorthand for `source_type`, with `family` derived
    /// automatically. `--emit-config` never emits this shorthand; see
    /// `portable_column_canonical_form_round_trips` for the emitted shape.
    #[test]
    fn portable_column_accepts_type_shorthand_and_derives_family() {
        let yaml = "{ name: id, type: bigint, nullable: false, primary_key: true }";
        let column: PortableColumn = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(column.source_type, "bigint");
        assert_eq!(column.family, SqlTypeFamily::BigInteger);
        assert!(column.primary_key);
    }

    #[test]
    fn portable_column_canonical_form_round_trips() {
        let column = PortableColumn {
            name: "id".to_string(),
            source_type: "bigint".to_string(),
            family: SqlTypeFamily::BigInteger,
            nullable: false,
            primary_key: true,
            unique: false,
            default_sql: None,
            generated: false,
            identity: false,
            collation: None,
        };

        let rendered = serde_yaml_ng::to_string(&column).unwrap();
        assert!(rendered.contains("source_type: bigint"));
        assert!(rendered.contains("family: big_integer"));
        assert!(!rendered.contains("\ntype:"));

        let reparsed: PortableColumn = serde_yaml_ng::from_str(&rendered).unwrap();
        assert_eq!(reparsed, column);
    }

    #[test]
    fn portable_column_rejects_unknown_fields_even_with_shorthand() {
        let yaml = "{ name: id, type: bigint, nullable: false, bogus: true }";
        let err = serde_yaml_ng::from_str::<PortableColumn>(yaml).unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn declared_character_length_only_accepts_bounded_text_types() {
        assert_eq!(declared_character_length("varchar(8)"), Some(8));
        assert_eq!(declared_character_length("VARCHAR(255)"), Some(255));
        assert_eq!(declared_character_length("character varying(42)"), Some(42));
        assert_eq!(declared_character_length("nvarchar(12)"), Some(12));
        assert_eq!(declared_character_length("text"), None);
        assert_eq!(declared_character_length("decimal(10,2)"), None);
        assert_eq!(declared_character_length("geometry(4326)"), None);
    }

    #[test]
    fn declared_numeric_shapes_capture_width_and_signedness() {
        assert_eq!(
            declared_integer_bounds("tinyint unsigned", None),
            Some((0, 255))
        );
        assert_eq!(
            declared_integer_bounds("tinyint", Some(SqlDialect::MySql)),
            Some((-128, 127))
        );
        assert_eq!(
            declared_integer_bounds("tinyint", Some(SqlDialect::Mssql)),
            Some((0, 255))
        );
        assert_eq!(declared_integer_bounds("tinyint", None), None);
        assert_eq!(
            conservative_integer_generation_bounds("tinyint"),
            Some((0, 127))
        );
        assert_eq!(
            declared_integer_bounds("smallint", None),
            Some((-32_768, 32_767))
        );
        assert_eq!(
            declared_integer_bounds("bigint unsigned", None),
            Some((0, u64::MAX as i128))
        );
        assert_eq!(declared_integer_bounds("decimal(10,2)", None), None);
        assert_eq!(declared_decimal_shape("decimal(10,8)"), Some((10, 8)));
        assert_eq!(declared_decimal_shape("NUMERIC(12)"), Some((12, 0)));
        assert_eq!(declared_decimal_shape("varchar(12)"), None);
    }
}
