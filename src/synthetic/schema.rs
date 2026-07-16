//! Dialect-agnostic, serializable schema types for synthetic data generation.

use crate::parser::SqlDialect;
use crate::schema::{Column, Schema, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Coarse-grained classification of a SQL column type, independent of the
/// source dialect's exact type name. Generation strategies key off this
/// instead of re-deriving it from `source_type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlTypeFamily {
    Integer,
    BigInteger,
    Decimal,
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
            ColumnType::Decimal => SqlTypeFamily::Decimal,
            ColumnType::Bool => SqlTypeFamily::Boolean,
            ColumnType::Text => SqlTypeFamily::Text,
            ColumnType::Uuid => SqlTypeFamily::Uuid,
            ColumnType::DateTime => SqlTypeFamily::DateTime,
            ColumnType::Other(_) => {
                let lower = column.source_type.to_lowercase();
                if lower.contains("json") {
                    SqlTypeFamily::Json
                } else if lower.contains("blob")
                    || lower.contains("binary")
                    || lower.contains("bytea")
                {
                    SqlTypeFamily::Bytes
                } else {
                    SqlTypeFamily::Other
                }
            }
        }
    }
}

/// Portable column: everything a generation strategy needs to know about a
/// single column, independent of the source SQL dialect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// A table-level UNIQUE constraint, covering one or more columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableUniqueConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub columns: Vec<String>,
}

/// A CHECK constraint, with its raw SQL expression preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableCheckConstraint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub expression: String,
}

/// An index definition (not necessarily unique; see
/// [`PortableUniqueConstraint`] for UNIQUE constraints).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
