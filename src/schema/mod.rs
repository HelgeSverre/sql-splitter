//! Schema analysis module for FK-aware operations.
//!
//! This module provides:
//! - Data models for table schemas, columns, and foreign keys
//! - MySQL DDL parsing for extracting schema information
//! - Dependency graph construction with topological sorting
//! - Cycle detection for handling circular FK relationships

mod ddl;
mod graph;

pub use ddl::*;
pub use graph::*;

use ahash::AHashMap;
use std::fmt;

/// Unique identifier for a table within a schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TableId({})", self.0)
    }
}

/// Unique identifier for a column within a table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub u16);

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnId({})", self.0)
    }
}

/// SQL column type classification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    /// Integer types: INT, INTEGER, TINYINT, SMALLINT, MEDIUMINT
    Int,
    /// Big integer types: BIGINT
    BigInt,
    /// Text types: CHAR, VARCHAR, TEXT, etc.
    Text,
    /// UUID types (detected by column name or type)
    Uuid,
    /// Decimal/numeric types
    Decimal,
    /// Date/time types
    DateTime,
    /// Boolean type
    Bool,
    /// Any other type
    Other(String),
}

impl ColumnType {
    /// Parse a SQL type string into a ColumnType
    /// Supports MySQL, PostgreSQL, and SQLite types
    pub fn from_sql_type(type_str: &str) -> Self {
        let type_lower = type_str.to_lowercase();
        let base_type = type_lower.split('(').next().unwrap_or(&type_lower).trim();

        match base_type {
            // Integer types (all dialects)
            "int" | "integer" | "tinyint" | "smallint" | "mediumint" | "int4" | "int2" => {
                ColumnType::Int
            }
            // Auto-increment integer types (PostgreSQL)
            "serial" | "smallserial" => ColumnType::Int,
            "bigint" | "int8" | "bigserial" => ColumnType::BigInt,
            // Text types (all dialects)
            "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum"
            | "set" | "character" => ColumnType::Text,
            // Decimal types (all dialects)
            "decimal" | "numeric" | "float" | "double" | "real" | "float4" | "float8" | "money" => {
                ColumnType::Decimal
            }
            // Date/time types (all dialects)
            "date" | "datetime" | "timestamp" | "time" | "year" | "timestamptz" | "timetz"
            | "interval" => ColumnType::DateTime,
            // Boolean (all dialects)
            "bool" | "boolean" => ColumnType::Bool,
            // Binary types
            "binary" | "varbinary" | "blob" | "bytea" => {
                // Could be UUID if binary(16)
                if type_lower.contains("16") {
                    ColumnType::Uuid
                } else {
                    ColumnType::Other(type_str.to_string())
                }
            }
            "uuid" => ColumnType::Uuid,
            _ => ColumnType::Other(type_str.to_string()),
        }
    }

    /// Parse a MySQL type string into a ColumnType (alias for from_sql_type)
    pub fn from_mysql_type(type_str: &str) -> Self {
        Self::from_sql_type(type_str)
    }
}

/// Column definition within a table
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column type
    pub col_type: ColumnType,
    /// Position in table (0-indexed)
    pub ordinal: ColumnId,
    /// Whether this column is part of the primary key
    pub is_primary_key: bool,
    /// Whether this column allows NULL values
    pub is_nullable: bool,
}

/// Index definition
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Index type (BTREE, HASH, GIN, etc.)
    pub index_type: Option<String>,
}

/// Foreign key constraint definition
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Constraint name (optional)
    pub name: Option<String>,
    /// Column IDs in this table that form the FK
    pub columns: Vec<ColumnId>,
    /// Column names in this table (before resolution)
    pub column_names: Vec<String>,
    /// Referenced table name
    pub referenced_table: String,
    /// Referenced column names
    pub referenced_columns: Vec<String>,
    /// Resolved referenced table ID (set after schema is complete)
    pub referenced_table_id: Option<TableId>,
}

/// Complete table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Table ID within the schema
    pub id: TableId,
    /// Column definitions in order
    pub columns: Vec<Column>,
    /// Primary key column IDs (ordered for composite PKs)
    pub primary_key: Vec<ColumnId>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKey>,
    /// Index definitions
    pub indexes: Vec<IndexDef>,
    /// Raw CREATE TABLE statement (for output)
    pub create_statement: Option<String>,
}

impl TableSchema {
    /// Create a new empty table schema
    pub fn new(name: String, id: TableId) -> Self {
        Self {
            name,
            id,
            columns: Vec::new(),
            primary_key: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            create_statement: None,
        }
    }

    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get column ID by name
    pub fn get_column_id(&self, name: &str) -> Option<ColumnId> {
        self.get_column(name).map(|c| c.ordinal)
    }

    /// Get column by ID
    pub fn column(&self, id: ColumnId) -> Option<&Column> {
        self.columns.get(id.0 as usize)
    }

    /// Check if column is part of the primary key
    pub fn is_pk_column(&self, col_id: ColumnId) -> bool {
        self.primary_key.contains(&col_id)
    }

    /// Get all FK column IDs (columns that reference other tables)
    pub fn fk_column_ids(&self) -> Vec<ColumnId> {
        self.foreign_keys
            .iter()
            .flat_map(|fk| fk.columns.iter().copied())
            .collect()
    }
}

/// Complete database schema
#[derive(Debug)]
pub struct Schema {
    /// Map from table name to table ID
    pub tables: AHashMap<String, TableId>,
    /// Table schemas indexed by TableId
    pub table_schemas: Vec<TableSchema>,
}

impl Schema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            tables: AHashMap::new(),
            table_schemas: Vec::new(),
        }
    }

    /// Get table ID by name (case-insensitive)
    pub fn get_table_id(&self, name: &str) -> Option<TableId> {
        // Try exact match first
        if let Some(&id) = self.tables.get(name) {
            return Some(id);
        }
        // Try case-insensitive match
        let name_lower = name.to_lowercase();
        self.tables
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, &id)| id)
    }

    /// Get table schema by ID
    pub fn table(&self, id: TableId) -> Option<&TableSchema> {
        self.table_schemas.get(id.0 as usize)
    }

    /// Get mutable table schema by ID
    pub fn table_mut(&mut self, id: TableId) -> Option<&mut TableSchema> {
        self.table_schemas.get_mut(id.0 as usize)
    }

    /// Get table schema by name
    pub fn get_table(&self, name: &str) -> Option<&TableSchema> {
        self.get_table_id(name).and_then(|id| self.table(id))
    }

    /// Add a new table schema, returning its ID
    pub fn add_table(&mut self, mut schema: TableSchema) -> TableId {
        let id = TableId(self.table_schemas.len() as u32);
        schema.id = id;
        self.tables.insert(schema.name.clone(), id);
        self.table_schemas.push(schema);
        id
    }

    /// Resolve all foreign key references to table IDs
    pub fn resolve_foreign_keys(&mut self) {
        let table_ids: AHashMap<String, TableId> = self.tables.clone();

        for table in &mut self.table_schemas {
            for fk in &mut table.foreign_keys {
                fk.referenced_table_id = table_ids
                    .get(&fk.referenced_table)
                    .or_else(|| {
                        // Case-insensitive fallback
                        let lower = fk.referenced_table.to_lowercase();
                        table_ids
                            .iter()
                            .find(|(k, _)| k.to_lowercase() == lower)
                            .map(|(_, v)| v)
                    })
                    .copied();
            }
        }
    }

    /// Get the number of tables
    pub fn len(&self) -> usize {
        self.table_schemas.len()
    }

    /// Check if schema is empty
    pub fn is_empty(&self) -> bool {
        self.table_schemas.is_empty()
    }

    /// Iterate over all table schemas
    pub fn iter(&self) -> impl Iterator<Item = &TableSchema> {
        self.table_schemas.iter()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}
