//! Schema comparison for diff command.

use super::{parse_ignore_patterns, should_ignore_column, should_include_table, DiffConfig};
use crate::schema::{Column, ColumnType, ForeignKey, IndexDef, Schema, TableSchema};
use glob::Pattern;
use serde::Serialize;

/// Differences between two schemas
#[derive(Debug, Serialize)]
pub struct SchemaDiff {
    /// Tables that exist only in the new schema
    pub tables_added: Vec<TableInfo>,
    /// Tables that exist only in the old schema
    pub tables_removed: Vec<String>,
    /// Tables that exist in both but have differences
    pub tables_modified: Vec<TableModification>,
}

impl SchemaDiff {
    /// Check if there are any differences
    pub fn has_changes(&self) -> bool {
        !self.tables_added.is_empty()
            || !self.tables_removed.is_empty()
            || !self.tables_modified.is_empty()
    }
}

/// Basic info about a table for added tables
#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_statement: Option<String>,
}

impl From<&TableSchema> for TableInfo {
    fn from(t: &TableSchema) -> Self {
        Self {
            name: t.name.clone(),
            columns: t.columns.iter().map(ColumnInfo::from).collect(),
            primary_key: t
                .primary_key
                .iter()
                .filter_map(|id| t.column(*id).map(|c| c.name.clone()))
                .collect(),
            create_statement: t.create_statement.clone(),
        }
    }
}

/// Column info for serialization
#[derive(Debug, Serialize, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub col_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

impl From<&Column> for ColumnInfo {
    fn from(c: &Column) -> Self {
        Self {
            name: c.name.clone(),
            col_type: format_column_type(&c.col_type),
            is_nullable: c.is_nullable,
            is_primary_key: c.is_primary_key,
        }
    }
}

fn format_column_type(ct: &ColumnType) -> String {
    match ct {
        ColumnType::Int => "INT".to_string(),
        ColumnType::BigInt => "BIGINT".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Decimal => "DECIMAL".to_string(),
        ColumnType::DateTime => "DATETIME".to_string(),
        ColumnType::Bool => "BOOLEAN".to_string(),
        ColumnType::Other(s) => s.clone(),
    }
}

/// Modifications to an existing table
#[derive(Debug, Serialize)]
pub struct TableModification {
    /// Table name
    pub table_name: String,
    /// Columns added in the new schema
    pub columns_added: Vec<ColumnInfo>,
    /// Columns removed in the new schema
    pub columns_removed: Vec<ColumnInfo>,
    /// Columns with type or nullability changes
    pub columns_modified: Vec<ColumnChange>,
    /// Whether the primary key changed
    pub pk_changed: bool,
    /// Old primary key columns (if changed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_pk: Option<Vec<String>>,
    /// New primary key columns (if changed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_pk: Option<Vec<String>>,
    /// Foreign keys added
    pub fks_added: Vec<FkInfo>,
    /// Foreign keys removed
    pub fks_removed: Vec<FkInfo>,
    /// Indexes added
    pub indexes_added: Vec<IndexInfo>,
    /// Indexes removed
    pub indexes_removed: Vec<IndexInfo>,
}

impl TableModification {
    /// Check if there are any modifications
    pub fn has_changes(&self) -> bool {
        !self.columns_added.is_empty()
            || !self.columns_removed.is_empty()
            || !self.columns_modified.is_empty()
            || self.pk_changed
            || !self.fks_added.is_empty()
            || !self.fks_removed.is_empty()
            || !self.indexes_added.is_empty()
            || !self.indexes_removed.is_empty()
    }
}

/// Change to a column definition
#[derive(Debug, Serialize)]
pub struct ColumnChange {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_nullable: Option<bool>,
}

/// Foreign key info for serialization
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct FkInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

impl From<&ForeignKey> for FkInfo {
    fn from(fk: &ForeignKey) -> Self {
        Self {
            name: fk.name.clone(),
            columns: fk.column_names.clone(),
            referenced_table: fk.referenced_table.clone(),
            referenced_columns: fk.referenced_columns.clone(),
        }
    }
}

/// Index info for serialization
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_type: Option<String>,
}

impl From<&IndexDef> for IndexInfo {
    fn from(idx: &IndexDef) -> Self {
        Self {
            name: idx.name.clone(),
            columns: idx.columns.clone(),
            is_unique: idx.is_unique,
            index_type: idx.index_type.clone(),
        }
    }
}

/// Compare two schemas and return the differences
pub fn compare_schemas(
    old_schema: &Schema,
    new_schema: &Schema,
    config: &DiffConfig,
) -> SchemaDiff {
    let mut tables_added = Vec::new();
    let mut tables_removed = Vec::new();
    let mut tables_modified = Vec::new();

    // Parse ignore patterns
    let ignore_patterns = parse_ignore_patterns(&config.ignore_columns);

    // Find tables in new but not in old (added)
    for new_table in new_schema.iter() {
        if !should_include_table(&new_table.name, &config.tables, &config.exclude) {
            continue;
        }

        if old_schema.get_table(&new_table.name).is_none() {
            // Filter out ignored columns from added table info
            let mut table_info = TableInfo::from(new_table);
            if !ignore_patterns.is_empty() {
                table_info.columns.retain(|col| {
                    !should_ignore_column(&new_table.name, &col.name, &ignore_patterns)
                });
            }
            tables_added.push(table_info);
        }
    }

    // Find tables in old but not in new (removed) and tables in both (check for modifications)
    for old_table in old_schema.iter() {
        if !should_include_table(&old_table.name, &config.tables, &config.exclude) {
            continue;
        }

        match new_schema.get_table(&old_table.name) {
            None => {
                tables_removed.push(old_table.name.clone());
            }
            Some(new_table) => {
                let modification =
                    compare_tables(old_table, new_table, &old_table.name, &ignore_patterns);
                if modification.has_changes() {
                    tables_modified.push(modification);
                }
            }
        }
    }

    SchemaDiff {
        tables_added,
        tables_removed,
        tables_modified,
    }
}

/// Compare two table schemas
fn compare_tables(
    old_table: &TableSchema,
    new_table: &TableSchema,
    table_name: &str,
    ignore_patterns: &[Pattern],
) -> TableModification {
    let mut columns_added = Vec::new();
    let mut columns_removed = Vec::new();
    let mut columns_modified = Vec::new();

    // Build column maps for efficient lookup
    let old_columns: std::collections::HashMap<String, &Column> = old_table
        .columns
        .iter()
        .map(|c| (c.name.to_lowercase(), c))
        .collect();
    let new_columns: std::collections::HashMap<String, &Column> = new_table
        .columns
        .iter()
        .map(|c| (c.name.to_lowercase(), c))
        .collect();

    // Find added columns
    for new_col in &new_table.columns {
        // Skip ignored columns
        if should_ignore_column(table_name, &new_col.name, ignore_patterns) {
            continue;
        }
        let key = new_col.name.to_lowercase();
        if !old_columns.contains_key(&key) {
            columns_added.push(ColumnInfo::from(new_col));
        }
    }

    // Find removed and modified columns
    for old_col in &old_table.columns {
        // Skip ignored columns
        if should_ignore_column(table_name, &old_col.name, ignore_patterns) {
            continue;
        }
        let key = old_col.name.to_lowercase();
        match new_columns.get(&key) {
            None => {
                columns_removed.push(ColumnInfo::from(old_col));
            }
            Some(new_col) => {
                if let Some(change) = compare_columns(old_col, new_col) {
                    columns_modified.push(change);
                }
            }
        }
    }

    // Compare primary keys
    let old_pk: Vec<String> = old_table
        .primary_key
        .iter()
        .filter_map(|id| old_table.column(*id).map(|c| c.name.clone()))
        .collect();
    let new_pk: Vec<String> = new_table
        .primary_key
        .iter()
        .filter_map(|id| new_table.column(*id).map(|c| c.name.clone()))
        .collect();

    let pk_changed = old_pk != new_pk;

    // Compare foreign keys
    let old_fks: Vec<FkInfo> = old_table.foreign_keys.iter().map(FkInfo::from).collect();
    let new_fks: Vec<FkInfo> = new_table.foreign_keys.iter().map(FkInfo::from).collect();

    let fks_added: Vec<FkInfo> = new_fks
        .iter()
        .filter(|fk| !old_fks.contains(fk))
        .cloned()
        .collect();
    let fks_removed: Vec<FkInfo> = old_fks
        .iter()
        .filter(|fk| !new_fks.contains(fk))
        .cloned()
        .collect();

    // Compare indexes
    let old_indexes: Vec<IndexInfo> = old_table.indexes.iter().map(IndexInfo::from).collect();
    let new_indexes: Vec<IndexInfo> = new_table.indexes.iter().map(IndexInfo::from).collect();

    let indexes_added: Vec<IndexInfo> = new_indexes
        .iter()
        .filter(|idx| !old_indexes.contains(idx))
        .cloned()
        .collect();
    let indexes_removed: Vec<IndexInfo> = old_indexes
        .iter()
        .filter(|idx| !new_indexes.contains(idx))
        .cloned()
        .collect();

    TableModification {
        table_name: old_table.name.clone(),
        columns_added,
        columns_removed,
        columns_modified,
        pk_changed,
        old_pk: if pk_changed { Some(old_pk) } else { None },
        new_pk: if pk_changed { Some(new_pk) } else { None },
        fks_added,
        fks_removed,
        indexes_added,
        indexes_removed,
    }
}

/// Compare two column definitions
fn compare_columns(old_col: &Column, new_col: &Column) -> Option<ColumnChange> {
    let type_changed = old_col.col_type != new_col.col_type;
    let nullable_changed = old_col.is_nullable != new_col.is_nullable;

    if !type_changed && !nullable_changed {
        return None;
    }

    Some(ColumnChange {
        name: old_col.name.clone(),
        old_type: if type_changed {
            Some(format_column_type(&old_col.col_type))
        } else {
            None
        },
        new_type: if type_changed {
            Some(format_column_type(&new_col.col_type))
        } else {
            None
        },
        old_nullable: if nullable_changed {
            Some(old_col.is_nullable)
        } else {
            None
        },
        new_nullable: if nullable_changed {
            Some(new_col.is_nullable)
        } else {
            None
        },
    })
}
