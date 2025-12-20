//! Schema model for defining tables, columns, and relationships.
//!
//! This module provides a dialect-agnostic way to define database schemas
//! that can be rendered to MySQL, PostgreSQL, or SQLite.

use std::collections::HashMap;

/// SQL data types (dialect-agnostic)
#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    /// Auto-incrementing integer (SERIAL in PG, AUTO_INCREMENT in MySQL)
    Serial,
    /// 32-bit integer
    Integer,
    /// 64-bit integer
    BigInt,
    /// Variable-length string
    VarChar(u16),
    /// Unlimited text
    Text,
    /// Boolean
    Boolean,
    /// Decimal with precision and scale
    Decimal(u8, u8),
    /// Timestamp/datetime
    Timestamp,
    /// Date only
    Date,
}

impl SqlType {
    /// Returns the MySQL type string
    pub fn to_mysql(&self) -> String {
        match self {
            SqlType::Serial => "INT AUTO_INCREMENT".to_string(),
            SqlType::Integer => "INT".to_string(),
            SqlType::BigInt => "BIGINT".to_string(),
            SqlType::VarChar(n) => format!("VARCHAR({})", n),
            SqlType::Text => "TEXT".to_string(),
            SqlType::Boolean => "TINYINT(1)".to_string(),
            SqlType::Decimal(p, s) => format!("DECIMAL({},{})", p, s),
            SqlType::Timestamp => "DATETIME".to_string(),
            SqlType::Date => "DATE".to_string(),
        }
    }

    /// Returns the PostgreSQL type string
    pub fn to_postgres(&self) -> String {
        match self {
            SqlType::Serial => "SERIAL".to_string(),
            SqlType::Integer => "INTEGER".to_string(),
            SqlType::BigInt => "BIGINT".to_string(),
            SqlType::VarChar(n) => format!("VARCHAR({})", n),
            SqlType::Text => "TEXT".to_string(),
            SqlType::Boolean => "BOOLEAN".to_string(),
            SqlType::Decimal(p, s) => format!("DECIMAL({},{})", p, s),
            SqlType::Timestamp => "TIMESTAMP".to_string(),
            SqlType::Date => "DATE".to_string(),
        }
    }

    /// Returns the SQLite type string
    pub fn to_sqlite(&self) -> String {
        match self {
            SqlType::Serial => "INTEGER".to_string(), // PRIMARY KEY implies AUTOINCREMENT
            SqlType::Integer => "INTEGER".to_string(),
            SqlType::BigInt => "INTEGER".to_string(),
            SqlType::VarChar(_) => "TEXT".to_string(),
            SqlType::Text => "TEXT".to_string(),
            SqlType::Boolean => "INTEGER".to_string(),
            SqlType::Decimal(_, _) => "REAL".to_string(),
            SqlType::Timestamp => "TEXT".to_string(),
            SqlType::Date => "TEXT".to_string(),
        }
    }
}

/// Foreign key reference action
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum FkAction {
    #[default]
    NoAction,
    Cascade,
    SetNull,
    Restrict,
}

impl FkAction {
    pub fn to_sql(&self) -> &'static str {
        match self {
            FkAction::NoAction => "NO ACTION",
            FkAction::Cascade => "CASCADE",
            FkAction::SetNull => "SET NULL",
            FkAction::Restrict => "RESTRICT",
        }
    }
}

/// Foreign key constraint
#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub to_table: String,
    pub to_column: String,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub not_null: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
    pub foreign_key: Option<ForeignKey>,
}

impl Column {
    pub fn new(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self {
            name: name.into(),
            sql_type,
            not_null: false,
            primary_key: false,
            unique: false,
            default: None,
            foreign_key: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.not_null = true;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default = Some(value.into());
        self
    }

    pub fn references(
        mut self,
        table: impl Into<String>,
        column: impl Into<String>,
        on_delete: FkAction,
    ) -> Self {
        self.foreign_key = Some(ForeignKey {
            to_table: table.into(),
            to_column: column.into(),
            on_delete,
            on_update: FkAction::NoAction,
        });
        self
    }
}

/// Table definition
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub has_timestamps: bool,
    pub has_soft_deletes: bool,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            has_timestamps: false,
            has_soft_deletes: false,
        }
    }

    pub fn column(mut self, col: Column) -> Self {
        self.columns.push(col);
        self
    }

    pub fn timestamps(mut self) -> Self {
        self.has_timestamps = true;
        self
    }

    pub fn soft_deletes(mut self) -> Self {
        self.has_soft_deletes = true;
        self
    }

    /// Get the primary key column name (assumes single-column PK)
    pub fn primary_key_column(&self) -> Option<&str> {
        self.columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.as_str())
    }

    /// Get all foreign key relationships
    pub fn foreign_keys(&self) -> Vec<(&str, &ForeignKey)> {
        self.columns
            .iter()
            .filter_map(|c| c.foreign_key.as_ref().map(|fk| (c.name.as_str(), fk)))
            .collect()
    }

    /// Check if this table has a specific column
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.iter().any(|c| c.name == name)
    }

    /// Get all column names (including timestamp columns if enabled)
    pub fn all_column_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        if self.has_timestamps {
            names.push("created_at");
            names.push("updated_at");
        }
        if self.has_soft_deletes {
            names.push("deleted_at");
        }
        names
    }
}

/// Complete schema definition
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: Vec<Table>,
    table_index: HashMap<String, usize>,
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn table(mut self, table: Table) -> Self {
        let idx = self.tables.len();
        self.table_index.insert(table.name.clone(), idx);
        self.tables.push(table);
        self
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.table_index.get(name).map(|&idx| &self.tables[idx])
    }

    /// Get tables in topological order (dependencies first)
    pub fn tables_in_order(&self) -> Vec<&Table> {
        // Simple topological sort based on FK dependencies
        let mut visited = vec![false; self.tables.len()];
        let mut result = Vec::with_capacity(self.tables.len());

        fn visit<'a>(
            idx: usize,
            tables: &'a [Table],
            table_index: &HashMap<String, usize>,
            visited: &mut [bool],
            result: &mut Vec<&'a Table>,
        ) {
            if visited[idx] {
                return;
            }
            visited[idx] = true;

            // Visit dependencies first
            for (_, fk) in tables[idx].foreign_keys() {
                if let Some(&dep_idx) = table_index.get(&fk.to_table) {
                    visit(dep_idx, tables, table_index, visited, result);
                }
            }

            result.push(&tables[idx]);
        }

        for idx in 0..self.tables.len() {
            visit(
                idx,
                &self.tables,
                &self.table_index,
                &mut visited,
                &mut result,
            );
        }

        result
    }
}

/// Table classification for multi-tenant scenarios
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableRole {
    /// Has tenant_id column, root of tenant data
    TenantRoot,
    /// No tenant_id, but FK chain leads to tenant table
    TenantDependent,
    /// Junction/pivot table (only FKs + metadata)
    Junction,
    /// Global lookup data (no tenant association)
    Global,
    /// Framework/system table
    System,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_builder() {
        let col = Column::new("id", SqlType::Serial).primary_key().not_null();

        assert_eq!(col.name, "id");
        assert!(col.primary_key);
        assert!(col.not_null);
    }

    #[test]
    fn test_table_builder() {
        let table = Table::new("users")
            .column(Column::new("id", SqlType::Serial).primary_key())
            .column(
                Column::new("tenant_id", SqlType::Integer)
                    .not_null()
                    .references("tenants", "id", FkAction::Cascade),
            )
            .column(Column::new("email", SqlType::VarChar(255)).not_null())
            .timestamps();

        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 3);
        assert!(table.has_timestamps);
        assert_eq!(table.primary_key_column(), Some("id"));
        assert_eq!(table.foreign_keys().len(), 1);
    }

    #[test]
    fn test_schema_topological_order() {
        let schema = Schema::new()
            .table(Table::new("tenants").column(Column::new("id", SqlType::Serial).primary_key()))
            .table(
                Table::new("users")
                    .column(Column::new("id", SqlType::Serial).primary_key())
                    .column(Column::new("tenant_id", SqlType::Integer).references(
                        "tenants",
                        "id",
                        FkAction::Cascade,
                    )),
            )
            .table(
                Table::new("posts")
                    .column(Column::new("id", SqlType::Serial).primary_key())
                    .column(Column::new("user_id", SqlType::Integer).references(
                        "users",
                        "id",
                        FkAction::Cascade,
                    )),
            );

        let ordered = schema.tables_in_order();
        let names: Vec<&str> = ordered.iter().map(|t| t.name.as_str()).collect();

        // tenants should come before users, users before posts
        let tenant_idx = names.iter().position(|&n| n == "tenants").unwrap();
        let users_idx = names.iter().position(|&n| n == "users").unwrap();
        let posts_idx = names.iter().position(|&n| n == "posts").unwrap();

        assert!(tenant_idx < users_idx);
        assert!(users_idx < posts_idx);
    }
}
