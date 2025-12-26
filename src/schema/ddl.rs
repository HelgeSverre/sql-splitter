//! MySQL DDL parsing for schema extraction.
//!
//! Parses CREATE TABLE and ALTER TABLE statements to extract:
//! - Column definitions with types
//! - Primary key constraints
//! - Foreign key constraints

use super::{Column, ColumnId, ColumnType, ForeignKey, IndexDef, Schema, TableId, TableSchema};
use once_cell::sync::Lazy;
use regex::Regex;

/// Regex to extract table name from CREATE TABLE
/// Supports: `table` (MySQL), "table" (PostgreSQL), [table] (MSSQL), table (SQLite/unquoted), schema.table
static CREATE_TABLE_NAME_RE: Lazy<Regex> = Lazy::new(|| {
    // Match table name with various quoting styles including MSSQL brackets
    // Pattern handles: schema.table, [schema].[table], `schema`.`table`, "schema"."table"
    Regex::new(r#"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[\[\]`"\w]+\s*\.\s*)*[\[`"]?([^\[\]`"\s(]+)[\]`"]?"#)
        .unwrap()
});

/// Regex to extract table name from ALTER TABLE
/// Supports: `table` (MySQL), "table" (PostgreSQL), [table] (MSSQL), table (SQLite/unquoted), schema.table
static ALTER_TABLE_NAME_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)ALTER\s+TABLE\s+(?:ONLY\s+)?(?:[\[\]`"\w]+\s*\.\s*)*[\[`"]?([^\[\]`"\s]+)[\]`"]?"#).unwrap()
});

/// Regex for column definition
/// Supports: `column` (MySQL), "column" (PostgreSQL), [column] (MSSQL), column (unquoted)
static COLUMN_DEF_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"^\s*[\[`"]?([^\[\]`"\s,]+)[\]`"]?\s+(\w+(?:\([^)]+\))?(?:\s+unsigned)?)"#).unwrap()
});

/// Regex for PRIMARY KEY constraint
/// Supports MSSQL CLUSTERED/NONCLUSTERED keywords: PRIMARY KEY CLUSTERED ([col])
static PRIMARY_KEY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)PRIMARY\s+KEY\s*(?:CLUSTERED\s+|NONCLUSTERED\s+)?\(([^)]+)\)").unwrap()
});

/// Regex for inline PRIMARY KEY on column
static INLINE_PRIMARY_KEY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bPRIMARY\s+KEY\b").unwrap());

/// Regex for FOREIGN KEY constraint with optional constraint name
/// Supports: `name` (MySQL), "name" (PostgreSQL), [name] (MSSQL), name (unquoted)
static FOREIGN_KEY_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?i)(?:CONSTRAINT\s+[\[`"]?([^\[\]`"\s]+)[\]`"]?\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+(?:[\[\]`"\w]+\s*\.\s*)*[\[`"]?([^\[\]`"\s(]+)[\]`"]?\s*\(([^)]+)\)"#,
    )
    .unwrap()
});

/// Regex to detect NOT NULL constraint
static NOT_NULL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNOT\s+NULL\b").unwrap());

/// Regex for inline INDEX/KEY in CREATE TABLE
/// Matches: INDEX idx_name (col1, col2), KEY idx_name (col1), UNIQUE INDEX idx_name (col1)
/// Supports MSSQL bracket quoting: INDEX [idx_name] ([col])
static INLINE_INDEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(?:(UNIQUE)\s+)?(?:INDEX|KEY)\s+[\[`"]?(\w+)[\]`"]?\s*\(([^)]+)\)"#).unwrap()
});

/// Regex for CREATE INDEX statement
/// Matches: CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX [IF NOT EXISTS] idx_name ON table [USING method] (columns)
/// Supports MSSQL bracket quoting and schema prefixes
static CREATE_INDEX_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?i)CREATE\s+(UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?[\[`"]?(\w+)[\]`"]?\s+ON\s+(?:[\[\]`"\w]+\s*\.\s*)*[\[`"]?(\w+)[\]`"]?\s*(?:USING\s+(\w+)\s*)?\(([^)]+)\)"#,
    )
    .unwrap()
});

/// Builder for constructing schema from DDL statements
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        Self {
            schema: Schema::new(),
        }
    }

    /// Parse a CREATE TABLE statement and add to schema
    pub fn parse_create_table(&mut self, stmt: &str) -> Option<TableId> {
        let table_name = extract_create_table_name(stmt)?;

        // Check if table already exists
        if self.schema.get_table_id(&table_name).is_some() {
            return self.schema.get_table_id(&table_name);
        }

        let mut table = TableSchema::new(table_name, TableId(0));
        table.create_statement = Some(stmt.to_string());

        // Extract the body between first ( and last )
        let body = extract_table_body(stmt)?;

        // Parse columns and constraints
        parse_table_body(&body, &mut table);

        // Add table to schema
        Some(self.schema.add_table(table))
    }

    /// Parse an ALTER TABLE statement and update existing table
    pub fn parse_alter_table(&mut self, stmt: &str) -> Option<TableId> {
        let table_name = extract_alter_table_name(stmt)?;
        let table_id = self.schema.get_table_id(&table_name)?;

        // Parse any FK constraints added by ALTER TABLE
        for fk in parse_foreign_keys(stmt) {
            if let Some(table) = self.schema.table_mut(table_id) {
                // Resolve column names to IDs
                let mut resolved_fk = fk;
                resolved_fk.columns = resolved_fk
                    .column_names
                    .iter()
                    .filter_map(|name| table.get_column_id(name))
                    .collect();
                table.foreign_keys.push(resolved_fk);
            }
        }

        Some(table_id)
    }

    /// Parse a CREATE INDEX statement and add to the appropriate table
    pub fn parse_create_index(&mut self, stmt: &str) -> Option<TableId> {
        let caps = CREATE_INDEX_RE.captures(stmt)?;

        let is_unique = caps.get(1).is_some();
        let index_name = caps.get(2)?.as_str().to_string();
        let table_name = caps.get(3)?.as_str().to_string();
        let index_type = caps.get(4).map(|m| m.as_str().to_uppercase());
        let columns_str = caps.get(5)?.as_str();
        let columns = parse_column_list(columns_str);

        let table_id = self.schema.get_table_id(&table_name)?;

        if let Some(table) = self.schema.table_mut(table_id) {
            table.indexes.push(IndexDef {
                name: index_name,
                columns,
                is_unique,
                index_type,
            });
        }

        Some(table_id)
    }

    /// Finalize the schema, resolving all FK references
    pub fn build(mut self) -> Schema {
        self.schema.resolve_foreign_keys();
        self.schema
    }

    /// Get current schema (for inspection during building)
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Extract table name from CREATE TABLE statement
pub fn extract_create_table_name(stmt: &str) -> Option<String> {
    CREATE_TABLE_NAME_RE
        .captures(stmt)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

/// Extract table name from ALTER TABLE statement
pub fn extract_alter_table_name(stmt: &str) -> Option<String> {
    ALTER_TABLE_NAME_RE
        .captures(stmt)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

/// Extract the body of a CREATE TABLE statement (between first ( and matching ))
fn extract_table_body(stmt: &str) -> Option<String> {
    let bytes = stmt.as_bytes();
    let mut depth = 0;
    let mut start = None;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, &b) in bytes.iter().enumerate() {
        if escape_next {
            escape_next = false;
            continue;
        }

        if b == b'\\' && in_string {
            escape_next = true;
            continue;
        }

        if b == b'\'' {
            in_string = !in_string;
            continue;
        }

        if in_string {
            continue;
        }

        if b == b'(' {
            if depth == 0 {
                start = Some(i + 1);
            }
            depth += 1;
        } else if b == b')' {
            depth -= 1;
            if depth == 0 {
                if let Some(s) = start {
                    return Some(stmt[s..i].to_string());
                }
            }
        }
    }

    None
}

/// Parse the body of a CREATE TABLE to extract columns and constraints
fn parse_table_body(body: &str, table: &mut TableSchema) {
    // Split by commas, but respect nested parentheses
    let parts = split_table_body(body);

    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Check if this is a constraint or a column
        let upper = trimmed.to_uppercase();
        if upper.starts_with("PRIMARY KEY")
            || upper.starts_with("CONSTRAINT")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("KEY ")
            || upper.starts_with("INDEX ")
            || upper.starts_with("UNIQUE ")
            || upper.starts_with("FULLTEXT ")
            || upper.starts_with("SPATIAL ")
            || upper.starts_with("CHECK ")
        {
            // Parse constraints
            if let Some(pk_cols) = parse_primary_key_constraint(trimmed) {
                for col_name in pk_cols {
                    if let Some(col) = table
                        .columns
                        .iter_mut()
                        .find(|c| c.name.eq_ignore_ascii_case(&col_name))
                    {
                        col.is_primary_key = true;
                        if !table.primary_key.contains(&col.ordinal) {
                            table.primary_key.push(col.ordinal);
                        }
                    }
                }
            }

            for fk in parse_foreign_keys(trimmed) {
                let mut resolved_fk = fk;
                resolved_fk.columns = resolved_fk
                    .column_names
                    .iter()
                    .filter_map(|name| table.get_column_id(name))
                    .collect();
                table.foreign_keys.push(resolved_fk);
            }

            // Parse inline indexes (INDEX, KEY, UNIQUE INDEX, UNIQUE KEY)
            if let Some(idx) = parse_inline_index(trimmed) {
                table.indexes.push(idx);
            }
        } else {
            // Parse column definition
            if let Some(col) = parse_column_def(trimmed, ColumnId(table.columns.len() as u16)) {
                // Check for inline PRIMARY KEY
                if INLINE_PRIMARY_KEY_RE.is_match(trimmed) {
                    let mut col = col;
                    col.is_primary_key = true;
                    table.primary_key.push(col.ordinal);
                    table.columns.push(col);
                } else {
                    table.columns.push(col);
                }
            }
        }
    }
}

/// Split table body by commas, respecting nested parentheses
pub fn split_table_body(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for ch in body.chars() {
        if escape_next {
            current.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' && in_string {
            current.push(ch);
            escape_next = true;
            continue;
        }

        if ch == '\'' {
            in_string = !in_string;
            current.push(ch);
            continue;
        }

        if in_string {
            current.push(ch);
            continue;
        }

        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current = String::new();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    parts
}

/// Parse a column definition
fn parse_column_def(def: &str, ordinal: ColumnId) -> Option<Column> {
    let caps = COLUMN_DEF_RE.captures(def)?;
    let name = caps.get(1)?.as_str().to_string();
    let type_str = caps.get(2)?.as_str();

    let col_type = ColumnType::from_mysql_type(type_str);
    let is_nullable = !NOT_NULL_RE.is_match(def);

    Some(Column {
        name,
        col_type,
        ordinal,
        is_primary_key: false,
        is_nullable,
    })
}

/// Parse PRIMARY KEY constraint, returns column names
fn parse_primary_key_constraint(constraint: &str) -> Option<Vec<String>> {
    let caps = PRIMARY_KEY_RE.captures(constraint)?;
    let cols_str = caps.get(1)?.as_str();
    Some(parse_column_list(cols_str))
}

/// Parse inline INDEX/KEY constraint from CREATE TABLE body
fn parse_inline_index(constraint: &str) -> Option<IndexDef> {
    let caps = INLINE_INDEX_RE.captures(constraint)?;

    let is_unique = caps.get(1).is_some();
    let index_name = caps.get(2)?.as_str().to_string();
    let columns_str = caps.get(3)?.as_str();
    let columns = parse_column_list(columns_str);

    Some(IndexDef {
        name: index_name,
        columns,
        is_unique,
        index_type: None, // Inline indexes don't specify type
    })
}

/// Parse FOREIGN KEY constraints from a statement
fn parse_foreign_keys(stmt: &str) -> Vec<ForeignKey> {
    let mut fks = Vec::new();

    for caps in FOREIGN_KEY_RE.captures_iter(stmt) {
        let name = caps.get(1).map(|m| m.as_str().to_string());
        let local_cols = caps
            .get(2)
            .map(|m| parse_column_list(m.as_str()))
            .unwrap_or_default();
        let ref_table = caps
            .get(3)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        let ref_cols = caps
            .get(4)
            .map(|m| parse_column_list(m.as_str()))
            .unwrap_or_default();

        if !local_cols.is_empty() && !ref_table.is_empty() && !ref_cols.is_empty() {
            fks.push(ForeignKey {
                name,
                columns: Vec::new(), // Will be resolved later
                column_names: local_cols,
                referenced_table: ref_table,
                referenced_columns: ref_cols,
                referenced_table_id: None,
            });
        }
    }

    fks
}

/// Parse a comma-separated column list, stripping quotes (backticks, double quotes, brackets)
pub fn parse_column_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|c| {
            c.trim()
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('[')
                .trim_matches(']')
                .to_string()
        })
        .filter(|c| !c.is_empty())
        .collect()
}
