//! MySQL DDL parsing for schema extraction.
//!
//! Parses CREATE TABLE and ALTER TABLE statements to extract:
//! - Column definitions with types
//! - Primary key constraints
//! - Foreign key constraints

use super::{
    CheckConstraint, Column, ColumnId, ColumnType, ForeignKey, IndexDef, Schema, TableId,
    TableSchema, UniqueConstraint,
};
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
    Regex::new(
        r#"(?i)ALTER\s+TABLE\s+(?:ONLY\s+)?(?:[\[\]`"\w]+\s*\.\s*)*[\[`"]?([^\[\]`"\s]+)[\]`"]?"#,
    )
    .unwrap()
});

/// Regex for column definition
/// Supports: `column` (MySQL), "column" (PostgreSQL), [column] (MSSQL), column (unquoted)
static COLUMN_DEF_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"^\s*[\[`"]?([^\[\]`"\s,]+)[\]`"]?\s+(\w+(?:\([^)]+\))?(?:\s+unsigned)?)"#)
        .unwrap()
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

            // Parse a bare table-level UNIQUE (col, ...) constraint, marking a
            // single covered column as unique too.
            if let Some(uc) = parse_unique_constraint(trimmed) {
                if let [only_column] = uc.columns.as_slice() {
                    if let Some(col) = table
                        .columns
                        .iter_mut()
                        .find(|c| c.name.eq_ignore_ascii_case(only_column))
                    {
                        col.is_unique = true;
                    }
                }
                table.unique_constraints.push(uc);
            }

            // Parse a table-level CHECK (...) constraint
            if let Some(cc) = parse_check_constraint(trimmed) {
                table.check_constraints.push(cc);
            }
        } else {
            // Parse column definition
            if let Some((col, inline_check)) =
                parse_column_def(trimmed, ColumnId(table.columns.len() as u16))
            {
                if let Some(expression) = inline_check {
                    table.check_constraints.push(CheckConstraint {
                        name: None,
                        expression,
                    });
                }

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

/// Parse a column definition, returning the column plus any inline
/// column-level CHECK expression found among its modifiers (callers attach
/// that to the table's check constraints, since `Column` has no such field).
fn parse_column_def(def: &str, ordinal: ColumnId) -> Option<(Column, Option<String>)> {
    let caps = COLUMN_DEF_RE.captures(def)?;
    let whole = caps.get(0)?;
    let name = caps.get(1)?.as_str().to_string();
    let type_str = caps.get(2)?.as_str().to_string();

    let col_type = ColumnType::from_mysql_type(&type_str);
    let is_nullable = !NOT_NULL_RE.is_match(def);

    let remainder = &def[whole.end()..];
    let modifiers = parse_column_modifiers(remainder);
    // PostgreSQL `serial`/`bigserial`/`smallserial` columns are backed by an
    // implicit sequence, so they carry identity semantics even without an
    // explicit IDENTITY/AUTO_INCREMENT modifier.
    let is_identity = modifiers.is_identity || type_str.to_lowercase().contains("serial");

    let column = Column {
        name,
        col_type,
        source_type: type_str,
        ordinal,
        is_primary_key: false,
        is_nullable,
        is_unique: modifiers.is_unique,
        default_sql: modifiers.default_sql,
        is_generated: modifiers.is_generated,
        is_identity,
        collation: modifiers.collation,
    };

    Some((column, modifiers.inline_check))
}

/// Modifiers found after a column's name and type: UNIQUE, DEFAULT,
/// GENERATED .. AS (...), IDENTITY/AUTO_INCREMENT, COLLATE, and inline CHECK.
#[derive(Debug, Default)]
struct ColumnModifiers {
    is_unique: bool,
    default_sql: Option<String>,
    is_generated: bool,
    is_identity: bool,
    collation: Option<String>,
    inline_check: Option<String>,
}

/// Parse the column modifiers following the name/type, using
/// [`tokenize_ddl`] so that quoted defaults and parenthesized expressions
/// (e.g. `DEFAULT 'active'`, `CHECK (a > 0 AND (b < 1))`) are kept intact
/// rather than split on whitespace.
fn parse_column_modifiers(remainder: &str) -> ColumnModifiers {
    let tokens = tokenize_ddl(remainder);
    let mut modifiers = ColumnModifiers::default();
    let mut i = 0;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        match upper.as_str() {
            "UNIQUE" => {
                modifiers.is_unique = true;
                i += 1;
            }
            "DEFAULT" => {
                if let Some(value) = tokens.get(i + 1) {
                    modifiers.default_sql = Some(value.clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "COLLATE" => {
                if let Some(value) = tokens.get(i + 1) {
                    modifiers.collation = Some(strip_quotes(value));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "CHECK" => {
                if let Some(expr) = tokens.get(i + 1) {
                    modifiers.inline_check = Some(strip_outer_parens(expr));
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "GENERATED" => {
                // Skip ALWAYS/BY/DEFAULT until AS, then inspect what follows:
                // `AS (expr)` is a computed column, `AS IDENTITY` is identity.
                let mut j = i + 1;
                while j < tokens.len() && !tokens[j].eq_ignore_ascii_case("AS") {
                    j += 1;
                }
                if let Some(as_expr) = tokens.get(j + 1) {
                    if as_expr.starts_with('(') {
                        modifiers.is_generated = true;
                    } else if as_expr.eq_ignore_ascii_case("IDENTITY") {
                        modifiers.is_identity = true;
                    }
                }
                i = j + 2;
            }
            "AUTO_INCREMENT" | "AUTOINCREMENT" => {
                modifiers.is_identity = true;
                i += 1;
            }
            _ if upper.starts_with("IDENTITY(") || upper == "IDENTITY" => {
                modifiers.is_identity = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    modifiers
}

/// Split a fragment of DDL into whitespace-separated tokens, treating
/// single-quoted strings and parenthesized groups (including nested
/// parentheses) as atomic units. This lets callers pull out `DEFAULT`,
/// `CHECK`, and `GENERATED ... AS (...)` values without splitting the SQL
/// expressions they contain on internal whitespace.
fn tokenize_ddl(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut depth = 0u32;
    let mut in_string = false;
    let mut escape_next = false;

    for ch in s.chars() {
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
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            c if c.is_whitespace() && depth == 0 => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Strip surrounding quote/bracket characters from an identifier-like token
fn strip_quotes(s: &str) -> String {
    s.trim_matches('\'')
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

/// Strip a single layer of enclosing parentheses from an expression token,
/// if present (used to store CHECK expressions without the outer parens).
fn strip_outer_parens(s: &str) -> String {
    let trimmed = s.trim();
    match trimmed
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    {
        Some(inner) => inner.trim().to_string(),
        None => trimmed.to_string(),
    }
}

/// Parse a table-level UNIQUE constraint: `[CONSTRAINT name] UNIQUE (cols)`.
/// Does not match `UNIQUE INDEX`/`UNIQUE KEY` forms, which are handled by
/// [`parse_inline_index`].
fn parse_unique_constraint(constraint: &str) -> Option<UniqueConstraint> {
    let tokens = tokenize_ddl(constraint);
    let mut i = 0;
    let mut name = None;

    if tokens.first()?.eq_ignore_ascii_case("CONSTRAINT") {
        name = tokens.get(1).map(|s| strip_quotes(s));
        i = 2;
    }

    if !tokens.get(i)?.eq_ignore_ascii_case("UNIQUE") {
        return None;
    }

    let cols_token = tokens.get(i + 1)?;
    if !cols_token.starts_with('(') {
        // Not a bare `UNIQUE (cols)` constraint (e.g. `UNIQUE INDEX ...` or
        // `UNIQUE KEY ...`, handled separately by `parse_inline_index`).
        return None;
    }
    let columns = parse_column_list(strip_outer_parens(cols_token).as_str());
    if columns.is_empty() {
        return None;
    }

    Some(UniqueConstraint { name, columns })
}

/// Parse a table-level CHECK constraint: `[CONSTRAINT name] CHECK (expr)`.
fn parse_check_constraint(constraint: &str) -> Option<CheckConstraint> {
    let tokens = tokenize_ddl(constraint);
    let mut i = 0;
    let mut name = None;

    if tokens.first()?.eq_ignore_ascii_case("CONSTRAINT") {
        name = tokens.get(1).map(|s| strip_quotes(s));
        i = 2;
    }

    if !tokens.get(i)?.eq_ignore_ascii_case("CHECK") {
        return None;
    }

    let expr_token = tokens.get(i + 1)?;
    if !expr_token.starts_with('(') {
        return None;
    }

    Some(CheckConstraint {
        name,
        expression: strip_outer_parens(expr_token),
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
