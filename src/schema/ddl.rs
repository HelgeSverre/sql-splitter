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
    Regex::new(
        r#"(?ix)
        CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?
        (
            (?:(?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)\s*\.\s*)*
            (?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)
        )
        "#,
    )
    .unwrap()
});

/// Regex to extract table name from ALTER TABLE
/// Supports: `table` (MySQL), "table" (PostgreSQL), [table] (MSSQL), table (SQLite/unquoted), schema.table
static ALTER_TABLE_NAME_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?ix)
        ALTER\s+TABLE\s+(?:ONLY\s+)?
        (
            (?:(?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)\s*\.\s*)*
            (?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)
        )
        "#,
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
        r#"(?ix)
        (?:CONSTRAINT\s+[\[`"]?([^\[\]`"\s]+)[\]`"]?\s+)?
        FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+
        (
            (?:(?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)\s*\.\s*)*
            (?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)
        )\s*\(([^)]+)\)
        "#,
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
        r#"(?ix)
        CREATE\s+(UNIQUE\s+)?(?:CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\s+
        (?:IF\s+NOT\s+EXISTS\s+)?[\[`"]?(\w+)[\]`"]?\s+ON\s+
        (
            (?:(?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)\s*\.\s*)*
            (?:\[(?:[^\]]|\]\])*\]|`(?:[^`]|``)*`|"(?:[^"]|"")*"|[^\s().,;]+)
        )\s*(?:USING\s+(\w+)\s*)?\(([^)]+)\)
        "#,
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

        // Extract the body after the CREATE TABLE identifier.
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
        let table_name = normalize_qualified_identifier(caps.get(3)?.as_str())?;
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
        .and_then(|m| normalize_qualified_identifier(m.as_str()))
}

/// Extract table name from ALTER TABLE statement
pub fn extract_alter_table_name(stmt: &str) -> Option<String> {
    ALTER_TABLE_NAME_RE
        .captures(stmt)
        .and_then(|c| c.get(1))
        .and_then(|m| normalize_qualified_identifier(m.as_str()))
}

/// Normalize a SQL table reference into a dotted, unquoted identity.
///
/// The schema graph uses this representation consistently for DDL, INSERT,
/// and COPY routing, so `tenant_a.users` and `tenant_b.users` remain distinct.
fn normalize_qualified_identifier(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut i = 0;
    let mut parts = Vec::new();

    loop {
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        let (quote, close) = match bytes[i] {
            b'`' => (Some(b'`'), b'`'),
            b'"' => (Some(b'"'), b'"'),
            b'[' => (Some(b'['), b']'),
            _ => (None, 0),
        };
        if quote.is_some() {
            i += 1;
        }

        let mut part = Vec::new();
        let mut terminated = quote.is_none();
        while i < bytes.len() {
            let byte = bytes[i];
            if let Some(open) = quote {
                if byte == close {
                    if bytes.get(i + 1) == Some(&close) {
                        part.push(close);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    terminated = true;
                    break;
                }
                // A backtick doubled inside an identifier and a doubled double
                // quote both use the same close-byte escape handled above.
                debug_assert!(open == b'`' || open == b'"' || open == b'[');
                part.push(byte);
                i += 1;
            } else if byte == b'.' || byte.is_ascii_whitespace() {
                break;
            } else {
                part.push(byte);
                i += 1;
            }
        }

        if !terminated || part.is_empty() {
            return None;
        }
        parts.push(canonicalize_qualified_part(
            String::from_utf8_lossy(&part).into_owned(),
            quote.is_some(),
        ));

        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i += 1;
        }
        if bytes.get(i) == Some(&b'.') {
            i += 1;
            continue;
        }
        break;
    }

    (!parts.is_empty()).then(|| parts.join("."))
}

/// Preserve a quoted table-reference component containing a dot. The dotted
/// schema identity uses unquoted dots as separators, so this marker lets the
/// writer reconstruct `public."user.log"` without changing its meaning.
fn canonicalize_qualified_part(part: String, was_quoted: bool) -> String {
    if was_quoted && part.contains('.') {
        format!("\"{}\"", part.replace('"', "\"\""))
    } else {
        part
    }
}

/// Extract the body of a CREATE TABLE statement.
///
/// Start after the matched table name rather than at the first opening
/// parenthesis in the statement. Dump comments commonly precede DDL and can
/// themselves contain parentheses, for example `NVARCHAR(MAX)`.
fn extract_table_body(stmt: &str) -> Option<String> {
    let bytes = stmt.as_bytes();
    let mut open = CREATE_TABLE_NAME_RE.find(stmt)?.end();

    loop {
        while bytes.get(open).is_some_and(u8::is_ascii_whitespace) {
            open += 1;
        }

        if bytes.get(open..open + 2) == Some(b"--") {
            open = bytes[open + 2..]
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(bytes.len(), |offset| open + 2 + offset + 1);
            continue;
        }

        if bytes.get(open..open + 2) == Some(b"/*") {
            let end = bytes[open + 2..]
                .windows(2)
                .position(|window| window == b"*/")?;
            open += end + 4;
            continue;
        }

        break;
    }

    if bytes.get(open) != Some(&b'(') {
        return None;
    }

    let start = open + 1;
    let mut depth = 1i32;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, &b) in bytes.iter().enumerate().skip(start) {
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
            depth += 1;
        } else if b == b')' {
            depth -= 1;
            if depth == 0 {
                return Some(stmt[start..i].to_string());
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
    let type_str = parse_column_type(caps.get(2)?.as_str(), &def[whole.end()..]);

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

/// Extend the first type token captured by [`COLUMN_DEF_RE`] through any
/// following type words. The DDL tokenizer keeps parameter lists atomic, so
/// this preserves types such as `DOUBLE PRECISION`, `CHARACTER VARYING(42)`,
/// and `TIMESTAMP(6) WITH TIME ZONE` without consuming column modifiers.
fn parse_column_type(first: &str, remainder: &str) -> String {
    let tokens = tokenize_ddl(remainder);
    let mut parts = vec![first];
    for (index, token) in tokens.iter().enumerate() {
        if starts_column_modifier(&tokens, index) {
            break;
        }
        parts.push(token);
    }
    parts.join(" ")
}

fn starts_column_modifier(tokens: &[String], index: usize) -> bool {
    let token = tokens[index].to_ascii_uppercase();
    if token == "CHARACTER"
        && tokens
            .get(index + 1)
            .is_some_and(|next| next.eq_ignore_ascii_case("SET"))
    {
        return true;
    }

    matches!(
        token.as_str(),
        "NOT"
            | "NULL"
            | "PRIMARY"
            | "UNIQUE"
            | "DEFAULT"
            | "CHECK"
            | "GENERATED"
            | "AS"
            | "AUTO_INCREMENT"
            | "AUTOINCREMENT"
            | "AUTO_RANDOM"
            | "IDENTITY"
            | "COLLATE"
            | "CONSTRAINT"
            | "REFERENCES"
            | "COMMENT"
            | "ON"
            | "BINARY"
            | "ASCII"
            | "UNICODE"
            | "BYTE"
            | "VISIBLE"
            | "INVISIBLE"
            | "COLUMN_FORMAT"
            | "ENGINE_ATTRIBUTE"
            | "SECONDARY_ENGINE_ATTRIBUTE"
            | "STORAGE"
            | "SRID"
            | "SPARSE"
            | "FILESTREAM"
            | "ROWGUIDCOL"
            | "HIDDEN"
            | "MASKED"
            | "PERSISTED"
            | "ENCRYPTED"
            | "COMPRESSION"
            | "KEY"
    ) || token.starts_with("IDENTITY(")
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
            "AS" => {
                if tokens
                    .get(i + 1)
                    .is_some_and(|expression| expression.starts_with('('))
                {
                    modifiers.is_generated = true;
                    i += 2;
                } else {
                    i += 1;
                }
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
            .and_then(|m| normalize_qualified_identifier(m.as_str()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_qualified_table_names_in_schema() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE tenant_a.users (id INT PRIMARY KEY);");
        builder.parse_create_table("CREATE TABLE tenant_b.users (id INT PRIMARY KEY);");
        let schema = builder.build();

        assert_eq!(schema.len(), 2);
        assert!(schema.get_table("tenant_a.users").is_some());
        assert!(schema.get_table("tenant_b.users").is_some());
    }

    #[test]
    fn resolves_qualified_foreign_keys_and_indexes() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE \"tenant_a\".\"parents\" (id INT PRIMARY KEY);");
        builder.parse_create_table(
            "CREATE TABLE \"tenant_a\".\"children\" (\
                id INT PRIMARY KEY, parent_id INT, \
                FOREIGN KEY (parent_id) REFERENCES \"tenant_a\".\"parents\" (id)\
            );",
        );
        builder.parse_create_index(
            "CREATE INDEX children_parent_idx ON \"tenant_a\".\"children\" (parent_id);",
        );
        let schema = builder.build();

        let parent = schema.get_table_id("tenant_a.parents").unwrap();
        let child = schema.get_table("tenant_a.children").unwrap();
        assert_eq!(child.foreign_keys[0].referenced_table, "tenant_a.parents");
        assert_eq!(child.foreign_keys[0].referenced_table_id, Some(parent));
        assert_eq!(child.indexes[0].name, "children_parent_idx");
    }

    #[test]
    fn extracts_quoted_qualified_table_names() {
        assert_eq!(
            extract_create_table_name("CREATE TABLE \"tenant_a\".\"users\" (id INT);"),
            Some("tenant_a.users".to_string())
        );
        assert_eq!(
            extract_alter_table_name("ALTER TABLE [tenant_a].[users] ADD COLUMN email TEXT;"),
            Some("tenant_a.users".to_string())
        );
        assert_eq!(
            extract_create_table_name("CREATE TABLE public.\"user.log\" (id INT);"),
            Some("public.\"user.log\"".to_string())
        );
    }

    #[test]
    fn parses_ddl_after_parenthesized_leading_comment() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table(
            "-- Converted type: NVARCHAR(MAX)\n\
             CREATE TABLE users (\n\
               id INTEGER NOT NULL,\n\
               PRIMARY KEY (id)\n\
             );",
        );

        let schema = builder.build();
        let users = schema.get_table("users").expect("users table");
        assert_eq!(users.primary_key.len(), 1);
        assert_eq!(users.columns[0].name, "id");
    }

    // -----------------------------------------------------------------------
    // Uncovered lines: normalize_qualified_identifier, extract_table_body
    // comments, inline CHECK.
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_qualified_identifier_edge_cases() {
        assert_eq!(normalize_qualified_identifier("a . b"), Some("a.b".into()));
        // Trailing dot then only whitespace: the second part is absent.
        assert_eq!(normalize_qualified_identifier("a. "), Some("a".into()));
        assert_eq!(normalize_qualified_identifier("   "), None);
        assert_eq!(normalize_qualified_identifier(""), None);
        // Unterminated / empty quoted part.
        assert_eq!(normalize_qualified_identifier("\"abc"), None);
        assert_eq!(normalize_qualified_identifier("``"), None);
        assert_eq!(normalize_qualified_identifier("[a]]b]"), Some("a]b".into()));
    }

    #[test]
    fn table_body_after_line_comment_between_name_and_paren() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE t -- comment (with parens)\n(id INT);");
        let schema = builder.build();
        assert_eq!(schema.get_table("t").unwrap().columns[0].name, "id");
    }

    #[test]
    fn line_comment_running_to_eof_yields_no_body() {
        let mut builder = SchemaBuilder::new();
        assert_eq!(
            builder.parse_create_table("CREATE TABLE t -- (id INT)"),
            None
        );
    }

    #[test]
    fn table_body_after_block_comment_between_name_and_paren() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE t /* c (x) */ /* d */ (id INT);");
        let schema = builder.build();
        assert_eq!(schema.get_table("t").unwrap().columns[0].name, "id");
    }

    #[test]
    fn unterminated_block_comment_before_body_yields_none() {
        let mut builder = SchemaBuilder::new();
        assert_eq!(
            builder.parse_create_table("CREATE TABLE t /* c (id INT);"),
            None
        );
    }

    #[test]
    fn inline_column_check_is_attached_to_table() {
        let mut builder = SchemaBuilder::new();
        builder.parse_create_table("CREATE TABLE t (a INT CHECK (a > 0), b INT CHECK);");
        let schema = builder.build();
        let t = schema.get_table("t").unwrap();
        assert_eq!(
            t.check_constraints,
            vec![CheckConstraint {
                name: None,
                expression: "a > 0".into()
            }]
        );
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[1].source_type, "INT");
    }

    // -----------------------------------------------------------------------
    // Side-by-side pins of the two DDL lexers. `tokenize_ddl` is this
    // module's char-level lexer; `convert::ddl::tokenize` is the byte-level,
    // dialect-aware one. Tests ending in `_diverges` document a disagreement
    // for the same input, one assertion per lexer.
    // -----------------------------------------------------------------------

    use crate::convert::ddl::{table_ddl_type_refs, tokenize, LexRules, Token};
    use crate::parser::SqlDialect;

    fn mysql(stmt: &str) -> Vec<Token> {
        tokenize(stmt, LexRules::for_dialect(SqlDialect::MySql))
    }

    fn pg(stmt: &str) -> Vec<Token> {
        tokenize(stmt, LexRules::for_dialect(SqlDialect::Postgres))
    }

    fn columns(stmt: &str, dialect: SqlDialect) -> Vec<String> {
        table_ddl_type_refs(stmt, dialect)
            .into_iter()
            .map(|r| r.column)
            .collect()
    }

    #[test]
    fn lexers_backslash_quote_diverges() {
        // convert, MySQL: `\'` is an escaped quote — one decoded string.
        assert_eq!(mysql(r"'it\'s'"), vec![Token::Str(0, 7, "it's".into())]);
        // convert, Postgres: `\` is literal; the string ends at the second
        // quote, `s` is an identifier, the last quote opens an empty string.
        assert_eq!(
            pg(r"'it\'s'"),
            vec![
                Token::Str(0, 5, "it\\".into()),
                Token::Ident(5, 6),
                Token::Str(6, 7, String::new()),
            ]
        );
        // schema: backslash is always an escape (MySQL semantics), raw text.
        assert_eq!(tokenize_ddl(r"'it\'s' x"), vec![r"'it\'s'", "x"]);

        let stmt = r"CREATE TABLE t (a text DEFAULT 'it\'s, z', m mood);";
        assert_eq!(columns(stmt, SqlDialect::MySql), ["a", "m"]);
        // Postgres: `, z'` splits the list and the final quote swallows `)`.
        assert_eq!(columns(stmt, SqlDialect::Postgres), ["a"]);
        assert_eq!(
            split_table_body(r"a text DEFAULT 'it\'s, z', m mood"),
            vec![r"a text DEFAULT 'it\'s, z'", "m mood"]
        );
    }

    #[test]
    fn lexers_trailing_backslash_diverges() {
        assert_eq!(mysql(r"'a\"), vec![Token::Str(0, 3, "a".into())]);
        assert_eq!(pg(r"'a\"), vec![Token::Str(0, 3, "a\\".into())]);
        assert_eq!(tokenize_ddl(r"'a\"), vec![r"'a\"]);
    }

    #[test]
    fn lexers_mysql_escape_sequences() {
        assert_eq!(
            mysql(r"'\b\n\r\t\Z\%\_\0\q'"),
            vec![Token::Str(0, 20, "\u{8}\n\r\t\u{1a}\\%\\_\0q".into())]
        );
        assert_eq!(
            pg(r"'\b\n\r\t\Z\%\_\0\q'"),
            vec![Token::Str(0, 20, r"\b\n\r\t\Z\%\_\0\q".into())]
        );
        assert_eq!(
            tokenize_ddl(r"'\b\n\r\t\Z\%\_\0\q'"),
            vec![r"'\b\n\r\t\Z\%\_\0\q'"]
        );
    }

    #[test]
    fn lexers_doubled_single_quote_agree() {
        assert_eq!(mysql("'it''s'"), vec![Token::Str(0, 7, "it's".into())]);
        assert_eq!(pg("'it''s'"), vec![Token::Str(0, 7, "it's".into())]);
        assert_eq!(tokenize_ddl("'it''s, z' x"), vec!["'it''s, z'", "x"]);
        let stmt = "CREATE TABLE t (a text DEFAULT 'it''s, z', m mood);";
        assert_eq!(columns(stmt, SqlDialect::MySql), ["a", "m"]);
        assert_eq!(columns(stmt, SqlDialect::Postgres), ["a", "m"]);
        assert_eq!(
            split_table_body("a text DEFAULT 'it''s, z', m mood"),
            vec!["a text DEFAULT 'it''s, z'", "m mood"]
        );
    }

    #[test]
    fn lexers_backtick_identifier_diverges() {
        assert_eq!(
            mysql("`a``b`"),
            vec![Token::QuotedIdent(0, 6, "a`b".into())]
        );
        // Postgres rules: backtick is a stray byte.
        assert_eq!(
            pg("`a``b`"),
            vec![
                Token::Other(0, 1),
                Token::Ident(1, 2),
                Token::Other(2, 3),
                Token::Other(3, 4),
                Token::Ident(4, 5),
                Token::Other(5, 6),
            ]
        );
        // schema: no identifier quoting; whitespace inside splits.
        assert_eq!(tokenize_ddl("`a``b`"), vec!["`a``b`"]);
        assert_eq!(tokenize_ddl("`a b`"), vec!["`a", "b`"]);
        assert_eq!(
            columns("CREATE TABLE t (`a b` int, m mood);", SqlDialect::MySql),
            ["a b", "m"]
        );
    }

    #[test]
    fn lexers_double_quote_identifier_diverges() {
        assert_eq!(
            pg(r#""a""b""#),
            vec![Token::QuotedIdent(0, 6, "a\"b".into())]
        );
        // MySQL rules: `"..."` is a string (doubling and backslashes apply).
        assert_eq!(mysql(r#""a""b""#), vec![Token::Str(0, 6, "a\"b".into())]);
        assert_eq!(mysql(r#""a\"b""#), vec![Token::Str(0, 6, "a\"b".into())]);
        assert_eq!(tokenize_ddl(r#""a""b""#), vec![r#""a""b""#]);
        assert_eq!(tokenize_ddl(r#""a b""#), vec![r#""a"#, r#"b""#]);
        assert_eq!(
            columns(
                r#"CREATE TABLE t ("a b" int, m mood);"#,
                SqlDialect::Postgres
            ),
            ["a b", "m"]
        );
    }

    #[test]
    fn lexers_bracket_identifier() {
        // convert (both dialects): brackets are punctuation, `]]` is two.
        let expected = vec![
            Token::Punct(0, b'['),
            Token::Ident(1, 2),
            Token::Punct(2, b']'),
            Token::Punct(3, b']'),
            Token::Ident(4, 5),
            Token::Punct(5, b']'),
        ];
        assert_eq!(mysql("[a]]b]"), expected);
        assert_eq!(pg("[a]]b]"), expected);
        assert_eq!(tokenize_ddl("[a]]b]"), vec!["[a]]b]"]);
        assert_eq!(tokenize_ddl("[a b]"), vec!["[a", "b]"]);
    }

    #[test]
    fn lexers_nested_parentheses_agree() {
        let stmt =
            "CREATE TABLE t (a int CHECK (a > 0 AND (b < 1)), b text DEFAULT (('x')), c mood);";
        assert_eq!(columns(stmt, SqlDialect::MySql), ["a", "b", "c"]);
        assert_eq!(columns(stmt, SqlDialect::Postgres), ["a", "b", "c"]);
        assert_eq!(
            tokenize_ddl("CHECK (a > 0 AND (b < 1)) DEFAULT (('x'))"),
            vec!["CHECK", "(a > 0 AND (b < 1))", "DEFAULT", "(('x'))"]
        );
        // Unbalanced `)` saturates instead of underflowing.
        assert_eq!(tokenize_ddl(") x"), vec![")", "x"]);
        // A paren inside a string does not change depth.
        assert_eq!(tokenize_ddl("'(' x"), vec!["'('", "x"]);
        assert_eq!(
            columns(
                "CREATE TABLE t (a text DEFAULT '(', m mood);",
                SqlDialect::Postgres
            ),
            ["a", "m"]
        );
    }

    #[test]
    fn lexers_e_string() {
        assert_eq!(
            pg(r"E'a\'b\n'"),
            vec![Token::Ident(0, 1), Token::Str(1, 9, "a'b\n".into())]
        );
        // The `E` prefix check is dialect-independent: MySQL rules decode
        // `\x41` as Postgres would (`A`), not as MySQL would (`x41`).
        assert_eq!(
            mysql(r"E'\x41'"),
            vec![Token::Ident(0, 1), Token::Str(1, 7, "A".into())]
        );
        assert_eq!(mysql(r"'\x41'"), vec![Token::Str(0, 6, "x41".into())]);
        // An identifier merely ending in `e` is not a prefix.
        assert_eq!(
            pg(r"type'\'"),
            vec![Token::Ident(0, 4), Token::Str(4, 7, "\\".into())]
        );
        // schema: raw, one token (backslash always escapes).
        assert_eq!(tokenize_ddl(r"E'a\'b\n' x"), vec![r"E'a\'b\n'", "x"]);
    }

    #[test]
    fn lexers_dollar_quoted_body_diverges() {
        assert_eq!(pg("$$a,b$$"), vec![Token::Str(0, 7, "a,b".into())]);
        assert_eq!(pg("$fn$x$fn$"), vec![Token::Str(0, 9, "x".into())]);
        // MySQL rules: `$` is an identifier byte.
        assert_eq!(
            mysql("$$a,b$$"),
            vec![
                Token::Ident(0, 3),
                Token::Punct(3, b','),
                Token::Ident(4, 7)
            ]
        );
        // schema: no dollar quoting; whitespace splits the body.
        assert_eq!(tokenize_ddl("$$a b$$"), vec!["$$a", "b$$"]);
        assert_eq!(split_table_body("$$a,b$$"), vec!["$$a", "b$$"]);
        assert_eq!(
            columns(
                "CREATE TABLE t (a text DEFAULT $$x, y$$, m mood);",
                SqlDialect::Postgres
            ),
            ["a", "m"]
        );
        assert_eq!(
            split_table_body("a text DEFAULT $$x, y$$, m mood"),
            vec!["a text DEFAULT $$x", "y$$", "m mood"]
        );
    }

    #[test]
    fn lexers_unterminated_or_invalid_dollar_tag() {
        assert_eq!(
            pg("$$abc"),
            vec![
                Token::Punct(0, b'$'),
                Token::Punct(1, b'$'),
                Token::Ident(2, 5)
            ]
        );
        assert_eq!(
            pg("$a b"),
            vec![
                Token::Punct(0, b'$'),
                Token::Ident(1, 2),
                Token::Whitespace(2, 3),
                Token::Ident(3, 4),
            ]
        );
        assert_eq!(pg("$1"), vec![Token::Punct(0, b'$'), Token::Ident(1, 2)]);
        assert_eq!(tokenize_ddl("$$abc"), vec!["$$abc"]);
    }

    #[test]
    fn lexers_comments_diverge() {
        for lex in [mysql, pg] {
            assert_eq!(
                lex("-- x\nb"),
                vec![
                    Token::Comment(0, 4),
                    Token::Whitespace(4, 5),
                    Token::Ident(5, 6)
                ]
            );
            assert_eq!(
                lex("/* a */b"),
                vec![Token::Comment(0, 7), Token::Ident(7, 8)]
            );
            assert_eq!(lex("/* a"), vec![Token::Comment(0, 4)]);
            assert_eq!(lex("'-- x'"), vec![Token::Str(0, 6, "-- x".into())]);
        }
        // schema: comment text is ordinary tokens.
        assert_eq!(tokenize_ddl("-- x\nb"), vec!["--", "x", "b"]);
        assert_eq!(tokenize_ddl("/* a */b"), vec!["/*", "a", "*/b"]);

        let stmt = "CREATE TABLE t (a int, -- trailing (x,\n b int);";
        assert_eq!(columns(stmt, SqlDialect::MySql), ["a", "b"]);
        assert_eq!(columns(stmt, SqlDialect::Postgres), ["a", "b"]);
        // schema: `(` inside the comment never closes.
        assert_eq!(
            split_table_body("a int, -- trailing (x,\n b int"),
            vec!["a int", "-- trailing (x,\n b int"]
        );
    }

    #[test]
    fn lexers_hash_comment_diverges() {
        assert_eq!(
            mysql("# c\nb"),
            vec![
                Token::Comment(0, 3),
                Token::Whitespace(3, 4),
                Token::Ident(4, 5)
            ]
        );
        assert_eq!(
            pg("# c\nb"),
            vec![
                Token::Other(0, 1),
                Token::Whitespace(1, 2),
                Token::Ident(2, 3),
                Token::Whitespace(3, 4),
                Token::Ident(4, 5),
            ]
        );
        assert_eq!(tokenize_ddl("# c\nb"), vec!["#", "c", "b"]);
    }

    #[test]
    fn lexers_unterminated_strings() {
        assert_eq!(mysql("'abc"), vec![Token::Str(0, 4, "abc".into())]);
        assert_eq!(pg("'abc"), vec![Token::Str(0, 4, "abc".into())]);
        assert_eq!(pg("\"abc"), vec![Token::QuotedIdent(0, 4, "abc".into())]);
        assert_eq!(mysql("\"abc"), vec![Token::Str(0, 4, "abc".into())]);
        assert_eq!(mysql("`abc"), vec![Token::QuotedIdent(0, 4, "abc".into())]);
        assert_eq!(tokenize_ddl("'abc def, x"), vec!["'abc def, x"]);
        assert_eq!(tokenize_ddl("(a b, c"), vec!["(a b, c"]);
        // convert: the string swallows `)`, no definition is flushed.
        assert!(columns(
            "CREATE TABLE t (a text DEFAULT 'oops, m mood);",
            SqlDialect::Postgres
        )
        .is_empty());
    }
}
