//! MySQL INSERT statement row parser.
//!
//! Parses INSERT INTO ... VALUES statements to extract individual rows
//! and optionally extract PK/FK column values for dependency tracking.

use crate::parser::SqlDialect;
use crate::schema::{ColumnId, ColumnType, TableSchema};
use ahash::AHashSet;
use smallvec::SmallVec;

/// Primary key value representation supporting common types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PkValue {
    /// Integer value (covers most PKs)
    Int(i64),
    /// Big integer value
    BigInt(i128),
    /// Text/string value
    Text(Box<str>),
    /// NULL value (typically means "no dependency" for FKs)
    Null,
}

impl PkValue {
    /// Check if this is a NULL value
    pub fn is_null(&self) -> bool {
        matches!(self, PkValue::Null)
    }
}

/// Tuple of PK values for composite primary keys
pub type PkTuple = SmallVec<[PkValue; 2]>;

/// Set of primary key values for a table (stores full tuples)
pub type PkSet = AHashSet<PkTuple>;

/// Compact hash-based set of primary keys for memory efficiency.
/// Uses 64-bit hashes instead of full values - suitable for large datasets
/// where collision risk is acceptable (sampling, validation).
pub type PkHashSet = AHashSet<u64>;

/// Hash a PK tuple into a compact 64-bit hash for memory-efficient storage.
/// Uses AHash for fast, high-quality hashing.
pub fn hash_pk_tuple(pk: &PkTuple) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();

    // Include arity (number of columns) in the hash
    (pk.len() as u8).hash(&mut hasher);

    for v in pk {
        match v {
            PkValue::Int(i) => {
                0u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            PkValue::BigInt(i) => {
                1u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            PkValue::Text(s) => {
                2u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            PkValue::Null => {
                3u8.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

/// Reference to a specific foreign key in a table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FkRef {
    /// Table containing the FK
    pub table_id: u32,
    /// Index of the FK within the table's foreign_keys vector
    pub fk_index: u16,
}

/// A parsed row from an INSERT statement
#[derive(Debug, Clone)]
pub struct ParsedRow {
    /// Raw bytes of the row value list: "(val1, val2, ...)"
    pub raw: Vec<u8>,
    /// Parsed values for each column (for bulk loading)
    pub values: Vec<ParsedValue>,
    /// Extracted primary key values (if table has PK and values are non-NULL)
    pub pk: Option<PkTuple>,
    /// Extracted foreign key values with their references
    /// Only includes FKs where all columns are non-NULL
    pub fk_values: Vec<(FkRef, PkTuple)>,
    /// All column values (for data diff comparison)
    pub all_values: Vec<PkValue>,
    /// Mapping from schema column index to value index (for finding specific columns)
    /// If column_map[schema_col_idx] == Some(val_idx), then all_values[val_idx] is the value
    pub column_map: Vec<Option<usize>>,
}

impl ParsedRow {
    /// Get the value for a specific schema column index
    pub fn get_column_value(&self, schema_col_index: usize) -> Option<&PkValue> {
        self.column_map
            .get(schema_col_index)
            .and_then(|v| *v)
            .and_then(|val_idx| self.all_values.get(val_idx))
    }
}

#[inline]
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Find the byte offset just past the `VALUES` keyword, matching it as a
/// keyword rather than a substring: it must be word-boundaried and must not
/// appear inside a quoted identifier (backtick / double-quote / bracket) or a
/// string literal. This prevents tables like `product_values` or
/// `` `order values` `` from being mistaken for the VALUES clause (bug #2).
pub(crate) fn find_values_keyword_pos(stmt: &[u8]) -> Option<usize> {
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_bracket = false;
    let mut i = 0;

    while i < stmt.len() {
        let b = stmt[i];

        if in_single {
            // Only need to find the end of the string; treat '' as an escaped
            // quote so we don't exit early. Backslash handling is irrelevant
            // here because string literals only ever follow VALUES.
            if b == b'\'' {
                if stmt.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                in_double = false;
            }
            i += 1;
            continue;
        }
        if in_backtick {
            if b == b'`' {
                in_backtick = false;
            }
            i += 1;
            continue;
        }
        if in_bracket {
            if b == b']' {
                in_bracket = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => {
                in_single = true;
                i += 1;
                continue;
            }
            b'"' => {
                in_double = true;
                i += 1;
                continue;
            }
            b'`' => {
                in_backtick = true;
                i += 1;
                continue;
            }
            b'[' => {
                in_bracket = true;
                i += 1;
                continue;
            }
            _ => {}
        }

        if (b == b'V' || b == b'v')
            && i + 6 <= stmt.len()
            && stmt[i..i + 6].eq_ignore_ascii_case(b"VALUES")
        {
            let before_ok = i == 0 || !is_ident_byte(stmt[i - 1]);
            let after_ok = stmt.get(i + 6).is_none_or(|&c| !is_ident_byte(c));
            if before_ok && after_ok {
                return Some(i + 6);
            }
        }

        i += 1;
    }

    None
}

/// Extract a column list `(a, b, c)` appearing immediately before the VALUES
/// keyword, given the byte offset just past VALUES. Shared by
/// [`InsertParser::parse_column_list`] and [`parse_insert_for_bulk`] (bug #2).
pub(crate) fn extract_column_list_before(stmt: &[u8], values_pos: usize) -> Option<Vec<String>> {
    let before = &stmt[..values_pos.saturating_sub(6)];
    let s = String::from_utf8_lossy(before);

    let close = s.rfind(')')?;
    let open = s[..close].rfind('(')?;
    let col_list = &s[open + 1..close];

    let upper = col_list.to_uppercase();
    if col_list.trim().is_empty() || upper.contains("SELECT") {
        return None;
    }

    let columns: Vec<String> = col_list
        .split(',')
        .map(|c| {
            c.trim()
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('[')
                .trim_matches(']')
                .to_string()
        })
        .collect();

    if columns.is_empty() {
        None
    } else {
        Some(columns)
    }
}

/// Parser for MySQL INSERT statements
pub struct InsertParser<'a> {
    stmt: &'a [u8],
    pos: usize,
    table_schema: Option<&'a TableSchema>,
    /// Column order in the INSERT (maps value index -> column ID)
    column_order: Vec<Option<ColumnId>>,
    /// Reverse of `column_order` (schema column ordinal -> value index),
    /// computed once per statement so per-row FK/column lookups are O(1).
    col_to_value: Vec<Option<usize>>,
    /// Dialect governs string escaping. Only MySQL treats `\` as an escape
    /// character; Postgres/SQLite/MSSQL treat it as a literal byte.
    dialect: SqlDialect,
}

impl<'a> InsertParser<'a> {
    /// Create a new parser for an INSERT statement (defaults to MySQL escaping)
    pub fn new(stmt: &'a [u8]) -> Self {
        Self {
            stmt,
            pos: 0,
            table_schema: None,
            column_order: Vec::new(),
            col_to_value: Vec::new(),
            dialect: SqlDialect::MySql,
        }
    }

    /// Set the table schema for PK/FK extraction
    pub fn with_schema(mut self, schema: &'a TableSchema) -> Self {
        self.table_schema = Some(schema);
        self
    }

    /// Set the dialect so string values are unescaped correctly (bug #3).
    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Parse all rows from the INSERT statement
    pub fn parse_rows(&mut self) -> anyhow::Result<Vec<ParsedRow>> {
        // Find the VALUES keyword
        let values_pos = self.find_values_keyword()?;
        self.pos = values_pos;

        // Parse column list if present
        self.parse_column_list();

        // Precompute the reverse column map once for the whole statement.
        if let Some(schema) = self.table_schema {
            let mut map = vec![None; schema.columns.len()];
            for (val_idx, col_id_opt) in self.column_order.iter().enumerate() {
                if let Some(col_id) = col_id_opt {
                    let ord = col_id.0 as usize;
                    if ord < map.len() {
                        map[ord] = Some(val_idx);
                    }
                }
            }
            self.col_to_value = map;
        }

        // Parse each row
        let mut rows = Vec::new();
        while self.pos < self.stmt.len() {
            self.skip_whitespace();

            if self.pos >= self.stmt.len() {
                break;
            }

            if self.stmt[self.pos] == b'(' {
                if let Some(row) = self.parse_row()? {
                    rows.push(row);
                }
            } else if self.stmt[self.pos] == b',' {
                self.pos += 1;
            } else if self.stmt[self.pos] == b';' {
                break;
            } else {
                self.pos += 1;
            }
        }

        Ok(rows)
    }

    /// Find the VALUES keyword and return position after it
    fn find_values_keyword(&self) -> anyhow::Result<usize> {
        find_values_keyword_pos(self.stmt)
            .ok_or_else(|| anyhow::anyhow!("INSERT statement missing VALUES keyword"))
    }

    /// Parse optional column list after INSERT INTO table_name
    fn parse_column_list(&mut self) {
        if self.table_schema.is_none() {
            return;
        }

        let schema = self.table_schema.unwrap();

        // self.pos is just past VALUES; look for a column list immediately
        // before it (shared with the bulk-loader path).
        if let Some(cols) = extract_column_list_before(self.stmt, self.pos) {
            self.column_order = cols.iter().map(|name| schema.get_column_id(name)).collect();
            return;
        }

        // No explicit column list - use natural order
        self.column_order = schema.columns.iter().map(|c| Some(c.ordinal)).collect();
    }

    /// Parse a single row "(val1, val2, ...)"
    fn parse_row(&mut self) -> anyhow::Result<Option<ParsedRow>> {
        self.skip_whitespace();

        if self.pos >= self.stmt.len() || self.stmt[self.pos] != b'(' {
            return Ok(None);
        }

        let start = self.pos;
        self.pos += 1; // Skip '('

        let mut values: Vec<ParsedValue> = Vec::new();
        let mut depth = 1;

        while self.pos < self.stmt.len() && depth > 0 {
            self.skip_whitespace();

            if self.pos >= self.stmt.len() {
                break;
            }

            match self.stmt[self.pos] {
                b'(' => {
                    depth += 1;
                    self.pos += 1;
                }
                b')' => {
                    depth -= 1;
                    self.pos += 1;
                }
                b',' if depth == 1 => {
                    self.pos += 1;
                }
                _ if depth == 1 => {
                    values.push(self.parse_value()?);
                }
                _ => {
                    self.pos += 1;
                }
            }
        }

        let end = self.pos;
        let raw = self.stmt[start..end].to_vec();

        // Extract PK, FK, all values, and column map if we have a schema
        let (pk, fk_values, all_values, column_map) = if let Some(schema) = self.table_schema {
            let (pk, fk_values, all_values) = self.extract_pk_fk(&values, schema);
            let column_map = self.build_column_map(schema);
            (pk, fk_values, all_values, column_map)
        } else {
            (None, Vec::new(), Vec::new(), Vec::new())
        };

        Ok(Some(ParsedRow {
            raw,
            values,
            pk,
            fk_values,
            all_values,
            column_map,
        }))
    }

    /// Parse a single value (string, number, NULL, etc.)
    fn parse_value(&mut self) -> anyhow::Result<ParsedValue> {
        self.skip_whitespace();

        if self.pos >= self.stmt.len() {
            return Ok(ParsedValue::Null);
        }

        let b = self.stmt[self.pos];

        // NULL
        if self.pos + 4 <= self.stmt.len() {
            let word = &self.stmt[self.pos..self.pos + 4];
            if word.eq_ignore_ascii_case(b"NULL") {
                self.pos += 4;
                return Ok(ParsedValue::Null);
            }
        }

        // String literal (including MSSQL N'...' Unicode prefix)
        if b == b'\'' {
            return self.parse_string_value();
        }

        // MSSQL N'...' Unicode string literal
        if (b == b'N' || b == b'n')
            && self.pos + 1 < self.stmt.len()
            && self.stmt[self.pos + 1] == b'\''
        {
            self.pos += 1; // Skip the N prefix
            return self.parse_string_value();
        }

        // Hex literal (0x...)
        if b == b'0' && self.pos + 1 < self.stmt.len() {
            let next = self.stmt[self.pos + 1];
            if next == b'x' || next == b'X' {
                return self.parse_hex_value();
            }
        }

        // Number or expression
        self.parse_number_value()
    }

    /// Parse a string literal 'value'
    fn parse_string_value(&mut self) -> anyhow::Result<ParsedValue> {
        self.pos += 1; // Skip opening quote

        // Backslash is only an escape character in MySQL. Postgres (standard
        // strings), SQLite and MSSQL treat it as a literal byte, so applying
        // MySQL unescaping there corrupts Windows paths, regexes, JSON, etc.
        let honor_backslash = self.dialect == SqlDialect::MySql;

        let mut value = Vec::new();
        let mut escape_next = false;

        while self.pos < self.stmt.len() {
            let b = self.stmt[self.pos];

            if escape_next {
                // Handle MySQL escape sequences
                let escaped = match b {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'0' => 0,
                    _ => b, // \', \\, etc.
                };
                value.push(escaped);
                escape_next = false;
                self.pos += 1;
            } else if b == b'\\' && honor_backslash {
                escape_next = true;
                self.pos += 1;
            } else if b == b'\'' {
                // Check for escaped quote ''
                if self.pos + 1 < self.stmt.len() && self.stmt[self.pos + 1] == b'\'' {
                    value.push(b'\'');
                    self.pos += 2;
                } else {
                    self.pos += 1; // End of string
                    break;
                }
            } else {
                value.push(b);
                self.pos += 1;
            }
        }

        let text = String::from_utf8_lossy(&value).into_owned();

        Ok(ParsedValue::String { value: text })
    }

    /// Parse a hex literal 0xABCD...
    fn parse_hex_value(&mut self) -> anyhow::Result<ParsedValue> {
        let start = self.pos;
        self.pos += 2; // Skip 0x

        while self.pos < self.stmt.len() {
            let b = self.stmt[self.pos];
            if b.is_ascii_hexdigit() {
                self.pos += 1;
            } else {
                break;
            }
        }

        let raw = self.stmt[start..self.pos].to_vec();
        Ok(ParsedValue::Hex(raw))
    }

    /// Parse a number or other non-string value
    fn parse_number_value(&mut self) -> anyhow::Result<ParsedValue> {
        let start = self.pos;
        let mut has_dot = false;

        // Handle leading minus
        if self.pos < self.stmt.len() && self.stmt[self.pos] == b'-' {
            self.pos += 1;
        }

        while self.pos < self.stmt.len() {
            let b = self.stmt[self.pos];
            if b.is_ascii_digit() {
                self.pos += 1;
            } else if b == b'.' && !has_dot {
                has_dot = true;
                self.pos += 1;
            } else if b == b'e' || b == b'E' {
                // Scientific notation
                self.pos += 1;
                if self.pos < self.stmt.len()
                    && (self.stmt[self.pos] == b'+' || self.stmt[self.pos] == b'-')
                {
                    self.pos += 1;
                }
            } else if b == b',' || b == b')' || b.is_ascii_whitespace() {
                break;
            } else {
                // Unknown character: this is an expression/function call rather
                // than a plain number. Skip to the delimiter that ends this
                // value, respecting string/identifier quotes (' and ") and
                // nested parentheses so a value like
                // ST_GeomFromText('POLYGON((0 0,1 1))') or CONCAT("x)y", "z")
                // isn't split inside its own string/parens (bug #11).
                let mut depth = 0i32;
                let mut quote: Option<u8> = None;
                let honor_backslash = self.dialect == SqlDialect::MySql;
                while self.pos < self.stmt.len() {
                    let c = self.stmt[self.pos];
                    if let Some(q) = quote {
                        if c == b'\\' && honor_backslash {
                            self.pos += 2;
                            continue;
                        }
                        if c == q {
                            // A doubled quote ('' or "") stays inside.
                            if self.stmt.get(self.pos + 1) == Some(&q) {
                                self.pos += 2;
                                continue;
                            }
                            quote = None;
                        }
                        self.pos += 1;
                        continue;
                    }
                    match c {
                        b'\'' | b'"' => quote = Some(c),
                        b'(' => depth += 1,
                        b')' if depth > 0 => depth -= 1,
                        b')' => break,
                        b',' if depth == 0 => break,
                        _ => {}
                    }
                    self.pos += 1;
                }
                break;
            }
        }

        let raw = self.stmt[start..self.pos].to_vec();
        let value_str = String::from_utf8_lossy(&raw);

        // Try to parse as integer
        if !has_dot {
            if let Ok(n) = value_str.parse::<i64>() {
                return Ok(ParsedValue::Integer(n));
            }
            if let Ok(n) = value_str.parse::<i128>() {
                return Ok(ParsedValue::BigInteger(n));
            }
        }

        // Fall back to raw value
        Ok(ParsedValue::Other(raw))
    }

    /// Skip whitespace and newlines
    fn skip_whitespace(&mut self) {
        while self.pos < self.stmt.len() {
            let b = self.stmt[self.pos];
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Extract PK, FK, and all values from parsed values
    fn extract_pk_fk(
        &self,
        values: &[ParsedValue],
        schema: &TableSchema,
    ) -> (Option<PkTuple>, Vec<(FkRef, PkTuple)>, Vec<PkValue>) {
        let mut pk_values = PkTuple::new();
        let mut fk_values = Vec::new();

        // Build all_values: convert each value to PkValue
        let all_values: Vec<PkValue> = values
            .iter()
            .enumerate()
            .map(|(idx, v)| {
                let col = self
                    .column_order
                    .get(idx)
                    .and_then(|c| *c)
                    .and_then(|id| schema.column(id));
                self.value_to_pk(v, col)
            })
            .collect();

        // Build PK from columns marked as primary key
        for (idx, col_id_opt) in self.column_order.iter().enumerate() {
            if let Some(col_id) = col_id_opt {
                if schema.is_pk_column(*col_id) {
                    if let Some(value) = values.get(idx) {
                        let pk_val = self.value_to_pk(value, schema.column(*col_id));
                        pk_values.push(pk_val);
                    }
                }
            }
        }

        // Build FK tuples
        for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
            if fk.referenced_table_id.is_none() {
                continue;
            }

            let mut fk_tuple = PkTuple::new();
            let mut all_non_null = true;

            for &col_id in &fk.columns {
                // Find the value index for this column via the precomputed map.
                if let Some(idx) = self.col_to_value.get(col_id.0 as usize).copied().flatten() {
                    if let Some(value) = values.get(idx) {
                        let pk_val = self.value_to_pk(value, schema.column(col_id));
                        if pk_val.is_null() {
                            all_non_null = false;
                            break;
                        }
                        fk_tuple.push(pk_val);
                    }
                }
            }

            if all_non_null && !fk_tuple.is_empty() {
                fk_values.push((
                    FkRef {
                        table_id: schema.id.0,
                        fk_index: fk_idx as u16,
                    },
                    fk_tuple,
                ));
            }
        }

        let pk = if pk_values.is_empty() || pk_values.iter().any(|v| v.is_null()) {
            None
        } else {
            Some(pk_values)
        };

        (pk, fk_values, all_values)
    }

    /// The schema-column-ordinal -> value-index map, precomputed once per
    /// statement in [`parse_rows`].
    fn build_column_map(&self, _schema: &TableSchema) -> Vec<Option<usize>> {
        self.col_to_value.clone()
    }

    /// Convert a parsed value to a PkValue
    fn value_to_pk(&self, value: &ParsedValue, col: Option<&crate::schema::Column>) -> PkValue {
        match value {
            ParsedValue::Null => PkValue::Null,
            ParsedValue::Integer(n) => PkValue::Int(*n),
            ParsedValue::BigInteger(n) => PkValue::BigInt(*n),
            ParsedValue::String { value } => {
                // Check if this might be an integer stored as string
                if let Some(col) = col {
                    match col.col_type {
                        ColumnType::Int => {
                            if let Ok(n) = value.parse::<i64>() {
                                return PkValue::Int(n);
                            }
                        }
                        ColumnType::BigInt => {
                            if let Ok(n) = value.parse::<i128>() {
                                return PkValue::BigInt(n);
                            }
                        }
                        _ => {}
                    }
                }
                PkValue::Text(value.clone().into_boxed_str())
            }
            ParsedValue::Hex(raw) => {
                PkValue::Text(String::from_utf8_lossy(raw).into_owned().into_boxed_str())
            }
            ParsedValue::Other(raw) => {
                PkValue::Text(String::from_utf8_lossy(raw).into_owned().into_boxed_str())
            }
        }
    }
}

/// Parsed value from an INSERT statement
///
/// Used for bulk loading via DuckDB Appender and for PK/FK extraction.
#[derive(Debug, Clone)]
pub enum ParsedValue {
    /// NULL value
    Null,
    /// Integer value (fits in i64)
    Integer(i64),
    /// Big integer value (requires i128)
    BigInteger(i128),
    /// String/text value (already unescaped)
    String { value: String },
    /// Hex literal (0xABCD...)
    Hex(Vec<u8>),
    /// Other value (decimals, floats, expressions) as raw bytes
    Other(Vec<u8>),
}

/// Parse all rows from an INSERT statement using the given dialect's escaping.
pub fn parse_insert_rows(
    stmt: &[u8],
    schema: &TableSchema,
    dialect: SqlDialect,
) -> anyhow::Result<Vec<ParsedRow>> {
    let mut parser = InsertParser::new(stmt)
        .with_schema(schema)
        .with_dialect(dialect);
    parser.parse_rows()
}

/// Parse all rows from a MySQL INSERT statement
pub fn parse_mysql_insert_rows(
    stmt: &[u8],
    schema: &TableSchema,
) -> anyhow::Result<Vec<ParsedRow>> {
    parse_insert_rows(stmt, schema, SqlDialect::MySql)
}

/// Parse rows without schema (just raw row extraction)
#[allow(dead_code)]
pub fn parse_mysql_insert_rows_raw(stmt: &[u8]) -> anyhow::Result<Vec<ParsedRow>> {
    let mut parser = InsertParser::new(stmt);
    parser.parse_rows()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert_for_bulk_simple() {
        let sql = b"INSERT INTO users VALUES (1, 'Alice')";
        let result = parse_insert_for_bulk(sql, SqlDialect::MySql).unwrap();
        assert_eq!(result.table, "users");
        assert!(result.columns.is_none());
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_parse_insert_for_bulk_with_columns() {
        let sql = b"INSERT INTO users (name, id) VALUES ('Alice', 1)";
        let result = parse_insert_for_bulk(sql, SqlDialect::MySql).unwrap();
        assert_eq!(result.table, "users");
        assert_eq!(
            result.columns,
            Some(vec!["name".to_string(), "id".to_string()])
        );
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_parse_insert_for_bulk_mssql() {
        let sql =
            b"INSERT INTO [dbo].[users] ([email], [name]) VALUES (N'alice@example.com', N'Alice')";
        let result = parse_insert_for_bulk(sql, SqlDialect::Mssql).unwrap();
        assert_eq!(result.table, "users");
        assert_eq!(
            result.columns,
            Some(vec!["email".to_string(), "name".to_string()])
        );
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_parse_insert_for_bulk_mysql() {
        let sql = b"INSERT INTO `users` (`id`, `name`) VALUES (1, 'Bob')";
        let result = parse_insert_for_bulk(sql, SqlDialect::MySql).unwrap();
        assert_eq!(result.table, "users");
        assert_eq!(
            result.columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
        assert_eq!(result.rows.len(), 1);
    }
}

/// Result of parsing INSERT values for bulk loading
#[derive(Debug, Clone)]
pub struct InsertValues {
    /// Table name (without schema prefix or quotes)
    pub table: String,
    /// Column list if specified, None if using natural order
    pub columns: Option<Vec<String>>,
    /// Parsed rows with values
    pub rows: Vec<Vec<ParsedValue>>,
}

/// Parse INSERT statement for bulk loading (extracts table, columns, and values)
///
/// This function extracts table name, optional column list, and all VALUES
/// from an INSERT statement without requiring a schema. It's optimized for
/// bulk loading into DuckDB via the Appender API.
pub fn parse_insert_for_bulk(stmt: &[u8], dialect: SqlDialect) -> anyhow::Result<InsertValues> {
    let stmt_str = String::from_utf8_lossy(stmt);
    let upper = stmt_str.to_uppercase();

    // Extract table name: INSERT INTO [schema.]table_name [(columns)] VALUES
    let table = extract_insert_table_name(&stmt_str, &upper)?;

    // Locate VALUES as a keyword (not a substring) so tables/columns containing
    // "values" don't shift parsing (bug #2), then extract the column list.
    let columns =
        find_values_keyword_pos(stmt).and_then(|pos| extract_column_list_before(stmt, pos));

    // Parse rows using the existing parser
    let mut parser = InsertParser::new(stmt).with_dialect(dialect);
    let parsed_rows = parser.parse_rows()?;

    let rows = parsed_rows.into_iter().map(|r| r.values).collect();

    Ok(InsertValues {
        table,
        columns,
        rows,
    })
}

/// Extract table name from INSERT statement
fn extract_insert_table_name(stmt: &str, upper: &str) -> anyhow::Result<String> {
    // Find "INSERT INTO" or "INSERT"
    let start_pos = if let Some(pos) = upper.find("INSERT INTO") {
        pos + 11 // Length of "INSERT INTO"
    } else if let Some(pos) = upper.find("INSERT") {
        pos + 6 // Length of "INSERT"
    } else {
        anyhow::bail!("Not an INSERT statement");
    };

    // Skip whitespace
    let remaining = stmt[start_pos..].trim_start();

    // Extract the full table reference (might be schema.table or just table)
    let table_ref = extract_table_reference(remaining)?;

    // Strip schema prefix if present
    if let Some(dot_pos) = table_ref.rfind('.') {
        let table_part = &table_ref[dot_pos + 1..];
        Ok(strip_identifier_quotes(table_part))
    } else {
        Ok(strip_identifier_quotes(&table_ref))
    }
}

/// Extract a full table reference (e.g., "[dbo].[users]" or "schema.table")
fn extract_table_reference(s: &str) -> anyhow::Result<String> {
    let s = s.trim();

    if s.is_empty() {
        anyhow::bail!("Empty table reference");
    }

    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            '[' => {
                // MSSQL bracket quoting
                chars.next();
                result.push('[');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    result.push(inner);
                    if inner == ']' {
                        break;
                    }
                }
            }
            '`' => {
                // MySQL backtick quoting
                chars.next();
                result.push('`');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    result.push(inner);
                    if inner == '`' {
                        break;
                    }
                }
            }
            '"' => {
                // PostgreSQL/SQLite double-quote
                chars.next();
                result.push('"');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    result.push(inner);
                    if inner == '"' {
                        break;
                    }
                }
            }
            '.' => {
                // Schema separator
                chars.next();
                result.push('.');
            }
            c if c.is_whitespace() || c == '(' || c == ',' => {
                // End of table reference
                break;
            }
            _ => {
                // Regular identifier character
                chars.next();
                result.push(c);
            }
        }
    }

    if result.is_empty() {
        anyhow::bail!("Empty table reference");
    }

    Ok(result)
}

/// Strip quotes from an identifier
fn strip_identifier_quotes(s: &str) -> String {
    s.trim_matches('`')
        .trim_matches('"')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}
