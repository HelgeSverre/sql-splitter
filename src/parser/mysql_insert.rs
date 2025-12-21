//! MySQL INSERT statement row parser.
//!
//! Parses INSERT INTO ... VALUES statements to extract individual rows
//! and optionally extract PK/FK column values for dependency tracking.

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
    /// Extracted primary key values (if table has PK and values are non-NULL)
    pub pk: Option<PkTuple>,
    /// Extracted foreign key values with their references
    /// Only includes FKs where all columns are non-NULL
    pub fk_values: Vec<(FkRef, PkTuple)>,
}

/// Parser for MySQL INSERT statements
pub struct InsertParser<'a> {
    stmt: &'a [u8],
    pos: usize,
    table_schema: Option<&'a TableSchema>,
    /// Column order in the INSERT (maps value index -> column ID)
    column_order: Vec<Option<ColumnId>>,
}

impl<'a> InsertParser<'a> {
    /// Create a new parser for an INSERT statement
    pub fn new(stmt: &'a [u8]) -> Self {
        Self {
            stmt,
            pos: 0,
            table_schema: None,
            column_order: Vec::new(),
        }
    }

    /// Set the table schema for PK/FK extraction
    pub fn with_schema(mut self, schema: &'a TableSchema) -> Self {
        self.table_schema = Some(schema);
        self
    }

    /// Parse all rows from the INSERT statement
    pub fn parse_rows(&mut self) -> anyhow::Result<Vec<ParsedRow>> {
        // Find the VALUES keyword
        let values_pos = self.find_values_keyword()?;
        self.pos = values_pos;

        // Parse column list if present
        self.parse_column_list();

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
        let stmt_str = String::from_utf8_lossy(self.stmt);
        let upper = stmt_str.to_uppercase();

        if let Some(pos) = upper.find("VALUES") {
            Ok(pos + 6) // Length of "VALUES"
        } else {
            anyhow::bail!("INSERT statement missing VALUES keyword")
        }
    }

    /// Parse optional column list after INSERT INTO table_name
    fn parse_column_list(&mut self) {
        if self.table_schema.is_none() {
            return;
        }

        let schema = self.table_schema.unwrap();

        // Look for column list between table name and VALUES
        // We need to look backwards from current position (after VALUES)
        let before_values = &self.stmt[..self.pos.saturating_sub(6)];
        let stmt_str = String::from_utf8_lossy(before_values);

        // Find the last (...) before VALUES
        if let Some(close_paren) = stmt_str.rfind(')') {
            if let Some(open_paren) = stmt_str[..close_paren].rfind('(') {
                let col_list = &stmt_str[open_paren + 1..close_paren];
                // Check if this looks like a column list (no VALUES, etc.)
                if !col_list.to_uppercase().contains("SELECT") {
                    let cols: Vec<&str> = col_list.split(',').collect();
                    self.column_order = cols
                        .iter()
                        .map(|c| {
                            let name = c.trim().trim_matches('`').trim_matches('"');
                            schema.get_column_id(name)
                        })
                        .collect();
                    return;
                }
            }
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

        // Extract PK and FK values if we have a schema
        let (pk, fk_values) = if let Some(schema) = self.table_schema {
            self.extract_pk_fk(&values, schema)
        } else {
            (None, Vec::new())
        };

        Ok(Some(ParsedRow { raw, pk, fk_values }))
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

        // String literal
        if b == b'\'' {
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
            } else if b == b'\\' {
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
                // Unknown character in number, skip to next delimiter
                while self.pos < self.stmt.len() {
                    let c = self.stmt[self.pos];
                    if c == b',' || c == b')' {
                        break;
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

    /// Extract PK and FK values from parsed values
    fn extract_pk_fk(
        &self,
        values: &[ParsedValue],
        schema: &TableSchema,
    ) -> (Option<PkTuple>, Vec<(FkRef, PkTuple)>) {
        let mut pk_values = PkTuple::new();
        let mut fk_values = Vec::new();

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
                // Find the value index for this column
                if let Some(idx) = self.column_order.iter().position(|&c| c == Some(col_id)) {
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

        (pk, fk_values)
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

/// Internal representation of a parsed value
#[derive(Debug, Clone)]
enum ParsedValue {
    Null,
    Integer(i64),
    BigInteger(i128),
    String { value: String },
    Hex(Vec<u8>),
    Other(Vec<u8>),
}

/// Parse all rows from a MySQL INSERT statement
pub fn parse_mysql_insert_rows(
    stmt: &[u8],
    schema: &TableSchema,
) -> anyhow::Result<Vec<ParsedRow>> {
    let mut parser = InsertParser::new(stmt).with_schema(schema);
    parser.parse_rows()
}

/// Parse rows without schema (just raw row extraction)
pub fn parse_mysql_insert_rows_raw(stmt: &[u8]) -> anyhow::Result<Vec<ParsedRow>> {
    let mut parser = InsertParser::new(stmt);
    parser.parse_rows()
}
