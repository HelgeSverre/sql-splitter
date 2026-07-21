//! MySQL INSERT statement row parser.
//!
//! Parses INSERT INTO ... VALUES statements to extract individual rows
//! and optionally extract PK/FK column values for dependency tracking.

use crate::parser::{RowFlow, SqlDialect};
use crate::schema::{ColumnId, ColumnType, TableSchema};
use ahash::AHashSet;
use smallvec::SmallVec;
use std::sync::Arc;

/// How much derived data to compute per parsed row.
///
/// PK/FK extraction and especially `all_values` (one `PkValue` — often a
/// `String` allocation — per column per row) dominate row-parsing cost, so
/// consumers that don't need them should opt out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowExtraction {
    /// Only `raw` and `values` — no PK/FK/all_values work (e.g. redact).
    ValuesOnly,
    /// `pk` and `fk_values`, skipping `all_values` (e.g. sample, validate).
    PkFk,
    /// Everything, including `all_values` for full-row comparison
    /// (e.g. diff, shard).
    #[default]
    Full,
}

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
            // Int and BigInt of the same integer must hash identically: a value
            // dumped unquoted (Int) and the same value dumped quoted in a wider
            // column (BigInt) are the same key for hash-based FK/PK matching.
            // Normalize both to a common tag and width.
            PkValue::Int(i) => {
                0u8.hash(&mut hasher);
                i128::from(*i).hash(&mut hasher);
            }
            PkValue::BigInt(i) => {
                0u8.hash(&mut hasher);
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
    /// All column values (for data diff comparison).
    /// Empty unless parsed with [`RowExtraction::Full`].
    pub all_values: Vec<PkValue>,
    /// Mapping from schema column index to value index (for finding specific columns).
    /// If column_map[schema_col_idx] == Some(val_idx), then all_values[val_idx] is the value.
    /// Shared across all rows of a statement (computed once).
    pub column_map: Arc<[Option<usize>]>,
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

/// Copy bytes into a `String`, validating UTF-8 once and falling back to a
/// lossy re-encode only for invalid input. Unlike `from_utf8_lossy(..).into_owned()`,
/// the valid path is a straight validate + copy with no chunk iteration.
#[inline]
pub(crate) fn bytes_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_owned(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
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

/// Build the reverse column map (schema column ordinal -> value index) for a
/// statement/block, shared with every row via `Arc` instead of cloned per row.
/// Used by both the INSERT and COPY parsers.
pub(crate) fn build_col_to_value(
    column_order: &[Option<ColumnId>],
    n_columns: usize,
) -> Arc<[Option<usize>]> {
    let mut map = vec![None; n_columns];
    for (val_idx, col_id_opt) in column_order.iter().enumerate() {
        if let Some(col_id) = col_id_opt {
            let ord = col_id.0 as usize;
            if ord < map.len() {
                map[ord] = Some(val_idx);
            }
        }
    }
    map.into()
}

/// Coerce a textual value to an integer [`PkValue`] when the schema column is
/// `Int`/`BigInt` (integer PKs are often dumped as quoted strings). Shared by
/// the INSERT and COPY `value_to_pk` paths so the coercion rules can't drift
/// between MySQL and Postgres dumps.
pub(crate) fn coerce_text_pk(s: &str, col: Option<&crate::schema::Column>) -> Option<PkValue> {
    match col?.col_type {
        ColumnType::Int => s.parse::<i64>().ok().map(PkValue::Int),
        ColumnType::BigInt => s.parse::<i128>().ok().map(PkValue::BigInt),
        _ => None,
    }
}

/// Extract PK tuple, FK tuples and all column values from one parsed row,
/// shared by the INSERT and COPY parsers. `to_pk` converts a dialect-specific
/// value into a [`PkValue`]; `column_order` maps value index -> column, and
/// `col_to_value` is its precomputed inverse (schema ordinal -> value index).
/// `all_values` (a `PkValue` per column, often a `String` allocation) is only
/// built when `include_all_values` is set — see [`RowExtraction`].
#[allow(clippy::type_complexity)]
pub(crate) fn extract_pk_fk_generic<V>(
    values: &[V],
    schema: &TableSchema,
    column_order: &[Option<ColumnId>],
    col_to_value: &[Option<usize>],
    include_all_values: bool,
    to_pk: impl Fn(&V, Option<&crate::schema::Column>) -> PkValue,
) -> (Option<PkTuple>, Vec<(FkRef, PkTuple)>, Vec<PkValue>) {
    // All column values, in value order.
    let all_values: Vec<PkValue> = if include_all_values {
        values
            .iter()
            .enumerate()
            .map(|(idx, v)| {
                let col = column_order
                    .get(idx)
                    .and_then(|c| *c)
                    .and_then(|id| schema.column(id));
                to_pk(v, col)
            })
            .collect()
    } else {
        Vec::new()
    };

    // PK from columns marked primary key.
    let mut pk_values = PkTuple::new();
    for (idx, col_id_opt) in column_order.iter().enumerate() {
        if let Some(col_id) = col_id_opt {
            if schema.is_pk_column(*col_id) {
                if let Some(value) = values.get(idx) {
                    pk_values.push(to_pk(value, schema.column(*col_id)));
                }
            }
        }
    }

    // FK tuples (only when every FK column is non-NULL).
    let mut fk_values = Vec::new();
    for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
        if fk.referenced_table_id.is_none() {
            continue;
        }
        let mut fk_tuple = PkTuple::new();
        let mut all_non_null = true;
        for &col_id in &fk.columns {
            if let Some(idx) = col_to_value.get(col_id.0 as usize).copied().flatten() {
                if let Some(value) = values.get(idx) {
                    let pk_val = to_pk(value, schema.column(col_id));
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

/// Parser for MySQL INSERT statements
pub struct InsertParser<'a> {
    stmt: &'a [u8],
    pos: usize,
    table_schema: Option<&'a TableSchema>,
    /// Column order in the INSERT (maps value index -> column ID)
    column_order: Vec<Option<ColumnId>>,
    /// Reverse of `column_order` (schema column ordinal -> value index),
    /// computed once per statement and shared with every row via `Arc`.
    col_to_value: Arc<[Option<usize>]>,
    /// Dialect governs string escaping. Only MySQL treats `\` as an escape
    /// character; Postgres/SQLite/MSSQL treat it as a literal byte.
    dialect: SqlDialect,
    /// How much derived data to compute per row.
    extraction: RowExtraction,
    /// Set by [`InsertParser::parse_row`]: whether the last tuple closed its
    /// parenthesis within the available bytes (`true`) or ran out mid-tuple
    /// (`false`). Consumed by [`scan_insert_tuple`] for incremental streaming.
    last_complete: bool,
}

impl<'a> InsertParser<'a> {
    /// Create a new parser for an INSERT statement (defaults to MySQL escaping)
    pub fn new(stmt: &'a [u8]) -> Self {
        Self {
            stmt,
            pos: 0,
            table_schema: None,
            column_order: Vec::new(),
            col_to_value: Arc::from(Vec::new()),
            dialect: SqlDialect::MySql,
            extraction: RowExtraction::Full,
            last_complete: true,
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

    /// Choose how much per-row derived data to compute (default: [`RowExtraction::Full`]).
    pub fn with_extraction(mut self, extraction: RowExtraction) -> Self {
        self.extraction = extraction;
        self
    }

    /// Parse all rows from the INSERT statement, collecting them into a `Vec`.
    ///
    /// A thin collecting adapter over [`InsertParser::parse_rows_visit`].
    pub fn parse_rows(&mut self) -> anyhow::Result<Vec<ParsedRow>> {
        let mut rows = Vec::new();
        self.parse_rows_visit(|row| {
            rows.push(row);
            Ok(RowFlow::Continue)
        })?;
        Ok(rows)
    }

    /// Stream each parsed row to `f` without collecting a statement-sized `Vec`.
    ///
    /// Only the current row is materialized at a time. `f` returns a
    /// [`RowFlow`]: `SkipStatement`/`Stop` both stop this statement's rows.
    pub fn parse_rows_visit<F>(&mut self, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(ParsedRow) -> anyhow::Result<RowFlow>,
    {
        // Find the VALUES keyword
        let values_pos = self.find_values_keyword()?;
        self.pos = values_pos;

        // Parse column list if present
        self.parse_column_list();

        // Precompute the reverse column map once for the whole statement.
        if let Some(schema) = self.table_schema {
            self.col_to_value = build_col_to_value(&self.column_order, schema.columns.len());
        }

        // Parse each row
        while self.pos < self.stmt.len() {
            self.skip_whitespace();

            if self.pos >= self.stmt.len() {
                break;
            }

            if self.stmt[self.pos] == b'(' {
                if let Some(row) = self.parse_row() {
                    match f(row)? {
                        RowFlow::Continue => {}
                        RowFlow::SkipStatement | RowFlow::Stop => break,
                    }
                }
            } else if self.stmt[self.pos] == b',' {
                self.pos += 1;
            } else if self.stmt[self.pos] == b';' {
                break;
            } else {
                self.pos += 1;
            }
        }

        Ok(())
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

    /// Parse a single row "(val1, val2, ...)". Infallible: malformed input
    /// degrades to best-effort values rather than an error (see `parse_value`).
    fn parse_row(&mut self) -> Option<ParsedRow> {
        self.skip_whitespace();

        if self.pos >= self.stmt.len() || self.stmt[self.pos] != b'(' {
            return None;
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
                    values.push(self.parse_value());
                }
                _ => {
                    self.pos += 1;
                }
            }
        }

        // depth == 0 means we consumed the matching ')'; depth > 0 means the
        // slice ended mid-tuple (used by the incremental streaming scanner).
        self.last_complete = depth == 0;

        let end = self.pos;
        let raw = self.stmt[start..end].to_vec();

        // Extract PK, FK, and (for Full extraction) all values if we have a schema
        let (pk, fk_values, all_values) = match (self.table_schema, self.extraction) {
            (Some(schema), RowExtraction::PkFk | RowExtraction::Full) => {
                self.extract_pk_fk(&values, schema)
            }
            _ => (None, Vec::new(), Vec::new()),
        };

        Some(ParsedRow {
            raw,
            values,
            pk,
            fk_values,
            all_values,
            column_map: Arc::clone(&self.col_to_value),
        })
    }

    /// Parse a single value (string, number, NULL, etc.)
    fn parse_value(&mut self) -> ParsedValue {
        self.skip_whitespace();

        if self.pos >= self.stmt.len() {
            return ParsedValue::Null;
        }

        let b = self.stmt[self.pos];

        // NULL
        if self.pos + 4 <= self.stmt.len() {
            let word = &self.stmt[self.pos..self.pos + 4];
            if word.eq_ignore_ascii_case(b"NULL") {
                self.pos += 4;
                return ParsedValue::Null;
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
    fn parse_string_value(&mut self) -> ParsedValue {
        self.pos += 1; // Skip opening quote

        // Backslash is only an escape character in MySQL. Postgres (standard
        // strings), SQLite and MSSQL treat it as a literal byte, so applying
        // MySQL unescaping there corrupts Windows paths, regexes, JSON, etc.
        let honor_backslash = self.dialect == SqlDialect::MySql;

        // memchr from one significant byte (quote/backslash) to the next and
        // bulk-copy the plain bytes in between. `owned` stays empty on the
        // fast path (no escapes at all), where the literal is a single slice.
        let mut owned: Vec<u8> = Vec::new();
        let mut chunk_start = self.pos;

        let content_end = loop {
            let rest = &self.stmt[self.pos..];
            let hit = if honor_backslash {
                memchr::memchr2(b'\'', b'\\', rest)
            } else {
                memchr::memchr(b'\'', rest)
            };
            let Some(off) = hit else {
                // Unterminated literal: take everything to the end.
                self.pos = self.stmt.len();
                break self.pos;
            };
            self.pos += off;

            if self.stmt[self.pos] == b'\\' {
                owned.extend_from_slice(&self.stmt[chunk_start..self.pos]);
                match self.stmt.get(self.pos + 1) {
                    Some(&c) => {
                        // MySQL escape sequences
                        let escaped = match c {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'0' => 0,
                            _ => c, // \', \\, etc.
                        };
                        owned.push(escaped);
                        self.pos += 2;
                    }
                    // Trailing backslash at end of statement: drop it.
                    None => self.pos += 1,
                }
                chunk_start = self.pos;
            } else if self.stmt.get(self.pos + 1) == Some(&b'\'') {
                // Doubled quote '' is an escaped quote: keep one.
                owned.extend_from_slice(&self.stmt[chunk_start..=self.pos]);
                self.pos += 2;
                chunk_start = self.pos;
            } else {
                let end = self.pos;
                self.pos += 1; // Consume the closing quote
                break end;
            }
        };

        let tail = &self.stmt[chunk_start..content_end];
        let value = if owned.is_empty() {
            bytes_to_string(tail)
        } else {
            owned.extend_from_slice(tail);
            // Consume the already-owned bytes without a second copy; only
            // invalid UTF-8 falls back to the lossy re-encode.
            match String::from_utf8(owned) {
                Ok(s) => s,
                Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
            }
        };

        ParsedValue::String { value }
    }

    /// Parse a hex literal 0xABCD...
    fn parse_hex_value(&mut self) -> ParsedValue {
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
        ParsedValue::Hex(raw)
    }

    /// Parse a number or other non-string value
    fn parse_number_value(&mut self) -> ParsedValue {
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

        let raw = &self.stmt[start..self.pos];

        // Try to parse as integer before copying: plain integers (the common
        // case for IDs) then need no allocation at all.
        if !has_dot {
            if let Ok(value_str) = std::str::from_utf8(raw) {
                if let Ok(n) = value_str.parse::<i64>() {
                    return ParsedValue::Integer(n);
                }
                if let Ok(n) = value_str.parse::<i128>() {
                    return ParsedValue::BigInteger(n);
                }
            }
        }

        // Fall back to raw value
        ParsedValue::Other(raw.to_vec())
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
        extract_pk_fk_generic(
            values,
            schema,
            &self.column_order,
            &self.col_to_value,
            self.extraction == RowExtraction::Full,
            |v, col| self.value_to_pk(v, col),
        )
    }

    /// Convert a parsed value to a PkValue
    fn value_to_pk(&self, value: &ParsedValue, col: Option<&crate::schema::Column>) -> PkValue {
        match value {
            ParsedValue::Null => PkValue::Null,
            ParsedValue::Integer(n) => PkValue::Int(*n),
            ParsedValue::BigInteger(n) => PkValue::BigInt(*n),
            ParsedValue::String { value } => {
                // Check if this might be an integer stored as string
                coerce_text_pk(value, col)
                    .unwrap_or_else(|| PkValue::Text(value.clone().into_boxed_str()))
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
    parse_insert_rows_with(stmt, schema, dialect, RowExtraction::Full)
}

/// Like [`parse_insert_rows`], but with an explicit [`RowExtraction`] level so
/// consumers that don't need `all_values` (or PK/FK at all) skip that work.
///
/// This is a collecting adapter over [`visit_insert_rows_with`]: it simply
/// pushes each streamed row into a `Vec`. Existing consumers that want the
/// whole statement's rows keep using it unchanged.
pub fn parse_insert_rows_with(
    stmt: &[u8],
    schema: &TableSchema,
    dialect: SqlDialect,
    extraction: RowExtraction,
) -> anyhow::Result<Vec<ParsedRow>> {
    let mut rows = Vec::new();
    visit_insert_rows_with(stmt, schema, dialect, extraction, |row| {
        rows.push(row);
        Ok(RowFlow::Continue)
    })?;
    Ok(rows)
}

/// Stream the rows of an INSERT statement to a visitor, one at a time, without
/// building a statement-sized `Vec`. `f` returns a [`RowFlow`] to stop early.
/// Only the fields required by `extraction` are allocated per row.
pub fn visit_insert_rows_with<F>(
    stmt: &[u8],
    schema: &TableSchema,
    dialect: SqlDialect,
    extraction: RowExtraction,
    f: F,
) -> anyhow::Result<()>
where
    F: FnMut(ParsedRow) -> anyhow::Result<RowFlow>,
{
    let mut parser = InsertParser::new(stmt)
        .with_schema(schema)
        .with_dialect(dialect)
        .with_extraction(extraction);
    parser.parse_rows_visit(f)
}

/// Precomputed per-statement column context, so [`Parser::visit_events`]
/// consumers can parse individual INSERT tuples (delivered as
/// [`crate::parser::ParserEvent::InsertRow`]) without re-deriving the column
/// order for every row. Built once per INSERT header.
///
/// [`Parser::visit_events`]: crate::parser::Parser::visit_events
pub struct InsertRowContext {
    column_order: Vec<Option<ColumnId>>,
    col_to_value: Arc<[Option<usize>]>,
}

impl InsertRowContext {
    /// Build the context from an INSERT header (the statement bytes up to and
    /// including the `VALUES` keyword) and the table schema, exactly as the
    /// full-statement parser would.
    pub fn from_header(header: &[u8], schema: &TableSchema) -> Self {
        let values_pos = find_values_keyword_pos(header).unwrap_or(header.len());
        let column_order: Vec<Option<ColumnId>> =
            match extract_column_list_before(header, values_pos) {
                Some(cols) => cols.iter().map(|n| schema.get_column_id(n)).collect(),
                None => schema.columns.iter().map(|c| Some(c.ordinal)).collect(),
            };
        let col_to_value = build_col_to_value(&column_order, schema.columns.len());
        Self {
            column_order,
            col_to_value,
        }
    }
}

/// Parse a single INSERT `(...)` tuple with a precomputed [`InsertRowContext`].
///
/// Produces a [`ParsedRow`] byte-identical to the one the full-statement parser
/// would produce for the same tuple bytes and column context.
pub fn parse_insert_tuple(
    tuple: &[u8],
    schema: &TableSchema,
    ctx: &InsertRowContext,
    dialect: SqlDialect,
    extraction: RowExtraction,
) -> Option<ParsedRow> {
    let mut p = InsertParser::new(tuple)
        .with_schema(schema)
        .with_dialect(dialect)
        .with_extraction(extraction);
    p.column_order = ctx.column_order.clone();
    p.col_to_value = Arc::clone(&ctx.col_to_value);
    p.parse_row()
}

/// Find the extent of one INSERT tuple beginning at `data[0]` (which must be
/// `(`), using the exact same logic as [`InsertParser::parse_row`].
///
/// Returns `(consumed, complete)`: `consumed` is the number of bytes the tuple
/// occupies (so `data[..consumed]` is the raw tuple), and `complete` is `false`
/// when `data` ended before the matching `)` (the caller should buffer more).
/// Returns `None` only when `data` does not start at a `(`.
pub(crate) fn scan_insert_tuple(data: &[u8], dialect: SqlDialect) -> Option<(usize, bool)> {
    let mut p = InsertParser::new(data)
        .with_dialect(dialect)
        .with_extraction(RowExtraction::ValuesOnly);
    // parse_row returns None only when not positioned at '('.
    p.parse_row()?;
    Some((p.pos, p.last_complete))
}

/// Parse all rows from a MySQL INSERT statement
pub fn parse_mysql_insert_rows(
    stmt: &[u8],
    schema: &TableSchema,
) -> anyhow::Result<Vec<ParsedRow>> {
    parse_insert_rows(stmt, schema, SqlDialect::MySql)
}

/// Parse rows without schema (just raw row extraction)
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
    // Extract table name: INSERT INTO [schema.]table_name [(columns)] VALUES
    let table = extract_insert_table_name(stmt, dialect)?;

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

/// Extract the table name from an INSERT statement without copying or
/// uppercasing the whole statement — extended INSERTs can be many MB, and the
/// table name lives in the first few bytes. Quote handling, schema stripping
/// and `IF EXISTS`/`ONLY` skipping are shared with the split path via
/// [`super::extract_table_name_flexible`].
fn extract_insert_table_name(stmt: &[u8], dialect: SqlDialect) -> anyhow::Result<String> {
    let stmt = super::strip_leading_comments_and_whitespace(stmt);

    const INSERT_INTO: &[u8] = b"INSERT INTO";
    const INSERT: &[u8] = b"INSERT";
    let offset = if stmt.len() >= INSERT_INTO.len()
        && stmt[..INSERT_INTO.len()].eq_ignore_ascii_case(INSERT_INTO)
    {
        INSERT_INTO.len()
    } else if stmt.len() >= INSERT.len() && stmt[..INSERT.len()].eq_ignore_ascii_case(INSERT) {
        INSERT.len()
    } else {
        anyhow::bail!("Not an INSERT statement");
    };

    super::extract_table_name_flexible(stmt, offset, dialect)
        .ok_or_else(|| anyhow::anyhow!("Empty table reference"))
}
