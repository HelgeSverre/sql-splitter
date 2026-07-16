//! PostgreSQL COPY statement parser.
//!
//! Parses COPY ... FROM stdin data blocks to extract individual rows
//! and optionally extract PK/FK column values for dependency tracking.

use crate::schema::{ColumnId, TableSchema};
use std::borrow::Cow;
use std::sync::Arc;

// Re-use types from mysql_insert for consistency
use super::mysql_insert::{coerce_text_pk, FkRef, PkValue, RowExtraction};

// Re-export (rather than re-declare) the shared PK tuple type.
pub use super::mysql_insert::PkTuple;

/// A parsed row from a COPY data block
#[derive(Debug, Clone)]
pub struct ParsedCopyRow {
    /// Raw bytes of the row (tab-separated values, no newline)
    pub raw: Vec<u8>,
    /// Extracted primary key values (if table has PK and values are non-NULL)
    pub pk: Option<PkTuple>,
    /// Extracted foreign key values with their references
    pub fk_values: Vec<(FkRef, PkTuple)>,
    /// All column values (for data diff comparison).
    /// Empty unless parsed with [`RowExtraction::Full`].
    pub all_values: Vec<PkValue>,
    /// Mapping from schema column index to value index (for finding specific
    /// columns). Shared across all rows of a block (computed once).
    pub column_map: Arc<[Option<usize>]>,
}

impl ParsedCopyRow {
    /// Get the value for a specific schema column index
    pub fn get_column_value(&self, schema_col_index: usize) -> Option<&PkValue> {
        self.column_map
            .get(schema_col_index)
            .and_then(|v| *v)
            .and_then(|val_idx| self.all_values.get(val_idx))
    }
}

/// Parser for PostgreSQL COPY data blocks
pub struct CopyParser<'a> {
    data: &'a [u8],
    table_schema: Option<&'a TableSchema>,
    /// Column names from the COPY header, resolved against the schema lazily
    /// in `parse_rows` (so builder order doesn't matter).
    column_names: Vec<String>,
    /// Resolved column order (value index -> column ID)
    column_order: Vec<Option<ColumnId>>,
    /// Reverse of `column_order` (schema column ordinal -> value index),
    /// computed once per block and shared with every row via `Arc`.
    col_to_value: Arc<[Option<usize>]>,
    /// How much derived data to compute per row.
    extraction: RowExtraction,
}

impl<'a> CopyParser<'a> {
    /// Create a new parser for COPY data
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            table_schema: None,
            column_names: Vec::new(),
            column_order: Vec::new(),
            col_to_value: Arc::from(Vec::new()),
            extraction: RowExtraction::Full,
        }
    }

    /// Set the table schema for PK/FK extraction
    pub fn with_schema(mut self, schema: &'a TableSchema) -> Self {
        self.table_schema = Some(schema);
        self
    }

    /// Choose how much per-row derived data to compute (default: [`RowExtraction::Full`]).
    pub fn with_extraction(mut self, extraction: RowExtraction) -> Self {
        self.extraction = extraction;
        self
    }

    /// Set column order from the COPY header. Resolution against the schema is
    /// deferred to `parse_rows`, so this may be called before `with_schema`.
    pub fn with_column_order(mut self, columns: Vec<String>) -> Self {
        self.column_names = columns;
        self
    }

    /// Parse all rows from the COPY data block
    pub fn parse_rows(&mut self) -> anyhow::Result<Vec<ParsedCopyRow>> {
        if let Some(schema) = self.table_schema {
            self.column_order = if self.column_names.is_empty() {
                // No explicit column list - use natural schema order
                schema.columns.iter().map(|c| Some(c.ordinal)).collect()
            } else {
                self.column_names
                    .iter()
                    .map(|name| schema.get_column_id(name))
                    .collect()
            };

            // Precompute the reverse column map once for the whole block.
            self.col_to_value =
                super::mysql_insert::build_col_to_value(&self.column_order, schema.columns.len());
        }

        // An empty line is a legitimate single empty-string value only for a
        // one-column table; for anything else it's padding and is skipped.
        let empty_line_is_row = self
            .table_schema
            .map(|s| s.columns.len() == 1)
            .unwrap_or(false);

        let mut rows = Vec::new();
        let mut pos = 0;

        while pos < self.data.len() {
            // Find end of line
            let line_end = memchr::memchr(b'\n', &self.data[pos..])
                .map(|p| pos + p)
                .unwrap_or(self.data.len());

            let mut line = &self.data[pos..line_end];
            // Strip a trailing CR so CRLF-terminated dumps don't leave \r in the
            // last value of every row (and so the \. terminator still matches).
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }

            // Check for terminator / skippable blank line
            if line == b"\\." || (line.is_empty() && !empty_line_is_row) {
                pos = line_end + 1;
                continue;
            }

            // Parse the row
            rows.push(self.parse_row(line));

            pos = line_end + 1;
        }

        Ok(rows)
    }

    /// Parse a single tab-separated row (infallible: any line is a valid row)
    fn parse_row(&self, line: &[u8]) -> ParsedCopyRow {
        let raw = line.to_vec();

        // Split, parse and extract PK/FK only when the caller wants them;
        // ValuesOnly consumers (e.g. redact) just need `raw`.
        let (pk, fk_values, all_values) = match (self.table_schema, self.extraction) {
            (Some(schema), RowExtraction::PkFk | RowExtraction::Full) => {
                let values: Vec<CopyValue> = self.split_and_parse_values(line);
                self.extract_pk_fk(&values, schema)
            }
            _ => (None, Vec::new(), Vec::new()),
        };

        ParsedCopyRow {
            raw,
            pk,
            fk_values,
            all_values,
            column_map: Arc::clone(&self.col_to_value),
        }
    }

    /// Split line by tabs and parse each value
    fn split_and_parse_values<'b>(&self, line: &'b [u8]) -> Vec<CopyValue<'b>> {
        let mut values = Vec::new();
        let mut start = 0;

        for (i, &b) in line.iter().enumerate() {
            if b == b'\t' {
                values.push(self.parse_copy_value(&line[start..i]));
                start = i + 1;
            }
        }
        // Last value
        if start <= line.len() {
            values.push(self.parse_copy_value(&line[start..]));
        }

        values
    }

    /// Parse a single COPY value
    fn parse_copy_value<'b>(&self, value: &'b [u8]) -> CopyValue<'b> {
        // Check for NULL marker
        if value == b"\\N" {
            return CopyValue::Null;
        }

        // Decode escape sequences; values without a backslash (the common
        // case) are borrowed as-is instead of copied.
        let decoded: Cow<'b, [u8]> = if memchr::memchr(b'\\', value).is_none() {
            Cow::Borrowed(value)
        } else {
            Cow::Owned(decode_copy_escapes(value))
        };

        // Try to parse as integer, but only when the text is the *canonical*
        // representation of that integer. Otherwise "0123" or "+5" would be
        // treated as numbers and compare equal to their numeric forms in diff,
        // silently conflating distinct text primary keys (bug #12).
        if let Ok(s) = std::str::from_utf8(&decoded) {
            if is_canonical_int(s) {
                if let Ok(n) = s.parse::<i64>() {
                    return CopyValue::Integer(n);
                }
                if let Ok(n) = s.parse::<i128>() {
                    return CopyValue::BigInteger(n);
                }
            }
        }

        CopyValue::Text(decoded)
    }

    /// Decode PostgreSQL COPY escape sequences.
    ///
    /// Thin wrapper kept for API compatibility; the implementation lives in
    /// the free function [`decode_copy_escapes`].
    pub fn decode_copy_escapes(&self, value: &[u8]) -> Vec<u8> {
        decode_copy_escapes(value)
    }

    /// Extract PK, FK, and all values from parsed values
    fn extract_pk_fk(
        &self,
        values: &[CopyValue<'_>],
        schema: &TableSchema,
    ) -> (Option<PkTuple>, Vec<(FkRef, PkTuple)>, Vec<PkValue>) {
        super::mysql_insert::extract_pk_fk_generic(
            values,
            schema,
            &self.column_order,
            &self.col_to_value,
            self.extraction == RowExtraction::Full,
            |v, col| self.value_to_pk(v, col),
        )
    }

    /// Convert a parsed value to a PkValue
    fn value_to_pk(&self, value: &CopyValue<'_>, col: Option<&crate::schema::Column>) -> PkValue {
        match value {
            CopyValue::Null => PkValue::Null,
            CopyValue::Integer(n) => PkValue::Int(*n),
            CopyValue::BigInteger(n) => PkValue::BigInt(*n),
            CopyValue::Text(bytes) => {
                let s = String::from_utf8_lossy(bytes);

                // Check if this might be an integer stored as text
                coerce_text_pk(&s, col)
                    .unwrap_or_else(|| PkValue::Text(s.into_owned().into_boxed_str()))
            }
        }
    }
}

/// Internal representation of a parsed COPY value. Text borrows from the line
/// unless escape decoding forced a copy.
#[derive(Debug, Clone)]
enum CopyValue<'a> {
    Null,
    Integer(i64),
    BigInteger(i128),
    Text(Cow<'a, [u8]>),
}

/// Decode PostgreSQL COPY escape sequences (`\t`, `\n`, `\r`, `\\`; a `\N`
/// or unknown escape passes through verbatim).
///
/// This is the canonical decoder for the COPY text wire format — consumers
/// elsewhere in the crate (e.g. the redactor's `ValueRewriter` in
/// `src/redactor/rewriter.rs`) should call this instead of keeping their own
/// copy, so escape-handling fixes land in exactly one place.
pub fn decode_copy_escapes(value: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(value.len());
    let mut i = 0;

    while i < value.len() {
        if value[i] == b'\\' && i + 1 < value.len() {
            let next = value[i + 1];
            let decoded = match next {
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'\\' => b'\\',
                b'N' => {
                    // \N is the NULL marker, not an escape; callers filter it
                    // out before decoding, so keep it verbatim here.
                    result.push(b'\\');
                    result.push(b'N');
                    i += 2;
                    continue;
                }
                _ => {
                    // Unknown escape, keep as-is
                    result.push(b'\\');
                    result.push(next);
                    i += 2;
                    continue;
                }
            };
            result.push(decoded);
            i += 2;
        } else {
            result.push(value[i]);
            i += 1;
        }
    }

    result
}

/// True when `s` is the canonical decimal rendering of an integer — digits
/// only, no `+`, no leading zeros, no `-0`. Combined with a successful parse
/// this is equivalent to `n.to_string() == s` without the allocation (bug #12).
fn is_canonical_int(s: &str) -> bool {
    let b = s.as_bytes();
    let (neg, digits) = match b.split_first() {
        Some((b'+', _)) => return false,
        Some((b'-', rest)) => (true, rest),
        _ => (false, b),
    };
    match digits {
        [] => false,
        [b'0'] => !neg,
        [b'0', ..] => false,
        _ => digits.iter().all(u8::is_ascii_digit),
    }
}

/// Parse column list from COPY header
pub fn parse_copy_columns(header: &str) -> Vec<String> {
    // COPY table_name (col1, col2, ...) FROM stdin;
    if let Some(start) = header.find('(') {
        if let Some(end) = header.find(')') {
            let cols = &header[start + 1..end];
            return cols
                .split(',')
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
        }
    }
    Vec::new()
}

/// Parse all rows from a PostgreSQL COPY data block
pub fn parse_postgres_copy_rows(
    data: &[u8],
    schema: &TableSchema,
    column_order: Vec<String>,
) -> anyhow::Result<Vec<ParsedCopyRow>> {
    parse_postgres_copy_rows_with(data, schema, column_order, RowExtraction::Full)
}

/// Like [`parse_postgres_copy_rows`], but with an explicit [`RowExtraction`]
/// level so consumers that don't need `all_values` skip that work.
pub fn parse_postgres_copy_rows_with(
    data: &[u8],
    schema: &TableSchema,
    column_order: Vec<String>,
    extraction: RowExtraction,
) -> anyhow::Result<Vec<ParsedCopyRow>> {
    let mut parser = CopyParser::new(data)
        .with_schema(schema)
        .with_column_order(column_order)
        .with_extraction(extraction);
    parser.parse_rows()
}
