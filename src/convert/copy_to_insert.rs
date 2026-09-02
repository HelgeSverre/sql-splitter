//! Convert PostgreSQL COPY FROM stdin statements to INSERT statements.
//!
//! Handles:
//! - Tab-separated value parsing
//! - NULL handling (\N → NULL)
//! - Escape sequence conversion (\t, \n, \\)
//! - Batched INSERT generation for efficiency

use once_cell::sync::Lazy;
use regex::Regex;

use crate::parser::postgres_copy::decode_copy_escapes;
use crate::parser::strip_leading_comments_and_whitespace;
use crate::parser::SqlDialect;
use crate::render::sql_string::SqlString;
use crate::transform_common::quote_identifier;

/// Maximum rows per INSERT statement (for readability and transaction size).
///
/// The DuckDB loader uses the same limit for its fast COPY path, then retries
/// a failed chunk one row at a time so one malformed value does not discard
/// unrelated rows from a dump.
pub(crate) const MAX_ROWS_PER_INSERT: usize = 1000;

/// Result of parsing a COPY header
#[derive(Debug, Clone)]
pub struct CopyHeader {
    /// Schema name (e.g., "public")
    pub schema: Option<String>,
    /// Table name
    pub table: String,
    /// Column list (may be empty if not specified)
    pub columns: Vec<String>,
}

/// Parse a COPY header to extract table and columns
/// Input: "COPY schema.table (col1, col2) FROM stdin;"
pub fn parse_copy_header(stmt: &str) -> Option<CopyHeader> {
    // Strip comments from the beginning
    let stmt = String::from_utf8_lossy(strip_leading_comments_and_whitespace(stmt.as_bytes()));

    static RE_COPY: Lazy<Regex> = Lazy::new(|| {
        // Pattern: COPY [ONLY] [schema.]table [(columns)] FROM stdin
        // Schema and table can be quoted with double quotes
        Regex::new(
            r#"(?i)^\s*COPY\s+(?:ONLY\s+)?(?:"?(\w+)"?\.)?["]?(\w+)["]?\s*(?:\(([^)]+)\))?\s+FROM\s+stdin"#
        ).unwrap()
    });

    let caps = RE_COPY.captures(&stmt)?;

    let schema = caps.get(1).map(|m| m.as_str().to_string());
    let table = caps.get(2)?.as_str().to_string();
    let columns = caps
        .get(3)
        .map(|m| {
            m.as_str()
                .split(',')
                .map(|c| c.trim().trim_matches('"').trim_matches('`').to_string())
                .collect()
        })
        .unwrap_or_default();

    Some(CopyHeader {
        schema,
        table,
        columns,
    })
}

/// Convert a COPY data block to INSERT statements
///
/// # Arguments
/// * `header` - Parsed COPY header with table/column info
/// * `data` - The data block (tab-separated rows ending with \.)
/// * `target_dialect` - Target SQL dialect for quoting
///
/// # Returns
/// Vector of INSERT statements as bytes
pub fn copy_to_inserts(
    header: &CopyHeader,
    data: &[u8],
    target_dialect: SqlDialect,
) -> Vec<Vec<u8>> {
    let rows = parse_copy_data(data);

    if rows.is_empty() {
        return Vec::new();
    }

    rows.chunks(MAX_ROWS_PER_INSERT)
        .map(|chunk| copy_rows_to_insert(header, chunk, target_dialect))
        .collect()
}

/// Convert one non-empty sequence of parsed COPY rows to a single INSERT.
///
/// This is crate-visible so the DuckDB loader can retry a rejected multi-row
/// insert one row at a time without reparsing or changing SQL escaping.
pub(crate) fn copy_rows_to_insert(
    header: &CopyHeader,
    rows: &[Vec<CopyValue>],
    target_dialect: SqlDialect,
) -> Vec<u8> {
    debug_assert!(!rows.is_empty());

    let table_ref = if let Some(ref schema) = header.schema {
        if target_dialect == SqlDialect::MySql {
            // MySQL: just use table name without schema
            quote_identifier(target_dialect, &header.table)
        } else if schema == "public" || schema == "pg_catalog" {
            // Common PostgreSQL schemas - strip for DuckDB compatibility
            quote_identifier(target_dialect, &header.table)
        } else {
            format!(
                "{}.{}",
                quote_identifier(target_dialect, schema),
                quote_identifier(target_dialect, &header.table)
            )
        }
    } else {
        quote_identifier(target_dialect, &header.table)
    };

    let columns_str = if header.columns.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = header
            .columns
            .iter()
            .map(|column| quote_identifier(target_dialect, column))
            .collect();
        format!(" ({})", cols.join(", "))
    };

    let mut insert = format!("INSERT INTO {}{} VALUES\n", table_ref, columns_str);

    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            insert.push_str(",\n");
        }
        insert.push('(');

        for (j, value) in row.iter().enumerate() {
            if j > 0 {
                insert.push_str(", ");
            }
            insert.push_str(&format_value(value, target_dialect));
        }

        insert.push(')');
    }

    insert.push(';');
    insert.into_bytes()
}

/// A parsed value from COPY data
#[derive(Debug, Clone)]
pub enum CopyValue {
    Null,
    Text(String),
}

/// Parse COPY data block into rows of values
pub fn parse_copy_data(data: &[u8]) -> Vec<Vec<CopyValue>> {
    let mut rows = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        // Find end of line
        let line_end = data[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| pos + p)
            .unwrap_or(data.len());

        let mut line = &data[pos..line_end];
        // Strip a trailing CR so CRLF-terminated dumps don't produce a phantom
        // `\.\r` terminator, phantom `\r` empty rows, or a `\r` retained in the
        // last column (mirrors CopyParser in parser/postgres_copy.rs).
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }

        // Check for terminator
        if line == b"\\." || line.is_empty() {
            pos = line_end + 1;
            continue;
        }

        // Parse the row
        let row = parse_row(line);
        if !row.is_empty() {
            rows.push(row);
        }

        pos = line_end + 1;
    }

    rows
}

/// Parse a single tab-separated row
fn parse_row(line: &[u8]) -> Vec<CopyValue> {
    let mut values = Vec::new();
    let mut start = 0;

    for (i, &b) in line.iter().enumerate() {
        if b == b'\t' {
            values.push(parse_value(&line[start..i]));
            start = i + 1;
        }
    }
    // Last value
    if start <= line.len() {
        values.push(parse_value(&line[start..]));
    }

    values
}

/// Parse a single COPY value
fn parse_value(value: &[u8]) -> CopyValue {
    // Check for NULL marker
    if value == b"\\N" {
        return CopyValue::Null;
    }

    // Decode escape sequences
    CopyValue::Text(String::from_utf8_lossy(&decode_copy_escapes(value)).into_owned())
}

/// Format a value for SQL INSERT
fn format_value(value: &CopyValue, dialect: SqlDialect) -> String {
    match value {
        CopyValue::Null => "NULL".to_string(),
        CopyValue::Text(s) => SqlString::new(dialect, s).to_string(),
    }
}
