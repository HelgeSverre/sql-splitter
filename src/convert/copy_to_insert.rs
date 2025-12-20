//! Convert PostgreSQL COPY FROM stdin statements to INSERT statements.
//!
//! Handles:
//! - Tab-separated value parsing
//! - NULL handling (\N → NULL)
//! - Escape sequence conversion (\t, \n, \\)
//! - Batched INSERT generation for efficiency

use once_cell::sync::Lazy;
use regex::Regex;

/// Maximum rows per INSERT statement (for readability and transaction size)
const MAX_ROWS_PER_INSERT: usize = 100;

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
    let stmt = strip_leading_comments(stmt);
    
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
    
    Some(CopyHeader { schema, table, columns })
}

/// Strip leading SQL comments from a string
fn strip_leading_comments(stmt: &str) -> String {
    let mut result = stmt.trim();
    loop {
        if result.starts_with("--") {
            if let Some(pos) = result.find('\n') {
                result = result[pos + 1..].trim();
                continue;
            } else {
                return String::new();
            }
        }
        if result.starts_with("/*") {
            if let Some(pos) = result.find("*/") {
                result = result[pos + 2..].trim();
                continue;
            } else {
                return String::new();
            }
        }
        break;
    }
    result.to_string()
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
    target_dialect: crate::parser::SqlDialect,
) -> Vec<Vec<u8>> {
    let mut inserts = Vec::new();
    let rows = parse_copy_data(data);
    
    if rows.is_empty() {
        return inserts;
    }
    
    // Build INSERT prefix
    let quote_char = match target_dialect {
        crate::parser::SqlDialect::MySql => '`',
        _ => '"',
    };
    
    let table_ref = if let Some(ref schema) = header.schema {
        if target_dialect == crate::parser::SqlDialect::MySql {
            // MySQL: just use table name without schema
            format!("{}{}{}", quote_char, header.table, quote_char)
        } else {
            format!(
                "{}{}{}.{}{}{}",
                quote_char, schema, quote_char,
                quote_char, header.table, quote_char
            )
        }
    } else {
        format!("{}{}{}", quote_char, header.table, quote_char)
    };
    
    let columns_str = if header.columns.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = header.columns
            .iter()
            .map(|c| format!("{}{}{}", quote_char, c, quote_char))
            .collect();
        format!(" ({})", cols.join(", "))
    };
    
    // Generate batched INSERTs
    for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
        let mut insert = format!("INSERT INTO {}{} VALUES\n", table_ref, columns_str);
        
        for (i, row) in chunk.iter().enumerate() {
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
        inserts.push(insert.into_bytes());
    }
    
    inserts
}

/// A parsed value from COPY data
#[derive(Debug, Clone)]
pub enum CopyValue {
    Null,
    Text(String),
}

/// Parse COPY data block into rows of values
fn parse_copy_data(data: &[u8]) -> Vec<Vec<CopyValue>> {
    let mut rows = Vec::new();
    let mut pos = 0;
    
    while pos < data.len() {
        // Find end of line
        let line_end = data[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| pos + p)
            .unwrap_or(data.len());
        
        let line = &data[pos..line_end];
        
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
    let decoded = decode_escapes(value);
    CopyValue::Text(decoded)
}

/// Decode PostgreSQL COPY escape sequences
fn decode_escapes(value: &[u8]) -> String {
    let mut result = String::with_capacity(value.len());
    let mut i = 0;
    
    while i < value.len() {
        if value[i] == b'\\' && i + 1 < value.len() {
            let next = value[i + 1];
            let decoded = match next {
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'\\' => '\\',
                b'b' => '\x08', // backspace
                b'f' => '\x0C', // form feed
                b'v' => '\x0B', // vertical tab
                _ => {
                    // Unknown escape or octal, try octal
                    if next.is_ascii_digit() {
                        // Try to parse octal (up to 3 digits)
                        let mut octal_val = 0u8;
                        let mut consumed = 0;
                        for j in 0..3 {
                            if i + 1 + j < value.len() {
                                let d = value[i + 1 + j];
                                if d >= b'0' && d <= b'7' {
                                    octal_val = octal_val * 8 + (d - b'0');
                                    consumed += 1;
                                } else {
                                    break;
                                }
                            }
                        }
                        if consumed > 0 {
                            result.push(octal_val as char);
                            i += 1 + consumed;
                            continue;
                        }
                    }
                    // Unknown escape, keep as-is
                    result.push('\\');
                    result.push(next as char);
                    i += 2;
                    continue;
                }
            };
            result.push(decoded);
            i += 2;
        } else {
            // Regular character - handle UTF-8 properly
            if value[i] < 128 {
                result.push(value[i] as char);
                i += 1;
            } else {
                // Multi-byte UTF-8 sequence
                let remaining = &value[i..];
                if let Ok(s) = std::str::from_utf8(remaining) {
                    if let Some(c) = s.chars().next() {
                        result.push(c);
                        i += c.len_utf8();
                    } else {
                        i += 1;
                    }
                } else {
                    // Invalid UTF-8, just push the byte as replacement char
                    result.push('\u{FFFD}');
                    i += 1;
                }
            }
        }
    }
    
    result
}

/// Format a value for SQL INSERT
fn format_value(value: &CopyValue, dialect: crate::parser::SqlDialect) -> String {
    match value {
        CopyValue::Null => "NULL".to_string(),
        CopyValue::Text(s) => {
            // Escape quotes based on dialect
            let escaped = match dialect {
                crate::parser::SqlDialect::MySql => {
                    // MySQL: escape with backslash
                    s.replace('\\', "\\\\")
                        .replace('\'', "\\'")
                        .replace('\n', "\\n")
                        .replace('\r', "\\r")
                        .replace('\t', "\\t")
                        .replace('\0', "\\0")
                }
                _ => {
                    // PostgreSQL/SQLite: escape by doubling
                    s.replace('\'', "''")
                }
            };
            format!("'{}'", escaped)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlDialect;
    
    #[test]
    fn test_parse_copy_header_simple() {
        let header = "COPY users (id, name, email) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
        assert_eq!(parsed.columns, vec!["id", "name", "email"]);
        assert!(parsed.schema.is_none());
    }
    
    #[test]
    fn test_parse_copy_header_with_schema() {
        let header = "COPY public.users (id, name) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "users");
    }
    
    #[test]
    fn test_parse_copy_header_quoted() {
        let header = r#"COPY "public"."my_table" ("id", "name") FROM stdin;"#;
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.schema, Some("public".to_string()));
        assert_eq!(parsed.table, "my_table");
    }
    
    #[test]
    fn test_parse_copy_header_with_comments() {
        let header = "--\n-- Data for table\n--\nCOPY users (id) FROM stdin;";
        let parsed = parse_copy_header(header).unwrap();
        assert_eq!(parsed.table, "users");
    }
    
    #[test]
    fn test_parse_copy_data() {
        let data = b"1\tAlice\talice@example.com\n2\tBob\tbob@example.com\n\\.";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 3);
    }
    
    #[test]
    fn test_null_handling() {
        let data = b"1\t\\N\ttest\n";
        let rows = parse_copy_data(data);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][1], CopyValue::Null));
    }
    
    #[test]
    fn test_escape_sequences() {
        let data = b"hello\\tworld\\n\n";
        let rows = parse_copy_data(data);
        if let CopyValue::Text(s) = &rows[0][0] {
            assert_eq!(s, "hello\tworld\n");
        } else {
            panic!("Expected Text");
        }
    }
    
    #[test]
    fn test_copy_to_insert_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n2\tBob\n\\.";
        
        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        assert_eq!(inserts.len(), 1);
        
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("INSERT INTO `users`"));
        assert!(sql.contains("(`id`, `name`)"));
        assert!(sql.contains("('1', 'Alice')"));
        assert!(sql.contains("('2', 'Bob')"));
    }
    
    #[test]
    fn test_copy_to_insert_postgres() {
        let header = CopyHeader {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let data = b"1\tAlice\n\\.";
        
        let inserts = copy_to_inserts(&header, data, SqlDialect::Postgres);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("\"public\".\"users\""));
    }
    
    #[test]
    fn test_copy_to_insert_with_null() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
        };
        let data = b"1\t\\N\n\\.";
        
        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("NULL"));
    }
    
    #[test]
    fn test_escape_quotes_mysql() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";
        
        let inserts = copy_to_inserts(&header, data, SqlDialect::MySql);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it\\'s a test"));
    }
    
    #[test]
    fn test_escape_quotes_postgres() {
        let header = CopyHeader {
            schema: None,
            table: "t".to_string(),
            columns: vec!["s".to_string()],
        };
        let data = b"it's a test\n\\.";
        
        let inserts = copy_to_inserts(&header, data, SqlDialect::Sqlite);
        let sql = String::from_utf8_lossy(&inserts[0]);
        assert!(sql.contains("it''s a test"));
    }
}
