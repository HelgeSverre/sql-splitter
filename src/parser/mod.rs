use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::io::{BufRead, BufReader, Read};

pub const SMALL_BUFFER_SIZE: usize = 64 * 1024;
pub const MEDIUM_BUFFER_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Unknown,
    CreateTable,
    Insert,
    CreateIndex,
    AlterTable,
    DropTable,
}

static CREATE_TABLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*CREATE\s+TABLE\s+`?([^\s`(]+)`?").unwrap());

static INSERT_INTO_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*INSERT\s+INTO\s+`?([^\s`(]+)`?").unwrap());

static CREATE_INDEX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)ON\s+`?([^\s`(;]+)`?").unwrap());

static ALTER_TABLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)ALTER\s+TABLE\s+`?([^\s`;]+)`?").unwrap());

static DROP_TABLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)DROP\s+TABLE\s+`?([^\s`;]+)`?").unwrap());

pub struct Parser<R: Read> {
    reader: BufReader<R>,
    stmt_buffer: Vec<u8>,
}

impl<R: Read> Parser<R> {
    pub fn new(reader: R, buffer_size: usize) -> Self {
        Self {
            reader: BufReader::with_capacity(buffer_size, reader),
            stmt_buffer: Vec::with_capacity(32 * 1024),
        }
    }

    pub fn read_statement(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.stmt_buffer.clear();

        let mut inside_single_quote = false;
        let mut inside_double_quote = false;
        let mut escaped = false;

        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                if self.stmt_buffer.is_empty() {
                    return Ok(None);
                }
                let result = std::mem::take(&mut self.stmt_buffer);
                return Ok(Some(result));
            }

            let mut consumed = 0;
            let mut found_terminator = false;

            for (i, &b) in buf.iter().enumerate() {
                let inside_string = inside_single_quote || inside_double_quote;

                if escaped {
                    escaped = false;
                    continue;
                }

                if b == b'\\' && inside_string {
                    escaped = true;
                    continue;
                }

                if b == b'\'' && !inside_double_quote {
                    inside_single_quote = !inside_single_quote;
                } else if b == b'"' && !inside_single_quote {
                    inside_double_quote = !inside_double_quote;
                } else if b == b';' && !inside_string {
                    self.stmt_buffer.extend_from_slice(&buf[..=i]);
                    consumed = i + 1;
                    found_terminator = true;
                    break;
                }
            }

            if found_terminator {
                self.reader.consume(consumed);
                let result = std::mem::take(&mut self.stmt_buffer);
                return Ok(Some(result));
            }

            self.stmt_buffer.extend_from_slice(buf);
            let len = buf.len();
            self.reader.consume(len);
        }
    }

    pub fn parse_statement(stmt: &[u8]) -> (StatementType, String) {
        let stmt = trim_ascii_start(stmt);

        if stmt.len() < 6 {
            return (StatementType::Unknown, String::new());
        }

        let upper_prefix: Vec<u8> = stmt
            .iter()
            .take(20)
            .map(|b| b.to_ascii_uppercase())
            .collect();

        if upper_prefix.starts_with(b"CREATE TABLE") {
            if let Some(name) = extract_table_name(stmt, 12) {
                return (StatementType::CreateTable, name);
            }
            if let Some(caps) = CREATE_TABLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::CreateTable,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        if upper_prefix.starts_with(b"INSERT INTO") {
            if let Some(name) = extract_table_name(stmt, 11) {
                return (StatementType::Insert, name);
            }
            if let Some(caps) = INSERT_INTO_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::Insert,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        if upper_prefix.starts_with(b"CREATE INDEX") {
            if let Some(caps) = CREATE_INDEX_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::CreateIndex,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        if upper_prefix.starts_with(b"ALTER TABLE") {
            if let Some(name) = extract_table_name(stmt, 11) {
                return (StatementType::AlterTable, name);
            }
            if let Some(caps) = ALTER_TABLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::AlterTable,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        if upper_prefix.starts_with(b"DROP TABLE") {
            if let Some(name) = extract_table_name(stmt, 10) {
                return (StatementType::DropTable, name);
            }
            if let Some(caps) = DROP_TABLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::DropTable,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        (StatementType::Unknown, String::new())
    }
}

#[inline]
fn trim_ascii_start(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|&b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        .unwrap_or(data.len());
    &data[start..]
}

#[inline]
fn extract_table_name(stmt: &[u8], offset: usize) -> Option<String> {
    let mut i = offset;

    while i < stmt.len() && is_whitespace(stmt[i]) {
        i += 1;
    }

    if i >= stmt.len() {
        return None;
    }

    let quote_char = if stmt[i] == b'`' || stmt[i] == b'"' {
        let q = stmt[i];
        i += 1;
        Some(q)
    } else {
        None
    };

    let start = i;

    while i < stmt.len() {
        let b = stmt[i];
        if let Some(q) = quote_char {
            if b == q {
                let name = &stmt[start..i];
                return Some(String::from_utf8_lossy(name).into_owned());
            }
        } else if is_whitespace(b) || b == b'(' || b == b';' || b == b',' {
            if i > start {
                let name = &stmt[start..i];
                return Some(String::from_utf8_lossy(name).into_owned());
            }
            return None;
        }
        i += 1;
    }

    if quote_char.is_none() && i > start {
        let name = &stmt[start..i];
        return Some(String::from_utf8_lossy(name).into_owned());
    }

    None
}

#[inline]
fn is_whitespace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

pub fn determine_buffer_size(file_size: u64) -> usize {
    if file_size > 1024 * 1024 * 1024 {
        MEDIUM_BUFFER_SIZE
    } else {
        SMALL_BUFFER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_create_table() {
        let stmt = b"CREATE TABLE users (id INT);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::CreateTable);
        assert_eq!(name, "users");
    }

    #[test]
    fn test_parse_create_table_backticks() {
        let stmt = b"CREATE TABLE `my_table` (id INT);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::CreateTable);
        assert_eq!(name, "my_table");
    }

    #[test]
    fn test_parse_insert() {
        let stmt = b"INSERT INTO posts VALUES (1, 'test');";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::Insert);
        assert_eq!(name, "posts");
    }

    #[test]
    fn test_parse_insert_backticks() {
        let stmt = b"INSERT INTO `comments` VALUES (1);";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::Insert);
        assert_eq!(name, "comments");
    }

    #[test]
    fn test_parse_alter_table() {
        let stmt = b"ALTER TABLE orders ADD COLUMN status INT;";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::AlterTable);
        assert_eq!(name, "orders");
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = b"DROP TABLE temp_data;";
        let (typ, name) = Parser::<&[u8]>::parse_statement(stmt);
        assert_eq!(typ, StatementType::DropTable);
        assert_eq!(name, "temp_data");
    }

    #[test]
    fn test_read_statement_basic() {
        let sql = b"CREATE TABLE t1 (id INT); INSERT INTO t1 VALUES (1);";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt1 = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt1, b"CREATE TABLE t1 (id INT);");

        let stmt2 = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt2, b" INSERT INTO t1 VALUES (1);");

        let stmt3 = parser.read_statement().unwrap();
        assert!(stmt3.is_none());
    }

    #[test]
    fn test_read_statement_with_strings() {
        let sql = b"INSERT INTO t1 VALUES ('hello; world');";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt, b"INSERT INTO t1 VALUES ('hello; world');");
    }

    #[test]
    fn test_read_statement_with_escaped_quotes() {
        let sql = b"INSERT INTO t1 VALUES ('it\\'s a test');";
        let mut parser = Parser::new(&sql[..], 1024);

        let stmt = parser.read_statement().unwrap().unwrap();
        assert_eq!(stmt, b"INSERT INTO t1 VALUES ('it\\'s a test');");
    }
}
