#[cfg(test)]
mod edge_case_tests;

use once_cell::sync::Lazy;
use regex::bytes::Regex;
use std::io::{BufRead, BufReader, Read};

pub const SMALL_BUFFER_SIZE: usize = 64 * 1024;
pub const MEDIUM_BUFFER_SIZE: usize = 256 * 1024;

/// SQL dialect for parser behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialect {
    /// MySQL/MariaDB mysqldump format (backtick quoting, backslash escapes)
    #[default]
    MySql,
    /// PostgreSQL pg_dump format (double-quote identifiers, COPY FROM stdin, dollar-quoting)
    Postgres,
    /// SQLite .dump format (double-quote identifiers, '' escapes)
    Sqlite,
}

impl std::str::FromStr for SqlDialect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(SqlDialect::MySql),
            "postgres" | "postgresql" | "pg" => Ok(SqlDialect::Postgres),
            "sqlite" | "sqlite3" => Ok(SqlDialect::Sqlite),
            _ => Err(format!(
                "Unknown dialect: {}. Valid options: mysql, postgres, sqlite",
                s
            )),
        }
    }
}

impl std::fmt::Display for SqlDialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlDialect::MySql => write!(f, "mysql"),
            SqlDialect::Postgres => write!(f, "postgres"),
            SqlDialect::Sqlite => write!(f, "sqlite"),
        }
    }
}

/// Result of dialect auto-detection
#[derive(Debug, Clone)]
pub struct DialectDetectionResult {
    pub dialect: SqlDialect,
    pub confidence: DialectConfidence,
}

/// Confidence level of dialect detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectConfidence {
    /// High confidence - found definitive markers (e.g., "pg_dump", "MySQL dump")
    High,
    /// Medium confidence - found likely markers
    Medium,
    /// Low confidence - defaulting to MySQL
    Low,
}

#[derive(Default)]
struct DialectScore {
    mysql: u32,
    postgres: u32,
    sqlite: u32,
}

/// Detect SQL dialect from file header content.
/// Reads up to 8KB and looks for dialect-specific markers.
pub fn detect_dialect(header: &[u8]) -> DialectDetectionResult {
    let mut score = DialectScore::default();

    // High confidence markers (+10)
    if contains_bytes(header, b"pg_dump") {
        score.postgres += 10;
    }
    if contains_bytes(header, b"PostgreSQL database dump") {
        score.postgres += 10;
    }
    if contains_bytes(header, b"MySQL dump") {
        score.mysql += 10;
    }
    if contains_bytes(header, b"MariaDB dump") {
        score.mysql += 10;
    }
    if contains_bytes(header, b"SQLite") {
        score.sqlite += 10;
    }

    // Medium confidence markers (+5)
    if contains_bytes(header, b"COPY ") && contains_bytes(header, b"FROM stdin") {
        score.postgres += 5;
    }
    if contains_bytes(header, b"search_path") {
        score.postgres += 5;
    }
    if contains_bytes(header, b"/*!40") || contains_bytes(header, b"/*!50") {
        score.mysql += 5;
    }
    if contains_bytes(header, b"LOCK TABLES") {
        score.mysql += 5;
    }
    if contains_bytes(header, b"PRAGMA") {
        score.sqlite += 5;
    }
    if contains_bytes(header, b"BEGIN TRANSACTION") {
        score.sqlite += 5;
    }

    // Low confidence markers (+2)
    if contains_bytes(header, b"$$") {
        score.postgres += 2;
    }
    if contains_bytes(header, b"CREATE EXTENSION") {
        score.postgres += 2;
    }
    // Backticks suggest MySQL (but check it's not inside a string)
    if header.contains(&b'`') {
        score.mysql += 2;
    }

    // Determine winner and confidence
    let max_score = score.mysql.max(score.postgres).max(score.sqlite);

    if max_score == 0 {
        return DialectDetectionResult {
            dialect: SqlDialect::MySql,
            confidence: DialectConfidence::Low,
        };
    }

    let (dialect, confidence) = if score.postgres > score.mysql && score.postgres > score.sqlite {
        let conf = if score.postgres >= 10 {
            DialectConfidence::High
        } else if score.postgres >= 5 {
            DialectConfidence::Medium
        } else {
            DialectConfidence::Low
        };
        (SqlDialect::Postgres, conf)
    } else if score.sqlite > score.mysql {
        let conf = if score.sqlite >= 10 {
            DialectConfidence::High
        } else if score.sqlite >= 5 {
            DialectConfidence::Medium
        } else {
            DialectConfidence::Low
        };
        (SqlDialect::Sqlite, conf)
    } else {
        let conf = if score.mysql >= 10 {
            DialectConfidence::High
        } else if score.mysql >= 5 {
            DialectConfidence::Medium
        } else {
            DialectConfidence::Low
        };
        (SqlDialect::MySql, conf)
    };

    DialectDetectionResult { dialect, confidence }
}

/// Detect dialect from a file, reading first 8KB
pub fn detect_dialect_from_file(path: &std::path::Path) -> std::io::Result<DialectDetectionResult> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut buf = [0u8; 8192];
    let n = file.read(&mut buf)?;
    Ok(detect_dialect(&buf[..n]))
}

#[inline]
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Unknown,
    CreateTable,
    Insert,
    CreateIndex,
    AlterTable,
    DropTable,
    /// PostgreSQL COPY FROM stdin
    Copy,
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

// PostgreSQL COPY statement regex
static COPY_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)^\s*COPY\s+(?:ONLY\s+)?[`"]?([^\s`"(]+)[`"]?"#).unwrap());

// More flexible table name regex that handles:
// - Backticks: `table`
// - Double quotes: "table"
// - Schema qualified: schema.table, `schema`.`table`, "schema"."table"
// - IF NOT EXISTS
static CREATE_TABLE_FLEXIBLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`"]?[\w]+[`"]?\s*\.\s*)?[`"]?([\w]+)[`"]?"#).unwrap()
});

static INSERT_FLEXIBLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?i)^\s*INSERT\s+INTO\s+(?:ONLY\s+)?(?:[`"]?[\w]+[`"]?\s*\.\s*)?[`"]?([\w]+)[`"]?"#,
    )
    .unwrap()
});

pub struct Parser<R: Read> {
    reader: BufReader<R>,
    stmt_buffer: Vec<u8>,
    dialect: SqlDialect,
    /// For PostgreSQL: true when reading COPY data block
    in_copy_data: bool,
}

impl<R: Read> Parser<R> {
    #[allow(dead_code)]
    pub fn new(reader: R, buffer_size: usize) -> Self {
        Self::with_dialect(reader, buffer_size, SqlDialect::default())
    }

    pub fn with_dialect(reader: R, buffer_size: usize, dialect: SqlDialect) -> Self {
        Self {
            reader: BufReader::with_capacity(buffer_size, reader),
            stmt_buffer: Vec::with_capacity(32 * 1024),
            dialect,
            in_copy_data: false,
        }
    }

    pub fn read_statement(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        // If we're in PostgreSQL COPY data mode, read until we see the terminator
        if self.in_copy_data {
            return self.read_copy_data();
        }

        self.stmt_buffer.clear();

        let mut inside_single_quote = false;
        let mut inside_double_quote = false;
        let mut escaped = false;
        let mut in_line_comment = false;
        // For PostgreSQL dollar-quoting: track the tag
        let mut in_dollar_quote = false;
        let mut dollar_tag: Vec<u8> = Vec::new();

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
                let inside_string = inside_single_quote || inside_double_quote || in_dollar_quote;

                // End of line comment on newline
                if in_line_comment {
                    if b == b'\n' {
                        in_line_comment = false;
                    }
                    continue;
                }

                if escaped {
                    escaped = false;
                    continue;
                }

                // Handle backslash escapes (MySQL style)
                if b == b'\\' && inside_string && self.dialect == SqlDialect::MySql {
                    escaped = true;
                    continue;
                }

                // Handle line comments (-- to end of line)
                if b == b'-' && !inside_string && i + 1 < buf.len() && buf[i + 1] == b'-' {
                    in_line_comment = true;
                    continue;
                }

                // Handle dollar-quoting for PostgreSQL
                if self.dialect == SqlDialect::Postgres
                    && !inside_single_quote
                    && !inside_double_quote
                {
                    if b == b'$' && !in_dollar_quote {
                        // Start of dollar-quote: scan for the closing $
                        if let Some(end) = buf[i + 1..].iter().position(|&c| c == b'$') {
                            dollar_tag = buf[i + 1..i + 1 + end].to_vec();
                            in_dollar_quote = true;
                            continue;
                        }
                    } else if b == b'$' && in_dollar_quote {
                        // Potential end of dollar-quote
                        let tag_len = dollar_tag.len();
                        if i + 1 + tag_len < buf.len()
                            && buf[i + 1..i + 1 + tag_len] == dollar_tag[..]
                            && buf.get(i + 1 + tag_len) == Some(&b'$')
                        {
                            in_dollar_quote = false;
                            dollar_tag.clear();
                            continue;
                        }
                    }
                }

                if b == b'\'' && !inside_double_quote && !in_dollar_quote {
                    inside_single_quote = !inside_single_quote;
                } else if b == b'"' && !inside_single_quote && !in_dollar_quote {
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

                // Check if this is a PostgreSQL COPY FROM stdin statement
                if self.dialect == SqlDialect::Postgres && self.is_copy_from_stdin(&result) {
                    self.in_copy_data = true;
                }

                return Ok(Some(result));
            }

            self.stmt_buffer.extend_from_slice(buf);
            let len = buf.len();
            self.reader.consume(len);
        }
    }

    /// Check if statement is a PostgreSQL COPY FROM stdin
    fn is_copy_from_stdin(&self, stmt: &[u8]) -> bool {
        // Strip leading comments (pg_dump adds -- comments before COPY statements)
        let stmt = strip_leading_comments_and_whitespace(stmt);
        if stmt.len() < 4 {
            return false;
        }

        // Take enough bytes to cover column lists - typical COPY statements are <500 bytes
        let upper: Vec<u8> = stmt
            .iter()
            .take(500)
            .map(|b| b.to_ascii_uppercase())
            .collect();
        upper.starts_with(b"COPY ")
            && (upper.windows(10).any(|w| w == b"FROM STDIN")
                || upper.windows(11).any(|w| w == b"FROM STDIN;"))
    }

    /// Read PostgreSQL COPY data block until we see the terminator line (\.)
    fn read_copy_data(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.stmt_buffer.clear();

        loop {
            // First, fill the buffer and check if empty
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                self.in_copy_data = false;
                if self.stmt_buffer.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(std::mem::take(&mut self.stmt_buffer)));
            }

            // Look for a newline in the buffer
            let newline_pos = buf.iter().position(|&b| b == b'\n');

            if let Some(i) = newline_pos {
                // Include this newline
                self.stmt_buffer.extend_from_slice(&buf[..=i]);
                self.reader.consume(i + 1);

                // Check if the line we just added ends the COPY block
                // Looking for a line that is just "\.\n" or "\.\r\n"
                if self.ends_with_copy_terminator() {
                    self.in_copy_data = false;
                    return Ok(Some(std::mem::take(&mut self.stmt_buffer)));
                }
                // Continue reading - we need to process more lines
            } else {
                // No newline found, consume the whole buffer and continue
                let len = buf.len();
                self.stmt_buffer.extend_from_slice(buf);
                self.reader.consume(len);
            }
        }
    }

    /// Check if buffer ends with the COPY terminator line (\.)
    fn ends_with_copy_terminator(&self) -> bool {
        let data = &self.stmt_buffer;
        if data.len() < 2 {
            return false;
        }

        // Look for a line that is just "\.\n" or "\.\r\n"
        // We need to find the start of the last line
        let last_newline = data[..data.len() - 1]
            .iter()
            .rposition(|&b| b == b'\n')
            .map(|i| i + 1)
            .unwrap_or(0);

        let last_line = &data[last_newline..];

        // Check if it's "\.\n" or "\.\r\n"
        last_line == b"\\.\n" || last_line == b"\\.\r\n"
    }

    #[allow(dead_code)]
    pub fn parse_statement(stmt: &[u8]) -> (StatementType, String) {
        Self::parse_statement_with_dialect(stmt, SqlDialect::MySql)
    }

    /// Parse a statement with dialect-specific handling
    pub fn parse_statement_with_dialect(
        stmt: &[u8],
        dialect: SqlDialect,
    ) -> (StatementType, String) {
        // Strip leading comments (e.g., pg_dump adds -- comments before statements)
        let stmt = strip_leading_comments_and_whitespace(stmt);

        if stmt.len() < 4 {
            return (StatementType::Unknown, String::new());
        }

        let upper_prefix: Vec<u8> = stmt
            .iter()
            .take(25)
            .map(|b| b.to_ascii_uppercase())
            .collect();

        // PostgreSQL COPY statement
        if upper_prefix.starts_with(b"COPY ") {
            if let Some(caps) = COPY_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    let name = String::from_utf8_lossy(m.as_bytes()).into_owned();
                    // Handle schema.table - extract just the table name
                    let table_name = name.split('.').next_back().unwrap_or(&name).to_string();
                    return (StatementType::Copy, table_name);
                }
            }
        }

        if upper_prefix.starts_with(b"CREATE TABLE") {
            // Try fast extraction first
            if let Some(name) = extract_table_name_flexible(stmt, 12, dialect) {
                return (StatementType::CreateTable, name);
            }
            // Fall back to flexible regex
            if let Some(caps) = CREATE_TABLE_FLEXIBLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::CreateTable,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
            // Original regex as last resort
            if let Some(caps) = CREATE_TABLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::CreateTable,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
            }
        }

        if upper_prefix.starts_with(b"INSERT INTO") || upper_prefix.starts_with(b"INSERT ONLY") {
            if let Some(name) = extract_table_name_flexible(stmt, 11, dialect) {
                return (StatementType::Insert, name);
            }
            if let Some(caps) = INSERT_FLEXIBLE_RE.captures(stmt) {
                if let Some(m) = caps.get(1) {
                    return (
                        StatementType::Insert,
                        String::from_utf8_lossy(m.as_bytes()).into_owned(),
                    );
                }
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
            if let Some(name) = extract_table_name_flexible(stmt, 11, dialect) {
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
            if let Some(name) = extract_table_name_flexible(stmt, 10, dialect) {
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

/// Strip leading whitespace and SQL line comments (`-- ...`) from a statement.
/// This makes parsing robust to pg_dump-style comment blocks before statements.
fn strip_leading_comments_and_whitespace(mut data: &[u8]) -> &[u8] {
    loop {
        // First trim leading ASCII whitespace
        data = trim_ascii_start(data);

        if data.len() >= 2 && data[0] == b'-' && data[1] == b'-' {
            // Skip until end of line
            if let Some(pos) = data.iter().position(|&b| b == b'\n') {
                data = &data[pos + 1..];
                continue;
            } else {
                // Comment runs to EOF, nothing left
                return &[];
            }
        }

        break;
    }

    data
}

/// Extract table name with support for:
/// - IF NOT EXISTS
/// - ONLY (PostgreSQL)
/// - Schema-qualified names (schema.table)
/// - Both backtick and double-quote quoting
#[inline]
fn extract_table_name_flexible(stmt: &[u8], offset: usize, dialect: SqlDialect) -> Option<String> {
    let mut i = offset;

    // Skip whitespace
    while i < stmt.len() && is_whitespace(stmt[i]) {
        i += 1;
    }

    if i >= stmt.len() {
        return None;
    }

    // Check for IF NOT EXISTS
    let upper_check: Vec<u8> = stmt[i..]
        .iter()
        .take(20)
        .map(|b| b.to_ascii_uppercase())
        .collect();
    if upper_check.starts_with(b"IF NOT EXISTS") {
        i += 13; // Skip "IF NOT EXISTS"
        while i < stmt.len() && is_whitespace(stmt[i]) {
            i += 1;
        }
    }

    // Check for ONLY (PostgreSQL)
    let upper_check: Vec<u8> = stmt[i..]
        .iter()
        .take(10)
        .map(|b| b.to_ascii_uppercase())
        .collect();
    if upper_check.starts_with(b"ONLY ") || upper_check.starts_with(b"ONLY\t") {
        i += 4;
        while i < stmt.len() && is_whitespace(stmt[i]) {
            i += 1;
        }
    }

    if i >= stmt.len() {
        return None;
    }

    // Read identifier (potentially schema-qualified)
    let mut parts: Vec<String> = Vec::new();

    loop {
        // Determine quote character
        let quote_char = match stmt.get(i) {
            Some(b'`') if dialect == SqlDialect::MySql => {
                i += 1;
                Some(b'`')
            }
            Some(b'"') if dialect != SqlDialect::MySql => {
                i += 1;
                Some(b'"')
            }
            Some(b'"') => {
                // Allow double quotes for MySQL too (though less common)
                i += 1;
                Some(b'"')
            }
            _ => None,
        };

        let start = i;

        while i < stmt.len() {
            let b = stmt[i];
            if let Some(q) = quote_char {
                if b == q {
                    let name = &stmt[start..i];
                    parts.push(String::from_utf8_lossy(name).into_owned());
                    i += 1; // Skip closing quote
                    break;
                }
            } else if is_whitespace(b) || b == b'(' || b == b';' || b == b',' || b == b'.' {
                if i > start {
                    let name = &stmt[start..i];
                    parts.push(String::from_utf8_lossy(name).into_owned());
                }
                break;
            }
            i += 1;
        }

        // If at end of quoted name without finding close quote, bail
        if quote_char.is_some() && i <= start {
            break;
        }

        // Check for schema separator (.)
        while i < stmt.len() && is_whitespace(stmt[i]) {
            i += 1;
        }

        if i < stmt.len() && stmt[i] == b'.' {
            i += 1; // Skip the dot
            while i < stmt.len() && is_whitespace(stmt[i]) {
                i += 1;
            }
            // Continue to read the next identifier (table name)
        } else {
            break;
        }
    }

    // Return the last part (table name), not the schema
    parts.pop()
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

#[cfg(test)]
mod copy_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_copy_from_stdin_detection() {
        let data = b"COPY public.table_001 (id, col_int, col_varchar, col_text, col_decimal, created_at) FROM stdin;\n1\t6892\tvalue_1\tLorem ipsum\n\\.\n";
        let reader = Cursor::new(&data[..]);
        let mut parser = Parser::with_dialect(reader, 1024, SqlDialect::Postgres);

        // First statement should be the COPY header
        let stmt1 = parser.read_statement().unwrap().unwrap();
        let s1 = String::from_utf8_lossy(&stmt1);
        assert!(s1.starts_with("COPY"), "First statement should be COPY");
        assert!(s1.contains("FROM stdin"), "Should contain FROM stdin");

        // Second statement should be the data block
        let stmt2 = parser.read_statement().unwrap().unwrap();
        let s2 = String::from_utf8_lossy(&stmt2);
        assert!(
            s2.contains("1\t6892"),
            "Data block should contain first row"
        );
        assert!(
            s2.ends_with("\\.\n"),
            "Data block should end with terminator"
        );
    }

    #[test]
    fn test_copy_with_leading_comments() {
        // pg_dump adds -- comments before COPY statements
        let data = b"--\n-- Data for Name: table_001\n--\n\nCOPY public.table_001 (id, name) FROM stdin;\n1\tfoo\n\\.\n";
        let reader = Cursor::new(&data[..]);
        let mut parser = Parser::with_dialect(reader, 1024, SqlDialect::Postgres);

        // First statement should be the COPY header (with leading comments)
        let stmt1 = parser.read_statement().unwrap().unwrap();
        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(&stmt1, SqlDialect::Postgres);
        assert_eq!(stmt_type, StatementType::Copy);
        assert_eq!(table_name, "table_001");

        // Second statement should be the data block
        let stmt2 = parser.read_statement().unwrap().unwrap();
        let s2 = String::from_utf8_lossy(&stmt2);
        assert!(
            s2.ends_with("\\.\n"),
            "Data block should end with terminator"
        );
    }
}

#[cfg(test)]
mod dialect_detection_tests {
    use super::*;

    #[test]
    fn test_detect_mysql_dump_header() {
        let header = b"-- MySQL dump 10.13  Distrib 8.0.32, for Linux (x86_64)
--
-- Host: localhost    Database: mydb
-- ------------------------------------------------------
-- Server version	8.0.32

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_mariadb_dump_header() {
        let header = b"-- MariaDB dump 10.19  Distrib 10.11.2-MariaDB
--
-- Host: localhost    Database: test
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_postgres_pgdump_header() {
        let header = b"--
-- PostgreSQL database dump
--

-- Dumped from database version 15.2
-- Dumped by pg_dump version 15.2

SET statement_timeout = 0;
SET search_path = public, pg_catalog;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_postgres_copy_statement() {
        let header = b"COPY public.users (id, name, email) FROM stdin;
1\tAlice\talice@example.com
2\tBob\tbob@example.com
\\.
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_postgres_dollar_quoting() {
        let header = b"CREATE OR REPLACE FUNCTION test() RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Hello';
END;
$$ LANGUAGE plpgsql;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
    }

    #[test]
    fn test_detect_sqlite_dump_header() {
        let header = b"PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES(1,'Alice');
COMMIT;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_detect_sqlite_pragma_only() {
        let header = b"PRAGMA foreign_keys=OFF;
CREATE TABLE test (id INT);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_mysql_backticks() {
        let header = b"CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
    }

    #[test]
    fn test_detect_mysql_conditional_comments() {
        let header = b"/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!50503 SET NAMES utf8mb4 */;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_mysql_lock_tables() {
        let header = b"LOCK TABLES `users` WRITE;
INSERT INTO `users` VALUES (1,'test');
UNLOCK TABLES;
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Medium);
    }

    #[test]
    fn test_detect_empty_defaults_to_mysql() {
        let header = b"";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_detect_generic_sql_defaults_to_mysql() {
        let header = b"CREATE TABLE users (id INT, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice');
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::MySql);
        assert_eq!(result.confidence, DialectConfidence::Low);
    }

    #[test]
    fn test_detect_postgres_create_extension() {
        let header = b"CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";
CREATE TABLE users (id uuid DEFAULT uuid_generate_v4());
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
    }

    #[test]
    fn test_detect_sqlite_comment() {
        let header = b"-- SQLite database dump
-- Created by sqlite3

CREATE TABLE test (id INTEGER);
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Sqlite);
        assert_eq!(result.confidence, DialectConfidence::High);
    }

    #[test]
    fn test_scoring_postgres_beats_mysql_backticks() {
        // pg_dump header with some backticks in data shouldn't confuse it
        let header = b"--
-- PostgreSQL database dump
--
-- Dumped by pg_dump version 15.2

INSERT INTO notes VALUES (1, 'Use `code` for inline code');
";
        let result = detect_dialect(header);
        assert_eq!(result.dialect, SqlDialect::Postgres);
        assert_eq!(result.confidence, DialectConfidence::High);
    }
}
