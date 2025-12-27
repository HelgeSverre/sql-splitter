pub mod mysql_insert;
pub mod postgres_copy;

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
    /// Microsoft SQL Server / T-SQL (square bracket identifiers, GO batches, N'unicode' strings)
    Mssql,
}

impl std::str::FromStr for SqlDialect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(SqlDialect::MySql),
            "postgres" | "postgresql" | "pg" => Ok(SqlDialect::Postgres),
            "sqlite" | "sqlite3" => Ok(SqlDialect::Sqlite),
            "mssql" | "sqlserver" | "sql_server" | "tsql" => Ok(SqlDialect::Mssql),
            _ => Err(format!(
                "Unknown dialect: {}. Valid options: mysql, postgres, sqlite, mssql",
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
            SqlDialect::Mssql => write!(f, "mssql"),
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
    mssql: u32,
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

    // Low confidence markers (+2)
    if contains_bytes(header, b"$$") {
        score.postgres += 2;
    }
    if contains_bytes(header, b"CREATE EXTENSION") {
        score.postgres += 2;
    }
    // BEGIN TRANSACTION is generic ANSI SQL, only slightly suggests SQLite
    if contains_bytes(header, b"BEGIN TRANSACTION") {
        score.sqlite += 2;
    }
    // Backticks suggest MySQL (could also appear in data/comments)
    if header.contains(&b'`') {
        score.mysql += 2;
    }

    // MSSQL/T-SQL markers
    // High confidence markers (+20)
    if contains_bytes(header, b"SET ANSI_NULLS") {
        score.mssql += 20;
    }
    if contains_bytes(header, b"SET QUOTED_IDENTIFIER") {
        score.mssql += 20;
    }

    // Medium confidence markers (+10-15)
    // GO as batch separator on its own line (check for common patterns)
    if contains_bytes(header, b"\nGO\n") || contains_bytes(header, b"\nGO\r\n") {
        score.mssql += 15;
    }
    // Square bracket identifiers
    if header.contains(&b'[') && header.contains(&b']') {
        score.mssql += 10;
    }
    if contains_bytes(header, b"IDENTITY(") {
        score.mssql += 10;
    }
    if contains_bytes(header, b"ON [PRIMARY]") {
        score.mssql += 10;
    }

    // Low confidence markers (+5)
    if contains_bytes(header, b"N'") {
        score.mssql += 5;
    }
    if contains_bytes(header, b"NVARCHAR") {
        score.mssql += 5;
    }
    if contains_bytes(header, b"CLUSTERED") {
        score.mssql += 5;
    }
    if contains_bytes(header, b"SET NOCOUNT") {
        score.mssql += 5;
    }

    // Determine winner and confidence
    let max_score = score
        .mysql
        .max(score.postgres)
        .max(score.sqlite)
        .max(score.mssql);

    if max_score == 0 {
        return DialectDetectionResult {
            dialect: SqlDialect::MySql,
            confidence: DialectConfidence::Low,
        };
    }

    // Find the dialect with the highest score
    let (dialect, winning_score) = if score.mssql > score.mysql
        && score.mssql > score.postgres
        && score.mssql > score.sqlite
    {
        (SqlDialect::Mssql, score.mssql)
    } else if score.postgres > score.mysql && score.postgres > score.sqlite {
        (SqlDialect::Postgres, score.postgres)
    } else if score.sqlite > score.mysql {
        (SqlDialect::Sqlite, score.sqlite)
    } else {
        (SqlDialect::MySql, score.mysql)
    };

    // Determine confidence based on winning score
    let confidence = if winning_score >= 10 {
        DialectConfidence::High
    } else if winning_score >= 5 {
        DialectConfidence::Medium
    } else {
        DialectConfidence::Low
    };

    DialectDetectionResult {
        dialect,
        confidence,
    }
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

/// Check if a line is a MSSQL GO batch separator
/// GO must be on its own line (with optional whitespace and optional repeat count)
/// Examples: "GO\n", "  GO  \n", "GO 100\n", "go\r\n"
fn is_go_line(line: &[u8]) -> bool {
    // Trim leading whitespace
    let mut start = 0;
    while start < line.len()
        && (line[start] == b' ' || line[start] == b'\t' || line[start] == b'\r')
    {
        start += 1;
    }

    // Trim trailing whitespace and newlines
    let mut end = line.len();
    while end > start
        && (line[end - 1] == b' '
            || line[end - 1] == b'\t'
            || line[end - 1] == b'\r'
            || line[end - 1] == b'\n')
    {
        end -= 1;
    }

    let trimmed = &line[start..end];

    if trimmed.len() < 2 {
        return false;
    }

    // Check for "GO" (case-insensitive)
    if trimmed.len() == 2 {
        return (trimmed[0] == b'G' || trimmed[0] == b'g')
            && (trimmed[1] == b'O' || trimmed[1] == b'o');
    }

    // Check for "GO <number>" pattern
    if (trimmed[0] == b'G' || trimmed[0] == b'g')
        && (trimmed[1] == b'O' || trimmed[1] == b'o')
        && (trimmed[2] == b' ' || trimmed[2] == b'\t')
    {
        // Rest should be whitespace and digits
        let rest = &trimmed[3..];
        let rest_trimmed = rest
            .iter()
            .skip_while(|&&b| b == b' ' || b == b'\t')
            .copied()
            .collect::<Vec<_>>();
        return rest_trimmed.is_empty() || rest_trimmed.iter().all(|&b| b.is_ascii_digit());
    }

    false
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

impl StatementType {
    /// Returns true if this is a schema-related statement (DDL)
    pub fn is_schema(&self) -> bool {
        matches!(
            self,
            StatementType::CreateTable
                | StatementType::CreateIndex
                | StatementType::AlterTable
                | StatementType::DropTable
        )
    }

    /// Returns true if this is a data-related statement (DML)
    pub fn is_data(&self) -> bool {
        matches!(self, StatementType::Insert | StatementType::Copy)
    }
}

/// Content filter mode for splitting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContentFilter {
    /// Include both schema and data statements (default)
    #[default]
    All,
    /// Only schema statements (CREATE TABLE, CREATE INDEX, ALTER TABLE, DROP TABLE)
    SchemaOnly,
    /// Only data statements (INSERT, COPY)
    DataOnly,
}

static CREATE_TABLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*CREATE\s+TABLE\s+`?([^\s`(]+)`?").unwrap());

static INSERT_INTO_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^\s*INSERT\s+INTO\s+`?([^\s`(]+)`?").unwrap());

static CREATE_INDEX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)ON\s+`?([^\s`(;]+)`?").unwrap());

// MSSQL CREATE INDEX: extracts table from ON [schema].[table] or ON [table]
// Matches: ON [table], ON [dbo].[table], ON [db].[dbo].[table]
// Captures the last bracketed or unbracketed identifier before (
static CREATE_INDEX_MSSQL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)ON\s+(?:\[?[^\[\]\s]+\]?\s*\.\s*)*\[([^\[\]]+)\]").unwrap());

static ALTER_TABLE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)ALTER\s+TABLE\s+`?([^\s`;]+)`?").unwrap());

static DROP_TABLE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[`"]?([^\s`"`;]+)[`"]?"#).unwrap()
});

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

        // For MSSQL, use line-based parsing to handle GO batch separator
        if self.dialect == SqlDialect::Mssql {
            return self.read_statement_mssql();
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
                            let tag_bytes = &buf[i + 1..i + 1 + end];

                            // Validate tag: must be empty OR identifier-like [A-Za-z_][A-Za-z0-9_]*
                            let is_valid_tag = if tag_bytes.is_empty() {
                                true
                            } else {
                                let mut iter = tag_bytes.iter();
                                match iter.next() {
                                    Some(&first)
                                        if first.is_ascii_alphabetic() || first == b'_' =>
                                    {
                                        iter.all(|&c| c.is_ascii_alphanumeric() || c == b'_')
                                    }
                                    _ => false,
                                }
                            };

                            if is_valid_tag {
                                dollar_tag = tag_bytes.to_vec();
                                in_dollar_quote = true;
                                continue;
                            }
                            // Invalid tag - treat $ as normal character
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

    /// Read MSSQL statement with GO batch separator support
    /// GO is a batch separator that appears on its own line
    fn read_statement_mssql(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.stmt_buffer.clear();

        let mut inside_single_quote = false;
        let mut inside_bracket_quote = false;
        let mut in_line_comment = false;
        let mut line_start = 0usize; // Track where current line started in stmt_buffer

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
                let inside_string = inside_single_quote || inside_bracket_quote;

                // End of line comment on newline
                if in_line_comment {
                    if b == b'\n' {
                        in_line_comment = false;
                        // Add to buffer and update line_start
                        self.stmt_buffer.extend_from_slice(&buf[consumed..=i]);
                        consumed = i + 1;
                        line_start = self.stmt_buffer.len();
                    }
                    continue;
                }

                // Handle line comments (-- to end of line)
                if b == b'-' && !inside_string && i + 1 < buf.len() && buf[i + 1] == b'-' {
                    in_line_comment = true;
                    continue;
                }

                // Handle N'...' unicode strings - treat N as prefix, ' as quote start
                // (The N is just a prefix, single quote handling is the same)

                // Handle string quotes
                if b == b'\'' && !inside_bracket_quote {
                    inside_single_quote = !inside_single_quote;
                } else if b == b'[' && !inside_single_quote {
                    inside_bracket_quote = true;
                } else if b == b']' && inside_bracket_quote {
                    // Check for escaped ]]
                    if i + 1 < buf.len() && buf[i + 1] == b']' {
                        // Skip the escape sequence - consume one extra ]
                        continue;
                    }
                    inside_bracket_quote = false;
                } else if b == b';' && !inside_string {
                    // Semicolon is a statement terminator in MSSQL too
                    self.stmt_buffer.extend_from_slice(&buf[consumed..=i]);
                    consumed = i + 1;
                    found_terminator = true;
                    break;
                } else if b == b'\n' && !inside_string {
                    // Check if the current line (from line_start to here) is just "GO"
                    // First, add bytes up to and including the newline
                    self.stmt_buffer.extend_from_slice(&buf[consumed..=i]);
                    consumed = i + 1;

                    // Get the line we just completed
                    let line = &self.stmt_buffer[line_start..];
                    if is_go_line(line) {
                        // Remove the GO line from the buffer
                        self.stmt_buffer.truncate(line_start);
                        // Trim trailing whitespace from the statement
                        while self
                            .stmt_buffer
                            .last()
                            .is_some_and(|&b| b == b'\n' || b == b'\r' || b == b' ' || b == b'\t')
                        {
                            self.stmt_buffer.pop();
                        }
                        // If we have content, return it
                        if !self.stmt_buffer.is_empty() {
                            self.reader.consume(consumed);
                            let result = std::mem::take(&mut self.stmt_buffer);
                            return Ok(Some(result));
                        }
                        // Otherwise, reset and continue (empty batch)
                        line_start = 0;
                    } else {
                        // Update line_start to after the newline
                        line_start = self.stmt_buffer.len();
                    }
                    continue;
                }
            }

            if found_terminator {
                self.reader.consume(consumed);
                let result = std::mem::take(&mut self.stmt_buffer);
                return Ok(Some(result));
            }

            // Add remaining bytes to buffer
            if consumed < buf.len() {
                self.stmt_buffer.extend_from_slice(&buf[consumed..]);
            }
            let len = buf.len();
            self.reader.consume(len);
        }
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

        if upper_prefix.starts_with(b"CREATE INDEX")
            || upper_prefix.starts_with(b"CREATE UNIQUE")
            || upper_prefix.starts_with(b"CREATE CLUSTERED")
            || upper_prefix.starts_with(b"CREATE NONCLUSTER")
        {
            // For MSSQL, try the bracket-aware regex first
            if dialect == SqlDialect::Mssql {
                if let Some(caps) = CREATE_INDEX_MSSQL_RE.captures(stmt) {
                    if let Some(m) = caps.get(1) {
                        return (
                            StatementType::CreateIndex,
                            String::from_utf8_lossy(m.as_bytes()).into_owned(),
                        );
                    }
                }
            }
            // Fall back to generic regex for MySQL/PostgreSQL/SQLite
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

        // MSSQL BULK INSERT - treat as Insert statement type
        if upper_prefix.starts_with(b"BULK INSERT") {
            if let Some(name) = extract_table_name_flexible(stmt, 11, dialect) {
                return (StatementType::Insert, name);
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

        // Handle -- line comments
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

        // Handle /* */ block comments (including MySQL conditional comments)
        if data.len() >= 2 && data[0] == b'/' && data[1] == b'*' {
            // Find the closing */
            let mut i = 2;
            let mut depth = 1;
            while i < data.len() - 1 && depth > 0 {
                if data[i] == b'*' && data[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else if data[i] == b'/' && data[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth == 0 {
                data = &data[i..];
                continue;
            } else {
                // Unclosed comment runs to EOF
                return &[];
            }
        }

        // Handle # line comments (MySQL)
        if !data.is_empty() && data[0] == b'#' {
            if let Some(pos) = data.iter().position(|&b| b == b'\n') {
                data = &data[pos + 1..];
                continue;
            } else {
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

    // Check for IF NOT EXISTS or IF EXISTS
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
    } else if upper_check.starts_with(b"IF EXISTS") {
        i += 9; // Skip "IF EXISTS"
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
        // Determine quote character based on dialect
        let (quote_char, close_char) = match stmt.get(i) {
            Some(b'`') if dialect == SqlDialect::MySql => {
                i += 1;
                (Some(b'`'), b'`')
            }
            Some(b'"') if dialect != SqlDialect::MySql => {
                i += 1;
                (Some(b'"'), b'"')
            }
            Some(b'"') => {
                // Allow double quotes for MySQL too (though less common)
                i += 1;
                (Some(b'"'), b'"')
            }
            Some(b'[') if dialect == SqlDialect::Mssql => {
                // MSSQL square bracket quoting
                i += 1;
                (Some(b'['), b']')
            }
            _ => (None, 0),
        };

        let start = i;

        while i < stmt.len() {
            let b = stmt[i];
            if quote_char.is_some() {
                if b == close_char {
                    // For MSSQL, check for escaped ]]
                    if dialect == SqlDialect::Mssql
                        && close_char == b']'
                        && i + 1 < stmt.len()
                        && stmt[i + 1] == b']'
                    {
                        // Escaped bracket, skip both
                        i += 2;
                        continue;
                    }
                    let name = &stmt[start..i];
                    // For MSSQL, unescape ]] to ]
                    let name_str = if dialect == SqlDialect::Mssql {
                        String::from_utf8_lossy(name).replace("]]", "]")
                    } else {
                        String::from_utf8_lossy(name).into_owned()
                    };
                    parts.push(name_str);
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
