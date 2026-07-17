pub mod mysql_insert;
pub mod postgres_copy;

// Re-export types for bulk loading (consumed by the duckdb loader and by
// library users; the bin target without duckdb-query doesn't use them)
#[cfg_attr(not(feature = "duckdb-query"), allow(unused_imports))]
pub use mysql_insert::{parse_insert_for_bulk, ParsedValue};

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

    // Simple "substring present => add weight to a dialect" markers. Confidence
    // is encoded by the weight: 20/10 = high, 5 = medium, 2 = low.
    const MARKERS: &[(&[u8], SqlDialect, u32)] = &[
        (b"pg_dump", SqlDialect::Postgres, 10),
        (b"PostgreSQL database dump", SqlDialect::Postgres, 10),
        (b"MySQL dump", SqlDialect::MySql, 10),
        (b"MariaDB dump", SqlDialect::MySql, 10),
        (b"SQLite", SqlDialect::Sqlite, 10),
        (b"search_path", SqlDialect::Postgres, 5),
        (b"LOCK TABLES", SqlDialect::MySql, 5),
        (b"PRAGMA", SqlDialect::Sqlite, 5),
        (b"CREATE EXTENSION", SqlDialect::Postgres, 2),
        // BEGIN TRANSACTION is generic ANSI SQL, only slightly suggests SQLite
        (b"BEGIN TRANSACTION", SqlDialect::Sqlite, 2),
        (b"SET ANSI_NULLS", SqlDialect::Mssql, 20),
        (b"SET QUOTED_IDENTIFIER", SqlDialect::Mssql, 20),
        (b"IDENTITY(", SqlDialect::Mssql, 10),
        (b"ON [PRIMARY]", SqlDialect::Mssql, 10),
        (b"NVARCHAR", SqlDialect::Mssql, 5),
        (b"CLUSTERED", SqlDialect::Mssql, 5),
        (b"SET NOCOUNT", SqlDialect::Mssql, 5),
    ];
    for &(needle, dialect, weight) in MARKERS {
        if contains_bytes(header, needle) {
            match dialect {
                SqlDialect::MySql => score.mysql += weight,
                SqlDialect::Postgres => score.postgres += weight,
                SqlDialect::Sqlite => score.sqlite += weight,
                SqlDialect::Mssql => score.mssql += weight,
            }
        }
    }

    // Compound / structural markers that don't fit the simple table.
    if contains_bytes(header, b"COPY ") && contains_bytes(header, b"FROM stdin") {
        score.postgres += 5;
    }
    if contains_bytes(header, b"/*!40") || contains_bytes(header, b"/*!50") {
        score.mysql += 5;
    }
    if contains_bytes(header, b"$$") {
        score.postgres += 2;
    }
    // Backticks suggest MySQL (could also appear in data/comments)
    if header.contains(&b'`') {
        score.mysql += 2;
    }
    // GO as batch separator on its own line (check for common patterns)
    if contains_bytes(header, b"\nGO\n") || contains_bytes(header, b"\nGO\r\n") {
        score.mssql += 15;
    }
    // Square-bracket identifiers, but distinguish them from PostgreSQL array
    // types: `[id]`/`[dbo]` (identifier-shaped) => MSSQL; `integer[]` (empty)
    // or `[5]` (non-identifier) => not MSSQL (bug #9).
    {
        let mut mssql_bracket_ident = false;
        let mut pg_array_brackets = false;
        let mut k = 0;
        while k < header.len() {
            if header[k] == b'[' {
                if let Some(rel) = header[k + 1..].iter().position(|&c| c == b']') {
                    let inner = &header[k + 1..k + 1 + rel];
                    let ident_shaped = inner
                        .first()
                        .is_some_and(|&c| c.is_ascii_alphabetic() || c == b'_')
                        && inner
                            .iter()
                            .all(|&c| c.is_ascii_alphanumeric() || c == b'_' || c == b' ');
                    if inner.is_empty() {
                        pg_array_brackets = true;
                    } else if ident_shaped {
                        mssql_bracket_ident = true;
                    }
                    k += rel + 2;
                    continue;
                }
            }
            k += 1;
        }
        if mssql_bracket_ident {
            score.mssql += 10;
        }
        if pg_array_brackets {
            score.postgres += 2;
        }
    }

    // N'unicode' literal — require a non-word char before N so ordinary data
    // ending in N before a quote (e.g. ...JOHN') doesn't score MSSQL (bug #9).
    {
        let mut k = 0;
        while k + 1 < header.len() {
            if header[k] == b'N' && header[k + 1] == b'\'' {
                let before_ok =
                    k == 0 || !(header[k - 1].is_ascii_alphanumeric() || header[k - 1] == b'_');
                if before_ok {
                    score.mssql += 5;
                    break;
                }
            }
            k += 1;
        }
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

/// Detect dialect from a file, reading its first 8KB. Goes through
/// [`crate::splitter::open_input`] so compressed and zipped inputs are
/// transparently decoded before sniffing — without that, detection would
/// see raw compressed bytes instead of SQL text.
pub fn detect_dialect_from_file(path: &std::path::Path) -> anyhow::Result<DialectDetectionResult> {
    use std::io::Read;

    let mut reader = crate::splitter::open_input(path)?;
    let mut buf = [0u8; 8192];
    let n = reader.read(&mut buf)?;
    Ok(detect_dialect(&buf[..n]))
}

#[inline]
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    memchr::memmem::find(haystack, needle).is_some()
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
        // Rest should be whitespace and digits (allocation-free scan)
        return trimmed[3..]
            .iter()
            .skip_while(|&&b| b == b' ' || b == b'\t')
            .all(|&b| b.is_ascii_digit());
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

// Word boundary before ON so index names ending in "on" (idx_position,
// idx_created_on, ...) don't match the "on" inside the identifier.
static CREATE_INDEX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bON\s+`?([^\s`(;]+)`?").unwrap());

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

/// Result of peeking at the byte after position `i` for a two-byte token
/// (`--`, `/*`, `*/`, `]]`). See [`Parser::peek2`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Peek2 {
    /// The next byte is present and matches: the token is complete.
    Hit,
    /// The next byte differs, or input ended at EOF (a dangling first byte at
    /// the tail is treated as a literal byte).
    Miss,
    /// The buffer ends here but more input may arrive: the caller must pause
    /// the scan (`break 'scan`) with its state intact so the token can never
    /// straddle a refill (see the `buf` field docs).
    NeedMore,
}

/// Control-flow signal returned by row/event visitors.
///
/// Lives in the parser layer (rather than a consumer module) so the streaming
/// row visitors ([`crate::parser::mysql_insert::visit_insert_rows_with`],
/// [`crate::parser::postgres_copy::visit_postgres_copy_rows_with`]) and
/// [`Parser::visit_events`] can all speak the same vocabulary. Re-exported from
/// `crate::transform_common` for the sample/shard consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFlow {
    /// Keep iterating.
    Continue,
    /// Skip the remaining rows of the current INSERT/COPY statement, then
    /// resume with the next statement.
    SkipStatement,
    /// Stop the walk entirely.
    Stop,
}

/// A single streaming event emitted by [`Parser::visit_events`].
///
/// Data rows are treated as stream boundaries: a multi-row INSERT is delivered
/// as its shared header plus one [`ParserEvent::InsertRow`] per tuple, and a
/// COPY block as [`ParserEvent::CopyStart`] / one [`ParserEvent::CopyRow`] per
/// line / [`ParserEvent::CopyEnd`] — never buffering the whole block. All byte
/// slices borrow from the parser's internal buffer (or, for `InsertRow.header`,
/// a small owned copy of the header) and are only valid until the callback
/// returns.
pub enum ParserEvent<'a> {
    /// A complete non-data statement (DDL / control), including its terminator.
    Statement(&'a [u8]),
    /// One INSERT `(...)` value tuple. `header` is the statement text up to and
    /// including the `VALUES` keyword (so the column list can be recovered);
    /// `first_in_statement` is true for the first row of each INSERT so callers
    /// can rebuild per-statement column context exactly once.
    InsertRow {
        header: &'a [u8],
        row: &'a [u8],
        first_in_statement: bool,
    },
    /// The `COPY ... FROM stdin;` header line that opens a COPY data block.
    CopyStart(&'a [u8]),
    /// One raw COPY data line (newline stripped; a trailing `\r`, if any, is
    /// left for the consumer to strip so it matches the collecting parser).
    CopyRow(&'a [u8]),
    /// The `\.` terminator that closes a COPY data block.
    CopyEnd,
}

/// Configurable safety caps on how much a single buffered unit may grow before
/// [`Parser::visit_events`] reports an error (rather than silently truncating
/// or buffering without bound). Defaults are generous; they exist to turn a
/// pathological/corrupt input into a diagnosable error instead of an OOM.
#[derive(Debug, Clone, Copy)]
pub struct ParserLimits {
    /// Max bytes for one non-data statement (DDL/control).
    pub max_ddl: usize,
    /// Max bytes for an INSERT/COPY header before the first row.
    pub max_header: usize,
    /// Max bytes for a single INSERT tuple or COPY line.
    pub max_row: usize,
}

impl Default for ParserLimits {
    fn default() -> Self {
        Self {
            max_ddl: 256 * 1024 * 1024,
            max_header: 64 * 1024 * 1024,
            max_row: 256 * 1024 * 1024,
        }
    }
}

pub struct Parser<R: Read> {
    reader: BufReader<R>,
    dialect: SqlDialect,
    /// For PostgreSQL: true when reading COPY data block
    in_copy_data: bool,
    /// Accumulated, not-yet-returned input. The scanners read whole `fill_buf`
    /// chunks into `buf` and scan it with an index cursor, so a multi-byte
    /// token (`--`, `/* */`, `$tag$`, `]]`) can never straddle an internal
    /// buffer boundary. `buf_pos` is the start of the next statement to return;
    /// bytes before it are already-returned and are dropped on the next refill
    /// (never per-statement) so scanning stays O(n) rather than O(n²).
    buf: Vec<u8>,
    buf_pos: usize,
    /// Number of bytes dropped off the front of `buf` so far, so an absolute
    /// input offset can be reported in [`Parser::visit_events`] diagnostics.
    base: u64,
    /// High-water mark of `buf.len()`, so tests can assert the streaming walker
    /// keeps its working set bounded.
    peak_buffered: usize,
    /// Safety caps for [`Parser::visit_events`].
    limits: ParserLimits,
}

impl<R: Read> Parser<R> {
    #[allow(dead_code)]
    pub fn new(reader: R, buffer_size: usize) -> Self {
        Self::with_dialect(reader, buffer_size, SqlDialect::default())
    }

    pub fn with_dialect(reader: R, buffer_size: usize, dialect: SqlDialect) -> Self {
        Self {
            reader: BufReader::with_capacity(buffer_size, reader),
            dialect,
            in_copy_data: false,
            buf: Vec::new(),
            buf_pos: 0,
            base: 0,
            peak_buffered: 0,
            limits: ParserLimits::default(),
        }
    }

    /// Override the [`ParserLimits`] used by [`Parser::visit_events`].
    #[allow(dead_code)]
    pub fn with_limits(mut self, limits: ParserLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Total bytes currently buffered in memory. Used by tests to assert the
    /// event walker keeps its working set bounded even for huge INSERT/COPY
    /// blocks.
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// High-water mark of buffered bytes over this parser's lifetime. Used by
    /// tests to assert [`Parser::visit_events`] never buffers a whole multi-row
    /// INSERT/COPY block.
    pub fn peak_buffered(&self) -> usize {
        self.peak_buffered
    }

    /// Drop the already-returned prefix `buf[..keep_from]` and append the next
    /// `fill_buf` chunk. Returns `(bytes_removed_from_front, got_more_data)`.
    /// Callers subtract `bytes_removed_from_front` from their scan indices.
    #[inline]
    fn grow_buffer(&mut self, keep_from: usize) -> std::io::Result<(usize, bool)> {
        if keep_from > 0 {
            self.buf.drain(..keep_from);
            self.base += keep_from as u64;
        }
        let chunk = self.reader.fill_buf()?;
        if chunk.is_empty() {
            return Ok((keep_from, false));
        }
        self.buf.extend_from_slice(chunk);
        let n = chunk.len();
        self.reader.consume(n);
        self.peak_buffered = self.peak_buffered.max(self.buf.len());
        Ok((keep_from, true))
    }

    /// Classify the byte after `i` against the second byte of a two-byte
    /// token. Centralizes the boundary handling shared by every such token in
    /// [`read_statement`](Self::read_statement) and `read_statement_mssql`.
    #[inline(always)]
    fn peek2(&self, i: usize, expected: u8, at_eof: bool) -> Peek2 {
        match self.buf.get(i + 1) {
            Some(&c) if c == expected => Peek2::Hit,
            Some(_) => Peek2::Miss,
            None if at_eof => Peek2::Miss,
            None => Peek2::NeedMore,
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

        let is_mysql = self.dialect == SqlDialect::MySql;
        let is_postgres = self.dialect == SqlDialect::Postgres;

        // Scan `self.buf` from the current statement start with an index cursor.
        // More data is appended by `grow_buffer` (which also drops already-
        // returned statements), so a multi-byte token (`--`, `/* */`, `$tag$`)
        // never straddles a chunk boundary: the scan waits for more bytes with
        // its state (`i`, quote/comment flags) intact.
        let mut start = self.buf_pos;
        let mut i = self.buf_pos;

        let mut inside_single_quote = false;
        let mut inside_double_quote = false;
        let mut inside_backtick = false; // MySQL `identifier` quoting (bug #10)
        let mut escaped = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false; // /* ... */ (bug #5)
        let mut in_dollar_quote = false;
        let mut dollar_tag: Vec<u8> = Vec::new();
        let mut at_eof = false;

        // Lookup table of bytes that are significant outside strings/comments;
        // everything else can be skipped in a tight loop.
        let mut sig = [false; 256];
        for &c in b";'\"-/" {
            sig[c as usize] = true;
        }
        if is_mysql {
            sig[b'`' as usize] = true;
            sig[b'#' as usize] = true;
        }
        if is_postgres {
            sig[b'$' as usize] = true;
        }

        loop {
            'scan: while i < self.buf.len() {
                let b = self.buf[i];

                // Inside a /* */ block comment: only the closing */ ends it.
                if in_block_comment {
                    if b == b'*' {
                        match self.peek2(i, b'/', at_eof) {
                            Peek2::Hit => {
                                in_block_comment = false;
                                i += 2;
                                continue;
                            }
                            Peek2::Miss => {
                                i += 1;
                                continue;
                            }
                            Peek2::NeedMore => break 'scan,
                        }
                    }
                    i += 1;
                    continue;
                }

                // Inside a -- or # line comment: consume until newline.
                if in_line_comment {
                    if b == b'\n' {
                        in_line_comment = false;
                    }
                    i += 1;
                    continue;
                }

                if escaped {
                    escaped = false;
                    i += 1;
                    continue;
                }

                let inside_string = inside_single_quote
                    || inside_double_quote
                    || inside_backtick
                    || in_dollar_quote;

                // Fast path: inside a string/quoted region only the closing
                // delimiter (and, for MySQL, a `\` escape) is significant, so
                // memchr past the ordinary bytes. This is the hot loop for
                // INSERT string data.
                if inside_string {
                    let rest = &self.buf[i..];
                    let hit = if inside_single_quote {
                        if is_mysql {
                            memchr::memchr2(b'\'', b'\\', rest)
                        } else {
                            memchr::memchr(b'\'', rest)
                        }
                    } else if inside_double_quote {
                        if is_mysql {
                            memchr::memchr2(b'"', b'\\', rest)
                        } else {
                            memchr::memchr(b'"', rest)
                        }
                    } else if inside_backtick {
                        memchr::memchr(b'`', rest)
                    } else {
                        // in_dollar_quote: only `$` can start the closing tag
                        memchr::memchr(b'$', rest)
                    };
                    match hit {
                        Some(0) => {} // significant byte at i
                        Some(off) => {
                            i += off;
                            continue;
                        }
                        None if at_eof => {
                            i = self.buf.len();
                            continue;
                        }
                        None => break 'scan,
                    }
                }

                // MySQL backslash escapes apply inside both '…' and "…" string
                // literals (but not backtick identifiers or dollar quotes).
                if b == b'\\' && (inside_single_quote || inside_double_quote) && is_mysql {
                    escaped = true;
                    i += 1;
                    continue;
                }

                if !inside_string {
                    // Fast path: skip runs of insignificant bytes outside any
                    // string/comment with a tight table lookup.
                    if !sig[b as usize] {
                        let mut j = i + 1;
                        while j < self.buf.len() && !sig[self.buf[j] as usize] {
                            j += 1;
                        }
                        i = j;
                        continue;
                    }
                    // -- line comment
                    if b == b'-' {
                        match self.peek2(i, b'-', at_eof) {
                            Peek2::Hit => {
                                // MySQL requires whitespace/EOL after `--` for it
                                // to be a comment; `a--b` is arithmetic. Other
                                // dialects always treat `--` as a comment.
                                if is_mysql {
                                    match self.buf.get(i + 2) {
                                        Some(&c)
                                            if c == b' '
                                                || c == b'\t'
                                                || c == b'\n'
                                                || c == b'\r' =>
                                        {
                                            in_line_comment = true;
                                            i += 2;
                                            continue;
                                        }
                                        None if at_eof => {
                                            in_line_comment = true;
                                            i += 2;
                                            continue;
                                        }
                                        None => break 'scan,
                                        // `--x`: not a comment, just consume one `-`.
                                        Some(_) => {
                                            i += 1;
                                            continue;
                                        }
                                    }
                                }
                                in_line_comment = true;
                                i += 2;
                                continue;
                            }
                            Peek2::Miss => {
                                i += 1;
                                continue;
                            }
                            Peek2::NeedMore => break 'scan,
                        }
                    }
                    // MySQL # line comment
                    if b == b'#' && is_mysql {
                        in_line_comment = true;
                        i += 1;
                        continue;
                    }
                    // /* block comment
                    if b == b'/' {
                        match self.peek2(i, b'*', at_eof) {
                            Peek2::Hit => {
                                in_block_comment = true;
                                i += 2;
                                continue;
                            }
                            Peek2::Miss => {
                                i += 1;
                                continue;
                            }
                            Peek2::NeedMore => break 'scan,
                        }
                    }
                }

                // PostgreSQL dollar-quoting (outside other quotes).
                if is_postgres && !inside_single_quote && !inside_double_quote && !inside_backtick {
                    if b == b'$' && !in_dollar_quote {
                        match self.buf[i + 1..].iter().position(|&c| c == b'$') {
                            Some(end) => {
                                let tag_bytes = &self.buf[i + 1..i + 1 + end];
                                if is_valid_dollar_tag(tag_bytes) {
                                    dollar_tag = tag_bytes.to_vec();
                                    in_dollar_quote = true;
                                    // Skip the whole opening $tag$ so its trailing
                                    // $ is not re-read as a closer (bug #7).
                                    i += end + 2;
                                    continue;
                                }
                                // Not a valid tag: treat $ as an ordinary byte.
                                i += 1;
                                continue;
                            }
                            None if at_eof => {
                                i += 1;
                                continue;
                            }
                            // No closing $ yet — it may arrive in the next chunk.
                            None => break 'scan,
                        }
                    } else if b == b'$' && in_dollar_quote {
                        let tag_len = dollar_tag.len();
                        if i + 1 + tag_len < self.buf.len() {
                            if self.buf[i + 1..i + 1 + tag_len] == dollar_tag[..]
                                && self.buf[i + 1 + tag_len] == b'$'
                            {
                                in_dollar_quote = false;
                                dollar_tag.clear();
                                i += tag_len + 2;
                                continue;
                            }
                            i += 1;
                            continue;
                        } else if at_eof {
                            i += 1;
                            continue;
                        } else {
                            break 'scan;
                        }
                    }
                }

                if b == b'\'' && !inside_double_quote && !inside_backtick && !in_dollar_quote {
                    inside_single_quote = !inside_single_quote;
                } else if b == b'"' && !inside_single_quote && !inside_backtick && !in_dollar_quote
                {
                    inside_double_quote = !inside_double_quote;
                } else if b == b'`'
                    && is_mysql
                    && !inside_single_quote
                    && !inside_double_quote
                    && !in_dollar_quote
                {
                    inside_backtick = !inside_backtick;
                } else if b == b';' && !inside_string {
                    // Statement terminator. The bytes after it stay in `buf` and
                    // become the next statement (buf_pos advances, no shifting).
                    let result = self.buf[start..=i].to_vec();
                    self.buf_pos = i + 1;

                    if is_postgres && self.is_copy_from_stdin(&result) {
                        self.in_copy_data = true;
                    }
                    return Ok(Some(result));
                }

                i += 1;
            }

            if at_eof {
                if start >= self.buf.len() {
                    return Ok(None);
                }
                let result = self.buf[start..].to_vec();
                self.buf_pos = self.buf.len();
                return Ok(Some(result));
            }

            // Need more bytes. Drop already-returned statements before `start`.
            let (removed, got_more) = self.grow_buffer(start)?;
            start -= removed;
            i -= removed;
            if !got_more {
                // Re-scan the remaining bytes once more with at_eof set so any
                // incomplete token at the tail is treated literally.
                at_eof = true;
            }
        }
    }

    /// Check if statement is a PostgreSQL COPY FROM stdin
    fn is_copy_from_stdin(&self, stmt: &[u8]) -> bool {
        // Strip leading comments (pg_dump adds -- comments before COPY statements)
        let stmt = strip_leading_comments_and_whitespace(stmt);
        if stmt.len() < 17 {
            // Minimum: "COPY x FROM STDIN" = 17 chars
            return false;
        }

        // Check prefix with stack-allocated buffer (avoid heap allocation)
        let mut prefix = [0u8; 5];
        for (i, &b) in stmt.iter().take(5).enumerate() {
            prefix[i] = b.to_ascii_uppercase();
        }
        if &prefix != b"COPY " {
            return false;
        }

        // Search for "FROM STDIN" case-insensitively without allocating
        // Look within first 500 bytes (typical COPY statements are shorter)
        let search_len = stmt.len().min(500);
        if search_len >= 10 {
            // Inclusive upper bound so a "FROM STDIN" ending exactly at the
            // window edge isn't missed.
            for i in 0..=(search_len - 10) {
                if stmt[i..i + 10]
                    .iter()
                    .zip(b"FROM STDIN".iter())
                    .all(|(&a, &b)| a.to_ascii_uppercase() == b)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Read PostgreSQL COPY data block until we see the terminator line (\.).
    /// The returned block includes everything up to and including the
    /// terminator line; anything after it is carried in `pending`.
    ///
    /// Note: a COPY block is buffered whole in memory (like every other
    /// statement here) — its size sets the memory floor for a pg_dump with one
    /// very large table. This is intentional: downstream consumers (redact,
    /// diff, sample) operate on a full block at a time.
    fn read_copy_data(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let mut start = self.buf_pos;
        let mut scan = self.buf_pos;

        loop {
            // Scan completed lines for the terminator.
            while let Some(rel) = memchr::memchr(b'\n', &self.buf[scan..]) {
                let nl = scan + rel;
                let line = &self.buf[scan..=nl];
                if line == b"\\.\n" || line == b"\\.\r\n" {
                    self.in_copy_data = false;
                    let result = self.buf[start..=nl].to_vec();
                    self.buf_pos = nl + 1;
                    return Ok(Some(result));
                }
                scan = nl + 1;
            }

            let (removed, got_more) = self.grow_buffer(start)?;
            start -= removed;
            scan -= removed;
            if !got_more {
                self.in_copy_data = false;
                if start >= self.buf.len() {
                    return Ok(None);
                }
                let result = self.buf[start..].to_vec();
                self.buf_pos = self.buf.len();
                return Ok(Some(result));
            }
        }
    }

    /// Stream the input as [`ParserEvent`]s with bounded memory.
    ///
    /// Ordinary DDL/control statements are buffered whole (they are small) and
    /// delivered as [`ParserEvent::Statement`]. A multi-row INSERT is delivered
    /// as its header plus one [`ParserEvent::InsertRow`] per tuple, and a COPY
    /// block as [`ParserEvent::CopyStart`] / [`ParserEvent::CopyRow`] per line /
    /// [`ParserEvent::CopyEnd`] — the whole multi-row block is never buffered at
    /// once. The callback must consume each borrowed slice before returning.
    ///
    /// Unlike [`read_statement`](Self::read_statement) (kept for compatibility),
    /// data statements are not routed through it: their rows are streamed.
    pub fn visit_events<F>(&mut self, mut on_event: F) -> anyhow::Result<()>
    where
        F: FnMut(ParserEvent<'_>) -> anyhow::Result<RowFlow>,
    {
        loop {
            // Ensure at least one byte at/after buf_pos, or stop at EOF.
            while self.buf_pos >= self.buf.len() {
                let (removed, more) = self.grow_buffer(self.buf_pos)?;
                self.buf_pos -= removed;
                if !more {
                    return Ok(());
                }
            }

            if self.peek_is_insert()? {
                if self.visit_insert_stmt(&mut on_event)? == RowFlow::Stop {
                    return Ok(());
                }
                continue;
            }

            // Non-INSERT: reuse read_statement to consume one DDL/control or
            // COPY header statement (small). COPY data blocks are then streamed
            // line-by-line rather than buffered whole.
            let stmt = match self.read_statement()? {
                Some(s) => s,
                None => return Ok(()),
            };
            if self.in_copy_data {
                self.in_copy_data = false;
                if on_event(ParserEvent::CopyStart(&stmt))? == RowFlow::Stop {
                    return Ok(());
                }
                if self.visit_copy_data(&mut on_event)? == RowFlow::Stop {
                    return Ok(());
                }
            } else if on_event(ParserEvent::Statement(&stmt))? == RowFlow::Stop {
                return Ok(());
            }
        }
    }

    /// Peek whether the next statement (after leading comments/whitespace) is an
    /// `INSERT INTO`/`INSERT ONLY`, growing the buffer just enough to decide.
    fn peek_is_insert(&mut self) -> anyhow::Result<bool> {
        loop {
            let stripped = strip_leading_comments_and_whitespace(&self.buf[self.buf_pos..]);
            if stripped.len() >= 11 {
                return Ok(starts_with_insert(stripped));
            }
            if self.buf.len() - self.buf_pos > self.limits.max_header {
                anyhow::bail!(
                    "statement header exceeds max_header ({} bytes) at input offset {}",
                    self.limits.max_header,
                    self.base + self.buf_pos as u64
                );
            }
            let (removed, more) = self.grow_buffer(self.buf_pos)?;
            self.buf_pos -= removed;
            if !more {
                let stripped = strip_leading_comments_and_whitespace(&self.buf[self.buf_pos..]);
                return Ok(starts_with_insert(stripped));
            }
        }
    }

    /// Stream one INSERT statement's tuples as [`ParserEvent::InsertRow`]s.
    fn visit_insert_stmt<F>(&mut self, on_event: &mut F) -> anyhow::Result<RowFlow>
    where
        F: FnMut(ParserEvent<'_>) -> anyhow::Result<RowFlow>,
    {
        // Locate the VALUES keyword, buffering only the header (bounded).
        let (header, values_abs) = loop {
            let stripped_len =
                strip_leading_comments_and_whitespace(&self.buf[self.buf_pos..]).len();
            let start = self.buf.len() - stripped_len;
            if let Some(vp) = mysql_insert::find_values_keyword_pos(&self.buf[start..]) {
                let values_abs = start + vp;
                break (self.buf[start..values_abs].to_vec(), values_abs);
            }
            if self.buf.len() - self.buf_pos > self.limits.max_header {
                anyhow::bail!(
                    "INSERT header exceeds max_header ({} bytes) at input offset {}",
                    self.limits.max_header,
                    self.base + self.buf_pos as u64
                );
            }
            let (removed, more) = self.grow_buffer(self.buf_pos)?;
            self.buf_pos -= removed;
            if !more {
                // No VALUES keyword (truncated / not a value INSERT): emit the
                // remaining bytes as an opaque statement so nothing is lost.
                let flow = on_event(ParserEvent::Statement(&self.buf[self.buf_pos..]))?;
                self.buf_pos = self.buf.len();
                return Ok(flow);
            }
        };

        let mut cur = values_abs;
        let mut first = true;
        let mut skipping = false;

        loop {
            // Skip inter-tuple whitespace and commas.
            loop {
                while cur < self.buf.len()
                    && matches!(self.buf[cur], b' ' | b'\t' | b'\n' | b'\r' | b',')
                {
                    cur += 1;
                }
                if cur < self.buf.len() {
                    break;
                }
                let (removed, more) = self.grow_buffer(cur)?;
                cur -= removed;
                if !more {
                    break;
                }
            }

            if cur >= self.buf.len() {
                self.buf_pos = self.buf.len();
                return Ok(RowFlow::Continue);
            }
            match self.buf[cur] {
                b';' => {
                    self.buf_pos = cur + 1;
                    return Ok(RowFlow::Continue);
                }
                b'(' => {}
                // Anything else (e.g. an MSSQL GO batch line) ends the INSERT;
                // leave it for the next statement.
                _ => {
                    self.buf_pos = cur;
                    return Ok(RowFlow::Continue);
                }
            }

            // Scan exactly one tuple, growing until it is complete or EOF.
            let (tuple_end, complete) = loop {
                if self.buf.len() - cur > self.limits.max_row {
                    anyhow::bail!(
                        "INSERT row exceeds max_row ({} bytes) at input offset {}",
                        self.limits.max_row,
                        self.base + cur as u64
                    );
                }
                match mysql_insert::scan_insert_tuple(&self.buf[cur..], self.dialect) {
                    Some((len, true)) => break (cur + len, true),
                    Some(_) => {
                        let (removed, more) = self.grow_buffer(cur)?;
                        cur -= removed;
                        if !more {
                            break (self.buf.len(), false);
                        }
                    }
                    None => {
                        self.buf_pos = cur;
                        return Ok(RowFlow::Continue);
                    }
                }
            };

            if skipping {
                cur = tuple_end;
            } else {
                let flow = on_event(ParserEvent::InsertRow {
                    header: &header,
                    row: &self.buf[cur..tuple_end],
                    first_in_statement: first,
                })?;
                first = false;
                match flow {
                    RowFlow::Continue => cur = tuple_end,
                    RowFlow::SkipStatement => {
                        skipping = true;
                        cur = tuple_end;
                    }
                    RowFlow::Stop => {
                        self.buf_pos = tuple_end;
                        return Ok(RowFlow::Stop);
                    }
                }
            }

            if !complete {
                self.buf_pos = self.buf.len();
                return Ok(RowFlow::Continue);
            }
        }
    }

    /// Stream a COPY data block (positioned just past the header's `;`) as
    /// [`ParserEvent::CopyRow`]s followed by [`ParserEvent::CopyEnd`].
    fn visit_copy_data<F>(&mut self, on_event: &mut F) -> anyhow::Result<RowFlow>
    where
        F: FnMut(ParserEvent<'_>) -> anyhow::Result<RowFlow>,
    {
        // Consume the line terminator that ends the `COPY ... FROM stdin;`
        // header line (read_statement stopped at the `;`), so the first emitted
        // CopyRow is a real data line rather than a phantom empty one.
        for &want in b"\r\n" {
            while self.buf_pos >= self.buf.len() {
                let (removed, more) = self.grow_buffer(self.buf_pos)?;
                self.buf_pos -= removed;
                if !more {
                    return Ok(RowFlow::Continue);
                }
            }
            if self.buf[self.buf_pos] == want {
                self.buf_pos += 1;
            }
        }

        let mut ls = self.buf_pos;
        let mut skipping = false;

        loop {
            let nl = loop {
                if let Some(rel) = memchr::memchr(b'\n', &self.buf[ls..]) {
                    break Some(ls + rel);
                }
                if self.buf.len() - ls > self.limits.max_row {
                    anyhow::bail!(
                        "COPY line exceeds max_row ({} bytes) at input offset {}",
                        self.limits.max_row,
                        self.base + ls as u64
                    );
                }
                let (removed, more) = self.grow_buffer(ls)?;
                ls -= removed;
                if !more {
                    break None;
                }
            };

            if nl.is_none() && ls >= self.buf.len() {
                // EOF with no trailing data.
                self.buf_pos = self.buf.len();
                return Ok(RowFlow::Continue);
            }

            let line_end = nl.unwrap_or(self.buf.len());
            let line = &self.buf[ls..line_end];
            let check: &[u8] = if line.last() == Some(&b'\r') {
                &line[..line.len() - 1]
            } else {
                line
            };

            if check == b"\\." {
                let flow = on_event(ParserEvent::CopyEnd)?;
                self.buf_pos = nl.map(|n| n + 1).unwrap_or(self.buf.len());
                return Ok(flow);
            }

            if !skipping {
                match on_event(ParserEvent::CopyRow(line))? {
                    RowFlow::Continue => {}
                    RowFlow::SkipStatement => skipping = true,
                    RowFlow::Stop => {
                        self.buf_pos = nl.map(|n| n + 1).unwrap_or(self.buf.len());
                        return Ok(RowFlow::Stop);
                    }
                }
            }

            match nl {
                Some(n) => ls = n + 1,
                None => {
                    self.buf_pos = self.buf.len();
                    return Ok(RowFlow::Continue);
                }
            }
        }
    }

    /// Read MSSQL statement with GO batch separator support.
    /// GO is a batch separator that appears on its own line; `;` also
    /// terminates. Uses the same `buf` + cursor model as [`read_statement`] so
    /// tokens can't straddle a chunk boundary and scanning stays O(n).
    fn read_statement_mssql(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let mut start = self.buf_pos;
        let mut i = self.buf_pos;
        let mut line_start = self.buf_pos;

        let mut inside_single_quote = false;
        let mut inside_bracket_quote = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut at_eof = false;

        loop {
            'scan: while i < self.buf.len() {
                let b = self.buf[i];

                if in_block_comment {
                    if b == b'*' {
                        match self.peek2(i, b'/', at_eof) {
                            Peek2::Hit => {
                                in_block_comment = false;
                                i += 2;
                                continue;
                            }
                            Peek2::Miss => {
                                i += 1;
                                continue;
                            }
                            Peek2::NeedMore => break 'scan,
                        }
                    }
                    i += 1;
                    continue;
                }

                if in_line_comment {
                    if b == b'\n' {
                        in_line_comment = false;
                        line_start = i + 1;
                    }
                    i += 1;
                    continue;
                }

                if inside_bracket_quote {
                    if b == b']' {
                        match self.peek2(i, b']', at_eof) {
                            // Escaped ]] stays inside the identifier (bug #8)
                            Peek2::Hit => {
                                i += 2;
                                continue;
                            }
                            Peek2::Miss => {
                                inside_bracket_quote = false;
                                i += 1;
                                continue;
                            }
                            Peek2::NeedMore => break 'scan,
                        }
                    }
                    i += 1;
                    continue;
                }

                if inside_single_quote {
                    // Fast-skip to the closing quote (MSSQL has no `\` escapes).
                    match memchr::memchr(b'\'', &self.buf[i..]) {
                        Some(0) => inside_single_quote = false,
                        Some(off) => {
                            i += off;
                            continue;
                        }
                        None if at_eof => {
                            i = self.buf.len();
                            continue;
                        }
                        None => break 'scan,
                    }
                    i += 1;
                    continue;
                }

                // Outside strings/comments.
                if b == b'-' {
                    match self.peek2(i, b'-', at_eof) {
                        Peek2::Hit => {
                            in_line_comment = true;
                            i += 2;
                            continue;
                        }
                        Peek2::Miss => {
                            i += 1;
                            continue;
                        }
                        Peek2::NeedMore => break 'scan,
                    }
                }
                if b == b'/' {
                    match self.peek2(i, b'*', at_eof) {
                        Peek2::Hit => {
                            in_block_comment = true;
                            i += 2;
                            continue;
                        }
                        Peek2::Miss => {
                            i += 1;
                            continue;
                        }
                        Peek2::NeedMore => break 'scan,
                    }
                }
                if b == b'\'' {
                    inside_single_quote = true;
                    i += 1;
                    continue;
                }
                if b == b'[' {
                    inside_bracket_quote = true;
                    i += 1;
                    continue;
                }
                if b == b';' {
                    let result = self.buf[start..=i].to_vec();
                    self.buf_pos = i + 1;
                    return Ok(Some(result));
                }
                if b == b'\n' {
                    if is_go_line(&self.buf[line_start..=i]) {
                        // Trim trailing whitespace before the GO line.
                        let mut stmt_end = line_start;
                        while stmt_end > start
                            && matches!(self.buf[stmt_end - 1], b'\n' | b'\r' | b' ' | b'\t')
                        {
                            stmt_end -= 1;
                        }
                        self.buf_pos = i + 1;
                        if stmt_end > start {
                            return Ok(Some(self.buf[start..stmt_end].to_vec()));
                        }
                        // Empty batch: start a new statement after the GO line.
                        start = i + 1;
                        line_start = i + 1;
                        i += 1;
                        continue;
                    }
                    line_start = i + 1;
                    i += 1;
                    continue;
                }

                i += 1;
            }

            if at_eof {
                if start >= self.buf.len() {
                    return Ok(None);
                }
                // Handle a trailing GO with no final newline (bug #12).
                if is_go_line(&self.buf[line_start..]) {
                    let mut stmt_end = line_start;
                    while stmt_end > start
                        && matches!(self.buf[stmt_end - 1], b'\n' | b'\r' | b' ' | b'\t')
                    {
                        stmt_end -= 1;
                    }
                    self.buf_pos = self.buf.len();
                    if stmt_end > start {
                        return Ok(Some(self.buf[start..stmt_end].to_vec()));
                    }
                    return Ok(None);
                }
                let result = self.buf[start..].to_vec();
                self.buf_pos = self.buf.len();
                return Ok(Some(result));
            }

            let (removed, got_more) = self.grow_buffer(start)?;
            start -= removed;
            i -= removed;
            line_start -= removed;
            if !got_more {
                at_eof = true;
            }
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

        // Use stack-allocated buffer to avoid heap allocation in hot path
        let mut upper_prefix = [0u8; 25];
        let prefix_len = stmt.len().min(25);
        for (i, &b) in stmt.iter().take(prefix_len).enumerate() {
            upper_prefix[i] = b.to_ascii_uppercase();
        }
        let upper_prefix = &upper_prefix[..prefix_len];

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
                    let name = String::from_utf8_lossy(m.as_bytes());
                    // Strip schema qualifier (public.users -> users) so indexes
                    // group under the same key as their CREATE TABLE, which also
                    // strips the schema.
                    let table_name = name.split('.').next_back().unwrap_or(&name).to_string();
                    return (StatementType::CreateIndex, table_name);
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
pub(crate) fn strip_leading_comments_and_whitespace(mut data: &[u8]) -> &[u8] {
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
pub(crate) fn extract_table_name_flexible(
    stmt: &[u8],
    offset: usize,
    dialect: SqlDialect,
) -> Option<String> {
    let mut i = offset;

    // Skip whitespace
    while i < stmt.len() && is_whitespace(stmt[i]) {
        i += 1;
    }

    if i >= stmt.len() {
        return None;
    }

    // Check for IF NOT EXISTS or IF EXISTS (stack-allocated to avoid heap allocation)
    let mut upper_check = [0u8; 20];
    let check_len = (stmt.len() - i).min(20);
    for (idx, &b) in stmt[i..].iter().take(check_len).enumerate() {
        upper_check[idx] = b.to_ascii_uppercase();
    }
    let upper_slice = &upper_check[..check_len];
    if upper_slice.starts_with(b"IF NOT EXISTS") {
        i += 13; // Skip "IF NOT EXISTS"
        while i < stmt.len() && is_whitespace(stmt[i]) {
            i += 1;
        }
    } else if upper_slice.starts_with(b"IF EXISTS") {
        i += 9; // Skip "IF EXISTS"
        while i < stmt.len() && is_whitespace(stmt[i]) {
            i += 1;
        }
    }

    // Check for ONLY (PostgreSQL) - reuse first 10 bytes or re-check if position changed
    let only_check = if i < stmt.len() {
        let mut buf = [0u8; 10];
        let len = (stmt.len() - i).min(10);
        for (idx, &b) in stmt[i..].iter().take(len).enumerate() {
            buf[idx] = b.to_ascii_uppercase();
        }
        (buf, len)
    } else {
        ([0u8; 10], 0)
    };
    let only_slice = &only_check.0[..only_check.1];
    if only_slice.starts_with(b"ONLY ") || only_slice.starts_with(b"ONLY\t") {
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
            // Double quotes are the standard identifier quote for
            // Postgres/SQLite/MSSQL, and accepted for MySQL too (less common).
            Some(b'"') => {
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

/// True if `s` begins with an `INSERT INTO`/`INSERT ONLY` value statement (the
/// forms that carry a `VALUES` list). `BULK INSERT` and other keywords are
/// intentionally excluded so [`Parser::visit_events`] only enters tuple-
/// streaming mode for statements it can actually stream.
#[inline]
fn starts_with_insert(s: &[u8]) -> bool {
    s.len() >= 11
        && (s[..11].eq_ignore_ascii_case(b"INSERT INTO")
            || s[..11].eq_ignore_ascii_case(b"INSERT ONLY"))
}

/// A dollar-quote tag is either empty (`$$`) or an identifier
/// (`[A-Za-z_][A-Za-z0-9_]*`), matching PostgreSQL's rules.
#[inline]
fn is_valid_dollar_tag(tag: &[u8]) -> bool {
    match tag.first() {
        None => true,
        Some(&first) if first.is_ascii_alphabetic() || first == b'_' => tag[1..]
            .iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'_'),
        _ => false,
    }
}

pub fn determine_buffer_size(file_size: u64) -> usize {
    if file_size > 1024 * 1024 * 1024 {
        MEDIUM_BUFFER_SIZE
    } else {
        SMALL_BUFFER_SIZE
    }
}
