//! SQL dump loader for importing dumps into DuckDB.

use super::types::TypeConverter;
use super::{ImportStats, QueryConfig};
use crate::convert::copy_to_insert::{copy_to_inserts, parse_copy_header, CopyHeader};
use crate::parser::{detect_dialect_from_file, Parser, SqlDialect, StatementType};
use crate::progress::ProgressReader;
use crate::splitter::Compression;
use anyhow::{Context, Result};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use once_cell::sync::Lazy;
use regex::Regex;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// Loads SQL dumps into a DuckDB database
pub struct DumpLoader<'a> {
    conn: &'a Connection,
    config: &'a QueryConfig,
}

impl<'a> DumpLoader<'a> {
    /// Create a new dump loader
    pub fn new(conn: &'a Connection, config: &'a QueryConfig) -> Self {
        Self { conn, config }
    }

    /// Load a SQL dump file into DuckDB
    pub fn load(&self, dump_path: &Path) -> Result<ImportStats> {
        let start = std::time::Instant::now();
        let mut stats = ImportStats::default();

        // Detect dialect
        let dialect = if let Some(d) = self.config.dialect {
            d
        } else {
            let result = detect_dialect_from_file(dump_path)?;
            result.dialect
        };

        // Get file size for progress
        let file_size = std::fs::metadata(dump_path)?.len();

        // Set up progress bar
        let progress_bar = if self.config.progress {
            let pb = ProgressBar::new(file_size);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%)")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            Some(pb)
        } else {
            None
        };

        // Open file with compression detection
        let file = File::open(dump_path).context("Failed to open dump file")?;
        let compression = Compression::from_path(dump_path);
        let reader: Box<dyn Read> = match compression {
            Compression::Gzip => Box::new(flate2::read::GzDecoder::new(file)),
            Compression::Bzip2 => Box::new(bzip2::read::BzDecoder::new(file)),
            Compression::Xz => Box::new(xz2::read::XzDecoder::new(file)),
            Compression::Zstd => Box::new(zstd::stream::Decoder::new(file)?),
            Compression::None => Box::new(file),
        };

        let reader: Box<dyn Read> = if let Some(ref pb) = progress_bar {
            let pb_clone = pb.clone();
            Box::new(ProgressReader::new(reader, move |bytes| {
                pb_clone.set_position(bytes);
            }))
        } else {
            reader
        };

        let buf_reader = BufReader::with_capacity(256 * 1024, reader);

        // Parse and load statements
        self.load_statements(buf_reader, dialect, &mut stats)?;

        if let Some(pb) = progress_bar {
            pb.finish_with_message("Import complete");
        }

        stats.duration_secs = start.elapsed().as_secs_f64();
        Ok(stats)
    }

    /// Load statements from a reader
    fn load_statements<R: Read>(
        &self,
        reader: BufReader<R>,
        dialect: SqlDialect,
        stats: &mut ImportStats,
    ) -> Result<()> {
        let mut parser = StatementReader::new(reader, dialect);
        let mut pending_copy: Option<CopyHeader> = None;

        while let Some(stmt_result) = parser.next_statement() {
            let stmt = stmt_result?;

            // Handle COPY data blocks
            if let Some(ref header) = pending_copy {
                if stmt.starts_with("\\.")
                    || stmt.trim().is_empty()
                    || Self::looks_like_copy_data(&stmt)
                {
                    // This is COPY data - convert to INSERTs
                    if !stmt.starts_with("\\.") && !stmt.trim().is_empty() {
                        let inserts =
                            copy_to_inserts(header, stmt.as_bytes(), SqlDialect::Postgres);
                        for insert in inserts {
                            let insert_sql = String::from_utf8_lossy(&insert);
                            if let Err(e) = self.conn.execute(&insert_sql, []) {
                                stats.warnings.push(format!(
                                    "Failed to insert COPY data for {}: {}",
                                    header.table, e
                                ));
                            } else {
                                stats.rows_inserted += Self::count_insert_rows(&insert_sql);
                            }
                        }
                    }
                    if stmt.starts_with("\\.") {
                        pending_copy = None;
                    }
                    continue;
                } else {
                    pending_copy = None;
                }
            }

            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(stmt.as_bytes(), dialect);

            // Filter tables if specified
            if let Some(ref tables) = self.config.tables {
                if !table_name.is_empty()
                    && !tables.iter().any(|t| t.eq_ignore_ascii_case(&table_name))
                {
                    continue;
                }
            }

            match stmt_type {
                StatementType::CreateTable => {
                    let duckdb_sql = self.convert_create_table(&stmt, dialect)?;
                    match self.conn.execute(&duckdb_sql, []) {
                        Ok(_) => stats.tables_created += 1,
                        Err(e) => {
                            stats
                                .warnings
                                .push(format!("Failed to create table {}: {}", table_name, e));
                            stats.statements_skipped += 1;
                        }
                    }
                }
                StatementType::Insert => {
                    let duckdb_sql = self.convert_insert(&stmt, dialect)?;
                    match self.conn.execute(&duckdb_sql, []) {
                        Ok(_) => {
                            stats.insert_statements += 1;
                            stats.rows_inserted += Self::count_insert_rows(&duckdb_sql);
                        }
                        Err(e) => {
                            stats
                                .warnings
                                .push(format!("Failed INSERT for {}: {}", table_name, e));
                            stats.statements_skipped += 1;
                        }
                    }
                }
                StatementType::Copy => {
                    // Parse COPY header and wait for data
                    if let Some(header) = parse_copy_header(&stmt) {
                        pending_copy = Some(header);
                    }
                }
                StatementType::CreateIndex => {
                    // Skip indexes - not needed for analytics queries
                    stats.statements_skipped += 1;
                }
                _ => {
                    // Skip other statements (ALTER, DROP, etc.)
                    stats.statements_skipped += 1;
                }
            }
        }

        Ok(())
    }

    /// Check if a line looks like COPY data (tab-separated values)
    fn looks_like_copy_data(line: &str) -> bool {
        // COPY data contains tabs and doesn't start with SQL keywords
        line.contains('\t')
            && !line.to_uppercase().starts_with("SELECT")
            && !line.to_uppercase().starts_with("INSERT")
            && !line.to_uppercase().starts_with("CREATE")
            && !line.to_uppercase().starts_with("DROP")
            && !line.to_uppercase().starts_with("ALTER")
    }

    /// Convert a CREATE TABLE statement to DuckDB-compatible SQL
    fn convert_create_table(&self, stmt: &str, dialect: SqlDialect) -> Result<String> {
        let mut result = stmt.to_string();

        // Convert identifier quoting
        result = Self::convert_identifiers(&result, dialect);

        // Remove MySQL-specific clauses FIRST (before type conversion)
        // This prevents "CHARACTER SET" from being confused with "CHAR" type
        result = Self::strip_mysql_clauses(&result);

        // Convert data types in column definitions
        result = Self::convert_types_in_statement(&result);

        // Remove PostgreSQL-specific syntax
        if dialect == SqlDialect::Postgres {
            result = Self::strip_postgres_syntax(&result);
        }

        Ok(result)
    }

    /// Convert an INSERT statement to DuckDB-compatible SQL
    fn convert_insert(&self, stmt: &str, dialect: SqlDialect) -> Result<String> {
        let mut result = stmt.to_string();

        // Convert identifier quoting
        result = Self::convert_identifiers(&result, dialect);

        // Convert MySQL backslash escapes to SQL standard
        if dialect == SqlDialect::MySql {
            result = Self::convert_mysql_escapes(&result);
        }

        // Remove PostgreSQL schema prefix
        if dialect == SqlDialect::Postgres {
            result = Self::strip_schema_prefix(&result);
        }

        Ok(result)
    }

    /// Convert MySQL backslash escapes to SQL standard
    fn convert_mysql_escapes(stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len() + 100);
        let mut chars = stmt.chars().peekable();
        let mut in_string = false;

        while let Some(c) = chars.next() {
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
            } else if c == '\\' && in_string {
                // Handle MySQL escape sequences
                match chars.peek() {
                    Some('\'') => {
                        // \' -> ''
                        chars.next();
                        result.push_str("''");
                    }
                    Some('\\') => {
                        // \\ -> \
                        chars.next();
                        result.push('\\');
                    }
                    Some('n') => {
                        // \n -> newline
                        chars.next();
                        result.push('\n');
                    }
                    Some('r') => {
                        // \r -> carriage return
                        chars.next();
                        result.push('\r');
                    }
                    Some('t') => {
                        // \t -> tab
                        chars.next();
                        result.push('\t');
                    }
                    Some('0') => {
                        // \0 -> NULL character (skip)
                        chars.next();
                    }
                    Some('"') => {
                        // \" -> "
                        chars.next();
                        result.push('"');
                    }
                    _ => {
                        // Unknown escape, keep backslash
                        result.push(c);
                    }
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert identifier quoting (backticks to double quotes)
    fn convert_identifiers(stmt: &str, dialect: SqlDialect) -> String {
        match dialect {
            SqlDialect::MySql => {
                // Convert backticks to double quotes
                let mut result = String::with_capacity(stmt.len());
                let mut in_string = false;
                let mut in_backtick = false;

                for c in stmt.chars() {
                    if c == '\'' && !in_backtick {
                        in_string = !in_string;
                        result.push(c);
                    } else if c == '`' && !in_string {
                        in_backtick = !in_backtick;
                        result.push('"');
                    } else {
                        result.push(c);
                    }
                }
                result
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert SQL types in a statement
    fn convert_types_in_statement(stmt: &str) -> String {
        // Pattern to match column definitions with types
        // Handles: TYPE, TYPE(size), TYPE UNSIGNED, TYPE WITH TIME ZONE
        // IMPORTANT: Order matters - longer types first to avoid partial matches (INTEGER before INT)
        static RE_COLUMN_TYPE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)\b(BIGSERIAL|SMALLSERIAL|SERIAL|BIGINT|SMALLINT|MEDIUMINT|TINYINT|INTEGER|INT|DOUBLE\s+PRECISION|DOUBLE|FLOAT|DECIMAL|NUMERIC|CHARACTER\s+VARYING|VARCHAR|CHAR|VARBINARY|BINARY|LONGTEXT|MEDIUMTEXT|TINYTEXT|TEXT|LONGBLOB|MEDIUMBLOB|TINYBLOB|BLOB|DATETIME|TIMESTAMPTZ|TIMESTAMP|TIMETZ|TIME|DATE|YEAR|ENUM|SET|JSONB|JSON|UUID|BYTEA|BOOLEAN|BOOL|BIT|REAL|MONEY|INTERVAL)(\s*\([^)]+\))?(\s+(?:UNSIGNED|WITH(?:OUT)?\s+TIME\s+ZONE))?").unwrap()
        });

        RE_COLUMN_TYPE
            .replace_all(stmt, |caps: &regex::Captures| {
                let full_match = caps.get(0).unwrap().as_str();
                TypeConverter::convert(full_match)
            })
            .to_string()
    }

    /// Strip MySQL-specific clauses
    fn strip_mysql_clauses(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // Remove ENGINE clause
        static RE_ENGINE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*ENGINE\s*=\s*\w+").unwrap());
        result = RE_ENGINE.replace_all(&result, "").to_string();

        // Remove AUTO_INCREMENT clause at table level
        static RE_AUTO_INC: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*AUTO_INCREMENT\s*=\s*\d+").unwrap());
        result = RE_AUTO_INC.replace_all(&result, "").to_string();

        // Remove column AUTO_INCREMENT
        result = result.replace(" AUTO_INCREMENT", "");
        result = result.replace(" auto_increment", "");

        // Remove CHARACTER SET in column definitions (must come before CHARSET)
        static RE_CHAR_SET: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*CHARACTER\s+SET\s+\w+").unwrap());
        result = RE_CHAR_SET.replace_all(&result, "").to_string();

        // Remove DEFAULT CHARSET
        static RE_CHARSET: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*(DEFAULT\s+)?CHARSET\s*=\s*\w+").unwrap());
        result = RE_CHARSET.replace_all(&result, "").to_string();

        // Remove COLLATE
        static RE_COLLATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*COLLATE\s*=?\s*\w+").unwrap());
        result = RE_COLLATE.replace_all(&result, "").to_string();

        // Remove ROW_FORMAT
        static RE_ROW_FORMAT: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*ROW_FORMAT\s*=\s*\w+").unwrap());
        result = RE_ROW_FORMAT.replace_all(&result, "").to_string();

        // Remove KEY_BLOCK_SIZE
        static RE_KEY_BLOCK: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*KEY_BLOCK_SIZE\s*=\s*\d+").unwrap());
        result = RE_KEY_BLOCK.replace_all(&result, "").to_string();

        // Remove COMMENT
        static RE_COMMENT: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*COMMENT\s*=?\s*'[^']*'").unwrap());
        result = RE_COMMENT.replace_all(&result, "").to_string();

        // Remove MySQL conditional comments
        static RE_COND_COMMENT: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\*!\d+\s*|\*/").unwrap());
        result = RE_COND_COMMENT.replace_all(&result, "").to_string();

        // Remove ON UPDATE CURRENT_TIMESTAMP
        static RE_ON_UPDATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*ON\s+UPDATE\s+CURRENT_TIMESTAMP").unwrap());
        result = RE_ON_UPDATE.replace_all(&result, "").to_string();

        result
    }

    /// Strip PostgreSQL-specific syntax
    fn strip_postgres_syntax(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // Remove schema prefix
        result = Self::strip_schema_prefix(&result);

        // Remove type casts
        static RE_CAST: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"::[a-zA-Z_][a-zA-Z0-9_]*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)*").unwrap()
        });
        result = RE_CAST.replace_all(&result, "").to_string();

        // Remove nextval() - DuckDB handles sequences differently
        static RE_NEXTVAL: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*DEFAULT\s+nextval\s*\([^)]+\)").unwrap());
        result = RE_NEXTVAL.replace_all(&result, "").to_string();

        // Convert now() to CURRENT_TIMESTAMP
        static RE_NOW: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bDEFAULT\s+now\s*\(\s*\)").unwrap());
        result = RE_NOW
            .replace_all(&result, "DEFAULT CURRENT_TIMESTAMP")
            .to_string();

        // Remove INHERITS clause
        static RE_INHERITS: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*INHERITS\s*\([^)]+\)").unwrap());
        result = RE_INHERITS.replace_all(&result, "").to_string();

        // Remove WITH clause (storage parameters)
        static RE_WITH: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\s*WITH\s*\([^)]+\)").unwrap());
        result = RE_WITH.replace_all(&result, "").to_string();

        result
    }

    /// Strip schema prefix (e.g., public.users -> users)
    fn strip_schema_prefix(stmt: &str) -> String {
        static RE_SCHEMA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)\b(public|pg_catalog|pg_temp)\s*\.\s*"#).unwrap());
        RE_SCHEMA.replace_all(stmt, "").to_string()
    }

    /// Count rows in an INSERT statement
    fn count_insert_rows(sql: &str) -> u64 {
        // Count VALUES clauses by counting opening parentheses after VALUES
        if let Some(values_pos) = sql.to_uppercase().find("VALUES") {
            let after_values = &sql[values_pos + 6..];
            // Count top-level opening parens (simple heuristic)
            let mut count = 0u64;
            let mut depth = 0;
            let mut in_string = false;
            let mut prev_char = ' ';

            for c in after_values.chars() {
                if c == '\'' && prev_char != '\\' {
                    in_string = !in_string;
                }
                if !in_string {
                    if c == '(' {
                        if depth == 0 {
                            count += 1;
                        }
                        depth += 1;
                    } else if c == ')' {
                        depth -= 1;
                    }
                }
                prev_char = c;
            }
            count
        } else {
            1
        }
    }
}

/// Statement reader that handles streaming SQL parsing
struct StatementReader<R> {
    reader: BufReader<R>,
    dialect: SqlDialect,
    buffer: String,
    eof: bool,
}

impl<R: Read> StatementReader<R> {
    fn new(reader: BufReader<R>, dialect: SqlDialect) -> Self {
        Self {
            reader,
            dialect,
            buffer: String::new(),
            eof: false,
        }
    }

    fn next_statement(&mut self) -> Option<Result<String>> {
        if self.eof && self.buffer.is_empty() {
            return None;
        }

        loop {
            // Try to find a complete statement in the buffer
            if let Some(stmt) = self.extract_statement() {
                return Some(Ok(stmt));
            }

            // Read more data
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => {
                    self.eof = true;
                    if !self.buffer.trim().is_empty() {
                        let stmt = std::mem::take(&mut self.buffer);
                        return Some(Ok(stmt));
                    }
                    return None;
                }
                Ok(_) => {
                    self.buffer.push_str(&line);
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }

    fn extract_statement(&mut self) -> Option<String> {
        let mut in_string = false;
        let mut in_dollar_quote = false;
        let mut escape_next = false;
        let mut chars = self.buffer.char_indices().peekable();
        let mut end_pos = None;

        while let Some((i, c)) = chars.next() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match c {
                '\\' if self.dialect == SqlDialect::MySql && in_string => {
                    escape_next = true;
                }
                '\'' if !in_dollar_quote => {
                    in_string = !in_string;
                }
                '$' if self.dialect == SqlDialect::Postgres && !in_string => {
                    // Check for dollar quote
                    if chars.peek().map(|(_, c)| *c == '$').unwrap_or(false) {
                        in_dollar_quote = !in_dollar_quote;
                        chars.next();
                    }
                }
                ';' if !in_string && !in_dollar_quote => {
                    end_pos = Some(i + 1);
                    break;
                }
                _ => {}
            }
        }

        if let Some(pos) = end_pos {
            let stmt = self.buffer[..pos].to_string();
            self.buffer = self.buffer[pos..].trim_start().to_string();

            // Skip empty statements and comments
            let trimmed = stmt.trim();
            if trimmed.is_empty()
                || trimmed.starts_with("--")
                || (trimmed.starts_with("/*") && !trimmed.contains("/*!"))
            {
                return self.extract_statement();
            }

            Some(stmt)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_insert_rows() {
        assert_eq!(
            DumpLoader::count_insert_rows("INSERT INTO t VALUES (1, 'a')"),
            1
        );
        assert_eq!(
            DumpLoader::count_insert_rows("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"),
            3
        );
        assert_eq!(
            DumpLoader::count_insert_rows("INSERT INTO t VALUES (1, '(test)')"),
            1
        );
    }

    #[test]
    fn test_strip_mysql_clauses() {
        let sql = "CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        let result = DumpLoader::strip_mysql_clauses(sql);
        assert!(!result.contains("ENGINE"));
        assert!(!result.contains("CHARSET"));
    }

    #[test]
    fn test_convert_identifiers() {
        let sql = "INSERT INTO `users` (`id`, `name`) VALUES (1, 'test')";
        let result = DumpLoader::convert_identifiers(sql, SqlDialect::MySql);
        assert_eq!(
            result,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'test')"
        );
    }

    #[test]
    fn test_looks_like_copy_data() {
        assert!(DumpLoader::looks_like_copy_data("1\tAlice\t2024-01-01"));
        assert!(!DumpLoader::looks_like_copy_data("SELECT * FROM users"));
        assert!(!DumpLoader::looks_like_copy_data("INSERT INTO t VALUES"));
    }
}
