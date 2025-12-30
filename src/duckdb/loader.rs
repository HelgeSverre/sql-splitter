//! SQL dump loader for importing dumps into DuckDB.

use super::batch::{flush_batch, BatchManager, MAX_ROWS_PER_BATCH};
use super::types::TypeConverter;
use super::{ImportStats, QueryConfig};
use crate::convert::copy_to_insert::{copy_to_inserts, parse_copy_header, CopyHeader};
use crate::parser::{
    detect_dialect_from_file, parse_insert_for_bulk, Parser, SqlDialect, StatementType,
};
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

/// Maximum COPY rows to accumulate per batch before converting to INSERTs.
/// This bounds memory usage while still enabling large batches for performance.
const MAX_COPY_ROWS_PER_BATCH: usize = 10_000;

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

        // Batched COPY data accumulator
        let mut copy_batch_data: Vec<u8> = Vec::new();
        let mut copy_batch_rows: usize = 0;

        // Track tables that failed (don't exist) to skip subsequent inserts
        let mut failed_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Batch manager for bulk INSERT loading via Appender API
        let mut batch_mgr = BatchManager::new(MAX_ROWS_PER_BATCH);

        while let Some(stmt_result) = parser.next_statement() {
            let stmt = stmt_result?;

            // Handle COPY data blocks with batching
            if let Some(ref header) = pending_copy {
                let trimmed = stmt.trim();

                // End-of-COPY marker: "\."
                if trimmed == "\\." {
                    // Flush any remaining batched rows
                    if !copy_batch_data.is_empty() {
                        self.process_copy_batch(
                            header,
                            &copy_batch_data,
                            stats,
                            &mut failed_tables,
                        );
                        copy_batch_data.clear();
                        copy_batch_rows = 0;
                    }
                    pending_copy = None;
                    parser.set_copy_mode(false);
                    continue;
                }

                // Empty/whitespace line inside COPY: skip
                if trimmed.is_empty() {
                    continue;
                }

                // Normal COPY data line - accumulate into batch
                if Self::looks_like_copy_data(&stmt) {
                    // Skip if we know this table doesn't exist
                    if failed_tables.contains(&header.table) {
                        continue;
                    }

                    copy_batch_data.extend_from_slice(stmt.as_bytes());
                    copy_batch_data.push(b'\n');
                    copy_batch_rows += 1;

                    // Flush when batch gets large enough
                    if copy_batch_rows >= MAX_COPY_ROWS_PER_BATCH {
                        self.process_copy_batch(
                            header,
                            &copy_batch_data,
                            stats,
                            &mut failed_tables,
                        );
                        copy_batch_data.clear();
                        copy_batch_rows = 0;
                    }
                    continue;
                }

                // Unexpected content inside COPY - flush and fall through to normal handling
                if !copy_batch_data.is_empty() {
                    self.process_copy_batch(header, &copy_batch_data, stats, &mut failed_tables);
                    copy_batch_data.clear();
                    copy_batch_rows = 0;
                }
                pending_copy = None;
                parser.set_copy_mode(false);
                // Fall through to normal statement handling
            }

            let (mut stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(stmt.as_bytes(), dialect);

            // For Postgres, check if statement contains COPY ... FROM stdin (may be after comments)
            if dialect == SqlDialect::Postgres && stmt_type == StatementType::Unknown {
                let upper = stmt.to_uppercase();
                if let Some(copy_pos) = upper.find("COPY ") {
                    let after_copy = &upper[copy_pos..];
                    if after_copy.contains("FROM STDIN") {
                        stmt_type = StatementType::Copy;
                    }
                }
            }

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
                    // Try bulk loading via Appender API first
                    if !self.try_queue_for_bulk(
                        &stmt,
                        dialect,
                        &mut batch_mgr,
                        stats,
                        &mut failed_tables,
                    ) {
                        // Fallback to direct execution
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

                    // Flush any ready batches
                    for mut batch in batch_mgr.get_ready_batches() {
                        flush_batch(self.conn, &mut batch, stats, &mut failed_tables)?;
                    }
                }
                StatementType::Copy => {
                    // Parse COPY header and start buffering data
                    if let Some(header) = parse_copy_header(&stmt) {
                        // Check if table already known to be missing
                        if failed_tables.contains(&header.table) {
                            // Enter COPY mode to skip line-by-line, then exit
                            parser.set_copy_mode(true);
                            Self::skip_copy_block(&mut parser);
                            parser.set_copy_mode(false);
                            continue;
                        }

                        // Proactively check if table exists before buffering data
                        if !self.table_exists(&header.table) {
                            failed_tables.insert(header.table.clone());
                            if stats.warnings.len() < 100 {
                                stats.warnings.push(format!(
                                    "Skipping COPY for non-existent table {}",
                                    header.table
                                ));
                            }
                            // Enter COPY mode to skip line-by-line, then exit
                            parser.set_copy_mode(true);
                            Self::skip_copy_block(&mut parser);
                            parser.set_copy_mode(false);
                            continue;
                        }

                        copy_batch_data.clear();
                        copy_batch_rows = 0;
                        pending_copy = Some(header);
                        parser.set_copy_mode(true);
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

        // Flush any remaining COPY batch (handles truncated dumps)
        if let Some(ref header) = pending_copy {
            if !copy_batch_data.is_empty() {
                self.process_copy_batch(header, &copy_batch_data, stats, &mut failed_tables);
            }
        }

        // Flush any remaining INSERT batches
        for mut batch in batch_mgr.drain_all() {
            flush_batch(self.conn, &mut batch, stats, &mut failed_tables)?;
        }

        Ok(())
    }

    /// Process a batch of COPY data rows, converting them to INSERTs
    fn process_copy_batch(
        &self,
        header: &CopyHeader,
        batch_data: &[u8],
        stats: &mut ImportStats,
        failed_tables: &mut std::collections::HashSet<String>,
    ) {
        if batch_data.is_empty() {
            return;
        }

        // Skip if we already know this table doesn't exist
        if failed_tables.contains(&header.table) {
            return;
        }

        let inserts = copy_to_inserts(header, batch_data, SqlDialect::Postgres);
        for insert in inserts {
            let insert_sql = String::from_utf8_lossy(&insert);
            match self.conn.execute(&insert_sql, []) {
                Ok(_) => {
                    stats.rows_inserted += Self::count_insert_rows(&insert_sql);
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // If table doesn't exist, mark it and skip future inserts
                    if err_str.contains("does not exist") {
                        failed_tables.insert(header.table.clone());
                        if stats.warnings.len() < 100 {
                            stats.warnings.push(format!(
                                "Table {} does not exist, skipping COPY data",
                                header.table
                            ));
                        }
                        return; // Skip rest of batch
                    }
                    // Limit warnings to avoid memory bloat on large failures
                    if stats.warnings.len() < 100 {
                        stats.warnings.push(format!(
                            "Failed to insert COPY data for {}: {}",
                            header.table, e
                        ));
                    }
                    stats.statements_skipped += 1;
                }
            }
        }
    }

    /// Check if a table exists in DuckDB
    fn table_exists(&self, table: &str) -> bool {
        let query = "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1";
        match self.conn.prepare(query) {
            Ok(mut stmt) => stmt.exists([table]).unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Try to queue an INSERT statement for bulk loading via Appender API.
    /// Returns true if successfully queued, false if fallback to direct execution is needed.
    fn try_queue_for_bulk(
        &self,
        stmt: &str,
        dialect: SqlDialect,
        batch_mgr: &mut BatchManager,
        stats: &mut ImportStats,
        failed_tables: &mut std::collections::HashSet<String>,
    ) -> bool {
        // Quick filter: skip statements with complex clauses that Appender can't handle
        let upper = stmt.to_uppercase();
        if upper.contains("ON DUPLICATE KEY")
            || upper.contains("ON CONFLICT")
            || upper.contains("REPLACE")
            || upper.contains("IGNORE")
            || upper.contains("RETURNING")
            || upper.contains("SELECT")
        {
            return false;
        }

        // Try to parse the INSERT statement
        let parsed = match parse_insert_for_bulk(stmt.as_bytes()) {
            Ok(p) => p,
            Err(_) => return false, // Parse failed, use fallback
        };

        // Skip tables we know don't exist
        if failed_tables.contains(&parsed.table) {
            return true; // Pretend we handled it to skip the statement
        }

        // Skip if no rows were parsed
        if parsed.rows.is_empty() {
            return false;
        }

        // Convert the statement for DuckDB (for fallback)
        let duckdb_sql = match self.convert_insert(stmt, dialect) {
            Ok(sql) => sql,
            Err(_) => return false,
        };

        // Queue the rows
        if let Some(mut batch) =
            batch_mgr.queue_insert(&parsed.table, parsed.columns, parsed.rows, duckdb_sql)
        {
            // Batch is ready to flush
            if let Err(e) = flush_batch(self.conn, &mut batch, stats, failed_tables) {
                if stats.warnings.len() < 100 {
                    stats.warnings.push(format!("Batch flush error: {}", e));
                }
            }
        }

        true
    }

    /// Skip a COPY data block without parsing/processing it
    fn skip_copy_block<R: Read>(parser: &mut StatementReader<R>) {
        while let Some(Ok(line)) = parser.next_statement() {
            if line.trim() == "\\." {
                break;
            }
        }
    }

    /// Check if a line looks like COPY data (tab-separated values or single-column values)
    fn looks_like_copy_data(line: &str) -> bool {
        let trimmed = line.trim();

        // Empty line is not COPY data
        if trimmed.is_empty() {
            return false;
        }

        // End-of-COPY marker
        if trimmed == "\\." {
            return false;
        }

        // Lines with tabs are definitely COPY data (multi-column)
        // For single-column data, we need to check it doesn't look like SQL
        let first_char = trimmed.chars().next().unwrap_or(' ');

        // Quick check: if starts with common SQL keyword first char, verify it's not SQL
        if matches!(
            first_char,
            'S' | 's'
                | 'I'
                | 'i'
                | 'C'
                | 'c'
                | 'D'
                | 'd'
                | 'A'
                | 'a'
                | 'U'
                | 'u'
                | 'G'
                | 'g'
                | '-'
                | '/'
        ) {
            let upper_prefix: String = trimmed.chars().take(7).collect::<String>().to_uppercase();
            if upper_prefix.starts_with("SELECT")
                || upper_prefix.starts_with("INSERT")
                || upper_prefix.starts_with("CREATE")
                || upper_prefix.starts_with("DROP")
                || upper_prefix.starts_with("ALTER")
                || upper_prefix.starts_with("UPDATE")
                || upper_prefix.starts_with("GRANT")
                || upper_prefix.starts_with("--")
                || upper_prefix.starts_with("/*")
            {
                return false;
            }
        }

        // If it contains a tab, it's multi-column COPY data
        // If it doesn't contain a tab but passed the SQL check, it's single-column data
        true
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

        // Remove MSSQL-specific syntax
        if dialect == SqlDialect::Mssql {
            result = Self::strip_mssql_syntax(&result);
        }

        // Remove SQLite-specific syntax
        if dialect == SqlDialect::Sqlite {
            result = Self::strip_sqlite_syntax(&result);
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

        // Remove MSSQL schema prefix (dbo., etc.)
        if dialect == SqlDialect::Mssql {
            result = Self::strip_mssql_schema_prefix(&result);
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

    /// Convert identifier quoting (backticks/brackets to double quotes)
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
            SqlDialect::Mssql => {
                // Convert brackets to double quotes, strip N prefix from strings
                let mut result = String::with_capacity(stmt.len());
                let mut in_string = false;
                let mut in_bracket = false;
                let mut chars = stmt.chars().peekable();

                while let Some(c) = chars.next() {
                    if c == '\'' && !in_bracket {
                        in_string = !in_string;
                        result.push(c);
                    } else if c == '[' && !in_string {
                        in_bracket = true;
                        result.push('"');
                    } else if c == ']' && !in_string {
                        // Handle ]] escape
                        if chars.peek() == Some(&']') {
                            chars.next();
                            result.push(']');
                        } else {
                            in_bracket = false;
                            result.push('"');
                        }
                    } else if c == 'N' && !in_string && !in_bracket && chars.peek() == Some(&'\'') {
                        // Strip N prefix from N'string'
                        // Don't push the N, the quote will be pushed in next iteration
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
        // IMPORTANT: Types must be preceded by quote/whitespace AND followed by whitespace/paren/comma (not a closing quote)
        // This prevents matching "date" as DATE type (column names inside quotes)
        // Includes MySQL, PostgreSQL, SQLite, and MSSQL types
        static RE_COLUMN_TYPE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i)(["'`\]\s])\s*(BIGSERIAL|SMALLSERIAL|SERIAL|BIGINT|SMALLINT|MEDIUMINT|TINYINT|INTEGER|INT|DOUBLE\s+PRECISION|DOUBLE|FLOAT|DECIMAL|NUMERIC|CHARACTER\s+VARYING|NVARCHAR|NCHAR|VARCHAR|CHAR|VARBINARY|BINARY|LONGTEXT|MEDIUMTEXT|TINYTEXT|NTEXT|TEXT|LONGBLOB|MEDIUMBLOB|TINYBLOB|IMAGE|BLOB|DATETIME2|DATETIMEOFFSET|SMALLDATETIME|DATETIME|TIMESTAMPTZ|TIMESTAMP|TIMETZ|TIME|DATE|YEAR|ENUM|SET|JSONB|JSON|UUID|UNIQUEIDENTIFIER|BYTEA|BOOLEAN|BOOL|BIT|REAL|MONEY|SMALLMONEY|INTERVAL|ROWVERSION|XML|SQL_VARIANT)\b(\s*\([^)]+\))?(\s+(?:UNSIGNED|WITH(?:OUT)?\s+TIME\s+ZONE))?"#).unwrap()
        });

        RE_COLUMN_TYPE
            .replace_all(stmt, |caps: &regex::Captures| {
                let full_match = caps.get(0).unwrap().as_str();
                let leading_char = caps.get(1).unwrap().as_str();
                let type_part = caps.get(2).unwrap().as_str();
                let size_part = caps.get(3).map(|m| m.as_str()).unwrap_or("");
                let suffix = caps.get(4).map(|m| m.as_str()).unwrap_or("");

                // Check if this looks like a quoted identifier (type is inside quotes)
                // If leading char is a quote and the character before the match is also a quote, skip
                let end_pos = caps.get(0).unwrap().end();
                let stmt_bytes = stmt.as_bytes();
                if end_pos < stmt_bytes.len() {
                    let next_char = stmt_bytes[end_pos] as char;
                    // If next character is a closing quote, this is a quoted identifier, not a type
                    if next_char == '"' || next_char == '\'' || next_char == '`' {
                        return full_match.to_string();
                    }
                }

                // Calculate the whitespace between leading char and type
                let ws_len = full_match.len()
                    - leading_char.len()
                    - type_part.len()
                    - size_part.len()
                    - suffix.len();
                let ws = &full_match[leading_char.len()..leading_char.len() + ws_len];

                let converted =
                    TypeConverter::convert(&format!("{}{}{}", type_part, size_part, suffix));
                format!("{}{}{}", leading_char, ws, converted)
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

        // Remove UNIQUE KEY constraint lines: UNIQUE KEY `name` (`col1`, `col2`)
        // Must handle both: ,UNIQUE KEY... at end of column list and UNIQUE KEY... on its own line
        static RE_UNIQUE_KEY: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i),?\s*UNIQUE\s+KEY\s+[`"']?\w+[`"']?\s*\([^)]+\)"#).unwrap()
        });
        result = RE_UNIQUE_KEY.replace_all(&result, "").to_string();

        // Remove KEY (index) constraint lines: KEY `name` (`col1`, `col2`)
        // This handles regular indexes and FULLTEXT indexes, but NOT PRIMARY KEY or FOREIGN KEY
        // We use a negative lookbehind pattern by only matching KEY that is preceded by comma or newline (not FOREIGN/PRIMARY)
        static RE_KEY_INDEX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(
                r#"(?i)(?:,\s*|\n\s*)(?:FULLTEXT\s+|SPATIAL\s+)?KEY\s+[`"']?\w+[`"']?\s*\([^)]+\)"#,
            )
            .unwrap()
        });
        result = RE_KEY_INDEX.replace_all(&result, "").to_string();

        // Remove GENERATED ALWAYS AS columns entirely
        // Match: `col` TYPE GENERATED ALWAYS AS (expr) STORED/VIRTUAL
        // The expression can contain nested parentheses so we match one level deep
        static RE_GENERATED_COL: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i),?\s*[`"']?\w+[`"']?\s+\w+\s+GENERATED\s+ALWAYS\s+AS\s*\((?:[^()]+|\([^()]*\))+\)\s*(?:STORED|VIRTUAL)?"#).unwrap()
        });
        result = RE_GENERATED_COL.replace_all(&result, "").to_string();

        // Remove entire FOREIGN KEY constraints (DuckDB enforces them which causes issues with batch loading)
        // Match: CONSTRAINT `name` FOREIGN KEY (...) REFERENCES ... [ON DELETE/UPDATE ...]
        // or just: FOREIGN KEY (...) REFERENCES ... [ON DELETE/UPDATE ...]
        static RE_FK_CONSTRAINT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i),?\s*(?:CONSTRAINT\s+[`"']?\w+[`"']?\s+)?FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+[`"']?\w+[`"']?\s*\([^)]+\)(?:\s+ON\s+(?:DELETE|UPDATE)\s+(?:CASCADE|SET\s+NULL|SET\s+DEFAULT|NO\s+ACTION|RESTRICT))*"#).unwrap()
        });
        result = RE_FK_CONSTRAINT.replace_all(&result, "").to_string();

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

    /// Strip MSSQL schema prefix (dbo., etc.) for both CREATE TABLE and INSERT
    fn strip_mssql_schema_prefix(stmt: &str) -> String {
        // Remove schema prefix (dbo., schema.) - quoted or unquoted
        static RE_SCHEMA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)"?(dbo|master|tempdb|model|msdb)"?\s*\.\s*"#).unwrap());
        RE_SCHEMA.replace_all(stmt, "").to_string()
    }

    /// Strip MSSQL-specific syntax
    fn strip_mssql_syntax(stmt: &str) -> String {
        let mut result = Self::strip_mssql_schema_prefix(stmt);

        // Remove IDENTITY clause and make the column nullable (so INSERTs without id work)
        // Pattern matches: INT IDENTITY(1,1) NOT NULL -> INT
        static RE_IDENTITY_NOT_NULL: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)\s*IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)\s*NOT\s+NULL").unwrap()
        });
        result = RE_IDENTITY_NOT_NULL.replace_all(&result, "").to_string();

        // Also handle IDENTITY without NOT NULL
        static RE_IDENTITY: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*IDENTITY\s*\(\s*\d+\s*,\s*\d+\s*\)").unwrap());
        result = RE_IDENTITY.replace_all(&result, "").to_string();

        // Remove CLUSTERED/NONCLUSTERED
        static RE_CLUSTERED: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*(?:NON)?CLUSTERED\s*").unwrap());
        result = RE_CLUSTERED.replace_all(&result, " ").to_string();

        // Remove ON [PRIMARY] (filegroup)
        static RE_FILEGROUP: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)\s*ON\s*"?PRIMARY"?"#).unwrap());
        result = RE_FILEGROUP.replace_all(&result, "").to_string();

        // Remove PRIMARY KEY constraints (they make columns NOT NULL which breaks IDENTITY column INSERTs)
        static RE_PK_CONSTRAINT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i),?\s*CONSTRAINT\s+"?\w+"?\s+PRIMARY\s+KEY\s+\([^)]+\)"#).unwrap()
        });
        result = RE_PK_CONSTRAINT.replace_all(&result, "").to_string();

        // Remove FOREIGN KEY constraints (analytics queries don't need FK enforcement)
        static RE_FK_CONSTRAINT: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r#"(?i),?\s*CONSTRAINT\s+"?\w+"?\s+FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+[^\s(]+\s*\([^)]+\)"#).unwrap()
        });
        result = RE_FK_CONSTRAINT.replace_all(&result, "").to_string();

        // Remove WITH clause for indexes
        static RE_WITH: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\s*WITH\s*\([^)]+\)").unwrap());
        result = RE_WITH.replace_all(&result, "").to_string();

        // Remove TEXTIMAGE_ON
        static RE_TEXTIMAGE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)\s*TEXTIMAGE_ON\s*"?\w+"?"#).unwrap());
        result = RE_TEXTIMAGE.replace_all(&result, "").to_string();

        // Convert GETDATE() to CURRENT_TIMESTAMP
        static RE_GETDATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bGETDATE\s*\(\s*\)").unwrap());
        result = RE_GETDATE
            .replace_all(&result, "CURRENT_TIMESTAMP")
            .to_string();

        // Convert NEWID() to gen_random_uuid()
        static RE_NEWID: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNEWID\s*\(\s*\)").unwrap());
        result = RE_NEWID
            .replace_all(&result, "gen_random_uuid()")
            .to_string();

        result
    }

    /// Strip SQLite-specific syntax not supported by DuckDB
    fn strip_sqlite_syntax(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // Remove AUTOINCREMENT (DuckDB handles auto-increment via sequences)
        // SQLite uses "INTEGER PRIMARY KEY AUTOINCREMENT"
        result = result.replace(" AUTOINCREMENT", "");
        result = result.replace(" autoincrement", "");

        // Remove IF NOT EXISTS (DuckDB supports this but we want clean imports)
        // Actually DuckDB does support IF NOT EXISTS, so leave it

        // Remove STRICT table modifier (SQLite 3.37+)
        static RE_STRICT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\)\s*STRICT\s*;").unwrap());
        result = RE_STRICT.replace_all(&result, ");").to_string();

        // Remove WITHOUT ROWID (SQLite optimization not needed for analytics)
        static RE_WITHOUT_ROWID: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\)\s*WITHOUT\s+ROWID\s*;").unwrap());
        result = RE_WITHOUT_ROWID.replace_all(&result, ");").to_string();

        result
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
    /// Position in buffer where unprocessed data starts (avoids O(n) shifts)
    buffer_pos: usize,
    eof: bool,
    /// Track if we're inside a PostgreSQL COPY data block
    in_copy_mode: bool,
}

impl<R: Read> StatementReader<R> {
    fn new(reader: BufReader<R>, dialect: SqlDialect) -> Self {
        Self {
            reader,
            dialect,
            buffer: String::new(),
            buffer_pos: 0,
            eof: false,
            in_copy_mode: false,
        }
    }

    /// Compact the buffer by removing already-processed data
    /// Only called periodically to avoid O(n²) behavior
    fn compact_buffer(&mut self) {
        if self.buffer_pos > 0 {
            self.buffer.drain(..self.buffer_pos);
            self.buffer_pos = 0;
        }
    }

    /// Get the unprocessed portion of the buffer
    fn remaining_buffer(&self) -> &str {
        &self.buffer[self.buffer_pos..]
    }

    /// Set COPY mode explicitly (called by DumpLoader when entering/exiting COPY blocks)
    fn set_copy_mode(&mut self, enabled: bool) {
        self.in_copy_mode = enabled;
    }

    /// Strip leading SQL comments (-- and /* */) from a string
    fn strip_leading_sql_comments(s: &str) -> &str {
        let mut result = s.trim();
        loop {
            // Skip -- line comments
            if result.starts_with("--") {
                if let Some(pos) = result.find('\n') {
                    result = result[pos + 1..].trim();
                    continue;
                } else {
                    return ""; // Only comment, no newline
                }
            }
            // Skip /* */ block comments
            if result.starts_with("/*") {
                if let Some(pos) = result.find("*/") {
                    result = result[pos + 2..].trim();
                    continue;
                } else {
                    return ""; // Unclosed block comment
                }
            }
            break;
        }
        result
    }

    fn next_statement(&mut self) -> Option<Result<String>> {
        if self.eof && self.remaining_buffer().is_empty() {
            return None;
        }

        loop {
            // In COPY mode, return each line individually until we see \.
            if self.in_copy_mode {
                if let Some(line) = self.extract_copy_line() {
                    return Some(Ok(line));
                }
            } else {
                // Try to find a complete statement in the buffer
                if let Some(stmt) = self.extract_statement() {
                    // COPY mode is now managed explicitly by DumpLoader via set_copy_mode()
                    return Some(Ok(stmt));
                }
            }

            // Compact buffer periodically to prevent unbounded growth
            // Only compact when processed portion is significant
            if self.buffer_pos > 64 * 1024 {
                self.compact_buffer();
            }

            // Read more data
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => {
                    self.eof = true;
                    self.in_copy_mode = false; // Reset on EOF
                    let remaining = self.remaining_buffer().trim();
                    if !remaining.is_empty() {
                        let stmt = remaining.to_string();
                        self.buffer.clear();
                        self.buffer_pos = 0;
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

    /// Extract a single line from the buffer for COPY data mode
    fn extract_copy_line(&mut self) -> Option<String> {
        let remaining = self.remaining_buffer();
        if let Some(newline_pos) = remaining.find('\n') {
            let line = remaining[..newline_pos].to_string();
            self.buffer_pos += newline_pos + 1;
            // COPY mode is managed by DumpLoader via set_copy_mode(), not here
            Some(line)
        } else {
            None
        }
    }

    fn extract_statement(&mut self) -> Option<String> {
        let remaining = self.remaining_buffer();
        let mut in_string = false;
        let mut in_dollar_quote = false;
        let mut in_bracket = false;
        let mut in_line_comment = false;
        let mut in_block_comment = false;
        let mut escape_next = false;
        let mut chars = remaining.char_indices().peekable();
        let mut end_pos = None;

        // For MSSQL, check for GO at start of line
        if self.dialect == SqlDialect::Mssql {
            if let Some(go_pos) = self.find_go_separator() {
                let stmt = remaining[..go_pos].to_string();
                // Skip past GO and any whitespace
                let after_go = &remaining[go_pos..];
                if let Some(line_end) = after_go.find('\n') {
                    self.buffer_pos += go_pos + line_end + 1;
                } else {
                    self.buffer_pos = self.buffer.len();
                }

                let trimmed = stmt.trim();
                if trimmed.is_empty()
                    || trimmed.starts_with("--")
                    || (trimmed.starts_with("/*") && !trimmed.contains("/*!"))
                {
                    return self.extract_statement();
                }
                return Some(stmt);
            }
        }

        while let Some((i, c)) = chars.next() {
            if escape_next {
                escape_next = false;
                continue;
            }

            // Handle line comments (-- to end of line)
            if in_line_comment {
                if c == '\n' {
                    in_line_comment = false;
                }
                continue;
            }

            // Handle block comments (/* to */)
            if in_block_comment {
                if c == '*' && chars.peek().map(|(_, c)| *c == '/').unwrap_or(false) {
                    chars.next();
                    in_block_comment = false;
                }
                continue;
            }

            match c {
                '\\' if self.dialect == SqlDialect::MySql && in_string => {
                    escape_next = true;
                }
                '\'' if !in_dollar_quote && !in_bracket => {
                    in_string = !in_string;
                }
                '[' if self.dialect == SqlDialect::Mssql && !in_string => {
                    in_bracket = true;
                }
                ']' if self.dialect == SqlDialect::Mssql && !in_string => {
                    // Handle ]] escape
                    if chars.peek().map(|(_, c)| *c == ']').unwrap_or(false) {
                        chars.next();
                    } else {
                        in_bracket = false;
                    }
                }
                '$' if self.dialect == SqlDialect::Postgres && !in_string => {
                    // Check for dollar quote
                    if chars.peek().map(|(_, c)| *c == '$').unwrap_or(false) {
                        in_dollar_quote = !in_dollar_quote;
                        chars.next();
                    }
                }
                '-' if !in_string && !in_dollar_quote && !in_bracket => {
                    // Check for -- line comment
                    if chars.peek().map(|(_, c)| *c == '-').unwrap_or(false) {
                        chars.next();
                        in_line_comment = true;
                    }
                }
                '/' if !in_string && !in_dollar_quote && !in_bracket => {
                    // Check for /* block comment
                    if chars.peek().map(|(_, c)| *c == '*').unwrap_or(false) {
                        chars.next();
                        in_block_comment = true;
                    }
                }
                ';' if !in_string && !in_dollar_quote && !in_bracket => {
                    end_pos = Some(i + 1);
                    break;
                }
                _ => {}
            }
        }

        if let Some(pos) = end_pos {
            let stmt = remaining[..pos].to_string();
            // Skip past the statement and any leading whitespace
            let after_stmt = &remaining[pos..];
            let trimmed_len = after_stmt.len() - after_stmt.trim_start().len();
            self.buffer_pos += pos + trimmed_len;

            // Skip empty statements and comments
            let trimmed = stmt.trim();

            // Strip leading comments from the statement before checking if it's just a comment
            let stripped = Self::strip_leading_sql_comments(trimmed);
            if stripped.is_empty() {
                return self.extract_statement();
            }

            // Use the stripped version for further processing
            let trimmed = stripped;

            // For Postgres COPY statements, auto-enter copy mode
            // This prevents accumulating COPY data while looking for the next semicolon
            if self.dialect == SqlDialect::Postgres {
                let upper = trimmed.to_uppercase();
                if upper.ends_with("FROM STDIN;") && upper.contains("COPY ") {
                    self.in_copy_mode = true;
                }
            }

            Some(stmt)
        } else {
            None
        }
    }

    /// Find GO batch separator at start of line (MSSQL)
    fn find_go_separator(&self) -> Option<usize> {
        let remaining = self.remaining_buffer();
        let mut in_string = false;
        let mut in_bracket = false;
        let mut line_start = 0;

        for (i, c) in remaining.char_indices() {
            if c == '\'' && !in_bracket {
                in_string = !in_string;
            } else if c == '[' && !in_string {
                in_bracket = true;
            } else if c == ']' && !in_string {
                in_bracket = false;
            } else if c == '\n' {
                line_start = i + 1;
            } else if !in_string && !in_bracket && i == line_start {
                // Check for GO at start of line
                let rest = &remaining[i..];
                if rest.len() >= 2 {
                    let word = &rest[..2.min(rest.len())];
                    if word.eq_ignore_ascii_case("GO") {
                        // Make sure it's just GO (not GO_SOMETHING)
                        let after_go = if rest.len() > 2 {
                            rest.chars().nth(2)
                        } else {
                            None
                        };
                        if after_go.is_none()
                            || after_go == Some('\n')
                            || after_go == Some('\r')
                            || after_go == Some(' ')
                            || after_go.unwrap().is_ascii_digit()
                        {
                            return Some(i);
                        }
                    }
                }
            }
        }
        None
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

    #[test]
    fn test_strip_mssql_syntax() {
        let sql = r#"CREATE TABLE "users" (
    "id" INTEGER NOT NULL,
    "email" VARCHAR(255) NOT NULL
)"#;
        // The IDENTITY should have been stripped by strip_mssql_syntax
        let result = DumpLoader::strip_mssql_syntax(sql);
        assert!(!result.contains("IDENTITY"), "IDENTITY should be stripped");

        // Test with IDENTITY(1,1) NOT NULL
        let sql_with_identity = r#"CREATE TABLE "users" (
    "id" INTEGER IDENTITY(1,1) NOT NULL,
    "email" VARCHAR(255) NOT NULL
)"#;
        let result2 = DumpLoader::strip_mssql_syntax(sql_with_identity);
        assert!(
            !result2.contains("IDENTITY"),
            "IDENTITY should be stripped: {}",
            result2
        );
        // Since we strip IDENTITY(1,1) NOT NULL, the column should become nullable
        assert!(
            !result2.contains("IDENTITY(1,1) NOT NULL"),
            "Should strip full IDENTITY NOT NULL"
        );
    }

    #[test]
    fn test_convert_mssql_identifiers() {
        let sql = "INSERT INTO [dbo].[users] ([id], [name]) VALUES (1, N'test')";
        let result = DumpLoader::convert_identifiers(sql, SqlDialect::Mssql);
        assert_eq!(
            result,
            "INSERT INTO \"dbo\".\"users\" (\"id\", \"name\") VALUES (1, 'test')"
        );
    }
}
