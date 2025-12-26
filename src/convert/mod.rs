//! Convert command for translating SQL dumps between dialects.
//!
//! Supports conversion between MySQL, PostgreSQL, and SQLite dialects with:
//! - Identifier quoting conversion (backticks ↔ double quotes)
//! - String escape normalization (\' ↔ '')
//! - Data type mapping (AUTO_INCREMENT ↔ SERIAL ↔ INTEGER PRIMARY KEY)
//! - COPY FROM stdin → INSERT conversion
//! - Session header conversion
//! - Warning system for unsupported features

pub mod copy_to_insert;
mod types;
mod warnings;

#[allow(unused_imports)]
pub use copy_to_insert::{
    copy_to_inserts, parse_copy_data, parse_copy_header, CopyHeader, CopyValue,
};

use crate::parser::{Parser, SqlDialect, StatementType};
use crate::progress::ProgressReader;
use crate::splitter::Compression;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

pub use types::TypeMapper;
pub use warnings::{ConvertWarning, WarningCollector};

/// Configuration for the convert command
#[derive(Debug)]
pub struct ConvertConfig {
    /// Input SQL file
    pub input: PathBuf,
    /// Output SQL file (None for stdout)
    pub output: Option<PathBuf>,
    /// Source dialect (auto-detected if None)
    pub from_dialect: Option<SqlDialect>,
    /// Target dialect
    pub to_dialect: SqlDialect,
    /// Dry run mode
    pub dry_run: bool,
    /// Show progress
    pub progress: bool,
    /// Strict mode (fail on any unsupported feature)
    pub strict: bool,
}

impl Default for ConvertConfig {
    fn default() -> Self {
        Self {
            input: PathBuf::new(),
            output: None,
            from_dialect: None,
            to_dialect: SqlDialect::Postgres,
            dry_run: false,
            progress: false,
            strict: false,
        }
    }
}

/// Statistics from convert operation
#[derive(Debug, Default, serde::Serialize)]
pub struct ConvertStats {
    /// Total statements processed
    pub statements_processed: u64,
    /// Statements converted
    pub statements_converted: u64,
    /// Statements passed through unchanged
    pub statements_unchanged: u64,
    /// Statements skipped (unsupported)
    pub statements_skipped: u64,
    /// Warnings generated
    pub warnings: Vec<ConvertWarning>,
}

/// Main converter that dispatches to specific dialect converters
pub struct Converter {
    from: SqlDialect,
    to: SqlDialect,
    warnings: WarningCollector,
    strict: bool,
    /// Pending COPY header for data block processing
    pending_copy_header: Option<CopyHeader>,
}

impl Converter {
    pub fn new(from: SqlDialect, to: SqlDialect) -> Self {
        Self {
            from,
            to,
            warnings: WarningCollector::new(),
            strict: false,
            pending_copy_header: None,
        }
    }

    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Check if we have a pending COPY header (waiting for data block)
    pub fn has_pending_copy(&self) -> bool {
        self.pending_copy_header.is_some()
    }

    /// Process a COPY data block using the pending header
    pub fn process_copy_data(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, ConvertWarning> {
        if let Some(header) = self.pending_copy_header.take() {
            if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
                // Convert COPY data to INSERT statements
                let inserts = copy_to_inserts(&header, data, self.to);
                return Ok(inserts);
            }
        }
        // Pass through if same dialect or no pending header
        Ok(vec![data.to_vec()])
    }

    /// Convert a single statement
    pub fn convert_statement(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(stmt, self.from);

        let table = if table_name.is_empty() {
            None
        } else {
            Some(table_name.as_str())
        };

        match stmt_type {
            StatementType::CreateTable => self.convert_create_table(stmt, table),
            StatementType::Insert => self.convert_insert(stmt, table),
            StatementType::CreateIndex => self.convert_create_index(stmt),
            StatementType::AlterTable => self.convert_alter_table(stmt),
            StatementType::DropTable => self.convert_drop_table(stmt),
            StatementType::Copy => self.convert_copy(stmt, table),
            StatementType::Unknown => self.convert_other(stmt),
        }
    }

    /// Convert CREATE TABLE statement
    fn convert_create_table(
        &mut self,
        stmt: &[u8],
        table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Detect unsupported features BEFORE conversion (so we see original types)
        self.detect_unsupported_features(&result, table_name)?;

        // Convert identifier quoting
        result = self.convert_identifiers(&result);

        // Convert data types
        result = self.convert_data_types(&result);

        // Convert AUTO_INCREMENT
        result = self.convert_auto_increment(&result, table_name);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.convert_nextval(&result);
            result = self.convert_default_now(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Convert string escapes
        result = self.convert_string_escapes(&result);

        // Strip MySQL conditional comments
        result = self.strip_conditional_comments(&result);

        // Convert ENGINE clause
        result = self.strip_engine_clause(&result);

        // Convert CHARSET/COLLATE
        result = self.strip_charset_clauses(&result);

        Ok(result.into_bytes())
    }

    /// Convert INSERT statement
    fn convert_insert(
        &mut self,
        stmt: &[u8],
        _table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Convert identifier quoting
        result = self.convert_identifiers(&result);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Convert string escapes (careful with data!)
        result = self.convert_string_escapes(&result);

        Ok(result.into_bytes())
    }

    /// Convert CREATE INDEX statement
    fn convert_create_index(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        // Convert identifier quoting
        result = self.convert_identifiers(&result);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.strip_schema_prefix(&result);
        }

        // Detect FULLTEXT/SPATIAL
        if result.contains("FULLTEXT") || result.contains("fulltext") {
            self.warnings.add(ConvertWarning::UnsupportedFeature {
                feature: "FULLTEXT INDEX".to_string(),
                suggestion: Some("Use PostgreSQL GIN index or skip".to_string()),
            });
            if self.strict {
                return Err(ConvertWarning::UnsupportedFeature {
                    feature: "FULLTEXT INDEX".to_string(),
                    suggestion: None,
                });
            }
        }

        Ok(result.into_bytes())
    }

    /// Convert ALTER TABLE statement
    fn convert_alter_table(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        result = self.convert_identifiers(&result);
        result = self.convert_data_types(&result);

        // Convert PostgreSQL-specific syntax
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_postgres_casts(&result);
            result = self.convert_nextval(&result);
            result = self.convert_default_now(&result);
            result = self.strip_schema_prefix(&result);
        }

        Ok(result.into_bytes())
    }

    /// Convert DROP TABLE statement
    fn convert_drop_table(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let mut result = stmt_str.to_string();

        result = self.convert_identifiers(&result);

        // Strip PostgreSQL schema prefix
        if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
            result = self.strip_schema_prefix(&result);
        }

        Ok(result.into_bytes())
    }

    /// Convert COPY statement (PostgreSQL-specific)
    ///
    /// This handles the COPY header. The data block is processed separately
    /// via process_copy_data() when called from the run() function.
    fn convert_copy(
        &mut self,
        stmt: &[u8],
        _table_name: Option<&str>,
    ) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);

        // Check if this contains "FROM stdin" (COPY header) or is data
        let upper = stmt_str.to_uppercase();
        if upper.contains("FROM STDIN") {
            // This is a COPY header - parse it and store for later
            if let Some(header) = parse_copy_header(&stmt_str) {
                if self.from == SqlDialect::Postgres && self.to != SqlDialect::Postgres {
                    // Store the header, will convert data block in process_copy_data
                    self.pending_copy_header = Some(header);
                    // Return empty - the actual INSERT will be generated from data
                    return Ok(Vec::new());
                }
            }
        }

        // If same dialect or couldn't parse, pass through
        Ok(stmt.to_vec())
    }

    /// Convert other statements (comments, session settings, etc.)
    fn convert_other(&mut self, stmt: &[u8]) -> Result<Vec<u8>, ConvertWarning> {
        let stmt_str = String::from_utf8_lossy(stmt);
        let result = stmt_str.to_string();
        let trimmed = result.trim();

        // Skip MySQL session commands when converting to other dialects
        if self.from == SqlDialect::MySql
            && self.to != SqlDialect::MySql
            && self.is_mysql_session_command(&result)
        {
            return Ok(Vec::new()); // Skip
        }

        // Skip PostgreSQL session commands and unsupported features when converting to other dialects
        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_postgres_session_command(&result)
        {
            return Ok(Vec::new()); // Skip
        }
        if self.from == SqlDialect::Postgres
            && self.to != SqlDialect::Postgres
            && self.is_postgres_only_feature(trimmed)
        {
            self.warnings.add(ConvertWarning::SkippedStatement {
                reason: "PostgreSQL-only feature".to_string(),
                statement_preview: trimmed.chars().take(60).collect(),
            });
            return Ok(Vec::new()); // Skip
        }

        // Skip SQLite pragmas when converting to other dialects
        if self.from == SqlDialect::Sqlite
            && self.to != SqlDialect::Sqlite
            && self.is_sqlite_pragma(&result)
        {
            return Ok(Vec::new()); // Skip
        }

        // Strip conditional comments
        if result.contains("/*!") {
            let stripped = self.strip_conditional_comments(&result);
            return Ok(stripped.into_bytes());
        }

        Ok(stmt.to_vec())
    }

    /// Check if statement is a MySQL session command
    fn is_mysql_session_command(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        upper.contains("SET NAMES")
            || upper.contains("SET CHARACTER")
            || upper.contains("SET SQL_MODE")
            || upper.contains("SET TIME_ZONE")
            || upper.contains("SET FOREIGN_KEY_CHECKS")
            || upper.contains("LOCK TABLES")
            || upper.contains("UNLOCK TABLES")
    }

    /// Check if statement is a PostgreSQL session command or unsupported statement
    fn is_postgres_session_command(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        // Session/transaction settings
        upper.contains("SET CLIENT_ENCODING")
            || upper.contains("SET STANDARD_CONFORMING_STRINGS")
            || upper.contains("SET CHECK_FUNCTION_BODIES")
            || upper.contains("SET SEARCH_PATH")
            || upper.contains("SET DEFAULT_TABLESPACE")
            || upper.contains("SET LOCK_TIMEOUT")
            || upper.contains("SET IDLE_IN_TRANSACTION_SESSION_TIMEOUT")
            || upper.contains("SET ROW_SECURITY")
            || upper.contains("SET STATEMENT_TIMEOUT")
            || upper.contains("SET XMLOPTION")
            || upper.contains("SET CLIENT_MIN_MESSAGES")
            || upper.contains("SET DEFAULT_TABLE_ACCESS_METHOD")
            || upper.contains("SELECT PG_CATALOG")
            // Ownership/permission statements
            || upper.contains("OWNER TO")
            || upper.contains("GRANT ")
            || upper.contains("REVOKE ")
    }

    /// Check if statement is a PostgreSQL-only feature that should be skipped
    fn is_postgres_only_feature(&self, stmt: &str) -> bool {
        // Strip leading comments to find the actual statement
        let stripped = self.strip_leading_sql_comments(stmt);
        let upper = stripped.to_uppercase();

        // These PostgreSQL features have no MySQL/SQLite equivalent
        upper.starts_with("CREATE DOMAIN")
            || upper.starts_with("CREATE TYPE")
            || upper.starts_with("CREATE FUNCTION")
            || upper.starts_with("CREATE PROCEDURE")
            || upper.starts_with("CREATE AGGREGATE")
            || upper.starts_with("CREATE OPERATOR")
            || upper.starts_with("CREATE SEQUENCE")
            || upper.starts_with("CREATE EXTENSION")
            || upper.starts_with("CREATE SCHEMA")
            || upper.starts_with("CREATE TRIGGER")
            || upper.starts_with("ALTER DOMAIN")
            || upper.starts_with("ALTER TYPE")
            || upper.starts_with("ALTER FUNCTION")
            || upper.starts_with("ALTER SEQUENCE")
            || upper.starts_with("ALTER SCHEMA")
            || upper.starts_with("COMMENT ON")
    }

    /// Strip leading SQL comments (-- and /* */) from a string
    fn strip_leading_sql_comments(&self, stmt: &str) -> String {
        let mut result = stmt.trim();
        loop {
            // Strip -- comments
            if result.starts_with("--") {
                if let Some(pos) = result.find('\n') {
                    result = result[pos + 1..].trim();
                    continue;
                } else {
                    return String::new();
                }
            }
            // Strip /* */ comments
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

    /// Check if statement is a SQLite pragma
    fn is_sqlite_pragma(&self, stmt: &str) -> bool {
        let upper = stmt.to_uppercase();
        upper.contains("PRAGMA")
    }

    /// Convert identifier quoting based on dialects
    fn convert_identifiers(&self, stmt: &str) -> String {
        match (self.from, self.to) {
            (SqlDialect::MySql, SqlDialect::Postgres | SqlDialect::Sqlite) => {
                // Backticks → double quotes
                self.backticks_to_double_quotes(stmt)
            }
            (SqlDialect::MySql, SqlDialect::Mssql) => {
                // Backticks → square brackets
                self.backticks_to_square_brackets(stmt)
            }
            (SqlDialect::Postgres | SqlDialect::Sqlite, SqlDialect::MySql) => {
                // Double quotes → backticks
                self.double_quotes_to_backticks(stmt)
            }
            (SqlDialect::Postgres | SqlDialect::Sqlite, SqlDialect::Mssql) => {
                // Double quotes → square brackets
                self.double_quotes_to_square_brackets(stmt)
            }
            (SqlDialect::Mssql, SqlDialect::MySql) => {
                // Square brackets → backticks
                self.square_brackets_to_backticks(stmt)
            }
            (SqlDialect::Mssql, SqlDialect::Postgres | SqlDialect::Sqlite) => {
                // Square brackets → double quotes
                self.square_brackets_to_double_quotes(stmt)
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert backticks to double quotes
    pub fn backticks_to_double_quotes(&self, stmt: &str) -> String {
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

    /// Convert double quotes to backticks
    pub fn double_quotes_to_backticks(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut in_dquote = false;
        let chars = stmt.chars();

        for c in chars {
            if c == '\'' && !in_dquote {
                in_string = !in_string;
                result.push(c);
            } else if c == '"' && !in_string {
                in_dquote = !in_dquote;
                result.push('`');
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert backticks to square brackets (for MSSQL)
    pub fn backticks_to_square_brackets(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut in_backtick = false;

        for c in stmt.chars() {
            if c == '\'' && !in_backtick {
                in_string = !in_string;
                result.push(c);
            } else if c == '`' && !in_string {
                if !in_backtick {
                    result.push('[');
                } else {
                    result.push(']');
                }
                in_backtick = !in_backtick;
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert double quotes to square brackets (for MSSQL)
    pub fn double_quotes_to_square_brackets(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;
        let mut in_dquote = false;

        for c in stmt.chars() {
            if c == '\'' && !in_dquote {
                in_string = !in_string;
                result.push(c);
            } else if c == '"' && !in_string {
                if !in_dquote {
                    result.push('[');
                } else {
                    result.push(']');
                }
                in_dquote = !in_dquote;
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert square brackets to backticks (from MSSQL to MySQL)
    pub fn square_brackets_to_backticks(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;

        for c in stmt.chars() {
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
            } else if !in_string && (c == '[' || c == ']') {
                result.push('`');
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert square brackets to double quotes (from MSSQL to PostgreSQL/SQLite)
    pub fn square_brackets_to_double_quotes(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut in_string = false;

        for c in stmt.chars() {
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
            } else if !in_string && (c == '[' || c == ']') {
                result.push('"');
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Convert data types between dialects
    fn convert_data_types(&self, stmt: &str) -> String {
        TypeMapper::convert(stmt, self.from, self.to)
    }

    /// Convert AUTO_INCREMENT/SERIAL syntax
    fn convert_auto_increment(&self, stmt: &str, _table_name: Option<&str>) -> String {
        match (self.from, self.to) {
            (SqlDialect::MySql, SqlDialect::Postgres) => {
                // INT AUTO_INCREMENT → SERIAL
                // BIGINT AUTO_INCREMENT → BIGSERIAL
                let result = stmt.replace("BIGINT AUTO_INCREMENT", "BIGSERIAL");
                let result = result.replace("bigint AUTO_INCREMENT", "BIGSERIAL");
                let result = result.replace("INT AUTO_INCREMENT", "SERIAL");
                let result = result.replace("int AUTO_INCREMENT", "SERIAL");
                result.replace("AUTO_INCREMENT", "") // Clean up any remaining
            }
            (SqlDialect::MySql, SqlDialect::Sqlite) => {
                // INT AUTO_INCREMENT PRIMARY KEY → INTEGER PRIMARY KEY
                // The AUTOINCREMENT keyword is optional in SQLite
                let result = stmt.replace("INT AUTO_INCREMENT", "INTEGER");
                let result = result.replace("int AUTO_INCREMENT", "INTEGER");
                result.replace("AUTO_INCREMENT", "")
            }
            (SqlDialect::Postgres, SqlDialect::MySql) => {
                // SERIAL → INT AUTO_INCREMENT
                // BIGSERIAL → BIGINT AUTO_INCREMENT
                let result = stmt.replace("BIGSERIAL", "BIGINT AUTO_INCREMENT");
                let result = result.replace("bigserial", "BIGINT AUTO_INCREMENT");
                let result = result.replace("SMALLSERIAL", "SMALLINT AUTO_INCREMENT");
                let result = result.replace("smallserial", "SMALLINT AUTO_INCREMENT");
                let result = result.replace("SERIAL", "INT AUTO_INCREMENT");
                result.replace("serial", "INT AUTO_INCREMENT")
            }
            (SqlDialect::Postgres, SqlDialect::Sqlite) => {
                // SERIAL → INTEGER (SQLite auto-increments INTEGER PRIMARY KEY)
                let result = stmt.replace("BIGSERIAL", "INTEGER");
                let result = result.replace("bigserial", "INTEGER");
                let result = result.replace("SMALLSERIAL", "INTEGER");
                let result = result.replace("smallserial", "INTEGER");
                let result = result.replace("SERIAL", "INTEGER");
                result.replace("serial", "INTEGER")
            }
            (SqlDialect::Sqlite, SqlDialect::MySql) => {
                // SQLite uses INTEGER PRIMARY KEY for auto-increment
                // We can't easily detect this pattern, so just pass through
                stmt.to_string()
            }
            (SqlDialect::Sqlite, SqlDialect::Postgres) => {
                // SQLite uses INTEGER PRIMARY KEY for auto-increment
                // We can't easily detect this pattern, so just pass through
                stmt.to_string()
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert string escape sequences
    fn convert_string_escapes(&self, stmt: &str) -> String {
        match (self.from, self.to) {
            (SqlDialect::MySql, SqlDialect::Postgres | SqlDialect::Sqlite) => {
                // MySQL uses \' for escaping, PostgreSQL/SQLite use ''
                self.mysql_escapes_to_standard(stmt)
            }
            _ => stmt.to_string(),
        }
    }

    /// Convert MySQL backslash escapes to standard SQL double-quote escapes
    fn mysql_escapes_to_standard(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut chars = stmt.chars().peekable();
        let mut in_string = false;

        while let Some(c) = chars.next() {
            if c == '\'' {
                in_string = !in_string;
                result.push(c);
            } else if c == '\\' && in_string {
                // Check next character
                if let Some(&next) = chars.peek() {
                    match next {
                        '\'' => {
                            // \' → ''
                            chars.next();
                            result.push_str("''");
                        }
                        '\\' => {
                            // \\ → keep as-is for data integrity
                            chars.next();
                            result.push_str("\\\\");
                        }
                        'n' | 'r' | 't' | '0' => {
                            // Keep common escapes as-is
                            result.push(c);
                        }
                        _ => {
                            result.push(c);
                        }
                    }
                } else {
                    result.push(c);
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Strip MySQL conditional comments /*!40101 ... */
    fn strip_conditional_comments(&self, stmt: &str) -> String {
        let mut result = String::with_capacity(stmt.len());
        let mut chars = stmt.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '/' && chars.peek() == Some(&'*') {
                chars.next(); // consume *
                if chars.peek() == Some(&'!') {
                    // Skip conditional comment
                    chars.next(); // consume !
                                  // Skip version number
                    while chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                        chars.next();
                    }
                    // Skip content until */
                    let mut depth = 1;
                    while depth > 0 {
                        match chars.next() {
                            Some('*') if chars.peek() == Some(&'/') => {
                                chars.next();
                                depth -= 1;
                            }
                            Some('/') if chars.peek() == Some(&'*') => {
                                chars.next();
                                depth += 1;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                } else {
                    // Regular comment, keep it
                    result.push('/');
                    result.push('*');
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Strip ENGINE clause
    fn strip_engine_clause(&self, stmt: &str) -> String {
        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        // Remove ENGINE=InnoDB, ENGINE=MyISAM, etc.
        let re = regex::Regex::new(r"(?i)\s*ENGINE\s*=\s*\w+").unwrap();
        re.replace_all(stmt, "").to_string()
    }

    /// Strip CHARSET/COLLATE clauses
    fn strip_charset_clauses(&self, stmt: &str) -> String {
        if self.to == SqlDialect::MySql {
            return stmt.to_string();
        }

        let result = stmt.to_string();
        let re1 = regex::Regex::new(r"(?i)\s*(DEFAULT\s+)?CHARSET\s*=\s*\w+").unwrap();
        let result = re1.replace_all(&result, "").to_string();

        let re2 = regex::Regex::new(r"(?i)\s*COLLATE\s*=?\s*\w+").unwrap();
        re2.replace_all(&result, "").to_string()
    }

    /// Strip PostgreSQL type casts (::type and ::regclass)
    fn strip_postgres_casts(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match ::regclass, ::text, ::integer, etc. (including complex types like character varying)
        static RE_CAST: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"::[a-zA-Z_][a-zA-Z0-9_]*(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)*").unwrap()
        });

        RE_CAST.replace_all(stmt, "").to_string()
    }

    /// Convert nextval('sequence') to NULL or remove (AUTO_INCREMENT handles it)
    fn convert_nextval(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match nextval('sequence_name'::regclass) or nextval('sequence_name')
        // Remove the DEFAULT nextval(...) entirely - AUTO_INCREMENT is already applied
        static RE_NEXTVAL: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*DEFAULT\s+nextval\s*\([^)]+\)").unwrap());

        RE_NEXTVAL.replace_all(stmt, "").to_string()
    }

    /// Convert DEFAULT now() to DEFAULT CURRENT_TIMESTAMP
    fn convert_default_now(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        static RE_NOW: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\bDEFAULT\s+now\s*\(\s*\)").unwrap());

        RE_NOW
            .replace_all(stmt, "DEFAULT CURRENT_TIMESTAMP")
            .to_string()
    }

    /// Strip schema prefix from table names (e.g., public.users -> users)
    fn strip_schema_prefix(&self, stmt: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Match schema.table patterns (with optional quotes)
        // Handle: public.table, "public"."table", public."table"
        static RE_SCHEMA: Lazy<Regex> =
            Lazy::new(|| Regex::new(r#"(?i)\b(public|pg_catalog|pg_temp)\s*\.\s*"#).unwrap());

        RE_SCHEMA.replace_all(stmt, "").to_string()
    }

    /// Detect unsupported features and add warnings
    fn detect_unsupported_features(
        &mut self,
        stmt: &str,
        table_name: Option<&str>,
    ) -> Result<(), ConvertWarning> {
        let upper = stmt.to_uppercase();

        // MySQL-specific features
        if self.from == SqlDialect::MySql {
            // ENUM types
            if upper.contains("ENUM(") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "ENUM type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Converted to VARCHAR - consider adding CHECK constraint".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // SET types (MySQL)
            if upper.contains("SET(") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "SET type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Converted to VARCHAR - SET semantics not preserved".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // UNSIGNED
            if upper.contains("UNSIGNED") {
                self.warnings.add(ConvertWarning::UnsupportedFeature {
                    feature: "UNSIGNED modifier".to_string(),
                    suggestion: Some(
                        "Removed - consider adding CHECK constraint for non-negative values"
                            .to_string(),
                    ),
                });
            }
        }

        // PostgreSQL-specific features
        if self.from == SqlDialect::Postgres {
            // Array types
            if upper.contains("[]") || upper.contains("ARRAY[") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: format!(
                        "Array type{}",
                        table_name
                            .map(|t| format!(" in table {}", t))
                            .unwrap_or_default()
                    ),
                    suggestion: Some(
                        "Array types not supported in target dialect - consider using JSON"
                            .to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // INHERITS
            if upper.contains("INHERITS") {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: "Table inheritance (INHERITS)".to_string(),
                    suggestion: Some(
                        "PostgreSQL table inheritance not supported in target dialect".to_string(),
                    ),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }

            // PARTITION BY
            if upper.contains("PARTITION BY") && self.to == SqlDialect::Sqlite {
                let warning = ConvertWarning::UnsupportedFeature {
                    feature: "Table partitioning".to_string(),
                    suggestion: Some("Partitioning not supported in SQLite".to_string()),
                };
                self.warnings.add(warning.clone());
                if self.strict {
                    return Err(warning);
                }
            }
        }

        Ok(())
    }

    /// Get collected warnings
    pub fn warnings(&self) -> &[ConvertWarning] {
        self.warnings.warnings()
    }
}

/// Run the convert command
pub fn run(config: ConvertConfig) -> anyhow::Result<ConvertStats> {
    let mut stats = ConvertStats::default();

    // Detect or use specified source dialect
    let from_dialect = if let Some(d) = config.from_dialect {
        d
    } else {
        let result = crate::parser::detect_dialect_from_file(&config.input)?;
        if config.progress {
            eprintln!(
                "Auto-detected source dialect: {} (confidence: {:?})",
                result.dialect, result.confidence
            );
        }
        result.dialect
    };

    // Check for same dialect
    if from_dialect == config.to_dialect {
        anyhow::bail!(
            "Source and target dialects are the same ({}). No conversion needed.",
            from_dialect
        );
    }

    // Get file size for progress tracking
    let file_size = std::fs::metadata(&config.input)?.len();

    let progress_bar = if config.progress {
        let pb = ProgressBar::new(file_size);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}",
            )
            .unwrap()
            .progress_chars("█▓▒░  ")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("Converting...");
        Some(pb)
    } else {
        None
    };

    // Create converter
    let mut converter = Converter::new(from_dialect, config.to_dialect).with_strict(config.strict);

    // Open input file with optional progress tracking
    let file = File::open(&config.input)?;
    let compression = Compression::from_path(&config.input);
    let reader: Box<dyn Read> = if let Some(ref pb) = progress_bar {
        let pb_clone = pb.clone();
        let progress_reader = ProgressReader::new(file, move |bytes| {
            pb_clone.set_position(bytes);
        });
        compression.wrap_reader(Box::new(progress_reader))
    } else {
        compression.wrap_reader(Box::new(file))
    };
    let mut parser = Parser::with_dialect(reader, 64 * 1024, from_dialect);

    // Open output
    let mut writer: Box<dyn Write> = if config.dry_run {
        Box::new(std::io::sink())
    } else {
        match &config.output {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Box::new(BufWriter::with_capacity(256 * 1024, File::create(path)?))
            }
            None => Box::new(BufWriter::new(std::io::stdout())),
        }
    };

    // Write header
    if !config.dry_run {
        write_header(&mut writer, &config, from_dialect)?;
    }

    // Process statements
    while let Some(stmt) = parser.read_statement()? {
        stats.statements_processed += 1;

        // Check if this is a COPY data block (follows a COPY header)
        if converter.has_pending_copy() {
            // This is a data block, convert it to INSERT statements
            match converter.process_copy_data(&stmt) {
                Ok(inserts) => {
                    for insert in inserts {
                        if !insert.is_empty() {
                            stats.statements_converted += 1;
                            if !config.dry_run {
                                writer.write_all(&insert)?;
                                writer.write_all(b"\n")?;
                            }
                        }
                    }
                }
                Err(warning) => {
                    stats.warnings.push(warning);
                    stats.statements_skipped += 1;
                }
            }
            continue;
        }

        match converter.convert_statement(&stmt) {
            Ok(converted) => {
                if converted.is_empty() {
                    stats.statements_skipped += 1;
                } else if converted == stmt {
                    stats.statements_unchanged += 1;
                    if !config.dry_run {
                        writer.write_all(&converted)?;
                        writer.write_all(b"\n")?;
                    }
                } else {
                    stats.statements_converted += 1;
                    if !config.dry_run {
                        writer.write_all(&converted)?;
                        writer.write_all(b"\n")?;
                    }
                }
            }
            Err(warning) => {
                stats.warnings.push(warning);
                stats.statements_skipped += 1;
            }
        }
    }

    // Collect warnings
    stats.warnings.extend(converter.warnings().iter().cloned());

    if let Some(pb) = progress_bar {
        pb.finish_with_message("done");
    }

    Ok(stats)
}

/// Write output header
fn write_header(
    writer: &mut dyn Write,
    config: &ConvertConfig,
    from: SqlDialect,
) -> std::io::Result<()> {
    writeln!(writer, "-- Converted by sql-splitter")?;
    writeln!(writer, "-- From: {} → To: {}", from, config.to_dialect)?;
    writeln!(writer, "-- Source: {}", config.input.display())?;
    writeln!(writer)?;

    // Write dialect-specific header
    match config.to_dialect {
        SqlDialect::Postgres => {
            writeln!(writer, "SET client_encoding = 'UTF8';")?;
            writeln!(writer, "SET standard_conforming_strings = on;")?;
        }
        SqlDialect::Sqlite => {
            writeln!(writer, "PRAGMA foreign_keys = OFF;")?;
        }
        SqlDialect::MySql => {
            writeln!(writer, "SET NAMES utf8mb4;")?;
            writeln!(writer, "SET FOREIGN_KEY_CHECKS = 0;")?;
        }
        SqlDialect::Mssql => {
            writeln!(writer, "SET ANSI_NULLS ON;")?;
            writeln!(writer, "SET QUOTED_IDENTIFIER ON;")?;
            writeln!(writer, "SET NOCOUNT ON;")?;
        }
    }
    writeln!(writer)?;

    Ok(())
}
