//! Validate module for SQL dump integrity checking.
//!
//! This module provides:
//! - SQL syntax validation (via parser error detection)
//! - DDL/DML consistency checks (INSERTs reference existing tables)
//! - Duplicate primary key detection (all dialects)
//! - FK referential integrity checking (all dialects)
//! - Encoding validation (UTF-8)

use crate::parser::{
    determine_buffer_size, mysql_insert, postgres_copy, Parser, SqlDialect, StatementType,
};
use crate::progress::ProgressReader;
use crate::schema::{Schema, SchemaBuilder, TableId};
use crate::splitter::Compression;
use ahash::{AHashMap, AHashSet};
use serde::Serialize;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

/// Maximum number of issues to collect before stopping
const MAX_ISSUES: usize = 1000;

/// Issue severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => write!(f, "ERROR"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Info => write!(f, "INFO"),
        }
    }
}

/// Location in the SQL dump where an issue was found
#[derive(Debug, Clone, Serialize)]
pub struct Location {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approx_line: Option<u64>,
}

impl Location {
    pub fn new() -> Self {
        Self {
            table: None,
            statement_index: None,
            approx_line: None,
        }
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn with_statement(mut self, index: u64) -> Self {
        self.statement_index = Some(index);
        self
    }

    #[allow(dead_code)]
    pub fn with_line(mut self, line: u64) -> Self {
        self.approx_line = Some(line);
        self
    }
}

impl Default for Location {
    fn default() -> Self {
        Self::new()
    }
}

/// A validation issue found in the SQL dump
#[derive(Debug, Clone, Serialize)]
pub struct ValidationIssue {
    pub code: &'static str,
    pub severity: Severity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
}

impl ValidationIssue {
    pub fn error(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Error,
            message: message.into(),
            location: None,
        }
    }

    pub fn warning(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Warning,
            message: message.into(),
            location: None,
        }
    }

    pub fn info(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: Severity::Info,
            message: message.into(),
            location: None,
        }
    }

    pub fn with_location(mut self, location: Location) -> Self {
        self.location = Some(location);
        self
    }
}

impl fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}]", self.severity, self.code)?;
        if let Some(ref loc) = self.location {
            if let Some(ref table) = loc.table {
                write!(f, " table={}", table)?;
            }
            if let Some(stmt) = loc.statement_index {
                write!(f, " stmt={}", stmt)?;
            }
            if let Some(line) = loc.approx_line {
                write!(f, " line~{}", line)?;
            }
        }
        write!(f, ": {}", self.message)
    }
}

/// Validation options
#[derive(Debug, Clone)]
pub struct ValidateOptions {
    pub path: PathBuf,
    pub dialect: Option<SqlDialect>,
    pub progress: bool,
    pub strict: bool,
    pub json: bool,
    pub max_rows_per_table: usize,
    pub fk_checks_enabled: bool,
    /// Optional global cap on tracked PK/FK keys for memory safety.
    /// When exceeded, PK/FK checks are skipped for the remainder of the run.
    /// If None, no limit is enforced (default).
    pub max_pk_fk_keys: Option<usize>,
}

/// Validation summary with collected issues
#[derive(Debug, Serialize)]
pub struct ValidationSummary {
    pub dialect: String,
    pub issues: Vec<ValidationIssue>,
    pub summary: SummaryStats,
    pub checks: CheckResults,
}

#[derive(Debug, Serialize)]
pub struct SummaryStats {
    pub errors: usize,
    pub warnings: usize,
    pub info: usize,
    pub tables_scanned: usize,
    pub statements_scanned: u64,
}

#[derive(Debug, Serialize)]
pub struct CheckResults {
    pub syntax: CheckStatus,
    pub encoding: CheckStatus,
    pub ddl_dml_consistency: CheckStatus,
    pub pk_duplicates: CheckStatus,
    pub fk_integrity: CheckStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Ok,
    Failed(usize),
    Skipped(String),
}

impl fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckStatus::Ok => write!(f, "OK"),
            CheckStatus::Failed(n) => write!(f, "{} issues", n),
            CheckStatus::Skipped(reason) => write!(f, "Skipped ({})", reason),
        }
    }
}

impl ValidationSummary {
    pub fn has_errors(&self) -> bool {
        self.summary.errors > 0
    }

    pub fn has_warnings(&self) -> bool {
        self.summary.warnings > 0
    }
}

/// Compact primary/foreign key representation for duplicate and FK checks.
/// We use a 64-bit hash; collision risk is negligible for realistic dumps.
type PkHash = u64;

/// Hash a list of PK/FK values into a compact 64-bit hash.
/// Uses AHash for fast, high-quality hashing.
fn hash_pk_values(values: &smallvec::SmallVec<[mysql_insert::PkValue; 2]>) -> PkHash {
    let mut hasher = ahash::AHasher::default();

    // Include arity (number of columns) in the hash to distinguish (1) from (1, NULL)
    (values.len() as u8).hash(&mut hasher);

    for v in values {
        match v {
            mysql_insert::PkValue::Int(i) => {
                0u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            mysql_insert::PkValue::BigInt(i) => {
                1u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            mysql_insert::PkValue::Text(s) => {
                2u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            mysql_insert::PkValue::Null => {
                3u8.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

/// Pending FK check to be validated after all PKs are loaded.
/// Uses compact hash representation to minimize memory usage.
struct PendingFkCheck {
    child_table_id: TableId,
    parent_table_id: TableId,
    fk_hash: PkHash,
    stmt_idx: u64,
}

/// Per-table tracking state for data checks.
/// Uses hashed PK values to minimize memory usage.
struct TableState {
    row_count: u64,
    /// Set of hashed PKs for duplicate and FK parent existence checks.
    /// When None, PK/FK checks for this table are skipped (due to row or memory limits).
    pk_values: Option<AHashSet<PkHash>>,
    pk_column_indices: Vec<usize>,
    pk_duplicates: u64,
    fk_missing_parents: u64,
}

impl TableState {
    fn new() -> Self {
        Self {
            row_count: 0,
            pk_values: Some(AHashSet::new()),
            pk_column_indices: Vec::new(),
            pk_duplicates: 0,
            fk_missing_parents: 0,
        }
    }

    fn with_pk_columns(mut self, indices: Vec<usize>) -> Self {
        self.pk_column_indices = indices;
        self
    }
}

/// SQL dump validator
pub struct Validator {
    options: ValidateOptions,
    issues: Vec<ValidationIssue>,
    dialect: SqlDialect,

    // DDL/DML tracking
    tables_from_ddl: AHashSet<String>,
    tables_from_dml: Vec<(String, u64)>, // (table_name, statement_index)

    // Schema for MySQL PK/FK checks
    schema_builder: SchemaBuilder,
    schema: Option<Schema>,

    // Per-table state for data checks
    table_states: AHashMap<TableId, TableState>,

    // Pending FK checks (deferred until all PKs are loaded)
    pending_fk_checks: Vec<PendingFkCheck>,

    // Progress callback for byte-based progress tracking (Arc for reuse across passes)
    progress_fn: Option<Arc<dyn Fn(u64) + Send + Sync>>,

    // Counters
    statement_count: u64,
    syntax_errors: usize,
    encoding_warnings: usize,
    ddl_dml_errors: usize,
    pk_errors: usize,
    fk_errors: usize,

    // Memory tracking for PK/FK checks
    tracked_pk_count: usize,
    tracked_fk_count: usize,
    pk_fk_checks_disabled_due_to_memory: bool,

    // PostgreSQL COPY context: (table_name, column_order, table_id)
    current_copy_context: Option<(String, Vec<String>, TableId)>,
}

impl Validator {
    pub fn new(options: ValidateOptions) -> Self {
        Self {
            dialect: options.dialect.unwrap_or(SqlDialect::MySql),
            options,
            issues: Vec::new(),
            tables_from_ddl: AHashSet::new(),
            tables_from_dml: Vec::new(),
            schema_builder: SchemaBuilder::new(),
            schema: None,
            table_states: AHashMap::new(),
            pending_fk_checks: Vec::new(),
            progress_fn: None,
            statement_count: 0,
            syntax_errors: 0,
            encoding_warnings: 0,
            ddl_dml_errors: 0,
            pk_errors: 0,
            fk_errors: 0,
            tracked_pk_count: 0,
            tracked_fk_count: 0,
            pk_fk_checks_disabled_due_to_memory: false,
            current_copy_context: None,
        }
    }

    /// Set a progress callback for byte-based progress tracking.
    /// The callback receives cumulative bytes read across both validation passes.
    pub fn with_progress<F>(mut self, f: F) -> Self
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        self.progress_fn = Some(Arc::new(f));
        self
    }

    fn add_issue(&mut self, issue: ValidationIssue) {
        if self.issues.len() >= MAX_ISSUES {
            return;
        }

        match issue.severity {
            Severity::Error => match issue.code {
                "SYNTAX" => self.syntax_errors += 1,
                "DDL_MISSING_TABLE" => self.ddl_dml_errors += 1,
                "DUPLICATE_PK" => self.pk_errors += 1,
                "FK_MISSING_PARENT" => self.fk_errors += 1,
                _ => {}
            },
            Severity::Warning => {
                if issue.code == "ENCODING" {
                    self.encoding_warnings += 1;
                }
            }
            Severity::Info => {}
        }

        self.issues.push(issue);
    }

    /// Check if we've exceeded the memory budget for PK/FK tracking.
    /// If so, disable further checks and free existing state.
    fn enforce_pk_fk_memory_budget(&mut self) {
        if self.pk_fk_checks_disabled_due_to_memory {
            return;
        }

        let Some(limit) = self.options.max_pk_fk_keys else {
            return;
        };

        let total_tracked = self.tracked_pk_count + self.tracked_fk_count;
        if total_tracked > limit {
            self.pk_fk_checks_disabled_due_to_memory = true;

            // Drop existing state to free memory
            for state in self.table_states.values_mut() {
                state.pk_values = None;
            }
            self.pending_fk_checks.clear();
            self.pending_fk_checks.shrink_to_fit();

            self.add_issue(ValidationIssue::warning(
                "PK_FK_CHECKS_SKIPPED_MEMORY",
                format!(
                    "Skipping PK/FK checks after tracking {} keys (memory limit of {} exceeded)",
                    total_tracked, limit
                ),
            ));
        }
    }

    pub fn validate(mut self) -> anyhow::Result<ValidationSummary> {
        let file = File::open(&self.options.path)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);

        // Pass 1 reports bytes as 0 to file_size/2 (first half of progress bar)
        let compression = Compression::from_path(&self.options.path);
        let reader: Box<dyn Read> = if let Some(ref cb) = self.progress_fn {
            let cb = Arc::clone(cb);
            let progress_reader = ProgressReader::new(file, move |bytes| {
                // Scale to first half: 0% to 50%
                cb(bytes / 2)
            });
            compression.wrap_reader(Box::new(progress_reader))
        } else {
            compression.wrap_reader(Box::new(file))
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, self.dialect);

        // Pass 1: Build schema and check DDL/DML consistency
        loop {
            match parser.read_statement() {
                Ok(Some(stmt)) => {
                    self.statement_count += 1;
                    self.process_statement(&stmt);
                }
                Ok(None) => break,
                Err(e) => {
                    self.add_issue(
                        ValidationIssue::error("SYNTAX", format!("Parser error: {}", e))
                            .with_location(
                                Location::new().with_statement(self.statement_count + 1),
                            ),
                    );
                    break;
                }
            }
        }

        // Check for DML referencing missing tables - collect issues first, then add them
        let missing_table_issues: Vec<_> = self
            .tables_from_dml
            .iter()
            .filter(|(table, _)| {
                let table_lower = table.to_lowercase();
                !self
                    .tables_from_ddl
                    .iter()
                    .any(|t| t.to_lowercase() == table_lower)
            })
            .map(|(table, stmt_idx)| {
                ValidationIssue::error(
                    "DDL_MISSING_TABLE",
                    format!(
                        "INSERT/COPY references table '{}' with no CREATE TABLE",
                        table
                    ),
                )
                .with_location(Location::new().with_table(table).with_statement(*stmt_idx))
            })
            .collect();

        for issue in missing_table_issues {
            self.add_issue(issue);
        }

        // Finalize schema and resolve FK references for data checks (all dialects)
        if self.options.fk_checks_enabled {
            self.schema = Some(self.schema_builder.build());
            self.schema_builder = SchemaBuilder::new(); // Reset to avoid double use
            self.initialize_table_states();
        }

        // Pass 2: Data checks (PK + collect FK refs) - requires re-reading the file
        let schema_not_empty = self.schema.as_ref().is_some_and(|s| !s.is_empty());
        if self.options.fk_checks_enabled && schema_not_empty {
            self.run_data_checks()?;
            // Now that all PKs are loaded, validate the collected FK references
            self.validate_pending_fk_checks();
        }

        Ok(self.build_summary())
    }

    fn process_statement(&mut self, stmt: &[u8]) {
        // Check encoding
        if std::str::from_utf8(stmt).is_err() {
            self.add_issue(
                ValidationIssue::warning("ENCODING", "Statement contains invalid UTF-8 bytes")
                    .with_location(Location::new().with_statement(self.statement_count)),
            );
        }

        let (stmt_type, table_name) =
            Parser::<&[u8]>::parse_statement_with_dialect(stmt, self.dialect);

        match stmt_type {
            StatementType::CreateTable => {
                if !table_name.is_empty() {
                    self.tables_from_ddl.insert(table_name.clone());

                    // Parse CREATE TABLE for schema info (all dialects supported)
                    if let Ok(stmt_str) = std::str::from_utf8(stmt) {
                        self.schema_builder.parse_create_table(stmt_str);
                    }
                }
            }
            StatementType::AlterTable => {
                // Parse ALTER TABLE for FK constraints (all dialects supported)
                if let Ok(stmt_str) = std::str::from_utf8(stmt) {
                    self.schema_builder.parse_alter_table(stmt_str);
                }
            }
            StatementType::Insert | StatementType::Copy => {
                if !table_name.is_empty() {
                    self.tables_from_dml
                        .push((table_name, self.statement_count));
                }
            }
            StatementType::Unknown => {
                // Could be a session command or comment - not an error
            }
            _ => {}
        }
    }

    fn initialize_table_states(&mut self) {
        let schema = match &self.schema {
            Some(s) => s,
            None => return,
        };

        for table_schema in schema.iter() {
            let pk_indices: Vec<usize> = table_schema
                .primary_key
                .iter()
                .map(|col_id| col_id.0 as usize)
                .collect();

            let state = TableState::new().with_pk_columns(pk_indices);
            self.table_states.insert(table_schema.id, state);
        }
    }

    fn run_data_checks(&mut self) -> anyhow::Result<()> {
        let file = File::open(&self.options.path)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);

        // Pass 2 reports bytes as file_size/2 to file_size (second half of progress bar)
        let compression = Compression::from_path(&self.options.path);
        let reader: Box<dyn Read> = if let Some(ref cb) = self.progress_fn {
            let cb = Arc::clone(cb);
            let progress_reader = ProgressReader::new(file, move |bytes| {
                // Scale to second half: 50% to 100%
                cb(file_size / 2 + bytes / 2)
            });
            compression.wrap_reader(Box::new(progress_reader))
        } else {
            compression.wrap_reader(Box::new(file))
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, self.dialect);
        let mut stmt_count: u64 = 0;

        // Reset COPY context for this pass
        self.current_copy_context = None;

        while let Some(stmt) = parser.read_statement()? {
            stmt_count += 1;

            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, self.dialect);

            // Handle PostgreSQL COPY data (separate statement from header)
            if self.dialect == SqlDialect::Postgres && stmt_type == StatementType::Unknown {
                // Check if this looks like COPY data (ends with \.)
                if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                    if let Some((ref copy_table, ref column_order, copy_table_id)) =
                        self.current_copy_context.clone()
                    {
                        self.check_copy_data(
                            &stmt,
                            copy_table_id,
                            copy_table,
                            column_order.clone(),
                            stmt_count,
                        );
                    }
                }
                self.current_copy_context = None;
                continue;
            }

            // Get table_id without holding a borrow on self.schema
            let table_id = match &self.schema {
                Some(s) => match s.get_table_id(&table_name) {
                    Some(id) => id,
                    None => continue,
                },
                None => continue,
            };

            match stmt_type {
                StatementType::Insert => {
                    // MySQL and SQLite use INSERT VALUES syntax
                    self.check_insert_statement(&stmt, table_id, &table_name, stmt_count);
                }
                StatementType::Copy => {
                    // For PostgreSQL COPY, the data comes in the next statement
                    // Save context for processing the data statement
                    let header = String::from_utf8_lossy(&stmt);
                    let column_order = postgres_copy::parse_copy_columns(&header);
                    self.current_copy_context = Some((table_name.clone(), column_order, table_id));
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Check rows from a MySQL/SQLite INSERT statement
    fn check_insert_statement(
        &mut self,
        stmt: &[u8],
        table_id: TableId,
        table_name: &str,
        stmt_count: u64,
    ) {
        let table_schema = match &self.schema {
            Some(s) => match s.table(table_id) {
                Some(ts) => ts,
                None => return,
            },
            None => return,
        };

        // Parse rows from INSERT using the schema (works for MySQL and SQLite)
        let rows = match mysql_insert::parse_mysql_insert_rows(stmt, table_schema) {
            Ok(r) => r,
            Err(_) => return,
        };

        for row in rows {
            self.check_mysql_row(table_id, table_name, &row, stmt_count);
        }
    }

    /// Check rows from a PostgreSQL COPY statement
    fn check_copy_statement(
        &mut self,
        stmt: &[u8],
        table_id: TableId,
        table_name: &str,
        stmt_count: u64,
    ) {
        // Find the COPY header line and the data section
        let stmt_str = match std::str::from_utf8(stmt) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Find the data section (after the header line ending with "FROM stdin;")
        let data_start = if let Some(pos) = stmt_str.find("FROM stdin;") {
            pos + "FROM stdin;".len()
        } else if let Some(pos) = stmt_str.find("from stdin;") {
            pos + "from stdin;".len()
        } else {
            return;
        };

        // Skip any whitespace/newlines after the header
        let data_section = stmt_str[data_start..].trim_start();
        if data_section.is_empty() {
            return;
        }

        // Parse column list from the header
        let header = &stmt_str[..data_start];
        let column_order = postgres_copy::parse_copy_columns(header);

        // Get table schema
        let table_schema = match &self.schema {
            Some(s) => match s.table(table_id) {
                Some(ts) => ts,
                None => return,
            },
            None => return,
        };

        // Parse the COPY data rows
        let rows = match postgres_copy::parse_postgres_copy_rows(
            data_section.as_bytes(),
            table_schema,
            column_order,
        ) {
            Ok(r) => r,
            Err(_) => return,
        };

        for row in rows {
            self.check_copy_row(table_id, table_name, &row, stmt_count);
        }
    }

    /// Check rows from PostgreSQL COPY data (separate statement from header)
    fn check_copy_data(
        &mut self,
        data_stmt: &[u8],
        table_id: TableId,
        table_name: &str,
        column_order: Vec<String>,
        stmt_count: u64,
    ) {
        // The data_stmt contains the raw COPY data lines (may have leading newline)
        // Strip leading whitespace/newlines
        let data: Vec<u8> = data_stmt
            .iter()
            .skip_while(|&&b| b == b'\n' || b == b'\r' || b == b' ' || b == b'\t')
            .cloned()
            .collect();

        if data.is_empty() {
            return;
        }

        // Get table schema
        let table_schema = match &self.schema {
            Some(s) => match s.table(table_id) {
                Some(ts) => ts,
                None => return,
            },
            None => return,
        };

        // Parse the COPY data rows
        let rows = match postgres_copy::parse_postgres_copy_rows(&data, table_schema, column_order)
        {
            Ok(r) => r,
            Err(_) => return,
        };

        for row in rows {
            self.check_copy_row(table_id, table_name, &row, stmt_count);
        }
    }

    /// Check a row from MySQL INSERT or SQLite INSERT
    fn check_mysql_row(
        &mut self,
        table_id: TableId,
        table_name: &str,
        row: &mysql_insert::ParsedRow,
        stmt_idx: u64,
    ) {
        self.check_row_common(
            table_id,
            table_name,
            row.pk.as_ref(),
            &row.fk_values,
            stmt_idx,
        );
    }

    /// Check a row from PostgreSQL COPY
    fn check_copy_row(
        &mut self,
        table_id: TableId,
        table_name: &str,
        row: &postgres_copy::ParsedCopyRow,
        stmt_idx: u64,
    ) {
        self.check_row_common(
            table_id,
            table_name,
            row.pk.as_ref(),
            &row.fk_values,
            stmt_idx,
        );
    }

    /// Common row checking logic for all dialects
    fn check_row_common(
        &mut self,
        table_id: TableId,
        table_name: &str,
        pk: Option<&smallvec::SmallVec<[mysql_insert::PkValue; 2]>>,
        fk_values: &[(
            mysql_insert::FkRef,
            smallvec::SmallVec<[mysql_insert::PkValue; 2]>,
        )],
        stmt_idx: u64,
    ) {
        // Skip if memory budget exceeded
        if self.pk_fk_checks_disabled_due_to_memory {
            return;
        }

        let max_rows = self.options.max_rows_per_table as u64;

        let state = match self.table_states.get_mut(&table_id) {
            Some(s) => s,
            None => return,
        };

        state.row_count += 1;

        // Check if we've exceeded max rows for this table
        if state.row_count > max_rows {
            if state.pk_values.is_some() {
                state.pk_values = None;
                self.add_issue(
                    ValidationIssue::warning(
                        "PK_CHECK_SKIPPED",
                        format!(
                            "Skipping PK/FK checks for table '{}' after {} rows (increase --max-rows-per-table)",
                            table_name, max_rows
                        ),
                    )
                    .with_location(Location::new().with_table(table_name)),
                );
            }
            return;
        }

        // PK duplicate check using hash-based storage (8 bytes per key instead of full values)
        if let Some(pk_values) = pk {
            if let Some(ref mut pk_set) = state.pk_values {
                let pk_hash = hash_pk_values(pk_values);

                if pk_set.insert(pk_hash) {
                    // Only count unique keys
                    self.tracked_pk_count += 1;
                    self.enforce_pk_fk_memory_budget();
                } else {
                    // Duplicate detected
                    state.pk_duplicates += 1;

                    // Build human-readable display on demand (duplicates are rare)
                    let pk_display: String = pk_values
                        .iter()
                        .map(|v| match v {
                            mysql_insert::PkValue::Int(i) => i.to_string(),
                            mysql_insert::PkValue::BigInt(i) => i.to_string(),
                            mysql_insert::PkValue::Text(s) => s.to_string(),
                            mysql_insert::PkValue::Null => "NULL".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");

                    self.add_issue(
                        ValidationIssue::error(
                            "DUPLICATE_PK",
                            format!(
                                "Duplicate primary key in table '{}': ({})",
                                table_name, pk_display
                            ),
                        )
                        .with_location(
                            Location::new()
                                .with_table(table_name)
                                .with_statement(stmt_idx),
                        ),
                    );
                }
            }
        }

        // Skip FK collection if checks are disabled
        if self.pk_fk_checks_disabled_due_to_memory {
            return;
        }

        // Collect FK references for deferred validation (after all PKs are loaded)
        // First, gather the FK checks into a temp vec to avoid borrow issues
        let new_fk_checks: Vec<PendingFkCheck> = {
            let schema = match &self.schema {
                Some(s) => s,
                None => return,
            };

            let table_schema = match schema.table(table_id) {
                Some(t) => t,
                None => return,
            };

            fk_values
                .iter()
                .filter_map(|(fk_ref, fk_vals)| {
                    // Skip if all FK values are NULL (nullable FK)
                    if fk_vals.iter().all(|v| v.is_null()) {
                        return None;
                    }

                    let fk_def = table_schema.foreign_keys.get(fk_ref.fk_index as usize)?;
                    let parent_table_id = fk_def.referenced_table_id?;

                    // Store only the hash, not full values - saves significant memory
                    let fk_hash = hash_pk_values(fk_vals);

                    Some(PendingFkCheck {
                        child_table_id: table_id,
                        parent_table_id,
                        fk_hash,
                        stmt_idx,
                    })
                })
                .collect()
        };

        // Now add the FK checks and update memory tracking
        let new_count = new_fk_checks.len();
        self.pending_fk_checks.extend(new_fk_checks);
        self.tracked_fk_count += new_count;

        if new_count > 0 {
            self.enforce_pk_fk_memory_budget();
        }
    }

    /// Validate all collected FK references after all PKs are loaded
    fn validate_pending_fk_checks(&mut self) {
        for check in std::mem::take(&mut self.pending_fk_checks) {
            let parent_has_pk = self
                .table_states
                .get(&check.parent_table_id)
                .and_then(|s| s.pk_values.as_ref())
                .is_some_and(|set| set.contains(&check.fk_hash));

            if !parent_has_pk {
                let state = match self.table_states.get_mut(&check.child_table_id) {
                    Some(s) => s,
                    None => continue,
                };
                state.fk_missing_parents += 1;

                // Only add issue for first few violations per table
                if state.fk_missing_parents <= 5 {
                    // Derive table names from the schema (not stored per FK to save memory)
                    let (child_name, parent_name) = if let Some(schema) = &self.schema {
                        let child = schema
                            .table(check.child_table_id)
                            .map(|t| t.name.clone())
                            .unwrap_or_else(|| "<unknown>".to_string());
                        let parent = schema
                            .table(check.parent_table_id)
                            .map(|t| t.name.clone())
                            .unwrap_or_else(|| "<unknown>".to_string());
                        (child, parent)
                    } else {
                        ("<unknown>".to_string(), "<unknown>".to_string())
                    };

                    self.add_issue(
                        ValidationIssue::error(
                            "FK_MISSING_PARENT",
                            format!(
                                "FK violation in '{}': references missing row in '{}'",
                                child_name, parent_name
                            ),
                        )
                        .with_location(
                            Location::new()
                                .with_table(child_name)
                                .with_statement(check.stmt_idx),
                        ),
                    );
                }
            }
        }
    }

    fn build_summary(&self) -> ValidationSummary {
        let errors = self
            .issues
            .iter()
            .filter(|i| matches!(i.severity, Severity::Error))
            .count();
        let warnings = self
            .issues
            .iter()
            .filter(|i| matches!(i.severity, Severity::Warning))
            .count();
        let info = self
            .issues
            .iter()
            .filter(|i| matches!(i.severity, Severity::Info))
            .count();

        let syntax_status = if self.syntax_errors > 0 {
            CheckStatus::Failed(self.syntax_errors)
        } else {
            CheckStatus::Ok
        };

        let encoding_status = if self.encoding_warnings > 0 {
            CheckStatus::Failed(self.encoding_warnings)
        } else {
            CheckStatus::Ok
        };

        let ddl_dml_status = if self.ddl_dml_errors > 0 {
            CheckStatus::Failed(self.ddl_dml_errors)
        } else {
            CheckStatus::Ok
        };

        let pk_status = if !self.options.fk_checks_enabled {
            CheckStatus::Skipped("--no-fk-checks".to_string())
        } else if self.pk_fk_checks_disabled_due_to_memory {
            CheckStatus::Skipped("memory limit exceeded".to_string())
        } else if self.pk_errors > 0 {
            CheckStatus::Failed(self.pk_errors)
        } else {
            CheckStatus::Ok
        };

        let fk_status = if !self.options.fk_checks_enabled {
            CheckStatus::Skipped("--no-fk-checks".to_string())
        } else if self.pk_fk_checks_disabled_due_to_memory {
            CheckStatus::Skipped("memory limit exceeded".to_string())
        } else if self.fk_errors > 0 {
            CheckStatus::Failed(self.fk_errors)
        } else {
            CheckStatus::Ok
        };

        ValidationSummary {
            dialect: self.dialect.to_string(),
            issues: self.issues.clone(),
            summary: SummaryStats {
                errors,
                warnings,
                info,
                tables_scanned: self.tables_from_ddl.len(),
                statements_scanned: self.statement_count,
            },
            checks: CheckResults {
                syntax: syntax_status,
                encoding: encoding_status,
                ddl_dml_consistency: ddl_dml_status,
                pk_duplicates: pk_status,
                fk_integrity: fk_status,
            },
        }
    }
}
