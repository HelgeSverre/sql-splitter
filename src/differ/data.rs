//! Data comparison for diff command.
//!
//! Uses streaming with memory-bounded PK tracking to compare row-level
//! differences between two SQL dumps.

use super::{should_include_table, DiffWarning};
use crate::parser::{
    determine_buffer_size, mysql_insert, postgres_copy, Parser, SqlDialect, StatementType,
};
use crate::pk::{hash_pk_values, PkHash};
use crate::progress::ProgressReader;
use crate::schema::Schema;
use crate::splitter::Compression;
use ahash::AHashMap;
use glob::Pattern;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

/// Options for data comparison
#[derive(Debug, Clone)]
pub struct DataDiffOptions {
    /// Maximum PK entries to track globally
    pub max_pk_entries_global: usize,
    /// Maximum PK entries per table
    pub max_pk_entries_per_table: usize,
    /// Number of sample rows to collect for verbose mode
    pub sample_size: usize,
    /// Tables to include (if empty, include all)
    pub tables: Vec<String>,
    /// Tables to exclude
    pub exclude: Vec<String>,
    /// Don't skip tables without PK, use all columns as key
    pub allow_no_pk: bool,
    /// Primary key overrides: table name -> column names
    pub pk_overrides: std::collections::HashMap<String, Vec<String>>,
    /// Column patterns to ignore (glob format: table.column)
    pub ignore_columns: Vec<String>,
}

impl Default for DataDiffOptions {
    fn default() -> Self {
        Self {
            max_pk_entries_global: 10_000_000,
            max_pk_entries_per_table: 5_000_000,
            sample_size: 0,
            tables: Vec::new(),
            exclude: Vec::new(),
            allow_no_pk: false,
            pk_overrides: std::collections::HashMap::new(),
            ignore_columns: Vec::new(),
        }
    }
}

/// Complete data diff result
#[derive(Debug, Serialize)]
pub struct DataDiff {
    /// Per-table data differences
    pub tables: HashMap<String, TableDataDiff>,
}

/// Data differences for a single table
#[derive(Debug, Serialize, Clone, Default)]
pub struct TableDataDiff {
    /// Row count in old file
    pub old_row_count: u64,
    /// Row count in new file
    pub new_row_count: u64,
    /// Number of rows added (in new but not old)
    pub added_count: u64,
    /// Number of rows removed (in old but not new)
    pub removed_count: u64,
    /// Number of rows modified (same PK, different content)
    pub modified_count: u64,
    /// Whether tracking was truncated due to memory limits
    pub truncated: bool,
    /// Sample PKs for added rows (only when verbose)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample_added_pks: Vec<String>,
    /// Sample PKs for removed rows (only when verbose)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample_removed_pks: Vec<String>,
    /// Sample PKs for modified rows (only when verbose)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample_modified_pks: Vec<String>,
}

/// Per-table state during data scanning
struct TableState {
    /// Row count seen
    row_count: u64,
    /// Map from PK hash to row digest (for detecting modifications)
    /// None if tracking was disabled due to memory limits
    pk_to_digest: Option<AHashMap<PkHash, u64>>,
    /// Map from PK hash to formatted PK string (only when sample_size > 0)
    pk_strings: Option<AHashMap<PkHash, String>>,
    /// Whether tracking was truncated
    truncated: bool,
}

impl TableState {
    fn new() -> Self {
        Self {
            row_count: 0,
            pk_to_digest: Some(AHashMap::new()),
            pk_strings: None,
            truncated: false,
        }
    }

    fn new_with_pk_strings() -> Self {
        Self {
            row_count: 0,
            pk_to_digest: Some(AHashMap::new()),
            pk_strings: Some(AHashMap::new()),
            truncated: false,
        }
    }
}

/// Hash non-PK column values to detect row modifications
fn hash_row_digest(values: &[mysql_insert::PkValue]) -> u64 {
    let mut hasher = ahash::AHasher::default();
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

/// Format a single PK value as a string
fn format_single_pk(v: &mysql_insert::PkValue) -> String {
    match v {
        mysql_insert::PkValue::Int(i) => i.to_string(),
        mysql_insert::PkValue::BigInt(i) => i.to_string(),
        mysql_insert::PkValue::Text(s) => s.to_string(),
        mysql_insert::PkValue::Null => "NULL".to_string(),
    }
}

/// Format a PK tuple as a string (single value as-is, composite as "(val1, val2)")
fn format_pk_value(pk: &[mysql_insert::PkValue]) -> String {
    if pk.len() == 1 {
        format_single_pk(&pk[0])
    } else {
        let parts: Vec<String> = pk.iter().map(format_single_pk).collect();
        format!("({})", parts.join(", "))
    }
}

/// Parse ignore column patterns into compiled Pattern objects
fn parse_ignore_patterns(patterns: &[String]) -> Vec<Pattern> {
    patterns
        .iter()
        .filter_map(|p| Pattern::new(&p.to_lowercase()).ok())
        .collect()
}

/// Check if a column should be ignored based on patterns
fn should_ignore_column(table: &str, column: &str, patterns: &[Pattern]) -> bool {
    let full_name = format!("{}.{}", table.to_lowercase(), column.to_lowercase());
    patterns.iter().any(|p| p.matches(&full_name))
}

/// Hash non-PK column values to detect row modifications, excluding ignored column indices
fn hash_row_digest_with_ignore(values: &[mysql_insert::PkValue], ignore_indices: &[usize]) -> u64 {
    let mut hasher = ahash::AHasher::default();
    for (i, v) in values.iter().enumerate() {
        if ignore_indices.contains(&i) {
            continue;
        }
        match v {
            mysql_insert::PkValue::Int(val) => {
                0u8.hash(&mut hasher);
                val.hash(&mut hasher);
            }
            mysql_insert::PkValue::BigInt(val) => {
                1u8.hash(&mut hasher);
                val.hash(&mut hasher);
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

/// Data differ engine that accumulates state across file scans
pub struct DataDiffer {
    options: DataDiffOptions,
    /// State for old file: table -> (pk_hash -> row_digest)
    old_state: AHashMap<String, TableState>,
    /// State for new file: table -> (pk_hash -> row_digest)
    new_state: AHashMap<String, TableState>,
    /// Total PK entries tracked
    total_pk_entries: usize,
    /// Whether global memory limit was exceeded
    global_truncated: bool,
    /// Current COPY context for PostgreSQL: (table_name, column_order)
    current_copy_context: Option<(String, Vec<String>)>,
    /// Warnings generated during diff
    warnings: Vec<DiffWarning>,
    /// Tables already warned about (to avoid duplicate warnings)
    warned_tables: AHashMap<String, ()>,
    /// Compiled ignore column patterns
    ignore_patterns: Vec<Pattern>,
    /// Cache of ignored column indices per table
    ignore_indices_cache: AHashMap<String, Vec<usize>>,
}

impl DataDiffer {
    /// Create a new data differ
    pub fn new(options: DataDiffOptions) -> Self {
        let ignore_patterns = parse_ignore_patterns(&options.ignore_columns);
        Self {
            options,
            old_state: AHashMap::new(),
            new_state: AHashMap::new(),
            total_pk_entries: 0,
            global_truncated: false,
            current_copy_context: None,
            warnings: Vec::new(),
            warned_tables: AHashMap::new(),
            ignore_patterns,
            ignore_indices_cache: AHashMap::new(),
        }
    }

    /// Get ignored column indices for a table
    fn get_ignore_indices(
        &mut self,
        table_name: &str,
        table_schema: &crate::schema::TableSchema,
    ) -> Vec<usize> {
        let table_lower = table_name.to_lowercase();
        if let Some(indices) = self.ignore_indices_cache.get(&table_lower) {
            return indices.clone();
        }

        // Get PK column indices
        let pk_indices: Vec<usize> = table_schema
            .primary_key
            .iter()
            .map(|col_id| col_id.0 as usize)
            .collect();

        let mut indices: Vec<usize> = Vec::new();
        for (i, col) in table_schema.columns.iter().enumerate() {
            if should_ignore_column(table_name, &col.name, &self.ignore_patterns) {
                // Warn if trying to ignore a PK column (but still allow it for non-PK uses)
                if pk_indices.contains(&i) && !self.warned_tables.contains_key(&table_lower) {
                    self.warnings.push(DiffWarning {
                        table: Some(table_name.to_string()),
                        message: format!(
                            "Ignoring primary key column '{}' may affect diff accuracy",
                            col.name
                        ),
                    });
                }
                indices.push(i);
            }
        }

        self.ignore_indices_cache
            .insert(table_lower, indices.clone());
        indices
    }

    /// Get effective PK column indices for a table, considering overrides
    /// Returns (indices, has_override, invalid_columns) tuple
    fn get_effective_pk_indices(
        &self,
        table_name: &str,
        table_schema: &crate::schema::TableSchema,
    ) -> (Vec<usize>, bool, Vec<String>) {
        if let Some(override_cols) = self.options.pk_overrides.get(&table_name.to_lowercase()) {
            let mut indices: Vec<usize> = Vec::new();
            let mut invalid_cols: Vec<String> = Vec::new();

            for col_name in override_cols {
                if let Some(idx) = table_schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                {
                    indices.push(idx);
                } else {
                    invalid_cols.push(col_name.clone());
                }
            }

            (indices, true, invalid_cols)
        } else {
            let indices: Vec<usize> = table_schema
                .primary_key
                .iter()
                .map(|col_id| col_id.0 as usize)
                .collect();
            (indices, false, Vec::new())
        }
    }

    /// Extract PK from all_values using the given column indices
    fn extract_pk_from_values(
        &self,
        all_values: &[mysql_insert::PkValue],
        pk_indices: &[usize],
    ) -> Option<smallvec::SmallVec<[mysql_insert::PkValue; 2]>> {
        if pk_indices.is_empty() {
            return None;
        }
        let mut pk_values: smallvec::SmallVec<[mysql_insert::PkValue; 2]> =
            smallvec::SmallVec::new();
        for &idx in pk_indices {
            if let Some(val) = all_values.get(idx) {
                if val.is_null() {
                    return None;
                }
                pk_values.push(val.clone());
            } else {
                return None;
            }
        }
        if pk_values.is_empty() {
            None
        } else {
            Some(pk_values)
        }
    }

    /// Scan a SQL file and accumulate PK/digest state
    #[allow(clippy::too_many_arguments)]
    pub fn scan_file(
        &mut self,
        path: &PathBuf,
        schema: &Schema,
        dialect: SqlDialect,
        is_old: bool,
        progress_fn: &Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
        byte_offset: u64,
        total_bytes: u64,
    ) -> anyhow::Result<()> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let compression = Compression::from_path(path);

        let reader: Box<dyn Read> = if let Some(ref cb) = progress_fn {
            let cb = Arc::clone(cb);
            let progress_reader = ProgressReader::new(file, move |bytes| {
                cb(byte_offset + bytes, total_bytes);
            });
            compression.wrap_reader(Box::new(progress_reader))
        } else {
            compression.wrap_reader(Box::new(file))
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        // Reset COPY context for this file scan
        self.current_copy_context = None;

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            // Handle PostgreSQL COPY data (separate statement from header)
            if dialect == SqlDialect::Postgres && stmt_type == StatementType::Unknown {
                // Check if this looks like COPY data (ends with \.)
                if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                    if let Some((ref copy_table, ref column_order)) =
                        self.current_copy_context.clone()
                    {
                        // Check table filter
                        if should_include_table(
                            copy_table,
                            &self.options.tables,
                            &self.options.exclude,
                        ) {
                            if let Some(table_schema) = schema.get_table(copy_table) {
                                let has_pk = !table_schema.primary_key.is_empty();
                                let has_pk_override = self
                                    .options
                                    .pk_overrides
                                    .contains_key(&copy_table.to_lowercase());
                                if has_pk || self.options.allow_no_pk || has_pk_override {
                                    self.process_copy_data(
                                        &stmt,
                                        copy_table,
                                        table_schema,
                                        column_order.clone(),
                                        is_old,
                                    )?;
                                } else if !self.warned_tables.contains_key(copy_table) {
                                    self.warned_tables.insert(copy_table.clone(), ());
                                    self.warnings.push(DiffWarning {
                                        table: Some(copy_table.clone()),
                                        message: "No primary key, data comparison skipped"
                                            .to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
                self.current_copy_context = None;
                continue;
            }

            if table_name.is_empty() {
                continue;
            }

            // Check table filter
            if !should_include_table(&table_name, &self.options.tables, &self.options.exclude) {
                continue;
            }

            // Get table schema for PK info
            let table_schema = match schema.get_table(&table_name) {
                Some(t) => t,
                None => continue,
            };

            // Handle tables without primary key (unless there's an override)
            let has_pk_override = self
                .options
                .pk_overrides
                .contains_key(&table_name.to_lowercase());
            if table_schema.primary_key.is_empty() && !self.options.allow_no_pk && !has_pk_override
            {
                // Emit warning once per table
                if !self.warned_tables.contains_key(&table_name) {
                    self.warned_tables.insert(table_name.clone(), ());
                    self.warnings.push(DiffWarning {
                        table: Some(table_name.clone()),
                        message: "No primary key, data comparison skipped".to_string(),
                    });
                }
                continue;
                // allow_no_pk is true - we'll use all columns as key (handled in process_*)
            }

            match stmt_type {
                StatementType::Insert => {
                    self.process_insert_statement(&stmt, &table_name, table_schema, is_old)?;
                }
                StatementType::Copy => {
                    // For PostgreSQL COPY, the data comes in the next statement
                    // Save context for processing the data statement
                    let header = String::from_utf8_lossy(&stmt);
                    let column_order = postgres_copy::parse_copy_columns(&header);
                    self.current_copy_context = Some((table_name.clone(), column_order));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Process an INSERT statement
    fn process_insert_statement(
        &mut self,
        stmt: &[u8],
        table_name: &str,
        table_schema: &crate::schema::TableSchema,
        is_old: bool,
    ) -> anyhow::Result<()> {
        let rows = mysql_insert::parse_mysql_insert_rows(stmt, table_schema)?;

        let (pk_indices, has_override, invalid_cols) =
            self.get_effective_pk_indices(table_name, table_schema);

        // Get ignore indices for this table
        let ignore_indices = self.get_ignore_indices(table_name, table_schema);

        // Warn about invalid override columns (once per table)
        if !invalid_cols.is_empty() && !self.warned_tables.contains_key(table_name) {
            self.warned_tables.insert(table_name.to_string(), ());
            self.warnings.push(DiffWarning {
                table: Some(table_name.to_string()),
                message: format!(
                    "Primary key override column(s) not found: {}",
                    invalid_cols.join(", ")
                ),
            });
        }

        for row in rows {
            let effective_pk = if has_override {
                self.extract_pk_from_values(&row.all_values, &pk_indices)
            } else {
                row.pk
            };
            self.record_row(
                table_name,
                &effective_pk,
                &row.all_values,
                is_old,
                &ignore_indices,
            );
        }

        Ok(())
    }

    /// Process PostgreSQL COPY data (the data lines after the COPY header)
    fn process_copy_data(
        &mut self,
        data_stmt: &[u8],
        table_name: &str,
        table_schema: &crate::schema::TableSchema,
        column_order: Vec<String>,
        is_old: bool,
    ) -> anyhow::Result<()> {
        // The data_stmt contains the raw COPY data lines (may have leading newline)
        // Strip leading whitespace/newlines
        let data = data_stmt
            .iter()
            .skip_while(|&&b| b == b'\n' || b == b'\r' || b == b' ' || b == b'\t')
            .cloned()
            .collect::<Vec<u8>>();

        if data.is_empty() {
            return Ok(());
        }

        let rows = postgres_copy::parse_postgres_copy_rows(&data, table_schema, column_order)?;

        let (pk_indices, has_override, invalid_cols) =
            self.get_effective_pk_indices(table_name, table_schema);

        // Get ignore indices for this table
        let ignore_indices = self.get_ignore_indices(table_name, table_schema);

        // Warn about invalid override columns (once per table)
        if !invalid_cols.is_empty() && !self.warned_tables.contains_key(table_name) {
            self.warned_tables.insert(table_name.to_string(), ());
            self.warnings.push(DiffWarning {
                table: Some(table_name.to_string()),
                message: format!(
                    "Primary key override column(s) not found: {}",
                    invalid_cols.join(", ")
                ),
            });
        }

        for row in rows {
            let effective_pk = if has_override {
                self.extract_pk_from_values(&row.all_values, &pk_indices)
            } else {
                row.pk
            };
            self.record_row(
                table_name,
                &effective_pk,
                &row.all_values,
                is_old,
                &ignore_indices,
            );
        }

        Ok(())
    }

    /// Record a row in the appropriate state map
    fn record_row(
        &mut self,
        table_name: &str,
        pk: &Option<smallvec::SmallVec<[mysql_insert::PkValue; 2]>>,
        all_values: &[mysql_insert::PkValue],
        is_old: bool,
        ignore_indices: &[usize],
    ) {
        if self.global_truncated {
            // Still count rows but don't track PKs
            let state = if is_old {
                self.old_state
                    .entry(table_name.to_string())
                    .or_insert_with(|| {
                        let mut s = TableState::new();
                        s.pk_to_digest = None;
                        s.truncated = true;
                        s
                    })
            } else {
                self.new_state
                    .entry(table_name.to_string())
                    .or_insert_with(|| {
                        let mut s = TableState::new();
                        s.pk_to_digest = None;
                        s.truncated = true;
                        s
                    })
            };
            state.row_count += 1;
            return;
        }

        let sample_size = self.options.sample_size;
        let state = if is_old {
            self.old_state
                .entry(table_name.to_string())
                .or_insert_with(|| {
                    if sample_size > 0 {
                        TableState::new_with_pk_strings()
                    } else {
                        TableState::new()
                    }
                })
        } else {
            self.new_state
                .entry(table_name.to_string())
                .or_insert_with(|| {
                    if sample_size > 0 {
                        TableState::new_with_pk_strings()
                    } else {
                        TableState::new()
                    }
                })
        };

        state.row_count += 1;

        // Check per-table limit
        if let Some(ref map) = state.pk_to_digest {
            if map.len() >= self.options.max_pk_entries_per_table {
                state.pk_to_digest = None;
                state.pk_strings = None;
                state.truncated = true;
                return;
            }
        }

        // Check global limit
        if self.total_pk_entries >= self.options.max_pk_entries_global {
            self.global_truncated = true;
            state.pk_to_digest = None;
            state.pk_strings = None;
            state.truncated = true;
            return;
        }

        // Record PK and digest
        if let Some(ref pk_values) = pk {
            if let Some(ref mut map) = state.pk_to_digest {
                let pk_hash = hash_pk_values(pk_values);
                let row_digest = if ignore_indices.is_empty() {
                    hash_row_digest(all_values)
                } else {
                    hash_row_digest_with_ignore(all_values, ignore_indices)
                };
                map.insert(pk_hash, row_digest);
                self.total_pk_entries += 1;

                // Also store PK string for sampling if enabled
                if let Some(ref mut pk_str_map) = state.pk_strings {
                    pk_str_map.insert(pk_hash, format_pk_value(pk_values));
                }
            }
        }
    }

    /// Compute the final diff from accumulated state
    pub fn compute_diff(self) -> (DataDiff, Vec<DiffWarning>) {
        let mut tables: HashMap<String, TableDataDiff> = HashMap::new();
        let sample_size = self.options.sample_size;

        // Get all table names from both states
        let mut all_tables: Vec<String> = self.old_state.keys().cloned().collect();
        for name in self.new_state.keys() {
            if !all_tables.contains(name) {
                all_tables.push(name.clone());
            }
        }

        for table_name in all_tables {
            let old_state = self.old_state.get(&table_name);
            let new_state = self.new_state.get(&table_name);

            let mut diff = TableDataDiff {
                old_row_count: old_state.map(|s| s.row_count).unwrap_or(0),
                new_row_count: new_state.map(|s| s.row_count).unwrap_or(0),
                truncated: old_state.map(|s| s.truncated).unwrap_or(false)
                    || new_state.map(|s| s.truncated).unwrap_or(false)
                    || self.global_truncated,
                ..Default::default()
            };

            // If we have full PK maps, compute detailed diff
            let old_map = old_state.and_then(|s| s.pk_to_digest.as_ref());
            let new_map = new_state.and_then(|s| s.pk_to_digest.as_ref());

            // Get PK string maps for sampling
            let old_pk_strings = old_state.and_then(|s| s.pk_strings.as_ref());
            let new_pk_strings = new_state.and_then(|s| s.pk_strings.as_ref());

            match (old_map, new_map) {
                (Some(old), Some(new)) => {
                    // Count added (in new but not old) and collect samples
                    for pk_hash in new.keys() {
                        if !old.contains_key(pk_hash) {
                            diff.added_count += 1;

                            // Collect sample PK strings
                            if sample_size > 0 && diff.sample_added_pks.len() < sample_size {
                                if let Some(pk_str) = new_pk_strings.and_then(|m| m.get(pk_hash)) {
                                    diff.sample_added_pks.push(pk_str.clone());
                                }
                            }
                        }
                    }

                    // Count removed (in old but not new) and modified (same PK, different digest)
                    for (pk_hash, old_digest) in old {
                        match new.get(pk_hash) {
                            None => {
                                diff.removed_count += 1;

                                // Collect sample PK strings
                                if sample_size > 0 && diff.sample_removed_pks.len() < sample_size {
                                    if let Some(pk_str) =
                                        old_pk_strings.and_then(|m| m.get(pk_hash))
                                    {
                                        diff.sample_removed_pks.push(pk_str.clone());
                                    }
                                }
                            }
                            Some(new_digest) => {
                                if old_digest != new_digest {
                                    diff.modified_count += 1;

                                    // Collect sample PK strings
                                    if sample_size > 0
                                        && diff.sample_modified_pks.len() < sample_size
                                    {
                                        if let Some(pk_str) =
                                            old_pk_strings.and_then(|m| m.get(pk_hash))
                                        {
                                            diff.sample_modified_pks.push(pk_str.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Truncated - can only report row count differences
                    if diff.new_row_count > diff.old_row_count {
                        diff.added_count = diff.new_row_count - diff.old_row_count;
                    } else if diff.old_row_count > diff.new_row_count {
                        diff.removed_count = diff.old_row_count - diff.new_row_count;
                    }
                }
            }

            // Only include tables with changes or both files had data
            if diff.old_row_count > 0
                || diff.new_row_count > 0
                || diff.added_count > 0
                || diff.removed_count > 0
                || diff.modified_count > 0
            {
                tables.insert(table_name, diff);
            }
        }

        (DataDiff { tables }, self.warnings)
    }
}
