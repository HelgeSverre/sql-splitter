//! Diff module for comparing two SQL dumps.
//!
//! This module provides:
//! - Schema comparison (tables added/removed/modified, columns, PKs, FKs)
//! - Data comparison (row counts: added/removed/modified)
//! - Memory-bounded operation using PK hashing
//! - Multiple output formats (text, json, sql)

mod data;
mod output;
mod schema;

pub use data::*;
pub use output::*;
pub use schema::*;

use crate::parser::{determine_buffer_size, Parser, SqlDialect, StatementType};
use crate::progress::ProgressReader;
use crate::schema::{Schema, SchemaBuilder};
use crate::splitter::Compression;
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for the diff operation
#[derive(Debug, Clone)]
pub struct DiffConfig {
    /// Path to the "old" SQL file
    pub old_path: PathBuf,
    /// Path to the "new" SQL file
    pub new_path: PathBuf,
    /// SQL dialect (auto-detected if None)
    pub dialect: Option<SqlDialect>,
    /// Only compare schema, skip data
    pub schema_only: bool,
    /// Only compare data, skip schema
    pub data_only: bool,
    /// Tables to include (if empty, include all)
    pub tables: Vec<String>,
    /// Tables to exclude
    pub exclude: Vec<String>,
    /// Output format
    pub format: DiffOutputFormat,
    /// Show verbose row-level details
    pub verbose: bool,
    /// Show progress bar
    pub progress: bool,
    /// Maximum PK entries to track globally
    pub max_pk_entries: usize,
    /// Don't skip tables without PK, use all columns as key
    pub allow_no_pk: bool,
    /// Ignore column order when comparing schemas
    pub ignore_column_order: bool,
    /// Primary key overrides: table name -> column names
    pub pk_overrides: std::collections::HashMap<String, Vec<String>>,
    /// Column patterns to ignore (glob format: table.column)
    pub ignore_columns: Vec<String>,
}

impl Default for DiffConfig {
    fn default() -> Self {
        Self {
            old_path: PathBuf::new(),
            new_path: PathBuf::new(),
            dialect: None,
            schema_only: false,
            data_only: false,
            tables: Vec::new(),
            exclude: Vec::new(),
            format: DiffOutputFormat::Text,
            verbose: false,
            progress: false,
            max_pk_entries: 10_000_000, // 10M entries ~= 160MB
            allow_no_pk: false,
            ignore_column_order: false,
            pk_overrides: std::collections::HashMap::new(),
            ignore_columns: Vec::new(),
        }
    }
}

/// Output format for diff results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DiffOutputFormat {
    #[default]
    Text,
    Json,
    Sql,
}

impl std::str::FromStr for DiffOutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            "sql" => Ok(Self::Sql),
            _ => Err(format!("Unknown format: {}. Use: text, json, sql", s)),
        }
    }
}

/// A warning generated during diff operation
#[derive(Debug, Serialize, Clone)]
pub struct DiffWarning {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub message: String,
}

/// Complete diff result
#[derive(Debug, Serialize)]
pub struct DiffResult {
    /// Schema differences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDiff>,
    /// Data differences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<DataDiff>,
    /// Warnings generated during diff
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<DiffWarning>,
    /// Summary statistics
    pub summary: DiffSummary,
}

/// Summary of differences
#[derive(Debug, Serialize)]
pub struct DiffSummary {
    /// Number of tables added
    pub tables_added: usize,
    /// Number of tables removed
    pub tables_removed: usize,
    /// Number of tables modified (schema or data)
    pub tables_modified: usize,
    /// Total rows added across all tables
    pub rows_added: u64,
    /// Total rows removed across all tables
    pub rows_removed: u64,
    /// Total rows modified across all tables
    pub rows_modified: u64,
    /// Whether any data was truncated due to memory limits
    pub truncated: bool,
}

/// Main differ engine
pub struct Differ {
    config: DiffConfig,
    dialect: SqlDialect,
    progress_fn: Option<Arc<dyn Fn(u64, u64) + Send + Sync>>,
}

impl Differ {
    /// Create a new differ with the given configuration
    pub fn new(config: DiffConfig) -> Self {
        Self {
            dialect: config.dialect.unwrap_or(SqlDialect::MySql),
            config,
            progress_fn: None,
        }
    }

    /// Set a progress callback (receives current bytes, total bytes)
    pub fn with_progress<F>(mut self, f: F) -> Self
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        self.progress_fn = Some(Arc::new(f));
        self
    }

    /// Run the diff operation
    pub fn diff(self) -> anyhow::Result<DiffResult> {
        // Calculate total bytes for progress (4 passes max: 2 schema + 2 data)
        let old_size = std::fs::metadata(&self.config.old_path)?.len();
        let new_size = std::fs::metadata(&self.config.new_path)?.len();
        let total_bytes = if self.config.schema_only || self.config.data_only {
            old_size + new_size
        } else {
            (old_size + new_size) * 2 // Schema pass + data pass for each file
        };

        // Pass 0: Extract schemas from both files
        let (old_schema, new_schema) = if !self.config.data_only {
            let old = self.extract_schema(&self.config.old_path.clone(), 0, total_bytes)?;
            let new = self.extract_schema(&self.config.new_path.clone(), old_size, total_bytes)?;
            (Some(old), Some(new))
        } else {
            // Even for data-only, we need schema for PK info
            let old = self.extract_schema(&self.config.old_path.clone(), 0, total_bytes)?;
            let new = self.extract_schema(&self.config.new_path.clone(), old_size, total_bytes)?;
            (Some(old), Some(new))
        };

        // Schema comparison
        let schema_diff = if !self.config.data_only {
            old_schema
                .as_ref()
                .zip(new_schema.as_ref())
                .map(|(old, new)| compare_schemas(old, new, &self.config))
        } else {
            None
        };

        // Data comparison
        let (data_diff, warnings) = if !self.config.schema_only {
            let old_schema = old_schema
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Schema required for data comparison"))?;
            let new_schema = new_schema
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Schema required for data comparison"))?;

            let base_offset = if self.config.data_only {
                0
            } else {
                old_size + new_size
            };

            let (data, warns) =
                self.compare_data(old_schema, new_schema, base_offset, total_bytes)?;
            (Some(data), warns)
        } else {
            (None, Vec::new())
        };

        // Build summary
        let summary = self.build_summary(&schema_diff, &data_diff);

        Ok(DiffResult {
            schema: schema_diff,
            data: data_diff,
            warnings,
            summary,
        })
    }

    /// Extract schema from a SQL file
    fn extract_schema(
        &self,
        path: &PathBuf,
        byte_offset: u64,
        total_bytes: u64,
    ) -> anyhow::Result<Schema> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let compression = Compression::from_path(path);

        let reader: Box<dyn Read> = if let Some(ref cb) = self.progress_fn {
            let cb = Arc::clone(cb);
            let progress_reader = ProgressReader::new(file, move |bytes| {
                cb(byte_offset + bytes, total_bytes);
            });
            compression.wrap_reader(Box::new(progress_reader))?
        } else {
            compression.wrap_reader(Box::new(file))?
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, self.dialect);
        let mut builder = SchemaBuilder::new();

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, _table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, self.dialect);

            match stmt_type {
                StatementType::CreateTable => {
                    if let Ok(stmt_str) = std::str::from_utf8(&stmt) {
                        builder.parse_create_table(stmt_str);
                    }
                }
                StatementType::AlterTable => {
                    if let Ok(stmt_str) = std::str::from_utf8(&stmt) {
                        builder.parse_alter_table(stmt_str);
                    }
                }
                StatementType::CreateIndex => {
                    if let Ok(stmt_str) = std::str::from_utf8(&stmt) {
                        builder.parse_create_index(stmt_str);
                    }
                }
                _ => {}
            }
        }

        Ok(builder.build())
    }

    /// Compare data between two SQL files
    fn compare_data(
        &self,
        old_schema: &Schema,
        new_schema: &Schema,
        byte_offset: u64,
        total_bytes: u64,
    ) -> anyhow::Result<(DataDiff, Vec<DiffWarning>)> {
        let mut data_differ = DataDiffer::new(DataDiffOptions {
            max_pk_entries_global: self.config.max_pk_entries,
            max_pk_entries_per_table: self.config.max_pk_entries / 2,
            sample_size: if self.config.verbose { 100 } else { 0 },
            tables: self.config.tables.clone(),
            exclude: self.config.exclude.clone(),
            allow_no_pk: self.config.allow_no_pk,
            pk_overrides: self.config.pk_overrides.clone(),
            ignore_columns: self.config.ignore_columns.clone(),
        });

        let old_size = std::fs::metadata(&self.config.old_path)?.len();

        // Pass 1: Scan old file
        data_differ.scan_file(
            &self.config.old_path,
            old_schema,
            self.dialect,
            true, // is_old
            &self.progress_fn,
            byte_offset,
            total_bytes,
        )?;

        // Pass 2: Scan new file
        data_differ.scan_file(
            &self.config.new_path,
            new_schema,
            self.dialect,
            false, // is_old
            &self.progress_fn,
            byte_offset + old_size,
            total_bytes,
        )?;

        Ok(data_differ.compute_diff())
    }

    /// Build summary from diff results
    fn build_summary(
        &self,
        schema_diff: &Option<SchemaDiff>,
        data_diff: &Option<DataDiff>,
    ) -> DiffSummary {
        let (tables_added, tables_removed, schema_modified) = schema_diff
            .as_ref()
            .map(|s| {
                (
                    s.tables_added.len(),
                    s.tables_removed.len(),
                    s.tables_modified.len(),
                )
            })
            .unwrap_or((0, 0, 0));

        let (rows_added, rows_removed, rows_modified, data_modified, truncated) = data_diff
            .as_ref()
            .map(|d| {
                let mut added = 0u64;
                let mut removed = 0u64;
                let mut modified = 0u64;
                let mut tables_with_changes = 0usize;
                let mut any_truncated = false;

                for table_diff in d.tables.values() {
                    added += table_diff.added_count;
                    removed += table_diff.removed_count;
                    modified += table_diff.modified_count;
                    if table_diff.added_count > 0
                        || table_diff.removed_count > 0
                        || table_diff.modified_count > 0
                    {
                        tables_with_changes += 1;
                    }
                    if table_diff.truncated {
                        any_truncated = true;
                    }
                }

                (added, removed, modified, tables_with_changes, any_truncated)
            })
            .unwrap_or((0, 0, 0, 0, false));

        DiffSummary {
            tables_added,
            tables_removed,
            tables_modified: schema_modified.max(data_modified),
            rows_added,
            rows_removed,
            rows_modified,
            truncated,
        }
    }
}

/// Check if a table should be included based on filter config
pub fn should_include_table(table_name: &str, tables: &[String], exclude: &[String]) -> bool {
    // If include list is specified, table must be in it
    if !tables.is_empty() {
        let name_lower = table_name.to_lowercase();
        if !tables.iter().any(|t| t.to_lowercase() == name_lower) {
            return false;
        }
    }

    // If table is in exclude list, skip it
    if !exclude.is_empty() {
        let name_lower = table_name.to_lowercase();
        if exclude.iter().any(|t| t.to_lowercase() == name_lower) {
            return false;
        }
    }

    true
}
