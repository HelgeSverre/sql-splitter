//! DuckDB query engine for SQL dump analytics.
//!
//! This module provides the ability to load SQL dumps into an embedded DuckDB
//! database and execute analytical queries on them.
//!
//! # Features
//!
//! - **Zero dependencies**: DuckDB is bundled and compiled into sql-splitter
//! - **Multi-dialect support**: MySQL, PostgreSQL, and SQLite dumps
//! - **Memory management**: Auto-switches to disk mode for large dumps
//! - **Caching**: Optional persistent cache for repeated queries
//!
//! # Example
//!
//! ```ignore
//! use sql_splitter::duckdb::{QueryEngine, QueryConfig, QueryResultFormatter, OutputFormat};
//! use std::path::Path;
//!
//! let config = QueryConfig::default();
//! let mut engine = QueryEngine::new(&config).unwrap();
//! engine.import_dump(Path::new("dump.sql")).unwrap();
//!
//! let result = engine.query("SELECT COUNT(*) FROM users").unwrap();
//! println!("{}", QueryResultFormatter::format(&result, OutputFormat::Table));
//! ```

mod batch;
mod cache;
mod loader;
mod output;
mod types;

// BatchManager is used internally by DumpLoader
#[allow(unused_imports)]
pub use batch::{flush_batch, BatchManager, InsertBatch, MAX_ROWS_PER_BATCH};
pub use cache::CacheManager;
pub use loader::DumpLoader;
pub use output::{OutputFormat, QueryResultFormatter};
#[allow(unused_imports)] // Used in tests
pub use types::TypeConverter;

use crate::parser::SqlDialect;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::{Path, PathBuf};

/// Configuration for the query engine
#[derive(Debug, Clone, Default)]
pub struct QueryConfig {
    /// Source SQL dialect (auto-detected if None)
    pub dialect: Option<SqlDialect>,
    /// Use disk-based storage instead of in-memory
    pub disk_mode: bool,
    /// Enable persistent caching
    pub cache_enabled: bool,
    /// Only import specific tables
    pub tables: Option<Vec<String>>,
    /// Memory limit for DuckDB (e.g., "4GB")
    pub memory_limit: Option<String>,
    /// Show progress during import
    pub progress: bool,
}

/// Statistics from dump import
#[derive(Debug, Default, Clone)]
pub struct ImportStats {
    /// Number of tables created
    pub tables_created: usize,
    /// Number of INSERT statements processed
    pub insert_statements: usize,
    /// Total rows inserted
    pub rows_inserted: u64,
    /// Statements skipped (unsupported)
    pub statements_skipped: usize,
    /// Warnings generated
    pub warnings: Vec<String>,
    /// Import duration in seconds
    pub duration_secs: f64,
}

impl std::fmt::Display for ImportStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} tables, {} rows imported in {:.2}s",
            self.tables_created, self.rows_inserted, self.duration_secs
        )
    }
}

/// Result of a query execution
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Column types (as strings)
    pub column_types: Vec<String>,
    /// Rows of data (each row is a vector of string values)
    pub rows: Vec<Vec<String>>,
    /// Query execution time in seconds
    pub execution_time_secs: f64,
}

impl QueryResult {
    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// The main query engine that wraps DuckDB
pub struct QueryEngine {
    conn: Connection,
    config: QueryConfig,
    import_stats: Option<ImportStats>,
    temp_db_path: Option<PathBuf>,
}

impl QueryEngine {
    /// Create a new query engine with the given configuration
    pub fn new(config: &QueryConfig) -> Result<Self> {
        let (conn, temp_db_path) = if config.disk_mode {
            let temp_dir = std::env::temp_dir();
            let temp_path = temp_dir.join(format!("sql-splitter-{}.duckdb", std::process::id()));
            let conn = Connection::open(&temp_path)
                .context("Failed to create disk-based DuckDB database")?;
            (conn, Some(temp_path))
        } else {
            let conn = Connection::open_in_memory()
                .context("Failed to create in-memory DuckDB database")?;
            (conn, None)
        };

        // Configure memory limit if specified
        if let Some(ref limit) = config.memory_limit {
            conn.execute(&format!("SET memory_limit = '{}'", limit), [])
                .context("Failed to set memory limit")?;
        }

        Ok(Self {
            conn,
            config: config.clone(),
            import_stats: None,
            temp_db_path,
        })
    }

    /// Create a query engine from a cached database file
    pub fn from_cache(cache_path: &Path, config: &QueryConfig) -> Result<Self> {
        let conn = Connection::open(cache_path).context("Failed to open cached DuckDB database")?;

        if let Some(ref limit) = config.memory_limit {
            conn.execute(&format!("SET memory_limit = '{}'", limit), [])
                .context("Failed to set memory limit")?;
        }

        Ok(Self {
            conn,
            config: config.clone(),
            import_stats: None,
            temp_db_path: None,
        })
    }

    /// Import a SQL dump file into the DuckDB database
    pub fn import_dump(&mut self, dump_path: &Path) -> Result<&ImportStats> {
        let loader = DumpLoader::new(&self.conn, &self.config);
        let stats = loader.load(dump_path)?;
        self.import_stats = Some(stats);
        // Safe: we just set import_stats to Some above
        Ok(self
            .import_stats
            .as_ref()
            .expect("import_stats was just set"))
    }

    /// Execute a query and return the results
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let start = std::time::Instant::now();

        let mut stmt = self
            .conn
            .prepare(sql)
            .with_context(|| format!("Failed to prepare query: {}", sql))?;

        // Execute the query and collect rows
        let mut rows_result = stmt
            .query([])
            .with_context(|| format!("Failed to execute query: {}", sql))?;

        // Collect all rows first
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut column_count = 0;

        while let Some(row) = rows_result.next()? {
            // Get column count from first row
            if column_count == 0 {
                column_count = row.as_ref().column_count();
            }

            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value: String = match row.get_ref(i) {
                    Ok(duckdb::types::ValueRef::Null) => "NULL".to_string(),
                    Ok(duckdb::types::ValueRef::Boolean(b)) => b.to_string(),
                    Ok(duckdb::types::ValueRef::TinyInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::SmallInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::Int(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::BigInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::HugeInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::UTinyInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::USmallInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::UInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::UBigInt(n)) => n.to_string(),
                    Ok(duckdb::types::ValueRef::Float(f)) => f.to_string(),
                    Ok(duckdb::types::ValueRef::Double(f)) => f.to_string(),
                    Ok(duckdb::types::ValueRef::Text(s)) => String::from_utf8_lossy(s).to_string(),
                    Ok(duckdb::types::ValueRef::Blob(b)) => {
                        format!("<blob {} bytes>", b.len())
                    }
                    Ok(duckdb::types::ValueRef::Decimal(d)) => d.to_string(),
                    Ok(duckdb::types::ValueRef::Timestamp(_, ts)) => {
                        // Convert timestamp to readable format
                        // DuckDB timestamps are microseconds since epoch
                        let secs = ts / 1_000_000;
                        let nanos = ((ts % 1_000_000) * 1000) as u32;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                            dt.format("%Y-%m-%d %H:%M:%S").to_string()
                        } else {
                            ts.to_string()
                        }
                    }
                    Ok(duckdb::types::ValueRef::Date32(days)) => {
                        // Days since epoch (1970-01-01)
                        if let Some(date) = chrono::NaiveDate::from_num_days_from_ce_opt(
                            719163 + days, // 719163 = days from 0001-01-01 to 1970-01-01
                        ) {
                            date.format("%Y-%m-%d").to_string()
                        } else {
                            days.to_string()
                        }
                    }
                    Ok(duckdb::types::ValueRef::Time64(_, micros)) => {
                        let secs = (micros / 1_000_000) as u32;
                        let nanos = ((micros % 1_000_000) * 1000) as u32;
                        if let Some(time) =
                            chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
                        {
                            time.format("%H:%M:%S").to_string()
                        } else {
                            micros.to_string()
                        }
                    }
                    Ok(other) => format!("{:?}", other),
                    Err(_) => "ERROR".to_string(),
                };
                values.push(value);
            }
            rows.push(values);
        }

        // Drop the rows iterator to release the mutable borrow
        drop(rows_result);

        // Now get column info from the statement
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| {
                stmt.column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| format!("col{}", i))
            })
            .collect();

        // Get column types
        let column_types: Vec<String> = (0..column_count)
            .map(|i| format!("{:?}", stmt.column_type(i)))
            .collect();

        Ok(QueryResult {
            columns,
            column_types,
            rows,
            execution_time_secs: start.elapsed().as_secs_f64(),
        })
    }

    /// Execute a statement that doesn't return results (e.g., CREATE, INSERT)
    pub fn execute(&self, sql: &str) -> Result<usize> {
        self.conn
            .execute(sql, [])
            .with_context(|| format!("Failed to execute: {}", sql))
    }

    /// Get list of tables in the database
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let result = self.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")?;
        Ok(result.rows.into_iter().map(|r| r[0].clone()).collect())
    }

    /// Get schema for a specific table
    pub fn describe_table(&self, table: &str) -> Result<QueryResult> {
        self.query(&format!("DESCRIBE \"{}\"", table))
    }

    /// Get import statistics (if a dump was imported)
    pub fn import_stats(&self) -> Option<&ImportStats> {
        self.import_stats.as_ref()
    }

    /// Get the underlying DuckDB connection (for advanced use)
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Save the current database to a file (for caching)
    pub fn save_to_file(&self, path: &Path) -> Result<()> {
        self.conn
            .execute(&format!("EXPORT DATABASE '{}'", path.display()), [])
            .context("Failed to export database")?;
        Ok(())
    }
}

impl Drop for QueryEngine {
    fn drop(&mut self) {
        // Clean up temporary database file if we created one
        if let Some(ref path) = self.temp_db_path {
            let _ = std::fs::remove_file(path);
            // Also try to remove the .wal file if it exists
            let wal_path = path.with_extension("duckdb.wal");
            let _ = std::fs::remove_file(wal_path);
        }
    }
}

/// Threshold for automatic disk mode (2 GB)
pub const DISK_MODE_THRESHOLD: u64 = 2 * 1024 * 1024 * 1024;

/// Determine if disk mode should be used based on file size
pub fn should_use_disk_mode(file_size: u64) -> bool {
    file_size > DISK_MODE_THRESHOLD
}
