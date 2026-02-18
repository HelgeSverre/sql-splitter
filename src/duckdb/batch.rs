//! Batch manager for DuckDB Appender-based bulk loading.
//!
//! This module provides efficient batched insertion of rows into DuckDB
//! using the Appender API instead of individual INSERT statement execution.

use crate::parser::ParsedValue;
use ahash::AHashMap;
use anyhow::Result;
use duckdb::Connection;

use super::ImportStats;

/// Maximum rows to accumulate per batch before flushing
pub const MAX_ROWS_PER_BATCH: usize = 10_000;

/// A batch of rows for a single table
#[derive(Debug)]
pub struct InsertBatch {
    /// Target table name
    pub table: String,
    /// Column list if explicitly specified
    pub columns: Option<Vec<String>>,
    /// Accumulated rows (each row is a Vec of ParsedValue)
    pub rows: Vec<Vec<ParsedValue>>,
    /// Original SQL statements for fallback execution
    pub statements: Vec<String>,
    /// Number of rows contributed by each statement
    pub rows_per_statement: Vec<usize>,
}

impl InsertBatch {
    /// Create a new batch for a table
    pub fn new(table: String, columns: Option<Vec<String>>) -> Self {
        Self {
            table,
            columns,
            rows: Vec::new(),
            statements: Vec::new(),
            rows_per_statement: Vec::new(),
        }
    }

    /// Total number of rows in batch
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Clear the batch
    pub fn clear(&mut self) {
        self.rows.clear();
        self.statements.clear();
        self.rows_per_statement.clear();
    }
}

/// Batch key: (table_name, column_layout)
/// Using Option<Vec<String>> for columns allows distinguishing between
/// different column orderings for the same table.
type BatchKey = (String, Option<Vec<String>>);

/// Manages batched INSERT operations for multiple tables
pub struct BatchManager {
    /// Active batches keyed by (table, columns)
    batches: AHashMap<BatchKey, InsertBatch>,
    /// Maximum rows per batch
    max_rows_per_batch: usize,
}

impl BatchManager {
    /// Create a new batch manager
    pub fn new(max_rows_per_batch: usize) -> Self {
        Self {
            batches: AHashMap::new(),
            max_rows_per_batch,
        }
    }

    /// Queue rows for insertion, returning a batch if it's ready to flush
    pub fn queue_insert(
        &mut self,
        table: &str,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<ParsedValue>>,
        original_sql: String,
    ) -> Option<InsertBatch> {
        let row_count = rows.len();
        let key = (table.to_string(), columns.clone());

        let batch = self
            .batches
            .entry(key)
            .or_insert_with(|| InsertBatch::new(table.to_string(), columns));

        batch.rows.extend(rows);
        batch.statements.push(original_sql);
        batch.rows_per_statement.push(row_count);

        // Check if we need to flush
        if batch.rows.len() >= self.max_rows_per_batch {
            // Take the batch out and return it
            let key = (table.to_string(), batch.columns.clone());
            self.batches.remove(&key)
        } else {
            None
        }
    }

    /// Get any batches that are ready to flush
    pub fn get_ready_batches(&mut self) -> Vec<InsertBatch> {
        let mut ready = Vec::new();
        let mut to_remove = Vec::new();

        for (key, batch) in &self.batches {
            if batch.rows.len() >= self.max_rows_per_batch {
                to_remove.push(key.clone());
            }
        }

        for key in to_remove {
            if let Some(batch) = self.batches.remove(&key) {
                ready.push(batch);
            }
        }

        ready
    }

    /// Flush all remaining batches
    pub fn drain_all(&mut self) -> Vec<InsertBatch> {
        self.batches.drain().map(|(_, batch)| batch).collect()
    }

    /// Check if there are any pending batches
    pub fn has_pending(&self) -> bool {
        !self.batches.is_empty()
    }
}

/// Format a ParsedValue for SQL insertion
fn format_value_for_sql(value: &ParsedValue) -> String {
    match value {
        ParsedValue::Null => "NULL".to_string(),
        ParsedValue::Integer(n) => n.to_string(),
        ParsedValue::BigInteger(n) => n.to_string(),
        ParsedValue::String { value } => {
            // Escape single quotes by doubling them (SQL standard)
            let escaped = value.replace('\'', "''");
            format!("'{}'", escaped)
        }
        ParsedValue::Hex(bytes) => {
            // Convert to hex string for DuckDB
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            format!("x'{}'", hex)
        }
        ParsedValue::Other(raw) => {
            let s = String::from_utf8_lossy(raw);
            // Try to parse as float
            if s.parse::<f64>().is_ok() {
                s.to_string()
            } else {
                // Treat as text
                let escaped = s.replace('\'', "''");
                format!("'{}'", escaped)
            }
        }
    }
}

/// Generate a batched INSERT statement from parsed values
fn generate_batch_insert(
    table: &str,
    columns: &Option<Vec<String>>,
    rows: &[Vec<ParsedValue>],
) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut sql = format!("INSERT INTO \"{}\"", table);

    // Add column list if specified
    if let Some(cols) = columns {
        sql.push_str(" (");
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('"');
            sql.push_str(col);
            sql.push('"');
        }
        sql.push(')');
    }

    sql.push_str(" VALUES\n");

    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(",\n");
        }
        sql.push('(');
        for (j, value) in row.iter().enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format_value_for_sql(value));
        }
        sql.push(')');
    }
    sql.push(';');

    sql
}

/// Flush a batch using DuckDB's Appender API with transactional fallback
pub fn flush_batch(
    conn: &Connection,
    batch: &mut InsertBatch,
    stats: &mut ImportStats,
    failed_tables: &mut std::collections::HashSet<String>,
) -> Result<()> {
    if batch.rows.is_empty() {
        return Ok(());
    }

    // Skip tables we know don't exist
    if failed_tables.contains(&batch.table) {
        batch.clear();
        return Ok(());
    }

    // Try the fast path with batched INSERT
    match try_batch_insert(conn, batch, stats) {
        Ok(true) => {
            // Success via batched INSERT
            batch.clear();
            Ok(())
        }
        Ok(false) => {
            // Table doesn't exist or other non-recoverable error
            failed_tables.insert(batch.table.clone());
            batch.clear();
            Ok(())
        }
        Err(_) => {
            // Batched INSERT failed (constraint violation, type mismatch, etc.)
            // Fall back to per-statement execution
            fallback_execute(conn, batch, stats)?;
            batch.clear();
            Ok(())
        }
    }
}

/// Try to insert using batched SQL execution, returns Ok(true) on success,
/// Ok(false) if table doesn't exist, Err on constraint/type errors
fn try_batch_insert(
    conn: &Connection,
    batch: &InsertBatch,
    stats: &mut ImportStats,
) -> Result<bool> {
    // Generate a single batched INSERT statement
    let batch_sql = generate_batch_insert(&batch.table, &batch.columns, &batch.rows);
    if batch_sql.is_empty() {
        return Ok(true);
    }

    // Execute the batched INSERT (within the loader's transaction context)
    match conn.execute(&batch_sql, []) {
        Ok(_) => {
            stats.insert_statements += batch.statements.len();
            stats.rows_inserted += batch.rows.len() as u64;
            Ok(true)
        }
        Err(e) => {
            let err_str = e.to_string();
            // Check if it's a "table not found" error
            if err_str.contains("does not exist") || err_str.contains("not found") {
                return Ok(false);
            }
            Err(e.into())
        }
    }
}

/// Fallback: execute original SQL statements one by one
fn fallback_execute(conn: &Connection, batch: &InsertBatch, stats: &mut ImportStats) -> Result<()> {
    for stmt in &batch.statements {
        match conn.execute(stmt, []) {
            Ok(_) => {
                stats.insert_statements += 1;
                stats.rows_inserted += count_insert_rows(stmt);
            }
            Err(e) => {
                if stats.warnings.len() < 100 {
                    stats.warnings.push(format!(
                        "Failed INSERT for {} in fallback: {}",
                        batch.table, e
                    ));
                }
                stats.statements_skipped += 1;
            }
        }
    }
    Ok(())
}

/// Count rows in an INSERT statement (simple heuristic)
fn count_insert_rows(sql: &str) -> u64 {
    if let Some(values_pos) = sql.to_uppercase().find("VALUES") {
        let after_values = &sql[values_pos + 6..];
        let mut count = 0u64;
        let mut depth: i32 = 0;
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
                    depth = depth.saturating_sub(1);
                }
            }
            prev_char = c;
        }
        count
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_manager_queue() {
        let mut mgr = BatchManager::new(100);

        let rows = vec![vec![
            ParsedValue::Integer(1),
            ParsedValue::String {
                value: "test".to_string(),
            },
        ]];

        let result = mgr.queue_insert(
            "users",
            None,
            rows,
            "INSERT INTO users VALUES (1, 'test')".to_string(),
        );
        assert!(result.is_none()); // Not ready yet
        assert!(mgr.has_pending());
    }

    #[test]
    fn test_batch_manager_flush_threshold() {
        let mut mgr = BatchManager::new(2);

        let rows1 = vec![vec![ParsedValue::Integer(1)]];
        let rows2 = vec![vec![ParsedValue::Integer(2)], vec![ParsedValue::Integer(3)]];

        mgr.queue_insert("test", None, rows1, "SQL1".to_string());
        let result = mgr.queue_insert("test", None, rows2, "SQL2".to_string());

        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.row_count(), 3);
    }

    #[test]
    fn test_count_insert_rows() {
        assert_eq!(count_insert_rows("INSERT INTO t VALUES (1)"), 1);
        assert_eq!(count_insert_rows("INSERT INTO t VALUES (1), (2), (3)"), 3);
        assert_eq!(
            count_insert_rows("INSERT INTO t VALUES (1, 'a(b)'), (2, 'c')"),
            2
        );
    }

    #[test]
    fn test_generate_batch_insert_with_columns() {
        let rows = vec![
            vec![
                ParsedValue::String {
                    value: "alice".to_string(),
                },
                ParsedValue::Integer(1),
            ],
            vec![
                ParsedValue::String {
                    value: "bob".to_string(),
                },
                ParsedValue::Integer(2),
            ],
        ];
        let columns = Some(vec!["name".to_string(), "id".to_string()]);
        let sql = generate_batch_insert("users", &columns, &rows);
        assert!(sql.contains("INSERT INTO \"users\" (\"name\", \"id\") VALUES"));
        assert!(sql.contains("'alice'"));
        assert!(sql.contains("'bob'"));
    }

    #[test]
    fn test_generate_batch_insert_without_columns() {
        let rows = vec![vec![
            ParsedValue::Integer(1),
            ParsedValue::String {
                value: "test".to_string(),
            },
        ]];
        let sql = generate_batch_insert("test", &None, &rows);
        assert_eq!(sql, "INSERT INTO \"test\" VALUES\n(1, 'test');");
    }
}
