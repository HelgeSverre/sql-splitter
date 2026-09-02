//! Batch manager for DuckDB Appender-based bulk loading.
//!
//! This module provides efficient batched insertion of rows into DuckDB
//! using the Appender API instead of individual INSERT statement execution.

use crate::parser::{ParsedValue, SqlDialect};
use crate::transform_common::quote_ident;
use ahash::AHashMap;
use anyhow::Result;
use duckdb::Connection;

use super::{is_missing_table_error, ImportStats};

/// Maximum rows to accumulate per batch before flushing
pub const MAX_ROWS_PER_BATCH: usize = 10_000;

/// A batch of rows for a single table
#[derive(Debug)]
pub struct InsertBatch {
    /// Target table name
    pub table: String,
    /// Source SQL dialect, used to map source-default schemas to DuckDB's
    /// default schema in generated batch inserts.
    pub dialect: SqlDialect,
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
    pub fn new(table: String, columns: Option<Vec<String>>, dialect: SqlDialect) -> Self {
        Self {
            table,
            dialect,
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

/// Batch key: (table_name, column_layout, source dialect).
/// Using Option<Vec<String>> for columns allows distinguishing between
/// different column orderings for the same table. The source dialect is part
/// of the key because it determines the DuckDB table-name mapping.
type BatchKey = (String, Option<Vec<String>>, SqlDialect);

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
        dialect: SqlDialect,
    ) -> Option<InsertBatch> {
        let row_count = rows.len();
        let key = (table.to_string(), columns.clone(), dialect);

        let batch = self
            .batches
            .entry(key)
            .or_insert_with(|| InsertBatch::new(table.to_string(), columns, dialect));

        batch.rows.extend(rows);
        batch.statements.push(original_sql);
        batch.rows_per_statement.push(row_count);

        // Check if we need to flush
        if batch.rows.len() >= self.max_rows_per_batch {
            // Take the batch out and return it
            let key = (table.to_string(), batch.columns.clone(), dialect);
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
            // `ParsedValue::Hex` carries the literal as written, `0x` prefix
            // included, so the digits are already hex -- re-encoding the bytes
            // would hex-encode the ASCII characters and turn 0x30 into
            // x'30783330'.
            format!("x'{}'", hex_digits(bytes))
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
    dialect: SqlDialect,
) -> String {
    if rows.is_empty() {
        return String::new();
    }

    // DuckDB loads PostgreSQL's `public` and SQL Server's standard system
    // schemas into its default schema. Match the DDL and direct-INSERT paths.
    let duckdb_table = duckdb_table_name(table, dialect);
    let mut sql = format!(
        "INSERT INTO {}",
        quote_ident(SqlDialect::Postgres, duckdb_table)
    );

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

    // Fastest path: hand the parsed values straight to DuckDB's Appender. The
    // batched-INSERT path below has to serialize every value back into SQL text
    // that DuckDB then re-parses -- profiling put ~75% of import time in
    // duckdb::ClientContext::CreatePreparedStatement doing exactly that. The
    // Appender skips the parser entirely. It is not always applicable, so it
    // reports false and leaves the batch untouched when it bails.
    match try_appender_insert(conn, batch, stats) {
        Ok(true) => {
            batch.clear();
            return Ok(());
        }
        Ok(false) => {} // not eligible, fall through to SQL
        Err(e) => {
            // The append was rolled back, so falling through cannot double-insert.
            if stats.warnings.len() < 100 {
                stats.warnings.push(format!(
                    "Appender failed for {}, using SQL: {}",
                    batch.table, e
                ));
            }
        }
    }

    // Try the fast path with batched INSERT
    match try_batch_insert(conn, batch, stats) {
        Ok(true) => {
            // Success via batched INSERT
            batch.clear();
            Ok(())
        }
        Ok(false) => {
            // Table doesn't exist: dropping the batch here would otherwise be
            // silent, unlike every other missing-table path in the loader
            // (COPY, fallback_execute, direct INSERT), which all warn.
            if stats.warnings.len() < 100 {
                stats.warnings.push(format!(
                    "Table {} does not exist, skipping {} row(s)",
                    batch.table,
                    batch.rows.len()
                ));
            }
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

/// Map the DuckDB table name the way the SQL path does, so both agree on which
/// table a batch targets.
fn duckdb_table_name(table: &str, dialect: SqlDialect) -> &str {
    match dialect {
        SqlDialect::Postgres => table.strip_prefix("public.").unwrap_or(table),
        SqlDialect::Mssql => ["dbo.", "master.", "tempdb.", "model.", "msdb."]
            .iter()
            .find_map(|prefix| table.strip_prefix(prefix))
            .unwrap_or(table),
        SqlDialect::MySql | SqlDialect::Sqlite => table,
    }
}

/// Convert a parsed value for the Appender. `Other` holds raw bytes for
/// decimals, floats and expressions like `NOW()`; binding those as text would
/// either lose exactness or be flatly wrong, so those batches take the SQL path
/// where DuckDB parses them into the column's own type.
fn appender_value(v: &ParsedValue) -> Option<duckdb::types::Value> {
    use duckdb::types::Value as V;
    Some(match v {
        ParsedValue::Null => V::Null,
        ParsedValue::Integer(i) => V::BigInt(*i),
        ParsedValue::BigInteger(i) => V::HugeInt(*i),
        ParsedValue::String { value } => V::Text(value.clone()),
        ParsedValue::Hex(b) => V::Blob(decode_hex_literal(b)?),
        ParsedValue::Other(_) => return None,
    })
}

/// The hex digits of a `0xABCD` literal, without the prefix.
fn hex_digits(raw: &[u8]) -> std::borrow::Cow<'_, str> {
    let digits = raw
        .strip_prefix(b"0x")
        .or_else(|| raw.strip_prefix(b"0X"))
        .unwrap_or(raw);
    String::from_utf8_lossy(digits)
}

/// Decode a `0xABCD` literal into the bytes it denotes. MySQL left-pads an odd
/// digit count, so `0xABC` is the two bytes `0x0A 0xBC`.
fn decode_hex_literal(raw: &[u8]) -> Option<Vec<u8>> {
    let digits = hex_digits(raw);
    let d = digits.as_bytes();
    if d.is_empty() || !d.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let mut out = Vec::with_capacity(d.len().div_ceil(2));
    let mut idx = 0;
    if d.len() % 2 == 1 {
        out.push((d[0] as char).to_digit(16)? as u8);
        idx = 1;
    }
    while idx < d.len() {
        let hi = (d[idx] as char).to_digit(16)? as u8;
        let lo = (d[idx + 1] as char).to_digit(16)? as u8;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Some(out)
}

/// Append a batch with DuckDB's Appender, bypassing SQL parsing entirely.
///
/// `Ok(false)` means the batch is not eligible and nothing was written, so the
/// caller should use the SQL path. `Err` means the append failed *and was rolled
/// back*, so the caller can also fall through without double-inserting.
///
/// Eligibility is deliberately strict. The Appender binds positionally across
/// every column of the table, so a batch whose column list differs from the
/// table's own order -- or omits a column -- would silently write values into
/// the wrong columns. That is worse than being slow, so anything short of an
/// exact match declines.
fn try_appender_insert(
    conn: &Connection,
    batch: &InsertBatch,
    stats: &mut ImportStats,
) -> Result<bool> {
    let table = duckdb_table_name(&batch.table, batch.dialect);

    // The table's real column order, straight from the catalog.
    let mut stmt = conn.prepare(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = ? ORDER BY ordinal_position",
    )?;
    let table_cols: Vec<String> = stmt
        .query_map([table], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<_, _>>()?;
    if table_cols.is_empty() {
        return Ok(false); // unknown table; let the SQL path produce the error
    }

    // An explicit column list must match the catalog exactly, in order.
    if let Some(cols) = &batch.columns {
        if cols.len() != table_cols.len()
            || !cols
                .iter()
                .zip(&table_cols)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
        {
            return Ok(false);
        }
    }

    // Every row must be full width and hold only bindable values.
    let width = table_cols.len();
    let mut values: Vec<duckdb::types::Value> = Vec::with_capacity(batch.rows.len() * width);
    for row in &batch.rows {
        if row.len() != width {
            return Ok(false);
        }
        for v in row {
            match appender_value(v) {
                Some(val) => values.push(val),
                None => return Ok(false),
            }
        }
    }

    // Wrap in a transaction so a type-cast failure part-way through leaves no
    // partially appended rows behind for the SQL fallback to duplicate.
    conn.execute_batch("BEGIN TRANSACTION")?;
    let appended = (|| -> duckdb::Result<()> {
        let mut app = conn.appender(table)?;
        let mut refs: Vec<&dyn duckdb::ToSql> = Vec::with_capacity(width);
        for chunk in values.chunks(width) {
            refs.clear();
            refs.extend(chunk.iter().map(|v| v as &dyn duckdb::ToSql));
            app.append_row(refs.as_slice())?;
        }
        app.flush()
    })();

    match appended {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            stats.insert_statements += batch.statements.len();
            stats.rows_inserted += batch.rows.len() as u64;
            Ok(true)
        }
        Err(e) => {
            conn.execute_batch("ROLLBACK")?;
            Err(e.into())
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
    let batch_sql = generate_batch_insert(&batch.table, &batch.columns, &batch.rows, batch.dialect);
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
            // Check if it's a "table not found" error
            if is_missing_table_error(&e) {
                return Ok(false);
            }
            Err(e.into())
        }
    }
}

/// Fallback: retry each row individually via freshly generated single-row
/// INSERTs.
///
/// The combined batch statement (already built from `batch.rows`, not the
/// original source text, by `try_batch_insert`) just failed. A multi-row
/// INSERT is atomic in DuckDB, so replaying the *original* source statements
/// verbatim -- each of which can itself hold hundreds of rows in one
/// `INSERT ... VALUES (...), (...), ...;` -- would drop every valid row
/// alongside the one bad row that caused the failure. Observed for real: a
/// single explicit NULL in a `NOT NULL` JSON column (`tags.slug`) took out
/// all 134 rows of that table's INSERT. Retrying row by row mirrors
/// `process_copy_rows` on the Postgres COPY path, which exists for the same
/// reason.
fn fallback_execute(conn: &Connection, batch: &InsertBatch, stats: &mut ImportStats) -> Result<()> {
    for row in &batch.rows {
        let insert_sql = generate_batch_insert(
            &batch.table,
            &batch.columns,
            std::slice::from_ref(row),
            batch.dialect,
        );
        match conn.execute(&insert_sql, []) {
            Ok(_) => {
                stats.insert_statements += 1;
                stats.rows_inserted += 1;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_literal_decodes_to_the_bytes_it_denotes() {
        // `ParsedValue::Hex` keeps the `0x` prefix, so a naive re-encode of the
        // payload turns 0x30 into the four ASCII bytes "0x30".
        assert_eq!(decode_hex_literal(b"0x30").unwrap(), vec![0x30]);
        assert_eq!(decode_hex_literal(b"0x623a303b").unwrap(), b"b:0;".to_vec());
        assert_eq!(decode_hex_literal(b"0XFF").unwrap(), vec![0xff]);
        // MySQL left-pads an odd digit count: 0xABC is 0x0A 0xBC.
        assert_eq!(decode_hex_literal(b"0xABC").unwrap(), vec![0x0a, 0xbc]);
        assert!(decode_hex_literal(b"0x").is_none());
        assert!(decode_hex_literal(b"0xZZ").is_none());
    }

    #[test]
    fn hex_literal_renders_as_sql_blob_without_double_encoding() {
        let v = ParsedValue::Hex(b"0x623a303b".to_vec());
        assert_eq!(format_value_for_sql(&v), "x'623a303b'");
    }

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
            SqlDialect::MySql,
        );
        assert!(result.is_none()); // Not ready yet
        assert!(mgr.has_pending());
    }

    #[test]
    fn test_batch_manager_flush_threshold() {
        let mut mgr = BatchManager::new(2);

        let rows1 = vec![vec![ParsedValue::Integer(1)]];
        let rows2 = vec![vec![ParsedValue::Integer(2)], vec![ParsedValue::Integer(3)]];

        mgr.queue_insert("test", None, rows1, "SQL1".to_string(), SqlDialect::MySql);
        let result = mgr.queue_insert("test", None, rows2, "SQL2".to_string(), SqlDialect::MySql);

        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.row_count(), 3);
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
        let sql = generate_batch_insert("users", &columns, &rows, SqlDialect::MySql);
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
        let sql = generate_batch_insert("test", &None, &rows, SqlDialect::MySql);
        assert_eq!(sql, "INSERT INTO \"test\" VALUES\n(1, 'test');");
    }

    #[test]
    fn batch_insert_quotes_qualified_table_components_separately() {
        let rows = vec![vec![ParsedValue::Integer(1)]];
        let sql = generate_batch_insert("tenant_a.users", &None, &rows, SqlDialect::Postgres);

        assert_eq!(sql, "INSERT INTO \"tenant_a\".\"users\" VALUES\n(1);");
    }

    #[test]
    fn batch_insert_maps_postgres_public_to_duckdb_default_schema() {
        let rows = vec![vec![ParsedValue::Integer(1)]];
        let sql = generate_batch_insert("public.users", &None, &rows, SqlDialect::Postgres);

        assert_eq!(sql, "INSERT INTO \"users\" VALUES\n(1);");
    }

    #[test]
    fn batch_insert_maps_mssql_dbo_to_duckdb_default_schema() {
        let rows = vec![vec![ParsedValue::Integer(1)]];
        let sql = generate_batch_insert("dbo.users", &None, &rows, SqlDialect::Mssql);

        assert_eq!(sql, "INSERT INTO \"users\" VALUES\n(1);");
    }

    #[test]
    fn flush_batch_warns_instead_of_silently_dropping_rows_for_a_missing_table() {
        // A dump with INSERTs for a table that never got a CREATE TABLE (e.g. a
        // partial mysqldump/export) must not vanish without a trace: the old
        // code cleared the batch and recorded nothing.
        let conn = Connection::open_in_memory().unwrap();
        let mut batch = InsertBatch::new("ghost_table".to_string(), None, SqlDialect::MySql);
        batch.rows.push(vec![ParsedValue::Integer(1)]);
        batch
            .statements
            .push("INSERT INTO ghost_table VALUES (1)".to_string());
        batch.rows_per_statement.push(1);

        let mut stats = ImportStats::default();
        let mut failed_tables = std::collections::HashSet::new();
        flush_batch(&conn, &mut batch, &mut stats, &mut failed_tables).unwrap();

        assert!(failed_tables.contains("ghost_table"));
        assert_eq!(stats.rows_inserted, 0);
        assert!(
            stats.warnings.iter().any(|w| w.contains("ghost_table")),
            "expected a warning naming the skipped table, got: {:?}",
            stats.warnings
        );
    }
}
