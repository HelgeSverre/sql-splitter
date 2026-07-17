//! Streams a compiled generation run to dialect-correct SQL.
//!
//! [`SqlRenderer`] is a [`RowSink`]: [`crate::generate::GenerationEngine::run`]
//! drives it one table at a time, and it writes DDL (unless `data_only`) plus
//! batched `INSERT`/`COPY` statements (unless `schema_only`) straight to a
//! [`BufWriter`], reusing one row buffer per active table so a long
//! generation run never allocates a formatted string per cell or per row —
//! every value is written through a [`std::fmt::Display`] wrapper straight
//! into the destination buffer. See [`super::ddl`] for `CREATE TABLE`
//! rendering.

use std::fmt::{self, Write as _};
use std::io::{self, BufWriter, Write};

use crate::convert::{ConvertWarning, WarningCollector};
use crate::generate::{GenerateError, GeneratedRow, GeneratedValue, PlannedTable, RowSink};
use crate::parser::SqlDialect;
use crate::synthetic::OutputMode;
use crate::transform_common::quote_ident;

use super::ddl;
use super::row_batch::RowBatch;
use super::sql_string::SqlString;

/// Rendering knobs for [`SqlRenderer`], independent of the compiled
/// [`crate::generate::GenerationPlan`] so a caller (the CLI, or a test) can
/// render the same plan under several target dialects.
#[derive(Debug, Clone, Copy)]
pub struct RenderOptions {
    /// The dialect to render SQL for.
    pub dialect: SqlDialect,
    /// The dialect the source schema's types/DDL were captured in, if known.
    /// `None` when the model has no `source:` block (e.g. hand-authored).
    pub source_dialect: Option<SqlDialect>,
    /// Whether to render DDL, rows, or both.
    pub mode: OutputMode,
    /// Force multi-row `INSERT` for PostgreSQL instead of `COPY`.
    pub no_copy: bool,
    /// Rows per `INSERT`/`COPY` batch.
    pub batch_size: usize,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            dialect: SqlDialect::MySql,
            source_dialect: None,
            mode: OutputMode::SchemaAndData,
            no_copy: false,
            batch_size: 1000,
        }
    }
}

/// Per-table render state, live between `begin_table` and `end_table`.
struct TableState {
    quoted_table: String,
    /// Whether this run renders row data at all (`false` under `schema_only`).
    data_enabled: bool,
    /// Whether rows render as PostgreSQL `COPY` rather than `INSERT`.
    use_copy: bool,
    /// Rows per batch/statement.
    batch_size: usize,
    /// Column indices to render, in schema order, excluding every column
    /// that renders `DEFAULT` for every row. `None` until the first row
    /// (a `DEFAULT` column is a per-column, not per-row, decision - see
    /// `SqlRenderer::classify_columns` - but the renderer only learns it by
    /// inspecting the first generated row).
    insert_columns: Option<Vec<usize>>,
    /// Buffered `INSERT` row tuples, reused across batches.
    row_batch: RowBatch,
    /// Buffered `COPY` lines, reused across batches (`RowBatch` joins with
    /// `,\n`, which is INSERT-tuple shaped; `COPY` wants one bare line per
    /// row, so this is a plain reused buffer instead).
    copy_batch: String,
    /// Rows buffered in `copy_batch` since the last flush.
    copy_rows: usize,
    /// Whether the classified `insert_columns` carry an explicit value for an
    /// `IDENTITY`/`GENERATED ALWAYS AS IDENTITY` column — the normal case for a
    /// primary key referenced by generated foreign keys. Such a value is
    /// rejected by MSSQL/PostgreSQL unless the load is wrapped
    /// (`SET IDENTITY_INSERT ... ON`/`OVERRIDING SYSTEM VALUE`). `None` until the
    /// first row classifies the columns.
    identity_insert: Option<bool>,
    /// Whether a `SET IDENTITY_INSERT <t> ON` has been emitted for this table
    /// (MSSQL) and therefore an `OFF` must close it out in `end_table`.
    identity_on: bool,
}

/// Streams generated rows to dialect-correct SQL as a [`RowSink`].
pub struct SqlRenderer<W: Write> {
    writer: BufWriter<W>,
    options: RenderOptions,
    warnings: WarningCollector,
    table: Option<TableState>,
}

impl<W: Write> SqlRenderer<W> {
    /// Wrap `writer` and render under `options`.
    pub fn new(writer: W, options: RenderOptions) -> Self {
        Self {
            writer: BufWriter::new(writer),
            options,
            warnings: WarningCollector::new(),
            table: None,
        }
    }

    /// Warnings collected while rendering DDL (e.g. a lossy cross-dialect
    /// type conversion).
    pub fn warnings(&self) -> &[ConvertWarning] {
        self.warnings.warnings()
    }

    /// Flush and unwrap the underlying writer.
    pub fn finish(mut self) -> Result<W, GenerateError> {
        self.writer.flush().map_err(io_err)?;
        self.writer
            .into_inner()
            .map_err(|err| io_err(err.into_error()))
    }

    /// Render `table`'s DDL: the raw `create_statement` when the render
    /// target matches the source dialect (see [`ddl::should_preserve_raw_ddl`]),
    /// otherwise a normalized `CREATE TABLE` built from its [`PortableSchema`]
    /// via [`ddl::render_create_table`].
    ///
    /// [`PortableSchema`]: crate::synthetic::schema::PortableSchema
    fn render_ddl(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        let dialect = self.options.dialect;
        if ddl::should_preserve_raw_ddl(&table.schema, self.options.source_dialect, dialect) {
            let statement = table.schema.create_statement.as_deref().unwrap_or_default();
            writeln!(self.writer, "{statement}").map_err(io_err)?;
        } else {
            let from = self.options.source_dialect.unwrap_or(dialect);
            let sql = ddl::render_create_table(&table.schema, from, dialect, &mut self.warnings);
            write!(self.writer, "{sql}").map_err(io_err)?;
        }
        if dialect == SqlDialect::Mssql {
            writeln!(self.writer, "GO").map_err(io_err)?;
        }
        Ok(())
    }

    /// Flush the buffered `INSERT` batch (a no-op if empty).
    fn flush_insert(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        let dialect = self.options.dialect;
        let state = self
            .table
            .as_mut()
            .expect("flush_insert is only called while a table is active");
        if state.row_batch.is_empty() {
            return Ok(());
        }
        let indices = state
            .insert_columns
            .as_ref()
            .expect("insert_columns is set before the first row is buffered");
        let identity_insert = state.identity_insert.unwrap_or(false);
        let column_list = quoted_column_list(dialect, table, indices);
        // MSSQL rejects an explicit value for an IDENTITY column unless
        // IDENTITY_INSERT is toggled on for the table; open it once, before the
        // first batch, and close it in `end_table`.
        if identity_insert && dialect == SqlDialect::Mssql && !state.identity_on {
            writeln!(self.writer, "SET IDENTITY_INSERT {} ON", state.quoted_table)
                .map_err(io_err)?;
            writeln!(self.writer, "GO").map_err(io_err)?;
            state.identity_on = true;
        }
        // PostgreSQL rejects an explicit value for a `GENERATED ALWAYS AS
        // IDENTITY` column unless the statement declares `OVERRIDING SYSTEM
        // VALUE`.
        let overriding = if identity_insert && dialect == SqlDialect::Postgres {
            " OVERRIDING SYSTEM VALUE"
        } else {
            ""
        };
        writeln!(
            self.writer,
            "INSERT INTO {} ({column_list}){overriding} VALUES",
            state.quoted_table
        )
        .map_err(io_err)?;
        writeln!(self.writer, "{};", state.row_batch.as_str()).map_err(io_err)?;
        if dialect == SqlDialect::Mssql {
            writeln!(self.writer, "GO").map_err(io_err)?;
        }
        state.row_batch.clear();
        Ok(())
    }

    /// Flush the buffered `COPY` batch (a no-op if empty).
    fn flush_copy(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        let state = self
            .table
            .as_mut()
            .expect("flush_copy is only called while a table is active");
        if state.copy_rows == 0 {
            return Ok(());
        }
        let indices = state
            .insert_columns
            .as_ref()
            .expect("insert_columns is set before the first row is buffered");
        let column_list = quoted_column_list(SqlDialect::Postgres, table, indices);
        writeln!(
            self.writer,
            "COPY {} ({column_list}) FROM stdin;",
            state.quoted_table
        )
        .map_err(io_err)?;
        write!(self.writer, "{}", state.copy_batch).map_err(io_err)?;
        writeln!(self.writer, "\\.").map_err(io_err)?;
        state.copy_batch.clear();
        state.copy_rows = 0;
        Ok(())
    }

    /// Determine, from the first generated row, which columns render
    /// `DEFAULT` for every row of this table (see [`TableState::insert_columns`]),
    /// and reject a `COPY` target for a `DEFAULT` column with no database
    /// default/identity to fall back on (`COPY`'s column list can omit a
    /// column, but every omitted column must have somewhere to get its value
    /// from).
    fn classify_columns(
        table: &PlannedTable,
        row: &GeneratedRow,
        use_copy: bool,
    ) -> Result<Vec<usize>, GenerateError> {
        let is_default_column: Vec<bool> = row
            .values
            .iter()
            .map(|value| matches!(value, GeneratedValue::Default))
            .collect();
        if use_copy {
            for (i, column) in table.columns.iter().enumerate() {
                if !is_default_column[i] {
                    continue;
                }
                let schema = &column.schema;
                if schema.default_sql.is_none() && !schema.identity && !schema.generated {
                    return Err(GenerateError::InvalidInput(format!(
                        "GEN-RENDER-COPY-DEFAULT: table `{}` column `{}` renders as DEFAULT but has no database default/identity for PostgreSQL COPY to fall back on; render with `no_copy` (multi-row INSERT) instead",
                        table.name, schema.name
                    )));
                }
            }
        }
        Ok((0..table.columns.len())
            .filter(|&i| !is_default_column[i])
            .collect())
    }
}

impl<W: Write> RowSink for SqlRenderer<W> {
    fn begin_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        if self.options.mode != OutputMode::DataOnly {
            self.render_ddl(table)?;
        }
        let data_enabled = self.options.mode != OutputMode::SchemaOnly;
        // A table carrying an identity column that may receive explicit values
        // must render as multi-row `INSERT` (not `COPY`) on PostgreSQL, so the
        // load can add `OVERRIDING SYSTEM VALUE`; `COPY` has no such clause.
        let has_identity = table.columns.iter().any(|column| column.schema.identity);
        let use_copy = data_enabled
            && self.options.dialect == SqlDialect::Postgres
            && !self.options.no_copy
            && !has_identity;
        let batch_size = self.options.batch_size.max(1);
        self.table = Some(TableState {
            quoted_table: quote_ident(self.options.dialect, &table.name),
            data_enabled,
            use_copy,
            batch_size,
            insert_columns: None,
            row_batch: RowBatch::with_capacity(batch_size, batch_size * 64),
            copy_batch: String::new(),
            copy_rows: 0,
            identity_insert: None,
            identity_on: false,
        });
        Ok(())
    }

    fn write_row(&mut self, table: &PlannedTable, row: &GeneratedRow) -> Result<(), GenerateError> {
        let dialect = self.options.dialect;
        let (data_enabled, use_copy, needs_classification) = match &self.table {
            Some(state) => (
                state.data_enabled,
                state.use_copy,
                state.insert_columns.is_none(),
            ),
            None => return Ok(()),
        };
        if !data_enabled {
            return Ok(());
        }
        if needs_classification {
            let insert_columns = Self::classify_columns(table, row, use_copy)?;
            // An identity column that survives classification (i.e. carries an
            // explicit value rather than rendering `DEFAULT`) needs the
            // engine-specific identity-insert wrapper to load.
            let identity_insert = matches!(dialect, SqlDialect::Mssql | SqlDialect::Postgres)
                && insert_columns
                    .iter()
                    .any(|&i| table.columns[i].schema.identity);
            let state = self.table.as_mut().expect("checked above");
            state.insert_columns = Some(insert_columns);
            state.identity_insert = Some(identity_insert);
        }

        let state = self.table.as_mut().expect("checked above");
        let indices = state
            .insert_columns
            .as_ref()
            .expect("set just above on the first row");

        if state.use_copy {
            write!(
                state.copy_batch,
                "{}",
                CopyRow {
                    indices,
                    values: &row.values,
                }
            )
            .map_err(fmt_err)?;
            state.copy_batch.push('\n');
            state.copy_rows += 1;
            if state.copy_rows >= state.batch_size {
                self.flush_copy(table)?;
            }
        } else {
            state
                .row_batch
                .push_fmt(format_args!(
                    "{}",
                    RowTuple {
                        dialect,
                        indices,
                        values: &row.values,
                    }
                ))
                .map_err(fmt_err)?;
            if state.row_batch.row_count() >= state.batch_size {
                self.flush_insert(table)?;
            }
        }
        Ok(())
    }

    fn end_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        let (data_enabled, use_copy) = match &self.table {
            Some(state) => (state.data_enabled, state.use_copy),
            None => (false, false),
        };
        if data_enabled {
            if use_copy {
                self.flush_copy(table)?;
            } else {
                self.flush_insert(table)?;
            }
        }
        // Close the MSSQL identity-insert wrapper opened in `flush_insert`.
        if let Some(state) = &self.table {
            if state.identity_on {
                writeln!(
                    self.writer,
                    "SET IDENTITY_INSERT {} OFF",
                    state.quoted_table
                )
                .map_err(io_err)?;
                writeln!(self.writer, "GO").map_err(io_err)?;
            }
        }
        self.table = None;
        Ok(())
    }
}

/// Map an [`io::Error`] into the closed [`GenerateError`] set: the
/// [`RowSink`] contract (fixed by Task 13) has no I/O variant, so a write
/// failure surfaces as `InvalidInput` with a distinguishing `GEN-RENDER-IO`
/// prefix rather than silently panicking or being swallowed.
fn io_err(err: io::Error) -> GenerateError {
    GenerateError::InvalidInput(format!("GEN-RENDER-IO: {err}"))
}

/// Map a [`fmt::Error`] the same way as [`io_err`]; writing into an in-memory
/// buffer is infallible in practice; this only exists so `?` type-checks.
fn fmt_err(_: fmt::Error) -> GenerateError {
    GenerateError::InvalidInput("GEN-RENDER-IO: formatting error".to_string())
}

/// Quote and comma-join the table's column names at `indices`, in order.
fn quoted_column_list(dialect: SqlDialect, table: &PlannedTable, indices: &[usize]) -> String {
    indices
        .iter()
        .map(|&i| quote_ident(dialect, &table.columns[i].schema.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One `INSERT` row tuple: `(v1, v2, ...)`, written straight into the
/// destination [`RowBatch`] with no intermediate per-cell allocation.
struct RowTuple<'a> {
    dialect: SqlDialect,
    indices: &'a [usize],
    values: &'a [GeneratedValue],
}

impl fmt::Display for RowTuple<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (position, &i) in self.indices.iter().enumerate() {
            if position > 0 {
                f.write_str(", ")?;
            }
            write!(
                f,
                "{}",
                InsertValue {
                    dialect: self.dialect,
                    value: &self.values[i],
                }
            )?;
        }
        f.write_str(")")
    }
}

/// A single value rendered as an `INSERT`-literal, dialect-correct via
/// [`SqlString`] for text-like values.
struct InsertValue<'a> {
    dialect: SqlDialect,
    value: &'a GeneratedValue,
}

impl fmt::Display for InsertValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            GeneratedValue::Null => f.write_str("NULL"),
            // Reachable only if a column classified as non-DEFAULT (from the
            // first row) ever renders `Default` on a later row; the engine's
            // per-table owner assignment makes that impossible today (see
            // `SqlRenderer::classify_columns`), but this still renders a
            // syntactically valid literal instead of corrupting the batch.
            GeneratedValue::Default => f.write_str("DEFAULT"),
            GeneratedValue::Boolean(value) => f.write_str(boolean_literal(self.dialect, *value)),
            GeneratedValue::Integer(value) => write!(f, "{value}"),
            GeneratedValue::Decimal { minor, scale } => write_decimal(f, *minor, *scale),
            GeneratedValue::Text(value) => write!(f, "{}", SqlString::new(self.dialect, value)),
            GeneratedValue::Bytes(value) => write_bytes_literal(f, self.dialect, value),
            GeneratedValue::DateTime(value) => write!(f, "{}", SqlString::new(self.dialect, value)),
            GeneratedValue::Json(value) => write!(f, "{}", SqlString::new(self.dialect, value)),
        }
    }
}

/// The dialect-correct boolean literal. MySQL/SQLite have no boolean type and
/// MSSQL's `BIT` only accepts `0`/`1`; PostgreSQL's `boolean` accepts (and
/// pretty-prints as) `TRUE`/`FALSE`.
fn boolean_literal(dialect: SqlDialect, value: bool) -> &'static str {
    match (dialect, value) {
        (SqlDialect::Postgres, true) => "TRUE",
        (SqlDialect::Postgres, false) => "FALSE",
        (_, true) => "1",
        (_, false) => "0",
    }
}

/// Render a fixed-point `{ minor, scale }` value as `123.45` (or a bare
/// integer when `scale` is `0`), without going through floating point.
fn write_decimal(f: &mut fmt::Formatter<'_>, minor: i128, scale: u32) -> fmt::Result {
    if scale == 0 {
        return write!(f, "{minor}");
    }
    let divisor = 10i128.pow(scale);
    if minor < 0 {
        f.write_str("-")?;
    }
    let magnitude = minor.unsigned_abs();
    let whole = magnitude / divisor as u128;
    let fraction = magnitude % divisor as u128;
    write!(f, "{whole}.{fraction:0width$}", width = scale as usize)
}

/// Render a bytes value as the dialect's binary-string-literal syntax.
fn write_bytes_literal(
    f: &mut fmt::Formatter<'_>,
    dialect: SqlDialect,
    bytes: &[u8],
) -> fmt::Result {
    match dialect {
        SqlDialect::MySql | SqlDialect::Sqlite => {
            f.write_str("X'")?;
            write_hex(f, bytes)?;
            f.write_str("'")
        }
        SqlDialect::Postgres => {
            f.write_str("'\\x")?;
            write_hex(f, bytes)?;
            f.write_str("'")
        }
        SqlDialect::Mssql => {
            f.write_str("0x")?;
            write_hex(f, bytes)
        }
    }
}

/// Write `bytes` as lowercase hex, two characters per byte, straight into
/// the formatter (no intermediate hex-encoded `String`).
fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

/// One `COPY` line: dialect-fixed to PostgreSQL's tab-delimited text format,
/// which differs from `INSERT`-literal escaping (see [`CopyValue`]).
struct CopyRow<'a> {
    indices: &'a [usize],
    values: &'a [GeneratedValue],
}

impl fmt::Display for CopyRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, &i) in self.indices.iter().enumerate() {
            if position > 0 {
                f.write_str("\t")?;
            }
            write!(f, "{}", CopyValue(&self.values[i]))?;
        }
        Ok(())
    }
}

/// A single value in PostgreSQL `COPY` text format: unquoted, tab/newline
/// delimited, `NULL` spelled `\N`, and backslash-escaped rather than
/// SQL-string-escaped (see the module docs for why this cannot reuse
/// [`SqlString`]).
struct CopyValue<'a>(&'a GeneratedValue);

impl fmt::Display for CopyValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            // `Default` cannot reach a COPY stream: `classify_columns`
            // excludes every DEFAULT column from `indices` before any
            // `CopyValue` is ever constructed.
            GeneratedValue::Null | GeneratedValue::Default => f.write_str("\\N"),
            GeneratedValue::Boolean(value) => f.write_str(if *value { "t" } else { "f" }),
            GeneratedValue::Integer(value) => write!(f, "{value}"),
            GeneratedValue::Decimal { minor, scale } => write_decimal(f, *minor, *scale),
            GeneratedValue::Text(value) => write_copy_escaped(f, value),
            GeneratedValue::Bytes(value) => {
                f.write_str("\\\\x")?;
                write_hex(f, value)
            }
            GeneratedValue::DateTime(value) => write_copy_escaped(f, value),
            GeneratedValue::Json(value) => write_copy_escaped(f, value),
        }
    }
}

/// Escape `value` for PostgreSQL `COPY` text format: backslash, tab,
/// newline, and carriage return each get a backslash-letter escape; every
/// other character (including `'`) is copied through unquoted.
fn write_copy_escaped(f: &mut fmt::Formatter<'_>, value: &str) -> fmt::Result {
    for ch in value.chars() {
        match ch {
            '\\' => f.write_str("\\\\")?,
            '\t' => f.write_str("\\t")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            other => f.write_char(other)?,
        }
    }
    Ok(())
}
