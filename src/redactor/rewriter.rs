//! Value rewriter for INSERT and COPY statement redaction.
//!
//! Handles parsing values, applying redaction strategies, and formatting
//! the redacted values back to SQL with proper dialect-aware escaping.

use crate::parser::mysql_insert::{InsertParser, ParsedValue, RowExtraction};
use crate::parser::postgres_copy::{decode_copy_escapes, parse_copy_columns, CopyParser};
use crate::parser::SqlDialect;
use crate::redactor::strategy::{
    ConstantStrategy, FakeStrategy, HashStrategy, MaskStrategy, NullStrategy, RedactValue,
    Strategy, StrategyKind,
};
use crate::schema::TableSchema;
use rand::rngs::StdRng;
use rand::SeedableRng;

/// Rewriter for INSERT and COPY statements
pub struct ValueRewriter {
    /// RNG for reproducible redaction
    rng: StdRng,
    /// Dialect for output formatting
    dialect: SqlDialect,
    /// Locale for fake data generation
    locale: String,
}

impl ValueRewriter {
    /// Create a new rewriter with optional seed for reproducibility
    pub fn new(seed: Option<u64>, dialect: SqlDialect, locale: String) -> Self {
        let rng = match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => rand::make_rng(),
        };
        Self {
            rng,
            dialect,
            locale,
        }
    }

    /// Rewrite an INSERT statement with redacted values
    pub fn rewrite_insert(
        &mut self,
        stmt: &[u8],
        table_name: &str,
        table: &TableSchema,
        strategies: &[StrategyKind],
    ) -> anyhow::Result<(Vec<u8>, u64, u64)> {
        // Parse the INSERT statement (dialect governs backslash unescaping so
        // non-MySQL strings aren't corrupted on re-serialization, bug #3)
        let mut parser = InsertParser::new(stmt)
            .with_schema(table)
            .with_dialect(self.dialect)
            .with_extraction(RowExtraction::ValuesOnly);
        let rows = parser.parse_rows()?;

        if rows.is_empty() {
            return Ok((stmt.to_vec(), 0, 0));
        }

        // Get the column list (if any) from the statement, locating VALUES as a
        // keyword so a table named like `*values*` doesn't misplace it (bug #2).
        let column_list = crate::parser::mysql_insert::find_values_keyword_pos(stmt)
            .and_then(|pos| crate::parser::mysql_insert::extract_column_list_before(stmt, pos));

        // Build the header: INSERT INTO table_name (columns) VALUES
        let mut result = self.build_insert_header(table_name, &column_list);

        let mut rows_redacted = 0u64;
        let mut columns_redacted = 0u64;
        let num_strategies = strategies.len();

        for (row_idx, row) in rows.iter().enumerate() {
            if row_idx > 0 {
                result.extend_from_slice(b",");
            }
            result.extend_from_slice(b"\n(");

            let mut row_had_redaction = false;

            for (col_idx, value) in row.values.iter().enumerate() {
                if col_idx > 0 {
                    result.extend_from_slice(b", ");
                }

                // Get strategy for this column (may be Skip if index out of bounds)
                let strategy = strategies.get(col_idx).unwrap_or(&StrategyKind::Skip);

                // Apply redaction
                let (redacted_sql, was_redacted) =
                    self.redact_value(value, strategy, col_idx < num_strategies);
                result.extend_from_slice(redacted_sql.as_bytes());

                if was_redacted {
                    columns_redacted += 1;
                    row_had_redaction = true;
                }
            }

            result.extend_from_slice(b")");
            if row_had_redaction {
                rows_redacted += 1;
            }
        }

        result.extend_from_slice(b";\n");

        Ok((result, rows_redacted, columns_redacted))
    }

    /// Rewrite a COPY statement with redacted values (PostgreSQL)
    ///
    /// Splits the statement into header line and data block, then delegates
    /// the row redaction loop to [`Self::rewrite_copy_data`].
    pub fn rewrite_copy(
        &mut self,
        stmt: &[u8],
        _table_name: &str,
        table: &TableSchema,
        strategies: &[StrategyKind],
    ) -> anyhow::Result<(Vec<u8>, u64, u64)> {
        // COPY statements include the header and data block
        // Format: COPY table (cols) FROM stdin;\ndata\n\.\n

        // Find the header line (ends with "FROM stdin;" or similar)
        let header_end = stmt
            .iter()
            .position(|&b| b == b'\n')
            .ok_or_else(|| anyhow::anyhow!("Invalid COPY statement: no newline"))?;
        let header = &stmt[..header_end];
        let data_block = &stmt[header_end + 1..];

        // Parse column list from header
        let columns = parse_copy_columns(&String::from_utf8_lossy(header));

        let (redacted_data, rows_redacted, columns_redacted) =
            self.rewrite_copy_data(data_block, table, strategies, &columns)?;

        // Build result: header + redacted data (including terminator)
        let mut result = Vec::with_capacity(header.len() + 1 + redacted_data.len());
        result.extend_from_slice(header);
        result.push(b'\n');
        result.extend_from_slice(&redacted_data);

        Ok((result, rows_redacted, columns_redacted))
    }

    /// Rewrite just the COPY data block (header handled separately)
    pub fn rewrite_copy_data(
        &mut self,
        data_block: &[u8],
        table: &TableSchema,
        strategies: &[StrategyKind],
        columns: &[String],
    ) -> anyhow::Result<(Vec<u8>, u64, u64)> {
        // Parse data rows
        let mut parser = CopyParser::new(data_block)
            .with_schema(table)
            .with_column_order(columns.to_vec())
            .with_extraction(RowExtraction::ValuesOnly);
        let rows = parser.parse_rows()?;

        if rows.is_empty() {
            return Ok((data_block.to_vec(), 0, 0));
        }

        // Build result: redacted data + terminator
        let mut result = Vec::with_capacity(data_block.len());

        let mut rows_redacted = 0u64;
        let mut columns_redacted = 0u64;

        for row in &rows {
            let mut row_had_redaction = false;
            let mut first = true;

            // Parse the raw values from the row
            let values = self.parse_copy_row_values(&row.raw);

            for (col_idx, value) in values.iter().enumerate() {
                if !first {
                    result.push(b'\t');
                }
                first = false;

                let strategy = strategies.get(col_idx).unwrap_or(&StrategyKind::Skip);
                let (redacted, was_redacted) = self.redact_copy_value(value, strategy);
                result.extend_from_slice(&redacted);

                if was_redacted {
                    columns_redacted += 1;
                    row_had_redaction = true;
                }
            }

            result.push(b'\n');
            if row_had_redaction {
                rows_redacted += 1;
            }
        }

        // Add terminator
        result.extend_from_slice(b"\\.\n");

        Ok((result, rows_redacted, columns_redacted))
    }

    /// Parse tab-separated values from a COPY row
    fn parse_copy_row_values(&self, raw: &[u8]) -> Vec<CopyValueRef> {
        let mut values = Vec::new();
        let mut start = 0;

        for (i, &b) in raw.iter().enumerate() {
            if b == b'\t' {
                values.push(self.parse_single_copy_value(&raw[start..i]));
                start = i + 1;
            }
        }
        // Last value
        if start <= raw.len() {
            values.push(self.parse_single_copy_value(&raw[start..]));
        }

        values
    }

    /// Parse a single COPY value
    fn parse_single_copy_value(&self, raw: &[u8]) -> CopyValueRef {
        if raw == b"\\N" {
            CopyValueRef::Null
        } else {
            CopyValueRef::Text(raw.to_vec())
        }
    }

    /// Redact a COPY value and return the redacted bytes
    fn redact_copy_value(
        &mut self,
        value: &CopyValueRef,
        strategy: &StrategyKind,
    ) -> (Vec<u8>, bool) {
        if matches!(strategy, StrategyKind::Skip) {
            let bytes = match value {
                CopyValueRef::Null => b"\\N".to_vec(),
                CopyValueRef::Text(t) => t.clone(),
            };
            return (bytes, false);
        }

        // Convert to RedactValue
        let redact_value = match value {
            CopyValueRef::Null => RedactValue::Null,
            CopyValueRef::Text(t) => {
                // Decode escape sequences first (canonical COPY decoder lives
                // in the parser module)
                let decoded = decode_copy_escapes(t);
                RedactValue::String(String::from_utf8_lossy(&decoded).into_owned())
            }
        };

        // Apply strategy
        let result = self.apply_strategy(&redact_value, strategy);

        // Convert back to COPY format
        let bytes = match result {
            RedactValue::Null => b"\\N".to_vec(),
            RedactValue::String(s) => self.encode_copy_escapes(&s),
            RedactValue::Integer(i) => i.to_string().into_bytes(),
            RedactValue::Bytes(b) => self.encode_copy_escapes(&String::from_utf8_lossy(&b)),
        };

        (bytes, true)
    }

    /// Encode string for COPY format (escape special characters)
    fn encode_copy_escapes(&self, value: &str) -> Vec<u8> {
        let mut result = Vec::with_capacity(value.len());

        for b in value.bytes() {
            match b {
                b'\n' => result.extend_from_slice(b"\\n"),
                b'\r' => result.extend_from_slice(b"\\r"),
                b'\t' => result.extend_from_slice(b"\\t"),
                b'\\' => result.extend_from_slice(b"\\\\"),
                _ => result.push(b),
            }
        }

        result
    }

    /// Build INSERT statement header
    fn build_insert_header(&self, table_name: &str, columns: &Option<Vec<String>>) -> Vec<u8> {
        let mut result = Vec::new();

        // INSERT INTO table_name
        result.extend_from_slice(b"INSERT INTO ");
        result.extend_from_slice(self.quote_identifier(table_name).as_bytes());

        // Optional column list
        if let Some(cols) = columns {
            result.extend_from_slice(b" (");
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    result.extend_from_slice(b", ");
                }
                result.extend_from_slice(self.quote_identifier(col).as_bytes());
            }
            result.extend_from_slice(b")");
        }

        result.extend_from_slice(b" VALUES");
        result
    }

    /// Quote an identifier based on dialect
    fn quote_identifier(&self, name: &str) -> String {
        crate::transform_common::quote_ident(self.dialect, name)
    }

    /// Redact a parsed value and format it for SQL output
    fn redact_value(
        &mut self,
        value: &ParsedValue,
        strategy: &StrategyKind,
        has_strategy: bool,
    ) -> (String, bool) {
        // Skip strategy means no redaction
        if !has_strategy || matches!(strategy, StrategyKind::Skip) {
            return (self.format_value(value), false);
        }

        // Convert ParsedValue to RedactValue
        let redact_value = self.parsed_to_redact(value);

        // Apply the strategy
        let result = self.apply_strategy(&redact_value, strategy);

        // Format the result for SQL
        (self.format_redact_value(&result), true)
    }

    /// Convert ParsedValue to RedactValue
    fn parsed_to_redact(&self, value: &ParsedValue) -> RedactValue {
        match value {
            ParsedValue::Null => RedactValue::Null,
            ParsedValue::Integer(n) => RedactValue::Integer(*n),
            ParsedValue::BigInteger(n) => RedactValue::Integer(*n as i64), // Potential truncation
            ParsedValue::String { value } => RedactValue::String(value.clone()),
            ParsedValue::Hex(bytes) => RedactValue::Bytes(bytes.clone()),
            ParsedValue::Other(bytes) => {
                RedactValue::String(String::from_utf8_lossy(bytes).into_owned())
            }
        }
    }

    /// Apply a redaction strategy to a value
    fn apply_strategy(&mut self, value: &RedactValue, strategy: &StrategyKind) -> RedactValue {
        match strategy {
            StrategyKind::Null => NullStrategy::new().apply(value, &mut self.rng),
            StrategyKind::Constant { value: constant } => {
                ConstantStrategy::new(constant.clone()).apply(value, &mut self.rng)
            }
            StrategyKind::Hash { preserve_domain } => {
                HashStrategy::new(*preserve_domain).apply(value, &mut self.rng)
            }
            StrategyKind::Mask { pattern } => {
                MaskStrategy::new(pattern.clone()).apply(value, &mut self.rng)
            }
            StrategyKind::Fake { generator } => {
                FakeStrategy::new(generator.clone(), self.locale.clone())
                    .apply(value, &mut self.rng)
            }
            StrategyKind::Shuffle => {
                // Shuffle is special - needs column-level state
                // For now, treat as skip (shuffle implemented at higher level)
                value.clone()
            }
            StrategyKind::Skip => value.clone(),
        }
    }

    /// Format a ParsedValue for SQL output
    fn format_value(&self, value: &ParsedValue) -> String {
        match value {
            ParsedValue::Null => "NULL".to_string(),
            ParsedValue::Integer(n) => n.to_string(),
            ParsedValue::BigInteger(n) => n.to_string(),
            ParsedValue::String { value } => self.format_sql_string(value),
            ParsedValue::Hex(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            ParsedValue::Other(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        }
    }

    /// Format a RedactValue for SQL output
    fn format_redact_value(&self, value: &RedactValue) -> String {
        match value {
            RedactValue::Null => "NULL".to_string(),
            RedactValue::Integer(n) => n.to_string(),
            RedactValue::String(s) => self.format_sql_string(s),
            RedactValue::Bytes(b) => {
                // Format as hex literal
                format!("0x{}", hex::encode(b))
            }
        }
    }

    /// Format a string for SQL with proper escaping based on dialect
    fn format_sql_string(&self, value: &str) -> String {
        match self.dialect {
            SqlDialect::MySql => {
                // MySQL uses backslash escaping
                let escaped = value
                    .replace('\\', "\\\\")
                    .replace('\'', "\\'")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t")
                    .replace('\0', "\\0");
                format!("'{}'", escaped)
            }
            SqlDialect::Postgres | SqlDialect::Sqlite => {
                // PostgreSQL/SQLite use doubled single quotes
                let escaped = value.replace('\'', "''");
                format!("'{}'", escaped)
            }
            SqlDialect::Mssql => {
                // MSSQL uses N'...' for Unicode strings with doubled quotes
                let escaped = value.replace('\'', "''");
                // Use N'...' for non-ASCII or always for safety
                if value.bytes().any(|b| b > 127) {
                    format!("N'{}'", escaped)
                } else {
                    format!("'{}'", escaped)
                }
            }
        }
    }
}

/// Internal COPY value representation
enum CopyValueRef {
    Null,
    Text(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnId, ColumnType, TableId, TableSchema};

    fn create_test_schema() -> TableSchema {
        TableSchema {
            name: "users".to_string(),
            id: TableId(0),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    col_type: ColumnType::Int,
                    ordinal: ColumnId(0),
                    is_primary_key: true,
                    is_nullable: false,
                },
                Column {
                    name: "email".to_string(),
                    col_type: ColumnType::Text,
                    ordinal: ColumnId(1),
                    is_primary_key: false,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    col_type: ColumnType::Text,
                    ordinal: ColumnId(2),
                    is_primary_key: false,
                    is_nullable: true,
                },
            ],
            primary_key: vec![ColumnId(0)],
            foreign_keys: vec![],
            indexes: vec![],
            create_statement: None,
        }
    }

    #[test]
    fn test_rewrite_insert_table_name_with_values() {
        // A table named `product_values` must not have its column list dropped
        // by matching the "values" substring inside the name (bug #2).
        let mut schema = create_test_schema();
        schema.name = "product_values".to_string();

        let mut rewriter = ValueRewriter::new(Some(1), SqlDialect::MySql, "en".to_string());
        let stmt = b"INSERT INTO `product_values` (`id`, `email`, `name`) VALUES (1, 'a@b.com', 'Al'), (2, 'c@d.com', 'Bo');";
        let strategies = vec![StrategyKind::Skip, StrategyKind::Skip, StrategyKind::Skip];

        let (result, _rows, _cols) = rewriter
            .rewrite_insert(stmt, "product_values", &schema, &strategies)
            .unwrap();
        let s = String::from_utf8_lossy(&result);

        assert!(
            s.contains("(`id`, `email`, `name`)"),
            "column list dropped: {s}"
        );
        // Two data rows, no phantom third row.
        assert_eq!(s.matches("@").count(), 2, "wrong row count: {s}");
    }

    #[test]
    fn test_rewrite_insert_mysql() {
        let mut rewriter = ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
        let schema = create_test_schema();

        let stmt = b"INSERT INTO `users` (`id`, `email`, `name`) VALUES (1, 'alice@example.com', 'Alice');";
        let strategies = vec![
            StrategyKind::Skip, // id
            StrategyKind::Hash {
                preserve_domain: true,
            }, // email
            StrategyKind::Fake {
                generator: "name".to_string(),
            }, // name
        ];

        let (result, rows, cols) = rewriter
            .rewrite_insert(stmt, "users", &schema, &strategies)
            .unwrap();
        let result_str = String::from_utf8_lossy(&result);

        assert!(result_str.contains("INSERT INTO `users`"));
        assert!(result_str.contains("VALUES"));
        assert_eq!(rows, 1);
        assert_eq!(cols, 2); // email and name were redacted
    }

    #[test]
    fn test_rewrite_insert_mssql() {
        let mut rewriter = ValueRewriter::new(Some(42), SqlDialect::Mssql, "en".to_string());
        let schema = create_test_schema();

        let stmt = b"INSERT INTO [users] ([id], [email], [name]) VALUES (1, N'alice@example.com', N'Alice');";
        let strategies = vec![
            StrategyKind::Skip, // id
            StrategyKind::Null, // email
            StrategyKind::Skip, // name
        ];

        let (result, rows, cols) = rewriter
            .rewrite_insert(stmt, "users", &schema, &strategies)
            .unwrap();
        let result_str = String::from_utf8_lossy(&result);

        assert!(result_str.contains("INSERT INTO [users]"));
        assert!(result_str.contains("NULL")); // email redacted to NULL
        assert_eq!(rows, 1);
        assert_eq!(cols, 1);
    }

    #[test]
    fn test_format_sql_string_mysql() {
        let rewriter = ValueRewriter::new(Some(42), SqlDialect::MySql, "en".to_string());
        assert_eq!(rewriter.format_sql_string("hello"), "'hello'");
        assert_eq!(rewriter.format_sql_string("it's"), "'it\\'s'");
        assert_eq!(rewriter.format_sql_string("line\nbreak"), "'line\\nbreak'");
    }

    #[test]
    fn test_format_sql_string_postgres() {
        let rewriter = ValueRewriter::new(Some(42), SqlDialect::Postgres, "en".to_string());
        assert_eq!(rewriter.format_sql_string("hello"), "'hello'");
        assert_eq!(rewriter.format_sql_string("it's"), "'it''s'");
    }

    #[test]
    fn test_format_sql_string_mssql() {
        let rewriter = ValueRewriter::new(Some(42), SqlDialect::Mssql, "en".to_string());
        assert_eq!(rewriter.format_sql_string("hello"), "'hello'");
        assert_eq!(rewriter.format_sql_string("café"), "N'café'");
    }

    #[test]
    fn test_quote_identifier() {
        let mysql = ValueRewriter::new(None, SqlDialect::MySql, "en".to_string());
        assert_eq!(mysql.quote_identifier("users"), "`users`");

        let pg = ValueRewriter::new(None, SqlDialect::Postgres, "en".to_string());
        assert_eq!(pg.quote_identifier("users"), "\"users\"");

        let mssql = ValueRewriter::new(None, SqlDialect::Mssql, "en".to_string());
        assert_eq!(mssql.quote_identifier("users"), "[users]");
    }
}
