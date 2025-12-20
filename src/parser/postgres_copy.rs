//! PostgreSQL COPY statement parser.
//!
//! Parses COPY ... FROM stdin data blocks to extract individual rows
//! and optionally extract PK/FK column values for dependency tracking.

use crate::schema::{ColumnId, ColumnType, TableSchema};
use smallvec::SmallVec;

// Re-use types from mysql_insert for consistency
use super::mysql_insert::{FkRef, PkValue};

/// Tuple of PK values for composite primary keys
pub type PkTuple = SmallVec<[PkValue; 2]>;

/// A parsed row from a COPY data block
#[derive(Debug, Clone)]
pub struct ParsedCopyRow {
    /// Raw bytes of the row (tab-separated values, no newline)
    pub raw: Vec<u8>,
    /// Extracted primary key values (if table has PK and values are non-NULL)
    pub pk: Option<PkTuple>,
    /// Extracted foreign key values with their references
    pub fk_values: Vec<(FkRef, PkTuple)>,
}

/// Parser for PostgreSQL COPY data blocks
pub struct CopyParser<'a> {
    data: &'a [u8],
    table_schema: Option<&'a TableSchema>,
    /// Column order from COPY header
    column_order: Vec<Option<ColumnId>>,
}

impl<'a> CopyParser<'a> {
    /// Create a new parser for COPY data
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            table_schema: None,
            column_order: Vec::new(),
        }
    }

    /// Set the table schema for PK/FK extraction
    pub fn with_schema(mut self, schema: &'a TableSchema) -> Self {
        self.table_schema = Some(schema);
        self
    }

    /// Set column order from COPY header
    pub fn with_column_order(mut self, columns: Vec<String>) -> Self {
        if let Some(schema) = self.table_schema {
            self.column_order = columns
                .iter()
                .map(|name| schema.get_column_id(name))
                .collect();
        }
        self
    }

    /// Parse all rows from the COPY data block
    pub fn parse_rows(&mut self) -> anyhow::Result<Vec<ParsedCopyRow>> {
        // If no explicit column order, use natural schema order
        if self.column_order.is_empty() {
            if let Some(schema) = self.table_schema {
                self.column_order = schema.columns.iter().map(|c| Some(c.ordinal)).collect();
            }
        }

        let mut rows = Vec::new();
        let mut pos = 0;

        while pos < self.data.len() {
            // Find end of line
            let line_end = self.data[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .map(|p| pos + p)
                .unwrap_or(self.data.len());

            let line = &self.data[pos..line_end];

            // Check for terminator
            if line == b"\\." || line.is_empty() {
                pos = line_end + 1;
                continue;
            }

            // Parse the row
            if let Some(row) = self.parse_row(line)? {
                rows.push(row);
            }

            pos = line_end + 1;
        }

        Ok(rows)
    }

    /// Parse a single tab-separated row
    fn parse_row(&self, line: &[u8]) -> anyhow::Result<Option<ParsedCopyRow>> {
        let raw = line.to_vec();

        // Split by tabs
        let values: Vec<CopyValue> = self.split_and_parse_values(line);

        // Extract PK and FK if we have schema
        let (pk, fk_values) = if let Some(schema) = self.table_schema {
            self.extract_pk_fk(&values, schema)
        } else {
            (None, Vec::new())
        };

        Ok(Some(ParsedCopyRow { raw, pk, fk_values }))
    }

    /// Split line by tabs and parse each value
    fn split_and_parse_values(&self, line: &[u8]) -> Vec<CopyValue> {
        let mut values = Vec::new();
        let mut start = 0;

        for (i, &b) in line.iter().enumerate() {
            if b == b'\t' {
                values.push(self.parse_copy_value(&line[start..i]));
                start = i + 1;
            }
        }
        // Last value
        if start <= line.len() {
            values.push(self.parse_copy_value(&line[start..]));
        }

        values
    }

    /// Parse a single COPY value
    fn parse_copy_value(&self, value: &[u8]) -> CopyValue {
        // Check for NULL marker
        if value == b"\\N" {
            return CopyValue::Null;
        }

        // Decode escape sequences
        let decoded = self.decode_copy_escapes(value);

        // Try to parse as integer
        if let Ok(s) = std::str::from_utf8(&decoded) {
            if let Ok(n) = s.parse::<i64>() {
                return CopyValue::Integer(n);
            }
            if let Ok(n) = s.parse::<i128>() {
                return CopyValue::BigInteger(n);
            }
        }

        CopyValue::Text(decoded)
    }

    /// Decode PostgreSQL COPY escape sequences
    fn decode_copy_escapes(&self, value: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(value.len());
        let mut i = 0;

        while i < value.len() {
            if value[i] == b'\\' && i + 1 < value.len() {
                let next = value[i + 1];
                let decoded = match next {
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'\\' => b'\\',
                    b'N' => {
                        // This shouldn't happen here since we check for \N above
                        result.push(b'\\');
                        result.push(b'N');
                        i += 2;
                        continue;
                    }
                    _ => {
                        // Unknown escape, keep as-is
                        result.push(b'\\');
                        result.push(next);
                        i += 2;
                        continue;
                    }
                };
                result.push(decoded);
                i += 2;
            } else {
                result.push(value[i]);
                i += 1;
            }
        }

        result
    }

    /// Extract PK and FK values from parsed values
    fn extract_pk_fk(
        &self,
        values: &[CopyValue],
        schema: &TableSchema,
    ) -> (Option<PkTuple>, Vec<(FkRef, PkTuple)>) {
        let mut pk_values = PkTuple::new();
        let mut fk_values = Vec::new();

        // Build PK from columns marked as primary key
        for (idx, col_id_opt) in self.column_order.iter().enumerate() {
            if let Some(col_id) = col_id_opt {
                if schema.is_pk_column(*col_id) {
                    if let Some(value) = values.get(idx) {
                        let pk_val = self.value_to_pk(value, schema.column(*col_id));
                        pk_values.push(pk_val);
                    }
                }
            }
        }

        // Build FK tuples
        for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
            if fk.referenced_table_id.is_none() {
                continue;
            }

            let mut fk_tuple = PkTuple::new();
            let mut all_non_null = true;

            for &col_id in &fk.columns {
                if let Some(idx) = self.column_order.iter().position(|&c| c == Some(col_id)) {
                    if let Some(value) = values.get(idx) {
                        let pk_val = self.value_to_pk(value, schema.column(col_id));
                        if pk_val.is_null() {
                            all_non_null = false;
                            break;
                        }
                        fk_tuple.push(pk_val);
                    }
                }
            }

            if all_non_null && !fk_tuple.is_empty() {
                fk_values.push((
                    FkRef {
                        table_id: schema.id.0,
                        fk_index: fk_idx as u16,
                    },
                    fk_tuple,
                ));
            }
        }

        let pk = if pk_values.is_empty() || pk_values.iter().any(|v| v.is_null()) {
            None
        } else {
            Some(pk_values)
        };

        (pk, fk_values)
    }

    /// Convert a parsed value to a PkValue
    fn value_to_pk(&self, value: &CopyValue, col: Option<&crate::schema::Column>) -> PkValue {
        match value {
            CopyValue::Null => PkValue::Null,
            CopyValue::Integer(n) => PkValue::Int(*n),
            CopyValue::BigInteger(n) => PkValue::BigInt(*n),
            CopyValue::Text(bytes) => {
                let s = String::from_utf8_lossy(bytes);

                // Check if this might be an integer stored as text
                if let Some(col) = col {
                    match col.col_type {
                        ColumnType::Int => {
                            if let Ok(n) = s.parse::<i64>() {
                                return PkValue::Int(n);
                            }
                        }
                        ColumnType::BigInt => {
                            if let Ok(n) = s.parse::<i128>() {
                                return PkValue::BigInt(n);
                            }
                        }
                        _ => {}
                    }
                }

                PkValue::Text(s.into_owned().into_boxed_str())
            }
        }
    }
}

/// Internal representation of a parsed COPY value
#[derive(Debug, Clone)]
enum CopyValue {
    Null,
    Integer(i64),
    BigInteger(i128),
    Text(Vec<u8>),
}

/// Parse column list from COPY header
pub fn parse_copy_columns(header: &str) -> Vec<String> {
    // COPY table_name (col1, col2, ...) FROM stdin;
    if let Some(start) = header.find('(') {
        if let Some(end) = header.find(')') {
            let cols = &header[start + 1..end];
            return cols
                .split(',')
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
        }
    }
    Vec::new()
}

/// Parse all rows from a PostgreSQL COPY data block
pub fn parse_postgres_copy_rows(
    data: &[u8],
    schema: &TableSchema,
    column_order: Vec<String>,
) -> anyhow::Result<Vec<ParsedCopyRow>> {
    let mut parser = CopyParser::new(data)
        .with_schema(schema)
        .with_column_order(column_order);
    parser.parse_rows()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Column, ColumnType, ForeignKey, TableId};

    fn create_simple_schema() -> TableSchema {
        let mut schema = TableSchema::new("users".to_string(), TableId(0));
        schema.columns = vec![
            Column {
                name: "id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(0),
                is_primary_key: true,
                is_nullable: false,
            },
            Column {
                name: "name".to_string(),
                col_type: ColumnType::Text,
                ordinal: ColumnId(1),
                is_primary_key: false,
                is_nullable: true,
            },
            Column {
                name: "company_id".to_string(),
                col_type: ColumnType::Int,
                ordinal: ColumnId(2),
                is_primary_key: false,
                is_nullable: true,
            },
        ];
        schema.primary_key = vec![ColumnId(0)];
        schema.foreign_keys = vec![ForeignKey {
            name: None,
            columns: vec![ColumnId(2)],
            column_names: vec!["company_id".to_string()],
            referenced_table: "companies".to_string(),
            referenced_columns: vec!["id".to_string()],
            referenced_table_id: Some(TableId(1)),
        }];
        schema
    }

    #[test]
    fn test_parse_copy_data() {
        let data = b"1\tAlice\t5\n2\tBob\t5\n3\tCarol\t\\N\n\\.";
        let schema = create_simple_schema();

        let rows = parse_postgres_copy_rows(
            data,
            &schema,
            vec![
                "id".to_string(),
                "name".to_string(),
                "company_id".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].pk.as_ref().unwrap()[0], PkValue::Int(1));
        assert_eq!(rows[1].pk.as_ref().unwrap()[0], PkValue::Int(2));
        assert_eq!(rows[2].pk.as_ref().unwrap()[0], PkValue::Int(3));
    }

    #[test]
    fn test_parse_null_values() {
        let data = b"1\t\\N\t\\N\n";
        let schema = create_simple_schema();

        let rows = parse_postgres_copy_rows(
            data,
            &schema,
            vec![
                "id".to_string(),
                "name".to_string(),
                "company_id".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        // FK should not be extracted when NULL
        assert!(rows[0].fk_values.is_empty());
    }

    #[test]
    fn test_parse_copy_columns() {
        let header = r#"COPY public.users (id, name, email) FROM stdin;"#;
        let cols = parse_copy_columns(header);
        assert_eq!(cols, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_decode_escapes() {
        let parser = CopyParser::new(&[]);
        let decoded = parser.decode_copy_escapes(b"hello\\tworld\\n");
        assert_eq!(decoded, b"hello\tworld\n");
    }
}
