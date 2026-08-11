use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentifierError {
    #[error("database identifier must not be empty")]
    Empty,
    #[error("database identifier must not contain NUL")]
    ContainsNul,
}

/// An unquoted database identifier. Adapters must quote this value; it is never SQL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Identifier(String);

impl Identifier {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdentifierError::Empty);
        }
        if value.contains('\0') {
            return Err(IdentifierError::ContainsNul);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl TryFrom<String> for Identifier {
    type Error = IdentifierError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Identifier> for String {
    fn from(value: Identifier) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QualifiedTable {
    pub namespace: Identifier,
    pub name: Identifier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    Binary,
    Text,
}

/// Lossless adapter-neutral representation of a database scalar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum DbValue {
    Null,
    Bool(bool),
    Signed(i128),
    Unsigned(u128),
    Decimal {
        coefficient: Vec<u8>,
        scale: i32,
    },
    Float32(u32),
    Float64(u64),
    Text(String),
    Bytes(Vec<u8>),
    Json(Vec<u8>),
    Date {
        year: i32,
        month: u8,
        day: u8,
    },
    Time {
        nanos: i128,
    },
    Timestamp {
        local: String,
        offset_minutes: Option<i16>,
        precision: u8,
    },
    Vendor {
        type_id: String,
        format: ValueFormat,
        bytes: Vec<u8>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyTuple(pub Vec<DbValue>);
impl KeyTuple {
    pub fn new(values: Vec<DbValue>) -> Self {
        Self(values)
    }
    pub fn as_slice(&self) -> &[DbValue] {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: Identifier,
    pub ordinal: u32,
    pub vendor_type: String,
    pub nullable: bool,
    pub collation: Option<String>,
    pub precision: Option<u32>,
    pub scale: Option<i32>,
    pub timezone_semantics: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RowBatchError {
    #[error("row has {actual} values but batch has {expected} columns")]
    ColumnCount { expected: usize, actual: usize },
    #[error("row limit {limit} is reached")]
    RowLimit { limit: usize },
    #[error("row of {row_bytes} bytes exceeds batch byte limit {limit}")]
    RowTooLarge { row_bytes: usize, limit: usize },
    #[error("adding row would exceed batch byte limit {limit}")]
    ByteLimit { limit: usize },
}

/// A row batch with independent hard row-count and encoded-byte bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowBatch {
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<DbValue>>,
    encoded_bytes: usize,
    max_rows: usize,
    max_encoded_bytes: usize,
}

impl RowBatch {
    pub fn new(columns: Vec<ColumnMeta>, max_rows: usize, max_encoded_bytes: usize) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            encoded_bytes: 0,
            max_rows,
            max_encoded_bytes,
        }
    }
    pub fn try_push(
        &mut self,
        row: Vec<DbValue>,
        encoded_bytes: usize,
    ) -> Result<(), RowBatchError> {
        if row.len() != self.columns.len() {
            return Err(RowBatchError::ColumnCount {
                expected: self.columns.len(),
                actual: row.len(),
            });
        }
        if self.rows.len() >= self.max_rows {
            return Err(RowBatchError::RowLimit {
                limit: self.max_rows,
            });
        }
        if encoded_bytes > self.max_encoded_bytes {
            return Err(RowBatchError::RowTooLarge {
                row_bytes: encoded_bytes,
                limit: self.max_encoded_bytes,
            });
        }
        let next =
            self.encoded_bytes
                .checked_add(encoded_bytes)
                .ok_or(RowBatchError::ByteLimit {
                    limit: self.max_encoded_bytes,
                })?;
        if next > self.max_encoded_bytes {
            return Err(RowBatchError::ByteLimit {
                limit: self.max_encoded_bytes,
            });
        }
        self.rows.push(row);
        self.encoded_bytes = next;
        Ok(())
    }
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }
    pub fn rows(&self) -> &[Vec<DbValue>] {
        &self.rows
    }
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VendorCatalog {
    pub format_version: u32,
    pub dialect: String,
    pub server_version: String,
    pub database: Identifier,
    pub namespaces: Vec<CatalogNamespace>,
    pub dependencies: Vec<CatalogDependency>,
    pub vendor_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogNamespace {
    pub id: String,
    pub name: Identifier,
    pub owner: Option<String>,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub objects: Vec<CatalogObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogObject {
    pub id: String,
    pub kind: CatalogObjectKind,
    pub name: Identifier,
    /// Exact adapter-supplied definition. It is evidence, not executable input.
    pub definition: Vec<u8>,
    pub attributes: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogObjectKind {
    Table,
    Column,
    Sequence,
    PrimaryKey,
    UniqueConstraint,
    CheckConstraint,
    ForeignKey,
    Index,
    View,
    Routine,
    Trigger,
    Event,
    Partition,
    Tablespace,
    Vendor(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDependency {
    pub from_object_id: String,
    pub to_object_id: String,
    pub dependency_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identifier_accepts_hostile_text_as_data() {
        let id = Identifier::new("x\"; DROP TABLE y;--").unwrap();
        assert_eq!(id.as_str(), "x\"; DROP TABLE y;--");
    }
    #[test]
    fn batch_enforces_both_bounds() {
        let mut b = RowBatch::new(Vec::new(), 1, 3);
        b.try_push(Vec::new(), 3).unwrap();
        assert!(matches!(
            b.try_push(Vec::new(), 0),
            Err(RowBatchError::RowLimit { .. })
        ));
    }
    #[test]
    fn oversized_first_row_is_rejected() {
        let mut b = RowBatch::new(Vec::new(), 2, 3);
        assert!(matches!(
            b.try_push(Vec::new(), 4),
            Err(RowBatchError::RowTooLarge { .. })
        ));
    }
}
