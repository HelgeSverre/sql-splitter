//! Data type mapping between SQL dialects.
//!
//! Handles conversion of data types including:
//! - Integer types (TINYINT, SMALLINT, INT, BIGINT)
//! - Float types (FLOAT, DOUBLE, DECIMAL)
//! - String types (CHAR, VARCHAR, TEXT)
//! - Binary types (BLOB, BYTEA)
//! - Date/time types (DATE, DATETIME, TIMESTAMP)
//! - Special types (ENUM, SET, JSON)

use crate::parser::SqlDialect;
use once_cell::sync::Lazy;
use regex::Regex;

/// Type mapper for converting between dialects
pub struct TypeMapper;

impl TypeMapper {
    /// Convert all data types in a statement
    pub fn convert(stmt: &str, from: SqlDialect, to: SqlDialect) -> String {
        match (from, to) {
            (SqlDialect::MySql, SqlDialect::Postgres) => Self::mysql_to_postgres(stmt),
            (SqlDialect::MySql, SqlDialect::Sqlite) => Self::mysql_to_sqlite(stmt),
            (SqlDialect::MySql, SqlDialect::Mssql) => Self::mysql_to_mssql(stmt),
            (SqlDialect::Postgres, SqlDialect::MySql) => Self::postgres_to_mysql(stmt),
            (SqlDialect::Postgres, SqlDialect::Sqlite) => Self::postgres_to_sqlite(stmt),
            (SqlDialect::Postgres, SqlDialect::Mssql) => Self::postgres_to_mssql(stmt),
            (SqlDialect::Sqlite, SqlDialect::MySql) => Self::sqlite_to_mysql(stmt),
            (SqlDialect::Sqlite, SqlDialect::Postgres) => Self::sqlite_to_postgres(stmt),
            (SqlDialect::Sqlite, SqlDialect::Mssql) => Self::sqlite_to_mssql(stmt),
            (SqlDialect::Mssql, SqlDialect::MySql) => Self::mssql_to_mysql(stmt),
            (SqlDialect::Mssql, SqlDialect::Postgres) => Self::mssql_to_postgres(stmt),
            (SqlDialect::Mssql, SqlDialect::Sqlite) => Self::mssql_to_sqlite(stmt),
            _ => stmt.to_string(),
        }
    }

    /// Convert MySQL types to PostgreSQL
    fn mysql_to_postgres(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // Integer types - strip display width, PostgreSQL doesn't use it
        result = RE_TINYINT_BOOL.replace_all(&result, "BOOLEAN").to_string();
        result = RE_TINYINT.replace_all(&result, "SMALLINT").to_string();
        result = RE_SMALLINT.replace_all(&result, "SMALLINT").to_string();
        result = RE_MEDIUMINT.replace_all(&result, "INTEGER").to_string();
        result = RE_INT_SIZE.replace_all(&result, "INTEGER").to_string();
        result = RE_BIGINT_SIZE.replace_all(&result, "BIGINT").to_string();

        // Float types
        result = RE_DOUBLE
            .replace_all(&result, "DOUBLE PRECISION")
            .to_string();
        result = RE_FLOAT.replace_all(&result, "REAL").to_string();

        // Text types
        result = RE_LONGTEXT.replace_all(&result, "TEXT").to_string();
        result = RE_MEDIUMTEXT.replace_all(&result, "TEXT").to_string();
        result = RE_TINYTEXT.replace_all(&result, "TEXT").to_string();

        // Binary types
        result = RE_LONGBLOB.replace_all(&result, "BYTEA").to_string();
        result = RE_MEDIUMBLOB.replace_all(&result, "BYTEA").to_string();
        result = RE_TINYBLOB.replace_all(&result, "BYTEA").to_string();
        result = RE_BLOB.replace_all(&result, "BYTEA").to_string();
        result = RE_VARBINARY.replace_all(&result, "BYTEA").to_string();
        result = RE_BINARY.replace_all(&result, "BYTEA").to_string();

        // Date/time types
        result = RE_DATETIME.replace_all(&result, "TIMESTAMP").to_string();

        // JSON
        result = RE_JSON.replace_all(&result, "JSONB").to_string();

        // ENUM - convert to VARCHAR (with warning)
        result = RE_ENUM.replace_all(&result, "VARCHAR(255)").to_string();

        // SET - convert to VARCHAR (with warning)
        result = RE_SET.replace_all(&result, "VARCHAR(255)").to_string();

        // UNSIGNED - remove
        result = RE_UNSIGNED.replace_all(&result, "").to_string();

        // ZEROFILL - remove
        result = RE_ZEROFILL.replace_all(&result, "").to_string();

        result
    }

    /// Convert MySQL types to SQLite
    fn mysql_to_sqlite(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // SQLite is lenient with types, but we normalize for consistency

        // Integer types - SQLite uses INTEGER
        result = RE_TINYINT.replace_all(&result, "INTEGER").to_string();
        result = RE_SMALLINT.replace_all(&result, "INTEGER").to_string();
        result = RE_MEDIUMINT.replace_all(&result, "INTEGER").to_string();
        result = RE_INT_SIZE.replace_all(&result, "INTEGER").to_string();
        result = RE_BIGINT_SIZE.replace_all(&result, "INTEGER").to_string();

        // Float types - SQLite uses REAL
        result = RE_DOUBLE.replace_all(&result, "REAL").to_string();
        result = RE_FLOAT.replace_all(&result, "REAL").to_string();
        result = RE_DECIMAL.replace_all(&result, "REAL").to_string();

        // Text types - all become TEXT
        result = RE_LONGTEXT.replace_all(&result, "TEXT").to_string();
        result = RE_MEDIUMTEXT.replace_all(&result, "TEXT").to_string();
        result = RE_TINYTEXT.replace_all(&result, "TEXT").to_string();
        result = RE_VARCHAR.replace_all(&result, "TEXT").to_string();
        result = RE_CHAR.replace_all(&result, "TEXT").to_string();

        // Binary types - SQLite uses BLOB
        result = RE_LONGBLOB.replace_all(&result, "BLOB").to_string();
        result = RE_MEDIUMBLOB.replace_all(&result, "BLOB").to_string();
        result = RE_TINYBLOB.replace_all(&result, "BLOB").to_string();
        result = RE_VARBINARY.replace_all(&result, "BLOB").to_string();
        result = RE_BINARY.replace_all(&result, "BLOB").to_string();

        // Date/time - SQLite stores as TEXT or INTEGER
        result = RE_DATETIME.replace_all(&result, "TEXT").to_string();
        result = RE_TIMESTAMP.replace_all(&result, "TEXT").to_string();
        result = RE_DATE.replace_all(&result, "TEXT").to_string();
        result = RE_TIME.replace_all(&result, "TEXT").to_string();

        // JSON - SQLite stores as TEXT
        result = RE_JSON.replace_all(&result, "TEXT").to_string();

        // ENUM/SET - convert to TEXT
        result = RE_ENUM.replace_all(&result, "TEXT").to_string();
        result = RE_SET.replace_all(&result, "TEXT").to_string();

        // UNSIGNED - remove
        result = RE_UNSIGNED.replace_all(&result, "").to_string();

        // ZEROFILL - remove
        result = RE_ZEROFILL.replace_all(&result, "").to_string();

        result
    }

    /// Convert PostgreSQL types to MySQL
    fn postgres_to_mysql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // SERIAL → INT AUTO_INCREMENT
        result = RE_BIGSERIAL
            .replace_all(&result, "BIGINT AUTO_INCREMENT")
            .to_string();
        result = RE_SERIAL
            .replace_all(&result, "INT AUTO_INCREMENT")
            .to_string();
        result = RE_SMALLSERIAL
            .replace_all(&result, "SMALLINT AUTO_INCREMENT")
            .to_string();

        // BYTEA → LONGBLOB
        result = RE_BYTEA.replace_all(&result, "LONGBLOB").to_string();

        // DOUBLE PRECISION → DOUBLE
        result = RE_DOUBLE_PRECISION
            .replace_all(&result, "DOUBLE")
            .to_string();

        // REAL → FLOAT
        result = RE_REAL.replace_all(&result, "FLOAT").to_string();

        // BOOLEAN → TINYINT(1)
        result = RE_BOOLEAN.replace_all(&result, "TINYINT(1)").to_string();

        // TIMESTAMPTZ → DATETIME
        result = RE_TIMESTAMPTZ.replace_all(&result, "DATETIME").to_string();

        // TIMESTAMP WITH TIME ZONE → DATETIME
        result = RE_TIMESTAMP_WITH_TZ
            .replace_all(&result, "DATETIME")
            .to_string();

        // TIMESTAMP WITHOUT TIME ZONE → DATETIME
        result = RE_TIMESTAMP_NO_TZ
            .replace_all(&result, "DATETIME")
            .to_string();

        // JSONB → JSON
        result = RE_JSONB.replace_all(&result, "JSON").to_string();

        // UUID → VARCHAR(36)
        result = RE_UUID.replace_all(&result, "VARCHAR(36)").to_string();

        result
    }

    /// Convert PostgreSQL types to SQLite
    fn postgres_to_sqlite(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // SERIAL → INTEGER (SQLite auto-increments INTEGER PRIMARY KEY)
        result = RE_BIGSERIAL.replace_all(&result, "INTEGER").to_string();
        result = RE_SERIAL.replace_all(&result, "INTEGER").to_string();
        result = RE_SMALLSERIAL.replace_all(&result, "INTEGER").to_string();

        // BYTEA → BLOB
        result = RE_BYTEA.replace_all(&result, "BLOB").to_string();

        // DOUBLE PRECISION → REAL
        result = RE_DOUBLE_PRECISION.replace_all(&result, "REAL").to_string();

        // BOOLEAN → INTEGER
        result = RE_BOOLEAN.replace_all(&result, "INTEGER").to_string();

        // Timestamps → TEXT
        result = RE_TIMESTAMPTZ.replace_all(&result, "TEXT").to_string();
        result = RE_TIMESTAMP_WITH_TZ
            .replace_all(&result, "TEXT")
            .to_string();
        result = RE_TIMESTAMP_NO_TZ.replace_all(&result, "TEXT").to_string();

        // JSONB/JSON → TEXT
        result = RE_JSONB.replace_all(&result, "TEXT").to_string();
        result = RE_JSON.replace_all(&result, "TEXT").to_string();

        // UUID → TEXT
        result = RE_UUID.replace_all(&result, "TEXT").to_string();

        // VARCHAR → TEXT
        result = RE_VARCHAR.replace_all(&result, "TEXT").to_string();

        result
    }

    /// Convert SQLite types to MySQL
    fn sqlite_to_mysql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // SQLite uses TEXT for everything, but we can preserve some type info
        // REAL → DOUBLE
        result = RE_REAL.replace_all(&result, "DOUBLE").to_string();

        // BLOB stays BLOB
        // TEXT stays TEXT
        // INTEGER stays INTEGER (MySQL will handle it)

        result
    }

    /// Convert SQLite types to PostgreSQL
    fn sqlite_to_postgres(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // REAL → DOUBLE PRECISION
        result = RE_REAL.replace_all(&result, "DOUBLE PRECISION").to_string();

        // BLOB → BYTEA
        result = RE_BLOB.replace_all(&result, "BYTEA").to_string();

        // INTEGER stays INTEGER
        // TEXT stays TEXT

        result
    }

    /// Convert MySQL types to MSSQL
    fn mysql_to_mssql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // AUTO_INCREMENT → IDENTITY(1,1) (handled elsewhere in convert_auto_increment)

        // Integer types - strip display width
        result = RE_TINYINT_BOOL.replace_all(&result, "BIT").to_string();
        result = RE_TINYINT.replace_all(&result, "TINYINT").to_string();
        result = RE_SMALLINT.replace_all(&result, "SMALLINT").to_string();
        result = RE_MEDIUMINT.replace_all(&result, "INT").to_string();
        result = RE_INT_SIZE.replace_all(&result, "INT").to_string();
        result = RE_BIGINT_SIZE.replace_all(&result, "BIGINT").to_string();

        // Float types
        result = RE_DOUBLE.replace_all(&result, "FLOAT").to_string();
        result = RE_FLOAT.replace_all(&result, "REAL").to_string();

        // Text types
        result = RE_LONGTEXT.replace_all(&result, "NVARCHAR(MAX)").to_string();
        result = RE_MEDIUMTEXT.replace_all(&result, "NVARCHAR(MAX)").to_string();
        result = RE_TINYTEXT.replace_all(&result, "NVARCHAR(255)").to_string();

        // Binary types
        result = RE_LONGBLOB.replace_all(&result, "VARBINARY(MAX)").to_string();
        result = RE_MEDIUMBLOB.replace_all(&result, "VARBINARY(MAX)").to_string();
        result = RE_TINYBLOB.replace_all(&result, "VARBINARY(255)").to_string();
        result = RE_BLOB.replace_all(&result, "VARBINARY(MAX)").to_string();

        // Date/time types
        result = RE_DATETIME.replace_all(&result, "DATETIME2").to_string();

        // JSON → NVARCHAR(MAX)
        result = RE_JSON.replace_all(&result, "NVARCHAR(MAX)").to_string();

        // ENUM → NVARCHAR(255)
        result = RE_ENUM.replace_all(&result, "NVARCHAR(255)").to_string();

        // SET → NVARCHAR(255)
        result = RE_SET.replace_all(&result, "NVARCHAR(255)").to_string();

        // UNSIGNED - remove
        result = RE_UNSIGNED.replace_all(&result, "").to_string();

        // ZEROFILL - remove
        result = RE_ZEROFILL.replace_all(&result, "").to_string();

        result
    }

    /// Convert PostgreSQL types to MSSQL
    fn postgres_to_mssql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // SERIAL → INT IDENTITY(1,1)
        result = RE_BIGSERIAL
            .replace_all(&result, "BIGINT IDENTITY(1,1)")
            .to_string();
        result = RE_SERIAL
            .replace_all(&result, "INT IDENTITY(1,1)")
            .to_string();
        result = RE_SMALLSERIAL
            .replace_all(&result, "SMALLINT IDENTITY(1,1)")
            .to_string();

        // BYTEA → VARBINARY(MAX)
        result = RE_BYTEA.replace_all(&result, "VARBINARY(MAX)").to_string();

        // DOUBLE PRECISION → FLOAT
        result = RE_DOUBLE_PRECISION.replace_all(&result, "FLOAT").to_string();

        // REAL stays REAL

        // BOOLEAN → BIT
        result = RE_BOOLEAN.replace_all(&result, "BIT").to_string();

        // TIMESTAMPTZ → DATETIMEOFFSET
        result = RE_TIMESTAMPTZ.replace_all(&result, "DATETIMEOFFSET").to_string();

        // TIMESTAMP WITH TIME ZONE → DATETIMEOFFSET
        result = RE_TIMESTAMP_WITH_TZ
            .replace_all(&result, "DATETIMEOFFSET")
            .to_string();

        // TIMESTAMP WITHOUT TIME ZONE → DATETIME2
        result = RE_TIMESTAMP_NO_TZ
            .replace_all(&result, "DATETIME2")
            .to_string();

        // JSONB → NVARCHAR(MAX)
        result = RE_JSONB.replace_all(&result, "NVARCHAR(MAX)").to_string();

        // JSON → NVARCHAR(MAX)
        result = RE_JSON.replace_all(&result, "NVARCHAR(MAX)").to_string();

        // UUID → UNIQUEIDENTIFIER
        result = RE_UUID.replace_all(&result, "UNIQUEIDENTIFIER").to_string();

        // TEXT → NVARCHAR(MAX)
        result = RE_TEXT.replace_all(&result, "NVARCHAR(MAX)").to_string();

        result
    }

    /// Convert SQLite types to MSSQL
    fn sqlite_to_mssql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // REAL → FLOAT
        result = RE_REAL.replace_all(&result, "FLOAT").to_string();

        // BLOB → VARBINARY(MAX)
        result = RE_BLOB.replace_all(&result, "VARBINARY(MAX)").to_string();

        // TEXT → NVARCHAR(MAX)
        result = RE_TEXT.replace_all(&result, "NVARCHAR(MAX)").to_string();

        result
    }

    /// Convert MSSQL types to MySQL
    fn mssql_to_mysql(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // IDENTITY → AUTO_INCREMENT (handled elsewhere)

        // BIT → TINYINT(1)
        result = RE_BIT.replace_all(&result, "TINYINT(1)").to_string();

        // NVARCHAR(MAX) → LONGTEXT
        result = RE_NVARCHAR_MAX.replace_all(&result, "LONGTEXT").to_string();

        // NVARCHAR(n) → VARCHAR(n)
        result = RE_NVARCHAR.replace_all(&result, "VARCHAR$1").to_string();

        // NCHAR(n) → CHAR(n)
        result = RE_NCHAR.replace_all(&result, "CHAR$1").to_string();

        // NTEXT → LONGTEXT
        result = RE_NTEXT.replace_all(&result, "LONGTEXT").to_string();

        // VARCHAR(MAX) → LONGTEXT
        result = RE_VARCHAR_MAX.replace_all(&result, "LONGTEXT").to_string();

        // VARBINARY(MAX) → LONGBLOB
        result = RE_VARBINARY_MAX.replace_all(&result, "LONGBLOB").to_string();

        // IMAGE → LONGBLOB
        result = RE_IMAGE.replace_all(&result, "LONGBLOB").to_string();

        // DATETIME2 → DATETIME(6)
        result = RE_DATETIME2.replace_all(&result, "DATETIME(6)").to_string();

        // DATETIMEOFFSET → DATETIME
        result = RE_DATETIMEOFFSET.replace_all(&result, "DATETIME").to_string();

        // SMALLDATETIME → DATETIME
        result = RE_SMALLDATETIME.replace_all(&result, "DATETIME").to_string();

        // MONEY → DECIMAL(19,4)
        result = RE_MONEY.replace_all(&result, "DECIMAL(19,4)").to_string();

        // SMALLMONEY → DECIMAL(10,4)
        result = RE_SMALLMONEY.replace_all(&result, "DECIMAL(10,4)").to_string();

        // UNIQUEIDENTIFIER → VARCHAR(36)
        result = RE_UNIQUEIDENTIFIER
            .replace_all(&result, "VARCHAR(36)")
            .to_string();

        // XML → LONGTEXT
        result = RE_XML.replace_all(&result, "LONGTEXT").to_string();

        // ROWVERSION/MSSQL TIMESTAMP → BINARY(8)
        result = RE_MSSQL_TIMESTAMP_BRACKETED.replace_all(&result, "BINARY(8)").to_string();
        result = RE_ROWVERSION_ONLY.replace_all(&result, "BINARY(8)").to_string();

        // Strip MSSQL-specific clauses
        result = RE_ON_PRIMARY.replace_all(&result, "").to_string();
        result = RE_CLUSTERED.replace_all(&result, "").to_string();
        result = RE_NONCLUSTERED.replace_all(&result, "").to_string();

        result
    }

    /// Convert MSSQL types to PostgreSQL
    fn mssql_to_postgres(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // IDENTITY → SERIAL (handled elsewhere)

        // IMPORTANT: Handle ROWVERSION first (before any TIMESTAMP conversion)
        // In MSSQL, TIMESTAMP is an alias for ROWVERSION (a binary type, not datetime!)
        // Use a more specific regex that matches MSSQL TIMESTAMP but not PostgreSQL TIMESTAMP
        result = RE_MSSQL_TIMESTAMP_BRACKETED.replace_all(&result, "BYTEA").to_string();
        result = RE_ROWVERSION_ONLY
            .replace_all(&result, "BYTEA")
            .to_string();

        // BIT → BOOLEAN
        result = RE_BIT.replace_all(&result, "BOOLEAN").to_string();

        // NVARCHAR(MAX) → TEXT
        result = RE_NVARCHAR_MAX.replace_all(&result, "TEXT").to_string();

        // NVARCHAR(n) → VARCHAR(n)
        result = RE_NVARCHAR.replace_all(&result, "VARCHAR$1").to_string();

        // NCHAR(n) → CHAR(n)
        result = RE_NCHAR.replace_all(&result, "CHAR$1").to_string();

        // NTEXT → TEXT
        result = RE_NTEXT.replace_all(&result, "TEXT").to_string();

        // VARCHAR(MAX) → TEXT
        result = RE_VARCHAR_MAX.replace_all(&result, "TEXT").to_string();

        // VARBINARY(MAX) → BYTEA
        result = RE_VARBINARY_MAX.replace_all(&result, "BYTEA").to_string();

        // VARBINARY(n) → BYTEA
        result = RE_VARBINARY.replace_all(&result, "BYTEA").to_string();

        // IMAGE → BYTEA
        result = RE_IMAGE.replace_all(&result, "BYTEA").to_string();

        // DATETIME2 → TIMESTAMP
        result = RE_DATETIME2.replace_all(&result, "TIMESTAMP").to_string();

        // DATETIME → TIMESTAMP (but not MSSQL TIMESTAMP which is already converted)
        result = RE_DATETIME.replace_all(&result, "TIMESTAMP").to_string();

        // DATETIMEOFFSET → TIMESTAMPTZ
        result = RE_DATETIMEOFFSET.replace_all(&result, "TIMESTAMPTZ").to_string();

        // SMALLDATETIME → TIMESTAMP
        result = RE_SMALLDATETIME.replace_all(&result, "TIMESTAMP").to_string();

        // MONEY → DECIMAL(19,4)
        result = RE_MONEY.replace_all(&result, "DECIMAL(19,4)").to_string();

        // SMALLMONEY → DECIMAL(10,4)
        result = RE_SMALLMONEY.replace_all(&result, "DECIMAL(10,4)").to_string();

        // UNIQUEIDENTIFIER → UUID
        result = RE_UNIQUEIDENTIFIER.replace_all(&result, "UUID").to_string();

        // XML → XML (PostgreSQL supports XML type)

        // FLOAT → DOUBLE PRECISION
        result = RE_FLOAT.replace_all(&result, "DOUBLE PRECISION").to_string();

        // Strip MSSQL-specific clauses
        result = RE_ON_PRIMARY.replace_all(&result, "").to_string();
        result = RE_CLUSTERED.replace_all(&result, "").to_string();
        result = RE_NONCLUSTERED.replace_all(&result, "").to_string();

        result
    }

    /// Convert MSSQL types to SQLite
    fn mssql_to_sqlite(stmt: &str) -> String {
        let mut result = stmt.to_string();

        // BIT → INTEGER
        result = RE_BIT.replace_all(&result, "INTEGER").to_string();

        // NVARCHAR → TEXT
        result = RE_NVARCHAR_MAX.replace_all(&result, "TEXT").to_string();
        result = RE_NVARCHAR.replace_all(&result, "TEXT").to_string();

        // NCHAR → TEXT
        result = RE_NCHAR.replace_all(&result, "TEXT").to_string();

        // NTEXT → TEXT
        result = RE_NTEXT.replace_all(&result, "TEXT").to_string();

        // VARCHAR(MAX) → TEXT
        result = RE_VARCHAR_MAX.replace_all(&result, "TEXT").to_string();

        // VARBINARY → BLOB
        result = RE_VARBINARY_MAX.replace_all(&result, "BLOB").to_string();
        result = RE_VARBINARY.replace_all(&result, "BLOB").to_string();

        // IMAGE → BLOB
        result = RE_IMAGE.replace_all(&result, "BLOB").to_string();

        // Date/time → TEXT
        result = RE_DATETIME2.replace_all(&result, "TEXT").to_string();
        result = RE_DATETIME.replace_all(&result, "TEXT").to_string();
        result = RE_DATETIMEOFFSET.replace_all(&result, "TEXT").to_string();
        result = RE_SMALLDATETIME.replace_all(&result, "TEXT").to_string();

        // MONEY → REAL
        result = RE_MONEY.replace_all(&result, "REAL").to_string();
        result = RE_SMALLMONEY.replace_all(&result, "REAL").to_string();

        // UNIQUEIDENTIFIER → TEXT
        result = RE_UNIQUEIDENTIFIER.replace_all(&result, "TEXT").to_string();

        // XML → TEXT
        result = RE_XML.replace_all(&result, "TEXT").to_string();

        // ROWVERSION/MSSQL TIMESTAMP → BLOB
        result = RE_MSSQL_TIMESTAMP_BRACKETED.replace_all(&result, "BLOB").to_string();
        result = RE_ROWVERSION_ONLY.replace_all(&result, "BLOB").to_string();

        // FLOAT → REAL
        result = RE_FLOAT.replace_all(&result, "REAL").to_string();

        // Strip MSSQL-specific clauses
        result = RE_ON_PRIMARY.replace_all(&result, "").to_string();
        result = RE_CLUSTERED.replace_all(&result, "").to_string();
        result = RE_NONCLUSTERED.replace_all(&result, "").to_string();

        result
    }
}

// Pre-compiled regexes for type matching
static RE_TINYINT_BOOL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTINYINT\s*\(\s*1\s*\)").unwrap());
static RE_TINYINT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTINYINT\s*(\(\s*\d+\s*\))?").unwrap());
static RE_SMALLINT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bSMALLINT\s*(\(\s*\d+\s*\))?").unwrap());
static RE_MEDIUMINT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bMEDIUMINT\s*(\(\s*\d+\s*\))?").unwrap());
static RE_INT_SIZE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bINT\s*\(\s*\d+\s*\)").unwrap());
static RE_BIGINT_SIZE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bBIGINT\s*\(\s*\d+\s*\)").unwrap());

static RE_DOUBLE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bDOUBLE\b").unwrap());
static RE_FLOAT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bFLOAT\s*(\(\s*\d+\s*(,\s*\d+\s*)?\))?").unwrap());
static RE_DECIMAL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDECIMAL\s*\(\s*\d+\s*(,\s*\d+\s*)?\)").unwrap());

static RE_LONGTEXT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bLONGTEXT\b").unwrap());
static RE_MEDIUMTEXT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bMEDIUMTEXT\b").unwrap());
static RE_TINYTEXT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bTINYTEXT\b").unwrap());
static RE_VARCHAR: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bVARCHAR\s*\(\s*\d+\s*\)").unwrap());
static RE_CHAR: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bCHAR\s*\(\s*\d+\s*\)").unwrap());

static RE_LONGBLOB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bLONGBLOB\b").unwrap());
static RE_MEDIUMBLOB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bMEDIUMBLOB\b").unwrap());
static RE_TINYBLOB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bTINYBLOB\b").unwrap());
static RE_BLOB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBLOB\b").unwrap());
static RE_VARBINARY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bVARBINARY\s*\(\s*\d+\s*\)").unwrap());
static RE_BINARY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBINARY\s*\(\s*\d+\s*\)").unwrap());

static RE_DATETIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDATETIME(\(\s*\d+\s*\))?").unwrap());
static RE_TIMESTAMP: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTIMESTAMP\s*(\(\s*\d+\s*\))?").unwrap());
static RE_DATE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bDATE\b").unwrap());
static RE_TIME: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bTIME\s*(\(\s*\d+\s*\))?").unwrap());

static RE_JSON: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bJSON\b").unwrap());

static RE_ENUM: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bENUM\s*\([^)]+\)").unwrap());
static RE_SET: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSET\s*\([^)]+\)").unwrap());

static RE_UNSIGNED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\s+UNSIGNED\b").unwrap());
static RE_ZEROFILL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\s+ZEROFILL\b").unwrap());

// PostgreSQL specific types
static RE_SERIAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSERIAL\b").unwrap());
static RE_BIGSERIAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBIGSERIAL\b").unwrap());
static RE_SMALLSERIAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSMALLSERIAL\b").unwrap());
static RE_BYTEA: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBYTEA\b").unwrap());
static RE_DOUBLE_PRECISION: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDOUBLE\s+PRECISION\b").unwrap());
static RE_REAL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bREAL\b").unwrap());
static RE_BOOLEAN: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBOOLEAN\b").unwrap());
static RE_TIMESTAMPTZ: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bTIMESTAMPTZ\b").unwrap());
static RE_TIMESTAMP_WITH_TZ: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTIMESTAMP\s+WITH\s+TIME\s+ZONE\b").unwrap());
static RE_TIMESTAMP_NO_TZ: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bTIMESTAMP\s+WITHOUT\s+TIME\s+ZONE\b").unwrap());
static RE_JSONB: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bJSONB\b").unwrap());
static RE_UUID: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bUUID\b").unwrap());
static RE_TEXT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bTEXT\b").unwrap());

// MSSQL specific types
static RE_BIT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bBIT\b").unwrap());
static RE_NVARCHAR_MAX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bNVARCHAR\s*\(\s*MAX\s*\)").unwrap());
static RE_NVARCHAR: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bNVARCHAR\s*(\(\s*\d+\s*\))").unwrap());
static RE_NCHAR: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bNCHAR\s*(\(\s*\d+\s*\))").unwrap());
static RE_NTEXT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNTEXT\b").unwrap());
static RE_VARCHAR_MAX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bVARCHAR\s*\(\s*MAX\s*\)").unwrap());
static RE_VARBINARY_MAX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bVARBINARY\s*\(\s*MAX\s*\)").unwrap());
static RE_IMAGE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bIMAGE\b").unwrap());
static RE_DATETIME2: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDATETIME2\s*(\(\s*\d+\s*\))?").unwrap());
static RE_DATETIMEOFFSET: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bDATETIMEOFFSET\s*(\(\s*\d+\s*\))?").unwrap());
static RE_SMALLDATETIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bSMALLDATETIME\b").unwrap());
static RE_MONEY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bMONEY\b").unwrap());
static RE_SMALLMONEY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSMALLMONEY\b").unwrap());
static RE_UNIQUEIDENTIFIER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bUNIQUEIDENTIFIER\b").unwrap());
static RE_XML: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bXML\b").unwrap());
// MSSQL TIMESTAMP type (binary versioning, NOT datetime) - only match bracketed [TIMESTAMP]
// or as column type after brackets. We can't match unbracketed standalone TIMESTAMP safely
// because it would conflict with PostgreSQL TIMESTAMP result. So we rely on context.
static RE_MSSQL_TIMESTAMP_BRACKETED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[\s*TIMESTAMP\s*\]").unwrap());
static RE_ROWVERSION_ONLY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bROWVERSION\b").unwrap());

// MSSQL-specific clauses to strip when converting to other dialects
static RE_ON_PRIMARY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\s*ON\s*\[\s*PRIMARY\s*\]").unwrap());
static RE_CLUSTERED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bCLUSTERED\s+").unwrap());
static RE_NONCLUSTERED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bNONCLUSTERED\s+").unwrap());
