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
            (SqlDialect::Postgres, SqlDialect::MySql) => Self::postgres_to_mysql(stmt),
            (SqlDialect::Postgres, SqlDialect::Sqlite) => Self::postgres_to_sqlite(stmt),
            (SqlDialect::Sqlite, SqlDialect::MySql) => Self::sqlite_to_mysql(stmt),
            (SqlDialect::Sqlite, SqlDialect::Postgres) => Self::sqlite_to_postgres(stmt),
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
