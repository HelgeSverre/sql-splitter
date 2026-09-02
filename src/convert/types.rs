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
use regex::{Captures, Regex};
use std::borrow::Cow;

use super::warnings::{ConvertWarning, WarningCollector};

/// Map a single column's `source_type` from `from` to `to`, reusing the same
/// regex-driven rules [`TypeMapper::convert`] applies to a whole statement.
///
/// This is the seam the synthetic-data renderer (`render::ddl`) calls
/// instead of maintaining its own generation-only type mapping: a column's
/// `source_type` (e.g. `"VARCHAR(255)"`) is itself a valid input to
/// [`TypeMapper::convert`], since every rule matches on the type token with
/// word boundaries rather than requiring surrounding `CREATE TABLE` context.
///
/// Same-dialect conversions are the identity (returned as-is, no warning).
/// A conversion that narrows a MySQL `ENUM`/`SET` to a plain string type
/// records a [`ConvertWarning::LossyConversion`] so callers that surface
/// warnings (e.g. `--emit-config`) can report it.
pub(crate) fn map_column_type(
    source_type: &str,
    from: SqlDialect,
    to: SqlDialect,
    warnings: &mut WarningCollector,
) -> String {
    if from == to {
        return source_type.to_string();
    }
    let mapped = TypeMapper::convert(source_type, from, to);
    if is_narrowed_by_conversion(source_type, to) {
        warnings.add(ConvertWarning::LossyConversion {
            from_type: source_type.to_string(),
            to_type: mapped.clone(),
            table: None,
            column: None,
        });
    }
    mapped
}

/// Whether mapping `source_type` to the `to` dialect loses type semantics the
/// target cannot preserve — a lossy conversion the caller should warn about (and
/// fail under `--strict`):
///
/// * Inline `ENUM`/`SET` mapping always collapses to a plain string. The
///   statement converter handles PostgreSQL↔MySQL enum preservation before it
///   calls this mapper.
/// * `JSON`/`JSONB` become an unvalidated text column on engines without a JSON
///   type (SQLite, MSSQL).
/// * `UUID`/`UNIQUEIDENTIFIER` lose their fixed 128-bit domain when stored as
///   text (MySQL, SQLite).
fn is_narrowed_by_conversion(source_type: &str, to: SqlDialect) -> bool {
    let lower = source_type.to_lowercase();
    if lower.contains("enum(") || lower.contains("set(") {
        return true;
    }
    if lower.contains("json") && matches!(to, SqlDialect::Sqlite | SqlDialect::Mssql) {
        return true;
    }
    if (lower.contains("uuid") || lower.contains("uniqueidentifier"))
        && matches!(to, SqlDialect::MySql | SqlDialect::Sqlite)
    {
        return true;
    }
    // An unbounded VARCHAR holds up to 1GB in PostgreSQL and is unlimited in
    // SQLite, but MySQL and SQL Server both require a width and have no
    // unbounded alternative legal in every position, so it maps to
    // VARCHAR(255) — anything longer is truncated at load time.
    if is_unbounded_varchar(source_type) && matches!(to, SqlDialect::MySql | SqlDialect::Mssql) {
        return true;
    }
    // Exact fixed-point (DECIMAL/NUMERIC) becomes a binary float (REAL) on
    // SQLite, silently losing precision.
    if (lower.contains("decimal") || lower.contains("numeric")) && to == SqlDialect::Sqlite {
        return true;
    }
    // A time-zone-bearing timestamp loses its zone when mapped to MySQL
    // DATETIME (which has no tz). MSSQL keeps it via DATETIMEOFFSET, and the
    // SQLite mapping renders TEXT that retains the zoned literal.
    // ("without time zone" does not contain the substring "with time zone".)
    if (lower.contains("timestamptz") || lower.contains("with time zone"))
        && to == SqlDialect::MySql
    {
        return true;
    }
    false
}

/// Whether `source_type` is a variable-length string type declared without a
/// length (`VARCHAR`, `CHARACTER VARYING`, …) — the form PostgreSQL allows and
/// MySQL does not.
fn is_unbounded_varchar(source_type: &str) -> bool {
    RE_VARCHAR_DECL
        .captures(source_type)
        .is_some_and(|caps| caps.name("len").is_none())
}

/// Give every length-less VARCHAR declaration an explicit width, for targets
/// that cannot store one without.
///
/// 255 rather than an unbounded type because every unbounded alternative is
/// invalid in some position a column can occupy: MySQL `TEXT` cannot carry a
/// DEFAULT (ERROR 1101) or be a key without a prefix length (ERROR 1170), and
/// SQL Server `VARCHAR(MAX)` cannot be an index key at all. pg_dump emits
/// PRIMARY KEY and UNIQUE as separate ALTER statements, so the column
/// definition alone never reveals whether a key is coming — only a width valid
/// everywhere is safe. It also matches the ENUM and UUID rules alongside it.
///
/// Declarations that already carry a length are left exactly as they are, as is
/// a `::` cast: those are removed wholesale by `strip_postgres_casts`, which
/// matches bare identifiers and would leave a dangling `(255)` behind.
///
/// ponytail: fixed 255 truncates longer values (`map_column_type` records a
/// LossyConversion for it); a configurable width is the upgrade path.
fn size_unbounded_varchar(stmt: &str) -> String {
    RE_VARCHAR_DECL
        .replace_all(stmt, |caps: &Captures| {
            if caps.name("cast").is_some() || caps.name("len").is_some() {
                caps[0].to_string()
            } else {
                "VARCHAR(255)".to_string()
            }
        })
        .to_string()
}

/// Type mapper for converting between dialects
pub struct TypeMapper;

impl TypeMapper {
    /// Convert all data types in a statement
    pub fn convert(stmt: &str, from: SqlDialect, to: SqlDialect) -> String {
        let converted = Self::convert_dialect_pair(stmt, from, to);
        if from == to {
            return converted;
        }

        // MySQL and SQL Server both require an explicit length on VARCHAR, and
        // they fail differently: MySQL rejects a bare declaration with ERROR
        // 1064, while SQL Server quietly reads it as VARCHAR(1) and then throws
        // Msg 2628 on the first row that does not fit. Applied per target rather
        // than per dialect pair because every source that permits an unbounded
        // declaration — PostgreSQL and SQLite — reaches both of them.
        match to {
            SqlDialect::MySql | SqlDialect::Mssql => size_unbounded_varchar(&converted),
            _ => converted,
        }
    }

    fn convert_dialect_pair(stmt: &str, from: SqlDialect, to: SqlDialect) -> String {
        match (from, to) {
            (SqlDialect::MySql, SqlDialect::Postgres) => apply(MYSQL_TO_POSTGRES, stmt),
            (SqlDialect::MySql, SqlDialect::Sqlite) => apply(MYSQL_TO_SQLITE, stmt),
            (SqlDialect::MySql, SqlDialect::Mssql) => apply(MYSQL_TO_MSSQL, stmt),
            (SqlDialect::Postgres, SqlDialect::MySql) => apply(POSTGRES_TO_MYSQL, stmt),
            (SqlDialect::Postgres, SqlDialect::Sqlite) => apply(POSTGRES_TO_SQLITE, stmt),
            (SqlDialect::Postgres, SqlDialect::Mssql) => apply(POSTGRES_TO_MSSQL, stmt),
            (SqlDialect::Sqlite, SqlDialect::MySql) => apply(SQLITE_TO_MYSQL, stmt),
            (SqlDialect::Sqlite, SqlDialect::Postgres) => apply(SQLITE_TO_POSTGRES, stmt),
            (SqlDialect::Sqlite, SqlDialect::Mssql) => apply(SQLITE_TO_MSSQL, stmt),
            (SqlDialect::Mssql, SqlDialect::MySql) => apply(MSSQL_TO_MYSQL, stmt),
            (SqlDialect::Mssql, SqlDialect::Postgres) => apply(MSSQL_TO_POSTGRES, stmt),
            (SqlDialect::Mssql, SqlDialect::Sqlite) => apply(MSSQL_TO_SQLITE, stmt),
            _ => stmt.to_string(),
        }
    }
}

/// One ordered rewrite table: each regex is applied to the output of the
/// previous one, so order within a table is load-bearing.
type Rules = &'static [(&'static Lazy<Regex>, &'static str)];

/// Apply `rules` in order. Borrows `stmt` until a rule actually matches, so a
/// statement that no rule touches costs one allocation, not one per rule.
fn apply(rules: Rules, stmt: &str) -> String {
    let mut result = Cow::Borrowed(stmt);
    for (re, rep) in rules {
        if let Cow::Owned(s) = re.replace_all(&result, *rep) {
            result = Cow::Owned(s);
        }
    }
    result.into_owned()
}

/// MySQL → PostgreSQL. Integer display widths are stripped; UNSIGNED/ZEROFILL dropped.
static MYSQL_TO_POSTGRES: Rules = &[
    (&RE_TINYINT_BOOL, "BOOLEAN"),
    (&RE_TINYINT, "SMALLINT"),
    (&RE_SMALLINT, "SMALLINT"),
    (&RE_MEDIUMINT, "INTEGER"),
    (&RE_INT_SIZE, "INTEGER"),
    (&RE_BIGINT_SIZE, "BIGINT"),
    (&RE_DOUBLE, "DOUBLE PRECISION"),
    (&RE_FLOAT, "REAL"),
    (&RE_LONGTEXT, "TEXT"),
    (&RE_MEDIUMTEXT, "TEXT"),
    (&RE_TINYTEXT, "TEXT"),
    (&RE_LONGBLOB, "BYTEA"),
    (&RE_MEDIUMBLOB, "BYTEA"),
    (&RE_TINYBLOB, "BYTEA"),
    (&RE_BLOB, "BYTEA"),
    (&RE_VARBINARY, "BYTEA"),
    (&RE_BINARY, "BYTEA"),
    (&RE_DATETIME, "TIMESTAMP"),
    (&RE_JSON, "JSONB"),
    // ENUM/SET → VARCHAR (lossy, warned by map_column_type)
    (&RE_ENUM, "VARCHAR(255)"),
    (&RE_SET, "VARCHAR(255)"),
    (&RE_UNSIGNED, ""),
    (&RE_ZEROFILL, ""),
];

/// MySQL → SQLite. SQLite is lenient with types; normalized for consistency.
static MYSQL_TO_SQLITE: Rules = &[
    (&RE_TINYINT, "INTEGER"),
    (&RE_SMALLINT, "INTEGER"),
    (&RE_MEDIUMINT, "INTEGER"),
    (&RE_INT_SIZE, "INTEGER"),
    (&RE_BIGINT_SIZE, "INTEGER"),
    (&RE_DOUBLE, "REAL"),
    (&RE_FLOAT, "REAL"),
    (&RE_DECIMAL, "REAL"),
    (&RE_LONGTEXT, "TEXT"),
    (&RE_MEDIUMTEXT, "TEXT"),
    (&RE_TINYTEXT, "TEXT"),
    (&RE_VARCHAR, "TEXT"),
    (&RE_CHAR, "TEXT"),
    (&RE_LONGBLOB, "BLOB"),
    (&RE_MEDIUMBLOB, "BLOB"),
    (&RE_TINYBLOB, "BLOB"),
    (&RE_VARBINARY, "BLOB"),
    (&RE_BINARY, "BLOB"),
    (&RE_DATETIME, "TEXT"),
    (&RE_TIMESTAMP, "TEXT"),
    (&RE_DATE, "TEXT"),
    (&RE_TIME, "TEXT"),
    (&RE_JSON, "TEXT"),
    (&RE_ENUM, "TEXT"),
    (&RE_SET, "TEXT"),
    (&RE_UNSIGNED, ""),
    (&RE_ZEROFILL, ""),
];

/// PostgreSQL → MySQL.
static POSTGRES_TO_MYSQL: Rules = &[
    (&RE_BIGSERIAL, "BIGINT AUTO_INCREMENT"),
    (&RE_SERIAL, "INT AUTO_INCREMENT"),
    (&RE_SMALLSERIAL, "SMALLINT AUTO_INCREMENT"),
    (&RE_BYTEA, "LONGBLOB"),
    (&RE_DOUBLE_PRECISION, "DOUBLE"),
    (&RE_REAL, "FLOAT"),
    (&RE_BOOLEAN, "TINYINT(1)"),
    (&RE_TIMESTAMPTZ, "DATETIME"),
    (&RE_TIMESTAMP_WITH_TZ, "DATETIME"),
    (&RE_TIMESTAMP_NO_TZ, "DATETIME"),
    (&RE_JSONB, "JSON"),
    (&RE_UUID, "VARCHAR(36)"),
];

/// PostgreSQL → SQLite. SERIAL → INTEGER (SQLite auto-increments INTEGER PRIMARY KEY).
static POSTGRES_TO_SQLITE: Rules = &[
    (&RE_BIGSERIAL, "INTEGER"),
    (&RE_SERIAL, "INTEGER"),
    (&RE_SMALLSERIAL, "INTEGER"),
    (&RE_BYTEA, "BLOB"),
    (&RE_DOUBLE_PRECISION, "REAL"),
    (&RE_BOOLEAN, "INTEGER"),
    (&RE_TIMESTAMPTZ, "TEXT"),
    (&RE_TIMESTAMP_WITH_TZ, "TEXT"),
    (&RE_TIMESTAMP_NO_TZ, "TEXT"),
    (&RE_JSONB, "TEXT"),
    (&RE_JSON, "TEXT"),
    (&RE_UUID, "TEXT"),
    (&RE_VARCHAR, "TEXT"),
];

/// SQLite → MySQL. BLOB/TEXT/INTEGER pass through.
static SQLITE_TO_MYSQL: Rules = &[(&RE_REAL, "DOUBLE")];

/// SQLite → PostgreSQL. INTEGER/TEXT pass through.
static SQLITE_TO_POSTGRES: Rules = &[(&RE_REAL, "DOUBLE PRECISION"), (&RE_BLOB, "BYTEA")];

/// MySQL → MSSQL. AUTO_INCREMENT → IDENTITY is handled in convert_auto_increment.
static MYSQL_TO_MSSQL: Rules = &[
    (&RE_TINYINT_BOOL, "BIT"),
    (&RE_TINYINT, "TINYINT"),
    (&RE_SMALLINT, "SMALLINT"),
    (&RE_MEDIUMINT, "INT"),
    (&RE_INT_SIZE, "INT"),
    (&RE_BIGINT_SIZE, "BIGINT"),
    (&RE_DOUBLE, "FLOAT"),
    (&RE_FLOAT, "REAL"),
    (&RE_LONGTEXT, "NVARCHAR(MAX)"),
    (&RE_MEDIUMTEXT, "NVARCHAR(MAX)"),
    (&RE_TINYTEXT, "NVARCHAR(255)"),
    (&RE_LONGBLOB, "VARBINARY(MAX)"),
    (&RE_MEDIUMBLOB, "VARBINARY(MAX)"),
    (&RE_TINYBLOB, "VARBINARY(255)"),
    (&RE_BLOB, "VARBINARY(MAX)"),
    (&RE_DATETIME, "DATETIME2"),
    (&RE_JSON, "NVARCHAR(MAX)"),
    (&RE_ENUM, "NVARCHAR(255)"),
    (&RE_SET, "NVARCHAR(255)"),
    (&RE_UNSIGNED, ""),
    (&RE_ZEROFILL, ""),
];

/// PostgreSQL → MSSQL. REAL passes through.
static POSTGRES_TO_MSSQL: Rules = &[
    (&RE_BIGSERIAL, "BIGINT IDENTITY(1,1)"),
    (&RE_SERIAL, "INT IDENTITY(1,1)"),
    (&RE_SMALLSERIAL, "SMALLINT IDENTITY(1,1)"),
    (&RE_BYTEA, "VARBINARY(MAX)"),
    (&RE_DOUBLE_PRECISION, "FLOAT"),
    (&RE_BOOLEAN, "BIT"),
    (&RE_TIMESTAMPTZ, "DATETIMEOFFSET"),
    (&RE_TIMESTAMP_WITH_TZ, "DATETIMEOFFSET"),
    (&RE_TIMESTAMP_NO_TZ, "DATETIME2"),
    (&RE_JSONB, "NVARCHAR(MAX)"),
    (&RE_JSON, "NVARCHAR(MAX)"),
    (&RE_UUID, "UNIQUEIDENTIFIER"),
    (&RE_TEXT, "NVARCHAR(MAX)"),
];

/// SQLite → MSSQL.
static SQLITE_TO_MSSQL: Rules = &[
    (&RE_REAL, "FLOAT"),
    (&RE_BLOB, "VARBINARY(MAX)"),
    (&RE_TEXT, "NVARCHAR(MAX)"),
];

/// MSSQL → MySQL. IDENTITY → AUTO_INCREMENT is handled elsewhere.
static MSSQL_TO_MYSQL: Rules = &[
    (&RE_BIT, "TINYINT(1)"),
    (&RE_NVARCHAR_MAX, "LONGTEXT"),
    (&RE_NVARCHAR, "VARCHAR$1"),
    (&RE_NCHAR, "CHAR$1"),
    (&RE_NTEXT, "LONGTEXT"),
    (&RE_VARCHAR_MAX, "LONGTEXT"),
    (&RE_VARBINARY_MAX, "LONGBLOB"),
    (&RE_IMAGE, "LONGBLOB"),
    (&RE_DATETIME2, "DATETIME(6)"),
    (&RE_DATETIMEOFFSET, "DATETIME"),
    (&RE_SMALLDATETIME, "DATETIME"),
    (&RE_MONEY, "DECIMAL(19,4)"),
    (&RE_SMALLMONEY, "DECIMAL(10,4)"),
    (&RE_UNIQUEIDENTIFIER, "VARCHAR(36)"),
    (&RE_XML, "LONGTEXT"),
    // ROWVERSION / MSSQL TIMESTAMP → BINARY(8)
    (&RE_MSSQL_TIMESTAMP_BRACKETED, "BINARY(8)"),
    (&RE_ROWVERSION_ONLY, "BINARY(8)"),
    // Strip MSSQL-specific clauses
    (&RE_ON_PRIMARY, ""),
    (&RE_CLUSTERED, ""),
    (&RE_NONCLUSTERED, ""),
];

/// MSSQL → PostgreSQL. IDENTITY → SERIAL is handled elsewhere; XML passes through.
///
/// ROWVERSION comes first: in MSSQL, TIMESTAMP is an alias for ROWVERSION (a
/// binary type, not a datetime), so it must be rewritten before any
/// DATETIME → TIMESTAMP rule produces a PostgreSQL TIMESTAMP.
static MSSQL_TO_POSTGRES: Rules = &[
    (&RE_MSSQL_TIMESTAMP_BRACKETED, "BYTEA"),
    (&RE_ROWVERSION_ONLY, "BYTEA"),
    (&RE_BIT, "BOOLEAN"),
    (&RE_NVARCHAR_MAX, "TEXT"),
    (&RE_NVARCHAR, "VARCHAR$1"),
    (&RE_NCHAR, "CHAR$1"),
    (&RE_NTEXT, "TEXT"),
    (&RE_VARCHAR_MAX, "TEXT"),
    (&RE_VARBINARY_MAX, "BYTEA"),
    (&RE_VARBINARY, "BYTEA"),
    (&RE_IMAGE, "BYTEA"),
    (&RE_DATETIME2, "TIMESTAMP"),
    (&RE_DATETIME, "TIMESTAMP"),
    (&RE_DATETIMEOFFSET, "TIMESTAMPTZ"),
    (&RE_SMALLDATETIME, "TIMESTAMP"),
    (&RE_MONEY, "DECIMAL(19,4)"),
    (&RE_SMALLMONEY, "DECIMAL(10,4)"),
    (&RE_UNIQUEIDENTIFIER, "UUID"),
    (&RE_FLOAT, "DOUBLE PRECISION"),
    (&RE_ON_PRIMARY, ""),
    (&RE_CLUSTERED, ""),
    (&RE_NONCLUSTERED, ""),
];

/// MSSQL → SQLite.
static MSSQL_TO_SQLITE: Rules = &[
    (&RE_BIT, "INTEGER"),
    (&RE_NVARCHAR_MAX, "TEXT"),
    (&RE_NVARCHAR, "TEXT"),
    (&RE_NCHAR, "TEXT"),
    (&RE_NTEXT, "TEXT"),
    (&RE_VARCHAR_MAX, "TEXT"),
    (&RE_VARBINARY_MAX, "BLOB"),
    (&RE_VARBINARY, "BLOB"),
    (&RE_IMAGE, "BLOB"),
    (&RE_DATETIME2, "TEXT"),
    (&RE_DATETIME, "TEXT"),
    (&RE_DATETIMEOFFSET, "TEXT"),
    (&RE_SMALLDATETIME, "TEXT"),
    (&RE_MONEY, "REAL"),
    (&RE_SMALLMONEY, "REAL"),
    (&RE_UNIQUEIDENTIFIER, "TEXT"),
    (&RE_XML, "TEXT"),
    (&RE_MSSQL_TIMESTAMP_BRACKETED, "BLOB"),
    (&RE_ROWVERSION_ONLY, "BLOB"),
    (&RE_FLOAT, "REAL"),
    (&RE_ON_PRIMARY, ""),
    (&RE_CLUSTERED, ""),
    (&RE_NONCLUSTERED, ""),
];

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
/// A PostgreSQL variable-length string declaration in any of its spellings,
/// with the length (if declared) captured as `len` and a leading `::` cast
/// marker captured as `cast`.
///
/// `pg_dump` writes `character varying`, never `varchar`, so the long spellings
/// are the ones that actually turn up in real dumps. Matching the optional
/// `::` prefix is what keeps the rewrite out of a cast such as
/// `DEFAULT 'active'::character varying`: those are removed wholesale later by
/// `strip_postgres_casts`, which matches bare identifiers and would leave a
/// dangling `(255)` behind if the type inside it had been rewritten.
static RE_VARCHAR_DECL: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)(?P<cast>::\s*)?\b(?:VARCHAR|(?:NATIONAL\s+)?(?:CHARACTER|CHAR)\s+VARYING)(?P<len>\s*\(\s*\d+\s*\))?",
    )
    .unwrap()
});
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

static RE_ENUM: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bENUM\s*\([^)]*(?:\([^)]*\)[^)]*)*\)").unwrap());
static RE_SET: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\bSET\s*\([^)]*(?:\([^)]*\)[^)]*)*\)").unwrap());

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
static RE_NCHAR: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNCHAR\s*(\(\s*\d+\s*\))").unwrap());
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
static RE_SMALLDATETIME: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bSMALLDATETIME\b").unwrap());
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
static RE_ROWVERSION_ONLY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bROWVERSION\b").unwrap());

// MSSQL-specific clauses to strip when converting to other dialects
static RE_ON_PRIMARY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\s*ON\s*\[\s*PRIMARY\s*\]").unwrap());
static RE_CLUSTERED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bCLUSTERED\s+").unwrap());
static RE_NONCLUSTERED: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\bNONCLUSTERED\s+").unwrap());

#[cfg(test)]
mod map_column_type_tests {
    use super::*;

    #[test]
    fn same_dialect_is_identity_and_warning_free() {
        let mut warnings = WarningCollector::new();
        let mapped = map_column_type(
            "VARCHAR(255)",
            SqlDialect::MySql,
            SqlDialect::MySql,
            &mut warnings,
        );
        assert_eq!(mapped, "VARCHAR(255)");
        assert!(!warnings.has_warnings());
    }

    #[test]
    fn cross_dialect_reuses_the_statement_level_regex_rules() {
        let mut warnings = WarningCollector::new();
        let mapped = map_column_type(
            "BIGINT(20)",
            SqlDialect::MySql,
            SqlDialect::Postgres,
            &mut warnings,
        );
        assert_eq!(mapped, "BIGINT");
    }

    #[test]
    fn postgres_unbounded_varchar_maps_to_a_sized_mysql_varchar() {
        for source in [
            "varchar",
            "VARCHAR",
            "character varying",
            "CHARACTER VARYING",
            "char varying",
        ] {
            let mut warnings = WarningCollector::new();
            let mapped = map_column_type(
                source,
                SqlDialect::Postgres,
                SqlDialect::MySql,
                &mut warnings,
            );
            assert_eq!(mapped, "VARCHAR(255)", "source: {source}");
            // PostgreSQL stores up to 1GB in an unbounded VARCHAR, so pinning it
            // to 255 truncates: callers surfacing warnings (and --strict) must
            // see this rather than discover it at load time.
            assert!(warnings.has_warnings(), "source: {source}");
            assert!(matches!(
                warnings.warnings()[0],
                ConvertWarning::LossyConversion { .. }
            ));
        }
    }

    #[test]
    fn postgres_sized_varchar_is_already_valid_mysql_and_is_left_alone() {
        for source in ["varchar(255)", "character varying(120)", "VARCHAR (64)"] {
            let mut warnings = WarningCollector::new();
            let mapped = map_column_type(
                source,
                SqlDialect::Postgres,
                SqlDialect::MySql,
                &mut warnings,
            );
            assert_eq!(mapped, source, "source: {source}");
            assert!(!warnings.has_warnings(), "source: {source}");
        }
    }

    #[test]
    fn narrowing_enum_to_a_plain_string_records_a_lossy_warning() {
        let mut warnings = WarningCollector::new();
        let mapped = map_column_type(
            "ENUM('a','b')",
            SqlDialect::MySql,
            SqlDialect::Sqlite,
            &mut warnings,
        );
        assert_eq!(mapped, "TEXT");
        assert!(warnings.has_warnings());
        assert!(matches!(
            warnings.warnings()[0],
            ConvertWarning::LossyConversion { .. }
        ));
    }

    #[test]
    fn inline_enum_mapping_without_a_registry_is_lossy() {
        let mut warnings = WarningCollector::new();
        let mapped = map_column_type(
            "ENUM('a','b')",
            SqlDialect::MySql,
            SqlDialect::Postgres,
            &mut warnings,
        );
        assert_eq!(mapped, "VARCHAR(255)");
        assert!(warnings.has_warnings());
    }

    #[test]
    fn mysql_set_to_postgres_is_lossy() {
        let mut warnings = WarningCollector::new();
        let mapped = map_column_type(
            "SET('read','write')",
            SqlDialect::MySql,
            SqlDialect::Postgres,
            &mut warnings,
        );
        assert_eq!(mapped, "VARCHAR(255)");
        assert!(warnings.has_warnings());
    }

    #[test]
    fn json_to_an_engine_without_a_json_type_is_lossy() {
        let mut warnings = WarningCollector::new();
        map_column_type("JSON", SqlDialect::MySql, SqlDialect::Sqlite, &mut warnings);
        assert!(warnings.has_warnings());

        // PostgreSQL keeps a native JSON type, so the same source is not lossy.
        let mut kept = WarningCollector::new();
        map_column_type("JSON", SqlDialect::MySql, SqlDialect::Postgres, &mut kept);
        assert!(!kept.has_warnings());
    }

    #[test]
    fn uuid_stored_as_text_is_lossy() {
        let mut warnings = WarningCollector::new();
        map_column_type(
            "UUID",
            SqlDialect::Postgres,
            SqlDialect::MySql,
            &mut warnings,
        );
        assert!(warnings.has_warnings());

        // MSSQL has UNIQUEIDENTIFIER, so a UUID keeps its native domain there.
        let mut kept = WarningCollector::new();
        map_column_type("UUID", SqlDialect::Postgres, SqlDialect::Mssql, &mut kept);
        assert!(!kept.has_warnings());
    }

    #[test]
    fn decimal_to_sqlite_real_is_lossy() {
        // SQLite has no exact fixed-point type; the mapper renders DECIMAL as
        // REAL (binary float), silently losing exact precision.
        let mut warnings = WarningCollector::new();
        map_column_type(
            "DECIMAL(10,2)",
            SqlDialect::MySql,
            SqlDialect::Sqlite,
            &mut warnings,
        );
        assert!(warnings.has_warnings());

        // PostgreSQL keeps a native DECIMAL, so the same source is not lossy.
        let mut kept = WarningCollector::new();
        map_column_type(
            "DECIMAL(10,2)",
            SqlDialect::MySql,
            SqlDialect::Postgres,
            &mut kept,
        );
        assert!(!kept.has_warnings());
    }

    #[test]
    fn timestamptz_to_mysql_drops_the_timezone_and_is_lossy() {
        // MySQL DATETIME has no time-zone component, so the tz is dropped.
        let mut warnings = WarningCollector::new();
        map_column_type(
            "TIMESTAMPTZ",
            SqlDialect::Postgres,
            SqlDialect::MySql,
            &mut warnings,
        );
        assert!(warnings.has_warnings());

        // MSSQL DATETIMEOFFSET preserves the time zone, so it is not lossy.
        let mut kept = WarningCollector::new();
        map_column_type(
            "TIMESTAMPTZ",
            SqlDialect::Postgres,
            SqlDialect::Mssql,
            &mut kept,
        );
        assert!(!kept.has_warnings());
    }
}
