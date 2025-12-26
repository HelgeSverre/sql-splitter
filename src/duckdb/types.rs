//! Type conversion from MySQL/PostgreSQL/SQLite types to DuckDB types.

use once_cell::sync::Lazy;
use regex::Regex;

/// DuckDB native types
#[derive(Debug, Clone, PartialEq)]
pub enum DuckDBType {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Decimal(Option<u8>, Option<u8>),
    Varchar(Option<u32>),
    Text,
    Blob,
    Date,
    Time,
    Timestamp,
    Interval,
    Uuid,
    Json,
}

impl std::fmt::Display for DuckDBType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DuckDBType::Boolean => write!(f, "BOOLEAN"),
            DuckDBType::TinyInt => write!(f, "TINYINT"),
            DuckDBType::SmallInt => write!(f, "SMALLINT"),
            DuckDBType::Integer => write!(f, "INTEGER"),
            DuckDBType::BigInt => write!(f, "BIGINT"),
            DuckDBType::Float => write!(f, "FLOAT"),
            DuckDBType::Double => write!(f, "DOUBLE"),
            DuckDBType::Decimal(p, s) => match (p, s) {
                (Some(p), Some(s)) => write!(f, "DECIMAL({}, {})", p, s),
                (Some(p), None) => write!(f, "DECIMAL({})", p),
                _ => write!(f, "DECIMAL"),
            },
            DuckDBType::Varchar(len) => match len {
                Some(n) => write!(f, "VARCHAR({})", n),
                None => write!(f, "VARCHAR"),
            },
            DuckDBType::Text => write!(f, "TEXT"),
            DuckDBType::Blob => write!(f, "BLOB"),
            DuckDBType::Date => write!(f, "DATE"),
            DuckDBType::Time => write!(f, "TIME"),
            DuckDBType::Timestamp => write!(f, "TIMESTAMP"),
            DuckDBType::Interval => write!(f, "INTERVAL"),
            DuckDBType::Uuid => write!(f, "UUID"),
            DuckDBType::Json => write!(f, "JSON"),
        }
    }
}

/// Converter for SQL types to DuckDB-compatible types
pub struct TypeConverter;

impl TypeConverter {
    /// Convert a SQL type string to a DuckDB-compatible type string
    pub fn convert(type_str: &str) -> String {
        let upper = type_str.to_uppercase();
        let trimmed = upper.trim();

        // Handle common MySQL types
        match trimmed {
            // Boolean
            "BOOL" | "BOOLEAN" => "BOOLEAN".to_string(),
            "TINYINT(1)" => "BOOLEAN".to_string(),

            // Integer types
            "TINYINT" => "TINYINT".to_string(),
            "SMALLINT" => "SMALLINT".to_string(),
            "MEDIUMINT" => "INTEGER".to_string(),
            "INT" | "INTEGER" => "INTEGER".to_string(),
            "BIGINT" => "BIGINT".to_string(),

            // MySQL UNSIGNED variants - DuckDB has unsigned types
            "TINYINT UNSIGNED" => "UTINYINT".to_string(),
            "SMALLINT UNSIGNED" => "USMALLINT".to_string(),
            "MEDIUMINT UNSIGNED" => "UINTEGER".to_string(),
            "INT UNSIGNED" | "INTEGER UNSIGNED" => "UINTEGER".to_string(),
            "BIGINT UNSIGNED" => "UBIGINT".to_string(),

            // PostgreSQL serial types
            "SERIAL" => "INTEGER".to_string(),
            "BIGSERIAL" => "BIGINT".to_string(),
            "SMALLSERIAL" => "SMALLINT".to_string(),

            // Floating point
            "FLOAT" | "FLOAT4" | "REAL" => "FLOAT".to_string(),
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => "DOUBLE".to_string(),

            // Decimal/Numeric
            "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => "DECIMAL".to_string(),
            "MONEY" => "DECIMAL(19,4)".to_string(),

            // String types
            "CHAR" => "VARCHAR".to_string(),
            "VARCHAR" | "CHARACTER VARYING" => "VARCHAR".to_string(),
            "TINYTEXT" => "VARCHAR(255)".to_string(),
            "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => "TEXT".to_string(),

            // Binary types
            "BINARY" | "VARBINARY" => "BLOB".to_string(),
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => "BLOB".to_string(),
            "BYTEA" => "BLOB".to_string(),

            // Date/Time types
            "DATE" => "DATE".to_string(),
            "TIME" | "TIME WITHOUT TIME ZONE" => "TIME".to_string(),
            "TIMETZ" | "TIME WITH TIME ZONE" => "TIMETZ".to_string(),
            "DATETIME" => "TIMESTAMP".to_string(),
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => "TIMESTAMP".to_string(),
            "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => "TIMESTAMPTZ".to_string(),
            "YEAR" => "SMALLINT".to_string(),
            "INTERVAL" => "INTERVAL".to_string(),

            // JSON
            "JSON" | "JSONB" => "JSON".to_string(),

            // UUID
            "UUID" => "UUID".to_string(),

            // Bit types
            "BIT" => "BOOLEAN".to_string(),

            _ => Self::convert_parameterized(trimmed),
        }
    }

    /// Handle parameterized types like VARCHAR(255), DECIMAL(10,2), etc.
    fn convert_parameterized(type_str: &str) -> String {
        // Pattern for types with parameters
        static RE_PARAMETERIZED: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"^(\w+)\s*\(([^)]+)\)(.*)$").unwrap());

        if let Some(caps) = RE_PARAMETERIZED.captures(type_str) {
            let base_type = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let params = caps.get(2).map(|m| m.as_str()).unwrap_or("");
            let suffix = caps.get(3).map(|m| m.as_str()).unwrap_or("").trim();

            let converted_base = match base_type {
                // Integer types with display width - strip the width
                "TINYINT" => {
                    if params == "1" {
                        return "BOOLEAN".to_string();
                    }
                    if suffix.contains("UNSIGNED") {
                        "UTINYINT"
                    } else {
                        "TINYINT"
                    }
                }
                "SMALLINT" => {
                    if suffix.contains("UNSIGNED") {
                        "USMALLINT"
                    } else {
                        "SMALLINT"
                    }
                }
                "MEDIUMINT" => {
                    if suffix.contains("UNSIGNED") {
                        "UINTEGER"
                    } else {
                        "INTEGER"
                    }
                }
                "INT" | "INTEGER" => {
                    if suffix.contains("UNSIGNED") {
                        "UINTEGER"
                    } else {
                        "INTEGER"
                    }
                }
                "BIGINT" => {
                    if suffix.contains("UNSIGNED") {
                        "UBIGINT"
                    } else {
                        "BIGINT"
                    }
                }

                // String types - preserve length
                "CHAR" | "CHARACTER" => {
                    return format!("VARCHAR({})", params);
                }
                "VARCHAR" | "CHARACTER VARYING" => {
                    return format!("VARCHAR({})", params);
                }
                "BINARY" | "VARBINARY" => return "BLOB".to_string(),

                // Decimal types - preserve precision and scale
                "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => {
                    return format!("DECIMAL({})", params);
                }

                // Float types with precision
                "FLOAT" => {
                    if let Ok(precision) = params.parse::<u32>() {
                        if precision <= 24 {
                            return "FLOAT".to_string();
                        } else {
                            return "DOUBLE".to_string();
                        }
                    }
                    return "FLOAT".to_string();
                }
                "DOUBLE" => return "DOUBLE".to_string(),

                // Time types with precision - DuckDB supports them
                "TIME" => return "TIME".to_string(),
                "TIMESTAMP" | "DATETIME" => return "TIMESTAMP".to_string(),

                // BIT fields
                "BIT" => {
                    if params == "1" {
                        return "BOOLEAN".to_string();
                    }
                    return "BITSTRING".to_string();
                }

                // ENUM - convert to VARCHAR with comment
                "ENUM" => return "VARCHAR".to_string(),

                // SET - convert to VARCHAR
                "SET" => return "VARCHAR".to_string(),

                _ => return type_str.to_string(),
            };

            return converted_base.to_string();
        }

        // Unknown type - pass through as-is
        type_str.to_string()
    }

    /// Convert an entire column definition
    pub fn convert_column_def(column_def: &str) -> String {
        // Handle AUTO_INCREMENT
        let mut result = column_def.to_string();

        // Replace type with converted type
        static RE_TYPE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"(?i)^(\s*`?[^`\s]+`?\s+)([A-Z][A-Z0-9_\s(),']+?)(\s+|$)").unwrap()
        });

        if let Some(caps) = RE_TYPE.captures(&result) {
            if let Some(type_match) = caps.get(2) {
                let original_type = type_match.as_str().trim();
                let converted_type = Self::convert(original_type);
                result = result.replacen(original_type, &converted_type, 1);
            }
        }

        // Remove AUTO_INCREMENT (DuckDB handles this differently)
        result = result.replace("AUTO_INCREMENT", "");
        result = result.replace("auto_increment", "");

        // Remove UNSIGNED (already handled in type conversion)
        result = result.replace(" UNSIGNED", "");
        result = result.replace(" unsigned", "");

        // Remove ZEROFILL
        result = result.replace(" ZEROFILL", "");
        result = result.replace(" zerofill", "");

        // Remove ON UPDATE CURRENT_TIMESTAMP
        static RE_ON_UPDATE: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*ON\s+UPDATE\s+CURRENT_TIMESTAMP").unwrap());
        result = RE_ON_UPDATE.replace_all(&result, "").to_string();

        // Remove CHARACTER SET
        static RE_CHARSET: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"(?i)\s*CHARACTER\s+SET\s+\w+").unwrap());
        result = RE_CHARSET.replace_all(&result, "").to_string();

        // Remove COLLATE
        static RE_COLLATE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\s*COLLATE\s+\w+").unwrap());
        result = RE_COLLATE.replace_all(&result, "").to_string();

        // Clean up multiple spaces
        static RE_SPACES: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
        result = RE_SPACES.replace_all(&result, " ").trim().to_string();

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_type_conversion() {
        assert_eq!(TypeConverter::convert("INT"), "INTEGER");
        assert_eq!(TypeConverter::convert("BIGINT"), "BIGINT");
        assert_eq!(TypeConverter::convert("VARCHAR"), "VARCHAR");
        assert_eq!(TypeConverter::convert("TEXT"), "TEXT");
        assert_eq!(TypeConverter::convert("DATETIME"), "TIMESTAMP");
        assert_eq!(TypeConverter::convert("BYTEA"), "BLOB");
    }

    #[test]
    fn test_parameterized_types() {
        assert_eq!(TypeConverter::convert("VARCHAR(255)"), "VARCHAR(255)");
        assert_eq!(TypeConverter::convert("DECIMAL(10,2)"), "DECIMAL(10,2)");
        assert_eq!(TypeConverter::convert("CHAR(1)"), "VARCHAR(1)");
        assert_eq!(TypeConverter::convert("TINYINT(1)"), "BOOLEAN");
    }

    #[test]
    fn test_unsigned_types() {
        assert_eq!(TypeConverter::convert("INT UNSIGNED"), "UINTEGER");
        assert_eq!(TypeConverter::convert("BIGINT UNSIGNED"), "UBIGINT");
        assert_eq!(TypeConverter::convert("TINYINT(3) UNSIGNED"), "UTINYINT");
    }

    #[test]
    fn test_mysql_specific() {
        assert_eq!(TypeConverter::convert("MEDIUMINT"), "INTEGER");
        assert_eq!(TypeConverter::convert("LONGTEXT"), "TEXT");
        assert_eq!(TypeConverter::convert("MEDIUMBLOB"), "BLOB");
        assert_eq!(TypeConverter::convert("YEAR"), "SMALLINT");
    }

    #[test]
    fn test_postgres_specific() {
        assert_eq!(TypeConverter::convert("SERIAL"), "INTEGER");
        assert_eq!(TypeConverter::convert("BIGSERIAL"), "BIGINT");
        assert_eq!(TypeConverter::convert("JSONB"), "JSON");
        assert_eq!(TypeConverter::convert("UUID"), "UUID");
    }

    #[test]
    fn test_enum_set() {
        assert_eq!(TypeConverter::convert("ENUM('a','b','c')"), "VARCHAR");
        assert_eq!(TypeConverter::convert("SET('x','y')"), "VARCHAR");
    }
}
