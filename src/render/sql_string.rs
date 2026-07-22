//! Borrowed, allocation-free SQL string-literal rendering.

use std::fmt::{self, Display, Formatter, Write};

use crate::parser::SqlDialect;

/// A borrowed value that renders as a dialect-correct, quoted SQL string
/// literal via [`Display`], without allocating an intermediate escaped
/// [`String`].
pub struct SqlString<'a> {
    dialect: SqlDialect,
    value: &'a str,
}

impl<'a> SqlString<'a> {
    /// Borrow `value` for rendering as a `dialect`-correct quoted literal.
    pub fn new(dialect: SqlDialect, value: &'a str) -> Self {
        Self { dialect, value }
    }
}

impl Display for SqlString<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // In PostgreSQL a backslash is only a literal character under
        // `standard_conforming_strings = on`; with it off, `\'` escapes the
        // quote and a value ending in a backslash breaks out of the literal.
        // Render any backslash-bearing value as an `E'...'` escape string with
        // doubled backslashes so it is unambiguous regardless of that server
        // setting. (SQLite/MSSQL never treat a backslash specially; MySQL's
        // default mode already backslash-escapes.)
        if self.dialect == SqlDialect::Postgres && self.value.contains('\\') {
            f.write_str("E'")?;
            for ch in self.value.chars() {
                match ch {
                    '\\' => f.write_str("\\\\")?,
                    '\'' => f.write_str("''")?,
                    ch => f.write_char(ch)?,
                }
            }
            return f.write_str("'");
        }
        if self.dialect == SqlDialect::Mssql {
            f.write_str("N")?;
        }
        f.write_str("'")?;
        for ch in self.value.chars() {
            match (self.dialect, ch) {
                (SqlDialect::MySql, '\\') => f.write_str("\\\\")?,
                (SqlDialect::MySql, '\'') => f.write_str("\\'")?,
                (SqlDialect::MySql, '\n') => f.write_str("\\n")?,
                (SqlDialect::MySql, '\r') => f.write_str("\\r")?,
                (SqlDialect::MySql, '\t') => f.write_str("\\t")?,
                (_, '\'') => f.write_str("''")?,
                (_, ch) => f.write_char(ch)?,
            }
        }
        f.write_str("'")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_string_escapes_each_dialect_without_intermediate_contract_changes() {
        let input = "a'b\\c\n\r\t";
        assert_eq!(
            SqlString::new(SqlDialect::MySql, input).to_string(),
            "'a\\'b\\\\c\\n\\r\\t'"
        );
        // The backslash forces an E'' escape string with a doubled backslash.
        assert_eq!(
            SqlString::new(SqlDialect::Postgres, input).to_string(),
            "E'a''b\\\\c\n\r\t'"
        );
        assert_eq!(
            SqlString::new(SqlDialect::Sqlite, input).to_string(),
            "'a''b\\c\n\r\t'"
        );
        assert_eq!(
            SqlString::new(SqlDialect::Mssql, input).to_string(),
            "N'a''b\\c\n\r\t'"
        );
    }

    #[test]
    fn postgres_backslash_values_are_standard_conforming_independent() {
        // A value ending in a backslash must not render as the ambiguous `'a\'`,
        // which breaks out of the literal when standard_conforming_strings is
        // off. Use an E'' escape string with a doubled backslash instead.
        assert_eq!(
            SqlString::new(SqlDialect::Postgres, "a\\").to_string(),
            "E'a\\\\'"
        );
        assert_eq!(
            SqlString::new(SqlDialect::Postgres, "x'y\\z").to_string(),
            "E'x''y\\\\z'"
        );
        // No backslash: unchanged plain literal.
        assert_eq!(
            SqlString::new(SqlDialect::Postgres, "x'y").to_string(),
            "'x''y'"
        );
    }

    #[test]
    fn sql_string_leaves_plain_text_untouched() {
        assert_eq!(
            SqlString::new(SqlDialect::MySql, "plain").to_string(),
            "'plain'"
        );
    }
}
