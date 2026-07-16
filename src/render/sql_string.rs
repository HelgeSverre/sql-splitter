//! Borrowed, allocation-free SQL string-literal rendering.

use std::fmt::{self, Display, Formatter, Write};

use crate::parser::SqlDialect;

/// A borrowed value that renders as a dialect-correct, quoted SQL string
/// literal via [`Display`], without allocating an intermediate escaped
/// [`String`].
///
/// # Examples
///
/// ```
/// use sql_splitter::parser::SqlDialect;
/// use sql_splitter::render::SqlString;
///
/// assert_eq!(
///     SqlString::new(SqlDialect::Postgres, "it's").to_string(),
///     "'it''s'"
/// );
/// ```
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
        assert_eq!(
            SqlString::new(SqlDialect::Postgres, input).to_string(),
            "'a''b\\c\n\r\t'"
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
    fn sql_string_leaves_plain_text_untouched() {
        assert_eq!(
            SqlString::new(SqlDialect::MySql, "plain").to_string(),
            "'plain'"
        );
    }
}
