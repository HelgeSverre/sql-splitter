//! Borrowed, allocation-free SQL string-literal rendering.

use std::fmt::{self, Display, Formatter};

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
        // Pieces are split only at ASCII bytes, so every piece is valid UTF-8.
        escape_into(self.dialect, self.value.as_bytes(), |piece| {
            f.write_str(std::str::from_utf8(piece).map_err(|_| fmt::Error)?)
        })
    }
}

/// Append `value` to `out` as a `dialect`-correct quoted SQL string literal.
///
/// Byte-oriented twin of [`SqlString`]: only ASCII bytes are ever escaped, so
/// arbitrary (even non-UTF-8) bytes pass through untouched.
pub fn push_sql_literal(out: &mut Vec<u8>, dialect: SqlDialect, value: &[u8]) {
    let Ok(()) = escape_into(dialect, value, |piece| {
        out.extend_from_slice(piece);
        Ok::<(), std::convert::Infallible>(())
    });
}

/// Feed the pieces of the quoted, escaped literal for `value` to `emit`.
///
/// In PostgreSQL a backslash is only a literal character under
/// `standard_conforming_strings = on`; with it off, `\'` escapes the quote and
/// a value ending in a backslash breaks out of the literal. Any
/// backslash-bearing value is therefore rendered as an `E'...'` escape string
/// with doubled backslashes so it is unambiguous regardless of that setting.
/// (SQLite/MSSQL never treat a backslash specially; MySQL's default mode
/// already backslash-escapes.)
fn escape_into<E>(
    dialect: SqlDialect,
    value: &[u8],
    mut emit: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    let pg_escape = dialect == SqlDialect::Postgres && value.contains(&b'\\');
    let escape = |b: u8| -> Option<&'static [u8]> {
        match (dialect, b) {
            (SqlDialect::MySql, b'\\') => Some(b"\\\\"),
            (SqlDialect::MySql, b'\'') => Some(b"\\'"),
            (SqlDialect::MySql, b'\n') => Some(b"\\n"),
            (SqlDialect::MySql, b'\r') => Some(b"\\r"),
            (SqlDialect::MySql, b'\t') => Some(b"\\t"),
            (SqlDialect::Postgres, b'\\') if pg_escape => Some(b"\\\\"),
            (_, b'\'') => Some(b"''"),
            _ => None,
        }
    };

    emit(match dialect {
        SqlDialect::Postgres if pg_escape => b"E'",
        SqlDialect::Mssql => b"N'",
        _ => b"'",
    })?;
    let mut start = 0;
    for (i, &b) in value.iter().enumerate() {
        if let Some(esc) = escape(b) {
            if start < i {
                emit(&value[start..i])?;
            }
            emit(esc)?;
            start = i + 1;
        }
    }
    if start < value.len() {
        emit(&value[start..])?;
    }
    emit(b"'")
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
