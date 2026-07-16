//! Shared helpers for PostgreSQL COPY data blocks.
//!
//! The parser emits a COPY statement's data section (the lines between
//! `COPY ... FROM stdin;` and the `\.` terminator) as a separate "Unknown"
//! statement. Both `validate` and `diff` need to recognize such blocks and
//! trim their leading whitespace; this module owns that logic so the two
//! commands (and any future COPY consumer) cannot drift.

/// Returns true if `stmt` looks like a PostgreSQL COPY data block, i.e. it
/// ends with the `\.` terminator line.
pub fn is_copy_data_block(stmt: &[u8]) -> bool {
    stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n")
}

/// Trim leading whitespace/newlines from a COPY data block without copying.
///
/// The block as emitted by the parser may start with the newline that
/// followed the COPY header; row parsing expects the first data line at
/// offset 0.
pub fn trim_copy_data(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|&b| !matches!(b, b'\n' | b'\r' | b' ' | b'\t'))
        .unwrap_or(data.len());
    &data[start..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_copy_terminator() {
        assert!(is_copy_data_block(b"1\tfoo\n2\tbar\n\\.\n"));
        assert!(is_copy_data_block(b"1\tfoo\r\n\\.\r\n"));
        assert!(!is_copy_data_block(b"INSERT INTO t VALUES (1);"));
        assert!(!is_copy_data_block(b"1\tfoo\n"));
    }

    #[test]
    fn trims_leading_whitespace_as_slice() {
        assert_eq!(trim_copy_data(b"\n1\tfoo\n\\.\n"), b"1\tfoo\n\\.\n");
        assert_eq!(trim_copy_data(b"\r\n\t 1\n"), b"1\n");
        assert_eq!(trim_copy_data(b"1\n"), b"1\n");
        assert_eq!(trim_copy_data(b"\n \t\r\n"), b"");
        assert_eq!(trim_copy_data(b""), b"");
    }
}
