//! A reusable row buffer for multi-tenant SQL generation.
//!
//! `RowBatch` replaces a `Vec<String>` of formatted rows with a single
//! [`String`] that is cleared and reused across batches, so a long-running
//! generation only pays for its buffer's allocation once instead of once per
//! flush.

use std::fmt::{self, Write};

/// Upper bound on the bytes reserved per row when sizing a batch's initial
/// buffer. `estimated_bytes` may be derived from column-width metadata that a
/// hostile or malformed schema could inflate; capping it relative to
/// `batch_size` keeps a single `RowBatch::with_capacity` call from becoming
/// an attacker-controlled allocation.
const MAX_BYTES_PER_ROW_HINT: usize = 8 * 1024;

/// A reusable buffer of formatted SQL row tuples, joined by `,\n`.
pub struct RowBatch {
    buf: String,
    rows: usize,
}

impl RowBatch {
    /// Create an empty batch sized for roughly `batch_size` rows, reserving
    /// up to `estimated_bytes` for the initial buffer. The reservation is
    /// capped at `batch_size * MAX_BYTES_PER_ROW_HINT` so a caller-supplied
    /// estimate can't force an outsized single allocation; the buffer still
    /// grows normally afterwards if rows exceed the estimate.
    pub fn with_capacity(batch_size: usize, estimated_bytes: usize) -> Self {
        let cap = estimated_bytes.min(batch_size.saturating_mul(MAX_BYTES_PER_ROW_HINT));
        Self {
            buf: String::with_capacity(cap),
            rows: 0,
        }
    }

    /// Append a formatted row, prefixing it with `,\n` unless it is the
    /// first row in the batch.
    ///
    /// Writing to a `String` is infallible, but the signature mirrors
    /// [`std::fmt::Write::write_fmt`] so callers can propagate the error with
    /// `?` rather than reach for `unwrap`.
    pub fn push_fmt(&mut self, args: fmt::Arguments<'_>) -> fmt::Result {
        if self.rows > 0 {
            self.buf.write_str(",\n")?;
        }
        self.buf.write_fmt(args)?;
        self.rows += 1;
        Ok(())
    }

    /// The batch's buffered rows, joined by `,\n`.
    pub fn as_str(&self) -> &str {
        &self.buf
    }

    /// How many rows have been pushed since the last [`RowBatch::clear`].
    pub fn row_count(&self) -> usize {
        self.rows
    }

    /// Whether any rows have been pushed since the last clear.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// The buffer's current capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    /// Reset the batch to empty without shrinking its buffer, so the next
    /// batch reuses the same allocation.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.rows = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_batch_reuses_capacity_after_clear() {
        let mut batch = RowBatch::with_capacity(4, 256);
        batch.push_fmt(format_args!("(1, 'a')")).unwrap();
        batch.push_fmt(format_args!("(2, 'b')")).unwrap();
        let capacity = batch.capacity();
        assert_eq!(batch.as_str(), "(1, 'a'),\n(2, 'b')");
        assert_eq!(batch.row_count(), 2);
        batch.clear();
        assert!(batch.is_empty());
        assert!(batch.capacity() >= capacity);
    }

    #[test]
    fn with_capacity_caps_reservation_relative_to_batch_size() {
        // A tiny batch_size must not honor a wildly inflated byte estimate;
        // the reservation is capped well below the requested estimate.
        let batch = RowBatch::with_capacity(1, usize::MAX / 2);
        assert!(batch.capacity() < usize::MAX / 4);
    }
}
