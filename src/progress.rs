//! Shared progress tracking utilities.
//!
//! This module provides a reusable `ProgressReader` wrapper that tracks bytes read
//! and calls a callback function, enabling byte-based progress bars across all commands.

use std::io::Read;

/// A reader wrapper that tracks bytes read and calls a progress callback.
///
/// This is used to provide byte-based progress tracking for commands that
/// stream through SQL dump files.
pub struct ProgressReader<R: Read> {
    reader: R,
    callback: Box<dyn Fn(u64)>,
    bytes_read: u64,
}

impl<R: Read> ProgressReader<R> {
    /// Create a new ProgressReader wrapping the given reader.
    ///
    /// The callback will be called with the total bytes read so far
    /// after each successful read operation.
    pub fn new<F>(reader: R, callback: F) -> Self
    where
        F: Fn(u64) + 'static,
    {
        Self {
            reader,
            callback: Box::new(callback),
            bytes_read: 0,
        }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.bytes_read += n as u64;
        (self.callback)(self.bytes_read);
        Ok(n)
    }
}
