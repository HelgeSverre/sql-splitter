//! Buffered file writers for splitting SQL statements into per-table files.

use ahash::AHashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Size of the BufWriter buffer per table file (256 KB).
pub const WRITER_BUFFER_SIZE: usize = 256 * 1024;
/// Number of statements to buffer before flushing (100).
pub const STMT_BUFFER_COUNT: usize = 100;

/// Buffered writer for a single table's SQL file.
pub struct TableWriter {
    writer: BufWriter<File>,
    write_count: usize,
    max_stmt_buffer: usize,
}

impl TableWriter {
    /// Create a new table writer for the given file path.
    pub fn new(filename: &Path) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, file);

        Ok(Self {
            writer,
            write_count: 0,
            max_stmt_buffer: STMT_BUFFER_COUNT,
        })
    }

    /// Write a SQL statement followed by a newline, flushing periodically.
    pub fn write_statement(&mut self, stmt: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(stmt)?;
        self.writer.write_all(b"\n")?;

        self.write_count += 1;
        if self.write_count >= self.max_stmt_buffer {
            self.write_count = 0;
            self.writer.flush()?;
        }

        Ok(())
    }

    /// Write a SQL statement with a custom suffix and newline, flushing periodically.
    pub fn write_statement_with_suffix(
        &mut self,
        stmt: &[u8],
        suffix: &[u8],
    ) -> std::io::Result<()> {
        self.writer.write_all(stmt)?;
        self.writer.write_all(suffix)?;
        self.writer.write_all(b"\n")?;

        self.write_count += 1;
        if self.write_count >= self.max_stmt_buffer {
            self.write_count = 0;
            self.writer.flush()?;
        }

        Ok(())
    }

    /// Flush the internal buffer to disk.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.write_count = 0;
        self.writer.flush()
    }
}

/// Pool of per-table writers, creating files on demand.
pub struct WriterPool {
    output_dir: PathBuf,
    writers: AHashMap<String, TableWriter>,
}

impl WriterPool {
    /// Create a new writer pool targeting the given output directory.
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            writers: AHashMap::new(),
        }
    }

    /// Create the output directory if it does not exist.
    pub fn ensure_output_dir(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.output_dir)
    }

    /// Get or create a writer for the given table name.
    pub fn get_writer(&mut self, table_name: &str) -> std::io::Result<&mut TableWriter> {
        use std::collections::hash_map::Entry;

        // Use entry API to avoid separate contains_key + get_mut (eliminates unwrap)
        match self.writers.entry(table_name.to_string()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let filename = self.output_dir.join(format!("{}.sql", table_name));
                let writer = TableWriter::new(&filename)?;
                Ok(entry.insert(writer))
            }
        }
    }

    /// Write a statement to the file for the given table.
    pub fn write_statement(&mut self, table_name: &str, stmt: &[u8]) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement(stmt)
    }

    /// Write a statement with suffix to the file for the given table.
    pub fn write_statement_with_suffix(
        &mut self,
        table_name: &str,
        stmt: &[u8],
        suffix: &[u8],
    ) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement_with_suffix(stmt, suffix)
    }

    /// Flush and close all writers.
    pub fn close_all(&mut self) -> std::io::Result<()> {
        for (_, writer) in self.writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}
