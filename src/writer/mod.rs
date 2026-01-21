use ahash::AHashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub const WRITER_BUFFER_SIZE: usize = 256 * 1024;
pub const STMT_BUFFER_COUNT: usize = 100;

pub struct TableWriter {
    writer: BufWriter<File>,
    write_count: usize,
    max_stmt_buffer: usize,
}

impl TableWriter {
    pub fn new(filename: &Path) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, file);

        Ok(Self {
            writer,
            write_count: 0,
            max_stmt_buffer: STMT_BUFFER_COUNT,
        })
    }

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

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.write_count = 0;
        self.writer.flush()
    }
}

pub struct WriterPool {
    output_dir: PathBuf,
    writers: AHashMap<String, TableWriter>,
}

impl WriterPool {
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            writers: AHashMap::new(),
        }
    }

    pub fn ensure_output_dir(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.output_dir)
    }

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

    pub fn write_statement(&mut self, table_name: &str, stmt: &[u8]) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement(stmt)
    }

    pub fn write_statement_with_suffix(
        &mut self,
        table_name: &str,
        stmt: &[u8],
        suffix: &[u8],
    ) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement_with_suffix(stmt, suffix)
    }

    pub fn close_all(&mut self) -> std::io::Result<()> {
        for (_, writer) in self.writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}
