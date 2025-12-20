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
        if !self.writers.contains_key(table_name) {
            let filename = self.output_dir.join(format!("{}.sql", table_name));
            let writer = TableWriter::new(&filename)?;
            self.writers.insert(table_name.to_string(), writer);
        }

        Ok(self.writers.get_mut(table_name).unwrap())
    }

    pub fn write_statement(&mut self, table_name: &str, stmt: &[u8]) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement(stmt)
    }

    pub fn close_all(&mut self) -> std::io::Result<()> {
        for (_, writer) in self.writers.iter_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_table_writer() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.sql");

        let mut writer = TableWriter::new(&file_path).unwrap();
        writer
            .write_statement(b"CREATE TABLE t1 (id INT);")
            .unwrap();
        writer
            .write_statement(b"INSERT INTO t1 VALUES (1);")
            .unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("CREATE TABLE t1"));
        assert!(content.contains("INSERT INTO t1"));
    }

    #[test]
    fn test_writer_pool() {
        let temp_dir = TempDir::new().unwrap();
        let mut pool = WriterPool::new(temp_dir.path().to_path_buf());
        pool.ensure_output_dir().unwrap();

        pool.write_statement("users", b"CREATE TABLE users (id INT);")
            .unwrap();
        pool.write_statement("posts", b"CREATE TABLE posts (id INT);")
            .unwrap();
        pool.write_statement("users", b"INSERT INTO users VALUES (1);")
            .unwrap();

        pool.close_all().unwrap();

        // Verify both table files were created
        let users_content = std::fs::read_to_string(temp_dir.path().join("users.sql")).unwrap();
        assert!(users_content.contains("CREATE TABLE users"));
        assert!(users_content.contains("INSERT INTO users"));

        let posts_content = std::fs::read_to_string(temp_dir.path().join("posts.sql")).unwrap();
        assert!(posts_content.contains("CREATE TABLE posts"));
    }
}
