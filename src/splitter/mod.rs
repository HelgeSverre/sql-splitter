use crate::parser::{determine_buffer_size, Parser, StatementType};
use crate::writer::WriterPool;
use ahash::AHashSet;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

pub struct Stats {
    pub statements_processed: u64,
    pub tables_found: usize,
    pub bytes_processed: u64,
    pub table_names: Vec<String>,
}

#[derive(Default)]
pub struct SplitterConfig {
    pub dry_run: bool,
    pub table_filter: Option<AHashSet<String>>,
    pub progress_fn: Option<Box<dyn Fn(u64)>>,
}

pub struct Splitter {
    input_file: PathBuf,
    output_dir: PathBuf,
    config: SplitterConfig,
}

impl Splitter {
    pub fn new(input_file: PathBuf, output_dir: PathBuf) -> Self {
        Self {
            input_file,
            output_dir,
            config: SplitterConfig::default(),
        }
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.config.dry_run = dry_run;
        self
    }

    pub fn with_table_filter(mut self, tables: Vec<String>) -> Self {
        if !tables.is_empty() {
            self.config.table_filter = Some(tables.into_iter().collect());
        }
        self
    }

    pub fn with_progress<F: Fn(u64) + 'static>(mut self, f: F) -> Self {
        self.config.progress_fn = Some(Box::new(f));
        self
    }

    pub fn split(self) -> anyhow::Result<Stats> {
        let file = File::open(&self.input_file)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);

        let reader: Box<dyn Read> = if self.config.progress_fn.is_some() {
            Box::new(ProgressReader::new(file, self.config.progress_fn.unwrap()))
        } else {
            Box::new(file)
        };

        let mut parser = Parser::new(reader, buffer_size);

        let mut writer_pool = WriterPool::new(self.output_dir.clone());
        if !self.config.dry_run {
            writer_pool.ensure_output_dir()?;
        }

        let mut tables_seen: AHashSet<String> = AHashSet::new();
        let mut stats = Stats {
            statements_processed: 0,
            tables_found: 0,
            bytes_processed: 0,
            table_names: Vec::new(),
        };

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement(&stmt);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            if let Some(ref filter) = self.config.table_filter {
                if !filter.contains(&table_name) {
                    continue;
                }
            }

            if !tables_seen.contains(&table_name) {
                tables_seen.insert(table_name.clone());
                stats.tables_found += 1;
                stats.table_names.push(table_name.clone());
            }

            if !self.config.dry_run {
                writer_pool.write_statement(&table_name, &stmt)?;
            }

            stats.statements_processed += 1;
            stats.bytes_processed += stmt.len() as u64;
        }

        if !self.config.dry_run {
            writer_pool.close_all()?;
        }

        Ok(stats)
    }
}

struct ProgressReader<R: Read> {
    reader: R,
    callback: Box<dyn Fn(u64)>,
    bytes_read: u64,
}

impl<R: Read> ProgressReader<R> {
    fn new(reader: R, callback: Box<dyn Fn(u64)>) -> Self {
        Self {
            reader,
            callback,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_splitter_basic() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(
            &input_file,
            b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nCREATE TABLE posts (id INT);\n",
        )
        .unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone());
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 2);
        assert_eq!(stats.statements_processed, 3);

        assert!(output_dir.join("users.sql").exists());
        assert!(output_dir.join("posts.sql").exists());
    }

    #[test]
    fn test_splitter_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(&input_file, b"CREATE TABLE users (id INT);").unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone()).with_dry_run(true);
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 1);
        assert!(!output_dir.exists());
    }

    #[test]
    fn test_splitter_table_filter() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(
            &input_file,
            b"CREATE TABLE users (id INT);\nCREATE TABLE posts (id INT);\nCREATE TABLE orders (id INT);",
        )
        .unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone())
            .with_table_filter(vec!["users".to_string(), "orders".to_string()]);
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 2);
        assert!(output_dir.join("users.sql").exists());
        assert!(!output_dir.join("posts.sql").exists());
        assert!(output_dir.join("orders.sql").exists());
    }
}
