use crate::parser::{determine_buffer_size, ContentFilter, Parser, SqlDialect, StatementType};
use crate::writer::WriterPool;
use ahash::AHashSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub struct Stats {
    pub statements_processed: u64,
    pub tables_found: usize,
    pub bytes_processed: u64,
    pub table_names: Vec<String>,
}

#[derive(Default)]
pub struct SplitterConfig {
    pub dialect: SqlDialect,
    pub dry_run: bool,
    pub table_filter: Option<AHashSet<String>>,
    pub progress_fn: Option<Box<dyn Fn(u64)>>,
    pub content_filter: ContentFilter,
}

/// Compression format detected from file extension
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Bzip2,
    Xz,
    Zstd,
}

impl Compression {
    /// Detect compression format from file extension
    pub fn from_path(path: &Path) -> Self {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase());

        match ext.as_deref() {
            Some("gz" | "gzip") => Compression::Gzip,
            Some("bz2" | "bzip2") => Compression::Bzip2,
            Some("xz" | "lzma") => Compression::Xz,
            Some("zst" | "zstd") => Compression::Zstd,
            _ => Compression::None,
        }
    }

    /// Wrap a reader with the appropriate decompressor
    pub fn wrap_reader<'a>(&self, reader: Box<dyn Read + 'a>) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => reader,
            Compression::Gzip => Box::new(flate2::read::GzDecoder::new(reader)),
            Compression::Bzip2 => Box::new(bzip2::read::BzDecoder::new(reader)),
            Compression::Xz => Box::new(xz2::read::XzDecoder::new(reader)),
            Compression::Zstd => Box::new(zstd::stream::read::Decoder::new(reader).unwrap()),
        }
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Bzip2 => write!(f, "bzip2"),
            Compression::Xz => write!(f, "xz"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
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

    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.config.dialect = dialect;
        self
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

    pub fn with_content_filter(mut self, filter: ContentFilter) -> Self {
        self.config.content_filter = filter;
        self
    }

    pub fn split(self) -> anyhow::Result<Stats> {
        let file = File::open(&self.input_file)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.config.dialect;
        let content_filter = self.config.content_filter;

        // Detect and apply decompression
        let compression = Compression::from_path(&self.input_file);

        let reader: Box<dyn Read> = if self.config.progress_fn.is_some() {
            let progress_reader = ProgressReader::new(file, self.config.progress_fn.unwrap());
            compression.wrap_reader(Box::new(progress_reader))
        } else {
            compression.wrap_reader(Box::new(file))
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

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

        // Track the last COPY table for PostgreSQL COPY data blocks
        let mut last_copy_table: Option<String> = None;

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, mut table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            // Track COPY statements for data association
            if stmt_type == StatementType::Copy {
                last_copy_table = Some(table_name.clone());
            }

            // Handle PostgreSQL COPY data blocks - associate with last COPY table
            let is_copy_data = if stmt_type == StatementType::Unknown && last_copy_table.is_some() {
                // Check if this looks like COPY data (ends with \.\n)
                if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                    table_name = last_copy_table.take().unwrap();
                    true
                } else {
                    false
                }
            } else {
                false
            };

            if !is_copy_data && (stmt_type == StatementType::Unknown || table_name.is_empty()) {
                continue;
            }

            // Apply content filter (schema-only or data-only)
            match content_filter {
                ContentFilter::SchemaOnly => {
                    if !stmt_type.is_schema() {
                        continue;
                    }
                }
                ContentFilter::DataOnly => {
                    // For data-only, include INSERT, COPY, and COPY data blocks
                    if !stmt_type.is_data() && !is_copy_data {
                        continue;
                    }
                }
                ContentFilter::All => {}
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

    #[test]
    fn test_splitter_schema_only() {
        use crate::parser::ContentFilter;

        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(
            &input_file,
            b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);",
        )
        .unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone())
            .with_content_filter(ContentFilter::SchemaOnly);
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 1);
        assert_eq!(stats.statements_processed, 1); // Only CREATE TABLE

        let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
        assert!(content.contains("CREATE TABLE"));
        assert!(!content.contains("INSERT"));
    }

    #[test]
    fn test_splitter_data_only() {
        use crate::parser::ContentFilter;

        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(
            &input_file,
            b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);",
        )
        .unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone())
            .with_content_filter(ContentFilter::DataOnly);
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 1);
        assert_eq!(stats.statements_processed, 2); // Only INSERTs

        let content = std::fs::read_to_string(output_dir.join("users.sql")).unwrap();
        assert!(!content.contains("CREATE TABLE"));
        assert!(content.contains("INSERT"));
    }

    #[test]
    fn test_splitter_gzip_compressed() {
        use flate2::write::GzEncoder;
        use flate2::Compression as GzCompression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql.gz");
        let output_dir = temp_dir.path().join("output");

        // Create gzipped SQL file
        let file = std::fs::File::create(&input_file).unwrap();
        let mut encoder = GzEncoder::new(file, GzCompression::default());
        encoder
            .write_all(b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);")
            .unwrap();
        encoder.finish().unwrap();

        let splitter = Splitter::new(input_file, output_dir.clone());
        let stats = splitter.split().unwrap();

        assert_eq!(stats.tables_found, 1);
        assert_eq!(stats.statements_processed, 2);
        assert!(output_dir.join("users.sql").exists());
    }

    #[test]
    fn test_compression_detection() {
        assert_eq!(
            Compression::from_path(std::path::Path::new("file.sql")),
            Compression::None
        );
        assert_eq!(
            Compression::from_path(std::path::Path::new("file.sql.gz")),
            Compression::Gzip
        );
        assert_eq!(
            Compression::from_path(std::path::Path::new("file.sql.bz2")),
            Compression::Bzip2
        );
        assert_eq!(
            Compression::from_path(std::path::Path::new("file.sql.xz")),
            Compression::Xz
        );
        assert_eq!(
            Compression::from_path(std::path::Path::new("file.sql.zst")),
            Compression::Zstd
        );
    }
}
