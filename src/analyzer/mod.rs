use crate::parser::{determine_buffer_size, Parser, SqlDialect, StatementType};
use ahash::AHashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TableStats {
    pub table_name: String,
    pub insert_count: u64,
    pub create_count: u64,
    pub total_bytes: u64,
    pub statement_count: u64,
}

impl TableStats {
    fn new(table_name: String) -> Self {
        Self {
            table_name,
            insert_count: 0,
            create_count: 0,
            total_bytes: 0,
            statement_count: 0,
        }
    }
}

pub struct Analyzer {
    input_file: PathBuf,
    dialect: SqlDialect,
    stats: AHashMap<String, TableStats>,
}

impl Analyzer {
    pub fn new(input_file: PathBuf) -> Self {
        Self {
            input_file,
            dialect: SqlDialect::default(),
            stats: AHashMap::new(),
        }
    }

    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn analyze(mut self) -> anyhow::Result<Vec<TableStats>> {
        let file = File::open(&self.input_file)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.dialect;

        let mut parser = Parser::with_dialect(file, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            self.update_stats(&table_name, stmt_type, stmt.len() as u64);
        }

        Ok(self.get_sorted_stats())
    }

    pub fn analyze_with_progress<F: Fn(u64)>(
        mut self,
        progress_fn: F,
    ) -> anyhow::Result<Vec<TableStats>> {
        let file = File::open(&self.input_file)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.dialect;

        let reader = ProgressReader::new(file, progress_fn);
        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) = Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            self.update_stats(&table_name, stmt_type, stmt.len() as u64);
        }

        Ok(self.get_sorted_stats())
    }

    fn update_stats(&mut self, table_name: &str, stmt_type: StatementType, bytes: u64) {
        let stats = self
            .stats
            .entry(table_name.to_string())
            .or_insert_with(|| TableStats::new(table_name.to_string()));

        stats.statement_count += 1;
        stats.total_bytes += bytes;

        match stmt_type {
            StatementType::CreateTable => stats.create_count += 1,
            StatementType::Insert | StatementType::Copy => stats.insert_count += 1,
            _ => {}
        }
    }

    fn get_sorted_stats(&self) -> Vec<TableStats> {
        let mut result: Vec<TableStats> = self.stats.values().cloned().collect();
        result.sort_by(|a, b| b.insert_count.cmp(&a.insert_count));
        result
    }
}

struct ProgressReader<R: Read, F: Fn(u64)> {
    reader: R,
    callback: F,
    bytes_read: u64,
}

impl<R: Read, F: Fn(u64)> ProgressReader<R, F> {
    fn new(reader: R, callback: F) -> Self {
        Self {
            reader,
            callback,
            bytes_read: 0,
        }
    }
}

impl<R: Read, F: Fn(u64)> Read for ProgressReader<R, F> {
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
    fn test_analyzer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");

        std::fs::write(
            &input_file,
            b"CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);\nCREATE TABLE posts (id INT);\nINSERT INTO posts VALUES (1);",
        )
        .unwrap();

        let analyzer = Analyzer::new(input_file);
        let stats = analyzer.analyze().unwrap();

        assert_eq!(stats.len(), 2);

        let users_stats = stats.iter().find(|s| s.table_name == "users").unwrap();
        assert_eq!(users_stats.insert_count, 2);
        assert_eq!(users_stats.create_count, 1);
        assert_eq!(users_stats.statement_count, 3);

        let posts_stats = stats.iter().find(|s| s.table_name == "posts").unwrap();
        assert_eq!(posts_stats.insert_count, 1);
        assert_eq!(posts_stats.create_count, 1);
        assert_eq!(posts_stats.statement_count, 2);
    }

    #[test]
    fn test_analyzer_sorted_by_insert_count() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.sql");

        std::fs::write(
            &input_file,
            b"CREATE TABLE a (id INT);\nINSERT INTO a VALUES (1);\nCREATE TABLE b (id INT);\nINSERT INTO b VALUES (1);\nINSERT INTO b VALUES (2);\nINSERT INTO b VALUES (3);",
        )
        .unwrap();

        let analyzer = Analyzer::new(input_file);
        let stats = analyzer.analyze().unwrap();

        assert_eq!(stats[0].table_name, "b");
        assert_eq!(stats[0].insert_count, 3);
        assert_eq!(stats[1].table_name, "a");
        assert_eq!(stats[1].insert_count, 1);
    }
}
