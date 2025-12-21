use crate::parser::{determine_buffer_size, Parser, SqlDialect, StatementType};
use crate::progress::ProgressReader;
use crate::splitter::Compression;
use ahash::AHashMap;
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
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

        // Detect and apply decompression
        let compression = Compression::from_path(&self.input_file);
        let reader: Box<dyn Read> = compression.wrap_reader(Box::new(file));

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            if stmt_type == StatementType::Unknown || table_name.is_empty() {
                continue;
            }

            self.update_stats(&table_name, stmt_type, stmt.len() as u64);
        }

        Ok(self.get_sorted_stats())
    }

    pub fn analyze_with_progress<F: Fn(u64) + 'static>(
        mut self,
        progress_fn: F,
    ) -> anyhow::Result<Vec<TableStats>> {
        let file = File::open(&self.input_file)?;
        let file_size = file.metadata()?.len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.dialect;

        // Detect and apply decompression
        let compression = Compression::from_path(&self.input_file);
        let progress_reader = ProgressReader::new(file, progress_fn);
        let reader: Box<dyn Read> = compression.wrap_reader(Box::new(progress_reader));

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

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
