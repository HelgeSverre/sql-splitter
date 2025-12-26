//! Real-world SQL dump test framework.
//!
//! Provides fixtures for downloading and caching public SQL dumps,
//! and utilities for running various sql-splitter operations on them.

mod cases;
mod convert_test;
mod graph_test;
mod query_test;
mod redact_test;
mod split_test;
mod validate_test;

#[cfg(test)]
pub use cases::get_case;

use once_cell::sync::Lazy;
use sql_splitter::parser::SqlDialect;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

pub use cases::{TestCase, TEST_CASES};

/// Cache directory for downloaded files
static CACHE_DIR: Lazy<PathBuf> = Lazy::new(|| {
    let dir = std::env::temp_dir().join("sql-splitter-realworld-cache");
    fs::create_dir_all(&dir).expect("Failed to create cache directory");
    dir
});

/// Mutex to prevent concurrent downloads of the same file
static DOWNLOAD_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Downloaded file fixture - provides lazy downloading and caching
pub struct Fixture {
    pub case: &'static TestCase,
    pub sql_path: PathBuf,
}

impl Fixture {
    /// Get or download a fixture for a test case
    pub fn get(case: &'static TestCase) -> io::Result<Self> {
        let sql_path = ensure_downloaded(case)?;
        Ok(Self { case, sql_path })
    }

    /// Get the SQL dialect for this fixture
    pub fn dialect(&self) -> SqlDialect {
        self.case.dialect.parse().unwrap_or(SqlDialect::MySql)
    }

    /// Read the SQL content as a string
    pub fn read_content(&self) -> io::Result<String> {
        fs::read_to_string(&self.sql_path)
    }

    /// Get file size in bytes
    pub fn file_size(&self) -> io::Result<u64> {
        Ok(fs::metadata(&self.sql_path)?.len())
    }

    /// Format file size for display
    pub fn file_size_display(&self) -> String {
        match self.file_size() {
            Ok(size) => format_size(size),
            Err(_) => "?".to_string(),
        }
    }
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Ensure a test case's SQL file is downloaded and extracted
fn ensure_downloaded(case: &TestCase) -> io::Result<PathBuf> {
    let _lock = DOWNLOAD_LOCK.lock().unwrap();

    let cache_subdir = CACHE_DIR.join(&case.name);
    let sql_path = cache_subdir.join(&case.sql_file);

    // Check if already cached
    if sql_path.exists() {
        return Ok(sql_path);
    }

    fs::create_dir_all(&cache_subdir)?;

    // Download the file
    let downloaded_file = cache_subdir.join(url_filename(&case.url));
    download_file(&case.url, &downloaded_file)?;

    // Extract if needed
    if let Some(ref unzip_cmd) = case.unzip_cmd {
        extract_file(unzip_cmd, &downloaded_file, &cache_subdir)?;
    }

    if !sql_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "SQL file not found after extraction: {}",
                sql_path.display()
            ),
        ));
    }

    Ok(sql_path)
}

fn url_filename(url: &str) -> String {
    url.rsplit('/').next().unwrap_or("download").to_string()
}

fn download_file(url: &str, dest: &Path) -> io::Result<()> {
    if dest.exists() {
        return Ok(());
    }

    eprintln!("Downloading: {}", url);

    let output = Command::new("curl")
        .args([
            "-fsSL",
            "--connect-timeout",
            "30",
            "--max-time",
            "600",
            "-o",
            dest.to_str().unwrap(),
            url,
        ])
        .output()?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Download failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        ));
    }

    Ok(())
}

fn extract_file(cmd: &str, archive: &Path, dest_dir: &Path) -> io::Result<()> {
    eprintln!("Extracting: {}", archive.display());

    let status = match cmd {
        "unzip -o" => Command::new("unzip")
            .args(["-o", "-q", archive.to_str().unwrap()])
            .current_dir(dest_dir)
            .status()?,
        "tar -xf" => Command::new("tar")
            .args(["-xf", archive.to_str().unwrap()])
            .current_dir(dest_dir)
            .status()?,
        "gunzip" => {
            let output_name = archive.file_stem().unwrap().to_str().unwrap();
            let output_path = dest_dir.join(output_name);

            let file = File::open(archive)?;
            let mut decoder = flate2::read::GzDecoder::new(BufReader::new(file));
            let mut output = File::create(&output_path)?;
            io::copy(&mut decoder, &mut output)?;

            return Ok(());
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unknown extraction command: {}", cmd),
            ));
        }
    };

    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Extraction failed with status: {}", status),
        ));
    }

    Ok(())
}

/// Create a temporary directory for test output
pub fn temp_output_dir(name: &str) -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(&format!("realworld-{}-", name))
        .tempdir()
}

/// Test result tracking
#[derive(Debug, Default)]
pub struct TestResults {
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
}

impl TestResults {
    pub fn pass(&mut self) {
        self.passed += 1;
    }

    pub fn fail(&mut self) {
        self.failed += 1;
    }

    pub fn skip(&mut self) {
        self.skipped += 1;
    }

    pub fn is_success(&self) -> bool {
        self.failed == 0
    }
}
