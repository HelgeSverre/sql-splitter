//! Glob pattern expansion utilities for CLI commands.
//!
//! Provides functions to expand glob patterns like `*.sql` or `dumps/**/*.sql`
//! into lists of matching file paths.

use std::path::{Path, PathBuf};

/// Result of expanding a file pattern (either a literal path or glob pattern).
#[derive(Debug)]
pub struct ExpandedFiles {
    pub files: Vec<PathBuf>,
    #[allow(dead_code)]
    pub pattern_was_glob: bool,
}

/// Check if a path string contains glob pattern characters.
pub fn is_glob_pattern(path: &str) -> bool {
    path.contains('*') || path.contains('?') || path.contains('[')
}

/// Expand a file path or glob pattern into a list of matching files.
///
/// If the input is a literal path (no glob characters), returns that single path.
/// If the input is a glob pattern, expands it and returns all matching files.
///
/// # Errors
///
/// Returns an error if:
/// - The glob pattern is invalid
/// - No files match the pattern
/// - A literal path doesn't exist
pub fn expand_file_pattern(pattern: &Path) -> anyhow::Result<ExpandedFiles> {
    let pattern_str = pattern.to_string_lossy();

    if !is_glob_pattern(&pattern_str) {
        if !pattern.exists() {
            anyhow::bail!("file does not exist: {}", pattern.display());
        }
        return Ok(ExpandedFiles {
            files: vec![pattern.to_path_buf()],
            pattern_was_glob: false,
        });
    }

    let entries: Vec<_> = glob::glob(&pattern_str)
        .map_err(|e| anyhow::anyhow!("invalid glob pattern '{}': {}", pattern_str, e))?
        .collect();

    let mut files = Vec::new();
    for entry in entries {
        match entry {
            Ok(path) => {
                if path.is_file() {
                    files.push(path);
                }
            }
            Err(e) => {
                anyhow::bail!("error reading path for pattern '{}': {}", pattern_str, e);
            }
        }
    }

    if files.is_empty() {
        anyhow::bail!("no files match pattern: {}", pattern_str);
    }

    files.sort();

    Ok(ExpandedFiles {
        files,
        pattern_was_glob: true,
    })
}

/// Per-file outcome produced by a multi-file command's worker closure.
pub enum FileOutcome<P> {
    /// The file was processed successfully; `P` is the per-file JSON payload.
    Success(P),
    /// The file failed. `payload` is an optional per-file JSON entry (some
    /// commands report failures in their JSON results, some only aggregate).
    Failure { payload: Option<P>, error: String },
}

/// Aggregate results of a multi-file run driven by [`drive_multi_file`].
pub struct MultiRun<P> {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    /// Files never attempted because `fail_fast` stopped the run early.
    pub skipped: usize,
    pub elapsed: std::time::Duration,
    pub errors: Vec<(PathBuf, String)>,
    /// Per-file JSON payloads, in input order.
    pub payloads: Vec<P>,
}

impl<P> MultiRun<P> {
    pub fn has_failures(&self) -> bool {
        self.failed > 0
    }
}

/// Drive a multi-file (glob) command run: iterate `files`, delegate each file
/// to `per_file`, and own the success/failure bookkeeping, `fail_fast` early
/// exit, elapsed timing, and skipped-tail accounting that every glob-capable
/// command previously duplicated.
///
/// `on_skipped` builds an optional payload entry for files never attempted
/// because `fail_fast` broke out of the loop (so JSON reports can stay
/// self-consistent); return `None` to omit skipped files from the payloads.
pub fn drive_multi_file<P>(
    files: &[PathBuf],
    fail_fast: bool,
    mut per_file: impl FnMut(usize, &Path) -> FileOutcome<P>,
    mut on_skipped: impl FnMut(&Path) -> Option<P>,
) -> MultiRun<P> {
    let start = std::time::Instant::now();
    let mut run = MultiRun {
        total: files.len(),
        succeeded: 0,
        failed: 0,
        skipped: 0,
        elapsed: std::time::Duration::ZERO,
        errors: Vec::new(),
        payloads: Vec::new(),
    };

    let mut attempted = 0;
    for (idx, file) in files.iter().enumerate() {
        attempted = idx + 1;
        match per_file(idx, file) {
            FileOutcome::Success(payload) => {
                run.succeeded += 1;
                run.payloads.push(payload);
            }
            FileOutcome::Failure { payload, error } => {
                run.failed += 1;
                run.errors.push((file.clone(), error));
                if let Some(payload) = payload {
                    run.payloads.push(payload);
                }
                if fail_fast {
                    break;
                }
            }
        }
    }

    run.skipped = files.len() - attempted;
    for file in files.iter().skip(attempted) {
        if let Some(payload) = on_skipped(file) {
            run.payloads.push(payload);
        }
    }

    run.elapsed = start.elapsed();
    run
}

/// Result type for multi-file command execution.
#[derive(Debug, Default)]
pub struct MultiFileResult {
    pub total_files: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub errors: Vec<(PathBuf, String)>,
}

impl MultiFileResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&mut self) {
        self.succeeded += 1;
    }

    pub fn record_failure(&mut self, path: PathBuf, error: String) {
        self.failed += 1;
        self.errors.push((path, error));
    }

    pub fn has_failures(&self) -> bool {
        self.failed > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_is_glob_pattern() {
        assert!(!is_glob_pattern("file.sql"));
        assert!(!is_glob_pattern("/path/to/file.sql"));
        assert!(is_glob_pattern("*.sql"));
        assert!(is_glob_pattern("dir/*.sql"));
        assert!(is_glob_pattern("**/*.sql"));
        assert!(is_glob_pattern("file?.sql"));
        assert!(is_glob_pattern("[abc].sql"));
    }

    #[test]
    fn test_expand_literal_path_exists() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("test.sql");
        fs::write(&file, "SELECT 1;").unwrap();

        let result = expand_file_pattern(&file).unwrap();
        assert!(!result.pattern_was_glob);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0], file);
    }

    #[test]
    fn test_expand_literal_path_not_exists() {
        let path = PathBuf::from("/nonexistent/file.sql");
        let result = expand_file_pattern(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_expand_glob_pattern() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.sql"), "SELECT 1;").unwrap();
        fs::write(dir.path().join("b.sql"), "SELECT 2;").unwrap();
        fs::write(dir.path().join("c.txt"), "not sql").unwrap();

        let pattern = dir.path().join("*.sql");
        let result = expand_file_pattern(&pattern).unwrap();

        assert!(result.pattern_was_glob);
        assert_eq!(result.files.len(), 2);
        assert!(result.files.iter().all(|f| f.extension().unwrap() == "sql"));
    }

    #[test]
    fn test_expand_glob_no_matches() {
        let dir = TempDir::new().unwrap();
        let pattern = dir.path().join("*.sql");
        let result = expand_file_pattern(&pattern);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no files match"));
    }

    #[test]
    fn test_expand_recursive_glob() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();

        fs::write(dir.path().join("a.sql"), "SELECT 1;").unwrap();
        fs::write(subdir.join("b.sql"), "SELECT 2;").unwrap();

        let pattern = dir.path().join("**/*.sql");
        let result = expand_file_pattern(&pattern).unwrap();

        assert!(result.pattern_was_glob);
        assert_eq!(result.files.len(), 2);
    }

    #[test]
    fn test_multi_file_result() {
        let mut result = MultiFileResult::new();
        result.total_files = 3;
        result.record_success();
        result.record_success();
        result.record_failure(PathBuf::from("bad.sql"), "parse error".to_string());

        assert_eq!(result.succeeded, 2);
        assert_eq!(result.failed, 1);
        assert!(result.has_failures());
        assert_eq!(result.errors.len(), 1);
    }
}
