//! Cache manager for persistent DuckDB databases.
//!
//! Caches imported SQL dumps as DuckDB database files for fast repeated queries.
//! Cache identity is (canonical path, size, mtime, --tables set, dialect), so
//! different table selections of the same dump get separate cache slots. Writes
//! are atomic: data is staged in a `.partial` file and renamed into place, and
//! the index is only updated after the data file is committed.

use crate::parser::SqlDialect;
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Cache entry metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    /// Canonical path of the original dump file
    pub dump_path: String,
    /// SHA256 hash of (path + size + mtime + tables + dialect)
    pub cache_key: String,
    /// Size of original dump file
    pub dump_size: u64,
    /// Modification time of dump file (as Unix timestamp)
    pub dump_mtime: u64,
    /// Size of cached DuckDB file
    pub cache_size: u64,
    /// When this cache entry was created
    pub created_at: u64,
    /// Number of tables in the cache
    pub table_count: usize,
    /// Total rows in the cache
    pub row_count: u64,
    /// Tables contained in the cache (None = entry from an older version)
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

/// Cache index containing all cache entries
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct CacheIndex {
    pub entries: Vec<CacheEntry>,
}

/// Manager for cached DuckDB databases
pub struct CacheManager {
    cache_dir: PathBuf,
}

/// DuckDB names its write-ahead log by appending `.wal` to the database path.
fn wal_path(db_path: &Path) -> PathBuf {
    let mut os = db_path.as_os_str().to_os_string();
    os.push(".wal");
    PathBuf::from(os)
}

fn mtime_secs(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Normalize a table selection for cache-key hashing: lowercased (matching the
/// loader's case-insensitive filter), sorted, deduped. `None` means all tables.
fn normalize_tables(tables: Option<&[String]>) -> String {
    match tables {
        None => "all".to_string(),
        Some(tables) => {
            let mut names: Vec<String> = tables.iter().map(|t| t.to_ascii_lowercase()).collect();
            names.sort();
            names.dedup();
            names.join(",")
        }
    }
}

impl CacheManager {
    /// Create a new cache manager with the default cache directory
    pub fn new() -> Result<Self> {
        let cache_dir = Self::default_cache_dir()?;
        fs::create_dir_all(&cache_dir).context("Failed to create cache directory")?;
        Ok(Self { cache_dir })
    }

    /// Create a cache manager with a custom cache directory
    pub fn with_dir(cache_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&cache_dir).context("Failed to create cache directory")?;
        Ok(Self { cache_dir })
    }

    /// Get the default cache directory (`SQL_SPLITTER_CACHE_DIR` overrides)
    pub fn default_cache_dir() -> Result<PathBuf> {
        if let Some(dir) = std::env::var_os("SQL_SPLITTER_CACHE_DIR").filter(|d| !d.is_empty()) {
            return Ok(PathBuf::from(dir));
        }

        let cache_base = dirs::cache_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
            .context("Could not determine cache directory")?;

        Ok(cache_base.join("sql-splitter").join("duckdb"))
    }

    /// Compute the cache key for a dump file and import configuration
    pub fn compute_cache_key(
        dump_path: &Path,
        tables: Option<&[String]>,
        dialect: Option<SqlDialect>,
    ) -> Result<String> {
        let canonical = dump_path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {}", dump_path.display()))?;

        let metadata = fs::metadata(&canonical)
            .with_context(|| format!("Failed to read metadata: {}", dump_path.display()))?;

        let dialect = dialect.map_or_else(|| "auto".to_string(), |d| d.to_string());
        let key_input = format!(
            "{}:{}:{}:{}:{}",
            canonical.display(),
            metadata.len(),
            mtime_secs(&metadata),
            normalize_tables(tables),
            dialect
        );

        let mut hasher = Sha256::new();
        hasher.update(key_input.as_bytes());
        let hash = hasher.finalize();

        Ok(hex::encode(&hash[..16])) // Use first 16 bytes for shorter filename
    }

    /// Get the path where a cached database would be stored
    pub fn cache_path(&self, cache_key: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.duckdb", cache_key))
    }

    /// Get the staging path where a cache is built before being committed
    pub fn partial_path(&self, cache_key: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.duckdb.partial", cache_key))
    }

    /// Check if a valid cache exists for a dump file and import configuration.
    ///
    /// Existence is sufficient: the key encodes the dump's size and mtime, and
    /// files only appear at the final path via atomic rename of a complete copy.
    pub fn has_valid_cache(
        &self,
        dump_path: &Path,
        tables: Option<&[String]>,
        dialect: Option<SqlDialect>,
    ) -> Result<bool> {
        let cache_key = Self::compute_cache_key(dump_path, tables, dialect)?;
        Ok(self.cache_path(&cache_key).exists())
    }

    /// Get the cache path for a dump file, if a valid cache exists
    pub fn get_cache(
        &self,
        dump_path: &Path,
        tables: Option<&[String]>,
        dialect: Option<SqlDialect>,
    ) -> Result<Option<PathBuf>> {
        if self.has_valid_cache(dump_path, tables, dialect)? {
            let cache_key = Self::compute_cache_key(dump_path, tables, dialect)?;
            Ok(Some(self.cache_path(&cache_key)))
        } else {
            Ok(None)
        }
    }

    /// Remove a staged partial cache file (leftover from an interrupted run)
    pub fn discard_partial(&self, cache_key: &str) -> Result<()> {
        let partial = self.partial_path(cache_key);
        if partial.exists() {
            fs::remove_file(&partial).context("Failed to remove partial cache file")?;
        }
        let wal = wal_path(&partial);
        if wal.exists() {
            fs::remove_file(&wal)?;
        }
        Ok(())
    }

    /// Commit a fully-written partial cache file: atomically move it into place
    /// and update the index. The previous cache and index are untouched if this
    /// (or anything before it) fails.
    pub fn commit_cache(
        &self,
        dump_path: &Path,
        cache_key: &str,
        tables: Vec<String>,
        row_count: u64,
    ) -> Result<PathBuf> {
        let partial = self.partial_path(cache_key);
        let cache_path = self.cache_path(cache_key);

        if !partial.exists() {
            anyhow::bail!("No staged cache file to commit: {}", partial.display());
        }

        // rename() does not overwrite on Windows; clear the destination first
        if cache_path.exists() {
            fs::remove_file(&cache_path).context("Failed to remove old cache file")?;
        }
        let old_wal = wal_path(&cache_path);
        if old_wal.exists() {
            let _ = fs::remove_file(&old_wal);
        }

        fs::rename(&partial, &cache_path).context("Failed to move cache file into place")?;
        let partial_wal = wal_path(&partial);
        if partial_wal.exists() {
            let _ = fs::remove_file(&partial_wal);
        }

        let canonical = dump_path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {}", dump_path.display()))?;
        let metadata = fs::metadata(&canonical)?;
        let dump_mtime = mtime_secs(&metadata);
        let dump_path_str = canonical.display().to_string();

        let mut index = self.load_index()?;

        // Garbage-collect caches of older versions of this dump file
        let stale_keys: Vec<String> = index
            .entries
            .iter()
            .filter(|e| {
                e.dump_path == dump_path_str
                    && (e.dump_size != metadata.len() || e.dump_mtime != dump_mtime)
            })
            .map(|e| e.cache_key.clone())
            .collect();
        for key in &stale_keys {
            let path = self.cache_path(key);
            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(wal_path(&path));
        }
        index
            .entries
            .retain(|e| e.cache_key != cache_key && !stale_keys.contains(&e.cache_key));

        index.entries.push(CacheEntry {
            dump_path: dump_path_str,
            cache_key: cache_key.to_string(),
            dump_size: metadata.len(),
            dump_mtime,
            cache_size: fs::metadata(&cache_path).map(|m| m.len()).unwrap_or(0),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            table_count: tables.len(),
            row_count,
            tables: Some(tables),
        });

        self.save_index(&index)?;
        Ok(cache_path)
    }

    /// Load the cache index. A corrupt index is treated as empty (with a
    /// warning) so one bad write can't permanently brick cache operations.
    pub fn load_index(&self) -> Result<CacheIndex> {
        let index_path = self.cache_dir.join("index.json");

        if !index_path.exists() {
            return Ok(CacheIndex::default());
        }

        let content = fs::read_to_string(&index_path).context("Failed to read cache index")?;
        Ok(serde_json::from_str(&content).unwrap_or_else(|e| {
            eprintln!(
                "Warning: cache index {} is corrupt ({}); treating it as empty",
                index_path.display(),
                e
            );
            CacheIndex::default()
        }))
    }

    /// Save the cache index atomically (write to temp file, then rename)
    fn save_index(&self, index: &CacheIndex) -> Result<()> {
        let index_path = self.cache_dir.join("index.json");
        let tmp_path = self.cache_dir.join("index.json.tmp");
        let content =
            serde_json::to_string_pretty(index).context("Failed to serialize cache index")?;
        fs::write(&tmp_path, content).context("Failed to write cache index")?;
        fs::rename(&tmp_path, &index_path).context("Failed to replace cache index")?;
        Ok(())
    }

    /// List all cache entries
    pub fn list_entries(&self) -> Result<Vec<CacheEntry>> {
        let index = self.load_index()?;
        Ok(index.entries)
    }

    /// Remove a specific cache entry
    pub fn remove_cache(&self, cache_key: &str) -> Result<()> {
        let cache_path = self.cache_path(cache_key);

        if cache_path.exists() {
            fs::remove_file(&cache_path).context("Failed to remove cache file")?;
        }

        // Also remove WAL and staged partial files if they exist
        let wal = wal_path(&cache_path);
        if wal.exists() {
            fs::remove_file(&wal)?;
        }
        self.discard_partial(cache_key)?;

        // Update index
        let mut index = self.load_index()?;
        index.entries.retain(|e| e.cache_key != cache_key);
        self.save_index(&index)?;

        Ok(())
    }

    /// Clear all cached databases.
    ///
    /// Sweeps the whole cache directory (data files, WAL files, staged
    /// partials) rather than trusting the index, so it also recovers from a
    /// lost/corrupt index and collects files written by older versions.
    /// Returns the number of cached databases removed.
    pub fn clear_all(&self) -> Result<usize> {
        let mut count = 0;

        for entry in fs::read_dir(&self.cache_dir).context("Failed to read cache directory")? {
            let path = entry?.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".duckdb") {
                count += 1;
            }
            if name.ends_with(".duckdb") || name.ends_with(".wal") || name.ends_with(".partial") {
                fs::remove_file(&path)
                    .with_context(|| format!("Failed to remove {}", path.display()))?;
            }
        }

        self.save_index(&CacheIndex::default())?;
        Ok(count)
    }

    /// Get total cache size in bytes
    pub fn total_size(&self) -> Result<u64> {
        let entries = self.list_entries()?;
        Ok(entries.iter().map(|e| e.cache_size).sum())
    }

    /// Get the cache directory path
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_test_cache() -> (CacheManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache_manager = CacheManager::with_dir(temp_dir.path().to_path_buf()).unwrap();
        (cache_manager, temp_dir)
    }

    fn strings(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_cache_key_computation() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key1 = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        let key2 = CacheManager::compute_cache_key(&test_file, None, None).unwrap();

        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 32); // 16 bytes hex encoded
    }

    #[test]
    fn test_cache_key_changes_with_content() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");

        fs::write(&test_file, "SELECT 1;").unwrap();
        let key1 = CacheManager::compute_cache_key(&test_file, None, None).unwrap();

        // Modify the file with different size (which is always captured, unlike mtime)
        fs::write(&test_file, "SELECT 2; -- with extra content to change size").unwrap();
        let key2 = CacheManager::compute_cache_key(&test_file, None, None).unwrap();

        // Key should be different because size changed
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_includes_tables() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let all = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        let users =
            CacheManager::compute_cache_key(&test_file, Some(&strings(&["users"])), None).unwrap();
        let both =
            CacheManager::compute_cache_key(&test_file, Some(&strings(&["users", "orders"])), None)
                .unwrap();

        assert_ne!(all, users);
        assert_ne!(all, both);
        assert_ne!(users, both);
    }

    #[test]
    fn test_cache_key_normalizes_tables() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let messy = CacheManager::compute_cache_key(
            &test_file,
            Some(&strings(&["Orders", "users", "USERS"])),
            None,
        )
        .unwrap();
        let clean =
            CacheManager::compute_cache_key(&test_file, Some(&strings(&["orders", "users"])), None)
                .unwrap();

        assert_eq!(messy, clean);
    }

    #[test]
    fn test_cache_key_includes_dialect() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let auto = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        let mysql =
            CacheManager::compute_cache_key(&test_file, None, Some(SqlDialect::MySql)).unwrap();

        assert_ne!(auto, mysql);
    }

    #[test]
    fn test_cache_path() {
        let (cache_manager, _temp_dir) = setup_test_cache();
        let cache_path = cache_manager.cache_path("abc123");
        assert!(cache_path.to_string_lossy().ends_with("abc123.duckdb"));
    }

    #[test]
    fn test_has_valid_cache_when_missing() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        assert!(!cache_manager
            .has_valid_cache(&test_file, None, None)
            .unwrap());
    }

    #[test]
    fn test_partial_is_not_a_valid_cache() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        fs::write(cache_manager.partial_path(&key), b"garbage").unwrap();

        assert!(!cache_manager
            .has_valid_cache(&test_file, None, None)
            .unwrap());
        assert!(cache_manager
            .get_cache(&test_file, None, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_commit_cache_renames_and_records_size() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        fs::write(cache_manager.partial_path(&key), b"fake database contents").unwrap();

        let cache_path = cache_manager
            .commit_cache(&test_file, &key, strings(&["users"]), 42)
            .unwrap();

        assert!(cache_path.exists());
        assert!(!cache_manager.partial_path(&key).exists());

        let entries = cache_manager.list_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].cache_size > 0);
        assert_eq!(entries[0].tables, Some(strings(&["users"])));
        assert_eq!(entries[0].table_count, 1);
        assert_eq!(entries[0].row_count, 42);
    }

    #[test]
    fn test_commit_without_partial_leaves_index_intact() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        fs::write(cache_manager.partial_path(&key), b"data").unwrap();
        cache_manager
            .commit_cache(&test_file, &key, strings(&["users"]), 1)
            .unwrap();

        // Committing a key with no staged file must fail without touching the index
        let result = cache_manager.commit_cache(&test_file, "deadbeef", strings(&["x"]), 1);
        assert!(result.is_err());
        assert_eq!(cache_manager.list_entries().unwrap().len(), 1);
    }

    #[test]
    fn test_two_table_sets_coexist() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key1 =
            CacheManager::compute_cache_key(&test_file, Some(&strings(&["users"])), None).unwrap();
        let key2 = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        assert_ne!(key1, key2);

        fs::write(cache_manager.partial_path(&key1), b"one").unwrap();
        cache_manager
            .commit_cache(&test_file, &key1, strings(&["users"]), 1)
            .unwrap();
        fs::write(cache_manager.partial_path(&key2), b"two").unwrap();
        cache_manager
            .commit_cache(&test_file, &key2, strings(&["users", "orders"]), 2)
            .unwrap();

        assert!(cache_manager.cache_path(&key1).exists());
        assert!(cache_manager.cache_path(&key2).exists());
        assert_eq!(cache_manager.list_entries().unwrap().len(), 2);
    }

    #[test]
    fn test_gc_deletes_caches_of_changed_dump() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let old_key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        fs::write(cache_manager.partial_path(&old_key), b"v1").unwrap();
        cache_manager
            .commit_cache(&test_file, &old_key, strings(&["users"]), 1)
            .unwrap();

        // Change the dump (size change guarantees a new key)
        fs::write(&test_file, "SELECT 2; -- different size now").unwrap();
        let new_key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        assert_ne!(old_key, new_key);
        fs::write(cache_manager.partial_path(&new_key), b"v2").unwrap();
        cache_manager
            .commit_cache(&test_file, &new_key, strings(&["users"]), 2)
            .unwrap();

        assert!(!cache_manager.cache_path(&old_key).exists());
        let entries = cache_manager.list_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cache_key, new_key);
    }

    #[test]
    fn test_corrupt_index_recovers_as_empty() {
        let (cache_manager, temp_dir) = setup_test_cache();
        fs::write(temp_dir.path().join("index.json"), "{broken json").unwrap();

        let index = cache_manager.load_index().unwrap();
        assert!(index.entries.is_empty());

        // And a subsequent commit works normally
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();
        let key = CacheManager::compute_cache_key(&test_file, None, None).unwrap();
        fs::write(cache_manager.partial_path(&key), b"data").unwrap();
        cache_manager
            .commit_cache(&test_file, &key, strings(&["users"]), 1)
            .unwrap();
        assert_eq!(cache_manager.list_entries().unwrap().len(), 1);
    }

    #[test]
    fn test_index_old_format_parses_with_unknown_tables() {
        let (cache_manager, temp_dir) = setup_test_cache();
        let old_entry = r#"{"entries":[{"dump_path":"/tmp/a.sql","cache_key":"abc","dump_size":1,"dump_mtime":2,"cache_size":3,"created_at":4,"table_count":5,"row_count":6}]}"#;
        fs::write(temp_dir.path().join("index.json"), old_entry).unwrap();

        let entries = cache_manager.list_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tables, None);
    }

    #[test]
    fn test_clear_all_sweeps_directory() {
        let (cache_manager, temp_dir) = setup_test_cache();
        fs::write(temp_dir.path().join("aa.duckdb"), b"x").unwrap();
        fs::write(temp_dir.path().join("aa.duckdb.wal"), b"x").unwrap();
        fs::write(temp_dir.path().join("bb.duckdb.partial"), b"x").unwrap();
        fs::write(temp_dir.path().join("index.json"), "{broken").unwrap();

        let count = cache_manager.clear_all().unwrap();
        assert_eq!(count, 1);
        assert!(!temp_dir.path().join("aa.duckdb").exists());
        assert!(!temp_dir.path().join("aa.duckdb.wal").exists());
        assert!(!temp_dir.path().join("bb.duckdb.partial").exists());
        assert!(cache_manager.list_entries().unwrap().is_empty());
    }

    #[test]
    fn test_list_entries_empty() {
        let (cache_manager, _temp_dir) = setup_test_cache();
        let entries = cache_manager.list_entries().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_total_size_empty() {
        let (cache_manager, _temp_dir) = setup_test_cache();
        assert_eq!(cache_manager.total_size().unwrap(), 0);
    }
}
