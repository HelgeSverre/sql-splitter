//! Cache manager for persistent DuckDB databases.
//!
//! Caches imported SQL dumps as DuckDB database files for fast repeated queries.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Cache entry metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    /// Original dump file path
    pub dump_path: String,
    /// SHA256 hash of (path + size + mtime)
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

    /// Get the default cache directory
    pub fn default_cache_dir() -> Result<PathBuf> {
        let cache_base = dirs::cache_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
            .context("Could not determine cache directory")?;

        Ok(cache_base.join("sql-splitter").join("duckdb"))
    }

    /// Compute the cache key for a dump file
    pub fn compute_cache_key(dump_path: &Path) -> Result<String> {
        let canonical = dump_path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {}", dump_path.display()))?;

        let metadata = fs::metadata(&canonical)
            .with_context(|| format!("Failed to read metadata: {}", dump_path.display()))?;

        let mtime = metadata
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let key_input = format!("{}:{}:{}", canonical.display(), metadata.len(), mtime);

        let mut hasher = Sha256::new();
        hasher.update(key_input.as_bytes());
        let hash = hasher.finalize();

        Ok(hex::encode(&hash[..16])) // Use first 16 bytes for shorter filename
    }

    /// Get the path where a cached database would be stored
    pub fn cache_path(&self, cache_key: &str) -> PathBuf {
        self.cache_dir.join(format!("{}.duckdb", cache_key))
    }

    /// Check if a valid cache exists for a dump file
    pub fn has_valid_cache(&self, dump_path: &Path) -> Result<bool> {
        let cache_key = Self::compute_cache_key(dump_path)?;
        let cache_path = self.cache_path(&cache_key);

        if !cache_path.exists() {
            return Ok(false);
        }

        // Check if cache is newer than dump
        let dump_mtime = fs::metadata(dump_path)?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let cache_mtime = fs::metadata(&cache_path)?
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH);

        Ok(cache_mtime > dump_mtime)
    }

    /// Get the cache path for a dump file, if a valid cache exists
    pub fn get_cache(&self, dump_path: &Path) -> Result<Option<PathBuf>> {
        if self.has_valid_cache(dump_path)? {
            let cache_key = Self::compute_cache_key(dump_path)?;
            Ok(Some(self.cache_path(&cache_key)))
        } else {
            Ok(None)
        }
    }

    /// Create a new cache entry for a dump file
    pub fn create_cache(
        &self,
        dump_path: &Path,
        table_count: usize,
        row_count: u64,
    ) -> Result<PathBuf> {
        let cache_key = Self::compute_cache_key(dump_path)?;
        let cache_path = self.cache_path(&cache_key);

        // Update index
        self.update_index(dump_path, &cache_key, table_count, row_count)?;

        Ok(cache_path)
    }

    /// Update the cache index
    fn update_index(
        &self,
        dump_path: &Path,
        cache_key: &str,
        table_count: usize,
        row_count: u64,
    ) -> Result<()> {
        let mut index = self.load_index()?;

        let metadata = fs::metadata(dump_path)?;
        let dump_mtime = metadata
            .modified()
            .unwrap_or(SystemTime::UNIX_EPOCH)
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let cache_path = self.cache_path(cache_key);
        let cache_size = fs::metadata(&cache_path).map(|m| m.len()).unwrap_or(0);

        let entry = CacheEntry {
            dump_path: dump_path.display().to_string(),
            cache_key: cache_key.to_string(),
            dump_size: metadata.len(),
            dump_mtime,
            cache_size,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            table_count,
            row_count,
        };

        // Remove old entry for this dump path
        index
            .entries
            .retain(|e| e.dump_path != dump_path.display().to_string());
        index.entries.push(entry);

        self.save_index(&index)?;
        Ok(())
    }

    /// Load the cache index
    pub fn load_index(&self) -> Result<CacheIndex> {
        let index_path = self.cache_dir.join("index.json");

        if !index_path.exists() {
            return Ok(CacheIndex::default());
        }

        let content = fs::read_to_string(&index_path).context("Failed to read cache index")?;
        serde_json::from_str(&content).context("Failed to parse cache index")
    }

    /// Save the cache index
    fn save_index(&self, index: &CacheIndex) -> Result<()> {
        let index_path = self.cache_dir.join("index.json");
        let content =
            serde_json::to_string_pretty(index).context("Failed to serialize cache index")?;
        fs::write(&index_path, content).context("Failed to write cache index")?;
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

        // Also remove WAL file if it exists
        let wal_path = cache_path.with_extension("duckdb.wal");
        if wal_path.exists() {
            fs::remove_file(&wal_path)?;
        }

        // Update index
        let mut index = self.load_index()?;
        index.entries.retain(|e| e.cache_key != cache_key);
        self.save_index(&index)?;

        Ok(())
    }

    /// Clear all cached databases
    pub fn clear_all(&self) -> Result<usize> {
        let entries = self.list_entries()?;
        let count = entries.len();

        for entry in entries {
            self.remove_cache(&entry.cache_key)?;
        }

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

impl Default for CacheManager {
    fn default() -> Self {
        Self::new().expect("Failed to create cache manager")
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

    #[test]
    fn test_cache_key_computation() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");
        fs::write(&test_file, "SELECT 1;").unwrap();

        let key1 = CacheManager::compute_cache_key(&test_file).unwrap();
        let key2 = CacheManager::compute_cache_key(&test_file).unwrap();

        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 32); // 16 bytes hex encoded
    }

    #[test]
    fn test_cache_key_changes_with_content() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.sql");

        fs::write(&test_file, "SELECT 1;").unwrap();
        let key1 = CacheManager::compute_cache_key(&test_file).unwrap();

        // Modify the file with different size (which is always captured, unlike mtime)
        fs::write(&test_file, "SELECT 2; -- with extra content to change size").unwrap();
        let key2 = CacheManager::compute_cache_key(&test_file).unwrap();

        // Key should be different because size changed
        assert_ne!(key1, key2);
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

        assert!(!cache_manager.has_valid_cache(&test_file).unwrap());
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
