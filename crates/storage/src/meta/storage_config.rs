use crate::compression::BlockCompressionType;
use paro_common::error::{self as paro_error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default sort partition size used by planner/executor heuristics.
pub const DEFAULT_SORT_PARTITION_SIZE: usize = 122_880;

/// Global storage configuration used by metadata subsystem.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StorageConfig {
    /// Data page size in bytes. Default: 256KB.
    pub page_size: usize,
    /// Maximum rows in one segment. Default: 1M.
    pub segment_max_rows: usize,
    /// MemTable memory limit in bytes. Default: 128MB.
    pub memtable_memory_limit: usize,
    /// Default compression for hot data.
    pub default_compression: BlockCompressionType,
    /// Compression for cold data.
    pub cold_compression: BlockCompressionType,
    /// Root directory of data files.
    pub data_root_dir: String,
    /// Root directory of metadata files.
    pub meta_root_dir: String,
    /// In-memory schema cache capacity.
    pub schema_cache_capacity: usize,
    /// Number of tablets loaded in parallel on startup.
    pub parallel_tablet_load: usize,
}

impl StorageConfig {
    pub const DEFAULT_PAGE_SIZE: usize = 256 * 1024;
    pub const DEFAULT_SEGMENT_MAX_ROWS: usize = 1_000_000;
    pub const DEFAULT_MEMTABLE_MEMORY_LIMIT: usize = 128 * 1024 * 1024;
    pub const DEFAULT_SCHEMA_CACHE_CAPACITY: usize = 1024;

    pub fn builder() -> StorageConfigBuilder {
        StorageConfigBuilder::default()
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to read StorageConfig from {:?}: {}",
                path, e
            ))
        })?;

        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
            paro_error::invalid_input(format!(
                "Failed to deserialize StorageConfig from {:?}: {}",
                path, e
            ))
        })?;
        let has_meta_root_dir = value.get("meta_root_dir").is_some();
        let mut config: StorageConfig = serde_json::from_value(value).map_err(|e| {
            paro_error::invalid_input(format!(
                "Failed to deserialize StorageConfig from {:?}: {}",
                path, e
            ))
        })?;

        if !has_meta_root_dir || config.meta_root_dir.trim().is_empty() {
            config.meta_root_dir = Self::derive_meta_root_dir(&config.data_root_dir);
        }
        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        self.validate()?;

        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                paro_error::io_error(format!(
                    "Failed to create StorageConfig parent directory {:?}: {}",
                    parent, e
                ))
            })?;
        }

        let tmp_path = Self::temp_path(path);
        let payload = serde_json::to_vec_pretty(self).map_err(|e| {
            paro_error::internal(format!("Failed to serialize StorageConfig to JSON: {}", e))
        })?;

        let mut file = File::create(&tmp_path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create StorageConfig temp file {:?}: {}",
                tmp_path, e
            ))
        })?;
        file.write_all(&payload).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to write StorageConfig temp file {:?}: {}",
                tmp_path, e
            ))
        })?;
        file.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync StorageConfig temp file {:?}: {}",
                tmp_path, e
            ))
        })?;

        fs::rename(&tmp_path, path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            paro_error::io_error(format!(
                "Failed to atomically save StorageConfig {:?} -> {:?}: {}",
                tmp_path, path, e
            ))
        })?;

        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if !self.page_size.is_power_of_two() {
            return Err(paro_error::invalid_input(format!(
                "page_size must be a power of two, got {}",
                self.page_size
            )));
        }
        if self.segment_max_rows == 0 {
            return Err(paro_error::invalid_input(
                "segment_max_rows must be greater than 0",
            ));
        }
        if self.memtable_memory_limit == 0 {
            return Err(paro_error::invalid_input(
                "memtable_memory_limit must be greater than 0",
            ));
        }
        if self.schema_cache_capacity == 0 {
            return Err(paro_error::invalid_input(
                "schema_cache_capacity must be greater than 0",
            ));
        }
        if self.parallel_tablet_load == 0 {
            return Err(paro_error::invalid_input(
                "parallel_tablet_load must be greater than 0",
            ));
        }

        if self.data_root_dir.trim().is_empty() {
            return Err(paro_error::invalid_input("data_root_dir must not be empty"));
        }
        if self.meta_root_dir.trim().is_empty() {
            return Err(paro_error::invalid_input("meta_root_dir must not be empty"));
        }

        let data_root = Path::new(&self.data_root_dir);
        let meta_root = Path::new(&self.meta_root_dir);
        Self::ensure_dir_writable(data_root, "data_root_dir")?;
        Self::ensure_dir_writable(meta_root, "meta_root_dir")?;
        Ok(())
    }

    fn derive_meta_root_dir(data_root_dir: &str) -> String {
        Path::new(data_root_dir)
            .join("meta")
            .to_string_lossy()
            .into_owned()
    }

    fn temp_path(path: &Path) -> PathBuf {
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        let file_name = path
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_else(|| "storage_config.json".to_string());
        path.with_file_name(format!("{}.tmp-{}-{}", file_name, pid, sequence))
    }

    fn ensure_dir_writable(path: &Path, label: &str) -> Result<()> {
        fs::create_dir_all(path).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to create {} directory {:?}: {}",
                label, path, e
            ))
        })?;

        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let test_file = path.join(format!(
            ".write-test-{}-{}-{}",
            label,
            std::process::id(),
            stamp
        ));

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&test_file)
            .map_err(|e| {
                paro_error::io_error(format!("{} is not writable ({:?}): {}", label, path, e))
            })?;
        file.write_all(b"ok").map_err(|e| {
            paro_error::io_error(format!(
                "Failed to write {} validation file {:?}: {}",
                label, test_file, e
            ))
        })?;
        file.sync_all().map_err(|e| {
            paro_error::io_error(format!(
                "Failed to fsync {} validation file {:?}: {}",
                label, test_file, e
            ))
        })?;

        fs::remove_file(&test_file).map_err(|e| {
            paro_error::io_error(format!(
                "Failed to cleanup {} validation file {:?}: {}",
                label, test_file, e
            ))
        })?;
        Ok(())
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let data_root_dir = "./data".to_string();
        Self {
            page_size: Self::DEFAULT_PAGE_SIZE,
            segment_max_rows: Self::DEFAULT_SEGMENT_MAX_ROWS,
            memtable_memory_limit: Self::DEFAULT_MEMTABLE_MEMORY_LIMIT,
            default_compression: BlockCompressionType::Lz4,
            cold_compression: BlockCompressionType::Zstd,
            meta_root_dir: Self::derive_meta_root_dir(&data_root_dir),
            data_root_dir,
            schema_cache_capacity: Self::DEFAULT_SCHEMA_CACHE_CAPACITY,
            parallel_tablet_load: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .max(1),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StorageConfigBuilder {
    page_size: Option<usize>,
    segment_max_rows: Option<usize>,
    memtable_memory_limit: Option<usize>,
    default_compression: Option<BlockCompressionType>,
    cold_compression: Option<BlockCompressionType>,
    data_root_dir: Option<String>,
    meta_root_dir: Option<String>,
    schema_cache_capacity: Option<usize>,
    parallel_tablet_load: Option<usize>,
}

impl StorageConfigBuilder {
    pub fn page_size(mut self, page_size: usize) -> Self {
        self.page_size = Some(page_size);
        self
    }

    pub fn segment_max_rows(mut self, segment_max_rows: usize) -> Self {
        self.segment_max_rows = Some(segment_max_rows);
        self
    }

    pub fn memtable_memory_limit(mut self, memtable_memory_limit: usize) -> Self {
        self.memtable_memory_limit = Some(memtable_memory_limit);
        self
    }

    pub fn default_compression(mut self, default_compression: BlockCompressionType) -> Self {
        self.default_compression = Some(default_compression);
        self
    }

    pub fn cold_compression(mut self, cold_compression: BlockCompressionType) -> Self {
        self.cold_compression = Some(cold_compression);
        self
    }

    pub fn data_root_dir(mut self, data_root_dir: impl Into<String>) -> Self {
        self.data_root_dir = Some(data_root_dir.into());
        self
    }

    pub fn meta_root_dir(mut self, meta_root_dir: impl Into<String>) -> Self {
        self.meta_root_dir = Some(meta_root_dir.into());
        self
    }

    pub fn schema_cache_capacity(mut self, schema_cache_capacity: usize) -> Self {
        self.schema_cache_capacity = Some(schema_cache_capacity);
        self
    }

    pub fn parallel_tablet_load(mut self, parallel_tablet_load: usize) -> Self {
        self.parallel_tablet_load = Some(parallel_tablet_load);
        self
    }

    pub fn build(self) -> Result<StorageConfig> {
        let defaults = StorageConfig::default();

        let data_root_dir = self
            .data_root_dir
            .unwrap_or_else(|| defaults.data_root_dir.clone());
        let meta_root_dir = self
            .meta_root_dir
            .unwrap_or_else(|| StorageConfig::derive_meta_root_dir(&data_root_dir));

        let config = StorageConfig {
            page_size: self.page_size.unwrap_or(defaults.page_size),
            segment_max_rows: self.segment_max_rows.unwrap_or(defaults.segment_max_rows),
            memtable_memory_limit: self
                .memtable_memory_limit
                .unwrap_or(defaults.memtable_memory_limit),
            default_compression: self
                .default_compression
                .unwrap_or(defaults.default_compression),
            cold_compression: self.cold_compression.unwrap_or(defaults.cold_compression),
            data_root_dir,
            meta_root_dir,
            schema_cache_capacity: self
                .schema_cache_capacity
                .unwrap_or(defaults.schema_cache_capacity),
            parallel_tablet_load: self
                .parallel_tablet_load
                .unwrap_or(defaults.parallel_tablet_load),
        };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn storage_config_roundtrip_test() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("meta/config/storage.json");
        let data_root = temp_dir.path().join("data");
        let meta_root = temp_dir.path().join("meta");

        let config = StorageConfig::builder()
            .data_root_dir(data_root.to_string_lossy())
            .meta_root_dir(meta_root.to_string_lossy())
            .page_size(512 * 1024)
            .segment_max_rows(2_000_000)
            .memtable_memory_limit(64 * 1024 * 1024)
            .default_compression(BlockCompressionType::Lz4)
            .cold_compression(BlockCompressionType::Zstd)
            .schema_cache_capacity(4096)
            .parallel_tablet_load(8)
            .build()
            .unwrap();

        config.save(&config_path).unwrap();
        let loaded = StorageConfig::load(&config_path).unwrap();
        assert_eq!(loaded, config);
    }

    #[test]
    fn storage_config_validation_test() {
        let temp_dir = tempdir().unwrap();
        let data_root = temp_dir.path().join("data");
        let meta_root = temp_dir.path().join("meta");

        let invalid = StorageConfig {
            page_size: 300 * 1024,
            segment_max_rows: 0,
            memtable_memory_limit: 0,
            default_compression: BlockCompressionType::Lz4,
            cold_compression: BlockCompressionType::Zstd,
            data_root_dir: data_root.to_string_lossy().into_owned(),
            meta_root_dir: meta_root.to_string_lossy().into_owned(),
            schema_cache_capacity: 0,
            parallel_tablet_load: 0,
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn storage_config_default_values_test() {
        let config = StorageConfig::default();
        assert_eq!(config.page_size, 256 * 1024);
        assert_eq!(config.segment_max_rows, 1_000_000);
        assert_eq!(config.memtable_memory_limit, 128 * 1024 * 1024);
        assert_eq!(config.default_compression, BlockCompressionType::Lz4);
        assert_eq!(config.cold_compression, BlockCompressionType::Zstd);
        assert_eq!(config.data_root_dir, "./data");
        assert_eq!(config.meta_root_dir, "./data/meta");
        assert!(config.schema_cache_capacity > 0);
        assert!(config.parallel_tablet_load > 0);
    }

    #[test]
    fn storage_config_load_derives_meta_root_dir_test() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("storage.json");
        let custom_data_root = temp_dir.path().join("custom-data");

        let payload = format!(
            r#"{{
  "page_size": 262144,
  "segment_max_rows": 1000000,
  "memtable_memory_limit": 134217728,
  "default_compression": "lz4",
  "cold_compression": "zstd",
  "data_root_dir": "{}",
  "schema_cache_capacity": 1024,
  "parallel_tablet_load": 4
}}"#,
            custom_data_root.to_string_lossy()
        );
        fs::write(&config_path, payload.as_bytes()).unwrap();

        let loaded = StorageConfig::load(&config_path).unwrap();
        assert_eq!(
            loaded.data_root_dir,
            custom_data_root.to_string_lossy().into_owned()
        );
        assert_eq!(
            loaded.meta_root_dir,
            custom_data_root.join("meta").to_string_lossy().into_owned()
        );
    }
}
